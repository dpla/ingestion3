"""Anomaly detection for ingest record counts.

Detects unexpected drops in record counts by comparing current ingest
to historical baselines from S3. Implements hard stops when anomalies
are detected.

Thresholds (global defaults):
- Harvest drop: >15% fewer records than previous harvest
- Mapping failure rate: >20% of records failing
- Output drop: >15% fewer mapped records than previous
"""

import subprocess
import re
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class AnomalyType(str, Enum):
    HARVEST_DROP = "harvest_drop"
    HIGH_FAILURE_RATE = "high_failure_rate"
    OUTPUT_DROP = "output_drop"
    FAILURE_RATE_SPIKE = "failure_rate_spike"


@dataclass
class IngestCounts:
    """Record counts from a _SUMMARY file."""
    hub: str
    timestamp: str
    stage: str = "mapping"  # "harvest" or "mapping"
    attempted: int = 0
    successful: int = 0
    failed: int = 0
    
    @property
    def failure_rate(self) -> float:
        if self.attempted == 0:
            return 0.0
        return self.failed / self.attempted
    
    @property
    def success_rate(self) -> float:
        return 1.0 - self.failure_rate
    
    @property
    def date_formatted(self) -> str:
        """Convert timestamp like 20251031_004227 to 10-31-2025."""
        if len(self.timestamp) >= 8:
            try:
                year = self.timestamp[0:4]
                month = self.timestamp[4:6]
                day = self.timestamp[6:8]
                return f"{month}-{day}-{year}"
            except (IndexError, ValueError):
                pass
        return self.timestamp
    
    def to_dict(self) -> dict:
        return {
            'hub': self.hub,
            'timestamp': self.timestamp,
            'date': self.date_formatted,
            'stage': self.stage,
            'attempted': self.attempted,
            'successful': self.successful,
            'failed': self.failed,
            'failure_rate': round(self.failure_rate, 4),
        }


@dataclass
class Anomaly:
    """Detected anomaly in record counts."""
    hub: str
    anomaly_type: AnomalyType
    severity: str  # 'warning' or 'critical'
    message: str
    current_value: float
    baseline_value: float
    threshold: float
    percent_change: float
    
    def to_dict(self) -> dict:
        return {
            'hub': self.hub,
            'type': self.anomaly_type.value,
            'severity': self.severity,
            'message': self.message,
            'current': self.current_value,
            'baseline': self.baseline_value,
            'threshold': self.threshold,
            'percent_change': round(self.percent_change, 2),
        }


@dataclass
class AnomalyReport:
    """Report of all anomalies detected for a hub."""
    hub: str
    current_mapping: Optional[IngestCounts] = None
    baseline_mapping: Optional[IngestCounts] = None
    current_harvest: Optional[IngestCounts] = None
    baseline_harvest: Optional[IngestCounts] = None
    anomalies: list[Anomaly] = field(default_factory=list)
    
    # Backward compatibility
    @property
    def current(self) -> Optional[IngestCounts]:
        return self.current_mapping
    
    @property
    def baseline(self) -> Optional[IngestCounts]:
        return self.baseline_mapping
    
    @property
    def has_critical(self) -> bool:
        return any(a.severity == 'critical' for a in self.anomalies)
    
    @property
    def should_halt(self) -> bool:
        """Return True if ingest should be halted."""
        return self.has_critical
    
    def to_dict(self) -> dict:
        return {
            'hub': self.hub,
            'current_mapping': self.current_mapping.to_dict() if self.current_mapping else None,
            'baseline_mapping': self.baseline_mapping.to_dict() if self.baseline_mapping else None,
            'current_harvest': self.current_harvest.to_dict() if self.current_harvest else None,
            'baseline_harvest': self.baseline_harvest.to_dict() if self.baseline_harvest else None,
            'anomalies': [a.to_dict() for a in self.anomalies],
            'should_halt': self.should_halt,
        }
    
    def format_report(self) -> str:
        """Format a human-readable report."""
        lines = []
        lines.append("=" * 70)
        
        if self.should_halt:
            lines.append(f"⛔ ANOMALY DETECTED - HARD STOP: {self.hub.upper()}")
        else:
            lines.append(f"⚠️  ANOMALY WARNING: {self.hub.upper()}")
        
        lines.append("=" * 70)
        
        # Harvest comparison
        lines.append("\n📥 HARVEST RECORDS")
        lines.append("-" * 40)
        if self.baseline_harvest:
            lines.append(f"  Baseline ({self.baseline_harvest.date_formatted}):")
            lines.append(f"    Records: {self.baseline_harvest.attempted:,}")
        else:
            lines.append("  Baseline: No historical harvest data")
        
        if self.current_harvest:
            lines.append(f"  Current  ({self.current_harvest.date_formatted}):")
            lines.append(f"    Records: {self.current_harvest.attempted:,}")
            if self.baseline_harvest and self.baseline_harvest.attempted > 0:
                change = ((self.current_harvest.attempted - self.baseline_harvest.attempted) 
                         / self.baseline_harvest.attempted * 100)
                arrow = "📈" if change >= 0 else "📉"
                lines.append(f"    Change:  {arrow} {change:+.1f}%")
        else:
            lines.append("  Current: No harvest data found")
        
        # Mapping comparison
        lines.append("\n🗺️  MAPPING RESULTS")
        lines.append("-" * 40)
        if self.baseline_mapping:
            lines.append(f"  Baseline ({self.baseline_mapping.date_formatted}):")
            lines.append(f"    Attempted:  {self.baseline_mapping.attempted:,}")
            lines.append(f"    Successful: {self.baseline_mapping.successful:,}")
            lines.append(f"    Failed:     {self.baseline_mapping.failed:,} ({self.baseline_mapping.failure_rate:.1%})")
        else:
            lines.append("  Baseline: No historical mapping data")
        
        if self.current_mapping:
            lines.append(f"  Current  ({self.current_mapping.date_formatted}):")
            lines.append(f"    Attempted:  {self.current_mapping.attempted:,}")
            lines.append(f"    Successful: {self.current_mapping.successful:,}")
            lines.append(f"    Failed:     {self.current_mapping.failed:,} ({self.current_mapping.failure_rate:.1%})")
            if self.baseline_mapping and self.baseline_mapping.successful > 0:
                change = ((self.current_mapping.successful - self.baseline_mapping.successful) 
                         / self.baseline_mapping.successful * 100)
                arrow = "📈" if change >= 0 else "📉"
                lines.append(f"    Change:  {arrow} {change:+.1f}%")
        else:
            lines.append("  Current: No mapping data found")
        
        # Anomalies
        if self.anomalies:
            lines.append(f"\n🚨 ANOMALIES DETECTED ({len(self.anomalies)})")
            lines.append("-" * 40)
            for a in self.anomalies:
                icon = "🔴 CRITICAL" if a.severity == 'critical' else "🟡 WARNING"
                lines.append(f"  {icon}: {a.anomaly_type.value}")
                lines.append(f"    {a.message}")
                lines.append(f"    Change: {a.percent_change:+.1f}% (threshold: ±{a.threshold:.0%})")
        
        if self.should_halt:
            lines.append("\n" + "=" * 70)
            lines.append("⛔ ACTION REQUIRED:")
            lines.append("  To proceed anyway:    --force-sync " + self.hub)
            lines.append("  To update baseline:   --update-baseline " + self.hub)
            lines.append("  To investigate:       Check harvest source / OAI feed")
        
        lines.append("=" * 70)
        return "\n".join(lines)


class AnomalyDetector:
    """Detects anomalies in ingest record counts."""
    
    # Global default thresholds
    HARVEST_DROP_THRESHOLD = 0.15      # 15% drop triggers warning
    HARVEST_DROP_CRITICAL = 0.30       # 30% drop triggers hard stop
    FAILURE_RATE_THRESHOLD = 0.20      # 20% failure rate triggers warning
    FAILURE_RATE_CRITICAL = 0.40       # 40% failure rate triggers hard stop
    OUTPUT_DROP_THRESHOLD = 0.15       # 15% output drop triggers warning
    OUTPUT_DROP_CRITICAL = 0.30        # 30% output drop triggers hard stop
    FAILURE_SPIKE_THRESHOLD = 0.10     # 10% increase in failure rate
    
    def __init__(
        self,
        s3_bucket: str = "dpla-master-dataset",
        aws_profile: str = "dpla",
        data_dir: str = "/Users/scott/dpla/data"
    ):
        self.s3_bucket = s3_bucket
        self.aws_profile = aws_profile
        self.data_dir = Path(data_dir)
    
    def list_s3_directories(self, hub: str, stage: str) -> list[str]:
        """List all directories in S3 for a hub/stage, sorted by date descending."""
        cmd = f"aws s3 ls s3://{self.s3_bucket}/{hub}/{stage}/ --profile {self.aws_profile}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            return []
        
        dirs = []
        for line in result.stdout.strip().split('\n'):
            if 'PRE' in line:
                match = re.search(r'PRE\s+(\d{8}_\d{6}-[^/]+)/', line)
                if match:
                    dirs.append(match.group(1))
        
        dirs.sort(reverse=True)
        return dirs
    
    def get_counts_from_s3(
        self, 
        hub: str, 
        stage: str = "mapping",
        index: int = 0  # 0 = most recent, 1 = second most recent, etc.
    ) -> Optional[IngestCounts]:
        """Get counts from S3 metadata file.
        
        Args:
            hub: Hub name
            stage: "mapping" (uses _SUMMARY) or "harvest" (uses _MANIFEST)
            index: Which version to get (0=latest, 1=previous, etc.)
        """
        dirs = self.list_s3_directories(hub, stage)
        
        if len(dirs) <= index:
            return None
        
        target_dir = dirs[index]
        
        # Use _SUMMARY for mapping, _MANIFEST for harvest
        if stage == "harvest":
            meta_file = "_MANIFEST"
            parser = self._parse_manifest
        else:
            meta_file = "_SUMMARY"
            parser = self._parse_summary
        
        # Download and parse
        s3_path = f"s3://{self.s3_bucket}/{hub}/{stage}/{target_dir}/{meta_file}"
        cmd = f"aws s3 cp {s3_path} - --profile {self.aws_profile}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            return None
        
        counts = parser(hub, result.stdout, target_dir[:15])
        if counts:
            counts.stage = stage
        return counts
    
    def get_baseline_from_s3(self, hub: str, stage: str = "mapping") -> Optional[IngestCounts]:
        """Get the second most recent _SUMMARY from S3 (baseline for comparison)."""
        return self.get_counts_from_s3(hub, stage, index=1)
    
    def get_latest_from_s3(self, hub: str, stage: str = "mapping") -> Optional[IngestCounts]:
        """Get the most recent _SUMMARY from S3."""
        return self.get_counts_from_s3(hub, stage, index=0)
    
    def get_current_counts(self, hub: str, stage: str = "mapping") -> Optional[IngestCounts]:
        """Get current ingest counts from local metadata file.
        
        Uses _SUMMARY for mapping, _MANIFEST for harvest.
        """
        
        stage_dir = self.data_dir / hub / stage
        if not stage_dir.exists():
            return None
        
        # Find most recent directory
        dirs = [d for d in stage_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
        if not dirs:
            return None
        
        latest = max(dirs, key=lambda d: d.name)
        
        # Use _SUMMARY for mapping, _MANIFEST for harvest
        if stage == "harvest":
            meta_file = latest / "_MANIFEST"
            parser = self._parse_manifest
        else:
            meta_file = latest / "_SUMMARY"
            parser = self._parse_summary
        
        if not meta_file.exists():
            return None
        
        content = meta_file.read_text()
        timestamp = latest.name[:15]  # Extract YYYYMMDD_HHMMSS
        
        counts = parser(hub, content, timestamp)
        if counts:
            counts.stage = stage
        return counts
    
    def _parse_summary(self, hub: str, content: str, timestamp: str) -> Optional[IngestCounts]:
        """Parse a _SUMMARY file content into IngestCounts."""
        
        counts = IngestCounts(hub=hub, timestamp=timestamp, stage="mapping")
        
        # Parse the counts using regex
        attempted_match = re.search(r'Attempted\.+([0-9,]+)', content)
        successful_match = re.search(r'Successful\.+([0-9,]+)', content)
        failed_match = re.search(r'Failed\.+([0-9,]+)', content)
        
        if attempted_match:
            counts.attempted = int(attempted_match.group(1).replace(',', ''))
        if successful_match:
            counts.successful = int(successful_match.group(1).replace(',', ''))
        if failed_match:
            counts.failed = int(failed_match.group(1).replace(',', ''))
        
        return counts
    
    def _parse_manifest(self, hub: str, content: str, timestamp: str) -> Optional[IngestCounts]:
        """Parse a _MANIFEST file content into IngestCounts (for harvest data)."""
        
        counts = IngestCounts(hub=hub, timestamp=timestamp, stage="harvest")
        
        # _MANIFEST format:
        # Activity: Harvest
        # Provider: sd
        # Record count: 95665
        # Start date/time: 2026-02-02 09:13:02
        
        record_match = re.search(r'Record count:\s*([0-9,]+)', content)
        
        if record_match:
            counts.attempted = int(record_match.group(1).replace(',', ''))
            counts.successful = counts.attempted  # Harvest doesn't have failures
            counts.failed = 0
        
        return counts
    
    def check_anomalies(
        self,
        hub: str,
        current_mapping: Optional[IngestCounts] = None,
        baseline_mapping: Optional[IngestCounts] = None,
        current_harvest: Optional[IngestCounts] = None,
        baseline_harvest: Optional[IngestCounts] = None,
    ) -> AnomalyReport:
        """Check for anomalies between current and baseline counts."""
        
        # Get mapping counts if not provided
        if current_mapping is None:
            current_mapping = self.get_current_counts(hub, stage="mapping")
        
        if baseline_mapping is None:
            baseline_mapping = self.get_baseline_from_s3(hub, stage="mapping")
        
        # Get harvest counts (for display, mapping attempted = harvest count)
        if current_harvest is None:
            current_harvest = self.get_current_counts(hub, stage="harvest")
        
        report = AnomalyReport(
            hub=hub,
            current_mapping=current_mapping,
            baseline_mapping=baseline_mapping,
            current_harvest=current_harvest,
            baseline_harvest=baseline_harvest,
        )
        
        # Can't check without current mapping data
        if current_mapping is None:
            return report
        
        # Check 1: Absolute failure rate
        if current_mapping.failure_rate > self.FAILURE_RATE_CRITICAL:
            report.anomalies.append(Anomaly(
                hub=hub,
                anomaly_type=AnomalyType.HIGH_FAILURE_RATE,
                severity='critical',
                message=f"Critical failure rate: {current_mapping.failure_rate:.1%} of records failed mapping",
                current_value=current_mapping.failure_rate,
                baseline_value=baseline_mapping.failure_rate if baseline_mapping else 0,
                threshold=self.FAILURE_RATE_CRITICAL,
                percent_change=current_mapping.failure_rate * 100,
            ))
        elif current_mapping.failure_rate > self.FAILURE_RATE_THRESHOLD:
            report.anomalies.append(Anomaly(
                hub=hub,
                anomaly_type=AnomalyType.HIGH_FAILURE_RATE,
                severity='warning',
                message=f"High failure rate: {current_mapping.failure_rate:.1%} of records failed mapping",
                current_value=current_mapping.failure_rate,
                baseline_value=baseline_mapping.failure_rate if baseline_mapping else 0,
                threshold=self.FAILURE_RATE_THRESHOLD,
                percent_change=current_mapping.failure_rate * 100,
            ))
        
        # Skip baseline comparisons if no baseline
        if baseline_mapping is None or baseline_mapping.successful == 0:
            return report
        
        # Check 2: Output drop (successful mapped records)
        output_change = (current_mapping.successful - baseline_mapping.successful) / baseline_mapping.successful
        if output_change < -self.OUTPUT_DROP_CRITICAL:
            report.anomalies.append(Anomaly(
                hub=hub,
                anomaly_type=AnomalyType.OUTPUT_DROP,
                severity='critical',
                message=f"Mapped records dropped from {baseline_mapping.successful:,} to {current_mapping.successful:,}",
                current_value=current_mapping.successful,
                baseline_value=baseline_mapping.successful,
                threshold=self.OUTPUT_DROP_CRITICAL,
                percent_change=output_change * 100,
            ))
        elif output_change < -self.OUTPUT_DROP_THRESHOLD:
            report.anomalies.append(Anomaly(
                hub=hub,
                anomaly_type=AnomalyType.OUTPUT_DROP,
                severity='warning',
                message=f"Mapped records dropped from {baseline_mapping.successful:,} to {current_mapping.successful:,}",
                current_value=current_mapping.successful,
                baseline_value=baseline_mapping.successful,
                threshold=self.OUTPUT_DROP_THRESHOLD,
                percent_change=output_change * 100,
            ))
        
        # Check 3: Harvest/attempted drop (using mapping attempted as proxy for harvest)
        if baseline_mapping.attempted > 0:
            harvest_change = (current_mapping.attempted - baseline_mapping.attempted) / baseline_mapping.attempted
            if harvest_change < -self.HARVEST_DROP_CRITICAL:
                report.anomalies.append(Anomaly(
                    hub=hub,
                    anomaly_type=AnomalyType.HARVEST_DROP,
                    severity='critical',
                    message=f"Harvest records dropped from {baseline_mapping.attempted:,} to {current_mapping.attempted:,}",
                    current_value=current_mapping.attempted,
                    baseline_value=baseline_mapping.attempted,
                    threshold=self.HARVEST_DROP_CRITICAL,
                    percent_change=harvest_change * 100,
                ))
            elif harvest_change < -self.HARVEST_DROP_THRESHOLD:
                report.anomalies.append(Anomaly(
                    hub=hub,
                    anomaly_type=AnomalyType.HARVEST_DROP,
                    severity='warning',
                    message=f"Harvest records dropped from {baseline_mapping.attempted:,} to {current_mapping.attempted:,}",
                    current_value=current_mapping.attempted,
                    baseline_value=baseline_mapping.attempted,
                    threshold=self.HARVEST_DROP_THRESHOLD,
                    percent_change=harvest_change * 100,
                ))
        
        # Check 4: Failure rate spike (compared to baseline)
        if baseline_mapping.failure_rate > 0:
            failure_rate_change = current_mapping.failure_rate - baseline_mapping.failure_rate
            if failure_rate_change > self.FAILURE_SPIKE_THRESHOLD:
                severity = 'critical' if failure_rate_change > 0.20 else 'warning'
                report.anomalies.append(Anomaly(
                    hub=hub,
                    anomaly_type=AnomalyType.FAILURE_RATE_SPIKE,
                    severity=severity,
                    message=f"Failure rate increased from {baseline_mapping.failure_rate:.1%} to {current_mapping.failure_rate:.1%}",
                    current_value=current_mapping.failure_rate,
                    baseline_value=baseline_mapping.failure_rate,
                    threshold=self.FAILURE_SPIKE_THRESHOLD,
                    percent_change=failure_rate_change * 100,
                ))
        
        return report
    
    def check_hub(
        self, 
        hub: str, 
        verbose: bool = True,
        source: str = "local"  # "local" or "s3"
    ) -> AnomalyReport:
        """Check a hub for anomalies and optionally print report.
        
        Args:
            hub: Hub name to check
            verbose: Print formatted output
            source: Where to get "new" data from:
                    "local" - compare local data vs S3 baseline
                    "s3" - compare most recent S3 vs previous S3
        """
        
        # Get data based on source mode FIRST (before printing)
        if source == "s3":
            # Compare most recent S3 to second most recent S3
            current_mapping = self.get_latest_from_s3(hub, stage="mapping")
            baseline_mapping = self.get_baseline_from_s3(hub, stage="mapping")  # Gets index=1
            current_harvest = self.get_latest_from_s3(hub, stage="harvest")
            baseline_harvest = self.get_baseline_from_s3(hub, stage="harvest")
            new_source = "s3"
            baseline_source = "s3"
        else:
            # Compare local to most recent S3
            current_mapping = self.get_current_counts(hub, stage="mapping")
            baseline_mapping = self.get_latest_from_s3(hub, stage="mapping")
            current_harvest = self.get_current_counts(hub, stage="harvest")
            baseline_harvest = self.get_latest_from_s3(hub, stage="harvest")
            new_source = "local"
            baseline_source = "s3"
        
        # Run anomaly checks BEFORE printing
        report = self.check_anomalies(
            hub,
            current_mapping=current_mapping,
            baseline_mapping=baseline_mapping,
            current_harvest=current_harvest,
            baseline_harvest=baseline_harvest,
        )
        
        if verbose:
            # Print header
            print(f"\n{'=' * 60}")
            print(f"  {hub.upper()}")
            print(f"{'=' * 60}")
            
            # Print anomaly status at TOP
            if report.anomalies:
                if report.should_halt:
                    print(f"\n  ⛔ HARD STOP - Critical anomalies detected")
                    print(f"     Use --force to override")
                else:
                    print(f"\n  ⚠️  WARNING - Anomalies detected")
                
                print(f"\n  🚨 ANOMALIES ({len(report.anomalies)})")
                print(f"  {'-' * 40}")
                for a in report.anomalies:
                    icon = "🔴 CRITICAL" if a.severity == 'critical' else "🟡 WARNING"
                    print(f"    {icon}: {a.anomaly_type.value}")
                    print(f"      {a.message}")
                    print(f"      Change: {a.percent_change:+.1f}% (threshold: ±{a.threshold:.0%})")
            else:
                print(f"\n  ✅ No anomalies")
            
            # Harvest section
            harvest_change = None
            if baseline_harvest and current_harvest and baseline_harvest.attempted > 0:
                harvest_change = ((current_harvest.attempted - baseline_harvest.attempted) 
                                 / baseline_harvest.attempted * 100)
            
            print(f"\n  📥 Harvest")
            if harvest_change is not None:
                arrow = "📈" if harvest_change >= 0 else "📉"
                print(f"  Change: {arrow} {harvest_change:+.1f}%")
            
            # Column-aligned output
            if baseline_harvest:
                print(f"  Previous    {baseline_source:5}    {baseline_harvest.date_formatted:10}    {baseline_harvest.attempted:>12,}")
            else:
                print(f"  Previous    {baseline_source:5}    {'N/A':10}    {'No data':>12}")
            
            if current_harvest:
                latest_tag = " (latest)" if source == "s3" else ""
                print(f"  New         {new_source:5}    {current_harvest.date_formatted:10}    {current_harvest.attempted:>12,}{latest_tag}")
            else:
                print(f"  New         {new_source:5}    {'N/A':10}    {'No data':>12}")
            
            # Mapping section
            mapping_change = None
            if baseline_mapping and current_mapping and baseline_mapping.successful > 0:
                mapping_change = ((current_mapping.successful - baseline_mapping.successful) 
                                 / baseline_mapping.successful * 100)
            
            print(f"\n  🗺️  Mapping")
            if mapping_change is not None:
                arrow = "📈" if mapping_change >= 0 else "📉"
                print(f"  Change: {arrow} {mapping_change:+.1f}%")
            
            # Header
            print(f"  {'':8}    {'Source':6}    {'Date':10}    {'Attempted':>10}    {'Success':>10}    {'Failed':>12}")
            print(f"  {'-' * 72}")
            
            if baseline_mapping:
                fail_str = f"{baseline_mapping.failed:,} ({baseline_mapping.failure_rate:.1%})"
                print(f"  {'Previous':8}    {baseline_source:6}    {baseline_mapping.date_formatted:10}    {baseline_mapping.attempted:>10,}    {baseline_mapping.successful:>10,}    {fail_str:>12}")
            else:
                print(f"  {'Previous':8}    {baseline_source:6}    {'N/A':10}    {'--':>10}    {'--':>10}    {'--':>12}")
            
            if current_mapping:
                fail_str = f"{current_mapping.failed:,} ({current_mapping.failure_rate:.1%})"
                latest_tag = " (latest)" if source == "s3" else ""
                print(f"  {'New':8}    {new_source:6}    {current_mapping.date_formatted:10}    {current_mapping.attempted:>10,}    {current_mapping.successful:>10,}    {fail_str:>12}{latest_tag}")
            else:
                print(f"  {'New':8}    {new_source:6}    {'N/A':10}    {'--':>10}    {'--':>10}    {'--':>12}")
        
        return report


def check_before_sync(hub: str, force: bool = False) -> bool:
    """
    Check for anomalies before S3 sync. Returns True if safe to proceed.
    
    Usage in orchestrator:
        if not check_before_sync(hub):
            return  # Halt the ingest
    """
    detector = AnomalyDetector()
    report = detector.check_hub(hub, verbose=True)
    
    if report.should_halt and not force:
        print(f"\n⛔ HALTING: {hub} ingest stopped due to critical anomalies")
        print(f"   Use --force-sync {hub} to override")
        return False
    
    if report.anomalies and not report.should_halt:
        print(f"\n⚠️  WARNING: {hub} has anomalies but proceeding (not critical)")
    
    return True


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Check for ingest anomalies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 anomaly_detector.py wisconsin           # Compare local to S3
  python3 anomaly_detector.py wisconsin --s3      # Compare S3 latest to previous
  python3 anomaly_detector.py --all-recent        # Check all hubs with local data
  python3 anomaly_detector.py bpl sd --json       # JSON output
        """
    )
    parser.add_argument('hubs', nargs='*', help='Hubs to check')
    parser.add_argument('--all-recent', action='store_true', help='Check all hubs with recent local data')
    parser.add_argument('--s3', action='store_true', help='Compare S3 latest vs S3 previous (instead of local vs S3)')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--force', action='store_true', help='Show what would happen with --force')
    
    args = parser.parse_args()
    
    detector = AnomalyDetector()
    source = "s3" if args.s3 else "local"
    
    if args.all_recent:
        # Find hubs with recent mapping data
        data_dir = Path("/Users/scott/dpla/data")
        hubs = []
        for d in data_dir.iterdir():
            if d.is_dir() and (d / "mapping").exists():
                hubs.append(d.name)
        args.hubs = sorted(hubs)
    
    results = []
    for hub in args.hubs:
        report = detector.check_hub(hub, verbose=not args.json, source=source)
        results.append(report.to_dict())
    
    if args.json:
        print(json.dumps(results, indent=2))
