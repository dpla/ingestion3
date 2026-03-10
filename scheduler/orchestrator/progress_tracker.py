"""Progress tracking for ingest operations.

Provides real-time status monitoring including:
- Running process detection and stage identification
- Record counts from harvest (avro) and remap (jsonl) outputs
- Elapsed time and estimated progress
- S3 sync status
"""

import subprocess
import json
import re
import os
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Literal
from enum import Enum


class Stage(str, Enum):
    """Ingest pipeline stages."""
    PENDING = "pending"
    DOWNLOADING = "downloading"  # S3 download for file-based hubs
    HARVESTING = "harvesting"
    MAPPING = "mapping"
    ENRICHING = "enriching"
    JSONL = "jsonl"
    REMAPPING = "remapping"  # Combined mapping+enrichment+jsonl
    SYNCING = "syncing"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class RecordCounts:
    """Record counts at different stages."""
    harvest_records: Optional[int] = None
    mapped_records: Optional[int] = None
    enriched_records: Optional[int] = None
    jsonl_records: Optional[int] = None
    failed_records: Optional[int] = None
    
    def to_dict(self) -> dict:
        return {k: v for k, v in {
            'harvest': self.harvest_records,
            'mapped': self.mapped_records,
            'enriched': self.enriched_records,
            'jsonl': self.jsonl_records,
            'failed': self.failed_records
        }.items() if v is not None}


@dataclass
class HubProgress:
    """Progress information for a single hub."""
    hub_name: str
    stage: Stage
    is_running: bool = False
    pid: Optional[int] = None
    started_at: Optional[str] = None
    elapsed_seconds: Optional[int] = None
    records: RecordCounts = field(default_factory=RecordCounts)
    progress_pct: Optional[float] = None  # For stages with known totals
    error: Optional[str] = None
    synced_to_s3: bool = False
    notes: Optional[str] = None
    
    def to_dict(self) -> dict:
        result = {
            'hub': self.hub_name,
            'stage': self.stage.value,
            'running': self.is_running,
        }
        if self.pid:
            result['pid'] = self.pid
        if self.started_at:
            result['started_at'] = self.started_at
        if self.elapsed_seconds:
            result['elapsed'] = f"{self.elapsed_seconds // 60}m {self.elapsed_seconds % 60}s"
        if self.records.harvest_records is not None:
            result['records'] = self.records.to_dict()
        if self.progress_pct is not None:
            result['progress'] = f"{self.progress_pct:.1f}%"
        if self.error:
            result['error'] = self.error
        if self.synced_to_s3:
            result['synced_to_s3'] = True
        if self.notes:
            result['notes'] = self.notes
        return result
    
    def summary(self) -> str:
        """One-line summary for display."""
        parts = [f"{self.hub_name}: {self.stage.value}"]
        if self.is_running:
            parts.append("⏳")
            if self.elapsed_seconds:
                parts.append(f"({self.elapsed_seconds // 60}m)")
        if self.records.harvest_records:
            parts.append(f"[{self.records.harvest_records:,} records]")
        if self.progress_pct is not None:
            parts.append(f"({self.progress_pct:.0f}%)")
        if self.synced_to_s3:
            parts.append("✅")
        if self.error:
            parts.append(f"❌ {self.error[:50]}")
        return " ".join(parts)


class ProgressTracker:
    """Tracks progress of ingest operations."""
    
    def __init__(self, data_dir: str = "/Users/scott/dpla/data"):
        self.data_dir = Path(data_dir)
        self.status_dir = self.data_dir / ".status"
        self.status_dir.mkdir(parents=True, exist_ok=True)
    
    def get_running_processes(self) -> dict[str, dict]:
        """Get all running ingest processes with their details."""
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True
        )
        
        processes = {}
        for line in result.stdout.split('\n'):
            # Match HarvestEntry or IngestRemap processes
            if 'HarvestEntry' in line or 'IngestRemap' in line:
                match = re.search(r'name=([a-z0-9-]+)', line)
                if match:
                    hub_name = match.group(1)
                    parts = line.split()
                    pid = int(parts[1])
                    
                    # Determine stage
                    if 'HarvestEntry' in line:
                        stage = Stage.HARVESTING
                    else:
                        stage = Stage.REMAPPING
                    
                    # Get process start time (from ps output)
                    # Column varies, but we can estimate elapsed time
                    processes[hub_name] = {
                        'pid': pid,
                        'stage': stage,
                        'command': 'harvest' if 'HarvestEntry' in line else 'remap'
                    }
            
            # Match aws s3 sync processes
            elif 'aws s3 sync' in line or 's3-sync.sh' in line:
                match = re.search(r'/([a-z0-9-]+)/(jsonl|harvest)', line)
                if match:
                    hub_name = match.group(1)
                    parts = line.split()
                    pid = int(parts[1])
                    processes[hub_name] = {
                        'pid': pid,
                        'stage': Stage.SYNCING,
                        'command': 's3-sync'
                    }
        
        return processes
    
    def count_avro_records(self, hub_name: str) -> Optional[int]:
        """Count records in harvest avro directory."""
        harvest_dir = self.data_dir / hub_name / "harvest"
        if not harvest_dir.exists():
            return None
        
        # Count .avro files and estimate records
        # Each avro partition typically contains ~10k-50k records
        avro_files = list(harvest_dir.glob("**/*.avro"))
        if not avro_files:
            return None
        
        # Use file size as rough estimator (average ~500 bytes per record)
        total_size = sum(f.stat().st_size for f in avro_files)
        estimated_records = total_size // 500
        
        return estimated_records
    
    def count_jsonl_records(self, hub_name: str) -> Optional[int]:
        """Count records in JSONL output directory."""
        jsonl_dir = self.data_dir / hub_name / "jsonl"
        if not jsonl_dir.exists():
            return None
        
        # Find the MAPRecord.jsonl files (these are the mapped records)
        map_files = list(jsonl_dir.glob("*-MAPRecord.jsonl"))
        if not map_files:
            # Try looking in subdirectories or with different patterns
            map_files = list(jsonl_dir.glob("**/*.jsonl"))
        
        if not map_files:
            return None
        
        # Count lines in JSONL files (gzipped or plain)
        total_records = 0
        for f in map_files:
            if 'MAPRecord' in f.name:
                try:
                    if f.suffix == '.gz' or '.gz' in f.name:
                        result = subprocess.run(
                            f"zcat '{f}' | wc -l",
                            capture_output=True, text=True, shell=True
                        )
                    else:
                        result = subprocess.run(
                            f"wc -l < '{f}'",
                            capture_output=True, text=True, shell=True
                        )
                    if result.returncode == 0:
                        total_records += int(result.stdout.strip())
                except (ValueError, subprocess.SubprocessError):
                    pass
        
        return total_records if total_records > 0 else None
    
    def count_failed_records(self, hub_name: str) -> Optional[int]:
        """Count failed records from remap."""
        jsonl_dir = self.data_dir / hub_name / "jsonl"
        if not jsonl_dir.exists():
            return None
        
        # Find failure files
        fail_files = list(jsonl_dir.glob("*-MAPFailure.jsonl"))
        if not fail_files:
            return None
        
        total_failed = 0
        for f in fail_files:
            try:
                if f.suffix == '.gz' or '.gz' in f.name:
                    result = subprocess.run(
                        f"zcat '{f}' | wc -l",
                        capture_output=True, text=True, shell=True
                    )
                else:
                    result = subprocess.run(
                        f"wc -l < '{f}'",
                        capture_output=True, text=True, shell=True
                    )
                if result.returncode == 0:
                    total_failed += int(result.stdout.strip())
            except (ValueError, subprocess.SubprocessError):
                pass
        
        return total_failed if total_failed > 0 else None
    
    def get_hub_progress(self, hub_name: str) -> HubProgress:
        """Get detailed progress for a single hub."""
        running = self.get_running_processes()
        
        # Check if currently running
        if hub_name in running:
            proc_info = running[hub_name]
            progress = HubProgress(
                hub_name=hub_name,
                stage=proc_info['stage'],
                is_running=True,
                pid=proc_info['pid']
            )
        else:
            # Determine stage from existing data
            progress = HubProgress(hub_name=hub_name, stage=Stage.PENDING)
        
        # Count records from outputs
        harvest_records = self.count_avro_records(hub_name)
        jsonl_records = self.count_jsonl_records(hub_name)
        failed_records = self.count_failed_records(hub_name)
        
        progress.records = RecordCounts(
            harvest_records=harvest_records,
            jsonl_records=jsonl_records,
            failed_records=failed_records
        )
        
        # Determine stage if not running
        if not progress.is_running:
            if jsonl_records and jsonl_records > 0:
                progress.stage = Stage.COMPLETE
            elif harvest_records and harvest_records > 0:
                progress.stage = Stage.HARVESTING  # Harvest done, ready for remap
                progress.notes = "Harvest complete, ready for remap"
        
        return progress
    
    def get_all_progress(self, hubs: list[str] = None) -> list[HubProgress]:
        """Get progress for all specified hubs or detect from running processes."""
        running = self.get_running_processes()
        
        if hubs is None:
            # Auto-detect from running processes and data directories
            hubs = set(running.keys())
            # Add hubs with data directories
            for d in self.data_dir.iterdir():
                if d.is_dir() and not d.name.startswith('.'):
                    hubs.add(d.name)
            hubs = sorted(hubs)
        
        return [self.get_hub_progress(hub) for hub in hubs]
    
    def get_status_summary(self, month: str = None, year: int = None) -> dict:
        """Get comprehensive status summary."""
        running = self.get_running_processes()
        
        # Load status file if exists
        if month and year:
            status_file = self.status_dir / f"{month.lower()}-{year}.json"
        else:
            # Find most recent status file
            status_files = list(self.status_dir.glob("*.json"))
            status_file = max(status_files, key=lambda f: f.stat().st_mtime) if status_files else None
        
        existing_status = {}
        if status_file and status_file.exists():
            try:
                existing_status = json.loads(status_file.read_text())
            except json.JSONDecodeError:
                pass
        
        # Build current status
        current_status = {
            'timestamp': datetime.now().isoformat(),
            'running_processes': {
                hub: {
                    'pid': info['pid'],
                    'stage': info['stage'].value,
                }
                for hub, info in running.items()
            },
            'hub_status': {}
        }
        
        # Get progress for all known hubs
        all_hubs = set(running.keys())
        if 'hubs' in existing_status:
            all_hubs.update(existing_status['hubs'].keys())
        
        for hub in sorted(all_hubs):
            progress = self.get_hub_progress(hub)
            current_status['hub_status'][hub] = progress.to_dict()
        
        return current_status
    
    def update_status_file(self, month: str, year: int, hub_updates: dict = None):
        """Update the persistent status file for a month."""
        status_file = self.status_dir / f"{month.lower()}-{year}.json"
        
        # Load existing
        status = {}
        if status_file.exists():
            try:
                status = json.loads(status_file.read_text())
            except json.JSONDecodeError:
                pass
        
        # Initialize structure if needed
        if 'hubs' not in status:
            status['hubs'] = {}
        
        status['updated_at'] = datetime.now().isoformat()
        status['month'] = month
        status['year'] = year
        
        # Apply updates
        if hub_updates:
            for hub, updates in hub_updates.items():
                if hub not in status['hubs']:
                    status['hubs'][hub] = {}
                status['hubs'][hub].update(updates)
        
        # Calculate summary
        hubs = status['hubs']
        status['summary'] = {
            'total_hubs': len(hubs),
            'complete': sum(1 for h in hubs.values() if h.get('status') == 'complete'),
            'failed': sum(1 for h in hubs.values() if h.get('status') == 'failed'),
            'in_progress': sum(1 for h in hubs.values() if h.get('status') == 'in_progress'),
            'pending': sum(1 for h in hubs.values() if h.get('status') == 'pending'),
            'total_records_synced': sum(
                h.get('records', 0) or 0 
                for h in hubs.values() 
                if h.get('synced_to_s3')
            )
        }
        
        # Save
        status_file.write_text(json.dumps(status, indent=2))
        return status
    
    def print_status_report(self, hubs: list[str] = None):
        """Print a formatted status report to stdout."""
        running = self.get_running_processes()
        
        print("\n" + "=" * 70)
        print(f"INGEST STATUS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Running processes
        if running:
            print(f"\n🔄 RUNNING PROCESSES ({len(running)}):")
            print("-" * 50)
            for hub, info in sorted(running.items()):
                print(f"  {hub:20} | {info['stage'].value:12} | PID: {info['pid']}")
        else:
            print("\n⏸️  No ingest processes currently running")
        
        # Hub status details
        print(f"\n📊 HUB STATUS:")
        print("-" * 50)
        
        progress_list = self.get_all_progress(hubs)
        
        # Group by stage
        by_stage = {}
        for p in progress_list:
            stage = p.stage.value
            if stage not in by_stage:
                by_stage[stage] = []
            by_stage[stage].append(p)
        
        stage_order = [
            'harvesting', 'mapping', 'enriching', 'jsonl',
            'remapping', 'syncing', 'complete', 'failed', 'pending',
        ]
        
        for stage in stage_order:
            if stage in by_stage:
                stage_hubs = by_stage[stage]
                emoji = {
                    'harvesting': '🌾', 'mapping': '🗺️', 'enriching': '✨',
                    'jsonl': '📦', 'remapping': '🔄', 'syncing': '☁️',
                    'complete': '✅', 'failed': '❌', 'pending': '⏳',
                }.get(stage, '•')
                
                print(f"\n{emoji} {stage.upper()} ({len(stage_hubs)}):")
                for p in sorted(stage_hubs, key=lambda x: x.hub_name):
                    records_str = ""
                    if p.records.jsonl_records:
                        records_str = f" | {p.records.jsonl_records:,} records"
                    elif p.records.harvest_records:
                        records_str = f" | ~{p.records.harvest_records:,} harvested"
                    
                    status_str = ""
                    if p.is_running:
                        status_str = " [RUNNING]"
                    elif p.synced_to_s3:
                        status_str = " [S3 ✓]"
                    
                    print(f"    {p.hub_name:20}{records_str}{status_str}")
        
        print("\n" + "=" * 70)


def get_status(hubs: list[str] = None, verbose: bool = True) -> dict:
    """Convenience function to get and optionally print status."""
    tracker = ProgressTracker()
    
    if verbose:
        tracker.print_status_report(hubs)
    
    return tracker.get_status_summary()


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Track ingest progress")
    parser.add_argument('--hubs', nargs='+', help='Specific hubs to check')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--month', help='Month name (e.g., january)')
    parser.add_argument('--year', type=int, help='Year')
    
    args = parser.parse_args()
    
    tracker = ProgressTracker()
    
    if args.json:
        status = tracker.get_status_summary(args.month, args.year)
        print(json.dumps(status, indent=2))
    else:
        tracker.print_status_report(args.hubs)
