"""Notifications and escalation for ingest failures."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional
import urllib.request
import urllib.error

from .s3_utils import build_s3_paths, count_avro_records, count_jsonl_records, get_latest_dir


# Maximum Slack message size (leave buffer under 40KB limit)
MAX_SLACK_TEXT_LENGTH = 35000


class Notifier:
    """Handles notifications and failure escalation."""

    def __init__(self, config):
        self.config = config
        self.escalation_dir = config.data_dir / "escalations"
        self.escalation_dir.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # Shared formatting helpers
    # =========================================================================

    @staticmethod
    def _format_counts_block(
        harvest: int | None = None,
        successful: int | None = None,
        failed: int | None = None,
        *,
        inline: bool = False,
    ) -> str:
        """Format a Harvested / Mapped (Successful + Failed) block.

        Args:
            harvest: Harvest record count (omitted when None).
            successful: Mapping successful count (omitted when None).
            failed: Mapping failed count (omitted when None).
            inline: If True, return a compact single-line version
                    suitable for list items (e.g. run-summary hub lines).

        Returns:
            Formatted Slack mrkdwn string.
        """
        if inline:
            parts = []
            if harvest is not None:
                parts.append(f"Harvested: `{harvest:,}`")
            if successful is not None:
                parts.append(f"Successful: `{successful:,}`")
            if failed is not None:
                parts.append(f"Failed: `{failed:,}`")
            return " · ".join(parts) if parts else "counts unavailable"

        lines: list[str] = []
        if harvest is not None:
            lines.append(f"Harvested: `{harvest:,}`")
        lines.append("Mapped:")
        if successful is not None:
            lines.append(f"  - Successful: `{successful:,}`")
        if failed is not None:
            lines.append(f"  - Failed: `{failed:,}`")
        return "\n".join(lines)

    def send_start_notification(
        self,
        run_id: str,
        hubs: list[str],
        test_prefix: Optional[str] = None
    ):
        """Send notification that a run has started."""
        prefix = f"{test_prefix} " if test_prefix else ""

        print(f"\n{'='*60}")
        print(f"  {prefix}DPLA Ingest Run Started: {run_id}")
        print(f"  Time: {datetime.now().isoformat()}")
        print(f"  Hubs: {len(hubs)}")
        print(f"{'='*60}\n")

        if self.config.slack_webhook:
            self._send_slack({
                "text": f"{prefix}:rocket: DPLA Ingest Started\nRun: `{run_id}`\nHubs: {', '.join(hubs)}"
            })

    def send_completion_notification(
        self,
        run_id: str,
        summary: dict,
        test_prefix: Optional[str] = None,
        write_drafts: bool = True
    ):
        """Send notification that a run has completed with enriched per-hub details."""
        prefix = f"{test_prefix} " if test_prefix else ""
        totals = summary.get('totals', {})
        complete = totals.get('complete', 0)
        failed = totals.get('failed', 0)
        total = totals.get('total', 0)

        status_emoji = ":white_check_mark:" if failed == 0 else ":warning:"

        # Console output
        print(f"\n{'='*60}")
        print(f"  {prefix}DPLA Ingest Run Complete: {run_id}")
        print(f"  Results: {complete}/{total} successful, {failed} failed")
        print(f"{'='*60}\n")

        # Enrich summary with per-hub counts and S3 paths
        enriched_hubs = self._enrich_hub_summary(summary.get('hubs', {}))

        # Write email drafts for completed hubs
        draft_dir = None
        hubs_without_email = []
        if write_drafts:
            draft_dir, hubs_without_email = self._write_email_drafts(
                run_id, enriched_hubs, test_prefix
            )

        # Build Slack message
        if self.config.slack_webhook:
            slack_text = self._build_completion_slack_message(
                run_id, complete, failed, total, enriched_hubs,
                draft_dir, hubs_without_email, prefix, status_emoji
            )
            self._send_slack({"text": slack_text})

    # =========================================================================
    # Per-stage notifications
    # =========================================================================

    def send_harvest_complete(
        self,
        run_id: str,
        hub: str,
        record_count: int | None = None,
        duration_seconds: int | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send notification that harvest completed for a hub."""
        prefix = f"{test_prefix} " if test_prefix else ""
        count_str = f"{record_count:,} records" if record_count else "count unavailable"
        dur_str = f" ({duration_seconds}s)" if duration_seconds else ""

        print(f"  [{hub}] Harvest complete: {count_str}{dur_str}")

        if self.config.slack_webhook:
            self._send_slack({
                "text": (
                    f"{prefix}:seedling: `{hub}` harvest complete\n"
                    f"Records: {count_str}{dur_str}"
                )
            })

    def send_mapping_complete(
        self,
        run_id: str,
        hub: str,
        attempted: int | None = None,
        successful: int | None = None,
        failed: int | None = None,
        duration_seconds: int | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send notification that mapping completed for a hub."""
        prefix = f"{test_prefix} " if test_prefix else ""
        dur_str = f" ({duration_seconds}s)" if duration_seconds else ""

        parts = []
        if attempted is not None:
            parts.append(f"{attempted:,} attempted")
        if successful is not None:
            parts.append(f"{successful:,} successful")
        if failed is not None and failed > 0:
            parts.append(f"{failed:,} failed")
        count_str = ", ".join(parts) if parts else "counts unavailable"

        print(f"  [{hub}] Mapping complete: {count_str}{dur_str}")

        if self.config.slack_webhook:
            self._send_slack({
                "text": (
                    f"{prefix}:world_map: `{hub}` mapping complete\n"
                    f"{count_str}{dur_str}"
                )
            })

    def send_enrichment_complete(
        self,
        run_id: str,
        hub: str,
        duration_seconds: int | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send notification that enrichment completed for a hub."""
        prefix = f"{test_prefix} " if test_prefix else ""
        dur_str = f" ({duration_seconds}s)" if duration_seconds else ""

        print(f"  [{hub}] Enrichment complete{dur_str}")

        if self.config.slack_webhook:
            self._send_slack({
                "text": f"{prefix}:sparkles: `{hub}` enrichment complete{dur_str}"
            })

    def send_jsonl_complete(
        self,
        run_id: str,
        hub: str,
        duration_seconds: int | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send notification that JSONL export completed for a hub."""
        prefix = f"{test_prefix} " if test_prefix else ""
        dur_str = f" ({duration_seconds}s)" if duration_seconds else ""

        print(f"  [{hub}] JSONL export complete{dur_str}")

        if self.config.slack_webhook:
            self._send_slack({
                "text": f"{prefix}:package: `{hub}` JSONL export complete{dur_str}"
            })

    def send_sync_complete(
        self,
        run_id: str,
        hub: str,
        duration_seconds: int | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send notification that S3 sync completed for a hub."""
        prefix = f"{test_prefix} " if test_prefix else ""
        dur_str = f" ({duration_seconds}s)" if duration_seconds else ""

        print(f"  [{hub}] Data synced to S3{dur_str}")

        if self.config.slack_webhook:
            self._send_slack({
                "text": f"{prefix}:cloud: `{hub}` data synced to S3{dur_str}"
            })

    # =========================================================================
    # Hub-complete success notification (→ #tech with @here)
    # =========================================================================

    def send_hub_complete_success(
        self,
        hub: str,
        harvest_count: int | None = None,
        mapping_attempted: int | None = None,
        mapping_successful: int | None = None,
        mapping_failed: int | None = None,
        current_run_date: str | None = None,
        test_prefix: Optional[str] = None,
    ):
        """Send a consolidated success notification for a completed hub.

        Posts to #tech (SLACK_TECH_WEBHOOK) with @here.  Includes current run
        summary with deltas from previous run, then previous run counts.

        Falls back to SLACK_WEBHOOK if SLACK_TECH_WEBHOOK is not set.
        """
        prefix = f"{test_prefix} " if test_prefix else ""
        date_str = current_run_date or datetime.now().strftime("%m-%d-%Y")

        # Fetch baseline from S3
        baseline = self._fetch_baseline(hub)

        # --- current run summary with deltas ---
        lines = [
            f"{prefix}<!here> :white_check_mark: *`{hub}` re-ingested* (*{date_str}*)",
            "",
        ]
        lines.extend(self._format_counts_with_deltas(
            harvest_count, mapping_successful, mapping_failed, baseline
        ))

        # --- previous run (plain counts) ---
        if baseline:
            prev_harvest = baseline.get("harvest_attempted")
            lines.append("")
            lines.append(f"vs previous run on *{baseline['date']}*")
            lines.extend(
                self._format_counts_block(
                    prev_harvest, baseline.get("successful"), baseline.get("failed"),
                ).split("\n")
            )

        text = "\n".join(lines)

        # Console output
        print(f"\n  [{hub}] Hub complete notification:")
        for line in lines:
            print(f"    {line}")

        self._send_slack_tech({"text": text})

    @staticmethod
    def _format_delta(current: int | None, baseline: int | None) -> str:
        """Format a delta suffix like ' (:arrow_up: `156`)'.

        Returns empty string if either value is None or delta is zero.
        """
        if current is None or baseline is None:
            return ""
        d = current - baseline
        if d == 0:
            return ""
        arrow = ":arrow_up:" if d > 0 else ":arrow_down:"
        return f"  ({arrow} `{abs(d):,}`)"

    def _format_counts_with_deltas(
        self,
        harvest: int | None,
        successful: int | None,
        failed: int | None,
        baseline: dict | None,
    ) -> list[str]:
        """Format Harvested / Mapped block with delta annotations from baseline."""
        lines: list[str] = []
        b_harvest = baseline.get("harvest_attempted") if baseline else None
        b_successful = baseline.get("successful") if baseline else None
        b_failed = baseline.get("failed") if baseline else None

        if harvest is not None:
            lines.append(f"Harvested: `{harvest:,}`{self._format_delta(harvest, b_harvest)}")
        lines.append("Mapped:")
        if successful is not None:
            lines.append(f"  - Successful: `{successful:,}`{self._format_delta(successful, b_successful)}")
        if failed is not None:
            lines.append(f"  - Failed: `{failed:,}`{self._format_delta(failed, b_failed)}")
        return lines

    def _fetch_baseline(self, hub: str) -> dict | None:
        """Fetch previous S3 run counts for a hub.

        Returns dict with date, successful, failed, harvest_attempted
        or None if no baseline exists.
        """
        try:
            from .anomaly_detector import AnomalyDetector

            s3_prefix = self.config.resolve_s3_prefix(hub)

            detector = AnomalyDetector(
                s3_bucket=self.config.get_s3_dest_bucket(),
                aws_profile=self.config.aws_profile,
                data_dir=str(self.config.data_dir),
            )

            # After sync, the current run is index=0 in S3 so we need
            # index=1 (the previous run) for comparison.
            baseline_mapping = detector.get_baseline_from_s3(s3_prefix, stage="mapping")
            if baseline_mapping is None:
                return None

            # Also fetch baseline harvest count
            baseline_harvest = detector.get_baseline_from_s3(s3_prefix, stage="harvest")

            return {
                "date": baseline_mapping.date_formatted,
                "successful": baseline_mapping.successful,
                "failed": baseline_mapping.failed,
                "harvest_attempted": baseline_harvest.attempted if baseline_harvest else None,
            }

        except Exception as e:
            print(f"  Warning: Could not fetch baseline for {hub}: {e}")
            return None

    def _send_slack_tech(self, payload: dict):
        """Send a Slack message to #tech (SLACK_TECH_WEBHOOK).

        Falls back to SLACK_WEBHOOK if SLACK_TECH_WEBHOOK is not configured.
        """
        webhook = self.config.slack_tech_webhook or self.config.slack_webhook
        if not webhook:
            return

        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                webhook,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            urllib.request.urlopen(req, timeout=10)
        except urllib.error.URLError as e:
            print(f"Warning: Could not send Slack #tech notification: {e}")

    # =========================================================================
    # Anomaly alerts
    # =========================================================================

    def send_anomaly_alert(
        self,
        hub: str,
        report,  # AnomalyReport from anomaly_detector
        test_prefix: Optional[str] = None
    ):
        """Send Slack alert for anomaly detection results.

        Only sends if anomalies were detected (warnings or critical).
        """
        if not report.anomalies:
            return  # No anomalies, no notification

        prefix = f"{test_prefix} " if test_prefix else ""

        if report.has_critical:
            emoji = ":x:"
            level = "ERROR"
            action = "Sync HALTED"
        else:
            emoji = ":warning:"
            level = "WARNING"
            action = "Sync proceeded"

        # Build short anomaly summary -- human-readable text, numbers in ticks,
        # parenthetical delta percentages like (-4.8%) get bold arrow treatment
        anomaly_lines = []
        for a in report.anomalies[:3]:  # Limit to first 3
            msg = a.message[:100]
            # Replace parenthetical delta percentages first: (-4.8%) → (:arrow_down: *-4.8%*)
            msg = re.sub(r'\((-?\d+\.?\d*)%\)', lambda m: (
                f"(:arrow_down: *{m.group(1)}%*)" if m.group(1).startswith('-')
                else f"(:arrow_up: *{m.group(1)}%*)"
            ), msg)
            # Wrap remaining inline numbers in ticks (skip numbers in bold *...%*)
            msg = re.sub(r'(?<![.\d*])(\d[\d,]+)(?![\d.]*[%*])', r'`\1`', msg)
            anomaly_lines.append(f"  - {msg}")

        if len(report.anomalies) > 3:
            anomaly_lines.append(f"  ... and {len(report.anomalies) - 3} more")

        message = (
            f"{prefix}{emoji} *`{hub}` — {action}* ({level})\n"
            + "\n".join(anomaly_lines)
        )

        # Add counts if available
        if report.current_mapping:
            cur = report.current_mapping
            block = self._format_counts_block(cur.attempted, cur.successful, cur.failed)
            message += f"\n\n*{cur.date_formatted}*\n{block}"
        if report.baseline_mapping:
            base = report.baseline_mapping
            block = self._format_counts_block(base.attempted, base.successful, base.failed)
            message += f"\n\n*{base.date_formatted}* (baseline)\n{block}"

        # Console output
        print(f"\n{'-'*60}")
        print(f"  {prefix}ANOMALY ALERT: {hub}")
        print(f"  Level: {level}")
        for a in report.anomalies:
            print(f"    [{a.severity}] {a.message}")
        print(f"{'-'*60}\n")

        if self.config.slack_webhook:
            self._send_slack({"text": message})

    def _enrich_hub_summary(self, hubs: dict) -> dict:
        """Enrich hub summary with counts from local files and S3 paths."""
        enriched = {}

        for hub_name, hub_info in hubs.items():
            enriched[hub_name] = dict(hub_info)  # Copy original info
            status = hub_info.get('status', '')

            if status == 'complete':
                # Get counts from local files
                counts = self._get_hub_counts(hub_name)
                enriched[hub_name].update(counts)

                # Get S3 paths
                s3_paths = build_s3_paths(
                    hub_name,
                    self.config.data_dir,
                    self.config.get_s3_dest_bucket(),
                    self.config.resolve_s3_prefix(hub_name),
                    aws_profile=self.config.aws_profile
                )
                enriched[hub_name]['s3_paths'] = s3_paths

            # For failed hubs, try to infer failure stage
            if status == 'failed':
                failure_stage = self._infer_failure_stage(hub_name, hub_info)
                enriched[hub_name]['failure_stage'] = failure_stage

        return enriched

    def _get_hub_counts(self, hub: str) -> dict:
        """Get harvest and mapping counts from local files."""
        counts = {
            'harvest_count': None,
            'mapping_attempted': None,
            'mapping_successful': None,
            'mapping_failed': None,
            'issues_summary': None,
            'summary_available': False,
        }

        data_dir = self.config.data_dir

        # Try to get harvest count from _MANIFEST
        harvest_dir = data_dir / hub / "harvest"
        latest_harvest = get_latest_dir(harvest_dir)
        if latest_harvest:
            manifest = latest_harvest / "_MANIFEST"
            if manifest.exists():
                counts['harvest_count'] = self._parse_manifest_count(manifest)

        # Try to get mapping counts from _SUMMARY
        mapping_dir = data_dir / hub / "mapping"
        latest_mapping = get_latest_dir(mapping_dir)
        if latest_mapping:
            summary_file = latest_mapping / "_SUMMARY"
            if summary_file.exists():
                mapping_counts = self._parse_summary_counts(summary_file)
                counts.update(mapping_counts)
                counts['summary_available'] = True
            else:
                # Fallback: try to count records from output files
                counts['mapping_successful'] = self._estimate_record_count(hub)

        return counts

    def _parse_manifest_count(self, manifest_path: Path) -> Optional[int]:
        """Parse record count from _MANIFEST file."""
        try:
            content = manifest_path.read_text()
            match = re.search(r'Record count:\s*([0-9,]+)', content)
            if match:
                return int(match.group(1).replace(',', ''))
        except Exception:
            pass
        return None

    def _parse_summary_counts(self, summary_path: Path) -> dict:
        """Parse counts and issues from _SUMMARY file."""
        counts = {
            'mapping_attempted': None,
            'mapping_successful': None,
            'mapping_failed': None,
            'issues_summary': None,
        }

        try:
            content = summary_path.read_text()

            # Parse counts
            attempted_match = re.search(r'Attempted\.+([0-9,]+)', content)
            successful_match = re.search(r'Successful\.+([0-9,]+)', content)
            failed_match = re.search(r'Failed\.+([0-9,]+)', content)

            if attempted_match:
                counts['mapping_attempted'] = int(attempted_match.group(1).replace(',', ''))
            if successful_match:
                counts['mapping_successful'] = int(successful_match.group(1).replace(',', ''))
            if failed_match:
                counts['mapping_failed'] = int(failed_match.group(1).replace(',', ''))

            # Extract issues summary (errors and warnings)
            # Format is "- Errors...8,584" under "Records" section
            # Look for the Records section errors specifically
            records_section = re.search(r'Records\n-\s*Errors\.+([0-9,]+)\n-\s*Warnings\.+([0-9,]+)', content)
            if records_section:
                errors_match = records_section
                warnings_match = None  # Already captured in groups
                # Override counts with Records section values
                counts['issues_summary'] = None
                errors_val = int(records_section.group(1).replace(',', ''))
                warnings_val = int(records_section.group(2).replace(',', ''))
                issues_parts = []
                if errors_val > 0:
                    issues_parts.append(f"{errors_val:,} errors")
                if warnings_val > 0:
                    issues_parts.append(f"{warnings_val:,} warnings")
                if issues_parts:
                    counts['issues_summary'] = ", ".join(issues_parts)
                return counts

            # Fallback: try simpler pattern matching
            errors_match = re.search(r'-\s*Errors\.+([0-9,]+)', content)
            warnings_match = re.search(r'-\s*Warnings\.+([0-9,]+)', content)

            issues_parts = []
            if errors_match and int(errors_match.group(1).replace(',', '')) > 0:
                issues_parts.append(f"{errors_match.group(1)} errors")
            if warnings_match and int(warnings_match.group(1).replace(',', '')) > 0:
                issues_parts.append(f"{warnings_match.group(1)} warnings")

            if issues_parts:
                counts['issues_summary'] = ", ".join(issues_parts)

        except Exception:
            pass

        return counts

    def _estimate_record_count(self, hub: str) -> Optional[int]:
        """Estimate record count from output files when _SUMMARY is missing."""
        data_dir = self.config.data_dir

        # Try JSONL first (most accurate)
        jsonl_dir = data_dir / hub / "jsonl"
        latest_jsonl = get_latest_dir(jsonl_dir)
        if latest_jsonl:
            count = count_jsonl_records(latest_jsonl)
            if count:
                return count

        # Try mapping avro files
        mapping_dir = data_dir / hub / "mapping"
        latest_mapping = get_latest_dir(mapping_dir)
        if latest_mapping:
            count = count_avro_records(latest_mapping)
            if count:
                return count

        return None

    def _infer_failure_stage(self, hub: str, hub_info: dict) -> str:
        """Infer which stage the hub failed at."""
        error = hub_info.get('error', '').lower()
        error_type = hub_info.get('error_type', '').lower()

        if 'anomaly' in error or 'anomaly' in error_type:
            return 'anomaly'
        if 'harvest' in error or 'harvest' in error_type:
            return 'harvest'
        if 'sync' in error or 's3' in error_type:
            return 'sync'
        if 'remap' in error or 'mapping' in error or 'enrich' in error:
            return 'mapping'

        # Check what output exists
        data_dir = self.config.data_dir
        harvest_dir = data_dir / hub / "harvest"
        mapping_dir = data_dir / hub / "mapping"

        if not get_latest_dir(harvest_dir):
            return 'harvest'
        if not get_latest_dir(mapping_dir):
            return 'mapping'

        return 'unknown'

    def _build_completion_slack_message(
        self,
        run_id: str,
        complete: int,
        failed: int,
        total: int,
        enriched_hubs: dict,
        draft_dir: Optional[Path],
        hubs_without_email: list[str],
        prefix: str,
        status_emoji: str
    ) -> str:
        """Build the Slack completion message with truncation for size limits.

        Failures are listed FIRST to ensure they aren't truncated.
        """
        lines = [
            f"{prefix}{status_emoji} *DPLA Ingest Run Summary*",
            "",
        ]

        # Failed hubs section FIRST (so failures aren't truncated)
        failed_hubs = [
            (name, info) for name, info in enriched_hubs.items()
            if info.get('status') == 'failed'
        ]

        if failed_hubs:
            lines.append("*Failed:*")
            for hub_name, info in failed_hubs:
                line = self._format_failed_hub_line(hub_name, info)
                lines.append(line)
            lines.append("")

        # Completed hubs section
        completed_hubs = [
            (name, info) for name, info in enriched_hubs.items()
            if info.get('status') == 'complete'
        ]

        if completed_hubs:
            lines.append("*Completed:*")
            for hub_name, info in completed_hubs:
                hub_lines = self._format_completed_hub_lines(hub_name, info)
                lines.extend(hub_lines)
            lines.append("")

        # Skipped hubs section (if any)
        skipped_hubs = [
            (name, info) for name, info in enriched_hubs.items()
            if info.get('status') == 'skipped'
        ]

        if skipped_hubs:
            lines.append(f"*Skipped:* {', '.join(name for name, _ in skipped_hubs)}")
            lines.append("")

        # No-email notice
        if hubs_without_email:
            lines.append(f":email: *Manual notification required* — no email in config for: {', '.join(hubs_without_email)}")

        # Join and truncate if needed
        text = "\n".join(lines)
        if len(text) > MAX_SLACK_TEXT_LENGTH:
            text = text[:MAX_SLACK_TEXT_LENGTH - 50] + "\n... (truncated)"

        return text

    def _format_completed_hub_lines(self, hub_name: str, info: dict) -> list[str]:
        """Format a completed hub entry for Slack (multi-line block)."""
        harvest = info.get('harvest_count')
        mapped = info.get('mapping_successful')
        failed_count = info.get('mapping_failed')

        header = f"  • `{hub_name}`"

        if info.get('summary_available') and mapped is not None:
            block = self._format_counts_block(harvest, mapped, failed_count)
            # Indent block lines under the hub bullet
            indented = "\n".join(f"    {l}" for l in block.split("\n"))
            return [header, indented]
        elif mapped:
            return [f"{header} — ~`{mapped:,}` records"]
        else:
            return [f"{header} — done"]

    def _format_failed_hub_line(self, hub_name: str, info: dict) -> str:
        """Format a single failed hub line for Slack."""
        stage = info.get('failure_stage', 'unknown')
        error = info.get('error', '')[:80] if info.get('error') else ''

        line = f"  • `{hub_name}` failed at {stage}"
        if error:
            line += f": {error}"

        return line

    def _build_draft_emails_snippet(self, draft_dir: Path) -> str | None:
        """Build a text snippet from draft email files for Slack code block.

        Reads each .draft.txt, extracts To/Subject lines, and returns a
        compact snippet that can be copy-pasted.  Truncates to stay within
        Slack's code-block limits (~3000 chars).
        """
        try:
            drafts = sorted(draft_dir.glob("*.draft.txt"))
            if not drafts:
                return None

            snippet_lines = []
            for draft_file in drafts:
                content = draft_file.read_text()
                # Extract To and Subject lines
                to_line = ""
                subject_line = ""
                for line in content.split("\n"):
                    if line.startswith("To:"):
                        to_line = line.strip()
                    elif line.startswith("Subject:"):
                        subject_line = line.strip()
                    if to_line and subject_line:
                        break
                hub_name = draft_file.stem.replace(".draft", "")
                snippet_lines.append(f"[{hub_name}]")
                if to_line:
                    snippet_lines.append(f"  {to_line}")
                if subject_line:
                    snippet_lines.append(f"  {subject_line}")
                snippet_lines.append("")

            snippet = "\n".join(snippet_lines).strip()
            # Keep under 3000 chars for Slack code blocks
            if len(snippet) > 3000:
                snippet = snippet[:2950] + "\n... (truncated)"
            return snippet
        except Exception:
            return None

    def _write_email_drafts(
        self,
        run_id: str,
        enriched_hubs: dict,
        test_prefix: Optional[str] = None
    ) -> tuple[Optional[Path], list[str]]:
        """Write email draft files for completed hubs.

        Returns:
            (draft_directory, list_of_hubs_without_email)
        """
        # Determine draft directory
        dir_name = f"hub-emails-{run_id}"
        if test_prefix:
            dir_name = f"hub-emails-TEST-{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        draft_dir = self.config.logs_dir / dir_name
        draft_dir.mkdir(parents=True, exist_ok=True)

        hubs_without_email = []
        drafts_written = False

        for hub_name, info in enriched_hubs.items():
            if info.get('status') != 'complete':
                continue

            # Get hub email from config
            hub_config = self.config.get_hub_config(hub_name)
            hub_email = hub_config.email if hub_config else ""

            if not hub_email:
                hubs_without_email.append(hub_name)
                continue

            # Build draft content
            draft_content = self._build_email_draft(
                hub_name, hub_email, info, test_prefix
            )

            # Write draft file
            draft_file = draft_dir / f"{hub_name}.draft.txt"
            draft_file.write_text(draft_content)
            drafts_written = True

        return (draft_dir if drafts_written else None), hubs_without_email

    def _build_email_draft(
        self,
        hub_name: str,
        hub_email: str,
        info: dict,
        test_prefix: Optional[str] = None
    ) -> str:
        """Build email draft content matching Emailer.scala format."""
        # For testing, override recipient
        recipient = "scott@dp.la" if test_prefix else hub_email
        provider_name = hub_name.upper()

        # Get hub config for provider name
        hub_config = self.config.get_hub_config(hub_name)
        if hub_config and hub_config.provider:
            provider_name = hub_config.provider

        prefix_line = f"[{test_prefix}] " if test_prefix else ""

        # Build subject
        month = datetime.now().strftime("%B %Y")
        subject = f"{prefix_line}DPLA Ingest Summary for {provider_name} - {month}"

        # Build body (matching Emailer.scala format)
        body_lines = [
            f"To: {recipient}",
            f"Subject: {subject}",
            f"Hub Email: {hub_email}" if test_prefix and hub_email != recipient else "",
            "",
            "This is an automated email summarizing the DPLA ingest. Please see attached ZIP file",
            "for record level information about errors and warnings.",
            "",
            "If you have questions please contact us at tech@dp.la",
            "",
            "- Ingestion documentation: https://github.com/dpla/ingestion3/",
            "",
            "--------------------------------------------------------------------------------",
            "",
        ]

        # Add summary content
        body_lines.extend(self._format_summary_for_email(hub_name, info))

        # Add S3 links if available
        s3_paths = info.get('s3_paths')
        failed_count = info.get('mapping_failed', 0)
        if s3_paths:
            body_lines.extend([
                "",
                "--------------------------------------------------------------------------------",
                "S3 Links (valid for 7 days):",
                "",
            ])
            if s3_paths.summary_url:
                body_lines.append(f"Summary: {s3_paths.summary_url}")
            if s3_paths.logs_url:
                if failed_count and failed_count > 0:
                    body_lines.append(f"Error Logs ({failed_count:,} failed records): {s3_paths.logs_url}")
                else:
                    body_lines.append(f"Logs: {s3_paths.logs_url}")

        body_lines.extend([
            "",
            "",
            "Bleep bloop.",
            "",
            "-----------------  END  -----------------",
        ])

        return "\n".join(line for line in body_lines if line is not None)

    def _format_summary_for_email(self, hub_name: str, info: dict) -> list[str]:
        """Format summary section for email body."""
        lines = []

        # Try to read actual _SUMMARY file content
        mapping_dir = self.config.data_dir / hub_name / "mapping"
        latest_mapping = get_latest_dir(mapping_dir)

        if latest_mapping:
            summary_file = latest_mapping / "_SUMMARY"
            if summary_file.exists():
                try:
                    content = summary_file.read_text()
                    # Drop last 5 lines (local log file references) per Emailer.scala
                    content_lines = content.strip().split('\n')
                    if len(content_lines) > 5:
                        content_lines = content_lines[:-5]
                    lines.extend(content_lines)
                    return lines
                except Exception:
                    pass

        # Fallback: build summary from parsed counts
        lines.append(f"Mapping Summary")
        lines.append("")
        lines.append(f"Provider: {hub_name.upper()}")
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        harvest = info.get('harvest_count')
        attempted = info.get('mapping_attempted')
        successful = info.get('mapping_successful')
        failed_count = info.get('mapping_failed')

        if harvest:
            lines.append(f"Harvested: {harvest:,}")
        if attempted is not None:
            lines.append(f"Attempted: {attempted:,}")
        if successful is not None:
            lines.append(f"Successful: {successful:,}")
        if failed_count is not None:
            lines.append(f"Failed: {failed_count:,}")

        if not info.get('summary_available'):
            lines.append("")
            lines.append("Note: Full summary file unavailable. Data is staged for indexing.")

        return lines

    def escalate_failures(
        self,
        failure_report: dict,
        test_prefix: Optional[str] = None
    ) -> Path:
        """Create escalation files for failed hubs."""
        prefix = f"{test_prefix} " if test_prefix else ""
        run_id = failure_report['run_id']

        # Write JSON report
        json_file = self.escalation_dir / f"failures-{run_id}.json"
        json_file.write_text(json.dumps(failure_report, indent=2, default=str))

        # Write human-readable markdown
        md_file = self.escalation_dir / f"failures-{run_id}.md"
        md_file.write_text(self._format_failure_markdown(failure_report))

        # Print to console
        print(f"\n{'='*60}")
        print(f"  {prefix}⚠️  FAILURES NEED ATTENTION")
        print(f"{'='*60}")
        print(f"  Run ID: {run_id}")
        print(f"  Failed hubs: {', '.join(failure_report['failed_hubs'])}")
        print(f"")
        print(f"  Review: {md_file}")
        print(f"")
        print(f"  Debug with Cursor Agent:")
        for hub in failure_report['failed_hubs'][:3]:
            print(f"    'Debug the {hub} ingest failure'")
        print(f"{'='*60}\n")

        # Send Slack alert
        if self.config.slack_webhook:
            mention = ""
            if getattr(self.config, 'slack_alert_user_id', None):
                mention = f"<@{self.config.slack_alert_user_id}> "

            # Build OAI context block if available in any failure
            oai_context = ""
            for hub_name, details in failure_report.get('failures', {}).items():
                diag = details.get('diagnosis') or {}
                ctx = diag.get('context') or {}
                if ctx.get('oai_set') or ctx.get('resumption_token'):
                    oai_context += f"\n\n*OAI debug context for `{hub_name}`*\n"
                    if ctx.get('oai_set'):
                        oai_context += f"• Set: `{ctx['oai_set']}`\n"
                    if ctx.get('resumption_token'):
                        oai_context += f"• Resumption token: `{ctx['resumption_token']}`\n"
                    if ctx.get('cursor') is not None:
                        cursor_str = str(ctx['cursor'])
                        if ctx.get('complete_list_size') is not None:
                            cursor_str += f" / {ctx['complete_list_size']}"
                        oai_context += f"• Cursor: {cursor_str}\n"
                    if ctx.get('url'):
                        oai_context += f"• URL: {ctx['url']}\n"

            # Include per-hub error summaries
            error_lines = ""
            for hub_name, details in failure_report.get('failures', {}).items():
                error_msg = details.get('error', 'Unknown')
                if len(error_msg) > 200:
                    error_msg = error_msg[:200] + "..."
                error_lines += f"\n`{hub_name}`: {error_msg}"

            self._send_slack({
                "text": f"{prefix}:x: DPLA Ingest Failures\n"
                       f"{mention}Harvest failure needs attention\n"
                       f"Run: `{run_id}`\n"
                       f"Failed: {', '.join(failure_report['failed_hubs'])}"
                       f"{error_lines}"
                       f"{oai_context}\n"
                       f"Review: `{md_file}`"
            })

        # Create GitHub issue
        if self.config.github_token:
            self._create_github_issue(failure_report)

        return md_file

    def _format_failure_markdown(self, report: dict) -> str:
        """Format failure report as markdown."""
        lines = [
            f"# Ingest Failures - {report['run_id']}",
            "",
            f"**Date**: {report['timestamp']}",
            f"**Month**: {report['month']}/{report['year']}",
            f"**Summary**: {report['summary']['failed']}/{report['summary']['total']} failed",
            "",
            "---",
            "",
            "## Failed Hubs",
            "",
        ]

        for hub, details in report.get('failures', {}).items():
            diagnosis = details.get('diagnosis') or {}
            lines.extend([
                f"### {hub}",
                "",
                f"**Error Type**: `{diagnosis.get('error_type', 'unknown')}`",
                "",
                f"**Description**: {diagnosis.get('description', 'No description')}",
                "",
                f"**Suggested Fix**:",
                "",
                f"> {diagnosis.get('suggested_fix', 'Review logs manually')}",
                "",
                f"**Retries**: {details.get('retries', 0)}",
                "",
            ])

            # OAI debug context
            ctx = diagnosis.get('context', {})
            if ctx.get('oai_set') or ctx.get('resumption_token'):
                lines.extend([
                    "**OAI Debug Context**:",
                    "",
                ])
                if ctx.get('oai_set'):
                    lines.append(f"- Set: `{ctx['oai_set']}`")
                if ctx.get('resumption_token'):
                    lines.append(f"- Resumption token: `{ctx['resumption_token']}`")
                if ctx.get('cursor') is not None:
                    cursor_str = str(ctx['cursor'])
                    if ctx.get('complete_list_size') is not None:
                        cursor_str += f" / {ctx['complete_list_size']}"
                    lines.append(f"- Cursor: {cursor_str}")
                if ctx.get('url'):
                    lines.append(f"- URL: {ctx['url']}")
                if ctx.get('first_id'):
                    lines.append(f"- First ID: `{ctx['first_id']}`")
                if ctx.get('last_id'):
                    lines.append(f"- Last ID: `{ctx['last_id']}`")
                lines.append("")

            log_snippet = ctx.get('log_snippet', '')
            if log_snippet:
                lines.extend([
                    "**Log Snippet**:",
                    "",
                    "```",
                    log_snippet[:2000],
                    "```",
                    "",
                ])

            lines.append("---")
            lines.append("")

        lines.extend([
            "## How to Debug",
            "",
            "### Option 1: Cursor Agent",
            "",
            "Open Cursor and say:",
            "",
            "```",
            f"Debug the {report['failed_hubs'][0] if report['failed_hubs'] else 'hub'} ingest failure",
            "```",
            "",
            "The agent will:",
            "1. Read this failure report",
            "2. Analyze logs",
            "3. Apply fixes",
            "4. Retry the ingest",
            "",
            "### Option 2: Manual",
            "",
            "1. Review logs in `ingestion3/logs/`",
            "2. Check i3.conf configuration",
            "3. Run individual scripts:",
            "   - `./scripts/harvest.sh <hub>`",
            "   - `./scripts/remap.sh <hub>`",
            "",
            "### Common Fixes",
            "",
            "| Error | Fix |",
            "|-------|-----|",
            "| OutOfMemoryError (SI) | Run xmll preprocessing |",
            "| Timeout | Retry, check feed status |",
            "| No output | Check OAI feed URL |",
            "| sbt conflict | Wait, kill stale processes |",
            "",
        ])

        return "\n".join(lines)

    def _send_slack(self, payload: dict):
        """Send a Slack webhook message."""
        if not self.config.slack_webhook:
            return

        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                self.config.slack_webhook,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            urllib.request.urlopen(req, timeout=10)
        except urllib.error.URLError as e:
            print(f"Warning: Could not send Slack notification: {e}")

    def _create_github_issue(self, failure_report: dict):
        """Create a GitHub issue for failures."""
        if not self.config.github_token:
            return

        # GitHub issue creation would go here
        # For now, just print a note
        print(f"Note: GitHub issue creation not yet implemented")


def format_hub_status(hub: str, status: str, duration: Optional[int] = None) -> str:
    """Format a hub status line for display."""
    status_icons = {
        'pending': '⏳',
        'preparing': '📥',
        'harvesting': '🌾',
        'mapping': '🗺️',
        'enriching': '✨',
        'jsonl_export': '📦',
        'remapping': '🔄',
        'syncing': '☁️',
        'complete': '✅',
        'failed': '❌',
        'skipped': '⏭️',
    }

    icon = status_icons.get(status, '❓')
    duration_str = f" ({duration}s)" if duration else ""

    return f"  {icon} {hub}: {status}{duration_str}"
