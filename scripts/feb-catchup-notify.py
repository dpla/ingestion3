#!/usr/bin/env python3
"""One-time script: sync February 2026 hubs to S3 and post Slack notifications.

For each hub with February-timestamped mapping data:
  1. Check if already synced to S3 (Feb data present in S3 jsonl/)
  2. If not synced: run anomaly detection, sync if safe
  3. Post hub-complete message to #tech (no @here) for all synced hubs

Usage:
    python scripts/feb-catchup-notify.py --dry-run   # preview only
    python scripts/feb-catchup-notify.py              # sync + notify
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scheduler.orchestrator.config import load_config
from scheduler.orchestrator.notifications import Notifier
from scheduler.orchestrator.anomaly_detector import AnomalyDetector
from scheduler.orchestrator.s3_utils import get_latest_dir

TARGET_MONTH = "202602"  # February 2026


def find_feb_hubs(data_dir: Path) -> list[dict]:
    """Find hubs with February 2026 mapping data.

    Returns list of dicts with hub name, mapping dir, timestamp, and counts.
    Uses the most recent February directory if multiple exist.
    """
    hubs = []
    for hub_dir in sorted(data_dir.iterdir()):
        if not hub_dir.is_dir() or hub_dir.name.startswith("."):
            continue
        mapping_dir = hub_dir / "mapping"
        if not mapping_dir.exists():
            continue

        # Find February dirs with _SUCCESS
        feb_dirs = []
        for d in mapping_dir.iterdir():
            if d.is_dir() and d.name.startswith(TARGET_MONTH) and (d / "_SUCCESS").exists():
                feb_dirs.append(d)

        if not feb_dirs:
            continue

        # Use most recent
        latest = max(feb_dirs, key=lambda d: d.name)
        ts = latest.name[:15]  # YYYYMMDD_HHMMSS

        # Parse counts from _SUMMARY
        counts = _parse_summary(latest / "_SUMMARY")

        # Parse harvest count from most recent Feb harvest _MANIFEST
        harvest_count = _get_feb_harvest_count(hub_dir / "harvest")

        hubs.append({
            "hub": hub_dir.name,
            "mapping_dir": latest,
            "timestamp": ts,
            "date": f"{ts[4:6]}-{ts[6:8]}-{ts[0:4]}",
            "harvest_count": harvest_count,
            **counts,
        })

    return hubs


def _parse_summary(summary_path: Path) -> dict:
    """Parse mapping counts from _SUMMARY file."""
    counts = {"attempted": None, "successful": None, "failed": None}
    if not summary_path.exists():
        return counts
    try:
        content = summary_path.read_text()
        m_a = re.search(r"Attempted\.+([0-9,]+)", content)
        m_s = re.search(r"Successful\.+([0-9,]+)", content)
        m_f = re.search(r"Failed\.+([0-9,]+)", content)
        if m_a:
            counts["attempted"] = int(m_a.group(1).replace(",", ""))
        if m_s:
            counts["successful"] = int(m_s.group(1).replace(",", ""))
        if m_f:
            counts["failed"] = int(m_f.group(1).replace(",", ""))
    except Exception:
        pass
    return counts


def _get_feb_harvest_count(harvest_dir: Path) -> int | None:
    """Get harvest record count from the most recent February harvest."""
    if not harvest_dir.exists():
        return None
    feb_dirs = [
        d for d in harvest_dir.iterdir()
        if d.is_dir() and d.name.startswith(TARGET_MONTH) and (d / "_SUCCESS").exists()
    ]
    if not feb_dirs:
        return None
    latest = max(feb_dirs, key=lambda d: d.name)
    manifest = latest / "_MANIFEST"
    if not manifest.exists():
        return None
    try:
        content = manifest.read_text()
        m = re.search(r"Record count:\s*([0-9,]+)", content)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return None


def check_s3_synced(hub: str, config) -> bool:
    """Check if February data already exists in S3 jsonl/ for this hub."""
    s3_prefix = config.get_s3_prefix(hub)
    try:
        out = subprocess.check_output(
            f"aws s3 ls s3://dpla-master-dataset/{s3_prefix}/jsonl/ --profile {config.aws_profile}",
            shell=True, text=True, stderr=subprocess.DEVNULL,
        )
        return any(TARGET_MONTH in line for line in out.strip().split("\n"))
    except Exception:
        return False


def run_anomaly_check(hub: str, config) -> tuple[bool, object]:
    """Run anomaly detection for a hub (local vs S3 baseline).

    Returns (safe_to_sync, anomaly_report).
    """
    detector = AnomalyDetector(
        data_dir=str(config.data_dir),
        aws_profile=config.aws_profile,
    )
    report = detector.check_hub(hub, verbose=False)

    if report.anomalies:
        for a in report.anomalies:
            print(f"    [{a.severity.upper()}] {a.message}")

    return not report.should_halt, report


def sync_to_s3(hub: str, config) -> bool:
    """Run s3-sync.sh for a hub. Returns True on success."""
    cmd = f"cd {config.i3_home} && ./scripts/s3-sync.sh {hub}"
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0:
            return True
        else:
            print(f"    S3 sync failed (exit {result.returncode}): {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"    S3 sync timed out after 600s")
        return False
    except Exception as e:
        print(f"    S3 sync error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Sync Feb 2026 hubs to S3 and post Slack notifications")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no sync or Slack posts")
    args = parser.parse_args()

    config = load_config()
    notifier = Notifier(config)

    print("=" * 60)
    print("  February 2026 — Catchup Sync & Notify")
    if args.dry_run:
        print("  MODE: DRY RUN (no sync, no Slack)")
    print("=" * 60)

    feb_hubs = find_feb_hubs(config.data_dir)
    print(f"\nFound {len(feb_hubs)} hubs with February mapping data:\n")

    notified = []
    newly_synced = []
    skipped_anomaly = []
    failed_sync = []

    for hub_info in feb_hubs:
        hub = hub_info["hub"]
        print(f"  {hub} ({hub_info['date']})")

        if hub_info["successful"] is not None:
            print(f"    Harvested: {hub_info['harvest_count']:,}" if hub_info["harvest_count"] else "    Harvested: unknown")
            print(f"    Mapped: {hub_info['successful']:,} successful, {hub_info['failed']:,} failed")
        else:
            print(f"    Counts: unavailable (_SUMMARY missing)")

        # Check if already synced
        already_synced = check_s3_synced(hub, config)

        if already_synced:
            print(f"    Already synced to S3")
            if args.dry_run:
                print(f"    [DRY RUN] Would post to #tech")
            else:
                _post_hub_notification(notifier, hub, hub_info, config)
            notified.append(hub)
            print()
            continue

        # Not yet synced — run anomaly detection
        print(f"    Not in S3 — checking anomalies...")
        safe, report = run_anomaly_check(hub, config)

        if not safe:
            print(f"    ⛔ CRITICAL anomalies — skipping sync and notification")
            skipped_anomaly.append(hub)
            if not args.dry_run:
                notifier.send_anomaly_alert(hub, report)
            print()
            continue

        if report.anomalies:
            print(f"    ⚠️  Warnings present — proceeding with sync")
            if not args.dry_run:
                notifier.send_anomaly_alert(hub, report)

        # S3 sync
        if args.dry_run:
            print(f"    [DRY RUN] Would sync to S3 and post to #tech")
            newly_synced.append(hub)
            notified.append(hub)
        else:
            print(f"    Syncing to S3...")
            if sync_to_s3(hub, config):
                print(f"    ✅ Synced")
                newly_synced.append(hub)
                notified.append(hub)
                _post_hub_notification(notifier, hub, hub_info, config)
            else:
                print(f"    ❌ Sync failed")
                failed_sync.append(hub)

        print()

    # Summary
    print("=" * 60)
    print(f"  SUMMARY")
    print(f"  Notified:     {len(notified)} hubs posted to #tech")
    if newly_synced:
        print(f"  Newly synced: {len(newly_synced)} — {', '.join(newly_synced)}")
    if skipped_anomaly:
        print(f"  Skipped:      {len(skipped_anomaly)} (anomalies) — {', '.join(skipped_anomaly)}")
    if failed_sync:
        print(f"  Failed sync:  {len(failed_sync)} — {', '.join(failed_sync)}")
    if args.dry_run:
        print(f"\n  DRY RUN — no changes made")
    print("=" * 60)


def _post_hub_notification(notifier: Notifier, hub: str, hub_info: dict, config):
    """Post a hub-complete notification to #tech without @here."""
    date_str = hub_info["date"]
    harvest_count = hub_info["harvest_count"]
    successful = hub_info["successful"]
    failed = hub_info["failed"]

    # Fetch baseline for deltas and previous run block
    baseline = notifier._fetch_baseline(hub)

    # Current run with deltas
    lines = [
        f":white_check_mark: *`{hub}` re-ingested* (*{date_str}*)",
        "",
    ]
    lines.extend(notifier._format_counts_with_deltas(
        harvest_count, successful, failed, baseline
    ))

    # Previous run (plain counts including harvest)
    if baseline:
        prev_harvest = baseline.get("harvest_attempted")
        lines.append("")
        lines.append(f"vs previous run on *{baseline['date']}*")
        lines.extend(
            Notifier._format_counts_block(
                prev_harvest, baseline.get("successful"), baseline.get("failed"),
            ).split("\n")
        )

    text = "\n".join(lines)

    # Console
    print(f"    Posting to #tech:")
    for line in lines:
        print(f"      {line}")

    notifier._send_slack_tech({"text": text})


if __name__ == "__main__":
    main()
