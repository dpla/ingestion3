"""Staged-hubs report: which hubs have new data in S3 from the current month.

Scans s3://dpla-master-dataset/<hub>/jsonl/ for directories whose timestamp
falls within the target month.  Useful for answering "which hubs have new
data staged for the next indexing run?"

Usage:
    python -m scheduler.orchestrator.staged_report               # current month
    python -m scheduler.orchestrator.staged_report --month=1     # January
    python -m scheduler.orchestrator.staged_report --slack        # post to #tech
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional


# S3 prefix → hub name (mirrors backlog_emails.py / Config.S3_PREFIX_MAP)
S3_PREFIX_TO_HUB = {
    "hathitrust": "hathi",
    "tennessee": "tn",
}


def _s3_prefix_to_hub(s3_prefix: str) -> str:
    return S3_PREFIX_TO_HUB.get(s3_prefix, s3_prefix)


def _dir_to_date(dir_name: str) -> Optional[str]:
    """Extract MM-DD-YYYY from a dir name starting with YYYYMMDD_."""
    if len(dir_name) >= 8 and dir_name[:8].isdigit():
        y, m, d = dir_name[0:4], dir_name[4:6], dir_name[6:8]
        return f"{m}-{d}-{y}"
    return None


def get_staged_hubs(
    bucket: str = "dpla-master-dataset",
    month: Optional[int] = None,
    year: Optional[int] = None,
    aws_profile: str = "dpla",
) -> list[dict]:
    """Return hubs with JSONL data from the given month.

    Each entry: {"hub": str, "s3_prefix": str, "dates": [str, ...]}
    """
    now = datetime.now()
    month = month or now.month
    year = year or now.year
    prefix_filter = f"{year}{month:02d}"

    # 1. List top-level prefixes
    cmd = ["aws", "s3", "ls", f"s3://{bucket}/", "--profile", aws_profile]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        print(f"Error listing S3 bucket: {out.stderr}", file=sys.stderr)
        return []

    hub_prefixes = []
    for line in out.stdout.strip().splitlines():
        if line.strip().startswith("PRE "):
            prefix = line.strip()[4:].rstrip("/")
            if prefix and not prefix.startswith("."):
                hub_prefixes.append(prefix)

    # 2. For each hub, check jsonl/ for matching month
    results = []
    for s3_prefix in sorted(hub_prefixes):
        cmd2 = [
            "aws", "s3", "ls",
            f"s3://{bucket}/{s3_prefix}/jsonl/",
            "--profile", aws_profile,
        ]
        out2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=60)
        if out2.returncode != 0:
            continue

        dates = []
        for line in out2.stdout.strip().splitlines():
            if not line.strip().startswith("PRE "):
                continue
            dir_name = line.strip()[4:].rstrip("/")
            if dir_name.startswith(prefix_filter):
                date_str = _dir_to_date(dir_name)
                if date_str:
                    dates.append(date_str)

        if dates:
            hub_name = _s3_prefix_to_hub(s3_prefix)
            results.append({
                "hub": hub_name,
                "s3_prefix": s3_prefix,
                "dates": sorted(set(dates)),
            })

    return results


def format_report(staged: list[dict], month: int, year: int) -> str:
    """Format the staged-hubs report for console or Slack."""
    month_name = datetime(year, month, 1).strftime("%B %Y")

    if not staged:
        return f"No hubs have new JSONL data staged for {month_name}."

    lines = [
        f"*Hubs with new data staged in S3 — {month_name}*",
        f"These hubs will be updated on the next indexing run.",
        "",
    ]

    for entry in staged:
        hub = entry["hub"]
        latest = entry["dates"][-1] if entry["dates"] else "unknown"
        lines.append(f"  • `{hub}` (latest: {latest})")

    lines.append("")
    lines.append(f"Total: {len(staged)} hubs")

    return "\n".join(lines)


def post_to_slack(text: str, webhook: Optional[str] = None):
    """Post text to Slack via webhook."""
    webhook = webhook or os.environ.get("SLACK_TECH_WEBHOOK") or os.environ.get("SLACK_WEBHOOK")
    if not webhook:
        print("No Slack webhook configured (SLACK_TECH_WEBHOOK / SLACK_WEBHOOK)", file=sys.stderr)
        return False

    try:
        payload = json.dumps({"text": text}).encode("utf-8")
        req = urllib.request.Request(
            webhook, data=payload, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        print("Posted to Slack.")
        return True
    except urllib.error.URLError as e:
        print(f"Slack post failed: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Report hubs with new data staged in S3 for the current month",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scheduler.orchestrator.staged_report                # current month
  python -m scheduler.orchestrator.staged_report --month=1      # January
  python -m scheduler.orchestrator.staged_report --slack         # post to #tech
  python -m scheduler.orchestrator.staged_report --json          # JSON output
        """,
    )
    parser.add_argument("--month", "-m", type=int, default=None, help="Month (1-12)")
    parser.add_argument("--year", "-y", type=int, default=None, help="Year (default: current)")
    parser.add_argument("--slack", action="store_true", help="Post report to Slack")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--profile", default="dpla", help="AWS profile")

    args = parser.parse_args()

    now = datetime.now()
    month = args.month or now.month
    year = args.year or now.year

    staged = get_staged_hubs(month=month, year=year, aws_profile=args.profile)

    if args.json:
        print(json.dumps(staged, indent=2))
    else:
        report = format_report(staged, month, year)
        print(report)

        if args.slack:
            post_to_slack(report)


if __name__ == "__main__":
    main()
