#!/usr/bin/env python3
"""
DPLA Postchecks

Checks whether all hubs have been ingested and indexed for the current month.

Sections:
  1. INGEST STATUS   — latest JSONL snapshot date + record count per hub in S3
  2. POST-INDEXER    — provider export, hub stats, sitemaps freshness
  3. API             — live record count from api.dp.la (requires DPLA_API_KEY)
  4. SUMMARY         — counts of fresh / stale / missing

Usage:
    python3 postchecks.py
    python3 postchecks.py --month 202605      # check specific month
    python3 postchecks.py --no-api            # skip API count check
    python3 postchecks.py --stale-days 60     # widen staleness window
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ---------- config ----------
REGION        = "us-east-1"
S3_DATASET    = "dpla-master-dataset"
S3_EXPORT     = "dpla-provider-export"
S3_ANALYTICS  = "dashboard-analytics"
S3_SITEMAPS   = "sitemaps.dp.la"
DEFAULT_STALE_DAYS = 45

# S3 prefix aliases (hub name → S3 prefix, matching common.sh's resolve_s3_prefix)
S3_ALIASES = {
    "hathi": "hathitrust",
    "tn":    "tennessee",
}


# ---------- AWS helpers ----------
def aws(args, check=True):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def s3_ls(prefix, check=False):
    """List an S3 prefix. Returns raw output string."""
    return aws(["s3", "ls", prefix, "--region", REGION], check=check)


def s3_read(s3_path):
    """Stream a small S3 object to stdout. Returns content string."""
    result = subprocess.run(
        ["aws", "s3", "cp", s3_path, "-", "--region", REGION],
        capture_output=True, text=True,
    )
    return result.stdout if result.returncode == 0 else ""


# ---------- color ----------
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
DIM    = "\033[2m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
USE_COLOR = sys.stdout.isatty()


def c(color, text):
    return f"{color}{text}{RESET}" if USE_COLOR else text


def pad(text, width):
    """Left-pad plain text (strip ANSI for width calc)."""
    plain = re.sub(r"\033\[[0-9;]*m", "", text)
    return text + " " * max(0, width - len(plain))


# ---------- ingest status helpers ----------
def parse_manifest_count(content):
    """Extract record count from _MANIFEST content."""
    m = re.search(r"^Record count:\s*([\d,]+)", content, re.MULTILINE)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def latest_jsonl_snapshot(hub):
    """
    Return (snapshot_dir, date_str, records) for the most recent JSONL snapshot
    for a hub in s3://dpla-master-dataset/<hub>/jsonl/.
    Returns (None, None, None) if none found.
    """
    s3_prefix = S3_ALIASES.get(hub, hub)
    ls = s3_ls(f"s3://{S3_DATASET}/{s3_prefix}/jsonl/")
    if not ls.strip():
        return None, None, None

    lines = [l for l in ls.splitlines() if l.strip()]
    # Lines are: "2026-05-02 12:00:00       PRE 20260502_120000-hub-MAP4_0.jsonl/"
    # or just dates for the snapshot dirs; pick the most recent by sorting
    latest_line = sorted(lines)[-1]
    date_str = latest_line.strip().split()[0]  # "2026-05-02"

    # Get directory name (last token, strip trailing /)
    dir_name = latest_line.strip().split()[-1].rstrip("/")
    snapshot_path = f"s3://{S3_DATASET}/{s3_prefix}/jsonl/{dir_name}/_MANIFEST"

    manifest = s3_read(snapshot_path)
    records = parse_manifest_count(manifest) if manifest else None

    return dir_name, date_str, records


def classify_age(date_str, now, stale_days):
    """
    Return (class, age_days) where class is one of:
      'current'  — within stale_days (green ✓ — covers cross-month cycles)
      'warning'  — between stale_days and stale_days*2 (yellow ⚠)
      'stale'    — older than stale_days*2 (red ✗)
      'unknown'  — date missing or unparseable
    """
    if not date_str:
        return "unknown", None
    try:
        snap_dt  = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        age_days = (now - snap_dt).days
        if age_days <= stale_days:
            return "current", age_days
        elif age_days <= stale_days * 2:
            return "warning", age_days
        else:
            return "stale", age_days
    except ValueError:
        return "unknown", None


# ---------- post-indexer helpers ----------
def check_s3_path_exists(s3_path):
    """Returns (exists: bool, date_str: str|None)."""
    ls = s3_ls(s3_path)
    if not ls.strip():
        return False, None
    date_str = ls.strip().splitlines()[0].strip().split()[0]
    return True, date_str


def check_s3_file_freshness(s3_path, today_str):
    """Returns ('fresh'|'stale'|'missing', date_str|None)."""
    ls = s3_ls(s3_path)
    if not ls.strip():
        return "missing", None
    date_str = ls.strip().split()[0]
    if date_str == today_str:
        return "fresh", date_str
    return "stale", date_str


# ---------- API check ----------
def load_api_key():
    secrets = os.path.expanduser("~/.dpla-secrets.env")
    if os.path.exists(secrets):
        with open(secrets) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DPLA_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("DPLA_API_KEY", "")


def fetch_api_count(api_key):
    try:
        result = subprocess.run(
            ["curl", "-s", f"https://api.dp.la/v2/items?api_key={api_key}&page_size=0"],
            capture_output=True, text=True, timeout=15,
        )
        data = json.loads(result.stdout)
        return data.get("count")
    except Exception:
        return None


# ---------- render ----------
def render(hub_results, now, month_str, stale_days, check_api):
    lines = []
    today_str = now.strftime("%Y-%m-%d")
    year, month = now.year, f"{now.month:02d}"

    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  DPLA Pipeline Status — {c(BOLD, month_str)}")
    lines.append(c(DIM, "=" * 70))

    # ── INGEST STATUS ────────────────────────────────────────────────────────
    lines.append("")
    lines.append(f"INGEST STATUS  (s3://{S3_DATASET}/)")
    lines.append(c(DIM, f"  {'HUB':<24} {'LATEST SNAPSHOT':<14} {'RECORDS':>12}   STATUS"))
    lines.append(c(DIM, "  " + "─" * 64))

    current_count = 0
    warning_count = 0
    stale_count   = 0
    missing_count = 0

    for hub, (snapshot, date_str, records) in sorted(hub_results.items()):
        age_class, age_days = classify_age(date_str, now, stale_days)

        rec_str  = f"{records:>12,}" if records is not None else f"{'—':>12}"
        date_col = date_str or "—"

        if age_class == "current":
            status = c(GREEN, f"✓ {age_days}d ago")
            current_count += 1
        elif age_class == "warning":
            status = c(YELLOW, f"⚠ {age_days}d ago — overdue?")
            warning_count += 1
        elif age_class == "stale":
            status = c(RED,    f"✗ {age_days}d ago — STALE")
            stale_count += 1
        else:
            status = c(RED,    "✗ NOT FOUND")
            missing_count += 1
            rec_str  = f"{'—':>12}"
            date_col = "—"

        hub_col = pad(c(BOLD if age_class == "current" else "", hub), 24)
        lines.append(f"  {hub_col} {date_col:<14} {rec_str}   {status}")

    lines.append(c(DIM, "  " + "─" * 64))
    total = len(hub_results)
    summary_parts = [c(GREEN, f"{current_count} within {stale_days}d")]
    if warning_count:
        summary_parts.append(c(YELLOW, f"{warning_count} overdue"))
    if stale_count:
        summary_parts.append(c(RED, f"{stale_count} stale"))
    if missing_count:
        summary_parts.append(c(RED, f"{missing_count} missing"))
    lines.append(f"  {total} hubs total — " + " | ".join(summary_parts))

    # ── POST-INDEXER ─────────────────────────────────────────────────────────
    lines.append("")
    lines.append("POST-INDEXER")

    # Provider export
    export_path = f"s3://{S3_EXPORT}/{year}/{month}/"
    exists, _ = check_s3_path_exists(export_path)
    if exists:
        lines.append(f"  {c(GREEN, '✓')} Provider export    {c(DIM, export_path)}")
    else:
        lines.append(f"  {c(RED, '✗')} Provider export    {c(DIM, export_path + ' — NOT FOUND')}")

    # Hub stats
    for label, s3_path in [
        ("Hub stats",   f"s3://{S3_ANALYTICS}/hub-stats/hub_stats.json"),
        ("Hub stats bw",f"s3://{S3_ANALYTICS}/hub-stats/hub_stats_bws.json"),
        ("Sitemaps",    f"s3://{S3_SITEMAPS}/sitemap/_MANIFEST"),
    ]:
        freshness, file_date = check_s3_file_freshness(s3_path, today_str)
        if freshness == "fresh":
            lines.append(f"  {c(GREEN, '✓')} {label:<18} {c(DIM, file_date)}")
        elif freshness == "stale":
            lines.append(f"  {c(YELLOW, '⚠')} {label:<18} last modified {file_date} (not today)")
        else:
            lines.append(f"  {c(RED, '✗')} {label:<18} NOT FOUND")

    # ── API ──────────────────────────────────────────────────────────────────
    if check_api:
        lines.append("")
        lines.append("API")
        api_key = load_api_key()
        if not api_key:
            lines.append(c(DIM, "  (DPLA_API_KEY not found in ~/.dpla-secrets.env — skipping)"))
        else:
            print(c(DIM, "  Fetching API count..."), end="\r", flush=True)
            count = fetch_api_count(api_key)
            if count is not None:
                lines.append(f"  {c(GREEN, '✓')} api.dp.la total records: {c(BOLD, f'{count:,}')}")
            else:
                lines.append(f"  {c(YELLOW, '⚠')} Could not reach api.dp.la")

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    lines.append("")
    not_current = warning_count + stale_count + missing_count
    if not_current == 0 and exists:
        lines.append("SUMMARY: " + c(GREEN, f"all {total} hubs current (within {stale_days}d), post-indexer outputs fresh"))
    elif not_current == 0:
        lines.append("SUMMARY: " + c(YELLOW, f"all {total} hubs current but post-indexer outputs missing/stale"))
    else:
        lines.append("SUMMARY: " + c(YELLOW if not_current <= 3 else RED,
            f"{not_current}/{total} hubs overdue or missing"))

    lines.append("")
    return "\n".join(lines)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="DPLA pipeline status — ingest + index check")
    parser.add_argument("--month",      help="Month to check (YYYYMM). Defaults to current month.")
    parser.add_argument("--no-api",     action="store_true", help="Skip API count check")
    parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS,
                        help=f"Days before a snapshot is considered stale (default: {DEFAULT_STALE_DAYS})")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    if args.month:
        if not re.match(r"^\d{6}$", args.month):
            sys.exit("--month must be YYYYMM (6 digits)")
        now = datetime(int(args.month[:4]), int(args.month[4:]), 1, tzinfo=timezone.utc)

    month_str = now.strftime("%B %Y")  # "May 2026"

    print(c(DIM, f"\n  Loading hub list from s3://{S3_DATASET}/ ..."), end="\r", flush=True)

    # Discover hubs from S3
    try:
        ls = s3_ls(f"s3://{S3_DATASET}/", check=True)
    except RuntimeError as e:
        sys.exit(f"\n  [ERROR] Could not list s3://{S3_DATASET}/:\n  {e}")

    hubs = [
        line.strip().rstrip("/").split()[-1]
        for line in ls.splitlines()
        if line.strip().endswith("/")
    ]
    if not hubs:
        sys.exit(f"\n  [ERROR] No hubs found in s3://{S3_DATASET}/")

    print(c(DIM, f"  Checking {len(hubs)} hubs in parallel...      "), end="\r", flush=True)

    # Fetch all hub statuses in parallel
    hub_results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(latest_jsonl_snapshot, hub): hub for hub in hubs}
        done = 0
        for future in as_completed(futures):
            hub = futures[future]
            try:
                snapshot, date_str, records = future.result()
            except Exception:
                snapshot, date_str, records = None, None, None
            hub_results[hub] = (snapshot, date_str, records)
            done += 1
            print(c(DIM, f"  Checking hubs... {done}/{len(hubs)}          "), end="\r", flush=True)

    print(" " * 50, end="\r", flush=True)  # clear progress line
    print(render(hub_results, now, month_str, args.stale_days, not args.no_api))


if __name__ == "__main__":
    main()
