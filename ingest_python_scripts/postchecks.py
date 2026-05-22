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

from hub_preflight import list_scheduled_hubs

# ---------- config ----------
REGION        = "us-east-1"
S3_DATASET    = "dpla-master-dataset"
S3_EXPORT     = "dpla-provider-export"
S3_ANALYTICS  = "dashboard-analytics"
S3_SITEMAPS   = "sitemaps.dp.la"
DEFAULT_STALE_DAYS = 30

# Load user-specific paths from ingestion3/.env (one level up from this script)
def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    )
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    return cfg

_env = _load_dotenv()
_conf_repo = _env.get("INGESTION3_CONF_REPO",
                       os.path.expanduser("~/Documents/Repos/ingestion3-conf"))
CONF_PATH = os.environ.get("I3_CONF") or os.path.join(_conf_repo, "i3.conf")

# S3 prefix aliases (hub name → S3 prefix, matching common.sh's resolve_s3_prefix)
S3_ALIASES = {
    "hathi": "hathitrust",
    "tn":    "tennessee",
}


# ---------- AWS helpers ----------
def aws(args, check=True):
    profile = [] if any(a.startswith("--profile") for a in args) else ["--profile", "dpla"]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
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

    aws s3 ls of a prefix returns lines like:
      "                       PRE 20260502_120000-hub-MAP4_0.jsonl/"
    There is no date column on PRE lines, so we parse the date from the
    directory name itself (YYYYMMDD_HHMMSS prefix).
    """
    s3_prefix = S3_ALIASES.get(hub, hub)
    ls = s3_ls(f"s3://{S3_DATASET}/{s3_prefix}/jsonl/")
    if not ls.strip():
        return None, None, None

    snapshot_dirs = []
    for line in ls.splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        dir_name = parts[-1].rstrip("/")
        m = re.match(r"^(\d{4})(\d{2})(\d{2})_\d{6}", dir_name)
        if m:
            date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            snapshot_dirs.append((date_str, dir_name))

    if not snapshot_dirs:
        return None, None, None

    snapshot_dirs.sort(reverse=True)
    date_str, dir_name = snapshot_dirs[0]

    manifest = s3_read(f"s3://{S3_DATASET}/{s3_prefix}/jsonl/{dir_name}/_MANIFEST")
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


# ---------- hub list from conf ----------
def get_hubs_from_conf(month_number, conf_path=CONF_PATH):
    """
    Return sorted list of active hub names scheduled for month_number (1–12).
    Delegates to hub_preflight.list_scheduled_hubs() — single source of truth.
    """
    hubs = list_scheduled_hubs(month_number, conf_path)
    if hubs is None:
        return []
    return sorted(
        h["hub"] for h in hubs
        if (h["status"] or "active").lower() == "active"
    )


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
        import urllib.request
        req = urllib.request.Request(
            "https://api.dp.la/v2/items?page_size=0",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data.get("count")
    except Exception:
        return None


# ---------- render ----------
def render(hub_results, today, check_dt, month_str, stale_days, check_api):
    """
    today     — actual current date (for age calculations)
    check_dt  — month being checked (for post-indexer S3 paths)
    """
    lines = []
    year, month = check_dt.year, f"{check_dt.month:02d}"

    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  DPLA Postchecks — {c(BOLD, month_str)}")
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
        age_class, age_days = classify_age(date_str, today, stale_days)

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

    # Hub stats + sitemaps — just show last-modified date
    for label, s3_path in [
        ("Hub stats",    f"s3://{S3_ANALYTICS}/hub-stats/hub_stats.json"),
        ("Hub stats bw", f"s3://{S3_ANALYTICS}/hub-stats/hub_stats_bws.json"),
        ("Sitemaps",     f"s3://{S3_SITEMAPS}/sitemap/_MANIFEST"),
    ]:
        ls = s3_ls(s3_path)
        if ls.strip():
            file_date = ls.strip().split()[0]
            lines.append(f"  {c(GREEN, '✓')} {label:<18} {c(DIM, file_date)}")
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

    today = datetime.now(timezone.utc)       # real today — used for age calculations
    if args.month:
        if not re.match(r"^\d{6}$", args.month):
            sys.exit("--month must be YYYYMM (6 digits)")
        check_dt = datetime(int(args.month[:4]), int(args.month[4:]), 1, tzinfo=timezone.utc)
    else:
        check_dt = today

    month_str = check_dt.strftime("%B %Y")  # "May 2026"

    print(c(DIM, f"\n  Loading hub list from i3.conf ({check_dt.strftime('%B')} schedule)..."),
          end="\r", flush=True)

    # Discover hubs scheduled for this month from i3.conf
    hubs = get_hubs_from_conf(check_dt.month)
    if not hubs:
        sys.exit(
            f"\n  [ERROR] No hubs found for month {check_dt.month} in i3.conf.\n"
            f"  Conf path: {CONF_PATH}\n"
            f"  Set I3_CONF=/path/to/i3.conf if it lives elsewhere."
        )

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
    print(render(hub_results, today, check_dt, month_str, args.stale_days, not args.no_api))


if __name__ == "__main__":
    main()
