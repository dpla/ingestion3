#!/usr/bin/env python3
"""
Show which S3 delivery each file hub's next harvest would read.

File hubs (harvest.type = "file" with an s3:// endpoint) don't need their
i3.conf endpoint kept current: at harvest time, ingest.sh and harvest.sh pick
the newest dated delivery in the bucket the endpoint names
(helper_scripts/resolve_file_delivery.py). This report lists, per hub, that
newest delivery. It changes nothing.

Deliveries can be dated folders (s3://bucket/20260619/) or dated files
(s3://bucket/hub-2026-05-24.jsonl); folders and files are compared by the date
in their name.

The bucket-listing and date helpers here are shared with
resolve_file_delivery.py.

Usage
-----
  python3 helper_scripts/file_deliveries.py                          # all file hubs
  python3 helper_scripts/file_deliveries.py --month 11               # hubs scheduled in November
  python3 helper_scripts/file_deliveries.py --conf /path/to/i3.conf  # custom path
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

# ── config ────────────────────────────────────────────────────────────────────

HERE = Path(__file__).resolve().parent

def _load_env():
    cfg = {}
    env_file = HERE.parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg

_env = _load_env()

AWS_PROFILE = (
    os.environ.get("AWS_PROFILE")
    or _env.get("AWS_PROFILE")
    or ("dpla" if (HERE.parent.parent / ".env").exists() else None)
)
REGION = "us-east-1"

DEFAULT_CONF = os.environ.get("I3_CONF") or str(
    Path(_env.get("INGESTION3_CONF_REPO",
                   os.path.expanduser("~/Documents/Repos/ingestion3-conf"))) / "i3.conf"
)

# ── output helpers ─────────────────────────────────────────────────────────────

def info(msg):  print(f"  {msg}", flush=True)
def ok(msg):    print(f"  ✓ {msg}", flush=True)
def warn(msg):  print(f"  ⚠ {msg}", flush=True)
def bad(msg):   print(f"  ✗ {msg}", flush=True)

def header(msg):
    bar = "═" * 60
    print(f"\n{bar}", flush=True)
    print(f"  {msg}", flush=True)
    print(f"{bar}", flush=True)

# ── AWS helpers ───────────────────────────────────────────────────────────────

def _profile_args():
    return ["--profile", AWS_PROFILE] if AWS_PROFILE else []


# Date patterns we recognize in S3 delivery names.
# Handles: YYYY-MM-DD, YYYYMMDD, MMDDYYYY, and file names containing these.
_DATE_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}"            # YYYY-MM-DD
    r"|(?:19|20)\d{6}"              # YYYYMMDD (starts 19xx or 20xx)
    r"|\d{4}(?:19|20)\d{2}"         # MMDDYYYY (ends 19xx or 20xx)
)


def _sortable_date(entry):
    """Return a YYYYMMDD string from a delivery name for sort (newest-first)."""
    # YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", entry)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    # YYYYMMDD
    m = re.search(r"((?:19|20)\d{6})", entry)
    if m:
        s = m.group(1)
        if 1900 <= int(s[:4]) <= 2099:
            return s
    # MMDDYYYY
    m = re.search(r"(\d{4}(?:19|20)\d{2})", entry)
    if m:
        s = m.group(1)
        return s[4:] + s[:4]
    return entry


def list_s3_root(bucket):
    """List top-level entries in s3://bucket/.

    Returns two lists, each sorted newest-first:
      dirs:   names with trailing '/'  e.g. ['20260901/', '20260619/']
              sorted by date embedded in name (PRE lines carry no timestamp)
      files:  names without trailing slash  e.g. ['hub-2026-05-24.jsonl']
              sorted by S3 LastModified timestamp so mis-named files sort correctly

    Only entries that contain a recognizable date in their name are returned.
    """
    r = subprocess.run(
        ["aws", "s3", "ls", f"s3://{bucket}/", "--region", REGION] + _profile_args(),
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"aws s3 ls s3://{bucket}/ failed: {r.stderr.strip()}")

    dirs = []
    files = []  # list of (last_modified_str, name)

    for line in r.stdout.splitlines():
        # Directory prefix: "                           PRE 20260619/"
        # No timestamp available for PRE entries — fall back to name-based sort.
        m = re.search(r"\bPRE\s+(.+?)/\s*$", line)
        if m:
            name = m.group(1) + "/"
            if _DATE_RE.search(name):
                dirs.append(name)
            continue
        # Object: "2026-06-19 14:23:05   4321 hub-2026-05-24.jsonl"
        m = re.search(
            r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\d+\s+(\S+)\s*$", line
        )
        if m:
            last_modified = m.group(1)  # "2026-06-19 14:23:05" — lexically sortable
            name = m.group(2)
            if _DATE_RE.search(name):
                files.append((last_modified, name))

    dirs.sort(key=_sortable_date, reverse=True)
    files.sort(key=lambda t: t[0], reverse=True)   # sort by S3 LastModified
    return dirs, [name for _, name in files]


# ── i3.conf parsing ───────────────────────────────────────────────────────────

def parse_file_hubs(conf_text):
    """Return {hub: {endpoint, bucket, months}} for all file hubs with S3 endpoints.

    months is the list of scheduled months from schedule.months (empty if not set).
    """
    # Strip comments
    text = re.sub(r"(?m)^\s*(#|//).*$", "", conf_text)

    hubs = {}

    for m in re.finditer(
        r"""^\s*([a-z0-9_-]+)\.harvest\.type\s*[=:]\s*["']file["']""",
        text, re.MULTILINE | re.IGNORECASE,
    ):
        hubs.setdefault(m.group(1).lower(), {})

    for m in re.finditer(
        r"""^\s*([a-z0-9_-]+)\.harvest\.endpoint\s*[=:]\s*["'](s3://[^"']+)["']""",
        text, re.MULTILINE | re.IGNORECASE,
    ):
        hub = m.group(1).lower()
        if hub in hubs:
            endpoint = m.group(2)  # preserve trailing slash exactly as in conf
            bucket_match = re.match(r"s3://([^/]+)", endpoint)
            if bucket_match:
                hubs[hub]["endpoint"] = endpoint
                hubs[hub]["bucket"] = bucket_match.group(1)

    for m in re.finditer(
        r"""^\s*([a-z0-9_-]+)\.schedule\.months\s*[=:]\s*\[([0-9,\s]+)\]""",
        text, re.MULTILINE | re.IGNORECASE,
    ):
        hub = m.group(1).lower()
        if hub in hubs:
            hubs[hub]["months"] = [
                int(x.strip()) for x in m.group(2).split(",") if x.strip().isdigit()
            ]

    # Only return hubs that have both type=file and an S3 endpoint
    return {h: v for h, v in hubs.items() if "bucket" in v}


def update_endpoint_in_conf(conf_text, hub, new_endpoint):
    """Replace the harvest.endpoint for hub in conf_text; return updated text."""
    pattern = (
        rf"""(^\s*{re.escape(hub)}\.harvest\.endpoint\s*[=:]\s*)"""
        rf"""(["'])([^"']+)(["'])"""
    )
    new_text, count = re.subn(
        pattern,
        lambda mo: mo.group(1) + mo.group(2) + new_endpoint + mo.group(4),
        conf_text,
        flags=re.MULTILINE | re.IGNORECASE,
    )
    if count == 0:
        raise RuntimeError(f"Could not find {hub}.harvest.endpoint in i3.conf to update")
    return new_text


def newest_delivery(bucket):
    """Return (entry, YYYYMMDD) for the newest dated delivery in the bucket,
    folder or file, or (None, None) if there is none.

    Folders and files are compared by the date in their name. Raises
    RuntimeError if the bucket can't be listed.
    """
    dirs, files = list_s3_root(bucket)
    candidates = dirs + files
    if not candidates:
        return None, None
    best = max(candidates, key=lambda e: _sortable_date(e.rstrip("/")))
    return best, _sortable_date(best.rstrip("/"))


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Show the newest S3 delivery each file hub's next harvest would read."
    )
    parser.add_argument("--conf", default=DEFAULT_CONF,
                        help=f"Path to i3.conf (default: {DEFAULT_CONF})")
    parser.add_argument("--month", type=int, metavar="N",
                        help="Only hubs scheduled for month N (1-12). Omit for all file hubs.")
    args = parser.parse_args()

    if args.month is not None and not (1 <= args.month <= 12):
        sys.exit(f"[bad] --month must be 1-12, got {args.month}")

    conf_path = Path(args.conf)
    if not conf_path.exists():
        sys.exit(f"[bad] i3.conf not found at {conf_path}")

    month_label = f" (month {args.month} only)" if args.month else ""
    header(f"Newest file hub deliveries{month_label}")
    info(f"Conf: {conf_path}")

    file_hubs = parse_file_hubs(conf_path.read_text())
    if args.month:
        file_hubs = {h: v for h, v in file_hubs.items() if args.month in v.get("months", [])}
    if not file_hubs:
        info("No file hubs with S3 endpoints" + (f" scheduled for month {args.month}." if args.month else "."))
        sys.exit(0)

    errors = []
    print()
    for hub in sorted(file_hubs):
        bucket = file_hubs[hub]["bucket"]
        try:
            entry, date = newest_delivery(bucket)
        except RuntimeError as e:
            bad(f"{hub:<12} {e}")
            errors.append(hub)
            continue
        if entry:
            ok(f"{hub:<12} s3://{bucket}/{entry}  ({date})")
        else:
            warn(f"{hub:<12} no dated delivery in s3://{bucket}/ (the harvest would fail)")

    print()
    info("Whether an older delivery was already harvested is checked at harvest time.")
    if errors:
        bad(f"Errors on: {', '.join(errors)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
