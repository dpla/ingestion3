#!/usr/bin/env python3
"""
Auto-update i3.conf harvest.endpoint for file-type hubs.

For each hub where harvest.type = "file" and the current endpoint is an S3
path, lists the bucket root to find the most recent dated delivery and updates
the endpoint in i3.conf if it has changed.

Endpoint types handled:
  - Dated directories: s3://bucket/20260619/     → finds latest PRE entry
  - Dated files:       s3://bucket/hub-2026-05-24.jsonl  → finds latest file

Designed to run on the GHA runner (not EC2) — reads i3.conf from a local
checkout of ingestion3-conf and writes changes back to that file.

Usage
-----
  python3 update_file_endpoints.py                          # dry run
  python3 update_file_endpoints.py --apply                  # write changes
  python3 update_file_endpoints.py --conf /path/to/i3.conf  # custom path
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
    env_file = HERE.parent / ".env"
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
    or ("dpla" if (HERE.parent / ".env").exists() else None)
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
    """Return {hub: {endpoint, bucket, is_dir}} for all file hubs with S3 endpoints.

    is_dir is True if the current endpoint ends with '/' (directory-type).
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
                hubs[hub]["is_dir"] = endpoint.endswith("/")

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


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Auto-update i3.conf endpoints for file-type hubs to latest S3 delivery."
    )
    parser.add_argument("--apply", action="store_true",
                        help="Write changes to i3.conf (default: dry run, print only)")
    parser.add_argument("--conf", default=DEFAULT_CONF,
                        help=f"Path to i3.conf (default: {DEFAULT_CONF})")
    args = parser.parse_args()

    conf_path = Path(args.conf)
    if not conf_path.exists():
        sys.exit(f"[bad] i3.conf not found at {conf_path}")

    header(f"File endpoint updater — {'APPLYING' if args.apply else 'DRY RUN'}")
    info(f"Conf: {conf_path}")

    conf_text = conf_path.read_text()
    file_hubs = parse_file_hubs(conf_text)

    if not file_hubs:
        info("No file-type hubs with S3 endpoints found.")
        sys.exit(0)

    info(f"{len(file_hubs)} file hub(s) found: {', '.join(sorted(file_hubs))}\n")

    changes = []
    errors = []

    for hub in sorted(file_hubs):
        data = file_hubs[hub]
        current = data["endpoint"]
        bucket = data["bucket"]
        is_dir = data["is_dir"]

        info(f"{hub}")
        info(f"  bucket:  s3://{bucket}/")
        info(f"  current: {current}")
        info(f"  type:    {'directory' if is_dir else 'file'}")

        try:
            dirs, files = list_s3_root(bucket)
        except RuntimeError as e:
            bad(f"  {e}")
            errors.append(hub)
            print()
            continue

        candidates = dirs if is_dir else files

        if not candidates:
            entry_type = "dated directories" if is_dir else "dated files"
            warn(f"  No {entry_type} found in s3://{bucket}/ — skipping")
            print()
            continue

        latest_entry = candidates[0]                # already sorted newest-first
        latest = f"s3://{bucket}/{latest_entry}"    # preserves trailing / for dirs

        # Extract the path component of current endpoint for date comparison
        current_path = current.rstrip("/").split("/", 3)[-1]  # e.g. "20260619" or "hub-2026-05-24.jsonl"
        current_date = _sortable_date(current_path)
        latest_date  = _sortable_date(latest_entry.rstrip("/"))

        if latest == current:
            ok(f"  already up to date")
        elif latest_date <= current_date:
            warn(f"  latest S3 delivery ({latest_entry.rstrip('/')}) is not newer than current ({current_path}) — skipping")
        else:
            info(f"  latest:  {latest}")
            changes.append((hub, current, latest))
            if args.apply:
                conf_text = update_endpoint_in_conf(conf_text, hub, latest)
                ok(f"  updated")
            else:
                info(f"  → would update to: {latest}")
        print()

    # Write file if applying
    if args.apply and changes:
        conf_path.write_text(conf_text)
        print(f"  Wrote {len(changes)} change(s) to {conf_path}")

    # Summary
    header("Summary")
    if changes:
        for hub, old, new in changes:
            status = "updated" if args.apply else "would update"
            print(f"  {hub}: {status}")
            print(f"    {old}")
            print(f"    → {new}")
    else:
        info("No endpoint changes needed.")

    if errors:
        bad(f"Errors on: {', '.join(errors)}")
        sys.exit(1)

    if not args.apply and changes:
        info("\nRe-run with --apply to write changes.")


if __name__ == "__main__":
    main()
