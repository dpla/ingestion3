#!/usr/bin/env python3
"""
Copy a Community Webs delivery to S3, ready for launch_cw.py.

Internet Archive delivers a SQLite file. This checks that it's readable, shows
its record count next to the previous one, and copies it to
s3://dpla-hub-community-webs/<timestamp>-community-webs.db.

Usage:
    python3 copy_cw.py ~/Downloads/community-webs.sqlite3

Then run launch_cw.py, which uses the newest copied file.
"""

import argparse
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from datetime import datetime

from launch_cw import AWS_PROFILE, S3_STAGING, previous_count


def count_records(db_path):
    """Count ait rows so a bad file fails before anything is uploaded."""
    try:
        uri = Path(db_path).resolve().as_uri() + "?mode=ro"  # as_uri percent-encodes the path
        with sqlite3.connect(uri, uri=True) as conn:
            return conn.execute("SELECT count(*) FROM ait").fetchone()[0]
    except sqlite3.Error as e:
        sys.exit(f"{db_path} is not a readable Community Webs export ({e}).")


def main():
    parser = argparse.ArgumentParser(description="Copy a Community Webs delivery to S3")
    parser.add_argument("db", help="Path to the .db/.sqlite3 file from Internet Archive")
    args = parser.parse_args()

    db_path = os.path.expanduser(args.db)
    if not os.path.isfile(db_path):
        sys.exit(f".db file not found: {db_path}")

    size_mb = os.path.getsize(db_path) / 1024 / 1024
    records = count_records(db_path)
    if records == 0:
        sys.exit(f"{db_path} has no records in the ait table; nothing copied.")
    prev = previous_count()
    print(f"  File:     {db_path} ({size_mb:.1f} MB)")
    print(f"  Records:  {records:,} (previous in S3: {f'{prev:,}' if prev is not None else 'unknown'})")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_dest = f"{S3_STAGING}/{timestamp}-community-webs.db"
    result = subprocess.run(
        ["aws", "s3", "cp", db_path, s3_dest, "--no-progress"]
        + (["--profile", AWS_PROFILE] if AWS_PROFILE else []),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"S3 upload failed: {result.stderr.strip()}")

    print(f"  ✓ Copied → {s3_dest}")
    print("\n  Next: python3 launch_cw.py")


if __name__ == "__main__":
    main()
