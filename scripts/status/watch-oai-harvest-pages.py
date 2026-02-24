#!/usr/bin/env python3
"""
Watch an OAI harvest log file for a single-endpoint (no setlist) harvest
and report page-by-page progress with record counts from resumption tokens.

Detects lines like:
  ... OaiMultiPageResponseBuilder - Loading page ...?verb=ListRecords&...
  ... resumptionToken=TOKEN:MODS:OFFSET:TOTAL::  (offset/total embedded in token)

Usage:
  ./venv/bin/python scripts/status/watch-oai-harvest-pages.py --log=<path> --hub=<hub>
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path


# resumptionToken=...:OFFSET:TOTAL:: embedded in URL
TOKEN_RE = re.compile(r'resumptionToken=[^:]+:[^:]+:[^:]+:(\d+):(\d+)::')
# First page (no resumption token, just set= or plain ListRecords)
FIRST_PAGE_RE = re.compile(r'Loading page .+\?verb=ListRecords(?!.*resumptionToken)')
HARVEST_DONE_RE = re.compile(r'Harvest completed|HarvestEntry.*completed|Step 2', re.IGNORECASE)


def fmt_elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


def watch(log_path: str, hub: str, poll: float = 3.0, summary_interval: int = 1800):
    path = Path(log_path)
    start_time = datetime.now()
    last_summary = start_time
    offset = 0
    page = 0
    total_records = None
    current_offset = 0
    done = False

    print(f"[{datetime.now():%H:%M:%S}] [{hub}] Watching: {log_path}")
    print(f"[{datetime.now():%H:%M:%S}] [{hub}] Single-endpoint harvest (no setlist) — tracking by page/record count")
    print(f"[{datetime.now():%H:%M:%S}] [{hub}] Summary interval: every {summary_interval // 60} minutes")
    print("-" * 60)
    sys.stdout.flush()

    while not done:
        try:
            text = path.read_text(errors='replace')
        except FileNotFoundError:
            time.sleep(poll)
            continue

        lines = text.splitlines()
        new_lines = lines[offset:]
        offset = len(lines)

        for line in new_lines:
            if HARVEST_DONE_RE.search(line):
                elapsed = (datetime.now() - start_time).total_seconds()
                pct = f"{current_offset / total_records * 100:.1f}%" if total_records else "?"
                print(f"[{datetime.now():%H:%M:%S}] [{hub}] ✓ Harvest complete — {current_offset:,}/{total_records or '?':,} records ({pct}) in {fmt_elapsed(elapsed)}")
                done = True
                break

            m = TOKEN_RE.search(line)
            if m:
                page += 1
                current_offset = int(m.group(1))
                total_records = int(m.group(2))
                elapsed = (datetime.now() - start_time).total_seconds()
                pct = current_offset / total_records * 100 if total_records else 0
                avg_per_page = elapsed / page if page else 0
                pages_left = ((total_records - current_offset) / 100) if total_records else 0
                eta = fmt_elapsed(avg_per_page * pages_left) if avg_per_page and pages_left else "?"
                print(f"[{datetime.now():%H:%M:%S}] [{hub}] Page {page:4d} — {current_offset:>7,}/{total_records:,} records ({pct:.1f}%)  ETA: ~{eta}")
                sys.stdout.flush()
                continue

            if FIRST_PAGE_RE.search(line):
                page += 1
                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"[{datetime.now():%H:%M:%S}] [{hub}] Page   1 — fetching first page... (elapsed: {fmt_elapsed(elapsed)})")
                sys.stdout.flush()

        # 30-minute summary
        now = datetime.now()
        if (now - last_summary).total_seconds() >= summary_interval:
            elapsed = (now - start_time).total_seconds()
            pct = f"{current_offset / total_records * 100:.1f}%" if total_records else "?"
            print()
            print(f"{'=' * 60}")
            print(f"  30-MINUTE SUMMARY [{hub}]  [{now:%H:%M:%S}]")
            print(f"  Records fetched: {current_offset:,}/{total_records or '?':,} ({pct})")
            print(f"  Pages fetched:   {page}")
            print(f"  Elapsed:         {fmt_elapsed(elapsed)}")
            print(f"{'=' * 60}")
            print()
            sys.stdout.flush()
            last_summary = now

        if not done:
            time.sleep(poll)

    elapsed = (datetime.now() - start_time).total_seconds()
    print()
    print(f"{'=' * 60}")
    print(f"  HARVEST COMPLETE [{hub}]  [{datetime.now():%H:%M:%S}]")
    print(f"  Total pages:    {page}")
    print(f"  Total records:  {total_records:,}" if total_records else f"  Total records:  {current_offset:,}")
    print(f"  Total elapsed:  {fmt_elapsed(elapsed)}")
    print(f"{'=' * 60}")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Watch single-endpoint OAI harvest log for page/record progress")
    parser.add_argument("--log", required=True, help="Path to harvest log file")
    parser.add_argument("--hub", required=True, help="Hub name (for display)")
    parser.add_argument("--poll", type=float, default=3.0, help="Poll interval in seconds")
    parser.add_argument("--summary-interval", type=int, default=1800, help="Summary interval in seconds (default: 1800 = 30min)")
    args = parser.parse_args()
    watch(args.log, args.hub, poll=args.poll, summary_interval=args.summary_interval)


if __name__ == "__main__":
    main()
