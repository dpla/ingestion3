#!/usr/bin/env python3
"""
Watch an OAI harvest log file and report set-by-set progress.

Usage:
  ./venv/bin/python scripts/watch-oai-harvest.py --log=<path> --total=<n>
  ./venv/bin/python scripts/watch-oai-harvest.py --log=<path> --conf=<i3.conf> --hub=<hub>

Detects set transitions in lines like:
  ... OaiMultiPageResponseBuilder - Loading page ...?set=<setname>&...
When a new set appears, the previous set is considered complete.
Prints a status line per completion, and a 30-minute summary.
"""

import argparse
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


SET_RE = re.compile(r'\?verb=ListRecords&set=([^&\s]+)&')
HARVEST_DONE_RE = re.compile(r'Harvest completed|HarvestEntry.*completed|Step 2', re.IGNORECASE)


def parse_total_from_conf(conf_path: str, hub: str) -> int:
    """Count sets in i3.conf setlist for the given hub."""
    text = Path(conf_path).read_text()
    m = re.search(rf'{re.escape(hub)}\.harvest\.setlist\s*=\s*"([^"]+)"', text)
    if not m:
        return 0
    return len(m.group(1).split(','))


def fmt_elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


def watch(log_path: str, total: int, poll: float = 2.0, summary_interval: int = 1800):
    path = Path(log_path)
    seen_sets = []       # ordered list of set names as they first appear
    current_set = None
    completed = []       # (set_name, timestamp, records_hint)
    start_time = datetime.now()
    last_summary = start_time
    offset = 0
    done = False

    print(f"[{datetime.now():%H:%M:%S}] Watching: {log_path}")
    print(f"[{datetime.now():%H:%M:%S}] Total sets: {total}")
    print(f"[{datetime.now():%H:%M:%S}] Summary interval: every {summary_interval // 60} minutes")
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
            # Detect harvest completion
            if HARVEST_DONE_RE.search(line):
                if current_set and current_set not in [s for s, _ in completed]:
                    completed.append((current_set, datetime.now()))
                    n = len(completed)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    print(f"[{datetime.now():%H:%M:%S}] ✓ Set {n}/{total}: {current_set}  (elapsed: {fmt_elapsed(elapsed)})")
                    sys.stdout.flush()
                done = True
                break

            m = SET_RE.search(line)
            if not m:
                continue
            set_name = m.group(1)

            if set_name == current_set:
                continue  # still paginating same set

            # New set started → previous set is complete
            if current_set is not None and current_set not in [s for s, _ in completed]:
                completed.append((current_set, datetime.now()))
                n = len(completed)
                elapsed = (datetime.now() - start_time).total_seconds()
                pct = (n / total * 100) if total else 0
                remaining = ""
                if n > 1:
                    avg = elapsed / n
                    eta_secs = avg * (total - n)
                    remaining = f"  ETA: ~{fmt_elapsed(eta_secs)}"
                print(f"[{datetime.now():%H:%M:%S}] ✓ Set {n}/{total} ({pct:.1f}%): {current_set}{remaining}")
                sys.stdout.flush()

            current_set = set_name
            if set_name not in seen_sets:
                seen_sets.append(set_name)

        # 30-minute summary
        now = datetime.now()
        if (now - last_summary).total_seconds() >= summary_interval:
            elapsed = (now - start_time).total_seconds()
            n = len(completed)
            pct = (n / total * 100) if total else 0
            avg = elapsed / n if n else 0
            eta = fmt_elapsed(avg * (total - n)) if n else "unknown"
            print()
            print(f"{'=' * 60}")
            print(f"  30-MINUTE SUMMARY  [{now:%H:%M:%S}]")
            print(f"  Sets complete: {n}/{total} ({pct:.1f}%)")
            print(f"  Elapsed:       {fmt_elapsed(elapsed)}")
            print(f"  Avg per set:   {fmt_elapsed(avg)}")
            print(f"  ETA remaining: ~{eta}")
            print(f"{'=' * 60}")
            print()
            sys.stdout.flush()
            last_summary = now

        if not done:
            time.sleep(poll)

    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    n = len(completed)
    print()
    print(f"{'=' * 60}")
    print(f"  HARVEST COMPLETE  [{datetime.now():%H:%M:%S}]")
    print(f"  Sets processed: {n}/{total}")
    print(f"  Total elapsed:  {fmt_elapsed(elapsed)}")
    print(f"{'=' * 60}")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Watch OAI harvest log for set-by-set progress")
    parser.add_argument("--log", required=True, help="Path to harvest log file")
    parser.add_argument("--total", type=int, default=0, help="Total number of sets")
    parser.add_argument("--conf", default=None, help="Path to i3.conf (to count sets)")
    parser.add_argument("--hub", default=None, help="Hub name (used with --conf)")
    parser.add_argument("--poll", type=float, default=2.0, help="Poll interval in seconds")
    parser.add_argument("--summary-interval", type=int, default=1800, help="Summary interval in seconds (default: 1800 = 30min)")
    args = parser.parse_args()

    total = args.total
    if not total and args.conf and args.hub:
        total = parse_total_from_conf(args.conf, args.hub)
        print(f"Parsed {total} sets from {args.conf} for hub '{args.hub}'")

    watch(args.log, total, poll=args.poll, summary_interval=args.summary_interval)


if __name__ == "__main__":
    main()
