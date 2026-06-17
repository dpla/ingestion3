#!/usr/bin/env python3
"""
Monthly pre-ingest hub checklist.

Lists all hubs scheduled for the current (or given) month from i3.conf,
with harvest types, special-case notes, and on-hold status. Use this at
the start of each ingest month to plan the run order.

For per-hub pre-flight checks (EC2 state, repo sync, JAR, endpoint),
use hub_preflight.py.

Usage:
    python3 pre_ingest_check.py              # current month
    python3 pre_ingest_check.py --month 5    # specific month
"""

import argparse
import sys
from datetime import datetime

from hub_preflight import CONF_PATH, MONTH_NAMES, SPECIAL_CASE_NOTES, list_scheduled_hubs


def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly pre-ingest hub checklist")
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        metavar="MONTH",
        help="Month number (1-12). Defaults to the current month.",
    )
    args = parser.parse_args()

    month = args.month or datetime.now().month
    if not (1 <= month <= 12):
        sys.exit(f"Invalid month: {month}. Use 1-12.")

    hubs = list_scheduled_hubs(month)
    if hubs is None:
        sys.exit(f"Could not read conf at {CONF_PATH}")

    label = f"{MONTH_NAMES[month]} (month {month})"
    print(f"\nHubs scheduled for {label}:  {len(hubs)} total")
    print(f"Conf: {CONF_PATH}\n")

    if not hubs:
        print("  (no hubs scheduled this month)\n")
        return

    active = [h for h in hubs if (h["status"] or "active").lower() == "active"]
    on_hold = [h for h in hubs if h not in active]

    if active:
        print(f"  ACTIVE ({len(active)})")
        for h in active:
            note = SPECIAL_CASE_NOTES.get(h["hub"], "")
            note_str = f"   ⚠  {note}" if note else ""
            print(f"    {h['hub']:<20} {h['harvest_type']:<10}{note_str}")
        print()

    if on_hold:
        print(f"  ON-HOLD ({len(on_hold)})")
        for h in on_hold:
            print(f"    {h['hub']:<20} {h['harvest_type']:<10}   {h['status']}")
        print()


if __name__ == "__main__":
    main()
