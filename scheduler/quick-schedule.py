#!/usr/bin/env python3
"""
quick-schedule.py - Quick view of which hubs need to be ingested for a specific month

Usage:
    python3 quick-schedule.py              # Show current month
    python3 quick-schedule.py 1            # Show January
    python3 quick-schedule.py jan          # Show January (abbreviation)
    python3 quick-schedule.py january      # Show January (full name)
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Month name mappings
MONTH_NAMES = {
    "january": 1, "jan": 1, "1": 1,
    "february": 2, "feb": 2, "2": 2,
    "march": 3, "mar": 3, "3": 3,
    "april": 4, "apr": 4, "4": 4,
    "may": 5, "5": 5,
    "june": 6, "jun": 6, "6": 6,
    "july": 7, "jul": 7, "7": 7,
    "august": 8, "aug": 8, "8": 8,
    "september": 9, "sep": 9, "9": 9,
    "october": 10, "oct": 10, "10": 10,
    "november": 11, "nov": 11, "11": 11,
    "december": 12, "dec": 12, "12": 12,
}

MONTH_DISPLAY = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

MONTH_ABBREV = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}


def parse_month_arg(arg: str) -> int:
    """Parse month argument (number, name, or abbreviation)."""
    arg_lower = arg.lower().strip()
    if arg_lower in MONTH_NAMES:
        return MONTH_NAMES[arg_lower]
    raise ValueError(f"Invalid month: {arg}. Use 1-12, month name, or abbreviation.")


def get_scheduled_hubs(schedule_file: Path, month: int) -> list:
    """Get list of hubs scheduled for the given month."""
    with open(schedule_file, 'r') as f:
        data = json.load(f)

    scheduled = []
    for hub in data.get('hubs', []):
        schedule = hub.get('schedule', {})
        months = schedule.get('months', [])
        if month in months:
            scheduled.append({
                'name': hub.get('name', ''),
                'provider_code': hub.get('provider_code', ''),
                'frequency': schedule.get('frequency', ''),
                'week': schedule.get('week', ''),
                'notes': schedule.get('notes', ''),
                'contacts': hub.get('contacts', [])
            })

    return scheduled


def main():
    script_dir = Path(__file__).parent
    schedule_file = script_dir / 'schedule.json'

    # Determine month
    if len(sys.argv) > 1:
        try:
            month = parse_month_arg(sys.argv[1])
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        month = datetime.now().month

    # Check if schedule file exists
    if not schedule_file.exists():
        print(f"Error: schedule.json not found at {schedule_file}", file=sys.stderr)
        sys.exit(1)

    # Get scheduled hubs
    hubs = get_scheduled_hubs(schedule_file, month)

    # Display results
    month_name = MONTH_DISPLAY[month]
    month_abbrev = MONTH_ABBREV[month]
    year = datetime.now().year

    print(f"\n{'='*70}")
    print(f"Hubs Scheduled for {month_name} {year}")
    print(f"{'='*70}\n")

    if not hubs:
        print(f"No hubs scheduled for {month_name}.")
        print(f"\nTo see all schedules, check: {schedule_file}")
        return

    print(f"Found {len(hubs)} hub(s):\n")

    for i, hub in enumerate(hubs, 1):
        print(f"{i}. {hub['name']}")
        print(f"   Provider Code: {hub['provider_code']}")
        print(f"   Frequency: {hub['frequency']}")
        if hub['week']:
            print(f"   Week: {hub['week']}")
        if hub['notes']:
            print(f"   Notes: {hub['notes']}")
        if hub['contacts']:
            contacts_str = ', '.join(hub['contacts'])
            print(f"   Contacts: {contacts_str}")
        print()

    print(f"{'='*70}")
    print(f"\nTo generate ingest commands, run:")
    print(f"  ./scheduler/schedule.sh")
    print(f"\nOr to ingest a specific hub:")
    for hub in hubs:
        print(f"  ./ingest.sh {hub['provider_code']}  # {hub['name']}")


if __name__ == "__main__":
    main()
