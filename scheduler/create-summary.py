#!/usr/bin/env python3
"""
create-summary.py - Create a human-readable summary of collected hub data
"""

import json
from collections import defaultdict

def main():
    with open('scheduler/hub-data-raw.json', 'r') as f:
        hubs = json.load(f)

    summary_lines = []
    summary_lines.append("=" * 80)
    summary_lines.append("HUB DATA COLLECTION SUMMARY")
    summary_lines.append("=" * 80)
    summary_lines.append(f"\nTotal Hubs: {len(hubs)}\n")

    # Statistics
    hubs_with_contacts = sum(1 for h in hubs if h.get('contacts'))
    hubs_with_schedules = sum(1 for h in hubs if h.get('schedule', {}).get('frequency'))
    total_contacts = sum(len(h.get('contacts', [])) for h in hubs)

    summary_lines.append(f"Statistics:")
    summary_lines.append(f"  - Hubs with contacts: {hubs_with_contacts}/{len(hubs)}")
    summary_lines.append(f"  - Hubs with schedules: {hubs_with_schedules}/{len(hubs)}")
    summary_lines.append(f"  - Total contact emails: {total_contacts}")
    summary_lines.append("")

    # Group by schedule frequency
    by_frequency = defaultdict(list)
    for hub in hubs:
        freq = hub.get('schedule', {}).get('frequency', 'unspecified')
        by_frequency[freq].append(hub)

    summary_lines.append("Hubs by Schedule Frequency:")
    summary_lines.append("-" * 80)
    for freq in sorted(by_frequency.keys()):
        count = len(by_frequency[freq])
        summary_lines.append(f"  {freq or 'unspecified'}: {count} hub(s)")
    summary_lines.append("")

    # Detailed hub information
    summary_lines.append("=" * 80)
    summary_lines.append("DETAILED HUB INFORMATION")
    summary_lines.append("=" * 80)
    summary_lines.append("")

    for i, hub in enumerate(hubs, 1):
        summary_lines.append(f"{i}. {hub['name']}")
        summary_lines.append(f"   Provider Code: {hub.get('provider_code', 'N/A')}")
        summary_lines.append(f"   Dashboard: {hub['dashboard_url']}")

        contacts = hub.get('contacts', [])
        if contacts:
            summary_lines.append(f"   Contacts ({len(contacts)}):")
            for email in contacts[:5]:  # Limit to first 5
                summary_lines.append(f"     - {email}")
            if len(contacts) > 5:
                summary_lines.append(f"     ... and {len(contacts) - 5} more")
        else:
            summary_lines.append("   Contacts: None found")

        schedule = hub.get('schedule', {})
        if schedule.get('frequency'):
            months = schedule.get('months', [])
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_str = ', '.join(month_names.get(m, str(m)) for m in months) if months else 'N/A'
            summary_lines.append(f"   Schedule: {schedule['frequency']} - Months: {month_str}")
            if schedule.get('week'):
                summary_lines.append(f"   Week: {schedule['week']}")
            if schedule.get('notes'):
                summary_lines.append(f"   Notes: {schedule['notes']}")
        else:
            summary_lines.append("   Schedule: Not specified")

        summary_lines.append("")

    # Write summary
    summary_text = '\n'.join(summary_lines)
    with open('scheduler/hub-data-summary.txt', 'w') as f:
        f.write(summary_text)

    print(summary_text)
    print(f"\n\nSummary saved to: scheduler/hub-data-summary.txt")

if __name__ == "__main__":
    main()
