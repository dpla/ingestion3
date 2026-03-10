#!/usr/bin/env python3
"""
parse-schedules.py - Parse schedule information from the main schedule page
"""

import json
import re
import urllib.request

# Schedule information extracted from the main schedule page HTML
# Format: "Hub Name": {"frequency": "...", "months": [1,2,3], "week": "last", "notes": "..."}
SCHEDULE_DATA = {
    "Plains to Peaks": {
        "frequency": "quarterly",
        "months": [10, 1, 4, 7],
        "week": "last",
        "notes": "Wikimedia Partner"
    },
    "Connecticut Digital Archive": {
        "frequency": "bi-annually",
        "months": [3, 9],
        "week": "",
        "notes": ""
    },
    "District Digital": {
        "frequency": "",
        "months": [],
        "week": "",
        "notes": "On hold until platform migration finished (End of 2024)"
    },
    "Sunshine State Digital Network": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "Digital Library of Georgia": {
        "frequency": "quarterly",
        "months": [12, 3, 6, 9],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "Illinois Digital Heritage": {
        "frequency": "quarterly",
        "months": [1, 4, 7, 10],
        "week": "",
        "notes": "Always send i3 summary and logs"
    },
    "Indiana Memory": {
        "frequency": "quarterly",
        "months": [3, 6, 9, 12],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "Digital Maine": {
        "frequency": "as-needed",
        "months": [],
        "week": "",
        "notes": ""
    },
    "Digital Maryland": {
        "frequency": "quarterly",
        "months": [3, 6, 9, 12],
        "week": "",
        "notes": ""
    },
    "Digital Commonwealth": {
        "frequency": "quarterly",
        "months": [10, 1, 4, 7],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "Michigan Service Hub": {
        "frequency": "quarterly",
        "months": [10, 1, 4, 7],
        "week": "",
        "notes": ""
    },
    "MDL": {
        "frequency": "bi-monthly",
        "months": [11, 1, 3, 5, 7, 8],
        "week": "",
        "notes": "Minnesota Digital Library + South Dakota"
    },
    "Heartland Hub": {
        "frequency": "quarterly",
        "months": [3, 6, 9, 12],
        "week": "",
        "notes": ""
    },
    "New Jersey / Delaware Collective": {
        "frequency": "quarterly",
        "months": [10, 1, 4, 7],
        "week": "",
        "notes": ""
    },
    "Ohio Digital Network": {
        "frequency": "quarterly",
        "months": [12, 3, 6, 9],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "OKHub": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "Mountain West Digital Library": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "NCDHC": {
        "frequency": "bi-annually",
        "months": [1, 7],
        "week": "",
        "notes": ""
    },
    "Northwest Digital Heritage": {
        "frequency": "quarterly",
        "months": [1, 4, 7, 10],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "PA Digital": {
        "frequency": "quarterly",
        "months": [12, 3, 6, 9],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "South Carolina Digital Library": {
        "frequency": "bi-annually",
        "months": [2, 8],
        "week": "",
        "notes": ""
    },
    "Digital Library of Tennessee": {
        "frequency": "quarterly",
        "months": [12, 6],
        "week": "",
        "notes": "Non-member ingests"
    },
    "Portal to Texas History": {
        "frequency": "bi-monthly",
        "months": [10, 12, 2, 4, 6, 8],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "Texas Digital Library": {
        "frequency": "quarterly",
        "months": [1, 4, 7, 10],
        "week": "",
        "notes": ""
    },
    "Green Mountain Digital Archive": {
        "frequency": "quarterly",
        "months": [2, 5, 8, 11],
        "week": "",
        "notes": ""
    },
    "Digital Virginias": {
        "frequency": "bi-annually",
        "months": [6, 12],
        "week": "",
        "notes": ""
    },
    "Recollection Wisconsin": {
        "frequency": "quarterly",
        "months": [10, 1, 4, 7],
        "week": "",
        "notes": ""
    },
    "BHL": {
        "frequency": "bi-monthly",
        "months": [12, 3, 6, 9],
        "week": "",
        "notes": ""
    },
    "David Rumsey Map Collection": {
        "frequency": "quarterly",
        "months": [12, 3, 6, 9],
        "week": "",
        "notes": ""
    },
    "Harvard Library": {
        "frequency": "monthly",
        "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "week": "",
        "notes": ""
    },
    "HathiTrust": {
        "frequency": "bi-annually",
        "months": [6, 12],
        "week": "",
        "notes": ""
    },
    "Internet Archive": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "Getty": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "Library of Congress": {
        "frequency": "as-needed",
        "months": [],
        "week": "",
        "notes": ""
    },
    "National Archives and Records Administration": {
        "frequency": "quarterly",
        "months": [1, 4, 7, 10],
        "week": "",
        "notes": "Wikimedia Partner"
    },
    "New York Public Library": {
        "frequency": "quarterly",
        "months": [2, 5, 8, 11],
        "week": "",
        "notes": ""
    },
    "Smithsonian Institution": {
        "frequency": "bi-monthly",
        "months": [10, 12, 2, 4, 6, 8],
        "week": "",
        "notes": ""
    },
    "GPO": {
        "frequency": "quarterly",
        "months": [11, 2, 5, 8],
        "week": "",
        "notes": ""
    },
    "CDL": {
        "frequency": "",
        "months": [],
        "week": "",
        "notes": "On hold pending platform migration"
    },
    "USC": {
        "frequency": "",
        "months": [],
        "week": "",
        "notes": ""
    },
    "Orbis Cascade Alliance": {
        "frequency": "",
        "months": [],
        "week": "",
        "notes": ""
    },
    "Big Sky Country Digital Network": {
        "frequency": "",
        "months": [],
        "week": "",
        "notes": ""
    }
}

def main():
    # Load existing hub data
    with open('scheduler/hub-data-raw.json', 'r') as f:
        hubs = json.load(f)

    # Update hubs with schedule information
    for hub in hubs:
        hub_name = hub['name']
        if hub_name in SCHEDULE_DATA:
            hub['schedule'] = SCHEDULE_DATA[hub_name]
        # Also check for partial matches
        else:
            for schedule_name, schedule_info in SCHEDULE_DATA.items():
                if schedule_name.lower() in hub_name.lower() or hub_name.lower() in schedule_name.lower():
                    hub['schedule'] = schedule_info
                    break

    # Save updated data
    with open('scheduler/hub-data-raw.json', 'w') as f:
        json.dump(hubs, f, indent=2)

    print(f"Updated {len(hubs)} hubs with schedule information")

if __name__ == "__main__":
    main()
