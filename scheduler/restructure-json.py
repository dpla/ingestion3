#!/usr/bin/env python3
"""Restructure JSON to match plan format"""

import json

with open('scheduler/hub-data-raw.json', 'r') as f:
    hubs = json.load(f)

output = {"hubs": hubs}

with open('scheduler/schedule.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f"Restructured schedule.json with {len(hubs)} hubs")
