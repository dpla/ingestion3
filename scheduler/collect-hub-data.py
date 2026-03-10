#!/usr/bin/env python3
"""
collect-hub-data.py - Extract hub metadata from configuration and schedule pages

This script collects hub contact information from the i3.conf configuration file
and schedule information from Confluence pages. Email addresses are read from
the config file's .email or .emails properties rather than being extracted from HTML.
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from typing import List, Dict, Set, Optional

BASE_URL = "https://digitalpubliclibraryofamerica.atlassian.net"

# Hub dashboard URLs
HUB_URLS = [
    "/wiki/spaces/CT/pages/113573915/David+Rumsey+Map+Collection+Dashboard",
    "/wiki/spaces/CT/pages/113672258/Internet+Archive+Dashboard",
    "/wiki/spaces/CT/pages/113868824/Harvard+Library+Dashboard",
    "/wiki/spaces/CT/pages/113901613/National+Archives+and+Records+Administration+Dashboard",
    "/wiki/spaces/CT/pages/149192718/Sunshine+State+Digital+Network+Dashboard",
    "/wiki/spaces/CT/pages/177963022/OKHub+Dashboard",
    "/wiki/spaces/CT/pages/235143169/Ohio+Digital+Network+Dashboard",
    "/wiki/spaces/CT/pages/276889601/District+Digital+Dashboard",
    "/wiki/spaces/CT/pages/391675905/Plains+to+Peaks+Dashboard",
    "/wiki/spaces/CT/pages/84938455/New+York+Public+Library+Dashboard",
    "/wiki/spaces/CT/pages/84970940/Library+of+Congress+Dashboard",
    "/wiki/spaces/CT/pages/84971116/NCDHC+Dashboard",
    "/wiki/spaces/CT/pages/84971302/Indiana+Memory+Dashboard",
    "/wiki/spaces/CT/pages/84971401/DLG+Dashboard",
    "/wiki/spaces/CT/pages/85141812/BHL+Dashboard",
    "/wiki/spaces/CT/pages/85417652/MDL+Dashboard",
    "/wiki/spaces/CT/pages/85417844/Illinois+Digital+Heritage+Dashboard",
    "/wiki/spaces/CT/pages/85462286/Heartland+Hub+Dashboard",
    "/wiki/spaces/CT/pages/85463029/Portal+to+Texas+History+Dashboard",
    "/wiki/spaces/CT/pages/85627860/Recollection+Wisconsin+Dashboard",
    "/wiki/spaces/CT/pages/85628036/South+Carolina+Digital+Library+Dashboard",
    "/wiki/spaces/CT/pages/85628206/Digital+Commonwealth+Dashboard",
    "/wiki/spaces/CT/pages/85628441/Smithsonian+Institution+Dashboard",
    "/wiki/spaces/CT/pages/85920546/Digital+Library+of+Tennessee+Dashboard",
    "/wiki/spaces/CT/pages/85929619/Michigan+Service+Hub+Dashboard",
    "/wiki/spaces/CT/pages/85929877/Mountain+West+Digital+Library+Dashboard",
    "/wiki/spaces/CT/pages/85930296/Getty+Dashboard",
    "/wiki/spaces/CT/pages/85930880/USC+Dashboard",
    "/wiki/spaces/CT/pages/862945312/Orbis+Cascade+Alliance+Dashboard",
    "/wiki/spaces/CT/pages/863207453/Digital+Virginias+Dashboard",
    "/wiki/spaces/CT/pages/86430746/CDL+Dashboard",
    "/wiki/spaces/CT/pages/86496527/Digital+Maine+Dashboard",
    "/wiki/spaces/CT/pages/86496913/HathiTrust+Dashboard",
    "/wiki/spaces/CT/pages/86498117/Big+Sky+Country+Digital+Network+Dashboard",
    "/wiki/spaces/CT/pages/86498373/Digital+Maryland+Dashboard",
    "/wiki/spaces/CT/pages/86543081/PA+Digital+Dashboard",
    "/wiki/spaces/CT/pages/88075716/GPO+Dashboard",
]

# Map hub names to provider codes (from existing harvest.sh)
PROVIDER_CODE_MAP = {
    "David Rumsey Map Collection": "david-rumsey",
    "Internet Archive": "ia",
    "Harvard Library": "harvard",
    "National Archives and Records Administration": "nara",
    "Sunshine State Digital Network": "florida",
    "OKHub": "oklahoma",
    "Ohio Digital Network": "ohio",
    "District Digital": "district-digital",
    "Plains to Peaks": "p2p",
    "New York Public Library": "nypl",
    "Library of Congress": "lc",
    "NCDHC": "digitalnc",
    "Indiana Memory": "indiana",
    "DLG": "georgia",
    "BHL": "bhl",
    "MDL": "minnesota",
    "Illinois Digital Heritage": "il",
    "Heartland Hub": "heartland",
    "Portal to Texas History": "texas",
    "Recollection Wisconsin": "wisconsin",
    "South Carolina Digital Library": "scdl",
    "Digital Commonwealth": "maryland",
    "Smithsonian Institution": "smithsonian",
    "Digital Library of Tennessee": "tennessee",
    "Michigan Service Hub": "mi",
    "Mountain West Digital Library": "mwdl",
    "Getty": "getty",
    "USC": "usc",
    "Orbis Cascade Alliance": "orbis-cascade",
    "Digital Virginias": "virginias",
    "CDL": "cdl",
    "Digital Maine": "maine",
    "HathiTrust": "hathi",
    "Big Sky Country Digital Network": "mt",
    "Digital Maryland": "maryland",
    "PA Digital": "pa",
    "GPO": "gpo",
}

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

# Default config file path
DEFAULT_CONFIG_PATH = os.path.expanduser("~/dpla/code/ingestion3-conf/i3.conf")

# Map provider codes to config file keys (for cases where they don't match exactly)
PROVIDER_CODE_TO_CONFIG_KEY = {
    "district-digital": "dc",  # District Digital uses "dc" in config
    "tennessee": "tn",  # Digital Library of Tennessee uses "tn" in config
    # "maryland" maps to "bpl" for Digital Commonwealth, but "maryland" also exists for Digital Maryland
    # We'll need to handle this case specially - "Digital Commonwealth" should use "bpl"
    # Note: "Digital Commonwealth" in PROVIDER_CODE_MAP maps to "maryland", but config has "bpl.email"
    # This is a known issue - "Digital Commonwealth" should probably map to "bpl" not "maryland"
}


def extract_hub_name(url_path: str) -> str:
    """Extract hub name from URL path."""
    name = url_path.split("/")[-1].replace("+Dashboard", "").replace("+", " ")
    return name.strip()


def parse_email_string(email_str: str) -> List[str]:
    """
    Parse email string from config file and extract email addresses.

    Handles formats like:
    - "email@example.com"
    - "Name<email@example.com>"
    - "email1@example.com,email2@example.com"
    - "Name1<email1@example.com>,Name2<email2@example.com>"
    """
    emails = []

    # Remove surrounding quotes if present
    email_str = email_str.strip().strip('"').strip("'")

    # Split by comma to handle multiple emails
    parts = [p.strip() for p in email_str.split(',')]

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Check if it's in "Name<email>" format
        match = re.search(r'<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>', part)
        if match:
            emails.append(match.group(1).lower())
        else:
            # Check if it's a plain email address
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if re.match(email_pattern, part):
                emails.append(part.lower())

    return emails


def parse_config_file(config_path: str) -> Dict[str, str]:
    """
    Parse the i3.conf properties file and return a dictionary of key-value pairs.

    Returns a dict mapping keys like "harvard.email" to their values.
    """
    config = {}

    if not os.path.exists(config_path):
        print(f"WARNING: Config file not found at {config_path}", file=sys.stderr)
        return config

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue

                # Parse key = value or key=value
                # Handle quoted values
                match = re.match(r'^([a-zA-Z0-9._-]+)\s*[=:]\s*(.+)$', line)
                if match:
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    # Remove surrounding quotes
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    config[key] = value
    except Exception as e:
        print(f"ERROR reading config file {config_path}: {e}", file=sys.stderr)

    return config


def normalize_provider_code_for_config(provider_code: str, hub_name: str = "") -> str:
    """
    Normalize provider code to match config file key.

    Handles special cases where provider code doesn't match config key.
    """
    # Check explicit mapping first
    if provider_code in PROVIDER_CODE_TO_CONFIG_KEY:
        return PROVIDER_CODE_TO_CONFIG_KEY[provider_code]

    # Special case: "Digital Commonwealth" uses "bpl" in config, not "maryland"
    if hub_name == "Digital Commonwealth" and provider_code == "maryland":
        return "bpl"

    # Default: use provider code as-is
    return provider_code


def get_emails_from_config(provider_code: str, hub_name: str = "", config_path: str = DEFAULT_CONFIG_PATH) -> List[str]:
    """
    Get email addresses for a provider from the config file.

    Tries both .email and .emails properties.
    Returns a list of email addresses (lowercased).
    """
    config = parse_config_file(config_path)

    # Normalize provider code to match config key
    config_key = normalize_provider_code_for_config(provider_code, hub_name)

    # Try .email first, then .emails
    email_key = f"{config_key}.email"
    emails_key = f"{config_key}.emails"

    email_value = None
    if email_key in config:
        email_value = config[email_key]
    elif emails_key in config:
        email_value = config[emails_key]

    if email_value:
        return parse_email_string(email_value)

    # No email found
    return []


def parse_schedule_text(text: str) -> Dict:
    """Parse schedule text into structured format."""
    text = text.lower()
    schedule = {
        "frequency": "",
        "months": [],
        "week": ""
    }

    # Determine frequency
    if "quarterly" in text:
        schedule["frequency"] = "quarterly"
    elif "bi-monthly" in text or "bi monthly" in text:
        schedule["frequency"] = "bi-monthly"
    elif "bi-annually" in text or "bi annually" in text or "bi-annual" in text:
        schedule["frequency"] = "bi-annually"
    elif "monthly" in text:
        schedule["frequency"] = "monthly"
    elif "as needed" in text:
        schedule["frequency"] = "as-needed"

    # Extract months
    for month_name, month_num in MONTH_MAP.items():
        if month_name in text:
            schedule["months"].append(month_num)

    # Extract week information
    if "last week" in text or "always run last week" in text:
        schedule["week"] = "last"
    elif "first week" in text:
        schedule["week"] = "first"

    return schedule


def fetch_page(url: str) -> str:
    """Fetch page content."""
    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}", file=sys.stderr)
        return ""


def parse_schedule_page() -> Dict[str, Dict]:
    """Parse the main schedule page to extract schedule information."""
    schedule_url = f"{BASE_URL}/wiki/spaces/CT/pages/84969744/Hub+Re-ingest+Schedule"
    html = fetch_page(schedule_url)

    schedules = {}

    # Extract schedule information from the HTML
    # Look for patterns like "Ingested quarterly: December, March, June, September"
    schedule_pattern = r'Ingested\s+(quarterly|bi-monthly|bi-annually|monthly|as needed)[^<]*'

    # Also look for hub names and their schedules
    # This is a simplified parser - in practice, the HTML structure may vary
    lines = html.split('\n')
    current_hub = None

    for line in lines:
        # Look for hub links
        hub_match = re.search(r'href="/wiki/spaces/CT/pages/(\d+)/([^"]+Dashboard)"', line)
        if hub_match:
            hub_name = hub_match.group(2).replace('+', ' ').replace(' Dashboard', '')
            current_hub = hub_name

        # Look for schedule information
        if current_hub and 'ingested' in line.lower():
            schedule_match = re.search(r'ingested\s+(quarterly|bi-monthly|bi-annually|monthly|as needed)[^<]*', line.lower())
            if schedule_match:
                schedules[current_hub] = parse_schedule_text(line)
                current_hub = None

    return schedules


def main():
    print(f"Collecting hub data from {len(HUB_URLS)} dashboard pages...")
    print(f"Reading emails from config file: {DEFAULT_CONFIG_PATH}")
    print("Fetching main schedule page for schedule information...")

    # Parse schedule page
    schedules = parse_schedule_page()
    time.sleep(1)

    hubs = []
    summary_lines = []

    for i, url_path in enumerate(HUB_URLS, 1):
        full_url = f"{BASE_URL}{url_path}"
        hub_name = extract_hub_name(url_path)
        provider_code = PROVIDER_CODE_MAP.get(hub_name, hub_name.lower().replace(' ', '-'))

        print(f"[{i}/{len(HUB_URLS)}] Processing: {hub_name}")
        summary_lines.append(f"\nHub: {hub_name}")
        summary_lines.append(f"  URL: {full_url}")
        summary_lines.append(f"  Provider Code: {provider_code}")

        # Get emails from config file
        emails = sorted(get_emails_from_config(provider_code, hub_name))
        if not emails:
            print(f"  WARNING: No email found in config for {provider_code} (hub: {hub_name})", file=sys.stderr)
            summary_lines.append(f"  WARNING: No email found in config for {provider_code}")

        # Fetch page is no longer needed for emails, but kept for potential future metadata extraction
        # Schedule info comes from parse_schedule_page()
        schedule_info = schedules.get(hub_name, {})

        # Build hub entry
        hub_entry = {
            "name": hub_name,
            "provider_code": provider_code,
            "dashboard_url": full_url,
            "contacts": emails,
            "schedule": schedule_info if schedule_info else {
                "frequency": "",
                "months": [],
                "week": ""
            },
            "feed_type": "",
            "feed_url": "",
            "metadata_format": "",
            "notes": ""
        }

        hubs.append(hub_entry)

        # Add to summary
        summary_lines.append(f"  Contacts: {len(emails)} email(s) from config")
        for email in emails[:5]:  # Limit to first 5
            summary_lines.append(f"    - {email}")
        if schedule_info:
            summary_lines.append(f"  Schedule: {schedule_info}")

        # Be nice to the server
        time.sleep(1)

    # Write JSON output
    with open('scheduler/hub-data-raw.json', 'w') as f:
        json.dump(hubs, f, indent=2)

    # Write summary
    with open('scheduler/hub-data-summary.txt', 'w') as f:
        f.write(f"Hub Data Collection Summary\n")
        f.write(f"Collected data from {len(hubs)} hubs\n")
        f.write("=" * 60 + "\n")
        f.write('\n'.join(summary_lines))

    print(f"\nData collection complete!")
    print(f"  - Processed {len(hubs)} hubs")
    print(f"  - Raw data: scheduler/hub-data-raw.json")
    print(f"  - Summary: scheduler/hub-data-summary.txt")


if __name__ == "__main__":
    main()
