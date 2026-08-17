#!/usr/bin/env python3
"""
MWDL source explorer — discovers harvestable query units for mwdl-harvest.py.

For each data_source:
  - If total <= MAX_SOURCE_SIZE: record as directly harvestable.
  - If total >  MAX_SOURCE_SIZE: scan years 1800-2030 to find year-level
    sub-partitions that are each within the Primo VE 10K page limit.

Output: mwdl-sources.json — used by mwdl-harvest.py.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Optional

API_KEY = "l8xxe1772f53c1b54de8b25553fda6e224f5"
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"

MAX_SOURCE_SIZE = 9900   # max records we can paginate in one query
REST_S          = 12     # seconds between requests
MAX_RETRIES     = 4
TIMEOUT         = 120

YEAR_RANGE = range(1800, 2031)

# All 10 known MWDL data_sources with approximate counts from facet discovery.
DATA_SOURCES = [
    "DIGCOLL_OCA_30",
    "DIGCOLL_OCA_30_UU",
    "DIGCOLL_WSU_18",
    "DIGCOLL_BYU_25",
    "DIGCOLL_UVU_19",
    "DIGCOLL_SUU_13",
    "DIGCOLL_UNL_14",
    "DIGCOLL_UNR_15",
    "DIGCOLL_BYU_12",
    "DIGCOLL_UUU_11",
]

OUTPUT_FILE = Path("/home/ec2-user/mwdl-harvest/mwdl-sources.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def slack_notify(msg: str) -> None:
    token   = os.environ.get("SLACK_BOT_TOKEN") or os.environ.get("SLACK_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "C02HEU2L3")
    if not token:
        return
    payload = json.dumps({"channel": channel, "text": msg}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def fetch_total(source: str, year: Optional[int] = None) -> int:
    mfacets = f"facet_data_source,include,{source}"
    if year is not None:
        mfacets += f"|facet_creationdate,include,{year}"
    params = urllib.parse.urlencode({
        "vid":         VID,
        "tab":         TAB,
        "scope":       SCOPE,
        "apikey":      API_KEY,
        "limit":       "1",
        "offset":      "0",
        "q":           "any,contains,a",
        "multiFacets": mfacets,
    })
    url = f"{BASE}?{params}"
    req = urllib.request.Request(url)
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            time.sleep(REST_S * (attempt + 1))
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read()).get("info", {}).get("total", 0)
        except Exception as e:
            print(f"  Error: {e}", flush=True)
            time.sleep(REST_S)
    return 0


def main():
    units   = []   # list of {"source": ..., "year": ...|None, "count": ...}
    too_large = []

    slack_notify(":arrow_forward: *mwdl source explorer started* — scanning 10 data sources")
    print("MWDL source explorer", flush=True)
    print(f"MAX_SOURCE_SIZE = {MAX_SOURCE_SIZE}", flush=True)
    print("=" * 60, flush=True)

    for source in DATA_SOURCES:
        time.sleep(REST_S)
        total = fetch_total(source)
        print(f"\n{source}: {total:,} records", flush=True)

        if total == 0:
            print("  → skipping (0 records)", flush=True)
            continue

        if total <= MAX_SOURCE_SIZE:
            units.append({"source": source, "year": None, "count": total})
            print(f"  → direct harvest ({total:,})", flush=True)
            continue

        # Too large — sub-partition by year
        print(f"  → too large, scanning years...", flush=True)
        too_large.append(source)
        source_total = 0
        for year in YEAR_RANGE:
            time.sleep(REST_S)
            ycount = fetch_total(source, year)
            if ycount == 0:
                continue
            print(f"    {year}: {ycount:,}", flush=True)
            if ycount > MAX_SOURCE_SIZE:
                print(f"    !! {year} still too large ({ycount:,}) — capping at {MAX_SOURCE_SIZE}", flush=True)
                ycount = MAX_SOURCE_SIZE  # we'll get as many as possible
            units.append({"source": source, "year": year, "count": ycount})
            source_total += ycount
        if source_total == 0:
            # No creationdate facet values — fall back to capped direct harvest
            capped = min(total, MAX_SOURCE_SIZE)
            print(f"  → year scanning yielded 0; falling back to capped direct harvest ({capped:,})", flush=True)
            units.append({"source": source, "year": None, "count": capped})
        else:
            print(f"  → {source_total:,} expected across years", flush=True)

    total_expected = sum(u["count"] for u in units)
    result = {
        "units":          units,
        "too_large":      too_large,
        "total_expected": total_expected,
        "unit_count":     len(units),
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print("\n" + "=" * 60, flush=True)
    print(f"Done. {len(units)} harvestable units, {total_expected:,} expected records.", flush=True)
    print(f"Output: {OUTPUT_FILE}", flush=True)
    slack_notify(
        f":white_check_mark: *mwdl source explorer complete* — "
        f"{len(units)} harvestable units, {total_expected:,} expected records. "
        f"Ready to run mwdl-harvest.py."
    )


if __name__ == "__main__":
    main()
