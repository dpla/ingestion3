#!/usr/bin/env python3
"""
Re-split MWDL prefix buckets that exceed MAX_RECORDS.

The Primo VE API hard-caps pagination at offset=200 (max 200 records per query).
This script takes an existing mwdl-prefixes.json and sub-splits any bucket
with count > MAX_RECORDS until all are ≤ MAX_RECORDS.

Reads:  mwdl-prefixes.json (existing)
Writes: mwdl-prefixes.json (in-place, updated)

Usage:
    python3 mwdl-resplit-buckets.py
    python3 mwdl-resplit-buckets.py --max-records 190  # default 190
"""

import argparse
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path
import re

def _read_api_key() -> str:
    candidates = [
        Path.home() / "ingestion3-conf" / "i3.conf",
        Path(__file__).parent.parent.parent / "ingestion3-conf" / "i3.conf",
    ]
    for path in candidates:
        if path.exists():
            m = re.search(r'^mwdl\.harvest\.apiKey\s*=\s*"([^"]+)"', path.read_text(), re.MULTILINE)
            if m:
                return m.group(1)
    raise RuntimeError("mwdl.harvest.apiKey not found in i3.conf")


API_KEY   = _read_api_key()
BASE      = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID       = "01UTAH_INST:MWDL"
TAB       = "LibraryCatalog"
SCOPE     = "MWDL"
CHARS     = list("abcdefghijklmnopqrstuvwxyz0123456789")
REST_S    = 5
TIMEOUT   = 90

OUTPUT_DIR  = Path("/home/ec2-user/mwdl-harvest")
OUTPUT_FILE = OUTPUT_DIR / "mwdl-prefixes.json"

query_count = 0


def get_count(prefix: str, data_source: str) -> int:
    global query_count
    params = {
        "vid":    VID,
        "tab":    TAB,
        "scope":  SCOPE,
        "apikey": API_KEY,
        "limit":  "1",
        "offset": "0",
        "q":      f"title,begins_with,{prefix}",
        "multiFacets": f"facet_data_source,include,{data_source}",
    }
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    for attempt in range(3):
        if attempt:
            time.sleep(REST_S * (attempt + 1))
        try:
            query_count += 1
            with urllib.request.urlopen(url, timeout=TIMEOUT) as r:
                return json.loads(r.read()).get("info", {}).get("total", 0)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
    return 0


def split_bucket(prefix: str, data_source: str, max_records: int) -> dict:
    """Recursively split prefix until all sub-buckets are ≤ max_records.
    Returns {"{data_source}::{sub_prefix}": count, ...}"""
    result = {}
    for c in CHARS:
        time.sleep(REST_S)
        sub = prefix + c
        count = get_count(sub, data_source)
        if count == 0:
            continue
        elif count <= max_records:
            result[f"{data_source}::{sub}"] = count
            print(f"  [{data_source}] '{sub}': {count} ✓", flush=True)
        else:
            print(f"  [{data_source}] '{sub}': {count} → splitting further", flush=True)
            result.update(split_bucket(sub, data_source, max_records))

    # Also try "prefix + space + char" for multi-word titles that don't split further
    space_count = get_count(prefix + " ", data_source)
    if space_count > 0:
        time.sleep(REST_S)
        if space_count <= max_records:
            result[f"{data_source}::{prefix} "] = space_count
        else:
            result.update(split_bucket(prefix + " ", data_source, max_records))

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-records", type=int, default=190,
                        help="Max records per bucket (default: 190, Primo hard-caps at offset=200)")
    args = parser.parse_args()
    max_records = args.max_records

    with open(OUTPUT_FILE) as f:
        data = json.load(f)

    queryable = data["queryable"]
    large = {k: v for k, v in queryable.items() if v > max_records}
    small = {k: v for k, v in queryable.items() if v <= max_records}

    print(f"Loaded {len(queryable)} buckets.", flush=True)
    print(f"  ≤{max_records} records (keep as-is): {len(small)}", flush=True)
    print(f"  >{max_records} records (need splitting): {len(large)}", flush=True)
    print(f"  Total records in large buckets: {sum(large.values()):,}", flush=True)
    print(flush=True)

    new_queryable = dict(small)
    splits_done = 0

    for i, (key, count) in enumerate(sorted(large.items())):
        data_source, prefix = key.split("::", 1)
        print(f"[{i+1}/{len(large)}] Splitting [{data_source}] '{prefix}' ({count} records)...", flush=True)
        sub_buckets = split_bucket(prefix, data_source, max_records)
        new_queryable.update(sub_buckets)
        splits_done += 1
        covered = sum(sub_buckets.values())
        gap = count - covered
        if gap > 0:
            print(f"  Gap: {gap} records not covered by sub-prefixes (likely misc/punctuation)", flush=True)
        print(f"  → {len(sub_buckets)} sub-buckets covering {covered:,} records", flush=True)

        # Save progress after each bucket in case of interruption
        _write_output(data, new_queryable)

    print(f"\nDone. {query_count} API calls made.", flush=True)
    print(f"Buckets: {len(queryable)} → {len(new_queryable)}", flush=True)
    print(f"Total records: {sum(queryable.values()):,} → {sum(new_queryable.values()):,}", flush=True)
    _write_output(data, new_queryable)


def _write_output(original: dict, new_queryable: dict):
    by_source: dict[str, dict[str, int]] = {}
    for key, count in new_queryable.items():
        ds, prefix = key.split("::", 1)
        by_source.setdefault(ds, {})[prefix] = count

    out = {
        "queryable":               dict(sorted(new_queryable.items())),
        "data_sources":            original.get("data_sources", []),
        "by_source":               {ds: dict(sorted(p.items())) for ds, p in by_source.items()},
        "splits_performed":        original.get("splits_performed", []),
        "total_records":           sum(new_queryable.values()),
        "total_queryable_buckets": len(new_queryable),
        "api_calls_made":          query_count,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
