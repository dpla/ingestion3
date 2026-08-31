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
REST_S    = 3
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


def split_bucket(prefix: str, data_source: str, max_records: int, _allow_space: bool = True) -> dict:
    """Recursively split prefix until all sub-buckets are ≤ max_records.
    _allow_space=False prevents infinite recursion on space-padded titles.
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
            result.update(split_bucket(sub, data_source, max_records, _allow_space=True))

    # One level of space expansion only — stops infinite recursion on space-padded titles
    # (e.g. a record titled "1    s" would otherwise recurse: '1 ', '1  ', '1   '... forever)
    if _allow_space:
        time.sleep(REST_S)
        space_prefix = prefix + " "
        space_count = get_count(space_prefix, data_source)
        if 0 < space_count <= max_records:
            result[f"{data_source}::{space_prefix}"] = space_count
            print(f"  [{data_source}] '{space_prefix}': {space_count} ✓", flush=True)
        elif space_count > max_records:
            # Split space-prefix by chars only — no further space expansion
            result.update(split_bucket(space_prefix, data_source, max_records, _allow_space=False))

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-records", type=int, default=190,
                        help="Max records per bucket (default: 190)")
    parser.add_argument("--worker-id",   type=int, default=0)
    parser.add_argument("--num-workers", type=int, default=1)
    args = parser.parse_args()
    max_records = args.max_records
    worker_id   = args.worker_id
    num_workers = args.num_workers

    with open(OUTPUT_FILE) as f:
        data = json.load(f)

    queryable = data["queryable"]
    all_large = sorted(k for k, v in queryable.items() if v > max_records)
    small     = {k: v for k, v in queryable.items() if v <= max_records}
    my_large  = all_large[worker_id::num_workers]

    print(f"Worker {worker_id}/{num_workers} — MWDL bucket resplit", flush=True)
    print(f"  Total large buckets: {len(all_large)}", flush=True)
    print(f"  My share:            {len(my_large)}", flush=True)
    print(flush=True)

    # Each worker writes its results to a sidecar file; worker 0 also merges at the end
    sidecar = OUTPUT_DIR / f"mwdl-resplit-{worker_id}.json"
    if sidecar.exists():
        with open(sidecar) as f:
            my_results = json.load(f)
        done_keys = set(my_results.keys())
        print(f"  Resuming — {len(done_keys)} sub-buckets already found", flush=True)
    else:
        my_results = {}
        done_keys  = set()

    processed_large_keys = set()

    for i, key in enumerate(my_large):
        data_source, prefix = key.split("::", 1)
        count = queryable[key]
        # Check if we already split this key (all its children are in my_results)
        if any(k.startswith(f"{data_source}::{prefix}") for k in done_keys):
            processed_large_keys.add(key)
            continue
        print(f"[{i+1}/{len(my_large)}] [{data_source}] '{prefix}' ({count})", flush=True)
        sub = split_bucket(prefix, data_source, max_records)
        my_results.update(sub)
        processed_large_keys.add(key)
        with open(sidecar, "w") as f:
            json.dump(my_results, f)
        print(f"  → {len(sub)} sub-buckets, {sum(sub.values()):,} records", flush=True)

    print(f"\nWorker {worker_id} done. {query_count} API calls.", flush=True)

    # Merge: wait for all sidecar files then rebuild prefixes.json (worker 0 only)
    if worker_id == 0:
        print("Merging all worker results...", flush=True)
        merged = dict(small)
        # Collect sidecar files from all workers
        for wid in range(num_workers):
            sc = OUTPUT_DIR / f"mwdl-resplit-{wid}.json"
            if sc.exists():
                with open(sc) as f:
                    merged.update(json.load(f))
                sc.unlink()
        # Remove original large keys replaced by sub-buckets
        for key in all_large:
            merged.pop(key, None)
        print(f"Buckets: {len(queryable)} → {len(merged)}", flush=True)
        print(f"Records: {sum(queryable.values()):,} → {sum(merged.values()):,}", flush=True)
        _write_output(data, merged)


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
