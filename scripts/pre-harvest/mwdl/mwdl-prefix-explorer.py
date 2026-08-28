#!/usr/bin/env python3
"""
Explore MWDL Primo VE API prefix buckets, partitioned by data_source first.

Strategy:
  1. Discover all data_source values from the API facets
  2. For each data_source:
     a. If total count < THRESHOLD → one bucket for the whole source
     b. Otherwise → trie-based title-prefix exploration within that source
  3. Write mwdl-prefixes.json with keys "{data_source}::{prefix}"

Output: mwdl-prefixes.json — used by mwdl-harvest.py
"""

import json
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Optional
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

THRESHOLD   = 5000
CHARS       = list("abcdefghijklmnopqrstuvwxyz0123456789")
REST_S      = 5
MAX_RETRIES = 3
TIMEOUT     = 90

OUTPUT_DIR  = Path("/home/ec2-user/mwdl-harvest")
OUTPUT_FILE = OUTPUT_DIR / "mwdl-prefixes.json"

queryable   = {}   # "{data_source}::{prefix}" -> count
needs_split = []
query_count = 0


def api_call(q: str, data_source: Optional[str] = None, limit: int = 1) -> Optional[dict]:
    global query_count
    params = {
        "vid":    VID,
        "tab":    TAB,
        "scope":  SCOPE,
        "apikey": API_KEY,
        "limit":  str(limit),
        "offset": "0",
        "q":      q,
    }
    if data_source:
        params["multiFacets"] = f"facet_data_source,include,{data_source}"
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = REST_S * (attempt + 1)
            print(f"    Retry {attempt}, waiting {wait}s...", flush=True)
            time.sleep(wait)
        query_count += 1
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read())
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_S)
    return None


def get_count(q: str, data_source: Optional[str] = None) -> int:
    data = api_call(q, data_source, limit=1)
    if data is None:
        return THRESHOLD  # force split on failure
    return data.get("info", {}).get("total", 0)


def get_data_sources() -> list[tuple[str, int]]:
    """Return [(data_source_code, total_count), ...] sorted by count desc."""
    print("Discovering data_source values...", flush=True)
    # Use a broad query that matches everything
    data = api_call("title,begins_with,a", limit=1)
    if data is None:
        raise RuntimeError("Failed to get data_source facets from API")
    sources = []
    for facet in data.get("facets", []):
        if facet.get("name") == "data_source":
            for v in facet.get("values", []):
                sources.append((v["value"], int(v["count"])))
    # The facet counts above are only for "title begins_with a" — we need totals.
    # Re-query each source without prefix to get real total.
    totals = []
    for ds, _ in sources:
        time.sleep(REST_S)
        total = get_count("title,begins_with,a", ds)
        # Use 'a' as a rough check; we'll get real count per-prefix in explore()
        totals.append((ds, total))
        print(f"  {ds}: {total:,} records starting with 'a' (sampling)", flush=True)
    return sources  # return original facet list; explore() will get real counts


def explore(prefix: str, data_source: str) -> int:
    """Recursively explore a prefix within a data_source. Returns total resolved."""
    time.sleep(REST_S)

    q = f"title,begins_with,{prefix}"
    count = get_count(q, data_source)

    if count == 0:
        return 0
    elif count < THRESHOLD:
        key = f"{data_source}::{prefix}"
        queryable[key] = count
        print(f"  [{data_source}] '{prefix}': {count:,} ✓", flush=True)
        return count

    needs_split.append((f"{data_source}::{prefix}", count))
    print(f"  [{data_source}] '{prefix}': {count:,} → splitting by char", flush=True)

    char_resolved = 0
    for c in CHARS:
        char_resolved += explore(prefix + c, data_source)

    gap = count - char_resolved
    if gap >= THRESHOLD:
        print(f"  [{data_source}] '{prefix}': {gap:,} unresolved → splitting by next word", flush=True)
        word_resolved = 0
        for c in CHARS:
            word_resolved += explore(prefix + " " + c, data_source)
        return char_resolved + word_resolved
    elif 0 < gap < THRESHOLD:
        key = f"{data_source}:: {prefix}_gap"
        print(f"  [{data_source}] '{prefix}': {gap:,} in gap (small, noted)", flush=True)

    return char_resolved


def explore_source(data_source: str) -> int:
    """Explore all title prefixes for one data_source."""
    # First, get the real total for this source using a simple query
    # We'll sum across all top chars
    time.sleep(REST_S)
    total_resolved = 0
    top_chars = CHARS
    for i, c in enumerate(top_chars):
        print(f"\n  [{data_source}] [{i+1}/{len(top_chars)}] prefix '{c}'...", flush=True)
        total_resolved += explore(c, data_source)
    return total_resolved


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("MWDL Primo VE title prefix exploration (data_source partitioned)", flush=True)
    print(f"Threshold: <{THRESHOLD} records per bucket", flush=True)
    print(f"Rest between queries: {REST_S}s", flush=True)
    print("=" * 60, flush=True)

    # Step 1: get data_sources
    data = api_call("title,begins_with,a", limit=1)
    if data is None:
        raise RuntimeError("Failed to contact API")
    sources = []
    for facet in data.get("facets", []):
        if facet.get("name") == "data_source":
            for v in facet.get("values", []):
                sources.append(v["value"])
    print(f"\nFound {len(sources)} data_sources: {sources}", flush=True)

    # Step 2: explore each source
    for i, ds in enumerate(sources):
        print(f"\n{'='*60}", flush=True)
        print(f"[{i+1}/{len(sources)}] data_source: {ds}", flush=True)
        explore_source(ds)

    print("\n" + "=" * 60, flush=True)
    total_records = sum(queryable.values())
    total_pages   = sum((c + 99) // 100 for c in queryable.values())

    print(f"Queryable buckets:      {len(queryable)}", flush=True)
    print(f"Total records covered:  {total_records:,}", flush=True)
    print(f"API calls made:         {query_count}", flush=True)
    print(f"Est. harvest time:      {total_pages * REST_S / 3600:.1f}h at {REST_S}s spacing", flush=True)

    top = sorted(queryable.items(), key=lambda x: -x[1])[:20]
    print("\nLargest buckets:", flush=True)
    for key, count in top:
        print(f"  '{key}': {count:,}", flush=True)

    # Group by data_source for worker assignment
    by_source: dict[str, dict[str, int]] = {}
    for key, count in queryable.items():
        ds, prefix = key.split("::", 1)
        by_source.setdefault(ds, {})[prefix] = count

    output = {
        "queryable":                dict(sorted(queryable.items())),
        "data_sources":             sources,
        "by_source":                {ds: dict(sorted(p.items())) for ds, p in by_source.items()},
        "splits_performed":         [(k, c) for k, c in needs_split],
        "total_records":            total_records,
        "total_queryable_buckets":  len(queryable),
        "total_pagination_pages":   total_pages,
        "api_calls_made":           query_count,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nOutput: {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
