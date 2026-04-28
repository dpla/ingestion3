#!/usr/bin/env python3
"""
Explore Mississippi Primo VE API title prefixes to find all queryable (<5000) result sets.

Uses a trie-based approach with NO hardcoded depth limit:
  1. Query a prefix → get count
  2. If count < threshold → it's queryable, done
  3. If count >= threshold → expand by appending each of [a-z0-9]
  4. After expanding, if many records are unaccounted for (the keyword itself
     is complete, e.g. "Letter"), expand by appending " " + [a-z0-9] to split
     on the next word in the title
  5. Recurse until every bucket is under threshold
"""

import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

API_KEY = "l8xx87aeef957145450eaf117a6c1f0d8c71"
BASE = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
THRESHOLD = 5000
CHARS = list("abcdefghijklmnopqrstuvwxyz0123456789")
REST_BETWEEN_QUERIES = 12  # seconds
MAX_RETRIES = 3
TIMEOUT = 90  # seconds per request
OUTPUT_DIR = Path("/home/ec2-user/mississippi-harvest")

# Track results
queryable = {}  # prefix -> count (under threshold, fully harvestable)
needs_split = []  # list of (prefix, count) that were split
query_count = 0  # total API calls made


def query_prefix(prefix):
    """Query the API for a title prefix with retries. Returns count or None."""
    global query_count
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = REST_BETWEEN_QUERIES * (attempt + 1)
            print(
                f"    Retry {attempt} for '{prefix}*', waiting {wait}s...", flush=True
            )
            time.sleep(wait)

        query_count += 1
        params = urllib.parse.urlencode(
            {
                "vid": "01USM_INST:MDL",
                "tab": "MDL",
                "scope": "MDL",
                "apikey": API_KEY,
                "limit": "1",
                "q": f"title,begins_with,{prefix}*",
            }
        )
        url = f"{BASE}?{params}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.loads(resp.read())
                return data.get("info", {}).get("total", 0)
        except Exception:
            pass
        time.sleep(REST_BETWEEN_QUERIES)

    return None  # all retries exhausted


def explore(prefix, parent_count=None):
    """
    Recursively explore a prefix. Returns total records resolved under it.
    """
    time.sleep(REST_BETWEEN_QUERIES)

    count = query_prefix(prefix)
    if count is None:
        # Couldn't get count even after retries — try splitting blind
        print(f"  '{prefix}*': TIMEOUT — splitting blind", flush=True)
        count = THRESHOLD  # force split
    elif count == 0:
        return 0
    elif count < THRESHOLD:
        queryable[prefix] = count
        print(f"  '{prefix}*': {count} ✓", flush=True)
        return count

    needs_split.append((prefix, count))
    print(f"  '{prefix}*': {count} → splitting by char extension", flush=True)

    char_resolved = 0
    for c in CHARS:
        char_resolved += explore(prefix + c, parent_count=count)

    gap = count - char_resolved
    if gap >= THRESHOLD:
        # Prefix is a complete word (e.g. "Letter") — split on next word.
        print(
            f"  '{prefix}*': {gap} unresolved after char extension "
            f"→ splitting by next word",
            flush=True,
        )
        word_resolved = 0
        for c in CHARS:
            word_resolved += explore(prefix + " " + c, parent_count=gap)

        remaining = gap - word_resolved
        if remaining > 0 and remaining < THRESHOLD:
            # Titles where this is the last word or next word is punctuation.
            print(
                f"  '{prefix}*': {remaining} records in gap "
                f"(no next-word match) — noted",
                flush=True,
            )
        elif remaining >= THRESHOLD:
            print(
                f"  ⚠ '{prefix}*': {remaining} records still "
                f"unresolvable after next-word split",
                flush=True,
            )

        return char_resolved + word_resolved
    elif 0 < gap < THRESHOLD:
        print(f"  '{prefix}*': {gap} in gap (small, noted)", flush=True)

    return char_resolved


def main():
    print("Mississippi Primo VE title prefix exploration", flush=True)
    print(f"Threshold: <{THRESHOLD} records per prefix", flush=True)
    print(f"Rest between queries: {REST_BETWEEN_QUERIES}s", flush=True)
    print("No depth limit — recurses until all buckets are under threshold", flush=True)
    print("=" * 60, flush=True)

    total_resolved = 0
    top_chars = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    for i, c in enumerate(top_chars):
        print(f"\n[{i + 1}/{len(top_chars)}] Exploring {c}*...", flush=True)
        resolved = explore(c)
        total_resolved += resolved
        print(f"  → {c}* resolved: {resolved}", flush=True)

    # Summary
    print("\n" + "=" * 60, flush=True)
    print("RESULTS", flush=True)
    print("=" * 60, flush=True)

    total_records = sum(queryable.values())
    print(f"\nQueryable prefixes: {len(queryable)}", flush=True)
    print(f"Total records covered: {total_records:,}", flush=True)
    print(f"API calls made: {query_count}", flush=True)

    # Estimate pagination queries needed
    total_pages = sum((count + 99) // 100 for count in queryable.values())

    print("\nTo harvest all covered records at limit=100:", flush=True)
    print(f"  Discovery queries (prefix counts): {len(queryable)}", flush=True)
    print(f"  Pagination queries: {total_pages:,}", flush=True)
    print(
        f"  Estimated harvest time at {REST_BETWEEN_QUERIES}s spacing: "
        f"{total_pages * REST_BETWEEN_QUERIES / 3600:.1f} hours",
        flush=True,
    )

    # Show the largest queryable buckets
    top_buckets = sorted(queryable.items(), key=lambda x: -x[1])[:20]
    print("\nLargest queryable buckets:", flush=True)
    for prefix, count in top_buckets:
        print(f"  '{prefix}*': {count}", flush=True)

    # Write detailed results to file
    output = {
        "queryable": dict(sorted(queryable.items())),
        "splits_performed": [(p, c) for p, c in needs_split],
        "total_records": total_records,
        "total_queryable_prefixes": len(queryable),
        "total_pagination_pages": total_pages,
        "api_calls_made": query_count,
    }
    outfile = OUTPUT_DIR / "mississippi-prefixes.json"
    with open(outfile, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nDetailed results written to {outfile}", flush=True)


if __name__ == "__main__":
    main()
