#!/usr/bin/env python3
"""
Explore MWDL Primo VE API title prefixes to find all queryable result sets.

Uses a trie-based approach with no hardcoded depth limit:
  1. Query a prefix → get count
  2. If count < THRESHOLD → queryable, done
  3. If count >= THRESHOLD → expand by appending each of [a-z0-9 ]
  4. After char expansion, if gap remains >= THRESHOLD → expand on next word
     (append space + [a-z0-9]) to split on the next word in the title
  5. Recurse until every bucket is under threshold

Output: mwdl-prefixes.json — used by mwdl-harvest.py.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Optional

# Load .env from repo root
def _load_env() -> None:
    env_path = Path(__file__).parent.parent.parent / ".env"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

_load_env()

API_KEY   = os.environ.get("MWDL_API_KEY", "")
BASE      = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID       = "01UTAH_INST:MWDL"
TAB       = "LibraryCatalog"
SCOPE     = "MWDL"

THRESHOLD  = 5000
CHARS      = list("abcdefghijklmnopqrstuvwxyz0123456789")
REST_S     = 12
MAX_RETRIES = 3
TIMEOUT    = 90

OUTPUT_DIR  = Path("/home/ec2-user/mwdl-harvest")
OUTPUT_FILE = OUTPUT_DIR / "mwdl-prefixes.json"

# State
queryable   = {}   # prefix -> count
needs_split = []   # [(prefix, count)]
query_count = 0


def query_prefix(prefix: str) -> Optional[int]:
    global query_count
    params = urllib.parse.urlencode({
        "vid":    VID,
        "tab":    TAB,
        "scope":  SCOPE,
        "apikey": API_KEY,
        "limit":  "1",
        "offset": "0",
        "q":      f"title,begins_with,{prefix}",
    })
    req = urllib.request.Request(f"{BASE}?{params}")
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = REST_S * (attempt + 1)
            print(f"    Retry {attempt} for '{prefix}*', waiting {wait}s...", flush=True)
            time.sleep(wait)
        query_count += 1
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.loads(resp.read())
                return data.get("info", {}).get("total", 0)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_S)
    return None


def explore(prefix: str) -> int:
    """Recursively explore a prefix. Returns total records resolved under it."""
    time.sleep(REST_S)

    count = query_prefix(prefix)
    if count is None:
        print(f"  '{prefix}*': TIMEOUT — splitting blind", flush=True)
        count = THRESHOLD  # force split
    elif count == 0:
        return 0
    elif count < THRESHOLD:
        queryable[prefix] = count
        print(f"  '{prefix}*': {count:,} ✓", flush=True)
        return count

    needs_split.append((prefix, count))
    print(f"  '{prefix}*': {count:,} → splitting by char", flush=True)

    char_resolved = 0
    for c in CHARS:
        char_resolved += explore(prefix + c)

    gap = count - char_resolved
    if gap >= THRESHOLD:
        # Prefix is a complete word — split on next word
        print(
            f"  '{prefix}*': {gap:,} unresolved after char extension "
            f"→ splitting by next word",
            flush=True,
        )
        word_resolved = 0
        for c in CHARS:
            word_resolved += explore(prefix + " " + c)

        remaining = gap - word_resolved
        if remaining > 0 and remaining < THRESHOLD:
            print(f"  '{prefix}*': {remaining:,} in gap (no next-word match) — noted", flush=True)
        elif remaining >= THRESHOLD:
            print(f"  ⚠ '{prefix}*': {remaining:,} still unresolvable after next-word split", flush=True)

        return char_resolved + word_resolved
    elif 0 < gap < THRESHOLD:
        print(f"  '{prefix}*': {gap:,} in gap (small, noted)", flush=True)

    return char_resolved


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("MWDL Primo VE title prefix exploration", flush=True)
    print(f"Threshold: <{THRESHOLD} records per prefix", flush=True)
    print(f"Rest between queries: {REST_S}s", flush=True)
    print("=" * 60, flush=True)

    total_resolved = 0
    top_chars = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    for i, c in enumerate(top_chars):
        print(f"\n[{i+1}/{len(top_chars)}] Exploring {c}*...", flush=True)
        resolved = explore(c)
        total_resolved += resolved
        print(f"  → {c}* resolved: {resolved:,}", flush=True)

    print("\n" + "=" * 60, flush=True)
    total_records = sum(queryable.values())
    total_pages   = sum((count + 99) // 100 for count in queryable.values())

    print(f"Queryable prefixes:     {len(queryable)}", flush=True)
    print(f"Total records covered:  {total_records:,}", flush=True)
    print(f"API calls made:         {query_count}", flush=True)
    print(f"Est. harvest time:      {total_pages * REST_S / 3600:.1f} hours at {REST_S}s spacing", flush=True)

    top_buckets = sorted(queryable.items(), key=lambda x: -x[1])[:20]
    print("\nLargest queryable buckets:", flush=True)
    for prefix, count in top_buckets:
        print(f"  '{prefix}*': {count:,}", flush=True)

    output = {
        "queryable":                dict(sorted(queryable.items())),
        "splits_performed":         [(p, c) for p, c in needs_split],
        "total_records":            total_records,
        "total_queryable_prefixes": len(queryable),
        "total_pagination_pages":   total_pages,
        "api_calls_made":           query_count,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nOutput: {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
