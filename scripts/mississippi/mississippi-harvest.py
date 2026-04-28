#!/usr/bin/env python3
"""
Harvest all Mississippi Primo VE records using title-prefix partitioning.

Reads the queryable prefix set from mississippi-prefixes.json, then paginates
through each prefix (limit=100 per page) and saves all records as one JSONL file.

Output: mississippi-harvest.jsonl — one JSON object per record (the full Primo
"doc" object), ready for ingestion mapping.
"""

import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

API_KEY = "l8xx87aeef957145450eaf117a6c1f0d8c71"
BASE = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
LIMIT = 100
REST_BETWEEN_QUERIES = 12  # seconds
MAX_RETRIES = 4
TIMEOUT = 120  # seconds per HTTP request

PROJECT_DIR = Path("/home/ec2-user/mississippi-harvest")
PREFIXES_FILE = PROJECT_DIR / "mississippi-prefixes.json"
OUTPUT_FILE = PROJECT_DIR / "mississippi-harvest.jsonl"
PROGRESS_FILE = PROJECT_DIR / "mississippi-harvest-progress.json"


def fetch_page(prefix, offset):
    """Fetch one page of results for a prefix+offset. Returns (docs, total) or raises."""
    params = urllib.parse.urlencode(
        {
            "vid": "01USM_INST:MDL",
            "tab": "MDL",
            "scope": "MDL",
            "apikey": API_KEY,
            "limit": str(LIMIT),
            "offset": str(offset),
            "q": f"title,begins_with,{prefix}*",
        }
    )
    url = f"{BASE}?{params}"
    req = urllib.request.Request(url)

    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = REST_BETWEEN_QUERIES * (attempt + 1)
            print(
                f"    Retry {attempt} for '{prefix}*' offset={offset}, "
                f"waiting {wait}s...",
                flush=True,
            )
            time.sleep(wait)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.loads(resp.read())
                docs = data.get("docs", [])
                total = data.get("info", {}).get("total", 0)
                return docs, total
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_BETWEEN_QUERIES)

    raise RuntimeError(
        f"Failed to fetch '{prefix}*' offset={offset} after {MAX_RETRIES} retries"
    )


def load_progress():
    """Load harvest progress (completed prefixes) from disk."""
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"completed_prefixes": [], "total_docs_written": 0}


def save_progress(progress):
    """Save harvest progress to disk."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def main():
    with open(PREFIXES_FILE) as f:
        prefix_data = json.load(f)
    queryable = prefix_data["queryable"]
    prefixes = sorted(queryable.keys())
    total_expected = sum(queryable.values())

    progress = load_progress()
    completed = set(progress["completed_prefixes"])
    docs_written = progress["total_docs_written"]

    remaining = [p for p in prefixes if p not in completed]

    print("Mississippi Primo VE harvest", flush=True)
    print(f"  Total prefixes: {len(prefixes)}", flush=True)
    print(f"  Already completed: {len(completed)}", flush=True)
    print(f"  Remaining: {len(remaining)}", flush=True)
    print(f"  Expected records: {total_expected:,}", flush=True)
    print(f"  Docs already written: {docs_written:,}", flush=True)
    print(f"  Rest between queries: {REST_BETWEEN_QUERIES}s", flush=True)
    print("=" * 60, flush=True)

    mode = "a" if completed else "w"
    out = open(OUTPUT_FILE, mode)

    try:
        for i, prefix in enumerate(remaining):
            expected_count = queryable[prefix]
            prefix_docs = 0
            offset = 0

            print(
                f"\n[{len(completed) + i + 1}/{len(prefixes)}] "
                f"'{prefix}*' ({expected_count} expected)",
                flush=True,
            )

            while True:
                time.sleep(REST_BETWEEN_QUERIES)
                docs, total = fetch_page(prefix, offset)

                if not docs:
                    break

                for doc in docs:
                    out.write(json.dumps(doc) + "\n")
                    prefix_docs += 1
                    docs_written += 1

                print(
                    f"  offset={offset}: {len(docs)} docs "
                    f"(prefix total: {prefix_docs}/{expected_count})",
                    flush=True,
                )

                offset += LIMIT
                if offset >= total or offset >= 5000:
                    break

            completed.add(prefix)
            progress["completed_prefixes"] = list(completed)
            progress["total_docs_written"] = docs_written
            save_progress(progress)

            print(
                f"  ✓ '{prefix}*' done: {prefix_docs} docs "
                f"(total so far: {docs_written:,})",
                flush=True,
            )

    except KeyboardInterrupt:
        print(
            f"\n\nInterrupted. Progress saved. {docs_written:,} docs written so far.",
            flush=True,
        )
    except Exception as e:
        print(f"\n\nError: {e}", flush=True)
        print(f"Progress saved. {docs_written:,} docs written so far.", flush=True)
    finally:
        out.close()
        save_progress(progress)

    print("\n" + "=" * 60, flush=True)
    print("Harvest complete!", flush=True)
    print(f"  Total docs written: {docs_written:,}", flush=True)
    print(f"  Expected: {total_expected:,}", flush=True)
    print(f"  Output: {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
