#!/usr/bin/env python3
"""
Harvest all MWDL Primo VE records using title-prefix partitioning.

Reads the queryable prefix set from mwdl-prefixes.json (produced by
mwdl-prefix-explorer.py), paginates through each prefix, and writes
all records to a single JSONL file.

Output: /home/ec2-user/mwdl-harvest/mwdl-harvest.jsonl
"""

import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse
from pathlib import Path

import re

def _read_api_key() -> str:
    """Read mwdl.harvest.apiKey from i3.conf."""
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

API_KEY = _read_api_key()
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"
LIMIT   = 100
MAX_OFFSET      = 4900   # stay safely under 5000 per prefix bucket
REST_S          = 15     # seconds between normal page fetches
THROTTLE_S      = 300    # seconds to wait on 401/429 (rate limit signal)
MAX_RETRIES     = 5
TIMEOUT         = 120

PROJECT_DIR   = Path("/home/ec2-user/mwdl-harvest")


def fetch_page(prefix: str, offset: int) -> "tuple[list, int]":
    params = urllib.parse.urlencode({
        "vid":    VID,
        "tab":    TAB,
        "scope":  SCOPE,
        "apikey": API_KEY,
        "limit":  str(LIMIT),
        "offset": str(offset),
        "q":      f"title,begins_with,{prefix}",
    })
    req = urllib.request.Request(f"{BASE}?{params}")
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            print(f"    Retry {attempt} for '{prefix}' offset={offset}...", flush=True)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
            try:
                data  = json.loads(raw)
            except json.JSONDecodeError:
                print(f"    Empty/non-JSON response (rate limit signal) — waiting {THROTTLE_S}s...", flush=True)
                time.sleep(THROTTLE_S)
                continue
            docs  = data.get("docs", [])
            total = data.get("info", {}).get("total", 0)
            return docs, total
        except urllib.error.HTTPError as e:
            if e.code in (401, 429):
                print(f"    HTTP {e.code} (rate limit) — waiting {THROTTLE_S}s before retry...", flush=True)
                time.sleep(THROTTLE_S)
            else:
                print(f"    HTTP {e.code}: {e}", flush=True)
                time.sleep(REST_S)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_S)
    raise RuntimeError(f"Failed to fetch '{prefix}' offset={offset} after {MAX_RETRIES} retries")


def load_progress(progress_file: Path) -> dict:
    try:
        with open(progress_file) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"completed_prefixes": [], "total_docs_written": 0}


def save_progress(progress: dict, progress_file: Path) -> None:
    with open(progress_file, "w") as f:
        json.dump(progress, f)


def run_worker(prefixes_file: Path, worker_id: int, num_workers: int):
    """Harvest the slice of prefixes assigned to this worker."""
    output_file   = PROJECT_DIR / f"mwdl-harvest-{worker_id}.jsonl"
    progress_file = PROJECT_DIR / f"mwdl-harvest-progress-{worker_id}.json"

    with open(prefixes_file) as f:
        prefix_data = json.load(f)

    queryable = prefix_data["queryable"]
    # Each worker takes every num_workers-th prefix starting at worker_id
    all_prefixes = sorted(queryable.keys())
    my_prefixes  = all_prefixes[worker_id::num_workers]
    total_expected = sum(queryable[p] for p in my_prefixes)

    progress     = load_progress(progress_file)
    completed    = set(progress["completed_prefixes"])
    docs_written = progress["total_docs_written"]
    remaining    = [p for p in my_prefixes if p not in completed]

    print(f"Worker {worker_id}/{num_workers} — MWDL Primo VE harvest", flush=True)
    print(f"  My prefixes:       {len(my_prefixes)}", flush=True)
    print(f"  Already done:      {len(completed)}", flush=True)
    print(f"  Remaining:         {len(remaining)}", flush=True)
    print(f"  Expected records:  {total_expected:,}", flush=True)
    print(f"  Docs written:      {docs_written:,}", flush=True)
    print(f"  Output:            {output_file}", flush=True)
    print("=" * 60, flush=True)

    mode = "a" if completed else "w"
    out  = open(output_file, mode)

    try:
        for i, prefix in enumerate(remaining):
            expected    = queryable[prefix]
            prefix_docs = 0
            offset      = 0

            print(f"\n[{len(completed)+i+1}/{len(my_prefixes)}] '{prefix}' ({expected} expected)", flush=True)

            while True:
                time.sleep(REST_S)
                docs, total = fetch_page(prefix, offset)

                if not docs:
                    break

                for doc in docs:
                    out.write(json.dumps(doc) + "\n")
                    prefix_docs += 1
                    docs_written += 1

                print(f"  offset={offset}: {len(docs)} docs (prefix total: {prefix_docs}/{expected})", flush=True)

                offset += LIMIT
                if offset >= total or offset > MAX_OFFSET:
                    break

            completed.add(prefix)
            progress["completed_prefixes"] = list(completed)
            progress["total_docs_written"] = docs_written
            save_progress(progress, progress_file)
            print(f"  ✓ '{prefix}': {prefix_docs} docs (total so far: {docs_written:,})", flush=True)

    except KeyboardInterrupt:
        print(f"\nInterrupted. {docs_written:,} docs written.", flush=True)
    except Exception as e:
        print(f"\nError: {e}", flush=True)
    finally:
        out.close()
        save_progress(progress, progress_file)

    print("\n" + "=" * 60, flush=True)
    print(f"Done. {docs_written:,} docs written to {output_file}", flush=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id",    type=int, default=0,
                        help="Worker index (0-based)")
    parser.add_argument("--num-workers",  type=int, default=1,
                        help="Total number of parallel workers")
    args = parser.parse_args()

    PROJECT_DIR.mkdir(parents=True, exist_ok=True)
    prefixes_file = PROJECT_DIR / "mwdl-prefixes.json"
    run_worker(prefixes_file, args.worker_id, args.num_workers)


if __name__ == "__main__":
    main()
