#!/usr/bin/env python3
"""
Harvest all MWDL Primo VE records using data_source + title-prefix partitioning.

Reads mwdl-prefixes.json (produced by mwdl-prefix-explorer.py).
Bucket keys are "{data_source}::{prefix}" — each bucket is fetched with
  q=title,begins_with,{prefix}
  multiFacets=facet_data_source,include,{data_source}

Usage:
    python3 mwdl-harvest.py                          # single worker, all buckets
    python3 mwdl-harvest.py --worker-id 0 --num-workers 2   # worker 0 of 2
"""

import argparse
import json
import time
import urllib.error
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


API_KEY = _read_api_key()
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"
LIMIT   = 100
MAX_OFFSET  = 4900
REST_S      = 20
THROTTLE_S  = 300    # wait on 401/429 or empty response
MAX_RETRIES = 10     # each retry waits THROTTLE_S → up to ~50 min before skipping a bucket
TIMEOUT     = 120

PROJECT_DIR = Path("/home/ec2-user/mwdl-harvest")


def fetch_page(prefix: str, data_source: str, offset: int) -> "tuple[list, int]":
    params = {
        "vid":    VID,
        "tab":    TAB,
        "scope":  SCOPE,
        "apikey": API_KEY,
        "limit":  str(LIMIT),
        "offset": str(offset),
        "q":      f"title,begins_with,{prefix}",
        "multiFacets": f"facet_data_source,include,{data_source}",
    }
    req = urllib.request.Request(f"{BASE}?{urllib.parse.urlencode(params)}")
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            print(f"    Retry {attempt} for [{data_source}] '{prefix}' offset={offset}...", flush=True)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
            try:
                data  = json.loads(raw)
            except json.JSONDecodeError:
                print(f"    Empty/non-JSON response (throttle) — waiting {THROTTLE_S}s...", flush=True)
                time.sleep(THROTTLE_S)
                continue
            docs  = data.get("docs", [])
            total = data.get("info", {}).get("total", 0)
            return docs, total
        except urllib.error.HTTPError as e:
            if e.code in (401, 429):
                print(f"    HTTP {e.code} (rate limit) — waiting {THROTTLE_S}s...", flush=True)
                time.sleep(THROTTLE_S)
            else:
                print(f"    HTTP {e.code}: {e}", flush=True)
                time.sleep(REST_S)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_S)
    raise RuntimeError(f"Failed [{data_source}] '{prefix}' offset={offset} after {MAX_RETRIES} retries")


def load_progress(progress_file: Path) -> dict:
    try:
        with open(progress_file) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"completed_buckets": [], "failed_buckets": [], "total_docs_written": 0}


def save_progress(progress: dict, progress_file: Path) -> None:
    with open(progress_file, "w") as f:
        json.dump(progress, f)


def run_worker(worker_id: int, num_workers: int):
    prefixes_file = PROJECT_DIR / "mwdl-prefixes.json"
    output_file   = PROJECT_DIR / f"mwdl-harvest-{worker_id}.jsonl"
    progress_file = PROJECT_DIR / f"mwdl-harvest-progress-{worker_id}.json"

    with open(prefixes_file) as f:
        prefix_data = json.load(f)

    queryable = prefix_data["queryable"]  # {"DS::prefix": count, ...}
    all_keys  = sorted(queryable.keys())
    my_keys   = all_keys[worker_id::num_workers]
    total_expected = sum(queryable[k] for k in my_keys)

    progress     = load_progress(progress_file)
    completed    = set(progress["completed_buckets"])
    failed       = set(progress.get("failed_buckets", []))
    docs_written = progress["total_docs_written"]
    remaining    = [k for k in my_keys if k not in completed and k not in failed]

    print(f"Worker {worker_id}/{num_workers} — MWDL harvest", flush=True)
    print(f"  My buckets:        {len(my_keys)}", flush=True)
    print(f"  Already done:      {len(completed)}", flush=True)
    print(f"  Previously failed: {len(failed)}", flush=True)
    print(f"  Remaining:         {len(remaining)}", flush=True)
    print(f"  Expected records:  {total_expected:,}", flush=True)
    print(f"  Output:            {output_file}", flush=True)
    print("=" * 60, flush=True)

    mode = "a" if (completed or failed) else "w"
    out  = open(output_file, mode)

    interrupted = False
    try:
        for i, key in enumerate(remaining):
            data_source, prefix = key.split("::", 1)
            expected    = queryable[key]
            bucket_docs = 0
            offset      = 0

            print(f"\n[{len(completed)+i+1}/{len(my_keys)}] [{data_source}] '{prefix}' ({expected} expected)", flush=True)

            bucket_failed = False
            while True:
                time.sleep(REST_S)
                try:
                    docs, total = fetch_page(prefix, data_source, offset)
                except RuntimeError as e:
                    # Exhausted all retries for this page — skip the bucket, keep going
                    print(f"  SKIP [{data_source}] '{prefix}': {e}", flush=True)
                    failed.add(key)
                    progress["failed_buckets"]    = list(failed)
                    progress["total_docs_written"] = docs_written
                    save_progress(progress, progress_file)
                    bucket_failed = True
                    break

                if not docs:
                    break

                for doc in docs:
                    out.write(json.dumps(doc) + "\n")
                    bucket_docs += 1
                    docs_written += 1

                print(f"  offset={offset}: {len(docs)} docs (bucket total: {bucket_docs}/{expected})", flush=True)

                offset += LIMIT
                if offset >= total or offset > MAX_OFFSET:
                    break

            if not bucket_failed:
                completed.add(key)
                progress["completed_buckets"]  = list(completed)
                progress["total_docs_written"] = docs_written
                save_progress(progress, progress_file)
                print(f"  ✓ [{data_source}] '{prefix}': {bucket_docs} docs (total so far: {docs_written:,})", flush=True)

    except KeyboardInterrupt:
        interrupted = True
        print(f"\nInterrupted. {docs_written:,} docs written.", flush=True)
    finally:
        out.close()
        save_progress(progress, progress_file)

    print("\n" + "=" * 60, flush=True)
    print(f"Done. {docs_written:,} docs written to {output_file}", flush=True)
    if failed:
        print(f"  WARNING: {len(failed)} bucket(s) skipped due to persistent errors:", flush=True)
        for b in sorted(failed):
            print(f"    {b}", flush=True)
    if interrupted or failed:
        import sys
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id",   type=int, default=0)
    parser.add_argument("--num-workers", type=int, default=1)
    args = parser.parse_args()
    PROJECT_DIR.mkdir(parents=True, exist_ok=True)
    run_worker(args.worker_id, args.num_workers)


if __name__ == "__main__":
    main()
