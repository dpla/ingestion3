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
import urllib.request
import urllib.parse
from pathlib import Path

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

API_KEY = os.environ.get("MWDL_API_KEY", "")
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"
LIMIT   = 100
MAX_OFFSET   = 4900   # stay safely under 5000 per prefix bucket
REST_S       = 12
MAX_RETRIES  = 4
TIMEOUT      = 120

PROJECT_DIR   = Path("/home/ec2-user/mwdl-harvest")
PREFIXES_FILE = PROJECT_DIR / "mwdl-prefixes.json"
OUTPUT_FILE   = PROJECT_DIR / "mwdl-harvest.jsonl"
PROGRESS_FILE = PROJECT_DIR / "mwdl-harvest-progress.json"


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
            wait = REST_S * (attempt + 1)
            print(f"    Retry {attempt} for '{prefix}*' offset={offset}, waiting {wait}s...", flush=True)
            time.sleep(wait)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data  = json.loads(resp.read())
                docs  = data.get("docs", [])
                total = data.get("info", {}).get("total", 0)
                return docs, total
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_S)
    raise RuntimeError(f"Failed to fetch '{prefix}*' offset={offset} after {MAX_RETRIES} retries")


def load_progress() -> dict:
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"completed_prefixes": [], "total_docs_written": 0}


def save_progress(progress: dict) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def main():
    PROJECT_DIR.mkdir(parents=True, exist_ok=True)

    with open(PREFIXES_FILE) as f:
        prefix_data = json.load(f)

    queryable      = prefix_data["queryable"]
    prefixes       = sorted(queryable.keys())
    total_expected = sum(queryable.values())

    progress     = load_progress()
    completed    = set(progress["completed_prefixes"])
    docs_written = progress["total_docs_written"]
    remaining    = [p for p in prefixes if p not in completed]

    print("MWDL Primo VE harvest", flush=True)
    print(f"  Total prefixes:    {len(prefixes)}", flush=True)
    print(f"  Already done:      {len(completed)}", flush=True)
    print(f"  Remaining:         {len(remaining)}", flush=True)
    print(f"  Expected records:  {total_expected:,}", flush=True)
    print(f"  Docs written:      {docs_written:,}", flush=True)
    print("=" * 60, flush=True)

    mode = "a" if completed else "w"
    out  = open(OUTPUT_FILE, mode)

    try:
        for i, prefix in enumerate(remaining):
            expected     = queryable[prefix]
            prefix_docs  = 0
            offset       = 0

            print(f"\n[{len(completed)+i+1}/{len(prefixes)}] '{prefix}*' ({expected} expected)", flush=True)

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
            progress["completed_prefixes"]  = list(completed)
            progress["total_docs_written"]  = docs_written
            save_progress(progress)
            print(f"  ✓ '{prefix}*': {prefix_docs} docs (total so far: {docs_written:,})", flush=True)

    except KeyboardInterrupt:
        print(f"\nInterrupted. {docs_written:,} docs written.", flush=True)
    except Exception as e:
        print(f"\nError: {e}", flush=True)
    finally:
        out.close()
        save_progress(progress)

    print("\n" + "=" * 60, flush=True)
    print(f"Done. {docs_written:,} docs written to {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
