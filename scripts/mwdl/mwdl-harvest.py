#!/usr/bin/env python3
"""
Harvest all MWDL Primo VE records using data_source + year partitioning.

Reads harvestable query units from mwdl-sources.json (produced by
mwdl-source-explorer.py), paginates through each unit, and writes all
unique records to a single JSONL file. Deduplicates by record ID.

Output: /home/ec2-user/mwdl-harvest/mwdl-harvest.jsonl
"""

import json
import os
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Optional

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


def slack_notify(msg: str) -> None:
    return  # disabled
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

API_KEY = _read_api_key()
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"
LIMIT   = 100

REST_BETWEEN_QUERIES = 12
MAX_RETRIES          = 3
TIMEOUT              = 30
MAX_OFFSET           = 9900   # Primo VE hard limit

PROJECT_DIR   = Path("/home/ec2-user/mwdl-harvest")
SOURCES_FILE  = PROJECT_DIR / "mwdl-sources.json"
OUTPUT_FILE   = PROJECT_DIR / "mwdl-harvest.jsonl"
PROGRESS_FILE = PROJECT_DIR / "mwdl-harvest-progress.json"


def unit_key(unit: dict) -> str:
    return (
        f"{unit['source']}|{unit.get('rtype') or 'all'}"
        f"|{unit.get('creator') or 'all'}|{unit.get('year') or 'all'}"
        f"|{unit.get('alpha') or 'all'}"
    )


def build_mfacets(unit: dict) -> "list[str]":
    parts = [f"facet_data_source,include,{unit['source']}"]
    if unit.get("rtype"):
        parts.append(f"facet_rtype,include,{unit['rtype']}")
    if unit.get("creator"):
        parts.append(f"facet_creator,include,{unit['creator']}")
    if unit.get("year"):
        parts.append(f"facet_creationdate,include,{unit['year']}")
    return parts


def fetch_page(unit: dict, offset: int) -> "tuple[list, int]":
    q = f"title,begins_with,{unit['alpha']}" if unit.get("alpha") else "any,contains,a"
    params = urllib.parse.urlencode({
        "vid":         VID,
        "tab":         TAB,
        "scope":       SCOPE,
        "apikey":      API_KEY,
        "limit":       str(LIMIT),
        "offset":      str(offset),
        "q":           q,
        "multiFacets": build_mfacets(unit),
    }, doseq=True)
    url = f"{BASE}?{params}"
    req = urllib.request.Request(url)

    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = REST_BETWEEN_QUERIES * (attempt + 1)
            print(f"    Retry {attempt} (offset={offset}), waiting {wait}s...", flush=True)
            time.sleep(wait)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data  = json.loads(resp.read())
                docs  = data.get("docs", [])
                total = data.get("info", {}).get("total", 0)
                return docs, total
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(REST_BETWEEN_QUERIES)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries (unit={unit_key(unit)}, offset={offset})")


def get_record_id(doc: dict) -> Optional[str]:
    control  = doc.get("pnx", {}).get("control", {})
    if isinstance(control, list):
        control = control[0] if control else {}
    recordid = control.get("recordid")
    if isinstance(recordid, list):
        return str(recordid[0]) if recordid else None
    return str(recordid) if recordid else None


def load_progress() -> dict:
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"completed_units": [], "total_docs_written": 0, "seen_ids_count": 0}


def save_progress(progress: dict) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def main():
    PROJECT_DIR.mkdir(parents=True, exist_ok=True)

    with open(SOURCES_FILE) as f:
        sources_data = json.load(f)

    units          = sources_data["units"]
    total_expected = sources_data["total_expected"]

    progress     = load_progress()
    completed    = set(progress["completed_units"])
    docs_written = progress["total_docs_written"]
    remaining    = [u for u in units if unit_key(u) not in completed]

    # Load seen IDs into memory for deduplication
    seen_ids = set()  # type: ignore
    if completed and OUTPUT_FILE.exists():
        print("Loading seen IDs for deduplication...", flush=True)
        with open(OUTPUT_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                    rid = get_record_id(doc)
                    if rid:
                        seen_ids.add(rid)
                except Exception:
                    pass
        print(f"  Loaded {len(seen_ids):,} seen IDs", flush=True)

    slack_notify(
        f":arrow_forward: *mwdl harvest started* — "
        f"{len(remaining)} units remaining, {total_expected:,} expected records"
    )
    print("MWDL Primo VE harvest", flush=True)
    print(f"  Total units:      {len(units)}", flush=True)
    print(f"  Already done:     {len(completed)}", flush=True)
    print(f"  Remaining:        {len(remaining)}", flush=True)
    print(f"  Expected records: {total_expected:,}", flush=True)
    print(f"  Docs written:     {docs_written:,}", flush=True)
    print("=" * 60, flush=True)

    mode = "a" if completed else "w"
    out  = open(OUTPUT_FILE, mode)

    try:
        for i, unit in enumerate(remaining):
            expected = unit["count"]
            key      = unit_key(unit)
            label    = (
                f"{unit['source']}"
                + (f" / rtype={unit['rtype']}" if unit.get("rtype") else "")
                + (f" / creator={str(unit['creator'])[:40]}" if unit.get("creator") else "")
                + (f" / year={unit['year']}" if unit.get("year") else "")
                + (f" / alpha={unit['alpha']}" if unit.get("alpha") else "")
            )

            unit_written  = 0
            unit_dupes    = 0
            offset        = 0

            print(f"\n[{len(completed)+i+1}/{len(units)}] {label} ({expected} expected)", flush=True)

            while offset <= MAX_OFFSET:
                time.sleep(REST_BETWEEN_QUERIES)
                docs, total = fetch_page(unit, offset)

                if not docs:
                    break

                for doc in docs:
                    rid = get_record_id(doc)
                    if rid and rid in seen_ids:
                        unit_dupes += 1
                        continue
                    if rid:
                        seen_ids.add(rid)
                    out.write(json.dumps(doc) + "\n")
                    unit_written += 1
                    docs_written += 1

                print(f"  offset={offset}: {len(docs)} fetched, {unit_written} new, {unit_dupes} dupes", flush=True)

                offset += LIMIT
                if offset > total or offset > MAX_OFFSET:
                    break

            completed.add(key)
            progress["completed_units"]   = list(completed)
            progress["total_docs_written"] = docs_written
            progress["seen_ids_count"]    = len(seen_ids)
            save_progress(progress)
            print(f"  ✓ {label}: {unit_written} new records (total so far: {docs_written:,})", flush=True)

    except KeyboardInterrupt:
        print(f"\nInterrupted. Progress saved. {docs_written:,} docs written.", flush=True)
        slack_notify(f":warning: *mwdl harvest interrupted* — {docs_written:,} docs written so far")
    except Exception as e:
        print(f"\nError: {e}", flush=True)
        print(f"Progress saved. {docs_written:,} docs written.", flush=True)
        slack_notify(f":x: *mwdl harvest ERROR* — {e}\n{docs_written:,} docs written so far")
    finally:
        out.close()
        save_progress(progress)

    print("\n" + "=" * 60, flush=True)
    print("Harvest complete!", flush=True)
    print(f"  Unique docs written: {docs_written:,}", flush=True)
    print(f"  Expected:            {total_expected:,}", flush=True)
    print(f"  Output:              {OUTPUT_FILE}", flush=True)
    slack_notify(
        f":white_check_mark: *mwdl harvest complete* — "
        f"{docs_written:,} unique records written. Ready to run mwdl-jsonl-to-avro.py."
    )


if __name__ == "__main__":
    main()
