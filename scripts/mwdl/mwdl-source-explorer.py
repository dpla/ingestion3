#!/usr/bin/env python3
"""
MWDL source explorer — discovers harvestable query units for mwdl-harvest.py.

Partitioning strategy (3 levels):
  1. source             — if total <= MAX_SOURCE_SIZE, harvest directly
  2. source + rtype     — if rtype slice <= MAX_SOURCE_SIZE, harvest
  3. source + rtype + creator — for slices still too large

For rtype slices that are too large, the explorer probes the creator
facet for that specific source+rtype. Each known creator becomes its
own unit. Records NOT captured by any known creator are added as a
"remainder" unit (source+rtype, no creator filter) relying on the
harvest script's deduplication to avoid double-counting.

Any source+rtype+creator unit still > MAX_SOURCE_SIZE is capped and
flagged — those need a 4th level or a bulk export from the provider.

Output: mwdl-sources.json — used by mwdl-harvest.py.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

API_KEY = "l8xxe1772f53c1b54de8b25553fda6e224f5"
BASE    = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VID     = "01UTAH_INST:MWDL"
TAB     = "LibraryCatalog"
SCOPE   = "MWDL"

MAX_SOURCE_SIZE = 9900
REST_S          = 12
MAX_RETRIES     = 3
TIMEOUT         = 30

DATA_SOURCES = [
    "DIGCOLL_OCA_30",
    "DIGCOLL_OCA_30_UU",
    "DIGCOLL_WSU_18",
    "DIGCOLL_BYU_25",
    "DIGCOLL_UVU_19",
    "DIGCOLL_SUU_13",
    "DIGCOLL_UNL_14",
    "DIGCOLL_UNR_15",
    "DIGCOLL_BYU_12",
    "DIGCOLL_UUU_11",
]

OUTPUT_FILE = Path("/home/ec2-user/mwdl-harvest/mwdl-sources.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def slack_notify(msg: str) -> None:
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


def _api_get(mfacets: str) -> dict:
    """Single Primo VE API call; returns parsed JSON or {}."""
    params = urllib.parse.urlencode({
        "vid":         VID,
        "tab":         TAB,
        "scope":       SCOPE,
        "apikey":      API_KEY,
        "limit":       "1",
        "offset":      "0",
        "q":           "any,contains,a",
        "multiFacets": mfacets,
    })
    req = urllib.request.Request(f"{BASE}?{params}")
    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            time.sleep(REST_S * (attempt + 1))
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read())
        except Exception as e:
            print(f"    API error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(REST_S)
    return {}


def fetch_info(mfacets: str) -> "Tuple[int, Dict[str, List[Tuple[str, int]]]]":
    """Return (total, {facet_name: [(value, count), ...]}) for a multiFacets query."""
    data   = _api_get(mfacets)
    total  = data.get("info", {}).get("total", 0)
    facets = {}
    for facet in data.get("facets", []):
        name   = facet.get("name", "")
        values = [
            (str(v["value"]), int(v.get("count", 0)))
            for v in facet.get("values", [])
            if v.get("value") is not None
        ]
        facets[name] = values
    return total, facets


def explore_source(source: str, total: int) -> "List[dict]":
    """Return harvestable units for a source using up to 3 partition levels."""
    indent = "  "

    # ── Level 1: fits directly ─────────────────────────────────────────────
    if total <= MAX_SOURCE_SIZE:
        print(f"{indent}→ direct harvest ({total:,})", flush=True)
        return [{"source": source, "rtype": None, "creator": None, "count": total}]

    # ── Level 2: partition by rtype ────────────────────────────────────────
    print(f"{indent}→ too large — probing rtype facet...", flush=True)
    time.sleep(REST_S)
    _, facets = fetch_info(f"facet_data_source,include,{source}")
    rtype_values = facets.get("rtype", [])

    if not rtype_values:
        capped = min(total, MAX_SOURCE_SIZE)
        print(f"{indent}  no rtype facet — capped direct harvest ({capped:,} of {total:,})", flush=True)
        return [{"source": source, "rtype": None, "creator": None, "count": capped}]

    units = []
    rtype_total_seen = 0

    for rtype_val, rtype_count in rtype_values:
        rtype_total_seen += rtype_count

        if rtype_count <= MAX_SOURCE_SIZE:
            print(f"{indent}  rtype={rtype_val}: {rtype_count:,} ✓", flush=True)
            units.append({"source": source, "rtype": rtype_val, "creator": None, "count": rtype_count})
            continue

        # ── Level 3: rtype slice too large — partition by creator ──────────
        print(f"{indent}  rtype={rtype_val}: {rtype_count:,} → probing creator facet...", flush=True)
        time.sleep(REST_S)
        mf2 = f"facet_data_source,include,{source}|facet_rtype,include,{rtype_val}"
        _, facets2 = fetch_info(mf2)
        creator_values = facets2.get("creator", [])

        if not creator_values:
            capped = min(rtype_count, MAX_SOURCE_SIZE)
            print(f"{indent}    no creator facet — capped ({capped:,} of {rtype_count:,})", flush=True)
            units.append({"source": source, "rtype": rtype_val, "creator": None, "count": capped})
            continue

        creator_seen_total = 0
        for creator_val, creator_count in creator_values:
            creator_seen_total += creator_count
            if creator_count <= MAX_SOURCE_SIZE:
                print(f"{indent}    creator={creator_val[:50]!r}: {creator_count:,} ✓", flush=True)
                units.append({"source": source, "rtype": rtype_val, "creator": creator_val, "count": creator_count})
            else:
                print(f"{indent}    creator={creator_val[:50]!r}: {creator_count:,} !! STILL TOO LARGE — capped at {MAX_SOURCE_SIZE}", flush=True)
                units.append({"source": source, "rtype": rtype_val, "creator": creator_val, "count": MAX_SOURCE_SIZE})

        # Remainder: records not covered by any known creator
        remainder = rtype_count - creator_seen_total
        if remainder > 0:
            print(f"{indent}    remainder (unpartitioned creators): ~{remainder:,}", flush=True)
            units.append({"source": source, "rtype": rtype_val, "creator": None, "count": remainder})

    # Rtype remainder: records not covered by any rtype value (rare but possible)
    rtype_remainder = total - rtype_total_seen
    if rtype_remainder > 0:
        print(f"{indent}  rtype remainder: ~{rtype_remainder:,}", flush=True)
        units.append({"source": source, "rtype": None, "creator": None, "count": rtype_remainder})

    return units


def main():
    all_units  = []
    capped     = []
    warnings   = []

    slack_notify(":arrow_forward: *mwdl source explorer started* — scanning 10 data sources (3-level partitioning)")
    print("MWDL source explorer — 3-level partitioning (source → rtype → creator)", flush=True)
    print(f"MAX_SOURCE_SIZE = {MAX_SOURCE_SIZE}", flush=True)
    print("=" * 60, flush=True)

    for source in DATA_SOURCES:
        time.sleep(REST_S)
        total, facets = fetch_info(f"facet_data_source,include,{source}")
        print(f"\n{source}: {total:,} records", flush=True)

        if total == 0:
            print("  → skipping (0 records)", flush=True)
            continue

        units = explore_source(source, total)
        all_units.extend(units)

        for u in units:
            label = f"{u['source']} / rtype={u['rtype'] or 'all'} / creator={str(u['creator'] or 'all')[:40]}"
            if u["count"] >= MAX_SOURCE_SIZE and (u["rtype"] or u["creator"]):
                warnings.append(f"  CAPPED: {label} ({u['count']:,})")

    total_expected = sum(u["count"] for u in all_units)

    result = {
        "units":          all_units,
        "total_expected": total_expected,
        "unit_count":     len(all_units),
        "capped_units":   [u for u in all_units if u["count"] >= MAX_SOURCE_SIZE],
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print("\n" + "=" * 60, flush=True)
    print(f"Done. {len(all_units)} harvestable units, {total_expected:,} expected records.", flush=True)
    if warnings:
        print(f"\nWARNINGS — {len(warnings)} capped units (may miss records):", flush=True)
        for w in warnings:
            print(w, flush=True)
    print(f"Output: {OUTPUT_FILE}", flush=True)

    slack_notify(
        f":white_check_mark: *mwdl source explorer complete* — "
        f"{len(all_units)} units, ~{total_expected:,} expected records"
        + (f"\n:warning: {len(warnings)} units still capped (need 4th level)" if warnings else "")
        + "\nReady to run mwdl-harvest.py."
    )


if __name__ == "__main__":
    main()
