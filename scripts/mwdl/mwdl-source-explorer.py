#!/usr/bin/env python3
"""
MWDL source explorer — discovers harvestable query units for mwdl-harvest.py.

Partitioning strategy (4 levels):
  1. source                          — harvest directly if <= MAX_SOURCE_SIZE
  2. source + rtype                  — if rtype slice fits
  3. source + rtype + creator        — if creator slice fits
  4. source + rtype + creator + year — for slices still too large

For large remainders (records not covered by any named creator), year
partitioning is applied to the full source+rtype slice; harvest-side
deduplication prevents double-counting with creator units.

Output: mwdl-sources.json — used by mwdl-harvest.py.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Load .env from repo root if MWDL_API_KEY not already in environment
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


ALPHA_LETTERS = list("abcdefghijklmnopqrstuvwxyz")


def _api_get(facets: List[str], q: str = "any,contains,a") -> dict:
    """Single Primo VE API call with multiple facet filters; returns parsed JSON or {}."""
    params = urllib.parse.urlencode({
        "vid":         VID,
        "tab":         TAB,
        "scope":       SCOPE,
        "apikey":      API_KEY,
        "limit":       "1",
        "offset":      "0",
        "q":           q,
        "multiFacets": facets,
    }, doseq=True)
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


def fetch_info(facets: List[str]) -> "Tuple[int, Dict[str, List[Tuple[str, int]]]]":
    """Return (total, {facet_name: [(value, count), ...]}) for a multiFacets query."""
    data   = _api_get(facets)
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


def partition_by_alpha(
    base_facets: List[str],
    source: str,
    rtype: Optional[str],
    creator: Optional[str],
    year: Optional[str],
    count: int,
    indent: str,
    prefix: str = "",
) -> "List[dict]":
    """Level 5: partition by title prefix using q=title,begins_with,{prefix+letter}.

    Recurses to 2-character prefixes if a single letter is still too large.
    Alpha units overlap with other units but harvest-side dedup handles it.
    """
    units = []
    alpha_seen = 0

    for letter in ALPHA_LETTERS:
        token = prefix + letter
        time.sleep(REST_S)
        data = _api_get(base_facets, q=f"title,begins_with,{token}")
        letter_count = data.get("info", {}).get("total", 0)
        if letter_count == 0:
            continue
        alpha_seen += letter_count
        if letter_count <= MAX_SOURCE_SIZE:
            print(f"{indent}alpha={token}: {letter_count:,} ✓", flush=True)
            units.append({"source": source, "rtype": rtype, "creator": creator, "year": year, "alpha": token, "count": letter_count})
        elif len(prefix) < 1:
            # Subdivide one more level (2-char prefix)
            print(f"{indent}alpha={token}: {letter_count:,} → subdividing...", flush=True)
            units.extend(partition_by_alpha(
                base_facets, source, rtype, creator, year,
                letter_count, indent + "  ", prefix=token
            ))
        else:
            # Already at 2-char prefix — cap
            print(f"{indent}alpha={token}: {letter_count:,} !! STILL TOO LARGE — capped", flush=True)
            units.append({"source": source, "rtype": rtype, "creator": creator, "year": year, "alpha": token, "count": MAX_SOURCE_SIZE})

    no_alpha = max(0, count - alpha_seen)
    if no_alpha > 0:
        capped = min(no_alpha, MAX_SOURCE_SIZE)
        flag = " (capped)" if no_alpha > MAX_SOURCE_SIZE else ""
        print(f"{indent}no-title remainder: ~{no_alpha:,}{flag}", flush=True)
        units.append({"source": source, "rtype": rtype, "creator": creator, "year": year, "alpha": None, "count": capped})

    return units


def partition_by_year(
    base_facets: List[str],
    source: str,
    rtype: Optional[str],
    creator: Optional[str],
    count: int,
    indent: str,
) -> "List[dict]":
    """Level 4: partition a too-large slice by creationdate year.

    Note: when used for large remainders (creator=None), base_facets covers the
    full rtype slice — the resulting year units overlap with named-creator units,
    but harvest-side deduplication handles that correctly.
    """
    time.sleep(REST_S)
    _, fdata = fetch_info(base_facets)
    year_values = fdata.get("creationdate", [])

    if not year_values:
        print(f"{indent}no year facet — probing alpha...", flush=True)
        return partition_by_alpha(base_facets, source, rtype, creator, None, count, indent)

    units = []
    year_seen = 0
    for year_val, year_count in year_values:
        year_seen += year_count
        if year_count <= MAX_SOURCE_SIZE:
            print(f"{indent}year={year_val}: {year_count:,} ✓", flush=True)
            units.append({"source": source, "rtype": rtype, "creator": creator, "year": year_val, "alpha": None, "count": year_count})
        else:
            print(f"{indent}year={year_val}: {year_count:,} !! STILL TOO LARGE — probing alpha...", flush=True)
            units.extend(partition_by_alpha(
                base_facets + [f"facet_creationdate,include,{year_val}"],
                source, rtype, creator, year_val, year_count, indent + "  "
            ))

    # Records with no creation date
    no_year = max(0, count - year_seen)
    if no_year > MAX_SOURCE_SIZE:
        print(f"{indent}no-year remainder ~{no_year:,} → probing alpha...", flush=True)
        units.extend(partition_by_alpha(base_facets, source, rtype, creator, None, no_year, indent))
    elif no_year > 0:
        print(f"{indent}no-year remainder: ~{no_year:,}", flush=True)
        units.append({"source": source, "rtype": rtype, "creator": creator, "year": None, "alpha": None, "count": no_year})

    return units


def explore_source(source: str, total: int) -> "List[dict]":
    """Return harvestable units for a source using up to 4 partition levels."""
    indent = "  "

    # ── Level 1: fits directly ─────────────────────────────────────────────
    if total <= MAX_SOURCE_SIZE:
        print(f"{indent}→ direct harvest ({total:,})", flush=True)
        return [{"source": source, "rtype": None, "creator": None, "year": None, "alpha": None, "count": total}]

    # ── Level 2: partition by rtype ────────────────────────────────────────
    print(f"{indent}→ too large — probing rtype facet...", flush=True)
    time.sleep(REST_S)
    _, facets = fetch_info([f"facet_data_source,include,{source}"])
    rtype_values = facets.get("rtype", [])

    if not rtype_values:
        print(f"{indent}  no rtype facet — probing year...", flush=True)
        return partition_by_year(
            [f"facet_data_source,include,{source}"],
            source, None, None, total, indent + "  "
        )

    units = []
    rtype_total_seen = 0

    for rtype_val, rtype_count in rtype_values:
        rtype_total_seen += rtype_count

        if rtype_count <= MAX_SOURCE_SIZE:
            print(f"{indent}  rtype={rtype_val}: {rtype_count:,} ✓", flush=True)
            units.append({"source": source, "rtype": rtype_val, "creator": None, "year": None, "alpha": None, "count": rtype_count})
            continue

        # ── Level 3: rtype slice too large — partition by creator ──────────
        print(f"{indent}  rtype={rtype_val}: {rtype_count:,} → probing creator facet...", flush=True)
        time.sleep(REST_S)
        _, facets2 = fetch_info([
            f"facet_data_source,include,{source}",
            f"facet_rtype,include,{rtype_val}",
        ])
        creator_values = facets2.get("creator", [])

        if not creator_values:
            print(f"{indent}    no creator facet — probing year...", flush=True)
            units.extend(partition_by_year(
                [f"facet_data_source,include,{source}", f"facet_rtype,include,{rtype_val}"],
                source, rtype_val, None, rtype_count, indent + "    "
            ))
            continue

        creator_seen_total = 0
        for creator_val, creator_count in creator_values:
            creator_seen_total += creator_count
            if creator_count <= MAX_SOURCE_SIZE:
                print(f"{indent}    creator={creator_val[:50]!r}: {creator_count:,} ✓", flush=True)
                units.append({"source": source, "rtype": rtype_val, "creator": creator_val, "year": None, "alpha": None, "count": creator_count})
            else:
                # ── Level 4: creator slice too large — partition by year ───
                print(f"{indent}    creator={creator_val[:50]!r}: {creator_count:,} → probing year...", flush=True)
                units.extend(partition_by_year(
                    [
                        f"facet_data_source,include,{source}",
                        f"facet_rtype,include,{rtype_val}",
                        f"facet_creator,include,{creator_val}",
                    ],
                    source, rtype_val, creator_val, creator_count, indent + "      "
                ))

        # Remainder: records not covered by any known creator
        remainder = rtype_count - creator_seen_total
        if remainder > MAX_SOURCE_SIZE:
            # Partition remainder by year across the full rtype slice (dedup handles overlap)
            print(f"{indent}    remainder ~{remainder:,} → too large, probing year across full rtype slice...", flush=True)
            units.extend(partition_by_year(
                [f"facet_data_source,include,{source}", f"facet_rtype,include,{rtype_val}"],
                source, rtype_val, None, rtype_count, indent + "      "
            ))
        elif remainder > 0:
            print(f"{indent}    remainder (unpartitioned creators): ~{remainder:,}", flush=True)
            units.append({"source": source, "rtype": rtype_val, "creator": None, "year": None, "alpha": None, "count": remainder})

    # Rtype remainder
    rtype_remainder = total - rtype_total_seen
    if rtype_remainder > 0:
        print(f"{indent}  rtype remainder: ~{rtype_remainder:,}", flush=True)
        units.append({"source": source, "rtype": None, "creator": None, "year": None, "alpha": None, "count": rtype_remainder})

    return units


def main():
    all_units  = []
    capped     = []
    warnings   = []

    slack_notify(":arrow_forward: *mwdl source explorer started* — scanning 10 data sources (4-level partitioning)")
    print("MWDL source explorer — 4-level partitioning (source → rtype → creator → year)", flush=True)
    print(f"MAX_SOURCE_SIZE = {MAX_SOURCE_SIZE}", flush=True)
    print("=" * 60, flush=True)

    for source in DATA_SOURCES:
        time.sleep(REST_S)
        total, facets = fetch_info([f"facet_data_source,include,{source}"])
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
