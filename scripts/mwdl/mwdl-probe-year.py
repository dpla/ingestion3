#!/usr/bin/env python3
"""Probe whether facet_creationdate is available for large rtype slices."""
import json, os, sys, urllib.request, urllib.parse
from pathlib import Path

def _load_env():
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

probes = [
    ("DIGCOLL_UUU_11", "images", None),
    ("DIGCOLL_UUU_11", "other",  None),
    ("DIGCOLL_UUU_11", "images", "N A"),
    ("DIGCOLL_BYU_12", "other",  "Anderson George Edward 1860 1928"),
]

CHECK_FACETS = ["topic", "lds02"]

for src, rtype, creator in probes:
    facets = [
        f"facet_data_source,include,{src}",
        f"facet_rtype,include,{rtype}",
    ]
    if creator:
        facets.append(f"facet_creator,include,{creator}")

    params = urllib.parse.urlencode({
        "vid":         "01UTAH_INST:MWDL",
        "tab":         "LibraryCatalog",
        "scope":       "MWDL",
        "apikey":      API_KEY,
        "limit":       "1",
        "offset":      "0",
        "q":           "any,contains,a",
        "multiFacets": facets,
    }, doseq=True)

    data  = json.loads(urllib.request.urlopen(f"{BASE}?{params}", timeout=30).read())
    total = data.get("info", {}).get("total", 0)
    returned_facets = {f["name"]: f.get("values", []) for f in data.get("facets", [])}

    label = f"{src}/{rtype}" + (f"/creator={creator[:30]}" if creator else "")
    print(f"\n{'='*60}")
    print(f"{label}: total={total:,}")
    print(f"  All facets: {list(returned_facets.keys())}")

    for fname in CHECK_FACETS:
        if fname in returned_facets:
            vals = returned_facets[fname]
            val_sum = sum(int(v.get("count", 0)) for v in vals)
            print(f"\n  {fname} ({len(vals)} values, sum={val_sum:,}):")
            for v in vals[:30]:
                print(f"    {v.get('value')}: {int(v.get('count', 0)):,}")
            if len(vals) > 30:
                print(f"    ... and {len(vals)-30} more")
        else:
            print(f"\n  !! No {fname} facet returned")
