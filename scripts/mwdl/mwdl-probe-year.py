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
    ("DIGCOLL_BYU_12", "other",  None),
    ("DIGCOLL_UUU_11", "images", "N A"),
    ("DIGCOLL_BYU_12", "other",  "Anderson George Edward 1860 1928"),
]

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
    print(f"\n{label}: total={total:,}")
    print(f"  Facets returned: {list(returned_facets.keys())}")
    if "facet_creationdate" in returned_facets:
        years = returned_facets["facet_creationdate"]
        print(f"  Years ({len(years)}): {[(v['value'], v['count']) for v in years[:10]]}")
    else:
        print("  !! No facet_creationdate returned")
