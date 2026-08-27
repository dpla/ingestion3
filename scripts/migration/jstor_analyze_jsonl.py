#!/usr/bin/env python3
"""Field-quality analysis over the JSTOR test-harvest JSONL (final IndexRecords)."""
import glob, gzip, json, re
from collections import Counter

JDIR = "/home/ec2-user/jstortest-full/artstor/jsonl"


def open_text(p):
    with open(p, "rb") as fh:
        magic = fh.read(2)
    return gzip.open(p, "rt", encoding="utf-8", errors="replace") if magic == b"\x1f\x8b" \
        else open(p, "r", encoding="utf-8", errors="replace")


def names(v):
    out = []
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict):
                out.append(x.get("name") or x.get("@id") or "")
            elif isinstance(x, str):
                out.append(x)
    elif isinstance(v, dict):
        out.append(v.get("name") or "")
    elif isinstance(v, str):
        out.append(v)
    return [s for s in out if s]


def main():
    parts = sorted(glob.glob(f"{JDIR}/*IndexRecord.jsonl/part-*"))
    tot = 0
    pres = Counter()
    dp = Counter(); typ = Counter(); fmt = Counter(); ext = Counter()
    mm_multi = 0; mm_present = 0
    has_creator = no_creator = no_creator_has_contrib = 0
    contrib_samples = []
    isa_ok = isa_bad = 0
    for part in parts:
        with open_text(part) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line); s = d.get("_source", d)
                tot += 1
                sr = s.get("sourceResource", {})
                cr = names(sr.get("creator")); co = names(sr.get("contributor"))
                typ.update(names(sr.get("type"))); fmt.update(names(sr.get("format"))); ext.update(names(sr.get("extent")))
                for fld, val in [("creator", cr), ("contributor", co), ("type", sr.get("type")),
                                 ("format", sr.get("format")), ("extent", sr.get("extent")),
                                 ("date", sr.get("date")), ("subject", sr.get("subject")),
                                 ("language", sr.get("language")), ("place", sr.get("spatial") or sr.get("place")),
                                 ("description", sr.get("description")), ("rights", sr.get("rights")),
                                 ("edmRights", s.get("rights")), ("dataProvider", s.get("dataProvider")),
                                 ("object", s.get("object")), ("mediaMaster", s.get("mediaMaster")),
                                 ("isShownAt", s.get("isShownAt"))]:
                    if val:
                        pres[fld] += 1
                if cr:
                    has_creator += 1
                else:
                    no_creator += 1
                    if co:
                        no_creator_has_contrib += 1
                        if len(contrib_samples) < 15:
                            contrib_samples.append((names([s.get("dataProvider")]), co, names(sr.get("title"))[:1]))
                dp.update(names([s.get("dataProvider")]))
                mm = s.get("mediaMaster") or []
                if mm:
                    mm_present += 1
                    if isinstance(mm, list) and len(mm) > 1:
                        mm_multi += 1
                isa = s.get("isShownAt") or ""
                if isinstance(isa, str) and re.match(r"https://www\.jstor\.org/stable/10\.2307/community\.\d+$", isa):
                    isa_ok += 1
                elif isa:
                    isa_bad += 1

    print(f"TOTAL jsonl records: {tot}\n")
    print("=== field presence (% of records) ===")
    for k in ["isShownAt", "dataProvider", "object", "mediaMaster", "rights", "edmRights", "title" if False else "creator",
              "contributor", "type", "format", "extent", "date", "subject", "language", "place", "description"]:
        print(f"  {k:14s} {pres.get(k,0):7d}  ({100*pres.get(k,0)/tot:.1f}%)")
    print(f"\nisShownAt matches community-URL pattern: {isa_ok}/{tot}  (non-matching: {isa_bad})")
    print(f"mediaMaster present: {mm_present} ({100*mm_present/tot:.1f}%), multi-value: {mm_multi}")
    print(f"\ncreator present: {has_creator}  | absent: {no_creator}  | absent-but-has-contributor: {no_creator_has_contrib}")
    print("  sample [dataProvider | contributor(s) | title] where creator absent but contributor present:")
    for dpn, co, ti in contrib_samples:
        print(f"    {dpn} | {co} | {ti}")
    print(f"\n=== dataProvider distinct: {len(dp)} (top 30) ===")
    for v, c in dp.most_common(30):
        print(f"  {c:7d}  {v[:70]}")
    print(f"\n=== type distinct: {len(typ)} (top 30) ===")
    for v, c in typ.most_common(30):
        print(f"  {c:7d}  {v[:60]!r}")
    print(f"\n=== format distinct: {len(fmt)} (top 30) ===")
    for v, c in fmt.most_common(30):
        print(f"  {c:7d}  {v[:60]!r}")
    print(f"\n=== extent distinct: {len(ext)} (top 20) ===")
    for v, c in ext.most_common(20):
        print(f"  {c:7d}  {v[:60]!r}")


if __name__ == "__main__":
    main()
