#!/usr/bin/env python3
"""Compare the freshly-mapped/enriched JSTOR IndexRecords against the existing
2022 Artstor IndexRecords for the confirmed same-object pairs, to see where the
current IthakaMapping output diverges from what is live (i.e. what the mapper
still needs)."""
import csv, json, subprocess, glob, gzip


def open_text(path):
    with open(path, "rb") as fh:
        magic = fh.read(2)
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") if magic == b"\x1f\x8b" \
        else open(path, "r", encoding="utf-8", errors="replace")

D = "/home/ec2-user/jstortest"
OLD_JSONL = "s3://dpla-master-dataset/artstor/jsonl/20220809_083659-artstor-MAP3_1.IndexRecord.jsonl/"


def field(src, *path, first=False):
    cur = src
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    if first and isinstance(cur, list):
        return cur[0] if cur else None
    return cur


def name_of(v):
    if isinstance(v, dict):
        return v.get("name") or v.get("@id")
    if isinstance(v, list) and v:
        return name_of(v[0])
    return v


def brief(v, n=70):
    if v is None:
        return "-"
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    s = " ".join(s.split())
    return s[:n]


def main():
    pairs = []
    with open(f"{D}/pairs.tsv") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            pairs.append(row)
    new_by_id, old_by_id = {}, {}

    newf = sorted(glob.glob(f"{D}/artstor/jsonl/*IndexRecord.jsonl/part-*"))[-1]
    with open_text(newf) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            src = d.get("_source", d)
            new_by_id[src.get("id")] = src

    old_targets = {p["old_dpla_id"] for p in pairs}
    parts = subprocess.run(["aws", "s3", "ls", OLD_JSONL], capture_output=True, text=True).stdout.split()
    parts = [OLD_JSONL + p for p in parts if p.startswith("part-") and p.endswith(".txt")]
    remaining = set(old_targets)
    for part in parts:
        if not remaining:
            break
        p = subprocess.Popen(["aws", "s3", "cp", part, "-"], stdout=subprocess.PIPE)
        for raw in p.stdout:
            if not remaining:
                p.kill(); break
            line = raw.decode("utf-8", "replace")
            hit = next((t for t in remaining if t in line), None)
            if not hit:
                continue
            try:
                d = json.loads(line); src = d.get("_source", d)
                if src.get("id") in remaining:
                    old_by_id[src.get("id")] = src; remaining.discard(src.get("id"))
            except Exception:
                pass
        p.wait()

    FIELDS = [
        ("provider", lambda s: name_of(s.get("provider"))),
        ("dataProvider", lambda s: name_of(s.get("dataProvider"))),
        ("isShownAt", lambda s: s.get("isShownAt")),
        ("object(thumb)", lambda s: s.get("object")),
        ("title", lambda s: field(s, "sourceResource", "title", first=True)),
        ("type", lambda s: field(s, "sourceResource", "type")),
        ("format", lambda s: field(s, "sourceResource", "format")),
        ("extent", lambda s: field(s, "sourceResource", "extent")),
        ("rights", lambda s: field(s, "sourceResource", "rights", first=True)),
        ("edmRights", lambda s: s.get("rights") or field(s, "sourceResource", "edmRights")),
        ("creator", lambda s: field(s, "sourceResource", "creator")),
        ("date", lambda s: field(s, "sourceResource", "date")),
        ("place", lambda s: field(s, "sourceResource", "spatial") or field(s, "sourceResource", "place")),
    ]
    for p in pairs:
        new = new_by_id.get(p["new_dpla_id"]); old = old_by_id.get(p["old_dpla_id"])
        print("\n" + "=" * 78)
        print(f"community={p['community_num']}  old_dpla={p['old_dpla_id'][:12]}  new_dpla={p['new_dpla_id'][:12]}")
        if not new:
            print("  (new record dropped in mapping)");
        if not old:
            print("  (old record not found)")
        if not (new and old):
            continue
        for label, fn in FIELDS:
            try:
                nv, ov = fn(new), fn(old)
            except Exception:
                nv = ov = "ERR"
            flag = "" if brief(nv) == brief(ov) else "  <-- DIFF"
            print(f"  {label:14s} NEW: {brief(nv):72s}{flag}")
            print(f"  {'':14s} OLD: {brief(ov)}")


if __name__ == "__main__":
    main()
