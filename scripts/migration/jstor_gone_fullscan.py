#!/usr/bin/env python3
"""Scan every 'gone' crosswalk community id (present in the crosswalk but absent
from the JSTOR Forum test harvest) against the live OAI feed via GetRecord, to
determine whether they still exist. Classifies each as:
  exists_set     - record present WITH a <setSpec> (in a set)
  exists_setless - record present with NO <setSpec> (de-listed from all sets)
  deleted        - OAI delete tombstone (status="deleted")
  nonexistent    - idDoesNotExist (not in the OAI feed at all)
Splits by crosswalk pass (1 = reliable media-derived id, 2 = suspect redirect id)
and, for exists_set, whether the set is in our harvested setlist.
Best-effort, bounded concurrency. Writes gone_status.tsv + a printed summary.
"""
import csv, glob, re, subprocess
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import fastavro

OUT = "/home/ec2-user/jstortest-full"
HARV = sorted(glob.glob(f"{OUT}/artstor/harvest/*-artstor-OriginalRecord.avro/part-*.avro"))
XW1 = "/home/ec2-user/jstor-crosswalk/crosswalk.tsv"          # pass 1 (media)
XW2 = "/home/ec2-user/jstor-crosswalk/crosswalk_pass2.tsv"    # pass 2 (redirect)
CONF = "/home/ec2-user/ingestion3-conf/i3.conf"
WORKERS = 20
SETSPEC = re.compile(r"<setSpec>([^<]+)</setSpec>")


def load_nums(path):
    s = set()
    with open(path) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        for row in r:
            if row:
                s.add(row[0].strip())
    return s


def main():
    our = set(re.search(r'artstor\.harvest\.setlist\s*=\s*"([^"]+)"', open(CONF).read()).group(1).split(","))
    harvest = set()
    for p in HARV:
        with open(p, "rb") as f:
            for r in fastavro.reader(f):
                harvest.add(str(r["id"]).strip())
    p1, p2 = load_nums(XW1), load_nums(XW2)
    allx = p1 | p2
    gone = [c for c in allx if c not in harvest]
    print(f"harvest={len(harvest)} crosswalk={len(allx)} gone={len(gone)}", flush=True)

    def classify(comm):
        xml = subprocess.run(
            ["curl", "-sS", "-m", "25", f"http://oai.forum.jstor.org/oai/?verb=GetRecord&metadataPrefix=oai_dc&identifier={comm}"],
            capture_output=True, text=True).stdout
        if "idDoesNotExist" in xml:
            return comm, "nonexistent", ""
        if 'status="deleted"' in xml:
            return comm, "deleted", ""
        if "<metadata" in xml:
            specs = SETSPEC.findall(xml)
            return comm, ("exists_set" if specs else "exists_setless"), "|".join(specs)
        return comm, "other", ""

    status = Counter()
    by_pass = {"1": Counter(), "2": Counter()}
    set_in_our = set_not_our = 0
    done = 0
    with open(f"{OUT}/gone_status.tsv", "w") as out:
        out.write("community_num\tpass\tstatus\tsetspecs\tset_in_our_setlist\n")
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for comm, st, specs in ex.map(classify, gone):
                pss = "1" if comm in p1 else "2"
                inour = ""
                if st == "exists_set":
                    specset = set(specs.split("|"))
                    if specset & our:
                        set_in_our += 1; inour = "yes"
                    else:
                        set_not_our += 1; inour = "no"
                status[st] += 1
                by_pass[pss][st] += 1
                out.write(f"{comm}\t{pss}\t{st}\t{specs}\t{inour}\n")
                done += 1
                if done % 3000 == 0:
                    print(f"  {done}/{len(gone)} ...", flush=True)

    n = len(gone)
    print(f"\n==== GONE-ID SCAN ({n} ids) ====")
    for k in ["exists_set", "exists_setless", "deleted", "nonexistent", "other"]:
        print(f"  {k:14s} {status.get(k,0):6d}  ({100*status.get(k,0)/n:.1f}%)")
    print(f"\n  of exists_set: set IS in our setlist (real miss/bug): {set_in_our}   set NOT in our setlist: {set_not_our}")
    print(f"\n  by crosswalk pass:")
    for p in ("1", "2"):
        tot = sum(by_pass[p].values())
        print(f"   pass{p} (n={tot}): " + ", ".join(f"{k}={by_pass[p][k]}" for k in ["exists_set","exists_setless","deleted","nonexistent"] if by_pass[p][k]))
    still = status.get("exists_set", 0) + status.get("exists_setless", 0)
    print(f"\n  STILL EXISTS but not harvested: {still} ({100*still/n:.1f}%)  [setless={status.get('exists_setless',0)}]")


if __name__ == "__main__":
    main()
