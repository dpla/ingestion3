#!/usr/bin/env python3
"""Pass 2 of the JSTOR crosswalk: recover community ids for records that had no
usable media URL in pass 1, by resolving the legacy Artstor isShownAt redirect
(openlibrary.artstor.org/object/... -> jstor.org/stable/community.<n>).

Polite concurrency (small worker pool) so we don't hammer ITHAKA. Reads
misses.tsv from pass 1; writes recovered rows to crosswalk_pass2.tsv and
unresolved rows to losses.tsv (candidate genuine takedowns).
"""
import csv, re, subprocess, os
from concurrent.futures import ThreadPoolExecutor, as_completed

D = "/home/ec2-user/jstor-crosswalk"
MISSES = f"{D}/misses.tsv"
OUT_OK = f"{D}/crosswalk_pass2.tsv"
OUT_LOSS = f"{D}/losses.tsv"
COMM = re.compile(r"community\.(\d+)")
WORKERS = 10


def resolve(row):
    oai, dpla, isa = row
    if not isa or isa == "None":
        return ("loss", oai, dpla, "no-isShownAt")
    try:
        r = subprocess.run(
            ["curl", "-sS", "-m", "30", "-A", "Mozilla/5.0", "-o", "/dev/null",
             "-w", "%{url_effective}", "-L", isa],
            capture_output=True, text=True, timeout=45)
        final = r.stdout.strip()
    except Exception as e:
        return ("loss", oai, dpla, f"err:{type(e).__name__}")
    m = COMM.search(final)
    return ("ok", m.group(1), oai, dpla) if m else ("loss", oai, dpla, final[:80] or "no-redirect")


def main():
    rows = []
    with open(MISSES) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        for row in r:
            if len(row) >= 3:
                rows.append((row[0], row[1], row[2]))
    total = len(rows); ok = loss = done = 0
    fok = open(OUT_OK, "w"); floss = open(OUT_LOSS, "w")
    fok.write("community_num\told_oai_id\tdpla_id\n")
    floss.write("old_oai_id\tdpla_id\treason\n")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(resolve, row) for row in rows]
        for fut in as_completed(futs):
            res = fut.result(); done += 1
            if res[0] == "ok":
                _, num, oai, dpla = res; fok.write(f"{num}\t{oai}\t{dpla}\n"); ok += 1
            else:
                _, oai, dpla, why = res; floss.write(f"{oai}\t{dpla}\t{why}\n"); loss += 1
            if done % 500 == 0:
                fok.flush(); floss.flush()
                print(f"progress {done}/{total} recovered={ok} loss={loss}", flush=True)
    fok.close(); floss.close()
    print(f"DONE total={total} recovered={ok} losses={loss}", flush=True)
    with open(f"{D}/pass2.done", "w") as m:
        m.write(f"recovered={ok}\tlosses={loss}\ttotal={total}\n")


if __name__ == "__main__":
    main()
