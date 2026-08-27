#!/usr/bin/env python3
"""Verify pass-1 crosswalk rows against the live JSTOR site.

Pass 1 derived community_num offline from each legacy record's media URL, but
never confirmed the item still exists on JSTOR. This pass resolves each pass-1
row's legacy Artstor isShownAt (reconstructed from the old OAI id) through its
redirect and checks:
  - LOSS     : does not resolve to jstor.org/stable/...community.<n>
               (e.g. lands on /artstor-page-not-found => taken down since 2022)
  - MISMATCH : resolves to community.<n> but <n> != the media-derived community_num
               (=> pass-1 media extraction was wrong for that row)
  - OK       : resolves and matches

Input : crosswalk.tsv (pass 1: community_num, old_oai_id, dpla_id)
Output: pass1_losses.tsv, pass1_mismatch.tsv, pass1_verify.done
"""
import csv, re, subprocess, os
from concurrent.futures import ThreadPoolExecutor, as_completed

D = "/home/ec2-user/jstor-crosswalk"
IN = f"{D}/crosswalk.tsv"
LOSS = f"{D}/pass1_losses.tsv"
MISM = f"{D}/pass1_mismatch.tsv"
LOG = f"{D}/pass1_verify.log"
COMM = re.compile(r"community\.(\d+)")
OAI_PREFIX = "oai:oaicat.oclc.org:"
BASE = "http://search.openlibrary.artstor.org/object/"
WORKERS = 20


def isa_from_oai(old_oai):
    ssid = old_oai[len(OAI_PREFIX):] if old_oai.startswith(OAI_PREFIX) else old_oai
    return BASE + ssid


def check(row):
    comm, oai, dpla = row
    isa = isa_from_oai(oai)
    try:
        r = subprocess.run(
            ["curl", "-sS", "-m", "30", "-A", "Mozilla/5.0", "-o", "/dev/null",
             "-r", "0-0", "-L", "-w", "%{url_effective}", isa],
            capture_output=True, text=True, timeout=45)
        final = r.stdout.strip()
    except Exception as e:
        return ("loss", comm, oai, dpla, f"err:{type(e).__name__}")
    m = COMM.search(final)
    if not m:
        return ("loss", comm, oai, dpla, final[:90] or "no-redirect")
    if m.group(1) != comm:
        return ("mismatch", comm, oai, dpla, m.group(1))
    return ("ok", comm, oai, dpla, "")


def main():
    rows = []
    with open(IN) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        for row in r:
            if len(row) >= 3:
                rows.append((row[0], row[1], row[2]))
    total = len(rows); ok = loss = mism = done = 0
    floss = open(LOSS, "w"); fmis = open(MISM, "w"); flog = open(LOG, "w")
    floss.write("community_num\told_oai_id\tdpla_id\tfinal_url\n")
    fmis.write("media_community_num\tredirect_community_num\told_oai_id\tdpla_id\n")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(check, row) for row in rows]
        for fut in as_completed(futs):
            kind, comm, oai, dpla, extra = fut.result(); done += 1
            if kind == "ok":
                ok += 1
            elif kind == "loss":
                loss += 1; floss.write(f"{comm}\t{oai}\t{dpla}\t{extra}\n")
            else:
                mism += 1; fmis.write(f"{comm}\t{extra}\t{oai}\t{dpla}\n")
            if done % 2000 == 0:
                floss.flush(); fmis.flush()
                flog.write(f"progress {done}/{total} ok={ok} loss={loss} mismatch={mism}\n"); flog.flush()
    floss.close(); fmis.close()
    flog.write(f"DONE total={total} ok={ok} loss={loss} mismatch={mism}\n"); flog.close()
    with open(f"{D}/pass1_verify.done", "w") as m:
        m.write(f"ok={ok}\tloss={loss}\tmismatch={mism}\ttotal={total}\n")


if __name__ == "__main__":
    main()
