#!/usr/bin/env python3
"""Build a JSTOR-Forum -> legacy-Artstor DPLA ID crosswalk.

Artstor was merged into JSTOR Forum (ITHAKA). The old OAI feed
(oaicat.oclc.org) minted DPLA item IDs as md5("artstor--<old_oai_id>"). The new
JSTOR Forum feed emits bare-numeric OAI header identifiers (the JSTOR "community"
object id). To re-harvest from JSTOR Forum WITHOUT re-minting DPLA IDs, the mapper
needs to recover, for each new community id, the original oai:oaicat.oclc.org id.

The new community id is already embedded in each legacy record's media URL:
    media.artstor.net/imgstor/size1/sslps/c<SSID>/<COMMUNITY_NUM>.jpg
and <COMMUNITY_NUM> == the JSTOR Forum OAI header id (verified against live
openlibrary.artstor.org redirects, which resolve to jstor.org/stable/community.<n>).

Pass 1 (this script, offline): scan the legacy jsonl snapshot in S3 and extract
community_num from the media URL. Emits a crosswalk plus miss/ambiguous lists.
Pass 2 (later, HTTP): for misses, resolve the legacy isShownAt redirect.

Usage:
    jstor_id_crosswalk.py --jsonl s3://.../artstor/jsonl/<dir>/ --out ./out [--sample 40]
"""
import argparse, json, os, re, subprocess, sys
from collections import Counter

MEDIA_RE = re.compile(r'media\.artstor\.net/[^\s"\'\\]*?/(\d+)\.(?:jpe?g|png|gif|tiff?)', re.I)


def s3_parts(prefix):
    out = subprocess.run(["aws", "s3", "ls", prefix], capture_output=True, text=True, check=True).stdout
    parts = []
    for line in out.splitlines():
        name = line.split()[-1]
        if name.startswith("part-") and name.endswith(".txt"):
            parts.append(prefix + name)
    return sorted(parts)


def stream_lines(s3uri):
    # Stream a single S3 object to stdout and yield decoded lines (avoids storing 90MB parts).
    p = subprocess.Popen(["aws", "s3", "cp", s3uri, "-"], stdout=subprocess.PIPE)
    for raw in p.stdout:
        yield raw.decode("utf-8", "replace")
    p.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True, help="S3 prefix of the legacy artstor jsonl dir (trailing /)")
    ap.add_argument("--out", required=True, help="local output dir")
    ap.add_argument("--sample", type=int, default=40, help="how many clean rows to HTTP-validate")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    parts = s3_parts(args.jsonl)
    print(f"parts: {len(parts)}", flush=True)

    total = clean = miss = ambiguous = bad = 0
    seen_community = Counter()
    xwalk_fh = open(os.path.join(args.out, "crosswalk.tsv"), "w")
    miss_fh = open(os.path.join(args.out, "misses.tsv"), "w")
    amb_fh = open(os.path.join(args.out, "ambiguous.tsv"), "w")
    xwalk_fh.write("community_num\told_oai_id\tdpla_id\n")
    miss_fh.write("old_oai_id\tdpla_id\tis_shown_at\n")
    amb_fh.write("candidates\told_oai_id\tdpla_id\n")
    sample = []

    for part in parts:
        for line in stream_lines(part):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                d = json.loads(line)
                src = d.get("_source", {})
                old_oai = src.get("_id") or d.get("_id")
                dpla_id = src.get("id")
                isa = src.get("isShownAt")
                if isinstance(isa, dict):
                    isa = isa.get("@id")
            except Exception:
                bad += 1
                continue
            if not old_oai or not dpla_id:
                bad += 1
                continue
            cands = sorted(set(MEDIA_RE.findall(line)))
            if len(cands) == 1:
                num = cands[0]
                clean += 1
                seen_community[num] += 1
                xwalk_fh.write(f"{num}\t{old_oai}\t{dpla_id}\n")
                if len(sample) < args.sample and total % 97 == 0:
                    sample.append((num, isa))
            elif len(cands) == 0:
                miss += 1
                miss_fh.write(f"{old_oai}\t{dpla_id}\t{isa}\n")
            else:
                ambiguous += 1
                amb_fh.write(f"{','.join(cands)}\t{old_oai}\t{dpla_id}\n")

    for fh in (xwalk_fh, miss_fh, amb_fh):
        fh.close()

    dupes = {k: v for k, v in seen_community.items() if v > 1}
    print("\n==== PASS 1 (offline, media-URL extraction) ====")
    print(f"total records        : {total}")
    print(f"clean single-match   : {clean}  ({100*clean/total:.2f}%)" if total else "no records")
    print(f"no media / miss      : {miss}")
    print(f"ambiguous (multi-num): {ambiguous}")
    print(f"unparseable/no-ids   : {bad}")
    print(f"duplicate community# : {len(dupes)} (community ids mapping to >1 legacy record)")
    if dupes:
        for k in list(dupes)[:10]:
            print(f"   dup {k} -> {dupes[k]} records")

    # ---- HTTP validation of a sample: does media-derived num == redirect community.<num>? ----
    if sample:
        print(f"\n==== validation: resolving {len(sample)} legacy isShownAt redirects ====")
        ok = bad_v = noisa = 0
        for num, isa in sample:
            if not isa:
                noisa += 1
                continue
            final = subprocess.run(
                ["curl", "-sS", "-m", "30", "-A", "Mozilla/5.0", "-o", "/dev/null",
                 "-w", "%{url_effective}", "-L", isa],
                capture_output=True, text=True).stdout.strip()
            m = re.search(r"community\.(\d+)", final)
            got = m.group(1) if m else None
            status = "OK" if got == num else "MISMATCH"
            if got == num:
                ok += 1
            else:
                bad_v += 1
            print(f"  [{status}] media={num} redirect={got}  {isa}")
        print(f"validation: {ok} match, {bad_v} mismatch, {noisa} no-isShownAt")


if __name__ == "__main__":
    main()
