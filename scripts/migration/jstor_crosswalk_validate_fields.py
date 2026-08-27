#!/usr/bin/env python3
"""Field-level validation of the JSTOR crosswalk, with a control group.

The media-URL -> community_num association is an undocumented inference. Where the
legacy isShownAt redirect works it independently confirms the pairing, but for
dead-redirect rows the media URL is the ONLY evidence. To gauge whether media-only
pairings are trustworthy, compare title + contributing institution between the old
2022 record (from the jsonl snapshot) and the live JSTOR record (OAI GetRecord) for:

  CONTROL : rows recovered via the redirect (crosswalk_pass2.tsv) -- known-correct
  TEST    : rows crosswalked by media URL whose redirect is dead (pass1_losses.tsv)

If TEST matches at ~the CONTROL rate, media-only pairings are sound.
"""
import csv, json, re, subprocess, random
D = "/home/ec2-user/jstor-crosswalk"
JSONL = "s3://dpla-master-dataset/artstor/jsonl/20220809_083659-artstor-MAP3_1.IndexRecord.jsonl/"
N = 30

def norm(s):
    s = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    return re.sub(r"\s+", " ", s).strip()

def toks(s):
    return set(norm(s).split())

def title_match(old, news):
    o = toks(old)
    if not o:
        return None
    for nt in news:
        n = toks(nt)
        if not n:
            continue
        j = len(o & n) / len(o | n)
        if norm(old) == norm(nt) or j >= 0.6 or o <= n or n <= o:
            return True
    return False

def inst_match(old_inst, new_src, suffix):
    oi, ns = toks(old_inst), toks(new_src)
    stop = {"the", "of", "and", "library", "libraries", "college", "university", "museum", "art", "collection", "collections"}
    if (oi - stop) & (ns - stop):
        return True
    if suffix and suffix.lower() in norm(new_src):
        return True
    return False

def sample(path, n, cols):
    rows = []
    with open(path) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        for row in r:
            if len(row) >= cols:
                rows.append(row)
    random.seed(5)
    return random.sample(rows, min(n, len(rows)))

def getrecord(comm):
    xml = subprocess.run(["curl", "-sS", "-m", "30",
        f"http://oai.forum.jstor.org/oai/?verb=GetRecord&metadataPrefix=oai_dc&identifier={comm}"],
        capture_output=True, text=True).stdout
    titles = [re.sub(r"\s+", " ", t).strip() for t in re.findall(r"<oai_dc:title>(.*?)</oai_dc:title>", xml, re.S)]
    src = re.findall(r"<oai_dc:source>(.*?)</oai_dc:source>", xml, re.S)
    exists = ("<metadata" in xml or "<record" in xml) and "idDoesNotExist" not in xml
    return titles, [re.sub(r"\s+", " ", s).strip() for s in src], exists

def main():
    control = [(r[0], r[1]) for r in sample(f"{D}/crosswalk_pass2.tsv", N, 3)]   # comm, old_oai
    test = [(r[0], r[1]) for r in sample(f"{D}/pass1_losses.tsv", N, 4)]         # comm, old_oai
    targets = {oai: None for _, oai in control + test}

    # one scan of the jsonl to pull old title + institution for the targets
    parts = subprocess.run(["aws", "s3", "ls", JSONL], capture_output=True, text=True).stdout.split()
    parts = [JSONL + p for p in parts if p.startswith("part-") and p.endswith(".txt")]
    remaining = set(targets)
    for part in parts:
        if not remaining:
            break
        p = subprocess.Popen(["aws", "s3", "cp", part, "-"], stdout=subprocess.PIPE)
        for raw in p.stdout:
            if not remaining:
                p.kill(); break
            line = raw.decode("utf-8", "replace")
            for oai in list(remaining):
                if oai not in line:
                    continue
                try:
                    d = json.loads(line); src = d.get("_source", {})
                    if src.get("_id") != oai:
                        continue
                    t = src.get("sourceResource", {}).get("title")
                    t = t[0] if isinstance(t, list) and t else (t if isinstance(t, str) else "")
                    dp = src.get("dataProvider")
                    if isinstance(dp, list):
                        dp = dp[0] if dp else {}
                    inst = dp.get("name", "") if isinstance(dp, dict) else str(dp)
                    targets[oai] = (t, inst)
                    remaining.discard(oai)
                except Exception:
                    pass
        p.wait()

    def run_group(name, rows):
        print(f"\n===== {name} (n={len(rows)}) =====")
        tm = im = both = gone = noold = 0
        for comm, oai in rows:
            suffix = (re.search(r"_([A-Za-z]+)$", oai) or [None, ""])[1]
            old = targets.get(oai)
            titles, srcs, exists = getrecord(comm)
            if not exists:
                gone += 1; print(f"  comm={comm:>10} GONE-from-feed"); continue
            if not old:
                noold += 1; print(f"  comm={comm:>10} (old record not found in jsonl)"); continue
            oldt, oldi = old
            t_ok = title_match(oldt, titles)
            i_ok = inst_match(oldi, " ".join(srcs), suffix)
            if t_ok: tm += 1
            if i_ok: im += 1
            if t_ok and i_ok: both += 1
            print(f"  comm={comm:>10} T={'Y' if t_ok else 'n'} I={'Y' if i_ok else 'n'}")
            print(f"      OLD title: {oldt[:80]!r}")
            print(f"      NEW title: {(titles[0] if titles else '')[:80]!r}")
            print(f"      OLD inst : {oldi[:50]!r}   NEW source: {(srcs[0] if srcs else '')[:50]!r}")
        n = len(rows)
        print(f"  --> title match {tm}/{n}, institution match {im}/{n}, both {both}/{n}, gone {gone}, no-old {noold}")

    run_group("CONTROL (redirect-confirmed)", control)
    run_group("TEST (media-only, dead redirect)", test)

if __name__ == "__main__":
    main()
