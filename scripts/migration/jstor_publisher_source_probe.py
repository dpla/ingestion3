#!/usr/bin/env python3
"""Investigate whether JSTOR's oai_dc:publisher is a reliable *contributing
institution* source for dataProvider, or whether it conflates the actual
publisher (a separate concept + a separate DPLA field). Samples community ids
from the crosswalk, GETs each GetRecord, and reports:
  - presence of publisher (in <about> vs <metadata>) and dc:source, multiplicity
  - the universe of distinct publisher values (to spot non-institutional ones)
  - publisher-vs-source (dis)agreement where both are present
"""
import csv, re, subprocess, random, urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

XWALK = "/home/ec2-user/jstor-crosswalk/jstor_id_crosswalk.tsv"
N = 500
WORKERS = 24
META_RE = re.compile(r"<metadata>(.*?)</metadata>", re.S)
ABOUT_RE = re.compile(r"<about>(.*?)</about>", re.S)
PUB_RE = re.compile(r"<(?:oai_dc:|dc:)?publisher>(.*?)</(?:oai_dc:|dc:)?publisher>", re.S)
SRC_RE = re.compile(r"<(?:oai_dc:|dc:)?source>(.*?)</(?:oai_dc:|dc:)?source>", re.S)
INST_WORDS = ("university", "college", "library", "libraries", "museum", "institute",
              "archives", "society", "historical", "foundation", "gallery", "collection",
              "school", "academy", "seminary", "center", "centre")
PUB_WORDS = ("press", "publishing", "publisher", "publications", "journal", "books",
             "company", "co.", "inc", "ltd", "media", "productions")


def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def get(cid):
    url = f"http://oai.forum.jstor.org/oai/?verb=GetRecord&metadataPrefix=oai_dc&identifier={cid}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "DPLA-ingest"}), timeout=25) as r:
            return cid, r.read().decode("utf-8", "replace")
    except Exception:
        return cid, ""


def looks_publisher(v):
    lo = v.lower()
    return any(w in lo for w in PUB_WORDS) and not any(w in lo for w in INST_WORDS)


def main():
    ids = []
    with open(XWALK) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        for row in r:
            if row:
                ids.append((row[0], row[1]))  # community, old_oai
    random.seed(13)
    sample = random.sample(ids, min(N, len(ids)))
    xml_by = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for cid, xml in ex.map(lambda t: get(t[0]), sample):
            xml_by[cid] = xml

    tot = have_about_pub = have_meta_pub = have_src = multi_pub = both = agree = disagree = 0
    pub_values = Counter()
    nonInst = []
    disagreements = []
    for cid, old_oai in sample:
        xml = xml_by.get(cid, "")
        if "<record" not in xml:
            continue
        tot += 1
        mb = META_RE.search(xml); ab = ABOUT_RE.search(xml)
        meta_pub = [norm(p) for p in PUB_RE.findall(mb.group(1) if mb else "") if norm(p)]
        about_pub = [norm(p) for p in PUB_RE.findall(ab.group(1) if ab else "") if norm(p)]
        srcs = [norm(s) for s in SRC_RE.findall(mb.group(1) if mb else "") if norm(s)]
        pubs = about_pub + meta_pub
        if about_pub: have_about_pub += 1
        if meta_pub: have_meta_pub += 1
        if srcs: have_src += 1
        if len(pubs) > 1: multi_pub += 1
        for p in pubs:
            pub_values[p] += 1
            if looks_publisher(p):
                nonInst.append((cid, p, srcs[:1]))
        if pubs and srcs:
            both += 1
            ptok = set(re.sub(r"[^a-z0-9 ]", " ", pubs[0].lower()).split())
            stok = set(re.sub(r"[^a-z0-9 ]", " ", " ".join(srcs).lower()).split())
            stop = {"the", "of", "and", "a", "department", "special", "collections", "library", "libraries"}
            if (ptok - stop) & (stok - stop):
                agree += 1
            else:
                disagree += 1
                if len(disagreements) < 20:
                    disagreements.append((cid, pubs[0], srcs))

    print(f"sampled (with a record): {tot}")
    print(f"has publisher in <about>   : {have_about_pub} ({100*have_about_pub/tot:.1f}%)")
    print(f"has publisher in <metadata>: {have_meta_pub} ({100*have_meta_pub/tot:.1f}%)")
    print(f"has dc:source              : {have_src} ({100*have_src/tot:.1f}%)")
    print(f"multi-valued publisher     : {multi_pub}")
    print(f"\npublisher & source both present: {both}  -> token-agree {agree}, disagree {disagree}")
    print(f"\n=== distinct publisher values (top 40 of {len(pub_values)}) ===")
    for v, c in pub_values.most_common(40):
        flag = "  <== looks like PUBLISHER not institution" if looks_publisher(v) else ""
        print(f"  {c:4d}  {v[:80]}{flag}")
    print(f"\n=== publisher values flagged non-institutional ({len(set(p for _,p,_ in nonInst))} distinct) ===")
    for cid, p, s in nonInst[:20]:
        print(f"  comm={cid}  pub={p[:60]!r}  src={s}")
    print(f"\n=== publisher/source disagreements (up to 20) ===")
    for cid, p, s in disagreements:
        print(f"  comm={cid}\n     pub: {p[:80]!r}\n     src: {[x[:80] for x in s]}")


if __name__ == "__main__":
    main()
