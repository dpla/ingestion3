#!/usr/bin/env python3
"""Disambiguate the two publisher locations in JSTOR records: does <about> hold
the contributing institution while <metadata> holds the work's actual publisher?
If so, they separate cleanly (about -> dataProvider, metadata -> publisher). Prints
every record that has a <metadata> publisher and/or a non-institutional <about>
publisher, showing about-pub | metadata-pub | source side by side.
"""
import csv, re, random, urllib.request
from concurrent.futures import ThreadPoolExecutor

XWALK = "/home/ec2-user/jstor-crosswalk/jstor_id_crosswalk.tsv"
N = 500
WORKERS = 24
META_RE = re.compile(r"<metadata>(.*?)</metadata>", re.S)
ABOUT_RE = re.compile(r"<about>(.*?)</about>", re.S)
PUB_RE = re.compile(r"<(?:oai_dc:|dc:)?publisher>(.*?)</(?:oai_dc:|dc:)?publisher>", re.S)
SRC_RE = re.compile(r"<(?:oai_dc:|dc:)?source>(.*?)</(?:oai_dc:|dc:)?source>", re.S)
INST_WORDS = ("university", "college", "library", "libraries", "museum", "institute",
              "archives", "society", "historical", "foundation", "gallery",
              "school", "academy", "seminary", "cuny")
PUB_WORDS = ("press", "publishing", "publisher", "publications", "journal", "books",
             "co.", " co ", "inc", "ltd", "agency", "postcard", "post card", "curteich")


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


def pubs(block):
    return [norm(p) for p in PUB_RE.findall(block) if norm(p)]


def main():
    ids = []
    with open(XWALK) as f:
        r = csv.reader(f, delimiter="\t"); next(r, None)
        ids = [row[0] for row in r if row]
    random.seed(13)
    sample = random.sample(ids, min(N, len(ids)))
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(get, sample))

    tot = has_meta = about_nonInst = 0
    meta_rows = []
    about_pub_rows = []
    for cid, xml in results:
        if "<record" not in xml:
            continue
        tot += 1
        mb = META_RE.search(xml); ab = ABOUT_RE.search(xml)
        meta_pub = pubs(mb.group(1) if mb else "")
        about_pub = pubs(ab.group(1) if ab else "")
        srcs = [norm(s) for s in SRC_RE.findall(mb.group(1) if mb else "")]
        if meta_pub:
            has_meta += 1
            meta_rows.append((cid, about_pub, meta_pub, srcs[:1]))
        if any(looks_publisher(p) for p in about_pub):
            about_nonInst += 1
            about_pub_rows.append((cid, about_pub, meta_pub, srcs[:1]))

    print(f"records: {tot}")
    print(f"with <metadata> publisher : {has_meta}")
    print(f"with non-institutional <about> publisher : {about_nonInst}")

    print(f"\n=== records WITH a <metadata> publisher (about | metadata | source) ===")
    for cid, a, m, s in meta_rows:
        print(f"  {cid}: about={a}  META={m}  src={s}")

    print(f"\n=== records whose <about> publisher looks non-institutional ===")
    for cid, a, m, s in about_pub_rows:
        print(f"  {cid}: ABOUT={a}  metadata={m}  src={s}")


if __name__ == "__main__":
    main()
