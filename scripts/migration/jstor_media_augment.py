#!/usr/bin/env python3
"""Two-step harvest, step 2 for JSTOR (Ithaka): resolve each item's `Medias:`
listing endpoint and inject the resulting public media URLs back into the
harvested Avro document, so the offline mapper can populate `mediaMaster`.

JSTOR's OAI record only carries a `Medias:` *endpoint* URL (a JSON listing) plus a
paywalled `FullSize:` URL. The real, public master media lives at the endpoint's
`media_url`s. This step reads a harvest OriginalRecord Avro, GETs each record's
Medias endpoint (bounded concurrency), and injects one
`<oai_dc:identifier>Media:<url></oai_dc:identifier>` per media (ordered by the
listing's `sequence_number`) just before `</oai_dc:dc>`. IthakaMapping reads the
`Media:` prefix into `mediaMaster` and excludes it from `identifier`.

Best-effort by design: `mediaMaster` is optional and raises no mapping warning, so
records whose Medias fetch fails (timeout/5xx) are written through unchanged (no
media) rather than failing the run.

Usage:
    jstor_media_augment.py --input <harvest.avro dir> --output <augmented.avro dir>
                           [--workers 20] [--timeout 15] [--limit N]
"""
import argparse, glob, json, os, re, urllib.request
from concurrent.futures import ThreadPoolExecutor
from xml.sax.saxutils import escape
import fastavro

SCHEMA = {
    "namespace": "dpla.avro.v1", "type": "record", "name": "OriginalRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "ingestDate", "type": "long"},
        {"name": "provider", "type": "string"},
        {"name": "document", "type": "string"},
        {"name": "mimetype", "type": {"name": "MimeType", "type": "enum",
                                      "symbols": ["application_json", "application_xml", "text_turtle"]}},
    ],
}
MEDIAS_RE = re.compile(r"Medias:\s*(https?://[^\s<\"']+)")
DC_CLOSE = ("</oai_dc:dc>", "</dc:dc>", "</dc>")


def fetch_media_urls(medias_url, timeout):
    """Return ordered list of public media URLs, or None on failure."""
    try:
        req = urllib.request.Request(medias_url, headers={"User-Agent": "DPLA-ingest"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = sorted(data, key=lambda m: m.get("sequence_number", 0))
        return [m["media_url"] for m in items if m.get("media_url")]
    except Exception:
        return None


def inject(document, urls):
    if not urls:
        return document
    blob = "".join(f"<oai_dc:identifier>Media:{escape(u)}</oai_dc:identifier>" for u in urls)
    for close in DC_CLOSE:
        i = document.find(close)
        if i != -1:
            return document[:i] + blob + document[i:]
    return document  # no dc block found; leave unchanged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="harvest OriginalRecord .avro dir")
    ap.add_argument("--output", required=True, help="augmented OriginalRecord .avro dir")
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--timeout", type=int, default=15)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(args.output, exist_ok=True)

    records = []
    for part in sorted(glob.glob(f"{args.input}/part-*.avro")):
        with open(part, "rb") as f:
            for r in fastavro.reader(f):
                records.append(dict(r))
                if args.limit and len(records) >= args.limit:
                    break
        if args.limit and len(records) >= args.limit:
            break
    total = len(records)
    print(f"read {total} records", flush=True)

    medias = [(MEDIAS_RE.search(r["document"] or "") or [None, None])[1] for r in records]

    def resolve(idx):
        url = medias[idx]
        return idx, (fetch_media_urls(url, args.timeout) if url else None)

    results = {}
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for idx, urls in ex.map(resolve, range(total)):
            results[idx] = urls
            done += 1
            if done % 2000 == 0:
                print(f"resolved {done}/{total}", flush=True)

    no_medias = fetch_fail = injected = multi = media_total = 0
    for i, r in enumerate(records):
        urls = results.get(i)
        if medias[i] is None:
            no_medias += 1
        elif urls is None:
            fetch_fail += 1
        elif urls:
            r["document"] = inject(r["document"], urls)
            injected += 1
            media_total += len(urls)
            if len(urls) > 1:
                multi += 1

    with open(f"{args.output}/part-00000.avro", "wb") as out:
        fastavro.writer(out, SCHEMA, records)
    open(f"{args.output}/_SUCCESS", "w").close()

    print("\n==== media augmentation summary ====")
    print(f"total records         : {total}")
    print(f"had Medias: url        : {total - no_medias}")
    print(f"  injected (>=1 media) : {injected}  (multi-image: {multi})")
    print(f"  fetch failed          : {fetch_fail}")
    print(f"no Medias: url          : {no_medias}")
    print(f"total media urls injected: {media_total}")
    print(f"wrote {args.output}/part-00000.avro")


if __name__ == "__main__":
    main()
