#!/usr/bin/env python3
"""Build a tiny synthetic OriginalRecord Avro harvest from live JSTOR GetRecord
responses, for the affirmatively-confirmed old-Artstor<->new-JSTOR pairs, so the
real Scala mapping/enrichment can be run against fresh JSTOR data and compared to
the existing 2022 output. Also writes pairs.tsv (community_num, old_oai_id,
old_dpla_id, new_dpla_id) for the comparison step.
"""
import hashlib, os, re, subprocess, time
import fastavro

D = "/home/ec2-user/jstortest"
HARV = f"{D}/artstor/harvest/20260824_000000-artstor-OriginalRecord.avro"
XWALK = ["/home/ec2-user/jstor-crosswalk/crosswalk.tsv",
         "/home/ec2-user/jstor-crosswalk/crosswalk_pass2.tsv"]
# community_nums confirmed same-object via title+institution field match
COMMS = ["325968", "307310", "307840", "9274479", "293396", "324779",
         "306635", "9274194", "1780676", "1712256", "306463", "9408428"]

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


def main():
    os.makedirs(HARV, exist_ok=True)
    xmap = {}
    for fn in XWALK:
        if not os.path.exists(fn):
            continue
        with open(fn) as f:
            next(f, None)
            for line in f:
                p = line.rstrip("\n").split("\t")
                if len(p) >= 3:
                    xmap[p[0]] = (p[1], p[2])  # comm -> (old_oai, old_dpla)

    pairs, records = [], []
    for comm in COMMS:
        if comm not in xmap:
            print("NOT in crosswalk:", comm); continue
        old_oai, old_dpla = xmap[comm]
        xml = subprocess.run(["curl", "-sS", "-m", "30",
            f"http://oai.forum.jstor.org/oai/?verb=GetRecord&metadataPrefix=oai_dc&identifier={comm}"],
            capture_output=True, text=True).stdout
        m = re.search(r"(<record\b.*?</record>)", xml, re.S)
        if not m:
            print("no <record> for", comm); continue
        records.append({"id": comm, "ingestDate": int(time.time() * 1000),
                        "provider": "artstor", "document": m.group(1),
                        "mimetype": "application_xml"})
        pairs.append((comm, old_oai, old_dpla))

    with open(f"{HARV}/part-00000.avro", "wb") as out:
        fastavro.writer(out, SCHEMA, records)
    open(f"{HARV}/_SUCCESS", "w").close()

    with open(f"{D}/pairs.tsv", "w") as f:
        f.write("community_num\told_oai_id\told_dpla_id\tnew_dpla_id\n")
        for comm, old_oai, old_dpla in pairs:
            new_dpla = hashlib.md5(f"artstor--{comm}".encode()).hexdigest()
            f.write(f"{comm}\t{old_oai}\t{old_dpla}\t{new_dpla}\n")

    print(f"wrote {len(records)} records to {HARV}")
    # read back sanity
    with open(f"{HARV}/part-00000.avro", "rb") as fo:
        r0 = next(iter(fastavro.reader(fo)))
    print("sample id:", r0["id"], "| doc starts:", r0["document"][:70].replace("\n", " "))


if __name__ == "__main__":
    main()
