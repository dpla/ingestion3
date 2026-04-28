#!/usr/bin/env python3
import json
import time
from pathlib import Path

import fastavro

SCHEMA = {
    "namespace": "dpla.avro.v1",
    "type": "record",
    "name": "OriginalRecord",
    "doc": "",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "ingestDate", "type": "long", "doc": "UNIX timestamp"},
        {"name": "provider", "type": "string"},
        {"name": "document", "type": "string"},
        {
            "name": "mimetype",
            "type": {
                "name": "MimeType",
                "type": "enum",
                "symbols": ["application_json", "application_xml", "text_turtle"],
            },
        },
    ],
}

INPUT = Path("/home/ec2-user/mississippi-harvest/mississippi-harvest.jsonl")
TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = Path(f"/home/ec2-user/data/mississippi/harvest/{TIMESTAMP}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "mississippi_harvest.avro"

parsed_schema = fastavro.parse_schema(SCHEMA)
ingest_date = int(time.time())
stats = {"written": 0, "skipped": 0}

print(f"Input:  {INPUT}", flush=True)
print(f"Output: {OUTPUT_FILE}", flush=True)


def iter_records(fin):
    for i, line in enumerate(fin, 1):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
            control = doc.get("pnx", {}).get("control", {})
            if isinstance(control, list):
                control = control[0] if control else {}
            recordid = control.get("recordid")
            record_id = (
                recordid[0] if isinstance(recordid, list) else (recordid or str(i))
            )
            stats["written"] += 1
            yield {
                "id": str(record_id),
                "ingestDate": ingest_date,
                "provider": "mississippi",
                "document": line,
                "mimetype": "application_json",
            }
        except Exception as e:
            stats["skipped"] += 1
            if stats["skipped"] <= 5:
                print(f"  Line {i} skipped: {e}", flush=True)


with open(INPUT) as fin, open(OUTPUT_FILE, "wb") as fout:
    fastavro.writer(fout, parsed_schema, iter_records(fin))

print(f"Done: {stats['written']:,} written, {stats['skipped']} skipped", flush=True)

with open(OUTPUT_DIR / "_MANIFEST", "w") as f:
    f.write(f"{stats['written']}\n")
print(f"Manifest: {stats['written']} records", flush=True)
print(f"TIMESTAMP={TIMESTAMP}", flush=True)
