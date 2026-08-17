#!/usr/bin/env python3
"""
Convert mwdl-harvest.jsonl to AVRO format for ingestion pipeline.

Input:  /home/ec2-user/mwdl-harvest/mwdl-harvest.jsonl
Output: /home/ec2-user/data/mwdl/harvest/<TIMESTAMP>/mwdl_harvest.avro
"""

import json
import os
import time
import urllib.request
from pathlib import Path


def slack_notify(msg: str) -> None:
    token   = os.environ.get("SLACK_BOT_TOKEN") or os.environ.get("SLACK_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "C02HEU2L3")
    if not token:
        return
    payload = json.dumps({"channel": channel, "text": msg}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass

import fastavro

SCHEMA = {
    "namespace": "dpla.avro.v1",
    "type":      "record",
    "name":      "OriginalRecord",
    "doc":       "",
    "fields": [
        {"name": "id",         "type": "string"},
        {"name": "ingestDate", "type": "long", "doc": "UNIX timestamp"},
        {"name": "provider",   "type": "string"},
        {"name": "document",   "type": "string"},
        {
            "name": "mimetype",
            "type": {
                "name":    "MimeType",
                "type":    "enum",
                "symbols": ["application_json", "application_xml", "text_turtle"],
            },
        },
    ],
}

INPUT      = Path("/home/ec2-user/mwdl-harvest/mwdl-harvest.jsonl")
TIMESTAMP  = time.strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = Path(f"/home/ec2-user/data/mwdl/harvest/{TIMESTAMP}-mwdl-OriginalRecord.avro")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "mwdl_harvest.avro"

parsed_schema = fastavro.parse_schema(SCHEMA)
ingest_date   = int(time.time())
stats         = {"written": 0, "skipped": 0}

print(f"Input:  {INPUT}", flush=True)
print(f"Output: {OUTPUT_FILE}", flush=True)


def iter_records(fin):
    for i, line in enumerate(fin, 1):
        line = line.strip()
        if not line:
            continue
        try:
            doc     = json.loads(line)
            control = doc.get("pnx", {}).get("control", {})
            if isinstance(control, list):
                control = control[0] if control else {}
            recordid  = control.get("recordid")
            record_id = recordid[0] if isinstance(recordid, list) else (recordid or str(i))
            stats["written"] += 1
            yield {
                "id":         str(record_id),
                "ingestDate": ingest_date,
                "provider":   "mwdl",
                "document":   line,
                "mimetype":   "application_json",
            }
        except Exception as e:
            stats["skipped"] += 1
            if stats["skipped"] <= 5:
                print(f"  Line {i} skipped: {e}", flush=True)


with open(INPUT) as fin, open(OUTPUT_FILE, "wb") as fout:
    fastavro.writer(fout, parsed_schema, iter_records(fin))

print(f"Done: {stats['written']:,} written, {stats['skipped']} skipped", flush=True)

manifest_path = OUTPUT_DIR / "_MANIFEST"
with open(manifest_path, "w") as f:
    f.write(f"Record count: {stats['written']:,}\n")

print(f"Manifest: {stats['written']:,} records", flush=True)
print(f"TIMESTAMP={TIMESTAMP}", flush=True)
slack_notify(
    f":white_check_mark: *mwdl avro conversion complete* — "
    f"{stats['written']:,} records written ({stats['skipped']} skipped). "
    f"Ready to run `ingest.sh mwdl --skip-harvest`."
)
