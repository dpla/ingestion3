#!/usr/bin/env python3
"""
One-time export: item → hub/contributor attribution from the live ES index.
Produces s3://dashboard-analytics/hub-stats/item_attribution.json.gz

Run ONCE on the ingest EC2 after the regular monthly rebuild. The output
is stable because item IDs in DPLA are permanently bound to a single hub
and contributor. Re-run only if a full re-index changes item→hub assignments.

Dashboard-side counterpart: dpla/dashboard-analytics#284

Usage:
    ./venv/bin/python scripts/export_item_attribution.py

Environment:
    ES_HOST      - Elasticsearch hostname (default: search-prod1.internal.dp.la)
    ES_PORT      - Elasticsearch port (default: 9200)
    AWS_PROFILE  - AWS profile name (optional; omit on EC2 to use instance role)
    BATCH_SIZE   - Records per ES page (default: 10000)
"""

import gzip
import json
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone

import boto3

ES_HOST = os.environ.get("ES_HOST", "search-prod1.internal.dp.la")
ES_PORT = int(os.environ.get("ES_PORT", "9200"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10000"))
BUCKET = "dashboard-analytics"
ATTR_KEY = "hub-stats/item_attribution.json.gz"


def es_search(query: dict) -> dict:
    url = f"http://{ES_HOST}:{ES_PORT}/dpla_alias/_search"
    data = json.dumps(query).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    if result.get("timed_out"):
        raise RuntimeError("Elasticsearch query timed out")
    shards = result.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(f"Elasticsearch shard failures: {shards}")
    return result


def export_attribution(out_path: str) -> int:
    """Stream all ES records to a gzipped JSONL file. Returns record count.

    Uses search_after pagination on _id to avoid deep-pagination penalties.
    Each output line is a JSON object:
        {"id": "<dpla_id>", "hub": "<provider.name>", "contributor": "<dataProvider.name>"}
    """
    query: dict = {
        "_source": ["provider.name", "dataProvider.name"],
        "query": {"match_all": {}},
        "sort": [{"_id": "asc"}],
        "size": BATCH_SIZE,
    }

    total_written = 0
    last_sort = None
    batch_num = 0

    with gzip.open(out_path, "wt", encoding="utf-8") as fout:
        while True:
            if last_sort is not None:
                query["search_after"] = last_sort

            result = es_search(query)
            hits = result["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                provider = src.get("provider", {})
                data_provider = src.get("dataProvider", {})
                fout.write(
                    json.dumps(
                        {
                            "id": hit["_id"],
                            "hub": provider.get("name", "") if isinstance(provider, dict) else "",
                            "contributor": data_provider.get("name", "") if isinstance(data_provider, dict) else "",
                        }
                    )
                    + "\n"
                )
                total_written += 1

            last_sort = hits[-1]["sort"]
            batch_num += 1

            if batch_num % 100 == 0:
                print(f"  {total_written:,} records exported...", flush=True)

    return total_written


def upload_to_s3(local_path: str) -> None:
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    s3 = session.client("s3", region_name="us-east-1")
    file_size = os.path.getsize(local_path)
    print(
        f"  Uploading {file_size / 1024 / 1024:.1f} MB to s3://{BUCKET}/{ATTR_KEY}...",
        flush=True,
    )
    s3.upload_file(
        local_path,
        BUCKET,
        ATTR_KEY,
        ExtraArgs={"ContentType": "application/gzip"},
    )
    print(f"  Uploaded s3://{BUCKET}/{ATTR_KEY}", flush=True)


def main() -> None:
    print("Exporting item attribution from ES...", flush=True)
    print(f"  ES:         {ES_HOST}:{ES_PORT}", flush=True)
    print(f"  Batch size: {BATCH_SIZE:,}", flush=True)
    print(f"  Output:     s3://{BUCKET}/{ATTR_KEY}", flush=True)
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}", flush=True)

    with tempfile.NamedTemporaryFile(
        suffix=".json.gz", prefix="item_attribution_", delete=False
    ) as tmp:
        tmp_path = tmp.name

    try:
        count = export_attribution(tmp_path)
        print(f"  {count:,} records written", flush=True)
        upload_to_s3(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    print(
        f"Done. {count:,} records exported at {datetime.now(timezone.utc).isoformat()}",
        flush=True,
    )


if __name__ == "__main__":
    main()
