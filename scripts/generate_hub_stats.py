#!/usr/bin/env python3
"""
Generate hub_stats.json and hub_stats_bws.json from the live ES index
and upload to s3://dashboard-analytics/hub-stats/.

Run on the ingest EC2 after each monthly index rebuild completes,
before final verification. Requires boto3 and network access to ES.

Usage:
    python3 scripts/generate_hub_stats.py

Environment:
    ES_HOST  - Elasticsearch hostname (default: search-prod1.internal.dp.la)
    ES_PORT  - Elasticsearch port (default: 9200)
"""

import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

ES_HOST = os.environ.get("ES_HOST", "search-prod1.internal.dp.la")
ES_PORT = int(os.environ.get("ES_PORT", "9200"))
BUCKET = "dashboard-analytics"
HUB_KEY = "hub-stats/hub_stats.json"
BWS_KEY = "hub-stats/hub_stats_bws.json"


def es_query(query: dict) -> dict:
    url = f"http://{ES_HOST}:{ES_PORT}/dpla_alias/_search"
    data = json.dumps(query).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def build_stats(bws: bool = False) -> dict:
    query: dict = {
        "size": 0,
        "aggs": {
            "hubs": {
                "terms": {"field": "provider.name.not_analyzed", "size": 200},
                "aggs": {
                    "contributors": {
                        "terms": {
                            "field": "dataProvider.name.not_analyzed",
                            "size": 2000,
                        }
                    }
                },
            }
        },
    }
    if bws:
        query["query"] = {"term": {"tags": "blackwomensuffrage"}}

    result = es_query(query)

    hubs = {}
    for hub_bucket in result["aggregations"]["hubs"]["buckets"]:
        hub_name = hub_bucket["key"]
        contributors = {
            c["key"]: c["doc_count"] for c in hub_bucket["contributors"]["buckets"]
        }
        hubs[hub_name] = {
            "item_count": hub_bucket["doc_count"],
            "contributors": contributors,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hubs": hubs,
    }


def upload(data: dict, key: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"  Uploaded s3://{BUCKET}/{key}", flush=True)


def main() -> None:
    print("Generating hub stats from ES...", flush=True)

    hub_stats = build_stats(bws=False)
    hub_count = len(hub_stats["hubs"])
    print(f"  {hub_count} hubs", flush=True)

    bws_stats = build_stats(bws=True)
    bws_hub_count = sum(1 for h in bws_stats["hubs"].values() if h["item_count"] > 0)
    print(f"  {bws_hub_count} hubs with BWS items", flush=True)

    upload(hub_stats, HUB_KEY)
    upload(bws_stats, BWS_KEY)

    print(
        f"Done. {hub_count} hubs, generated_at={hub_stats['generated_at']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
