#!/usr/bin/env python3
"""
Generate hub_stats.json and hub_stats_bws.json from the live ES index
and upload to s3://dashboard-analytics/hub-stats/.

Run on the ingest EC2 after each monthly index rebuild completes,
before final verification. Requires boto3 and network access to ES.

Uses two passes per stats file: first a hub-level totals aggregation,
then one per-hub query for contributor counts. A single nested aggregation
across all hubs exceeds ES's search.max_buckets limit.

Usage:
    ./venv/bin/python scripts/generate_hub_stats.py

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
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def hub_totals(bws: bool = False) -> dict:
    """Return {hub_name: item_count} for all hubs (or BWS-filtered hubs)."""
    query: dict = {
        "size": 0,
        "aggs": {
            "hubs": {"terms": {"field": "provider.name.not_analyzed", "size": 200}}
        },
    }
    if bws:
        query["query"] = {"term": {"tags": "blackwomensuffrage"}}
    result = es_query(query)
    hubs_agg = result["aggregations"]["hubs"]
    if hubs_agg.get("sum_other_doc_count", 0) > 0:
        raise RuntimeError(
            f"Hub aggregation truncated (sum_other_doc_count="
            f"{hubs_agg['sum_other_doc_count']}); increase size."
        )
    return {b["key"]: b["doc_count"] for b in hubs_agg["buckets"]}


def contributor_counts(hub_name: str, bws: bool = False) -> dict:
    """Return {contributor_name: item_count} for all contributors in a hub."""
    query: dict = {
        "size": 0,
        "query": {"term": {"provider.name.not_analyzed": hub_name}},
        "aggs": {
            "contributors": {
                "terms": {"field": "dataProvider.name.not_analyzed", "size": 2000}
            }
        },
    }
    if bws:
        query["query"] = {
            "bool": {
                "filter": [
                    {"term": {"provider.name.not_analyzed": hub_name}},
                    {"term": {"tags": "blackwomensuffrage"}},
                ]
            }
        }
    result = es_query(query)
    contributors_agg = result["aggregations"]["contributors"]
    if contributors_agg.get("sum_other_doc_count", 0) > 0:
        raise RuntimeError(
            f"Contributor aggregation truncated for hub '{hub_name}' "
            f"(sum_other_doc_count={contributors_agg['sum_other_doc_count']}); "
            f"increase size."
        )
    return {b["key"]: b["doc_count"] for b in contributors_agg["buckets"]}


def build_stats(bws: bool = False) -> dict:
    totals = hub_totals(bws)
    hubs = {}
    for hub_name, item_count in totals.items():
        contributors = contributor_counts(hub_name, bws)
        hubs[hub_name] = {"item_count": item_count, "contributors": contributors}
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
    bws_hub_count = len(bws_stats["hubs"])
    print(f"  {bws_hub_count} hubs with BWS items", flush=True)

    upload(hub_stats, HUB_KEY)
    upload(bws_stats, BWS_KEY)

    print(
        f"Done. {hub_count} hubs, generated_at={hub_stats['generated_at']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
