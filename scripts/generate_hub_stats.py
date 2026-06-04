#!/usr/bin/env python3
"""
Generate hub_stats.json, hub_stats_bws.json, and item_data_providers.json
from the live ES index and GA4, then upload to s3://dashboard-analytics/hub-stats/.

Run on the ingest EC2 after each monthly index rebuild completes,
before final verification. Requires boto3, google-analytics-data, and
network access to ES.

Uses two passes per stats file: first a hub-level totals aggregation,
then one per-hub query for contributor counts. A single nested aggregation
across all hubs exceeds ES's search.max_buckets limit.

Usage:
    ./venv/bin/python scripts/generate_hub_stats.py

Environment:
    ES_HOST             - Elasticsearch hostname (default: search-prod1.internal.dp.la)
    ES_PORT             - Elasticsearch port (default: 9200)
    AWS_PROFILE         - AWS profile name (optional; omit on EC2 to use instance role)
    GA4_PROPERTY_ID     - GA4 numeric property ID (required for item_data_providers)
    GA4_SECRET_NAME     - Secrets Manager secret name for GA4 service account JSON
                          (default: dpla/ga4-service-account)
    GA4_CLICK_EVENT     - GA4 event name for item click-throughs (default: click_item)
    GA4_HISTORY_START   - Earliest date for first-run backfill (default: 2018-01-01)
"""

import calendar
import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import boto3
import botocore.exceptions

ES_HOST = os.environ.get("ES_HOST", "search-prod1.internal.dp.la")
ES_PORT = int(os.environ.get("ES_PORT", "9200"))
BUCKET = "dashboard-analytics"
HUB_KEY = "hub-stats/hub_stats.json"
BWS_KEY = "hub-stats/hub_stats_bws.json"
IDP_KEY = "hub-stats/item_data_providers.json"
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")
GA4_SECRET_NAME = os.environ.get("GA4_SECRET_NAME", "dpla/ga4-service-account")
GA4_CLICK_EVENT = os.environ.get("GA4_CLICK_EVENT", "click_item")
GA4_HISTORY_START = os.environ.get("GA4_HISTORY_START", "2018-01-01")


def es_query(query: dict, timeout: int = 30) -> dict:
    url = f"http://{ES_HOST}:{ES_PORT}/dpla_alias/_search"
    data = json.dumps(query).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read())
    if data.get("timed_out"):
        raise RuntimeError(f"Elasticsearch query timed out: {query}")
    shards = data.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(f"Elasticsearch shard failures: {shards}")
    return data


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
                "terms": {"field": "dataProvider.name.not_analyzed", "size": 10000}
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


def build_stats(bws: bool = False, generated_at: Optional[str] = None) -> dict:
    if generated_at is None:
        generated_at = datetime.now(timezone.utc).isoformat()
    totals = hub_totals(bws)
    hubs = {}
    for hub_name, item_count in totals.items():
        contributors = contributor_counts(hub_name, bws)
        hubs[hub_name] = {"item_count": item_count, "contributors": contributors}
    return {
        "generated_at": generated_at,
        "hubs": hubs,
    }


def upload(data: dict, key: str) -> None:
    # Use AWS_PROFILE if set (local dev); on EC2 profile_name=None falls back
    # to the instance metadata credential chain.
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    session.client("s3", region_name="us-east-1").put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"  Uploaded s3://{BUCKET}/{key}", flush=True)


# ---------------------------------------------------------------------------
# item_data_providers.json
# ---------------------------------------------------------------------------


def prev_month_range() -> tuple[str, str]:
    """Return (start_date, end_date) ISO strings for the previous calendar month."""
    now = datetime.now(timezone.utc)
    year, month = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def fetch_ga4_credentials() -> dict:
    """Fetch GA4 service account JSON from AWS Secrets Manager."""
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    sm = session.client("secretsmanager", region_name="us-east-1")
    secret = sm.get_secret_value(SecretId=GA4_SECRET_NAME)
    return json.loads(secret["SecretString"])


def ga4_item_ids(credentials: dict, start_date: str, end_date: str) -> set:
    """Return set of item IDs from GA4 click-through events in [start_date, end_date].

    Item IDs are the first segment of the eventLabel dimension value,
    which has the format "{item_id} : {item_title}".
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Filter,
        FilterExpression,
        Metric,
        RunReportRequest,
    )
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_info(
        credentials,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    client = BetaAnalyticsDataClient(credentials=creds)

    item_ids: set = set()
    offset = 0
    page_size = 10000

    while True:
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="eventLabel")],
            metrics=[Metric(name="eventCount")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(
                        value=GA4_CLICK_EVENT,
                        match_type=Filter.StringFilter.MatchType.EXACT,
                    ),
                )
            ),
            offset=offset,
            limit=page_size,
        )
        response = client.run_report(request)
        if not response.rows:
            break
        for row in response.rows:
            label = row.dimension_values[0].value
            item_id = label.split(" : ")[0].strip()
            if item_id:
                item_ids.add(item_id)
        if len(response.rows) < page_size:
            break
        offset += page_size

    return item_ids


def fetch_existing_idp() -> dict:
    """Fetch existing item_data_providers.json from S3, or return empty structure."""
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    s3 = session.client("s3", region_name="us-east-1")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=IDP_KEY)
        return json.loads(obj["Body"].read())
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("  No existing item_data_providers.json; starting fresh.", flush=True)
            return {"items": {}}
        raise


def resolve_ids_from_es(ids: list) -> dict:
    """Batch-resolve DPLA item IDs → dataProvider names via ES ids query."""
    resolved = {}
    batch_size = 1000
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        result = es_query(
            {
                "query": {"ids": {"values": batch}},
                "_source": ["dataProvider.name"],
                "size": batch_size,
            },
            timeout=60,
        )
        for hit in result["hits"]["hits"]:
            dp = hit.get("_source", {}).get("dataProvider", {})
            name = dp.get("name", "") if isinstance(dp, dict) else ""
            if name:
                resolved[hit["_id"]] = name
    return resolved


def build_item_data_providers(generated_at: str) -> dict:
    """Build the cumulative item_data_providers.json.

    Fetches the existing mapping from S3, queries GA4 for new item IDs
    (previous calendar month, or full history on first run), resolves
    them against ES, and returns the merged mapping.
    """
    if not GA4_PROPERTY_ID:
        print(
            "  WARNING: GA4_PROPERTY_ID not set; skipping item_data_providers.",
            flush=True,
        )
        return {"generated_at": generated_at, "items": {}}

    existing = fetch_existing_idp()
    current_items: dict = existing.get("items", {})

    # First run (empty mapping): backfill all available GA4 history.
    # Subsequent runs: previous calendar month only.
    if current_items:
        start_date, end_date = prev_month_range()
    else:
        _, end_date = prev_month_range()
        start_date = GA4_HISTORY_START

    print(
        f"  Querying GA4 ({GA4_CLICK_EVENT}) {start_date} → {end_date}...", flush=True
    )
    credentials = fetch_ga4_credentials()
    seen_ids = ga4_item_ids(credentials, start_date, end_date)
    print(f"  {len(seen_ids):,} item IDs from GA4", flush=True)

    new_ids = [i for i in seen_ids if i not in current_items]
    print(f"  {len(new_ids):,} new IDs to resolve from ES", flush=True)

    if new_ids:
        resolved = resolve_ids_from_es(new_ids)
        current_items.update(resolved)
        print(f"  {len(resolved):,} IDs resolved", flush=True)

    return {"generated_at": generated_at, "items": current_items}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()

    print("Generating hub stats from ES...", flush=True)
    hub_stats = build_stats(bws=False, generated_at=generated_at)
    hub_count = len(hub_stats["hubs"])
    print(f"  {hub_count} hubs", flush=True)

    bws_stats = build_stats(bws=True, generated_at=generated_at)
    bws_hub_count = len(bws_stats["hubs"])
    print(f"  {bws_hub_count} hubs with BWS items", flush=True)

    print("Generating item_data_providers.json...", flush=True)
    idp = build_item_data_providers(generated_at)
    idp_count = len(idp.get("items", {}))
    print(f"  {idp_count:,} items in mapping", flush=True)

    upload(hub_stats, HUB_KEY)
    upload(bws_stats, BWS_KEY)
    upload(idp, IDP_KEY)

    print(
        f"Done. {hub_count} hubs, {idp_count:,} item mappings, "
        f"generated_at={generated_at}",
        flush=True,
    )


if __name__ == "__main__":
    main()
