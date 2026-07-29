#!/usr/bin/env python3
"""
Generate hub_stats.json, hub_stats_bws.json, and item_data_providers.json
from the live ES index and GA4, then upload to s3://dashboard-analytics/hub-stats/.

Run on the ingest EC2 after each monthly index rebuild completes, before
final verification. Run on day 5 of the month or later: GA4 adjusts data
for the just-ended month for ~72 hours (see GaPersistentCache::SETTLING_DAYS
in the Rails app). Requires boto3, google-analytics-data, and network access
to ES.

Run monthly to keep item_data_providers current. A missed month is
recoverable: GA4's retention setting does not limit Data API reports, so
old months can be re-queried by widening the window. The only hard floor
is the event_label dimension's registration date (2025-07-18).

Uses two passes per stats file: first a hub-level totals aggregation,
then one per-hub query for contributor counts. A single nested aggregation
across all hubs exceeds ES's search.max_buckets limit.

Both hub stats files upload before the GA4 work starts. If
item_data_providers.json cannot be updated, the script exits non-zero so
post_indexer.py alerts, but the hub stats uploads stand.

Usage:
    ./venv/bin/python scripts/generate_hub_stats.py

Environment:
    ES_HOST             - Elasticsearch hostname (default: search-prod1.internal.dp.la)
    ES_PORT             - Elasticsearch port (default: 9200)
    AWS_PROFILE         - AWS profile name (optional; omit on EC2 to use instance role)
    GA4_PROPERTY_ID     - GA4 numeric property ID (required for item_data_providers)
    GA4_SECRET_NAME     - Secrets Manager secret name for GA4 service account JSON
                          (default: dpla/ga4-service-account)
    GA4_HISTORY_START   - Earliest date for first-run backfill (default: 2025-07-18,
                          the registration date of the event_label custom
                          dimension; the API returns nothing before that date)
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import time
import traceback
import urllib.error
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
GA4_HISTORY_START = os.environ.get("GA4_HISTORY_START", "2025-07-18")

# The event tables whose rows the dashboard resolves contributor names for.
# In this GA4 property the event *type* lives in customEvent:event_category
# as "{event name} : {hub name}"; eventName holds the contributor name, so
# never filter on it. Must match WebsiteEvents::NAMES_BY_ID in the Rails app.
EVENT_CATEGORY_PREFIXES = [
    "View Item : ",
    "View Exhibition Item : ",
    "View Primary Source : ",
    "Click Through : ",
]

# DPLA item IDs are 32 hex chars. Filters out GA4's "(other)" bucket rows
# and malformed labels.
ITEM_ID_RE = re.compile(r"[0-9a-f]{32}")


def _es_search(url: str, payload: bytes, timeout: int, query: dict) -> dict:
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read())
    if data.get("timed_out"):
        raise RuntimeError(f"Elasticsearch query timed out: {query}")
    shards = data.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(f"Elasticsearch shard failures: {shards}")
    return data


def es_query(query: dict, timeout: int = 30) -> dict:
    """Search ES, retrying transient failures (network, 5xx, shard errors)."""
    url = f"http://{ES_HOST}:{ES_PORT}/dpla_alias/_search"
    payload = json.dumps(query).encode()
    for attempt in range(3):
        try:
            return _es_search(url, payload, timeout, query)
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise
            err: Exception = e
        except (OSError, RuntimeError) as e:
            err = e
        wait = 2 * (2**attempt)
        print(
            f"  ES transient error ({err.__class__.__name__}); "
            f"retrying in {wait}s...",
            flush=True,
        )
        time.sleep(wait)
    return _es_search(url, payload, timeout, query)


def aws_client(service: str):
    # AWS_PROFILE for local dev; profile_name=None on EC2 falls back to the
    # instance role.
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    return session.client(service, region_name="us-east-1")


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
    if not hubs_agg["buckets"]:
        raise RuntimeError(
            "Hub aggregation returned no buckets (empty index, wrong alias, "
            "or renamed field); refusing to overwrite the live stats files."
        )
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


def upload(data: dict, key: str, compact: bool = False) -> None:
    # compact halves the payload for the large cumulative file.
    body = (
        json.dumps(data, separators=(",", ":")) if compact else json.dumps(data, indent=2)
    )
    aws_client("s3").put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode(),
        ContentType="application/json",
    )
    print(f"  Uploaded s3://{BUCKET}/{key}", flush=True)


# ---------------------------------------------------------------------------
# item_data_providers.json
# ---------------------------------------------------------------------------


def recent_months_range() -> tuple[str, str]:
    """Return (start_date, end_date) ISO strings covering the previous two
    calendar months.

    Two months, not one: a run early in the month can miss events GA4 was
    still settling for the month before. The merge only adds IDs, so
    re-querying the same months is safe, and one missed run heals on the
    next.
    """
    now = datetime.now(timezone.utc)
    end_year, end_month = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
    start_year, start_month = (
        (end_year, end_month - 1) if end_month > 1 else (end_year - 1, 12)
    )
    last_day = calendar.monthrange(end_year, end_month)[1]
    return (
        f"{start_year:04d}-{start_month:02d}-01",
        f"{end_year:04d}-{end_month:02d}-{last_day:02d}",
    )


def fetch_ga4_credentials() -> dict:
    """Fetch GA4 service account JSON from AWS Secrets Manager."""
    secret = aws_client("secretsmanager").get_secret_value(SecretId=GA4_SECRET_NAME)
    raw = secret.get("SecretString") or secret["SecretBinary"]
    return json.loads(raw)


def ga4_item_ids(credentials: dict, start_date: str, end_date: str) -> set:
    """Return the set of item IDs from all four event tables in
    [start_date, end_date].

    Item IDs are the first segment of the customEvent:event_label value,
    which has the format "{item_id} : {item_title}".
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Filter,
        FilterExpression,
        FilterExpressionList,
        Metric,
        OrderBy,
        RunReportRequest,
    )
    from google.api_core.exceptions import (
        DeadlineExceeded,
        InternalServerError,
        ResourceExhausted,
        ServiceUnavailable,
        TooManyRequests,
    )
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_info(
        credentials,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    client = BetaAnalyticsDataClient(credentials=creds)

    transient = (
        DeadlineExceeded,
        InternalServerError,
        ResourceExhausted,
        ServiceUnavailable,
        TooManyRequests,
    )

    def run_report_with_retry(report_request):
        for attempt in range(3):
            try:
                return client.run_report(report_request)
            except transient as e:
                wait = 15 * (2**attempt)
                print(
                    f"  GA4 transient error ({e.__class__.__name__}); "
                    f"retrying in {wait}s...",
                    flush=True,
                )
                time.sleep(wait)
        return client.run_report(report_request)

    category_filters = [
        FilterExpression(
            filter=Filter(
                field_name="customEvent:event_category",
                string_filter=Filter.StringFilter(
                    value=prefix,
                    match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                ),
            )
        )
        for prefix in EVENT_CATEGORY_PREFIXES
    ]

    item_ids: set = set()
    dropped = 0
    thresholded = False
    offset = 0
    page_size = 10000

    while True:
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="customEvent:event_label")],
            metrics=[Metric(name="eventCount")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                or_group=FilterExpressionList(expressions=category_filters)
            ),
            # Explicit order: GA4 row order is unspecified without it, which
            # makes offset pagination skip or repeat rows.
            order_bys=[
                OrderBy(
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="customEvent:event_label"
                    )
                )
            ],
            offset=offset,
            limit=page_size,
        )
        response = run_report_with_retry(request)
        if response.metadata.data_loss_from_other_row:
            raise RuntimeError(
                "GA4 collapsed some event_label values into the '(other)' "
                "row (data_loss_from_other_row); this window is missing "
                "item ids that a re-query cannot recover."
            )
        if response.metadata.subject_to_thresholding:
            thresholded = True
        if not response.rows:
            break
        for row in response.rows:
            label = row.dimension_values[0].value
            item_id = label.split(" : ")[0].strip()
            if ITEM_ID_RE.fullmatch(item_id):
                item_ids.add(item_id)
            else:
                dropped += 1
        # row_count is the documented completion signal; a short page is not.
        offset += len(response.rows)
        if offset >= response.row_count:
            break

    if dropped:
        print(
            f"  {dropped:,} label rows dropped (no 32-hex id in label)",
            flush=True,
        )
    if thresholded:
        print(
            "  WARNING: GA4 withheld low-user rows from this report "
            "(subject_to_thresholding); some item ids are missing.",
            flush=True,
        )

    # Drift check: EVENT_CATEGORY_PREFIXES is hand-copied from the Rails
    # app. Surface any "{event} : {hub}" category we are not harvesting.
    try:
        cat_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="customEvent:event_category")],
            metrics=[Metric(name="eventCount")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=10000,
        )
        categories = {
            row.dimension_values[0].value
            for row in run_report_with_retry(cat_request).rows
        }
        unknown = sorted(
            c
            for c in categories
            if " : " in c
            and not any(c.startswith(p) for p in EVENT_CATEGORY_PREFIXES)
        )
        if unknown:
            print(
                f"  WARNING: {len(unknown)} event categories not in "
                f"EVENT_CATEGORY_PREFIXES; update the list if these are "
                f"item events: {unknown[:10]}",
                flush=True,
            )
    except Exception as e:
        print(
            f"  WARNING: category drift check failed "
            f"({e.__class__.__name__}: {e})",
            flush=True,
        )

    return item_ids


def fetch_existing_idp() -> dict:
    """Fetch existing item_data_providers.json from S3, or return empty structure."""
    try:
        obj = aws_client("s3").get_object(Bucket=BUCKET, Key=IDP_KEY)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("  No existing item_data_providers.json; starting fresh.", flush=True)
            return {"items": {}}
        raise
    try:
        existing = json.loads(obj["Body"].read())
    except ValueError as e:
        raise RuntimeError(
            f"Existing s3://{BUCKET}/{IDP_KEY} is not valid JSON ({e}). "
            "Inspect it; delete the object to rebuild from full history."
        ) from e
    items = existing.get("items") if isinstance(existing, dict) else None
    if not isinstance(items, dict):
        raise RuntimeError(
            f"Existing s3://{BUCKET}/{IDP_KEY} does not look like "
            '{"items": {...}}. Inspect it; delete the object to rebuild '
            "from full history."
        )
    return existing


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


def build_item_data_providers(generated_at: str) -> Optional[dict]:
    """Build the cumulative item_data_providers.json, or None if it must not
    be uploaded.

    Fetches the existing mapping from S3, queries GA4 for item IDs
    (previous two calendar months, or full history on first run), resolves
    them against ES, and returns the merged mapping. The merge only adds
    IDs, so a run can never shrink the mapping; when GA4 is misconfigured
    or returns nothing, returns None so the existing file stays untouched.
    """
    if not GA4_PROPERTY_ID:
        print(
            "  ERROR: GA4_PROPERTY_ID not set; leaving item_data_providers.json untouched.",
            flush=True,
        )
        return None

    existing = fetch_existing_idp()
    current_items: dict = existing["items"]

    # First run (empty mapping): backfill all available GA4 history.
    if current_items:
        start_date, end_date = recent_months_range()
    else:
        _, end_date = recent_months_range()
        start_date = GA4_HISTORY_START

    print(f"  Querying GA4 {start_date} → {end_date}...", flush=True)
    credentials = fetch_ga4_credentials()
    seen_ids = ga4_item_ids(credentials, start_date, end_date)
    print(f"  {len(seen_ids):,} item IDs from GA4", flush=True)

    if not seen_ids:
        print(
            "  ERROR: GA4 returned no item IDs — query or tagging is broken; "
            "leaving item_data_providers.json untouched.",
            flush=True,
        )
        return None

    new_count = sum(1 for i in seen_ids if i not in current_items)
    print(f"  {new_count:,} of these are new", flush=True)

    # Resolve every id in the window, not just new ones, so contributor
    # renames in ES reach recently active items.
    resolved = resolve_ids_from_es(list(seen_ids))
    if not resolved:
        print(
            "  ERROR: none of the GA4 ids resolved in ES — index or field "
            "mismatch; leaving item_data_providers.json untouched.",
            flush=True,
        )
        return None
    current_items.update(resolved)
    print(f"  {len(resolved):,} IDs resolved", flush=True)

    return {"generated_at": generated_at, "items": current_items}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()

    if datetime.now(timezone.utc).day < 5:
        print(
            "WARNING: running before day 5; GA4 may still be adjusting the "
            "just-ended month (see GaPersistentCache::SETTLING_DAYS).",
            flush=True,
        )

    # Build both stats files, then upload both: a failure mid-build
    # publishes nothing, and a GA4 failure later cannot block the stats
    # uploads, which have already happened.
    print("Generating hub stats from ES...", flush=True)
    hub_stats = build_stats(bws=False, generated_at=generated_at)
    hub_count = len(hub_stats["hubs"])
    print(f"  {hub_count} hubs", flush=True)

    bws_stats = build_stats(bws=True, generated_at=generated_at)
    print(f"  {len(bws_stats['hubs'])} hubs with BWS items", flush=True)

    upload(hub_stats, HUB_KEY)
    upload(bws_stats, BWS_KEY)

    print("Generating item_data_providers.json...", flush=True)
    try:
        idp = build_item_data_providers(generated_at)
    except Exception as e:
        print(traceback.format_exc(), flush=True)
        print(
            f"  ERROR: item_data_providers failed ({e.__class__.__name__}: {e}); "
            "existing file untouched.",
            flush=True,
        )
        idp = None

    # Exit non-zero on IDP failure so post_indexer's SSM status check alerts;
    # the hub stats uploads above have already happened either way.
    if idp is None:
        print(
            f"Done with errors. {hub_count} hubs uploaded; "
            f"item_data_providers.json not updated. generated_at={generated_at}",
            flush=True,
        )
        sys.exit(1)

    upload(idp, IDP_KEY, compact=True)
    print(
        f"Done. {hub_count} hubs, {len(idp['items']):,} item mappings, "
        f"generated_at={generated_at}",
        flush=True,
    )


if __name__ == "__main__":
    main()
