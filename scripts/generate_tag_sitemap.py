#!/usr/bin/env python3
"""
Generate sitemaps for tag-based DPLA hub subdomains (aviation, bws) by
querying Elasticsearch directly using PIT + search_after pagination.

This script runs on the ingest EC2 (inside the VPC with direct ES access),
invoked via SSM SendCommand from the generate-hub-sitemaps GH Actions workflow.

Replaces the public DPLA API pagination previously used for these two hubs:
  - No max_result_window (50K) constraint — PIT + search_after has no ceiling
  - No dataProvider/hex-prefix segmentation workaround
  - No load on api.dp.la or WAF
  - No 500-error risk from ES circuit-breaker pressure

Output format is identical to the provider-based hubs in
dpla-frontend/scripts/generate-hub-sitemaps.py:
  s3://sitemaps.dp.la/sitemap/<hub>/<timestamp>/all_item_urls_N.xml.gz  (shards)
  s3://sitemaps.dp.la/sitemap/<hub>/all_item_urls.xml                   (index)

Usage:
    venv/bin/python3 scripts/generate_tag_sitemap.py --hub aviation
    venv/bin/python3 scripts/generate_tag_sitemap.py --hub bws
    venv/bin/python3 scripts/generate_tag_sitemap.py --hub aviation --dry-run

Environment:
    ES_HOST   Elasticsearch hostname (default: search-prod1.internal.dp.la)
    ES_PORT   Elasticsearch port (default: 9200)
"""

import argparse
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from xml.sax.saxutils import escape

import boto3

# ── config ────────────────────────────────────────────────────────────────────

ES_HOST  = os.environ.get("ES_HOST", "search-prod1.internal.dp.la")
ES_PORT  = int(os.environ.get("ES_PORT", "9200"))
ES_INDEX = "dpla_alias"

SITEMAP_BUCKET = "sitemaps.dp.la"
ITEM_BASE      = "https://dp.la/item"
SHARD_SIZE     = 50_000   # max URLs per sitemap shard (sitemap protocol limit)
PAGE_SIZE      = 10_000   # hits per ES page (well under max; large for efficiency)
PIT_KEEP_ALIVE = "5m"     # refreshed on every search call

# hub_id → ES tag value
TAG_HUBS = {
    "aviation": "aviation",
    "bws":      "blackwomensuffrage",
}


# ── Elasticsearch helpers ─────────────────────────────────────────────────────

def _es(method: str, path: str, body=None, timeout: int = 60, retries: int = 5) -> dict:
    """Make a raw HTTP request to ES and return parsed JSON.

    Retries up to `retries` times on transient errors (HTTP 429/5xx, network
    timeouts) with exponential backoff.  Non-retryable HTTP errors raise
    immediately.
    """
    url  = f"http://{ES_HOST}:{ES_PORT}{path}"
    data = json.dumps(body).encode() if body is not None else None

    last_exc = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            if exc.code in (429, 500, 502, 503, 504):
                last_exc = RuntimeError(
                    f"ES {method} {path} → HTTP {exc.code}: {body_text[:200]}"
                )
                wait = 5 * (attempt + 1)
                print(
                    f"  Warning: ES {method} {path} → HTTP {exc.code} "
                    f"(attempt {attempt + 1}/{retries}), retrying in {wait}s…",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue
            raise RuntimeError(
                f"ES {method} {path} → HTTP {exc.code}: {body_text[:400]}"
            ) from exc
        except urllib.error.URLError as exc:
            last_exc = exc
            wait = 5 * (attempt + 1)
            print(
                f"  Warning: ES {method} {path} → network error: {exc.reason} "
                f"(attempt {attempt + 1}/{retries}), retrying in {wait}s…",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue

    raise RuntimeError(
        f"ES {method} {path} failed after {retries} attempts"
    ) from last_exc


def iter_ids_via_pit(tag_value: str):
    """
    Yield every DPLA item ID tagged with `tag_value` using PIT + search_after.

    This approach:
      - Opens a Point-in-Time snapshot of the index so results are stable
        across pages even if the index is updated mid-run.
      - Sorts by `id` (asc) and uses each page's last sort value as the
        cursor for the next page.
      - Has no result-window ceiling (unlike from/size pagination).
      - Closes the PIT in a finally block to avoid leaving open contexts.
    """
    # Open a Point-in-Time on the index alias.
    pit_resp = _es("POST", f"/{ES_INDEX}/_pit?keep_alive={PIT_KEEP_ALIVE}")
    pit_id   = pit_resp["id"]

    try:
        query = {
            "size":    PAGE_SIZE,
            "query":   {"term": {"tags": tag_value}},
            "_source": ["id"],
            "sort":    [{"id": {"order": "asc"}}],
            "pit":     {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
        }

        total_reported = False
        search_after   = None

        while True:
            if search_after is not None:
                query["search_after"] = search_after
            else:
                query.pop("search_after", None)

            resp = _es("GET", "/_search", query)
            hits = resp["hits"]["hits"]

            # Log total on first page.
            if not total_reported:
                total = resp["hits"]["total"]
                count = total["value"] if isinstance(total, dict) else total
                rel   = total.get("relation", "eq") if isinstance(total, dict) else "eq"
                print(
                    f"  ES total for tag={tag_value!r}: {count:,}"
                    + (" (lower bound)" if rel == "gte" else ""),
                    flush=True,
                )
                total_reported = True

            # ES may rotate the PIT id; use the latest one.
            if "pit_id" in resp:
                query["pit"]["id"] = resp["pit_id"]

            if not hits:
                break

            for hit in hits:
                item_id = (hit.get("_source") or {}).get("id")
                if item_id:
                    yield item_id

            if len(hits) < PAGE_SIZE:
                break  # last page

            search_after = hits[-1]["sort"]

    finally:
        # Always release the PIT context.
        try:
            _es("DELETE", "/_pit", {"id": query["pit"]["id"]})
        except Exception as exc:  # noqa: BLE001
            print(f"  Warning: could not close PIT: {exc}", file=sys.stderr)


# ── Sitemap XML builders ───────────────────────────────────────────────────────
# Format matches dpla-frontend/scripts/generate-hub-sitemaps.py exactly.

def _build_shard(urls: list[str], timestamp: datetime) -> str:
    now     = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    entries = "\n".join(
        f"  <url>\n    <loc>{escape(u)}</loc>\n    <lastmod>{now}</lastmod>\n  </url>"
        for u in urls
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{entries}\n"
        "</urlset>"
    )


def _build_index(shard_keys: list[str], timestamp: datetime) -> str:
    now     = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    entries = "\n".join(
        f"  <sitemap>\n"
        f"    <loc>https://{SITEMAP_BUCKET}/{key}</loc>\n"
        f"    <lastmod>{now}</lastmod>\n"
        f"  </sitemap>"
        for key in shard_keys
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{entries}\n"
        "</sitemapindex>"
    )


# ── S3 upload helpers ─────────────────────────────────────────────────────────

def _upload_shard(s3, key: str, xml: str, dry_run: bool, shard_urls: list[str]):
    if dry_run:
        print(f"\n--- {key} (first 3 URLs) ---")
        for url in shard_urls[:3]:
            print(f"  {url}")
        print(f"  … ({len(shard_urls):,} total)")
    else:
        compressed = gzip.compress(xml.encode("utf-8"))
        s3.put_object(
            Bucket=SITEMAP_BUCKET,
            Key=key,
            Body=compressed,
            ContentType="application/xml",
            ContentEncoding="gzip",
        )
        print(f"  uploaded s3://{SITEMAP_BUCKET}/{key}", flush=True)


def _upload_index(s3, key: str, xml: str, dry_run: bool):
    if dry_run:
        print(f"\n--- {key} ---")
        print(xml)
    else:
        s3.put_object(
            Bucket=SITEMAP_BUCKET,
            Key=key,
            Body=xml.encode("utf-8"),
            ContentType="application/xml",
        )
        print(f"  uploaded s3://{SITEMAP_BUCKET}/{key}", flush=True)


# ── Main generation logic ─────────────────────────────────────────────────────

def generate(hub_id: str, dry_run: bool, timestamp: datetime):
    tag_value = TAG_HUBS[hub_id]
    s3        = boto3.client("s3")
    ts_str    = timestamp.strftime("%Y%m%d-%H%M%S")

    shard_keys: list[str] = []
    shard_buf:  list[str] = []
    total = 0
    n     = 0

    print(f"  {hub_id}: querying ES for tag={tag_value!r}…", flush=True)

    for item_id in iter_ids_via_pit(tag_value):
        shard_buf.append(f"{ITEM_BASE}/{item_id}")
        total += 1

        if len(shard_buf) == SHARD_SIZE:
            n  += 1
            key = f"sitemap/{hub_id}/{ts_str}/all_item_urls_{n}.xml.gz"
            shard_keys.append(key)
            _upload_shard(s3, key, _build_shard(shard_buf, timestamp), dry_run, shard_buf)
            shard_buf = []

    # Final (possibly partial) shard.
    if shard_buf:
        n  += 1
        key = f"sitemap/{hub_id}/{ts_str}/all_item_urls_{n}.xml.gz"
        shard_keys.append(key)
        _upload_shard(s3, key, _build_shard(shard_buf, timestamp), dry_run, shard_buf)

    print(f"  {hub_id}: {total:,} IDs → {n} shard(s)", flush=True)

    if not shard_keys:
        raise RuntimeError(f"{hub_id}: no IDs found — is the ES index empty or tag wrong?")

    index_key = f"sitemap/{hub_id}/all_item_urls.xml"
    _upload_index(s3, index_key, _build_index(shard_keys, timestamp), dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="Generate tag-based hub sitemaps via direct ES access"
    )
    parser.add_argument(
        "--hub", required=True, choices=list(TAG_HUBS),
        help=f"Hub to generate: {', '.join(TAG_HUBS)}",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print sitemap XML to stdout instead of uploading to S3",
    )
    args = parser.parse_args()

    timestamp = datetime.now(timezone.utc)
    print(
        f"generate_tag_sitemap: hub={args.hub} tag={TAG_HUBS[args.hub]!r}"
        + (" (dry-run)" if args.dry_run else ""),
        flush=True,
    )

    generate(args.hub, args.dry_run, timestamp)
    print("generate_tag_sitemap: done", flush=True)


if __name__ == "__main__":
    main()
