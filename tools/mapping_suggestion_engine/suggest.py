#!/usr/bin/env python3
"""
DPLA Mapping Suggestion Engine

Flow:
  1. Parse mapper  → mapped vs. unmapped DPLA fields (no data needed)
  2. Query ES      → exact coverage for mapped fields (single aggregation)
  3. Scan AVRO     → source field inventory from originalRecord
  4. Suggest:
       Type A — unmapped field; source data exists → add a mapping
       Type B — mapped field; low ES coverage; fallback source paths available
       Type C — unmapped field; no source data → ask hub to export
"""

import argparse
import io
import json
import re
import sys
import urllib.request
import boto3
import fastavro
from collections import defaultdict

# ── DPLA field definitions ────────────────────────────────────────────────────

SOURCE_RESOURCE_FIELDS = [
    "alternateTitle",
    "collection",
    "contributor",
    "creator",
    "date",
    "description",
    "extent",
    "format",
    "genre",
    "identifier",
    "language",
    "place",
    "publisher",
    "relation",
    "replacedBy",
    "replaces",
    "rights",
    "rightsHolder",
    "subject",
    "temporal",
    "title",
    "type",
]

AGGREGATION_FIELDS = [
    "dataProvider",
    "isShownAt",
    "preview",
    "object",
    "edmRights",
    "intermediateProvider",
    "iiifManifest",
]

ALL_FIELDS = [(f, f"sr.{f}") for f in SOURCE_RESOURCE_FIELDS] + [
    (f, f"agg.{f}") for f in AGGREGATION_FIELDS
]

# Maps DPLA field name → Elasticsearch field path.
# Confirmed from live JSONL _source inspection (April 2026 ingest).
ES_FIELD = {
    "title": "sourceResource.title",
    "alternateTitle": "sourceResource.alternateTitle",
    "collection": "sourceResource.collection",
    "contributor": "sourceResource.contributor",
    "creator": "sourceResource.creator",
    "date": "sourceResource.date",
    "description": "sourceResource.description",
    "extent": "sourceResource.extent",
    "genre": "sourceResource.specType",
    "identifier": "sourceResource.identifier",
    "language": "sourceResource.language",
    "place": "sourceResource.spatial",
    "publisher": "sourceResource.publisher",
    "relation": "sourceResource.relation",
    "replacedBy": "sourceResource.replacedBy",
    "replaces": "sourceResource.replaces",
    "rights": "sourceResource.rights",
    "rightsHolder": "sourceResource.rightsHolder",
    "subject": "sourceResource.subject",
    "temporal": "sourceResource.temporal",
    "type": "sourceResource.type",
    "dataProvider": "dataProvider",
    "isShownAt": "isShownAt",
    "preview": "preview",
    "object": "object",
    "edmRights": "rights",  # top-level in ES (confirmed)
    "intermediateProvider": "intermediateProvider",
    "iiifManifest": "iiifManifest",
}

# ── Mapper parsing ────────────────────────────────────────────────────────────


def parse_mapper(mapper_path):
    """
    Parse a hub Mapping.scala file to find which DPLA fields have override defs
    and what source JSON paths each reads via extractString(s) traversals.

    Returns:
      mapped   — dict of {field_name: [source_dot_paths]}  (has override def)
      unmapped — set of field names with no override def
      used_paths — flat set of all source paths the mapper reads
    """
    try:
        content = open(mapper_path).read()
    except FileNotFoundError:
        log(f"ERROR: mapper file not found: {mapper_path}")
        sys.exit(1)

    override_re = re.compile(
        r"override\s+def\s+(\w+)\s*\([^)]*\)[^=]*=\s*\n?"
        r"((?:(?!override\s+def\s)(?!def\s+agent).)*)",
        re.DOTALL,
    )
    traversal_re = re.compile(
        r"(?:unwrap\s*\(data\)|data)\s*((?:\s*\\\s*\"[^\"]+\"\s*)+)"
    )
    segment_re = re.compile(r'\\\s*"([^"]+)"')

    mapped = {}
    used_paths = set()

    for m in override_re.finditer(content):
        name = m.group(1)
        body = m.group(2)
        paths = []
        for tm in traversal_re.finditer(body):
            segs = segment_re.findall(tm.group(1))
            if segs:
                p = ".".join(segs)
                paths.append(p)
                used_paths.add(p)
        mapped[name] = paths

    all_field_names = {f for f, _ in ALL_FIELDS}
    unmapped = all_field_names - set(mapped.keys())

    return mapped, unmapped, used_paths


# ── Elasticsearch coverage ────────────────────────────────────────────────────


def query_es_coverage(es_url, provider_id, fields):
    """
    Single ES aggregation query: one `exists` filter per field.
    Returns (total_records, {dpla_field: count}).
    """
    aggs = {
        f"has_{field}": {"filter": {"exists": {"field": ES_FIELD[field]}}}
        for field in fields
        if field in ES_FIELD
    }
    body = json.dumps(
        {
            "size": 0,
            "track_total_hits": True,
            "query": {"term": {"provider.@id": provider_id}},
            "aggs": aggs,
        }
    ).encode()

    req = urllib.request.Request(
        f"{es_url}/dpla_alias/_search",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    if data.get("timed_out"):
        raise RuntimeError("ES query timed out — results may be incomplete")
    shards = data.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(f"ES query had {shards['failed']} shard failures")

    total = data["hits"]["total"]
    if isinstance(total, dict):
        total = total["value"]

    coverage = {}
    for field in fields:
        key = f"has_{field}"
        if key in data.get("aggregations", {}):
            coverage[field] = data["aggregations"][key]["doc_count"]
        else:
            coverage[field] = 0

    return total, coverage


# ── AVRO source field inventory ───────────────────────────────────────────────


def list_avro_parts(path):
    """
    List AVRO part files from either an S3 prefix (s3://...) or a local directory.
    Returns sorted list of (path_or_key, size_bytes).
    """
    import os

    if path.startswith("s3://"):
        bucket, prefix = path[5:].split("/", 1)
        s3 = boto3.client("s3", region_name="us-east-1")
        paginator = s3.get_paginator("list_objects_v2")
        parts = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                fname = key.split("/")[-1]
                if fname.endswith(".avro") and not fname.startswith("."):
                    parts.append((f"s3://{bucket}/{key}", obj["Size"]))
        return sorted(parts)
    else:
        parts = []
        for fname in os.listdir(path):
            if fname.endswith(".avro") and not fname.startswith("."):
                full = os.path.join(path, fname)
                parts.append((full, os.path.getsize(full)))
        return sorted(parts)


def read_avro_records(path, max_records):
    """Read up to max_records from an AVRO file (local path or s3://...)."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        s3 = boto3.client("s3", region_name="us-east-1")
        buf = io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    else:
        buf = open(path, "rb")

    reader = fastavro.reader(buf)
    records = []
    for rec in reader:
        records.append(rec)
        if len(records) >= max_records:
            break
    return records


def walk_json(obj, prefix, seen, examples):
    """
    Recursively walk a parsed JSON object.
    Adds each non-empty path to `seen` (a set — one entry per path per record).
    Collects up to 5 value examples per path.
    Arrays are walked element-by-element (mirrors json4s \\ semantics).
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("@context", "@type", "aggregatedCHO"):
                continue
            path = f"{prefix}.{k}" if prefix else k
            if v is not None and v != "" and v != [] and v != {}:
                seen.add(path)
            walk_json(v, path, seen, examples)
    elif isinstance(obj, list):
        for item in obj:
            walk_json(item, prefix, seen, examples)
    else:
        if obj is not None and str(obj).strip():
            val = str(obj)[:120].strip()
            if val and val not in examples[prefix] and len(examples[prefix]) < 5:
                examples[prefix].append(val)


def scan_source_fields(avro_path, sample_size, per_part=None):
    """
    Read originalRecord from AVRO files and build a source field inventory.
    Returns (src_counts, src_examples, total_scanned).
    """
    parts = list_avro_parts(avro_path)
    log(f"  {len(parts)} AVRO part files in {avro_path}")

    src_counts = defaultdict(int)
    src_examples = defaultdict(list)
    total = 0
    cap = per_part if per_part else max(1, sample_size // max(len(parts), 1))

    for path, size_bytes in parts:
        if total >= sample_size:
            break
        mb = size_bytes // (1024 * 1024)
        log(f"  Reading {path.split('/')[-1]}  ({mb} MB, cap {cap})...")
        for rec in read_avro_records(path, cap):
            total += 1
            try:
                raw = rec.get("originalRecord") or ""
                source = json.loads(raw)
                seen = set()
                walk_json(source, "", seen, src_examples)
                for p in seen:
                    src_counts[p] += 1
            except (json.JSONDecodeError, TypeError):
                pass

    log(f"  Scanned {total:,} source records")
    return src_counts, src_examples, total


# ── Suggestion logic ──────────────────────────────────────────────────────────

FIELD_ALIASES = {
    "place": {
        "statelocatedin",
        "spatial",
        "coverage",
        "location",
        "place",
        "geographic",
        "city",
        "county",
        "state",
    },
    "type": {"spectype", "dctype", "type"},
    "extent": {"extent", "size", "dimensions", "filesize", "pagination"},
    "contributor": {"contributor", "editedby"},
    "publisher": {"publisher"},
    "relation": {"relation", "ispartof", "references", "relateditems", "hasnote"},
    "collection": {"collection", "setspec", "collectiontitle"},
    "alternateTitle": {
        "alttitle",
        "alternatetitle",
        "othertitle",
        "varyingformtitle",
        "uniformtitle",
    },
    "genre": {"genre", "spectype"},
    "temporal": {"temporal", "period", "era"},
    "rightsHolder": {"rightsholder", "licensee", "owner", "copyrightholder"},
    "replacedBy": {"replacedby", "supersededby"},
    "replaces": {"replaces", "supersedes"},
    "edmRights": {"rights", "license", "accessrights", "userestrictions"},
    "iiifManifest": {"iiif", "manifest"},
    "intermediateProvider": {"intermediateprovider", "servicehub", "hub"},
}

COVERAGE_THRESHOLD = 75.0  # below this, look for fallback paths


def find_source_candidates(
    dpla_field,
    src_counts,
    src_examples,
    n_total,
    used_paths,
    exclude=None,
    top_n=4,
    min_src_pct=0.5,
):
    """Return up to top_n source paths that semantically match a DPLA field."""
    exclude = set(exclude or [])
    name_variants = {dpla_field.lower()} | FIELD_ALIASES.get(dpla_field, set())

    candidates = []
    for path, count in sorted(src_counts.items(), key=lambda x: -x[1]):
        if path in used_paths or path in exclude:
            continue
        path_norm = re.sub(r"[._@\[\]]", "", path).lower()
        # Match if any alias appears as substring in the path, or path exactly
        # equals an alias. (Avoid path_norm-in-alias substring: "provider" is
        # a substring of "intermediateprovider" but is not a match for it.)
        if not (
            any(v in path_norm for v in name_variants) or path_norm in name_variants
        ):
            continue
        pct = 100.0 * count / n_total if n_total else 0
        if pct < min_src_pct:
            continue
        exs = [e for e in src_examples.get(path, []) if e.strip()][:3]
        candidates.append((path, pct, exs))
        if len(candidates) >= top_n:
            break
    return candidates


def scala_extract(src_path, scalar=False):
    """Format a dot-path as a Scala json4s extractString(s) snippet."""
    parts = src_path.split(".")
    traversal = " ".join(f'\\ "{p}"' for p in parts)
    fn = "extractString" if scalar else "extractStrings"
    return f"{fn}(unwrap(data) {traversal})"


# ── Output ────────────────────────────────────────────────────────────────────


def log(msg):
    print(msg, file=sys.stderr)


def bar(pct, width=20):
    return "█" * int(width * min(pct, 100.0) / 100)


def print_coverage_table(mapped, unmapped, es_coverage, n_total):
    print(f"\n{'─' * 72}")
    print("SECTION 1 — FIELD COVERAGE  (ES aggregation, full hub)")
    print(f"  Total Heartland records: {n_total:,}")
    print(f"{'─' * 72}")
    print(f"  {'Field':<45} {'Coverage':>10}  {'Source':<8}")
    print(f"  {'─' * 45} {'─' * 10}  {'─' * 8}")
    for dpla_field, _ in ALL_FIELDS:
        if dpla_field in mapped:
            count = es_coverage.get(dpla_field, 0)
            pct = 100.0 * count / n_total if n_total else 0
            ns = "agg" if dpla_field in AGGREGATION_FIELDS else "sr"
            label = f"{'agg' if ns == 'agg' else 'sourceResource'}.{dpla_field}"
            print(f"  {label:<45} {pct:>9.1f}%  {bar(pct)}")
        else:
            ns = "agg" if dpla_field in AGGREGATION_FIELDS else "sourceResource"
            label = f"{ns}.{dpla_field}"
            print(f"  {label:<45} {'—':>10}  (not mapped)")


def print_source_inventory(src_counts, src_examples, n):
    print(f"\n{'─' * 72}")
    print("SECTION 2 — SOURCE FIELD INVENTORY  (≥1% of records)")
    print(f"{'─' * 72}")
    threshold = max(1, n * 0.01)
    rows = [
        (p, 100.0 * c / n, src_examples.get(p, []))
        for p, c in sorted(src_counts.items(), key=lambda x: -x[1])
        if c >= threshold
    ]
    print(f"  {'Source path':<52} {'Coverage':>10}  {'Example'}")
    print(f"  {'─' * 52} {'─' * 10}  {'─' * 30}")
    for path, pct, exs in rows:
        example = " | ".join(exs[:2])[:55]
        print(f"  {path:<52} {pct:>9.1f}%  {example}")


def print_type_a(type_a):
    print(f"\n{'─' * 72}")
    print("SECTION 3A — NEW MAPPINGS  (not in mapper; source data exists)")
    print(f"{'─' * 72}")
    if not type_a:
        print("  None found.\n")
        return
    for dpla_field, candidates in type_a:
        ns = "agg" if dpla_field in AGGREGATION_FIELDS else "sourceResource"
        print(f"\n  ▶ {ns}.{dpla_field}")
        for src_path, src_pct, exs in candidates:
            print(f"      Source path  : {src_path}  ({src_pct:.1f}%)")
            print(f"      Examples     : {' | '.join(exs)[:80]}")
            print(f"      Scala snippet: {scala_extract(src_path)}")


def print_type_b(type_b):
    print(f"\n{'─' * 72}")
    print(
        f"SECTION 3B — COVERAGE IMPROVEMENTS  (mapped; <{COVERAGE_THRESHOLD:.0f}%; fallbacks available)"
    )
    print(f"{'─' * 72}")
    if not type_b:
        print("  None found.\n")
        return
    for dpla_field, pct, candidates, current_paths in type_b:
        ns = "agg" if dpla_field in AGGREGATION_FIELDS else "sourceResource"
        print(f"\n  ▶ {ns}.{dpla_field}  (current: {pct:.1f}%)")
        if current_paths:
            print(f"      Currently reads: {', '.join(current_paths)}")
        for src_path, src_pct, exs in candidates:
            print(f"      Fallback path  : {src_path}  ({src_pct:.1f}%)")
            print(f"      Examples       : {' | '.join(exs)[:80]}")
            print(f"      Scala snippet  : {scala_extract(src_path)}")


def print_type_c(type_c):
    print(f"\n{'─' * 72}")
    print("SECTION 3C — HUB GUIDANCE  (not mapped; absent from source)")
    print(f"{'─' * 72}")
    if not type_c:
        print("  None found.\n")
        return
    for dpla_field in type_c:
        ns = "agg" if dpla_field in AGGREGATION_FIELDS else "sourceResource"
        print(f"  • {ns}.{dpla_field}  — not in source data; ask hub to export")


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="DPLA Mapping Suggestion Engine")
    parser.add_argument("--hub", default="heartland")
    parser.add_argument(
        "--provider-id",
        default="http://dp.la/api/contributor/heartland-hub",
        help="ES provider.@id value to filter by hub",
    )
    parser.add_argument("--es-url", default="http://search-prod1.internal.dp.la:9200")
    parser.add_argument(
        "--avro-path",
        default="/home/ec2-user/data/heartland/mapping/"
        "20260323_200710-heartland-MAP4_0.MAPRecord.avro",
        help="Local dir or s3:// prefix of mapped AVRO files",
    )
    parser.add_argument("--mapper", default=None)
    parser.add_argument("--sample-size", type=int, default=60000)
    parser.add_argument("--per-part", type=int, default=None)
    args = parser.parse_args()

    if args.mapper is None:
        import pathlib

        repo_root = pathlib.Path(__file__).resolve().parents[2]
        hub_cap = args.hub.replace("-", "").capitalize()
        args.mapper = str(
            repo_root
            / "src/main/scala/dpla/ingestion3/mappers/providers"
            / f"{hub_cap}Mapping.scala"
        )

    log(f"\n{'=' * 72}")
    log(f"DPLA Mapping Suggestion Engine — {args.hub}")
    log(f"{'=' * 72}")

    # ── Phase 1: Parse mapper ─────────────────────────────────────────────────
    log("\nPhase 1: Parsing mapper...")
    mapped, unmapped, used_paths = parse_mapper(args.mapper)
    mapped_dpla = {f for f, _ in ALL_FIELDS} & set(mapped.keys())
    log(f"  Mapped fields ({len(mapped_dpla)}): {sorted(mapped_dpla)}")
    log(f"  Unmapped fields ({len(unmapped)}): {sorted(unmapped)}")

    # ── Phase 2: ES coverage for mapped fields ────────────────────────────────
    log(f"\nPhase 2: Querying ES for coverage ({args.es_url})...")
    try:
        n_total, es_coverage = query_es_coverage(
            args.es_url, args.provider_id, list(mapped_dpla)
        )
        log(f"  Total hub records: {n_total:,}")
        for field, count in sorted(es_coverage.items()):
            pct = 100.0 * count / n_total if n_total else 0
            log(f"  {field:<25} {count:>8,}  ({pct:.1f}%)")
    except Exception as e:
        log(f"  ES query failed: {e}")
        log("  Continuing without ES coverage data.")
        n_total, es_coverage = 0, {}

    # ── Phase 3: AVRO source scan ─────────────────────────────────────────────
    log(f"\nPhase 3: Scanning AVRO source fields ({args.avro_path})...")
    src_counts, src_examples, n_scanned = scan_source_fields(
        args.avro_path, args.sample_size, per_part=args.per_part
    )

    # ── Phase 4: Build suggestions ────────────────────────────────────────────
    type_a, type_b, type_c = [], [], []

    # Paths used by fields with meaningful ES output (>5%).  Paths used by
    # 0%-coverage fields are fair game for new mappings.
    MIN_EFFECTIVE_PCT = 5.0
    effective_used = set()
    for dpla_field in mapped_dpla:
        cnt = es_coverage.get(dpla_field, 0)
        if n_total and 100.0 * cnt / n_total >= MIN_EFFECTIVE_PCT:
            effective_used.update(mapped.get(dpla_field, []))

    for dpla_field in sorted(unmapped):
        candidates = find_source_candidates(
            dpla_field, src_counts, src_examples, n_scanned, effective_used
        )
        if candidates:
            type_a.append((dpla_field, candidates))
        else:
            type_c.append(dpla_field)

    for dpla_field in sorted(mapped_dpla):
        count = es_coverage.get(dpla_field, 0)
        pct = 100.0 * count / n_total if n_total else 0
        current_paths = mapped.get(dpla_field, [])
        if pct < COVERAGE_THRESHOLD:
            candidates = find_source_candidates(
                dpla_field,
                src_counts,
                src_examples,
                n_scanned,
                used_paths,
                exclude=current_paths,
                # Only suggest paths with materially higher source coverage.
                min_src_pct=max(pct + 5.0, 1.0),
            )
            if candidates:
                type_b.append((dpla_field, pct, candidates, current_paths))

    # ── Print report ──────────────────────────────────────────────────────────
    print(f"\n\n{'=' * 72}")
    print(f"MAPPING SUGGESTION REPORT — {args.hub.upper()}")
    print(f"{'=' * 72}")
    print(f"ES data : {n_total:,} records  ({args.provider_id})")
    print(f"AVRO    : {n_scanned:,} records scanned for source inventory")

    print_coverage_table(mapped_dpla, unmapped, es_coverage, n_total)
    print_source_inventory(src_counts, src_examples, n_scanned)
    print_type_a(type_a)
    print_type_b(type_b)
    print_type_c(type_c)

    print(f"\n{'=' * 72}")


if __name__ == "__main__":
    main()
