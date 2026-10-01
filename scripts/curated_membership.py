#!/usr/bin/env python3
"""
Extract exhibition and primary source set membership for DPLA items.

Crawls dpla/dpla-frontend exhibitions-data/ and dpla/pss-json data/ and
writes one JSON file mapping each referenced DPLA item ID (32-hex) to the
slugs it appears under:

    {
      "generated": "2026-08-18T00:00:00Z",
      "items": {
        "eb167f4df329081fcbcf0108cd6d6837": {
          "exhibitions": ["race-to-the-moon"],
          "primarySourceSets": []
        },
        ...
      }
    }

The JSONL export reads this file from the JAR classpath and stamps member
items with `exhibitions` and `primarySourceSets`. To refresh: re-run this
script, commit the snapshot, rebuild the JAR, then re-export JSONL per hub
(`./scripts/jsonl.sh <hub>`). No re-ingest needed.

Extraction:
  - Exhibitions: element_texts entries named "Has Version". Matching is
    case-insensitive; IDs with stray junk are recovered, unusable
    hex-like values are warned about.
  - Source sets: all dp.la/item links in each source's citation, falling
    back to the 32-hex token of the media filename. Fallback IDs are file
    hashes that usually, but not always, equal the item ID, so each one
    is warned about for auditing.
  - Slugs are taken verbatim; unusual shapes are warned about but kept.

Fetches retry transient failures with backoff. Per-slug failures are
collected; if any remain, nothing is written and the exit is non-zero.
Five consecutive failures abort the run. Set GITHUB_TOKEN if the GitHub
API rate limit (60/hour per IP) is a problem.

Usage:
    python3 scripts/curated_membership.py [output-path]

    Default output: src/main/resources/curated/curated-membership.json
    (relative to the repo root; works from any directory)

Stdlib only. Needs network access to raw.githubusercontent.com and
api.github.com.
"""

import http.client
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

FRONTEND_RAW = "https://raw.githubusercontent.com/dpla/dpla-frontend/main/exhibitions-data"
PSS_RAW = "https://raw.githubusercontent.com/dpla/pss-json/main/data"
PSS_TREES_API = "https://api.github.com/repos/dpla/pss-json/git/trees"

# 32-hex token not inside a longer hex run (rejects SHA-1 slices).
# Input is lowercased before matching.
HEX32_TOKEN = re.compile(r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])")
CITATION_ID = re.compile(r"dp\.la/item/([0-9a-f]{32})(?![0-9a-f])")
NEAR_MISS = re.compile(r"[0-9a-f]{24,}")
USUAL_SLUG = re.compile(r"^[a-z0-9-]+$")

DEFAULT_OUTPUT = str(
    Path(__file__).resolve().parent.parent
    / "src/main/resources/curated/curated-membership.json"
)

RETRIES = 4
MAX_CONSECUTIVE_FAILURES = 5


def warn(message):
    print(f"WARNING: {message}", file=sys.stderr)


def fetch_json(url):
    """GET a JSON document, retrying transient failures with backoff."""
    headers = {"User-Agent": "dpla-ingestion3"}
    token = os.environ.get("GITHUB_TOKEN")
    if token and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"Bearer {token}"
    delay = 2
    error = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 403 and e.headers.get("x-ratelimit-remaining") == "0":
                raise RuntimeError(
                    f"GitHub API rate limit exhausted for {url}; wait for "
                    "the hourly window or set GITHUB_TOKEN"
                ) from e
            if e.code < 500 and e.code != 429:
                raise
            error = e
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as e:
            # URLError/timeouts/resets, mid-body failures, truncated bodies
            error = e
        if attempt < RETRIES - 1:
            print(f"retrying {url}: {error}", file=sys.stderr)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"failed after {RETRIES} attempts: {url}: {error}")


def check_slug(slug, origin):
    if not USUAL_SLUG.match(slug):
        warn(f"{origin}: unusual slug {slug!r} (kept)")
    return slug


def exhibition_item_ids(data, slug):
    """Item IDs of one exhibition, from "Has Version" element texts."""
    ids = set()
    for page in data.get("pages") or []:
        for block in page.get("page_blocks") or []:
            for att in block.get("attachments") or []:
                for et in (att.get("item") or {}).get("element_texts") or []:
                    name = str((et.get("element") or {}).get("name") or "")
                    if name.strip().lower() != "has version":
                        continue
                    text = (et.get("text") or "").strip().lower()
                    match = HEX32_TOKEN.search(text)
                    if match:
                        ids.add(match.group(0))
                        if match.group(0) != text:
                            warn(
                                f"exhibition {slug}: recovered item ID "
                                f"{match.group(0)} from {text!r}"
                            )
                    elif NEAR_MISS.search(text):
                        warn(
                            f"exhibition {slug}: unusable hex-like "
                            f"Has Version value {text!r}"
                        )
    return ids


def as_list(value):
    return value if isinstance(value, list) else [value]


def pss_source_ids(part, slug):
    """Item IDs of one source: citation links first, contentUrl fallback.

    The citation sits at mainEntity[0].citation[] with a misspelled type
    key, so we regex for dp.la/item links instead of matching keys.
    """
    main = [m for m in as_list(part.get("mainEntity") or []) if isinstance(m, dict)]
    first = main[0] if main else {}
    ids = CITATION_ID.findall(json.dumps(first.get("citation", "")).lower())
    if ids:
        return ids
    media = [m for m in as_list(first.get("associatedMedia") or []) if isinstance(m, dict)]
    content_url = str(media[0].get("contentUrl", "")).lower() if media else ""
    match = HEX32_TOKEN.search(content_url)
    if match:
        warn(
            f"source set {slug}: no citation link; using contentUrl "
            f"token {match.group(0)} from {content_url!r}"
        )
        return [match.group(0)]
    return []


def pss_slugs():
    """Set slugs from the pss-json data/ tree (no entry-count cap)."""
    root = fetch_json(f"{PSS_TREES_API}/main")
    data_sha = next(
        e["sha"]
        for e in root["tree"]
        if e["path"] == "data" and e["type"] == "tree"
    )
    tree = fetch_json(f"{PSS_TREES_API}/{data_sha}")
    if tree.get("truncated"):
        raise RuntimeError("pss-json data/ tree listing was truncated")
    return sorted(e["path"] for e in tree["tree"] if e["type"] == "tree")


def crawl(slugs, fetch_url, extract_ids, label):
    """Fetch and parse each slug, collecting failures; abort early when
    everything is failing."""
    members = {}
    failures = []
    consecutive = 0
    for slug in slugs:
        try:
            data = fetch_json(fetch_url(slug))
            ids, note = extract_ids(data, slug)
        except Exception as e:
            failures.append(f"{label} {slug}: {e}")
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                failures.append(
                    f"aborting after {consecutive} consecutive failures"
                )
                break
            continue
        consecutive = 0
        for item_id in ids:
            members.setdefault(item_id, set()).add(slug)
        print(f"{label} {slug}: {len(ids)} items{note}", file=sys.stderr)
    return members, failures


def exhibition_items():
    """Return ({item_id: {exhibition slugs}}, failures)."""
    index = fetch_json(f"{FRONTEND_RAW}/exhibitions.json")
    slugs = list(dict.fromkeys(
        check_slug(e["slug"], "exhibitions.json") for e in index["exhibitions"]
    ))
    return crawl(
        slugs,
        lambda slug: f"{FRONTEND_RAW}/{slug}.json",
        lambda data, slug: (exhibition_item_ids(data, slug), ""),
        "exhibition",
    )


def pss_extract(data, slug):
    ids = set()
    missed = 0
    for part in data.get("hasPart") or []:
        if part.get("disambiguatingDescription") != "source":
            continue
        part_ids = pss_source_ids(part, slug)
        if part_ids:
            ids.update(part_ids)
        else:
            missed += 1
    note = f", {missed} sources without an ID" if missed else ""
    return ids, note


def pss_items():
    """Return ({item_id: {source set slugs}}, failures)."""
    slugs = [check_slug(s, "pss-json data/") for s in pss_slugs()]
    return crawl(
        slugs,
        lambda slug: f"{PSS_RAW}/{slug}/updated_{slug}.json",
        pss_extract,
        "source set",
    )


def main() -> int:
    output = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    try:
        exhibitions, exhibition_failures = exhibition_items()
        source_sets, pss_failures = pss_items()
    except RuntimeError as e:
        warn(str(e))
        return 1

    failures = exhibition_failures + pss_failures
    if failures:
        for failure in failures:
            warn(f"fetch failed: {failure}")
        print(
            f"{len(failures)} failures; not writing {output}",
            file=sys.stderr,
        )
        return 1

    items = {}
    for field, members in (
        ("exhibitions", exhibitions),
        ("primarySourceSets", source_sets),
    ):
        for item_id, slugs in members.items():
            entry = items.setdefault(
                item_id, {"exhibitions": [], "primarySourceSets": []}
            )
            entry[field] = sorted(slugs)

    doc = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": dict(sorted(items.items())),
    }
    # Write to a temp file and rename
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(
            dir=os.path.dirname(output) or ".", suffix=".tmp"
        )
        with os.fdopen(fd, "w") as f:
            json.dump(doc, f, indent=2, sort_keys=False)
            f.write("\n")
        os.chmod(tmp, 0o644)
        os.replace(tmp, output)
        tmp = None
    finally:
        if tmp is not None and os.path.exists(tmp):
            os.unlink(tmp)

    n_ex = sum(1 for v in items.values() if v["exhibitions"])
    n_pss = sum(1 for v in items.values() if v["primarySourceSets"])
    print(
        f"wrote {output}: {len(items)} item IDs "
        f"({n_ex} in exhibitions, {n_pss} in source sets)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
