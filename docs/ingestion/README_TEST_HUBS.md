# Test Hub Ingests

This document describes the conventions for developing and testing a new DPLA hub before it is approved for production.

## Overview

When a proposed hub is under evaluation, all related code and configuration should be kept in the repository but clearly marked as experimental. Test ingests run the full harvest → mapping → enrichment → JSONL pipeline **and sync to S3 like any other hub**, so test data is preserved in `s3://dpla-master-dataset/<hub>/`. What keeps test data off dp.la is `included_in_index = false`, not skipping the S3 sync.

---

## i3.conf Convention

A test hub's config block in `ingestion3-conf/i3.conf` has:

```ini
nga.status = test
nga.included_in_index = false
nga.harvest.type = file
nga.harvest.endpoint = "s3://dpla-hub-nga/..."
# no nga.email — so no partner summary email is sent
# ... rest of config
```

- **`included_in_index = false`** is the setting that matters. The index rebuild (`launch_indexer.py`) and the monthly batch exports (parquet, JSONL dumps, metadata quality reports, sitemaps) both skip hubs with this flag, so test data never reaches dp.la or the public bulk exports.
- **No `email` key.** `ingest.sh` only sends the partner summary email when the hub has an `email` entry.
- **`status = test`** is a label for humans. No script changes its behaviour based on it.

---

## Scala Mapper / Harvester Convention

Test hub mapper and harvester classes live in the `experimental` subpackage:

```text
src/main/scala/dpla/ingestion3/mappers/providers/experimental/
```

Every file in this package must include a header comment:

```scala
/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * This mapper is under evaluation and has not been approved for inclusion
 * in the DPLA production index. Do not set `included_in_index = true`
 * in i3.conf until the hub has been formally approved.
 */
```

When a hub is approved, move the mapper to the standard `providers/` package and remove the header comment.

---

## Graduating a Test Hub to Production

When a hub is approved:

1. Move the mapper from `providers/experimental/` to `providers/` and remove the `TEST HUB` header comment
2. In i3.conf, set `<hub>.included_in_index = true`, add the partner `<hub>.email` contacts, and remove `<hub>.status = test`
3. Open a PR for the mapper move in ingestion3
4. Commit the i3.conf change in ingestion3-conf
5. Run a full ingest — the hub's next snapshot is picked up by the following index rebuild and monthly batch, and the partner receives the summary email
