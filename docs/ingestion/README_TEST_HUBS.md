# Test Hub Ingests

This document describes the conventions for developing and testing a new DPLA hub before it is approved for production.

## Overview

When a proposed hub is under evaluation, all related code and configuration should be kept in the repository but clearly marked as experimental. Test ingests run the full harvest → mapping → enrichment → JSONL pipeline but **never sync to S3**, so test data cannot be accidentally picked up by the sparkindexer or appear on dp.la.

---

## i3.conf Convention

Add a `status` key to the hub's config block in `ingestion3-conf/i3.conf`:

```
nga.status = test
nga.harvest.type = file
nga.harvest.endpoint = "s3://dpla-hub-nga/..."
# ... rest of config
```

The `status = test` flag is the authoritative marker. The hub ingest skill reads this flag and automatically adjusts its behaviour (see below). When a hub is approved for production, remove the `status` line (or change it to `status = production`).

---

## Scala Mapper / Harvester Convention

Test hub mapper and harvester classes live in the `experimental` subpackage:

```
src/main/scala/dpla/ingestion3/mappers/providers/experimental/
```

Every file in this package must include a header comment:

```scala
/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * This mapper is under evaluation and has not been approved for inclusion
 * in the DPLA production index. Do not remove the `status = test` flag
 * from i3.conf until the hub has been formally approved.
 */
```

When a hub is approved, move the mapper to the standard `providers/` package and remove the header comment.

### Finding experimental mappers

The hub ingest skill is aware of this convention. When ingesting a test hub, it will look for the mapper in both `providers/experimental/` and `providers/` so no manual path adjustment is needed.

To list all experimental mappers:
```bash
ls src/main/scala/dpla/ingestion3/mappers/providers/experimental/
```

---

## How the Hub Ingest Skill Treats Test Hubs

When `<hub>.status = test` is present in i3.conf, the skill automatically:

1. **Announces** at the start that this is a test hub and lists the differences in behaviour
2. **Skips S3 sync** — passes `--skip-s3-sync` to `ingest.sh`, so JSONL output stays on EC2 only
3. **Skips the partner summary email** — does not send via AWS SES
4. **Still generates all summaries and logs** — the mapping summary, record counts, and JSONL output are all produced normally and remain available on EC2 for manual review or to share with a partner contact if desired

---

## Graduating a Test Hub to Production

When a hub is approved:

1. Move the mapper from `providers/experimental/` to `providers/` and remove the `TEST HUB` header comment
2. Remove `<hub>.status = test` from i3.conf (or delete the line entirely)
3. Open a PR for the mapper move in ingestion3
4. Commit the i3.conf change in ingestion3-conf
5. Run a full ingest — the skill will now treat it as a production hub and perform S3 sync and partner email normally
