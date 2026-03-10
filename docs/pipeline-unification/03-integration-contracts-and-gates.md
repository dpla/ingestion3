# DPLA Pipeline Unification -- Integration Contracts and Safety Gates

**Audience:** Engineers, architects, operations staff

**Reading time:** 20 minutes

**Context:** This document defines the rules that hold the pipeline together. Treat it as an operational specification: the contracts here are the boundaries between the three systems, and the gates are the checkpoints that prevent bad data from reaching production. Read this before implementing any cross-system changes.

---

## Contents

- [Purpose](#purpose)
- [Contract 1: Hub Identity Mapping](#contract-1-hub-identity-mapping)
  - [Contract Rules](#contract-rules)
  - [Current State (Before Implementation)](#current-state-before-implementation)
  - [Target State](#target-state)
  - [Validation Test](#validation-test)
- [Contract 2: S3 Output Path Semantics](#contract-2-s3-output-path-semantics)
  - [Path Format](#path-format)
  - [Contract Rules](#contract-rules-1)
  - [Producer](#producer)
  - [Consumer](#consumer)
- [Contract 3: Index Promotion (Alias Flip)](#contract-3-index-promotion-alias-flip)
  - [Required Preconditions for Promotion](#required-preconditions-for-promotion)
  - [Elasticsearch Index Naming](#elasticsearch-index-naming)
- [Contract 4: Wikimedia Refresh](#contract-4-wikimedia-refresh)
  - [Required Preconditions for Refresh](#required-preconditions-for-refresh)
  - [institutions_v2.json Format](#institutions_v2json-format)
- [Safety Gates](#safety-gates)
  - [Gate A: Pre-Indexer Gate](#gate-a-pre-indexer-gate)
  - [Gate B: Pre-Flip Gate (Critical)](#gate-b-pre-flip-gate-critical)
  - [Gate C: Post-Flip Gate](#gate-c-post-flip-gate)
  - [Gate D: Wikimedia Refresh Gate](#gate-d-wikimedia-refresh-gate)
- [Observability Requirements](#observability-requirements)
- [Ownership Model](#ownership-model)
- [Change Management Rules](#change-management-rules)
- [Edge Cases That Must Be Handled](#edge-cases-that-must-be-handled)
- [Relationship to Greenfield Vision](#relationship-to-greenfield-vision)

---

## Purpose

This document defines the minimum explicit contracts required for safe orchestration across `ingestion3`, `sparkindexer`, and `ingest-wikimedia`. It also defines the mandatory safety gates that must pass before high-impact transitions proceed.

These contracts are operational boundaries. Breaking a contract breaks the pipeline. Changing a contract requires the change management process defined at the end of this document.

---

## Contract 1: Hub Identity Mapping

Each hub must resolve consistently across three naming domains:

- **Short name** (`i3.conf` style): `hathi`, `tn`, `georgia`, `nara`
- **S3 prefix** (storage and index discovery): `hathitrust`, `tennessee`, `georgia`, `nara`
- **Provider display name** (API and Wikimedia mapping): `HathiTrust`, `Digital Library of Tennessee`, `Digital Library of Georgia`, `NARA`

### Contract Rules

1. One canonical mapping source exists and is versioned. This is the **hub registry** -- a single JSON file that maps all three naming systems.
2. Every stage consumes that mapping from the same contract artifact.
3. An unknown hub or unresolved mapping is a **hard error**, not a warning. Processing must not proceed with an unmapped hub.

### Current State (Before Implementation)

The mapping is split across three locations:
- Short name to S3 prefix: `ingestion3/scheduler/orchestrator/config.py` (`S3_PREFIX_MAP`)
- S3 prefix to provider display name: `ingestion3/src/main/resources/wiki/institutions_v2.json`
- Display name in API: determined by sparkindexer's ES index content

### Target State

A canonical `hub-registry.json` stored in ingestion3, consumed by all three projects:

```json
{
  "georgia": {
    "short_name": "georgia",
    "s3_prefix": "georgia",
    "display_name": "Digital Library of Georgia",
    "institutions_json_key": "Digital Library of Georgia"
  },
  "hathi": {
    "short_name": "hathi",
    "s3_prefix": "hathitrust",
    "display_name": "HathiTrust",
    "institutions_json_key": "HathiTrust"
  },
  "tn": {
    "short_name": "tn",
    "s3_prefix": "tennessee",
    "display_name": "Digital Library of Tennessee",
    "institutions_json_key": "Digital Library of Tennessee"
  }
}
```

### Validation Test

A cross-project integration test verifies:
1. Every hub in `i3.conf` has an entry in `hub-registry.json`
2. Every S3 prefix in the registry corresponds to a directory in `s3://dpla-master-dataset/`
3. Every `institutions_json_key` matches a top-level key in `institutions_v2.json`

---

## Contract 2: S3 Output Path Semantics

Ingestion output paths must remain parseable and discoverable by sparkindexer's selection logic.

### Path Format

```
s3://dpla-master-dataset/{s3-prefix}/jsonl/{timestamp}-{hub-short-name}-MAP3_1.IndexRecord.jsonl/
```

- `{s3-prefix}`: From hub registry (usually the short name; exceptions: `hathi` -> `hathitrust`, `tn` -> `tennessee`)
- `{timestamp}`: Format `yyyyMMdd_HHmmss` (e.g., `20260215_103600`)
- `{hub-short-name}`: From `i3.conf` (e.g., `georgia`, `nara`)
- Files inside the directory: gzipped JSONL, one record per line
- `_SUCCESS` marker file indicates the directory is complete
- `_MANIFEST` file contains record counts

### Contract Rules

1. Path pattern is versioned and documented (this document is the canonical reference).
2. Timestamp format is fixed: `yyyyMMdd_HHmmss`. No timezone suffix, no alternative formats.
3. `_SUCCESS` plus `_MANIFEST` artifacts must be present before a directory is considered eligible for indexing.
4. Post-sync verification confirms destination integrity (local manifest matches S3 listing).

### Producer

`ingestion3`: `OutputHelper.scala` line 88 constructs the path. `s3-sync.sh` uploads to S3.

### Consumer

`sparkindexer`: `MasterDataset.scala` line 51 discovers paths by S3 listing and lexicographic comparison.

---

## Contract 3: Index Promotion (Alias Flip)

A candidate index is not considered promotable until all required validation checks pass. This is the highest-risk transition in the pipeline.

### Required Preconditions for Promotion

1. **Index exists and is queryable.** `GET /{index-name}/_stats` returns successfully.
2. **Expected schema and field presence.** A sample query returns records with all fields listed in the API field contract (see [Cross-Project DPLA API Field Contract](02-system-architecture.md#cross-project-dpla-api-field-contract)).
3. **Document count sanity.** Count is within an acceptable threshold of the previous production index (default: 2% tolerance). Large drops require human investigation.
4. **Cluster health.** `GET /_cluster/health` returns `green` or `yellow`. `red` blocks promotion.
5. **All shards assigned.** No unassigned shards for the candidate index.
6. **Previous alias target recorded.** The rollback target is known and documented before the flip executes.

### Elasticsearch Index Naming

```
dpla-all-{yyyyMMdd-HHmmss}
```

- Created by sparkindexer (`IndexerMain.scala`)
- Production alias: `dpla_alias` (points to the current active index)
- Previous indices retained for rollback (manual cleanup via `delete-old-indices.sh`)

---

## Contract 4: Wikimedia Refresh

Wikimedia ID refresh should occur only from a confirmed current production index state. Running refresh against a stale index causes incorrect eligibility determinations.

### Required Preconditions for Refresh

1. **Alias promotion completed and validated.** The post-flip health check has passed.
2. **Provider mapping contract passes.** `institutions_v2.json` is accessible and parseable.
3. **API health check passes.** A sample query to `api.dp.la/v2/items` returns expected results.
4. **All `institutions_v2.json` references use the `main` branch.** No references to `develop` or other branches.

### institutions_v2.json Format

```json
{
  "Hub Display Name": {
    "Wikidata": "Q12345",
    "upload": true,
    "institutions": {
      "Institution Display Name": {
        "Wikidata": "Q67890",
        "upload": true
      }
    }
  }
}
```

- **Location:** `ingestion3/src/main/resources/wiki/institutions_v2.json`
- **Canonical branch:** `main` (all references must use this branch)
- **Consumer:** ingest-wikimedia fetches from GitHub at runtime
- **Key matching:** `provider.name` from the DPLA API must exactly match a top-level key

---

## Safety Gates

Safety gates are mandatory checkpoints that block pipeline progression when something is wrong. When a gate fails, the pipeline stops and requires human intervention. These are not soft warnings -- they are hard blocks. The rationale for each gate is proportionate to the cost of the failure it prevents: Gate B (pre-flip) protects the single most consequential action in the monthly cycle.

### Gate A: Pre-Indexer Gate

**When:** Before launching the EMR indexing job.

**Block indexer start if:**
- Required ingestion outputs are missing or incomplete (no `_SUCCESS` markers in S3 for scheduled hubs)
- Run completeness policy is not met (e.g., fewer than N% of scheduled hubs completed successfully)
- sparkindexer JAR does not exist in S3

**Pass criteria:**
- All (or an acceptable subset of) scheduled hubs have `_SUCCESS` markers in their latest JSONL directories in S3
- JAR exists at `s3://dpla-sparkindexer/sparkindexer-assembly.jar`
- Human has approved proceeding (in controlled-execution mode) OR policy threshold is met (in automated mode)

**On failure:** Post to Slack with list of missing/incomplete hubs. Do not launch EMR.

### Gate B: Pre-Flip Gate (Critical)

**When:** After indexer completes, before alias promotion.

**Block alias promotion if any check fails:**

1. **Candidate index health.** Index exists, is queryable, cluster health is green/yellow.
2. **Document count sanity.** Count is within threshold of previous production index (default: ±2%). Significant deviations require human review.
3. **Sample query correctness.** Query 10 known DPLA IDs; verify each returns expected fields.
4. **Rollback target known.** Current `dpla_alias` target is recorded; that index still exists and is healthy.

**Pass criteria:** All four checks pass.

**On failure:** Post to Slack with specific failing checks and evidence. Do not flip. Require human investigation.

### Gate C: Post-Flip Gate

**When:** Immediately after alias flip executes.

**Actions:**
1. Run post-flip canary checks (query API, verify results come from new index by checking a record known to be new or updated).
2. Check API response times are within normal range.
3. Verify no elevated error rates.

**On canary failure:**
- Execute automatic rollback (flip alias back to previous index) within 5 minutes.
- Post high-severity alert to Slack.
- Block any downstream actions (Wikimedia refresh).

**On canary success:** Proceed to Wikimedia refresh.

### Gate D: Wikimedia Refresh Gate

**When:** After post-flip gate passes, before refreshing Wikimedia ID lists.

**Block refresh if:**
- Post-flip validation is incomplete or failed
- Provider mapping (`institutions_v2.json`) contains unresolved keys
- API contract check fails (sample query returns unexpected structure)

**On failure:** Post to Slack. Do not refresh ID lists. Wikimedia uploaders continue with existing lists (safe -- they just won't pick up new records until the issue is resolved).

---

## Observability Requirements

A gate that passes or fails silently provides no operational value. Every gate decision must emit a structured record containing:

| Field | Description |
|-------|-------------|
| `gate_name` | Which gate (A, B, C, D) |
| `result` | `PASS` or `FAIL` |
| `evidence` | Summary of what was checked and the values found |
| `run_id` | Unique identifier for the pipeline run |
| `stage` | Pipeline stage (ingestion, indexing, flip, wikimedia) |
| `timestamp` | ISO 8601 timestamp |
| `remediation_hint` | On failure: operator-facing description of what to investigate |

This record should be:
1. Logged to a file (for post-mortem analysis)
2. Posted to Slack (for immediate visibility)
3. Available to the pipeline status command (for unified view)

---

## Ownership Model

Today, one engineer effectively owns all of these contract areas. The table below documents intended ownership for when a team takes over -- so the receiving organization knows which role is responsible for each boundary, and who must be consulted when a change touches a shared interface.

| Contract Area | Primary Owner | Secondary Owner |
|---|---|---|
| Hub identity mapping | ingestion3 maintainer | Wikimedia operations |
| S3 path and metadata conventions | ingestion3 maintainer | sparkindexer maintainer |
| Index promotion rules | sparkindexer maintainer | orchestration/pipeline owner |
| Wikimedia refresh eligibility | ingest-wikimedia maintainer | orchestration/pipeline owner |
| API field contract | sparkindexer maintainer | ingest-wikimedia maintainer |

---

## Change Management Rules

A contract change that deploys to one project before the other will break the pipeline at the boundary between them. The following rules prevent that. Any contract change requires:

1. **Documented compatibility impact.** What breaks if this change is deployed to one project but not the others? Which downstream consumers are affected?
2. **Explicit version update.** Update the contract version in this document. Consumers should be able to detect they are operating against a changed contract.
3. **Staged rollout path.** Deploy the producer change first (with backward-compatible output), then deploy the consumer change, then remove backward compatibility.
4. **Rollback plan.** If the change causes problems, how do you revert? For S3 path changes, old data must remain accessible. For API field changes, the old field must continue to exist during transition.
5. **Cross-repo signoff.** Before deploying, verify all consuming repos have been updated or are compatible.

Without these, automated orchestration should treat the change as a potential contract violation and flag it for human review.

---

## Edge Cases That Must Be Handled

The following scenarios are known from operational experience. Each represents a situation where the contracts or gates could receive ambiguous input. Implementing the contracts without accounting for these will produce hard-to-diagnose failures in production.

- Hub with first-ever ingest (no historical baseline for anomaly comparison)
- Hub with legitimate large record drop (true data change vs. pipeline bug)
- Successful EMR completion with partial index quality issues (some records failed to write)
- API returns provider names with trailing whitespace or case differences
- IIIF/media source intermittently fails for a subset of partner records
- Partial S3 sync success without post-sync manifest verification
- Concurrent operations creating inconsistent "latest run" assumptions
- Notifications succeed while underlying stage outcome is ambiguous (the notification itself is not proof of success)

---

## Relationship to Greenfield Vision

These contracts are architecture-independent. Whether the pipeline runs on the current Scala+Python+EMR stack or a future Python+Fargate+Step Functions stack (see [Greenfield Vision](07-greenfield-vision.md)), the same contracts apply:

- Hubs must be named consistently across systems.
- S3 paths must follow a documented format.
- Index promotion must be validated before execution.
- Wikimedia refresh must use a confirmed-current index.

Getting these contracts right now means any future architecture change preserves pipeline correctness at the boundaries. The contracts are the stable core; everything else is implementation detail.
