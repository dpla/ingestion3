# Conversation Context: Ingest Pipeline Decomposition

Saved 2026-02-25 to resume this work later.

## Problem Statement

There are too many ways to run an ingest (auto-ingest.sh, ingest.sh, batch-ingest.sh, remap.sh, orchestrator), creating cognitive load for both agents and humans. The user's natural workflow is:

1. `schedule.sh feb` -- see what hubs are scheduled
2. Pick hubs manually (not all may be ready)
3. `ingest.sh vt mwdl nypl` or batch-ingest -- run those hubs

This makes auto-ingest.sh (which runs all scheduled hubs for a month) unnecessary. The ambiguity between ingest.sh, remap.sh, and auto-ingest.sh is confusing even to the author.

## Decisions Made

1. **Remove auto-ingest.sh** -- the "run all February hubs" workflow is replaced by manual hub selection after viewing the schedule.

2. **Remove remap.sh** -- redundant once ingest.sh chains atomic scripts. Use `ingest.sh --skip-harvest --skip-s3-sync` or run mapping/enrich/jsonl individually.

3. **ingest.sh auto-detects file hubs** -- calls `download-s3-data.sh` automatically for file hubs (florida, vt, ct, georgia, heartland, ohio, txdl, northwest-heritage, smithsonian) rather than requiring a separate manual step. Makes ingest.sh truly fire-and-forget.

4. **ingest.sh chains atomic scripts** -- instead of calling HarvestEntry and IngestRemap directly, it calls harvest.sh, mapping.sh, enrich.sh, jsonl.sh. This means ingest.sh and the orchestrator use the same building blocks.

5. **schedule.sh shows "Latest data" column** -- calls `s3-latest.sh --stage=jsonl --date-only` per hub to show the last JSONL date in S3. Helps decide which hubs need re-ingesting.

6. **s3-latest.sh gets --stage and --date-only flags** -- reduces AWS calls from 3 to 1 per hub when only one stage is needed.

## Key Findings During Research

### Current script roles (before changes)

| Script | What it does | How it runs stages |
|--------|-------------|-------------------|
| ingest.sh | Full pipeline, single hub | Calls HarvestEntry + IngestRemap DIRECTLY (not harvest.sh/mapping.sh) |
| harvest.sh | Harvest only | Calls HarvestEntry via run_entry |
| remap.sh | Mapping+enrichment+jsonl | Calls IngestRemap via run_ingest_remap |
| mapping.sh | Mapping only | Calls MappingEntry via run_entry |
| enrich.sh | Enrichment only | Calls EnrichEntry via run_entry |
| jsonl.sh | JSONL export only | Calls JsonlEntry via run_entry |
| auto-ingest.sh | Schedule-based, S3 download, harvest, remap, sync, email | Calls harvest.sh + remap.sh |
| batch-ingest.sh | Multiple hubs | Calls ingest.sh per hub |
| Orchestrator | Parallel, Slack, anomaly detection | Calls harvest.sh + mapping.sh + enrich.sh + jsonl.sh individually |

### Status tracking gap

- `harvest.sh`, `ingest.sh`, `remap.sh` call `write_hub_status`
- `mapping.sh`, `enrich.sh`, `jsonl.sh`, `s3-sync.sh` do NOT
- The status system (`state.py`, `status.py`) already supports `mapping`, `enriching`, `jsonl_export` stages
- `write_status.py` help text only lists `preparing, harvesting, remapping, syncing, complete, failed` but accepts any value

### Smithsonian preprocessing (3 implementations)

1. `auto-ingest.sh` `preprocess_smithsonian()` -- recompress + xmll + update i3.conf (BEING DELETED)
2. `hub_processor.py` `_preprocess_smithsonian()` -- Python async version, self-contained (UNAFFECTED)
3. `fix-si.sh` -- standalone, different behavior (moves files, does NOT update i3.conf) (NEEDS --update-conf flag)

### File hub S3 source buckets (duplicated in 2 places)

Bash (`auto-ingest.sh` / new `download-s3-data.sh`):
```
florida → dpla-hub-fl, smithsonian → dpla-hub-si, vt → dpla-hub-vt
ct → dpla-hub-ct, georgia → dpla-hub-ga, heartland → dpla-hub-mo
ohio → dpla-hub-ohio, txdl → dpla-hub-tdl
northwest-heritage → dpla-hub-northwest-heritage, nara → dpla-hub-nara
```

Python (`scheduler/orchestrator/config.py` `S3_SOURCE_BUCKETS`): same list.

### The orchestrator's auto-ingest.sh dependency

`hub_processor.py` `prepare()` (lines 125-128) sources the entire `auto-ingest.sh` to call `download_s3_data`:
```python
source ./auto-ingest.sh && download_s3_data {hub}
```
This triggers all of auto-ingest.sh's top-level setup (Java, logging, etc.) as a side effect. The new `download-s3-data.sh` is standalone and called as a subprocess, eliminating this.

### ingest.sh's --output path issue

`ingest.sh` sets `--output=$HARVEST_DIR` (which is `$DPLA_DATA/$PROVIDER/harvest`), but `harvest.sh` correctly uses `--output=$DPLA_DATA`. Per AGENTS.md, OutputHelper builds paths as `root/shortName/activity/timestamp-schema`, so `--output` must always be `$DPLA_DATA` (the data root). Refactoring ingest.sh to call harvest.sh directly fixes this.

## Files to Change (summary)

### Create
- `scripts/harvest/download-s3-data.sh` -- new, extracted from auto-ingest.sh

### Modify
- `scripts/status/s3-latest.sh` -- add --stage, --date-only
- `scripts/communication/schedule.sh` -- add Latest data column, --no-s3
- `scripts/mapping.sh` -- add write_hub_status
- `scripts/enrich.sh` -- add write_hub_status
- `scripts/jsonl.sh` -- add write_hub_status
- `scripts/s3-sync.sh` -- add write_hub_status
- `scripts/harvest/fix-si.sh` -- add --update-conf
- `scripts/ingest.sh` -- refactor to chain atomic scripts
- `scheduler/orchestrator/hub_processor.py` -- update prepare(), remove remap()
- `scheduler/orchestrator/write_status.py` -- update help text

### Delete
- `scripts/auto-ingest.sh`
- `scripts/remap.sh`

### Documentation to update
- `scripts/SCRIPTS.md`
- `scripts/tests/test-scripts.sh`
- `.claude/skills/s3-latest/SKILL.md`
- `.claude/skills/dpla-hub-info/SKILL.md`
- `.cursor/skills/dpla-hub-info/SKILL.md`
- `.cursor/skills/dpla-run-ingest/SKILL.md`
- `.cursor/skills/dpla-monitor-ingest-remap/SKILL.md`
- `.cursor/skills/dpla-ingest-debug/SKILL.md`
- `.claude/rules/run-ingest.md`
- `AGENTS.md`
- `docs/ingestion/GOLDEN_PATH.md`
- `docs/ingestion/README_NARA.md`
- `scheduler/orchestrator/config.py` (cross-reference comment)
