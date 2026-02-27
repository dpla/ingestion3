---
name: dpla-ingest-debug
description: Debug and fix DPLA hub ingestion failures (harvest/mapping/enrichment/jsonl/s3-sync/anomaly). Use when user asks why a hub failed, to debug an ingest failure, check an escalation report, or retry a failed hub/stage.
---

# DPLA Ingest Debugging

## Goal
Quickly identify *what stage failed*, find the relevant logs/escalation report, apply a targeted fix, and re-run only the necessary steps.

## Environment
For any commands that depend on project env (especially the orchestrator), run `source .env` first so `JAVA_HOME`, `DPLA_DATA`, `I3_CONF`, and `SLACK_WEBHOOK` are available.

## Gather Context (fast path)

1) If this was an orchestrator run, start with the escalation report (if present):
```bash
ls -lt data/escalations/ | head
# then open the relevant failures-<run_id>.md
```

2) Check current per-hub status (orchestrator writes these continuously):
```bash
./scripts/status/ingest-status.sh --watch
# or for one hub:
./scripts/status/ingest-status.sh <hub>
```

3) Find recent logs:
```bash
ls -lt logs/ | head
```

## Classify the Failure Stage
Look for one of: `harvest`, `mapping`, `enrichment`, `jsonl`, `sync`, `anomaly`.

If you have only a hub name and “it failed”, the quickest approach is:
- check `logs/status/<hub>.status` (JSON), and
- check the newest matching log in `logs/`.

## Re-run the Minimal Steps

All commands below assume you’re running from repo root.

Harvest failed:
```bash
./scripts/harvest.sh <hub>
```

Mapping/enrichment/jsonl failed (re-run remap):
```bash
./scripts/remap.sh <hub>
```

S3 sync failed (or you want to re-sync after a successful run):
```bash
./scripts/s3-sync.sh <hub>
```

Orchestrator retry (failed hubs from last run):
```bash
./venv/bin/python -m scheduler.orchestrator.main --retry-failed
```

Orchestrator retry (one hub):
```bash
./venv/bin/python -m scheduler.orchestrator.main --hub=<hub>
```

## Common Issues / Fix Heuristics

- **Timeout / unreachable feed**: retry harvest; if repeated, confirm the endpoint in i3.conf and capture the exact error line for escalation.
- **Missing input** (mapping complains about harvest input): harvest didn’t produce output → re-run harvest, then remap.
- **sbt contention / orphan processes**: prefer using the fat JAR path via scripts (they rebuild as needed). If you must intervene, inspect before killing:
  - `pgrep -fl 'sbt|java.*ingestion3'`
  - then `kill <pid>` (avoid broad `pkill` patterns unless you’re sure).
- **Smithsonian / NARA special workflows**: do not improvise; follow the dedicated runbooks (Smithsonian preprocessing, NARA delta workflow).

## Verify Output
After re-running, verify `_SUCCESS` markers and counts:
```bash
# Examples
ls "$DPLA_DATA/<hub>/harvest"/*/_SUCCESS
ls "$DPLA_DATA/<hub>/mapping"/*/_SUCCESS
ls "$DPLA_DATA/<hub>/jsonl"/*/_SUCCESS

cat "$DPLA_DATA/<hub>/mapping"/*/_SUMMARY | head
```
