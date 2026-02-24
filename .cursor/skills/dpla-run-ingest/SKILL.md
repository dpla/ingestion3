---
name: dpla-run-ingest
description: Execute a single-hub or manual ingest by following the correct runbook and scripts. Use when the user says run ingest for [hub], harvest [hub], remap [hub], or run the full pipeline for [hub]. Ensures harvest type is identified, correct runbook and scripts are used, and outputs are verified.
---

# DPLA Run Ingest

## Purpose
Run an ingest for a specific hub using the right runbook and scripts, then verify output. Use this for single-hub or manual runs (for multi-hub/scheduled runs, use the orchestrator instead).

## When to Use
- "Run ingest for [hub]"
- "Harvest [hub]"
- "Remap [hub]"
- "Full pipeline for [hub]"
- "Start harvest for [hub]"

**Environment:** Scripts that source `common.sh` (harvest.sh, ingest.sh, remap.sh, etc.) automatically load `$I3_HOME/.env` when present, so `JAVA_HOME`, `DPLA_DATA`, `I3_CONF`, `SLACK_WEBHOOK`, etc. are set before the JAR is built or the pipeline runs. You do not need to run `source .env` separately. Full checklist: [AGENTS.md](AGENTS.md) § Environment and build.

## Checklist

0. **JAR is built automatically:** When you run `./scripts/harvest.sh` (or ingest.sh, remap.sh, etc.), `run_entry` in common.sh runs `sbt assembly` if the JAR is missing or if any Scala source is newer than the JAR. So "harvest indiana" will use current code without a separate build step. (You can still run `sbt assembly` first to avoid a build delay on the first harvest.)
1. **Identify the hub** (e.g. from the user message).
2. **Get harvest type** from i3.conf (`$I3_CONF`, default `~/dpla/code/ingestion3-conf/i3.conf`): `<hub>.harvest.type`. Values: `localoai`, `api`, `file`, `nara.file.delta`.
3. **Pick the runbook** (see [AGENTS.md](AGENTS.md)):
   - `localoai` → runbooks/05-standard-oai-ingests.md
   - `api` → runbooks/06-standard-api-ingests.md
   - `file` → runbooks/04-file-based-imports.md (Smithsonian/NARA have dedicated runbooks)
   - `nara.file.delta` → runbooks/02-nara.md
   - Smithsonian (file) → runbooks/03-smithsonian.md
4. **Run the scripts** from the runbook (see [scripts/SCRIPTS.md](scripts/SCRIPTS.md)). Examples:
   - Full pipeline: `./scripts/ingest.sh <hub>`
   - Harvest only: `./scripts/harvest.sh <hub>`
   - Remap (mapping + enrich + jsonl): `./scripts/remap.sh <hub>`
   - NARA: `./scripts/harvest/nara-ingest.sh <nara-export.zip>`
5. **Verify** outputs: `_SUCCESS` in the step output dirs; `_MANIFEST` / `_SUMMARY` for counts.
6. **S3 sync** when the runbook says so: `./scripts/s3-sync.sh <hub>`.
7. **On failure:** Post to #tech-alerts or email tech@dp.la with hub, stage, and error or path to escalation report.

## Critical Rules

- **NARA / Smithsonian:** Do not run the standard ingest pipeline without their dedicated runbooks (NARA: delta merge; Smithsonian: preprocessing e.g. fix-si.sh).
- **Output path:** All Scala `--output` must be `$DPLA_DATA` (the data root), never `$DPLA_DATA/<hub>`. Scripts handle this; do not override with a hub-specific output path.
- **Python/scripts:** Use `./venv/bin/python` for Python; use `./scripts/` scripts from repo root. AWS: `--profile dpla`.

## Verification Commands

```bash
# Check a step completed
ls $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_SUCCESS

# Record counts
cat $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_MANIFEST
cat $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUMMARY
```

Incomplete runs (e.g. `_temporary` but no `_SUCCESS`) should be deleted before retrying.

## Key References

| Resource | Path |
|----------|------|
| Agent guide and runbooks | [AGENTS.md](AGENTS.md) |
| Script reference | [scripts/SCRIPTS.md](scripts/SCRIPTS.md) |
| Config | i3.conf at $I3_CONF |
| Runbook index | runbooks/README.md |
