# DPLA Run Ingest

Run an ingest for a specific hub using the right runbook and scripts, then verify output. Use for single-hub or manual runs (for multi-hub/scheduled runs, use the orchestrator instead).

**Apply when:** User says run ingest for [hub], harvest [hub], remap [hub], or full pipeline for [hub].

**Environment:** See [AGENTS.md](../../AGENTS.md) § Environment and build.

## Checklist

0. **Build the JAR:** Run `source .env` from repo root (so `JAVA_HOME`, `SLACK_WEBHOOK`, `DPLA_DATA`, etc. are set), then `sbt assembly`. Scripts (`./scripts/harvest.sh`, `ingest.sh`, `remap.sh`) and `run_entry` in common.sh rely on `.env` being sourced and use the JAR when present or fall back to `sbt runMain` when missing — they do not auto-build the JAR.
1. **Identify the hub** (e.g. from the user message).
2. **Get harvest type** from i3.conf (`$I3_CONF`, default `~/dpla/code/ingestion3-conf/i3.conf`): `<hub>.harvest.type`. Values: `localoai`, `api`, `file`, `nara.file.delta`.
3. **Pick the runbook:** See [runbooks/README.md](../../runbooks/README.md) for harvest-type to runbook mapping.
4. **Run the scripts** from the runbook (see [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md)). Examples:
   - Full pipeline: `./scripts/ingest.sh <hub>`
   - Harvest only: `./scripts/harvest.sh <hub>`
   - Remap (mapping + enrich + jsonl): `./scripts/remap.sh <hub>`
   - NARA: `./scripts/harvest/nara-ingest.sh <nara-export.zip>`
5. **Verify** outputs: `_SUCCESS` in the step output dirs; `_MANIFEST` / `_SUMMARY` for counts.
6. **S3 sync** when the runbook says so: `./scripts/s3-sync.sh <hub>`.
7. **On failure:** Post to #tech-alerts or email tech@dp.la with hub, stage, and error or path to escalation report.

## Before/after checklist

**Before running:**
- If the run will use the pipeline (harvest/mapping/remap/etc.), run `source .env` from repo root, then `sbt assembly` so the fat JAR reflects the current code (or confirm no Scala changes since last build). Scripts depend on `.env` variables being loaded.
- Confirm hub and harvest type; open the correct runbook (or [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) if runbooks are not yet available).
- If using the orchestrator, ensure `SLACK_WEBHOOK` is set (or plan to email tech@dp.la on failure).

**After a run:**
- If any hub failed: post failure summary to #tech-alerts or email tech@dp.la; include stage and reference escalation report if present.
- If the run completed: completion notification is sent by the orchestrator when applicable; if you ran only scripts, consider notifying status to #tech-alerts or tech@dp.la if that's standard for your workflow.

## Critical Rules

- **NARA / Smithsonian:** Do not run the standard ingest pipeline without their dedicated runbooks (NARA: delta merge; Smithsonian: preprocessing e.g. fix-si.sh).
- **Output path:** All Scala `--output` must be `$DPLA_DATA` (the data root), never `$DPLA_DATA/<hub>`. Scripts handle this. OutputHelper builds paths as `rootPath / shortName / activity / timestamp-schema`.
- **Python/scripts:** Use `./venv/bin/python` for Python; run `./scripts/` scripts from repo root. AWS: `--profile dpla`.

## Verification

Check each step completed:
- `ls $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_SUCCESS`
- `ls $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUCCESS`
- `ls $DPLA_DATA/<hub>/enrichment/<timestamped-dir>/_SUCCESS`
- `ls $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_SUCCESS`
- Record counts: `cat $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_MANIFEST` or `_SUMMARY` in mapping/enrichment

Incomplete runs (directories with `_temporary` but no `_SUCCESS`) should be deleted before retrying.

## Resuming failed steps

1. Check which steps completed (look for `_SUCCESS` files in `harvest/`, `mapping/`, `enrichment/`, `jsonl/`).
2. Re-run only the failed step and later steps. Example: if mapping succeeded but enrichment failed, run `./scripts/enrich.sh <hub>` then `./scripts/jsonl.sh <hub>`.
3. For the full pipeline (IngestRemap), it must be re-run from scratch since it does mapping+enrichment+jsonl in one Spark application.

## Key References

- [runbooks/README.md](../../runbooks/README.md) — runbook index and harvest-type mapping
- [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) — script reference
- [AGENTS.md](../../AGENTS.md) — environment, notifications, error patterns
- i3.conf at $I3_CONF
