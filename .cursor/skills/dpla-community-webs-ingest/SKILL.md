---
name: dpla-community-webs-ingest
description: Run Community Webs ingest from SQLite DB. Use when the user says harvest community-webs, run community-webs ingest, export community webs, or process community webs DB.
---

# DPLA Community Webs Ingest

## Purpose
Run the Community Webs ingest workflow: export SQLite DB → JSONL → ZIP → harvest → (optional) full pipeline. Internet Archive sends a SQLite database; the scripts handle the intermediate export step automatically.

## When to Use
- "Harvest community-webs"
- "Run community-webs ingest"
- "Export community webs DB"
- "Process community webs"
- "Ingest community-webs from database"

**Environment:** Source `.env` before running so `JAVA_HOME`, `DPLA_DATA`, `I3_CONF` are set. Scripts that source `common.sh` load `$I3_HOME/.env` when present. Full checklist: [AGENTS.md](AGENTS.md) § Environment and build.

## Workflow

1. **Place the DB** in `$DPLA_DATA/community-webs/originalRecords/` (or provide path via `--db=`).
2. **Run export + harvest** (or full pipeline):
   - Export + harvest only: `./scripts/community-webs-ingest.sh`
   - Full pipeline (harvest + map + enrich + jsonl): `./scripts/community-webs-ingest.sh --full`
   - Update i3.conf with output path: `./scripts/community-webs-ingest.sh --full --update-conf`
3. **Verify** outputs: `_SUCCESS` in harvest/mapping/enrichment/jsonl dirs.

## Scripts

| Script | Purpose |
|--------|---------|
| `community-webs-export.sh` | Export DB → JSONL → ZIP; validates schema before zipping |
| `community-webs-ingest.sh` | Orchestrates export + harvest (+ optional full pipeline) |

## Options

**community-webs-export.sh:**
- `--db=PATH` — Explicit DB path (default: auto-detect latest `*.db` in originalRecords)
- `--update-conf` — Update i3.conf `community-webs.harvest.endpoint`
- `--skip-validate` — Skip JSONL schema validation (not recommended)

**community-webs-ingest.sh:**
- `--db=PATH` — Pass to export script
- `--skip-export` — Use existing ZIP (endpoint must already point to it)
- `--full` — Run harvest + mapping + enrichment + jsonl
- `--update-conf` — Update i3.conf with export output directory

## Output

- Export: `$DPLA_DATA/community-webs/originalRecords/<YYYYMMDD>/community-webs-<timestamp>.zip`
- Harvest: `$DPLA_DATA/community-webs/harvest/`
- Full pipeline: mapping, enrichment, jsonl under `$DPLA_DATA/community-webs/`

## Validation

The export script runs `community-webs-validate-jsonl.py` on the JSONL before zipping. It checks:
- Each record has required field `id`
- Each line is valid JSON
- Records with `"status":"deleted"` are skipped (harvester filters these)

Tests: `./venv/bin/python -m pytest scripts/tests/test_community_webs_validation.py -v`

## Critical Rules

- **DB location:** Place `*.db` in `$DPLA_DATA/community-webs/originalRecords/` or use `--db=/path/to/file.db`
- **Prerequisites:** `sqlite3`, `jq` (brew install / apt install)
- **Output path:** i3.conf `community-webs.harvest.endpoint` must point to directory containing the ZIP

## Key References

| Resource | Path |
|----------|------|
| Script reference | [scripts/SCRIPTS.md](scripts/SCRIPTS.md) |
| Ingest docs | [README_INGESTS.md](README_INGESTS.md) |
| Agent guide | [AGENTS.md](AGENTS.md) |
