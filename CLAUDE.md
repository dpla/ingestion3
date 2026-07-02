# DPLA Ingestion 3 — Claude Guide

This repo is DPLA's data pipeline. It pulls in metadata from cultural heritage partners across the US, cleans and standardizes it, and loads it into the DPLA search index at dp.la.

---

## What this repo does

1. **Harvest** — pulls records from partner systems (OAI-PMH feeds, APIs, or file exports)
2. **Map** — converts partner metadata into DPLA's standard format
3. **Enrich** — normalizes dates, languages, types, and rights statements
4. **Export** — writes records to JSONL files and syncs to S3
5. **Index** — loads the JSONL into Elasticsearch so records appear on dp.la

---

## How ingests are run

The day-to-day workflow is managed through the Python scripts in `ingest_python_scripts/`. These run everything remotely via AWS — you don't need to SSH into any servers.

See [`ingest_python_scripts/README.md`](ingest_python_scripts/README.md) for the full monthly workflow.

The underlying shell scripts that do the actual pipeline work live in `scripts/`. See [`scripts/SCRIPTS.md`](scripts/SCRIPTS.md) for details on those.

---

## Before running anything

A few things need to be in place:

- **`.env` file** — the project config (Java path, data directory, Slack webhook, etc.). Run `source .env` from the repo root before using any pipeline scripts. The file is gitignored; ask a teammate for a copy if you don't have one.
- **Java 11+** — required to build and run the Scala pipeline. Set `JAVA_HOME` in `.env`.
- **Python virtualenv** — use `./venv/bin/python` or `source ./venv/bin/activate` for any Python scripts.
- **AWS profile** — use `--profile dpla` for all AWS commands.

---

## Key files and folders

| Path | What it is |
|------|-----------|
| `ingest_python_scripts/` | Python scripts for running the monthly ingest workflow |
| `scripts/` | Shell scripts that run the actual pipeline steps on EC2 |
| `src/main/scala/` | Scala source code for harvest, mapping, enrichment, and JSONL export |
| `ingestion3-conf/` (separate repo) | Hub configuration — endpoints, emails, schedules (`i3.conf`) |
| `scheduler/orchestrator/` | Python orchestrator for running multiple hubs in parallel |

---

## If something goes wrong

- Pipeline errors go to **Slack #tech-alerts** (if `SLACK_WEBHOOK` is configured) or **tech@dp.la**
- See [`AGENTS.md`](AGENTS.md) for a full list of error patterns and what causes them
- Log files land in `~/data/<hub>/<stage>/<timestamp>/_LOGS/` on the ingest EC2
