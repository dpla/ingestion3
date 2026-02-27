---
name: dpla-orchestrator
description: Run or monitor the DPLA Python ingest orchestrator. Use when the user says run orchestrator, parallel ingest, ingest status, run hubs, orchestrator dry-run, or retry failed hubs. Covers venv, main entry point, status script, and logs.
---

# DPLA Orchestrator

## Purpose
Run and monitor the Python orchestrator that drives the full ingestion pipeline (harvest → mapping → enrichment → JSONL → anomaly → S3 sync) for one or more hubs, with Slack notifications and parallel execution.

## When to Use
- "Run the orchestrator"
- "Parallel ingest"
- "Ingest status"
- "Run hubs X, Y, Z"
- "Orchestrator dry-run"
- "Retry failed hubs"
- "What's running in the orchestrator?"

**Environment:** Always run `source .env` from repo root before running the orchestrator so `JAVA_HOME`, `SLACK_WEBHOOK`, and other vars are set. Ensure the fat JAR is current: from repo root run `source .env` then `sbt assembly` before starting the orchestrator (or confirm no Scala changes since last build). Full checklist: [AGENTS.md](../../../AGENTS.md) § Environment and build.

## Always Use the Project Venv

```bash
source .env
./venv/bin/python -m scheduler.orchestrator.main [options]
```

Do not use system `python3`; use `./venv/bin/python` so dependencies and environment are correct.

## Main Commands

| Goal | Command |
|------|--------|
| Current month, all scheduled hubs | `./venv/bin/python -m scheduler.orchestrator.main` |
| Specific hubs | `./venv/bin/python -m scheduler.orchestrator.main --hub=wisconsin,p2p` |
| Parallel (2–3 hubs at once) | `./venv/bin/python -m scheduler.orchestrator.main --hub=wi,va,mn --parallel=3` |
| Specific month | `./venv/bin/python -m scheduler.orchestrator.main --month=2` |
| Preview only (no run) | `./venv/bin/python -m scheduler.orchestrator.main --dry-run` |
| Retry last run's failures | `./venv/bin/python -m scheduler.orchestrator.main --retry-failed` |
| Skip harvest (reuse data) | `./venv/bin/python -m scheduler.orchestrator.main --hub=wisconsin --skip-harvest` |
| Skip S3 sync | `./venv/bin/python -m scheduler.orchestrator.main --hub=wisconsin --skip-s3-sync` |

## Checking Status

Per-hub status is written to `logs/status/<hub>.status` (JSON). Use the status script:

```bash
./scripts/status/ingest-status.sh              # Table view
./scripts/status/ingest-status.sh --watch      # Auto-refresh (e.g. every 30s)
./scripts/status/ingest-status.sh wisconsin p2p # Specific hubs
./scripts/status/ingest-status.sh -v           # Verbose (stage history, durations)
./scripts/status/ingest-status.sh --json       # JSON (for scripting)
```

## Key Locations

| Resource | Path |
|----------|------|
| Orchestrator entry | scheduler/orchestrator/main.py |
| Config | scheduler/orchestrator/config.py; .env for SLACK_WEBHOOK, JAVA_HOME |
| Per-hub status | logs/status/<hub>.status |
| Escalation reports | data/escalations/failures-<run_id>.md |
| Email drafts (after run) | logs/hub-emails-<run_id>/ |

## Long-Running Runs

Harvests can run 12–24 hours. Use tmux or nohup so the run survives disconnection:

```bash
tmux new -s ingest
cd /path/to/ingestion3 && source .env
./venv/bin/python -m scheduler.orchestrator.main --hub=wisconsin,p2p --parallel=2
# Ctrl-B, D to detach; reattach with: tmux attach -t ingest
```

## Pipeline Stages (per hub)

1. Prepare (file hubs: download from S3)
2. Harvest
3. Mapping
4. Enrichment
5. JSONL export
6. Anomaly detection → S3 sync

Slack notifications go to #tech-alerts (and hub-complete to #tech when configured). Failures are written to `data/escalations/`.

## Reference

- Full runbook: [GOLDEN_PATH.md](../../../docs/ingestion/GOLDEN_PATH.md)
- Agent runbooks and notify policy: [AGENTS.md](../../../AGENTS.md)
