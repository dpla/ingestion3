# DPLA Orchestrator

Run and monitor the Python orchestrator that drives the full ingestion pipeline (harvest → mapping → enrichment → JSONL → anomaly → S3 sync) for one or more hubs, with Slack notifications and parallel execution.

**Apply when:** User says run orchestrator, parallel ingest, ingest status, run hubs X Y Z, orchestrator dry-run, or retry failed hubs.

**Environment:** Always run `source .env` from repo root before running the orchestrator so `JAVA_HOME`, `SLACK_WEBHOOK`, and other vars are set. Ensure the fat JAR is current: from repo root run `source .env` then `sbt assembly` before starting the orchestrator (or confirm no Scala changes since last build). Full checklist: [AGENTS.md](AGENTS.md) § Environment and build.

## Always Use the Project Venv

```bash
# From repo root; source .env for JAVA_HOME, SLACK_WEBHOOK, etc.
source .env
./venv/bin/python -m scheduler.orchestrator.main [options]
```

Do not use system `python3`; use `./venv/bin/python`.

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

```bash
./scripts/status/ingest-status.sh              # Table view
./scripts/status/ingest-status.sh --watch      # Auto-refresh
./scripts/status/ingest-status.sh wisconsin p2p # Specific hubs
./scripts/status/ingest-status.sh -v           # Verbose
./scripts/status/ingest-status.sh --json       # JSON output
```

Per-hub status files: `logs/status/<hub>.status`.

## Key Locations

| Resource | Path |
|----------|------|
| Orchestrator entry | scheduler/orchestrator/main.py |
| Config | .env for SLACK_WEBHOOK, JAVA_HOME |
| Per-hub status | logs/status/<hub>.status |
| Escalation reports | data/escalations/failures-<run_id>.md |
| Email drafts (after run) | logs/hub-emails-<run_id>/ |

## Long-Running Runs

Use tmux or nohup so the run survives disconnection (e.g. Warp closing):

```bash
tmux new -s ingest
cd /path/to/ingestion3 && source .env
./venv/bin/python -m scheduler.orchestrator.main --hub=wisconsin,p2p --parallel=2
# Ctrl-B, D to detach; tmux attach -t ingest to reattach

# Or nohup:
nohup ./venv/bin/python -m scheduler.orchestrator.main --hub=wi,p2p --parallel=2 \
  > logs/orchestrator-$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

## Pipeline Stages (per hub)

1. Prepare (file hubs: download from S3)
2. Harvest
3. Mapping
4. Enrichment
5. JSONL export
6. Anomaly detection → S3 sync

Slack → #tech-alerts (and hub-complete to #tech when configured). Failures → `data/escalations/`.

## Reference

- [GOLDEN_PATH.md](GOLDEN_PATH.md) — full runbook
- [AGENTS.md](AGENTS.md) — runbooks and notification policy
