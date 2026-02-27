---
name: dpla-staged-report
description: Report which hubs have new JSONL staged in S3 for a given month, and optionally post the report to Slack. Use when user asks what hubs are staged/ready for indexing, /ingest staged, or what changed this month in S3.
---

# dpla-staged-report
Uses `scheduler/orchestrator/staged_report.py` to scan `s3://dpla-master-dataset/<hub>/jsonl/` for timestamped dirs in a target month.

## Commands
```bash
# Export vars from .env so child processes see them
set -a
source .env
set +a

# Current month (console)
./venv/bin/python -m scheduler.orchestrator.staged_report

# February (month=2)
./venv/bin/python -m scheduler.orchestrator.staged_report --month=2

# JSON output (for scripting)
./venv/bin/python -m scheduler.orchestrator.staged_report --month=2 --json

# Post to Slack (uses SLACK_TECH_WEBHOOK then SLACK_WEBHOOK)
./venv/bin/python -m scheduler.orchestrator.staged_report --month=2 --slack

# If needed, set AWS profile explicitly
./venv/bin/python -m scheduler.orchestrator.staged_report --month=2 --profile=dpla
```

## Notes
- Requires AWS CLI access to list `dpla-master-dataset`.
- If Slack posting is requested, ensure `SLACK_TECH_WEBHOOK` or `SLACK_WEBHOOK` is set (usually via `source .env`).