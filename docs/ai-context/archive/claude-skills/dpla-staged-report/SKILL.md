---
name: dpla-staged-report
description: Report which hubs have new JSONL staged in S3 for a given month, and optionally post the report to Slack. Use when user asks what hubs are staged/ready for indexing or /ingest staged.
---

# dpla-staged-report
```bash
set -a
source .env
set +a

./venv/bin/python -m scheduler.orchestrator.staged_report --month=2

# Optional
# ./venv/bin/python -m scheduler.orchestrator.staged_report --month=2 --slack
# ./venv/bin/python -m scheduler.orchestrator.staged_report --month=2 --json
```