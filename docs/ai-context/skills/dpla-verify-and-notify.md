---
name: dpla-verify-and-notify
description: Verify ingest outcomes and send failure or status notifications to Slack or tech@dp.la. Use when the user asks to verify the ingest, check if it succeeded, notify about a failure, or post to tech-alerts.
---

# DPLA Verify and Notify

## Purpose
After a run or when something fails, verify pipeline output and ensure the right people are notified (Slack #tech-alerts or email tech@dp.la).

## When to Use
- "Verify the ingest"
- "Check if it succeeded"
- "Notify about the failure"
- "Post to tech-alerts"
- "Send failure alert"
- "Who do we notify when ingest fails?"

**Environment:** For commands that run the orchestrator or scripts, ensure `source .env` has been run in the shell so `JAVA_HOME`, `SLACK_WEBHOOK`, etc. are set.

## Verification

### Pipeline success markers
Each step writes `_SUCCESS` when complete:

```bash
ls $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/enrichment/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_SUCCESS
```

### Record counts
```bash
cat $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_MANIFEST
cat $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUMMARY
```

### Escalation reports (orchestrator runs)
When the orchestrator has failures, it writes:
- `data/escalations/failures-<run_id>.md` — human-readable
- `data/escalations/failures-<run_id>.json` — machine-readable

```bash
ls data/escalations/
cat data/escalations/failures-*.md | tail -1
```

## What to Report (failures)

| Stage | What to report |
|-------|----------------|
| Harvest | Feed unreachable, timeout, or harvest step failed |
| Mapping/remap | Mapping or IngestRemap failed (non-zero exit, no output) |
| Sync | S3 sync failed or was blocked |
| Anomaly | Anomaly detection halted sync (critical threshold) |

Include: hub name, stage that failed, one-line error or path to logs/escalation report.

## Where to Notify

- **Slack #tech-alerts:** Preferred when `SLACK_WEBHOOK` is set. Post a short message: hub, stage, error or link to report.
- **Email tech@dp.la:** Use when Slack is unavailable or as backup. Same content: hub, stage, error or attach/link failure report.

When using the orchestrator, it posts failure alerts automatically; when running scripts manually, you (or the agent) must post or email.

## Test notifications

To verify webhooks without running an ingest:

```bash
source .env
./venv/bin/python -m scheduler.orchestrator.main --dry-run-notify
```

Messages are sent with a `[TEST]` prefix.

## Reference

- Full notification policy: [AGENTS.md](AGENTS.md) (sections on notifications and #tech-alerts)
