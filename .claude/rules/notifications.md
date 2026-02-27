# DPLA Notifications

Ensure the right people are notified when something fails or when status updates are needed (Slack #tech-alerts or email tech@dp.la).

**Apply when:** User asks to notify about a failure, post to tech-alerts, send failure alert, or asks who to notify when ingest fails.

**Environment:** See [AGENTS.md](AGENTS.md) § Environment and build.

## What to Report (failures)

| Stage | What to report |
|-------|----------------|
| Harvest | Feed unreachable, timeout, or harvest step failed |
| Mapping/remap | Mapping or IngestRemap failed (non-zero exit, no output) |
| Sync | S3 sync failed or was blocked |
| Anomaly | Anomaly detection halted sync (critical threshold) |

Include: hub name, stage that failed, one-line error or path to logs/escalation report.

## Where to Notify

- **Slack #tech-alerts:** Preferred when `SLACK_WEBHOOK` is set. Post: hub, stage, error or link to report.
- **Email tech@dp.la:** When Slack is unavailable or as backup. Same content.

The orchestrator posts failure alerts automatically; when running scripts manually, you must post or email.

## Test notifications

```bash
source .env
./venv/bin/python -m scheduler.orchestrator.main --dry-run-notify
```

Messages are sent with a `[TEST]` prefix.

## Reference

- [AGENTS.md](AGENTS.md) — full notification policy and #tech-alerts
