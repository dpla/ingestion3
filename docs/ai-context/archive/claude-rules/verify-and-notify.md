# DPLA Verify and Notify

After a run or when something fails, verify pipeline output and ensure the right people are notified (Slack #tech-alerts or email tech@dp.la).

**Apply when:** User asks to verify the ingest, check if it succeeded, notify about a failure, or post to tech-alerts.

**Environment:** See [AGENTS.md](AGENTS.md) § Environment and build.

## Verification

Pipeline markers: `$DPLA_DATA/<hub>/{harvest,mapping,enrichment,jsonl}/<timestamped-dir>/_SUCCESS`. Escalation reports: `data/escalations/failures-<run_id>.md`. See [run-ingest.md](run-ingest.md) for verification details and resuming failed steps.

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
