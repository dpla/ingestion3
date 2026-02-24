# Slack Slash Commands — Planning Document

**Status:** Out of scope for current implementation. Revisit when ready to implement.

**Context:** DPLA ingestion pipeline runs produce data staged in S3. Stakeholders often ask about ingest status. Proposed slash commands would provide on-demand answers without running scripts or checking dashboards.

---

## Proposed Commands

| Command | Purpose |
|---------|---------|
| `/ingest staged` | List hubs with JSONL data from the current month (staged in S3, awaiting next indexing run) |
| *(Future)* `/ingest status <hub>` | Status for a specific hub (last run, record counts) |
| *(Future)* `/ingest failures` | List recently failed hubs |

---

## How Slack Slash Commands Work

1. User types `/ingest staged` in Slack
2. Slack sends an HTTP POST to a configured Request URL (within 3 seconds)
3. The endpoint must respond within 3 seconds, or use `response_url` for async/deferred replies
4. Response is displayed in the channel

**Requirements:**
- Public HTTPS endpoint (Slack cannot reach localhost)
- Valid TLS certificate (no self-signed)
- Request URL must be stable (e.g. API Gateway, Lambda Function URL)

---

## Infrastructure Options

| Option | Pros | Cons |
|--------|------|------|
| **Lambda + API Gateway** | Serverless, pay-per-use | Need to wire up API Gateway |
| **Lambda + Function URL** | Simpler than API Gateway | One endpoint per function |
| **Lambda + EventBridge (scheduled)** | Good for Monday report | Not on-demand |
| **Flask/FastAPI on EC2/ECS** | Full control | Server to maintain |
| **Separate project (Terraform)** | IaC for Lambda, IAM, API Gateway | Extra repo/module |

**Recommendation:** Implement as a **separate project** with Terraform (or similar) for:
- Lambda function(s)
- IAM roles and policies (least privilege for S3 read)
- API Gateway or Function URL
- Optional: EventBridge rule for Monday-morning scheduled report

This keeps ingestion3 focused on the pipeline; slash commands become a thin integration layer.

---

## Implementation Sketch

### 1. Shared Logic (in ingestion3 or extracted)

```
scheduler/orchestrator/staged_report.py
  get_staged_hubs_for_month(bucket, month, year, aws_profile) -> list[str]
```

- List `s3://dpla-master-dataset/` top-level prefixes (hubs)
- For each hub, list `s3://dpla-master-dataset/<hub>/jsonl/`
- Filter dirs by timestamp prefix (e.g. `202602` for February 2026)
- Return list of hub names

Can be reused by:
- Cron / Lambda scheduled job (Monday report)
- Slash command handler (on-demand)

### 2. Slash Command Handler (separate project)

**Lambda handler (pseudo-code):**
```python
def lambda_handler(event, context):
    # event is API Gateway / Function URL payload
    body = parse_slack_payload(event.get("body"))
    if body.get("command") == "/ingest" and body.get("text") == "staged":
        hubs = get_staged_hubs_for_month(bucket, month, year)
        return respond_200(f"Hubs with new data staged this month: {', '.join(hubs)}")
```

**IAM:** Read-only access to `s3://dpla-master-dataset/*` (ListBucket, GetObject on jsonl prefixes).

### 3. Slack App Setup

1. Create app at [api.slack.com/apps](https://api.slack.com/apps)
2. Add Slash Command: `/ingest` with Request URL = Lambda Function URL or API Gateway URL
3. Install app to workspace
4. Optionally add shortcut/description for subcommands (`staged`, etc.)

---

## S3 Prefix Mapping

ingestion3 uses `Config.S3_PREFIX_MAP` (hathi→hathitrust, tn→tennessee). S3 listings use **S3 prefixes**, not hub names. The staged report should map S3 prefixes back to hub names for display (reuse `S3_PREFIX_TO_HUB` from `backlog_emails.py` or equivalent).

---

## Next Steps (When Revisiting)

1. Create separate repo or terraform module: `dpla-ingest-slack-bot` or similar
2. Implement `get_staged_hubs_for_month()` in ingestion3 (or extract to shared package)
3. Define Terraform: Lambda, IAM, API Gateway/Function URL
4. Create Slack app and configure `/ingest` command
5. Add `.env.example` for `SLACK_SIGNING_SECRET` (verify requests) and any config
6. Optionally add EventBridge rule for Monday-morning report to #tech

---

## References

- [Slack Slash Commands](https://api.slack.com/interactivity/slash-commands)
- [AWS Lambda Function URLs](https://docs.aws.amazon.com/lambda/latest/dg/urls-intro.html)
- ingestion3: [scheduler/orchestrator/backlog_emails.py](../scheduler/orchestrator/backlog_emails.py) — S3 discovery pattern
- ingestion3: [scheduler/orchestrator/anomaly_detector.py](../scheduler/orchestrator/anomaly_detector.py) — `list_s3_directories`, `get_counts_from_s3`
