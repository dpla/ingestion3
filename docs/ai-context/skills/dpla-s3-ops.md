---
name: dpla-s3-ops
description: Run S3 sync, find latest S3 data, and perform AWS data operations for DPLA ingestion using the correct profile and scripts. Use when the user says sync to S3, check S3 sync, upload to S3, AWS bucket, check JSONL sync, latest S3 data for a hub, or when was hub last ingested.
---

# DPLA S3 Operations

## Purpose
Perform S3 and AWS data operations for the ingestion pipeline using the project's profile and scripts so credentials and buckets are correct. Includes finding latest data in S3 and syncing hub data.

## When to Use
- "Sync to S3"
- "Check S3 sync"
- "Upload to S3"
- "AWS bucket"
- "Check JSONL sync"
- "Latest S3 data for [hub]"
- "When was [hub] last ingested?"

**Environment:** Source `.env` from repo root before running scripts that need `DPLA_DATA` or AWS env (e.g. `source .env`).

## Always Use AWS Profile

All AWS CLI commands must use:

```bash
aws ... --profile dpla
```

Scripts in `scripts/` use `AWS_PROFILE=dpla` by default (see scripts/common.sh / SCRIPTS.md). When invoking `aws` directly, always add `--profile dpla`.

## Find Latest Data in S3

To find the latest harvest/mapping/jsonl exports in S3 for a hub:

```bash
source .env && bash scripts/status/s3-latest.sh <hub>
```

This checks `s3://dpla-master-dataset/<hub>/` for the most recent timestamped directories under harvest, mapping, and jsonl.

## Sync Hub Data to S3

Use the project script (handles anomaly detection and paths):

```bash
./scripts/s3-sync.sh <hub>
./scripts/s3-sync.sh <hub> <subdir>   # Optional: sync a specific subdir
```

The script uses the correct bucket and prefix; do not bypass it with raw `aws s3 sync` unless you have a specific reason and use `--profile dpla`.

## Check JSONL Sync Status

To see how local JSONL exports compare to S3:

```bash
./scripts/status/check-jsonl-sync.sh
```

Uses AWS profile via script env; see [scripts/SCRIPTS.md](../../../scripts/SCRIPTS.md) for options.

## Anomaly Detection

The orchestrator (and s3-sync.sh when used in that flow) runs anomaly checks before syncing. If counts or failure rates change sharply, sync may be blocked (critical) or proceed with a warning. Escalation and Slack alerts are sent; see [AGENTS.md](../../../AGENTS.md) and [GOLDEN_PATH.md](../../../docs/ingestion/GOLDEN_PATH.md).

## Manual AWS Commands (when needed)

If you must run `aws` directly (e.g. list bucket, copy one file):

```bash
aws s3 ls s3://bucket-name/ --profile dpla
aws s3 cp local s3://bucket/key --profile dpla
```

Never omit `--profile dpla`.

## Key References

| Resource | Path |
|----------|------|
| Script reference | [scripts/SCRIPTS.md](../../../scripts/SCRIPTS.md) (s3-sync.sh, check-jsonl-sync.sh, s3-latest.sh) |
| Agent / notify policy | [AGENTS.md](../../../AGENTS.md) |
