# DPLA S3 and AWS Tools

Perform S3 and AWS data operations for the ingestion pipeline using the project's scripts. Includes finding latest data in S3 and syncing hub data.

**Apply when:** User says sync to S3, check S3 sync, upload to S3, AWS bucket, check JSONL sync, latest S3 data, or when was hub last ingested.

**Environment:** See [AGENTS.md](../../AGENTS.md) § Environment and build.

## AWS Credentials

No named profile is needed in GitHub Actions (OIDC role) or on the ingest EC2 (`common.sh` unsets `AWS_PROFILE` so the instance role is used). Scripts in `scripts/` pass `--profile` only when `AWS_PROFILE` is set.

Locally, set `AWS_PROFILE=dpla` in `.env` (or export it) unless your default AWS profile is the DPLA account. Only the Python launchers in `ingest_python_scripts/` fall back to `dpla`; shell scripts and raw `aws` commands use your default credentials.

## Find Latest Data in S3

To find the latest harvest/mapping/jsonl exports in S3 for a hub:

```bash
source .env && bash scripts/status/s3-latest.sh <hub>
```

This checks `s3://dpla-master-dataset/<hub>/` for the most recent timestamped directories under harvest, mapping, and jsonl.

## Sync Hub Data to S3

Use the project script (handles hub-to-S3-prefix aliases and paths):

```bash
./scripts/s3-sync.sh <hub>
./scripts/s3-sync.sh <hub> <subdir>   # Optional: specific subdir
```

Do not bypass with raw `aws s3 sync` unless necessary.

## Check JSONL Sync Status

```bash
./scripts/status/check-jsonl-sync.sh
```

See [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) for options.

## Anomaly Detection

The orchestrator runs anomaly checks before syncing; `ingest.sh` runs its own >5% record-drop safety check before syncing JSONL. `s3-sync.sh` itself does not. See [AGENTS.md](../../AGENTS.md).

## Reference

- [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) — s3-sync.sh, check-jsonl-sync.sh, s3-latest.sh
- [AGENTS.md](../../AGENTS.md) — notify policy
