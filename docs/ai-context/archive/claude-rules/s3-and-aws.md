# DPLA S3 and AWS

Perform S3 and AWS data operations for the ingestion pipeline using the project's profile and scripts.

**Apply when:** User says sync to S3, check S3 sync, upload to S3, AWS bucket, or check JSONL sync.

**Environment:** See [AGENTS.md](AGENTS.md) § Environment and build.

## Always Use AWS Profile

All AWS CLI commands must use:

```bash
aws ... --profile dpla
```

Scripts in `scripts/` use `AWS_PROFILE=dpla` by default. When invoking `aws` directly, always add `--profile dpla`.

## Sync Hub Data to S3

Use the project script (handles anomaly detection and paths):

```bash
./scripts/s3-sync.sh <hub>
./scripts/s3-sync.sh <hub> <subdir>   # Optional: specific subdir
```

Do not bypass with raw `aws s3 sync` unless necessary—and then still use `--profile dpla`.

## Check JSONL Sync Status

```bash
./scripts/status/check-jsonl-sync.sh
```

See [scripts/SCRIPTS.md](scripts/SCRIPTS.md) for options.

## Anomaly Detection

The orchestrator and s3-sync.sh run anomaly checks before syncing. See [AGENTS.md](AGENTS.md).

## Manual AWS Commands

If you must run `aws` directly, always use `--profile dpla`.

## Reference

- [scripts/SCRIPTS.md](scripts/SCRIPTS.md) — s3-sync.sh, check-jsonl-sync.sh
- [AGENTS.md](AGENTS.md) — notify policy
