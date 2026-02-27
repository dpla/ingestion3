# DPLA Validation

Verify pipeline output after a run to confirm success or identify failures.

**Apply when:** User asks to verify the ingest, check if it succeeded, or check pipeline output.

**Environment:** See [AGENTS.md](../../AGENTS.md) § Environment and build.

## Pipeline success markers

Each step writes `_SUCCESS` when complete:

```bash
ls $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/enrichment/<timestamped-dir>/_SUCCESS
ls $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_SUCCESS
```

## Record counts

```bash
cat $DPLA_DATA/<hub>/harvest/<timestamped-dir>/_MANIFEST
cat $DPLA_DATA/<hub>/mapping/<timestamped-dir>/_SUMMARY
```

## Escalation reports (orchestrator runs)

When the orchestrator has failures, it writes:
- `data/escalations/failures-<run_id>.md` — human-readable
- `data/escalations/failures-<run_id>.json` — machine-readable

```bash
ls data/escalations/
cat data/escalations/failures-*.md | tail -1
```

## Incomplete runs

Directories with `_temporary` but no `_SUCCESS` indicate an incomplete Spark write (crash or kill). Delete the incomplete timestamped directory and retry.

## Reference

- [AGENTS.md](../../AGENTS.md) — error patterns and fixes
