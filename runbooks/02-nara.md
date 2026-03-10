# NARA Delta Ingest Runbook

**Placeholder runbook.** Full guide: [README_NARA.md](../docs/ingestion/README_NARA.md)

## Summary

NARA uses a delta ingest workflow. Quarterly ZIP exports contain XML records by export group. Do **not** use standard `ingest.sh`. Use `nara-ingest.sh` which handles preprocessing, base sync, delta harvest, merge, and pipeline.

## Key script

```bash
./scripts/harvest/nara-ingest.sh --month=YYYYMM
```

Examples:

- Single month: `./scripts/harvest/nara-ingest.sh --month=202601`
- Two months (chain): `./scripts/harvest/nara-ingest.sh --month=202512,202601`
- Skip pipeline (merge only): `./scripts/harvest/nara-ingest.sh --month=202601 --skip-pipeline`

## Steps (high level)

1. **Preprocess** — Unzip export groups, repackage as tar.gz
2. **Sync base** — Pull base harvest from S3 if needed
3. **Delta harvest** — Harvest new/updated records
4. **Merge** — Merge delta into base
5. **Pipeline** — Mapping, enrichment, JSONL export
6. **S3 sync** — `./scripts/s3-sync.sh nara`

## References

- [README_NARA.md](../docs/ingestion/README_NARA.md) — Full guide (prerequisites, troubleshooting, manual workflow)
- [scripts/SCRIPTS.md](../scripts/SCRIPTS.md) — nara-ingest.sh options
