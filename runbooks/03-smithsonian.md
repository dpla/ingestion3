# Smithsonian Ingest Runbook

**Placeholder runbook.** Full guide: [README_SMITHSONIAN.md](../docs/ingestion/README_SMITHSONIAN.md)

## Summary

Smithsonian is file-based. Data comes from S3 (`dpla-hub-si`). Preprocessing is required: download, recompress gzip files, run xmll. The orchestrator handles preprocessing for automated runs. For manual runs, use `fix-si.sh`.

## Key scripts

1. **Download** — From `s3://dpla-hub-si/` to `$DPLA_DATA/smithsonian/originalRecords/`
2. **Preprocess** — `./scripts/harvest/fix-si.sh <folder>`
3. **Harvest** — `./scripts/harvest.sh smithsonian`
4. **Pipeline** — `./scripts/ingest.sh smithsonian --skip-harvest`

## Note

The orchestrator's `_preprocess_smithsonian()` handles preprocessing for scheduled runs. For manual runs, follow [README_SMITHSONIAN.md](../docs/ingestion/README_SMITHSONIAN.md) for recompress and xmll steps, or use `fix-si.sh` where applicable.

## References

- [README_SMITHSONIAN.md](../docs/ingestion/README_SMITHSONIAN.md) — Preprocessing details (recompress, xmll)
- [scripts/SCRIPTS.md](../scripts/SCRIPTS.md) — fix-si.sh, harvest.sh
