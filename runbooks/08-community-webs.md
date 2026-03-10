# Community Webs Ingest Runbook

**Placeholder runbook.** Data source: Internet Archive SQLite database.

## Summary

Internet Archive Community Webs sends a SQLite database. Use `community-webs-export.sh` to export to JSONL and ZIP, then `community-webs-ingest.sh` to run harvest (and optionally the full pipeline).

## Key scripts

```bash
# Place DB in $DPLA_DATA/community-webs/originalRecords/
./scripts/harvest/community-webs-ingest.sh              # Export + harvest only
./scripts/harvest/community-webs-ingest.sh --full       # Export + harvest + mapping + enrichment + jsonl
./scripts/harvest/community-webs-ingest.sh --full --update-conf   # Also update i3.conf endpoint
```

Or step by step:

- `./scripts/harvest/community-webs-export.sh [--db=PATH]` — Export DB to JSONL, validate, ZIP
- `./scripts/harvest.sh community-webs` — Harvest
- `./scripts/ingest.sh community-webs --skip-harvest` — Pipeline

## Steps

1. **Place DB** — In `$DPLA_DATA/community-webs/originalRecords/` (or use `--db=/path/to/file.db`)
2. **Export + harvest** — `./scripts/harvest/community-webs-ingest.sh`
3. **Full pipeline** — Add `--full` and optionally `--update-conf`
4. **S3 sync** — `./scripts/s3-sync.sh community-webs`

## References

- [.cursor/skills/dpla-community-webs-ingest/SKILL.md](../../.cursor/skills/dpla-community-webs-ingest/SKILL.md) — Community Webs skill
- [scripts/SCRIPTS.md](../scripts/SCRIPTS.md) — community-webs-export.sh, community-webs-ingest.sh
