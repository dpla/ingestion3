# Digital Virginias Ingest Runbook

**Placeholder runbook.** Data source: GitHub repositories (dplava org).

## Summary

Digital Virginias publishes metadata in multiple GitHub repos. Use `harvest-va.sh` to clone repos and create ZIP files. Then update i3.conf and run the standard harvest + pipeline.

## Key script

```bash
./scripts/harvest/harvest-va.sh [output_directory]
```

Default output: `~/dplava-harvest`. The script clones repos from github.com/dplava, creates ZIPs in `output/input/`.

## Steps

1. **Clone and package** — `./scripts/harvest/harvest-va.sh`
2. **Update i3.conf** — Set `virginias.harvest.endpoint = "<path-to-input-dir>"` (the script prints the value)
3. **Harvest** — `./scripts/harvest.sh virginias`
4. **Pipeline** — `./scripts/ingest.sh virginias --skip-harvest`
5. **S3 sync** — `./scripts/s3-sync.sh virginias`

## Requirements

- GitHub CLI (`gh`) must be installed and authenticated

## References

- [scripts/SCRIPTS.md](../scripts/SCRIPTS.md) — harvest-va.sh section
- i3.conf — virginias.harvest.endpoint
