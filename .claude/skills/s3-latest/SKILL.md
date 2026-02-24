---
name: s3-latest
description: Show the latest harvest, mapping, and JSONL data in S3 for a given hub
---

# s3-latest

Show the most recent S3 data for a hub across all three pipeline stages: harvest, mapping, and JSONL.

## When to Use

Use this skill when the user asks:
- "what is the latest data for [hub]"
- "what's the latest S3 data for [hub]"
- "when was [hub] last ingested"
- "check S3 for [hub]"
- "latest [hub] data"

## How It Works

Run three `aws s3 ls` commands in parallel against `s3://dpla-master-dataset/<hub>/` for the `harvest/`, `mapping/`, and `jsonl/` prefixes, sort by name (timestamps are ISO-prefixed so lexicographic sort = chronological), and display the most recent entry from each.

## Commands

```bash
# Run these in parallel (one call per stage)
aws s3 ls s3://dpla-master-dataset/<hub>/harvest/ --profile dpla
aws s3 ls s3://dpla-master-dataset/<hub>/mapping/ --profile dpla
aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/   --profile dpla
```

Pick the last entry from each listing (alphabetical sort = chronological for these timestamped directory names).

## Output Format

Present results as a markdown table:

| Stage | Latest | Date |
|-------|--------|------|
| **Harvest** | `YYYYMMDD_HHMMSS-<hub>-OriginalRecord.avro` | Mon DD, YYYY |
| **Mapping** | `YYYYMMDD_HHMMSS-<hub>-MAP4_0.MAPRecord.avro` | Mon DD, YYYY |
| **JSONL** | `YYYYMMDD_HHMMSS-<hub>-MAP3_1.IndexRecord.jsonl` | Mon DD, YYYY |

Parse the date from the directory name prefix (first 8 characters: `YYYYMMDD`) and format as human-readable (e.g. `Feb 4, 2026`).

## Examples

### Check latest data for MWDL
User: "what is the latest data for mwdl"

You should run:
```bash
aws s3 ls s3://dpla-master-dataset/mwdl/harvest/ --profile dpla
aws s3 ls s3://dpla-master-dataset/mwdl/mapping/ --profile dpla
aws s3 ls s3://dpla-master-dataset/mwdl/jsonl/   --profile dpla
```

Then display:

| Stage | Latest | Date |
|-------|--------|------|
| **Harvest** | `20260204_084647-mwdl-OriginalRecord.avro` | Feb 4, 2026 |
| **Mapping** | `20260205_080147-mwdl-MAP4_0.MAPRecord.avro` | Feb 5, 2026 |
| **JSONL** | `20260205_081350-mwdl-MAP3_1.IndexRecord.jsonl` | Feb 5, 2026 |

## Error Handling

| Situation | Response |
|-----------|----------|
| No entries in a stage | Show `(none)` for that row |
| Hub not found / bucket access error | Report the AWS error and confirm the hub name is correct in i3.conf |
| Stage directory missing entirely | Show `(no data)` for that row |

## Notes

- Always use `--profile dpla` for all AWS commands.
- Run all three `aws s3 ls` calls in parallel (single message, multiple Bash tool calls) for speed.
- Directory names are sorted lexicographically by AWS; the last entry is always the most recent.
- Some hubs may have an `enrichment/` stage too — only show it if the user asks.
