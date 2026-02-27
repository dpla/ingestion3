# DPLA Ingestion Runbooks

Index of runbooks for hub-specific ingest procedures. Start here to find the right runbook for a hub.

## Hub configuration

Hub configuration lives in i3.conf (`$I3_CONF`, default `~/dpla/code/ingestion3-conf/i3.conf`). Look up `<hub>.harvest.type` to determine harvest type and which runbook applies.

## Runbook mapping

| Harvest type / Hub | Runbook |
|--------------------|---------|
| `nara.file.delta` | [02-nara.md](02-nara.md) |
| Smithsonian (file hub) | [03-smithsonian.md](03-smithsonian.md) |
| virginias | [07-virginias.md](07-virginias.md) |
| community-webs | [08-community-webs.md](08-community-webs.md) |
| `localoai` | (future) 05-standard-oai-ingests.md |
| `api` | (future) 06-standard-api-ingests.md |
| `file` (general) | (future) 04-file-based-imports.md |

## Script reference

For script usage, options, and environment variables, see [scripts/SCRIPTS.md](../scripts/SCRIPTS.md).

## Agent workflow

1. Identify the hub (from user request or schedule).
2. Get harvest type from i3.conf: `<hub>.harvest.type`.
3. Select the runbook from the table above.
4. Follow the runbook steps.
5. Verify outputs and run S3 sync when the runbook indicates.
