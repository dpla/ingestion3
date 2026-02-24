---
name: send-email
description: Send ingest summary email to hub contacts on demand; reads email addresses from i3.conf and sends mapping summary with error logs
---

# send-email

Send DPLA ingest summary emails to hub contacts on demand. Useful for re-sending emails or sending summaries after manual ingests.

## When to Use

Use this skill when the user asks to:
- "send email for [hub]"
- "send summary email to [hub]"
- "resend email for [hub]"
- "email [hub] about their ingest"

## How It Works

The skill wraps the existing `scripts/send-ingest-email.sh` script which:
1. Reads hub email from `i3.conf` (e.g., `nara.email = "contact@nara.gov"`)
2. Finds the most recent mapping output directory for the hub
3. Extracts the `_SUMMARY` file content
4. Uses the Scala `Emailer` utility to send the email with:
   - Mapping summary (attempted, successful, failed counts)
   - Error/warning details
   - Pre-signed S3 links to full logs (7-day expiration)

## Usage

**Basic usage (latest mapping):**
```bash
source .env && ./scripts/send-ingest-email.sh <hub-name>
```

**Non-interactive (skip confirmation):**
```bash
source .env && ./scripts/send-ingest-email.sh --yes <hub-name>
```

**Specific mapping directory:**
```bash
source .env && ./scripts/send-ingest-email.sh <hub-name> <mapping-dir-path>
```

**Options:**
- `--yes` or `-y`: Skip the confirmation prompt and send immediately (useful for automation)

## Requirements

1. **Environment**: Must run `source .env` first to set `JAVA_HOME`, `DPLA_DATA`, `I3_HOME`, `I3_CONF`
2. **Hub config**: Hub must have email configured in i3.conf:
   ```hocon
   nara.email = "contact@example.com"
   # or multiple recipients
   maryland.email = "person1@example.com,person2@example.com"
   ```
3. **Mapping output**: Hub must have at least one mapping output directory at `$DPLA_DATA/<hub>/mapping/`
4. **_SUMMARY file**: The mapping directory must contain a `_SUMMARY` file

## Examples

### Send email for NARA
User: "send email for nara"

You should:
```bash
source .env && ./scripts/send-ingest-email.sh --yes nara
```

Note: The `--yes` flag skips the confirmation prompt. If the user wants to review first, omit it.

### Send email for a specific Maryland mapping
User: "send email for maryland using the mapping from Feb 10"

You should:
1. Find the Feb 10 mapping directory:
   ```bash
   ls -d $DPLA_DATA/maryland/mapping/202402*
   ```
2. Send email with that specific directory:
   ```bash
   source .env && ./scripts/send-ingest-email.sh maryland /path/to/mapping/dir
   ```

### Bulk email sending
User: "send emails for wisconsin, p2p, and maryland"

You should:
```bash
source .env
./scripts/send-ingest-email.sh --yes wisconsin
./scripts/send-ingest-email.sh --yes p2p
./scripts/send-ingest-email.sh --yes maryland
```

## Confirmation Prompt

By default, the script shows a preview of the summary and prompts:
```
Summary Preview:
---
[First 30 lines of _SUMMARY]
...
---

Send email? (y/n)
```

**When running via Claude Code:**
- Use `--yes` flag to skip the prompt when the user has already confirmed
- Example: `./scripts/send-ingest-email.sh --yes nara`
- Without `--yes`, the script will pause for user confirmation

## Error Handling

| Error | Meaning | Solution |
|-------|---------|----------|
| "No email configured for hub 'X'" | Missing `X.email` in i3.conf | Add email to i3.conf: `X.email = "contact@example.com"` |
| "No mapping directory found" | Hub has no mapping output | Run mapping first: `./scripts/remap.sh <hub>` |
| "Summary file not found" | Mapping incomplete or failed | Check mapping logs; rerun if needed |
| "Failed to send email" | Emailer.scala failed | Check Java/sbt setup; verify JAVA_HOME |

## Email Content

The email includes:
- **Subject**: `DPLA Ingest Summary for [Provider Name] - [Month Year]`
- **Body**:
  - Harvest count (if available)
  - Mapping summary (attempted, successful, failed)
  - Error/warning breakdown by type
  - Pre-signed S3 links to:
    - `_SUMMARY` file
    - Error logs ZIP (if there are failed records)
- **Attachment**: ZIP file with detailed error logs (sent via S3 link)

## Integration with Orchestrator

The orchestrator automatically creates **email drafts** at `logs/hub-emails-<run_id>/` after a run completes. These drafts are for review but are **not automatically sent**.

To send emails after an orchestrator run:
1. Review drafts: `ls logs/hub-emails-<run_id>/`
2. Send manually using this skill for each hub

## Notes

- **Test mode**: The Emailer.scala respects test mode and will send to `scott@dp.la` instead of hub contacts if configured
- **Multiple recipients**: i3.conf emails can be comma-separated: `"user1@example.com,user2@example.com"`
- **S3 links expire after 7 days** - recipients should download logs promptly
- **Run from repo root** - the script expects to be run from the ingestion3 repo root

## Related

- **Script source**: `scripts/send-ingest-email.sh`
- **Scala Emailer**: `src/main/scala/dpla/ingestion3/utils/Emailer.scala`
- **Config**: i3.conf at `$I3_CONF` (default: `~/dpla/code/ingestion3-conf/i3.conf`)
- **Orchestrator email drafts**: `logs/hub-emails-*/`
