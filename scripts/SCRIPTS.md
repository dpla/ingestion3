# DPLA Ingestion Scripts

Shell scripts for running the DPLA ingestion3 pipeline. All scripts are cross-platform compatible with macOS and Ubuntu Linux.

## Script locations

Scripts are grouped by purpose. Run from repo root (e.g. `./scripts/ingest.sh maryland` or `./scripts/harvest/nara-ingest.sh --month=202601`).

| Folder | Purpose | Scripts |
|--------|---------|---------|
| **scripts/** (root) | Core pipeline, batch, S3, monitoring | `ingest.sh`, `harvest.sh`, `remap.sh`, `mapping.sh`, `enrich.sh`, `jsonl.sh`, `auto-ingest.sh`, `batch-ingest.sh`, `s3-sync.sh`, `common.sh`, `ingest-watchdog.sh` |
| **scripts/communication/** | Schedule, email, Slack | `schedule.sh`, `send-ingest-email.sh`, `notify-harvest-failure.sh`, `send-harvest-failure-email.py` |
| **scripts/delete/** | Record removal | `delete-by-id.sh`, `delete-from-jsonl.sh`, `delete-from-jsonl.py` |
| **scripts/harvest/** | Harvest helpers, NARA, Community Webs, SI, VA | `nara-ingest.sh`, `community-webs-export.sh`, `community-webs-ingest.sh`, `community-webs-validate-jsonl.py`, `fix-si.sh`, `harvest-va.sh` |
| **scripts/status/** | Status and sync checks | `ingest-status.sh`, `check-jsonl-sync.sh`, `monitor-pipeline.sh`, `monitor-remap.sh`, `hub-info.sh`, `s3-latest.sh`, `staged-report.sh`, `oai-harvest-watch.sh`, `watch-oai-harvest.py`, `watch-oai-harvest-pages.py` |

## Quick Reference

| Script | Purpose | Usage |
|--------|---------|-------|
| `ingest.sh` | Full pipeline (harvest → map → enrich → jsonl → S3 sync) | `./scripts/ingest.sh <hub>` |
| `ingest-watchdog.sh` | Cron watchdog: detects ingests killed by SIGKILL and alerts Slack | `*/5 * * * * /home/ec2-user/ingestion3/scripts/ingest-watchdog.sh` (via crontab) |
| `harvest.sh` | Harvest records from OAI/API/file source | `./scripts/harvest.sh <hub>` |
| `remap.sh` | Re-run mapping → enrichment → jsonl | `./scripts/remap.sh <hub>` |
| `mapping.sh` | Transform harvested records to DPLA MAP | `./scripts/mapping.sh <hub>` |
| `enrich.sh` | Enrich/normalize DPLA MAP records | `./scripts/enrich.sh <hub>` |
| `jsonl.sh` | Export enriched records to JSON-L | `./scripts/jsonl.sh <hub>` |
| `auto-ingest.sh` | Automated monthly ingestion | `./scripts/auto-ingest.sh [--hub=<hub>]` |
| `batch-ingest.sh` | Run pipeline for multiple hubs | `./scripts/batch-ingest.sh <hub1> <hub2>...` |
| `harvest/nara-ingest.sh` | NARA delta ingest pipeline | `./scripts/harvest/nara-ingest.sh <nara-export.zip>` |
| `harvest/community-webs-export.sh` | Export Community Webs SQLite DB to JSONL/ZIP | `./scripts/harvest/community-webs-export.sh [--db=PATH]` |
| `harvest/community-webs-ingest.sh` | Community Webs ingest: export + harvest (+ optional full pipeline) | `./scripts/harvest/community-webs-ingest.sh [--full]` |
| `communication/schedule.sh` | Query hub ingest schedules | `./scripts/communication/schedule.sh [month\|hub]` |
| `s3-sync.sh` | Sync hub data to S3 | `./scripts/s3-sync.sh <hub> [subdir]` |
| `harvest/fix-si.sh` | Preprocess Smithsonian data | `./scripts/harvest/fix-si.sh <folder>` |
| `harvest/harvest-va.sh` | Download Digital Virginias repos | `./scripts/harvest/harvest-va.sh [output-dir]` |
| `status/check-jsonl-sync.sh` | Check JSONL sync status with S3 | `./scripts/status/check-jsonl-sync.sh` |
| `delete/delete-by-id.sh` | Delete records from Elasticsearch | `./scripts/delete/delete-by-id.sh <id>...` |
| `delete/delete-from-jsonl.sh` | Delete records from S3 JSONL files | `./scripts/delete/delete-from-jsonl.sh --hub <hub> <id>...` |
| `communication/send-ingest-email.sh` | Send ingest summary email | `./scripts/communication/send-ingest-email.sh [--yes] <hub>` |
| *scheduling_emails* (Python) | Monthly pre-scheduling email to hub contacts | `./venv/bin/python -m scheduler.orchestrator.scheduling_emails [--month=N] --dry-run \| --draft \| --send` |
| `status/ingest-status.sh` | Check ingest status (orchestrator or manual runs) | `./scripts/status/ingest-status.sh` |
| `communication/notify-harvest-failure.sh` | Send Slack and email (tech@dp.la) on harvest failure | `./scripts/communication/notify-harvest-failure.sh <hub> "<error>"` |
| `status/hub-info.sh` | Show hub config from i3.conf | `./scripts/status/hub-info.sh <hub>` |
| `status/s3-latest.sh` | Show latest S3 data for a hub (harvest/mapping/jsonl) | `./scripts/status/s3-latest.sh <hub>` |
| `status/staged-report.sh` | Report hubs with JSONL staged in S3 for a month | `./scripts/status/staged-report.sh [month]` |
| `status/oai-harvest-watch.sh` | Run OAI harvest and watch set-by-set progress + ETA | `./scripts/status/oai-harvest-watch.sh <hub>` |
| `status/monitor-remap.sh` | Poll mapping/enrichment/jsonl stage outputs during manual remap | `./scripts/status/monitor-remap.sh <hub>` |

## Environment Variables

All scripts use these environment variables (with sensible defaults):

| Variable | Description | Default |
|----------|-------------|---------|
| `I3_HOME` | Ingestion3 repository root | Auto-detected from script location |
| `DPLA_DATA` | Data output directory | `$HOME/dpla/data` |
| `I3_CONF` | Path to i3.conf configuration | `$HOME/dpla/code/ingestion3-conf/i3.conf` |
| `JAVA_HOME` | Java installation directory | Auto-detected |
| `SPARK_MASTER` | Spark execution mode (pipeline parallelism) | `local[4]` |
| `AWS_PROFILE` | AWS credentials profile | `dpla` |
| `I3_JAR` | Override path to ingestion3 fat JAR | `$I3_HOME/target/scala-2.13/ingestion3-assembly-0.0.1.jar` |

All scripts use the fat JAR via `run_entry` in common.sh. **The JAR is built automatically** when it is missing or when any Scala source under `src/main/scala` is newer than the JAR, so running `./scripts/harvest.sh indiana` (or ingest.sh, remap.sh, etc.) will run `sbt assembly` first when needed and then use the JAR. You can still build once manually to avoid a build delay on the first run:

```bash
cd "$I3_HOME" && sbt assembly
./scripts/remap.sh maryland
```

## Shared Configuration (common.sh)

All scripts source `common.sh` which provides:

- **Env file:** When `$I3_HOME/.env` exists, `common.sh` sources it during init, so `SLACK_WEBHOOK`, `JAVA_HOME`, and other vars are set for pipeline runs and harvest-failure notifications without needing to run `source .env` first.

- **Platform detection**: `$PLATFORM` is set to `macos` or `linux`
- **Portable utilities**: `sed_i`, `get_script_dir`, `get_common_dir`
- **Java setup**: `setup_java <memory>` auto-detects Java and sets up environment
- **Logging**: `log_info`, `log_warn`, `log_error`, `log_success`, `print_step`
- **Validation**: `require_command`, `require_file`, `require_dir`, `die`
- **Hub helpers**: `get_provider_name`, `get_hub_email`, `get_harvest_type`
- **Process management**: `kill_tree <pid>` — recursively kill a process and all its descendants (prevents orphan JVMs)
- **Entry runner**: `run_entry <EntryClass> [--arg=val ...]` — runs any Scala entry class via JAR; builds the JAR first if missing or if Scala sources are newer than the JAR
- **IngestRemap runner**: `run_ingest_remap <input> <output> <conf> <name>` — convenience wrapper for IngestRemap
- **Status writer**: `write_hub_status <hub> <status> [--error=msg]` — writes per-hub `.status` file for ingest-status.sh (used by ingest.sh, harvest.sh, remap.sh)
- **Data finder**: `find_latest_data <provider> <step>` — finds the most recent timestamped directory for a pipeline step

### Example: Using common.sh in a new script

```bash
#!/usr/bin/env bash
set -euo pipefail

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java (4g memory)
setup_java "4g" || die "Failed to setup Java"

# Use provided variables and functions
log_info "I3_HOME is: $I3_HOME"
log_info "Platform is: $PLATFORM"
```

## Script Details

### ingest-watchdog.sh - SIGKILL Watchdog

Runs every 5 minutes via cron. Detects ingests that were killed with SIGKILL
(e.g. cgroup eviction), which bypasses bash EXIT traps and leaves no failure
notification. When a dead ingest is found, sends a Slack alert to `#tech-alerts`
via `SLACK_BOT_TOKEN`.

**Install** (ec2-user crontab):
```
*/5 * * * * /home/ec2-user/ingestion3/scripts/ingest-watchdog.sh
```

**How it works:**
1. Scans `$I3_HOME/logs/status/*.status` for in-progress statuses (`harvesting`,
   `remapping`, `enriching`, `jsonl`, `syncing`).
2. Skips status files updated within the last 5 minutes (buffer for the gap
   between pipeline steps).
3. Skips hubs where an `ingest.sh <hub>` process is still running.
4. Alerts Slack and touches `$STATUS_DIR/<hub>.watchdog-alerted` (sentinel file).

**Deduplication via sentinel file:**
Once alerted, the sentinel file's mtime is compared to the status file's mtime on
every subsequent run. If the sentinel is newer, the alert is suppressed — the
death event has already been reported. When a new ingest starts, `write_hub_status`
updates the status file's mtime, making the sentinel stale and re-arming the
watchdog automatically.

**Required env vars** (loaded from `$I3_HOME/.env`):

| Variable | Description |
|----------|-------------|
| `SLACK_BOT_TOKEN` | Bot token for Slack `chat.postMessage` API |
| `SLACK_CHANNEL` | Channel ID to post to (default: `C02HEU2L3` = `#tech-alerts`) |

### ingest.sh - Full Pipeline

Runs the complete ingestion pipeline for a hub:

```bash
./scripts/ingest.sh maryland                  # Full pipeline
./scripts/ingest.sh maryland --skip-harvest   # Use existing harvest data
./scripts/ingest.sh maryland --harvest-only   # Only harvest
```

#### IP-restricted hubs (TAILSCALE_EXIT_NODE)

Some partners whitelist specific source IPs on their OAI endpoints. For those hubs, `ingest.sh` automatically routes harvest traffic through a Tailscale exit node that holds the whitelisted IP. The exit node is set before the harvest step and cleared (and tailscaled stopped) immediately after, so downstream steps (mapping, enrichment, S3 sync) use normal routing. On failure, the same cleanup runs best-effort via an EXIT trap — it attempts to clear the exit node and stop `tailscaled` even when the harvest fails.

The exit node is configured per-provider in a `case` block near the top of `ingest.sh`:

```bash
case "$PROVIDER" in
    njde) TAILSCALE_EXIT_NODE="100.82.233.38" ;;  # main-vpc; IP whitelisted by Rutgers
esac
```

**Current IP-restricted hubs:**

| Hub | Partner | Whitelisted IP | Tailscale exit node |
|-----|---------|---------------|---------------------|
| `getty` | J. Paul Getty Trust (ExLibris Primo API) | `100.82.233.38` | `main-vpc` |
| `njde` | Rutgers (NJ Digital Library) | `100.82.233.38` | `main-vpc` |

**Prerequisites** for IP-restricted hubs:
- Tailscale must be installed on the EC2 (`tailscale` command on PATH)
- The exit node machine (`main-vpc` at `100.82.233.38`) must have exit node routes advertised (`tailscale set --advertise-exit-node` on main-vpc) and approved in the Tailscale admin console (tailscale.com/admin → Machines → main-vpc → Edit route settings)
- `ec2-user` must have passwordless sudo for `systemctl` (standard on the ingest EC2)
- Tailscale authentication is per-machine (stored in `/var/lib/tailscale/` on the EC2 EBS volume) — no per-operator credentials needed

**Node key rotation (important — ~180 day maintenance task):**

Tailscale node keys expire approximately every 180 days. When the ingest EC2's key expires, the njde harvest will fail immediately with a clear error message:

```
Tailscale node key has expired on this EC2. Re-authenticate with:
  sudo tailscale up --auth-key=<key>
```

To fix: generate a reusable pre-auth key at tailscale.com/admin → Settings → Keys, then SSH or SSM into the EC2 and run:

```bash
sudo tailscale up --auth-key=<your-reusable-key>
```

This is a one-time re-authentication of the EC2 machine — not tied to any individual operator. After re-authenticating, re-run the njde ingest normally.

### auto-ingest.sh - Automated Monthly Ingestion

Processes hubs scheduled for the current month:

```bash
./scripts/auto-ingest.sh                    # All scheduled hubs
./scripts/auto-ingest.sh --hub=maryland     # Single hub
./scripts/auto-ingest.sh --month=3          # March hubs
./scripts/auto-ingest.sh --dry-run          # Preview only
./scripts/auto-ingest.sh --skip-harvest     # Skip harvest step
./scripts/auto-ingest.sh --no-email         # Skip email notifications
./scripts/auto-ingest.sh --no-s3-sync       # Skip S3 sync
```

### nara-ingest.sh - NARA Delta Ingest

Handles NARA's large dataset with delta updates. Located in `scripts/harvest/`:

```bash
./scripts/harvest/nara-ingest.sh --month=202602
./scripts/harvest/nara-ingest.sh --month=202602 --skip-preprocess  # delta dir already exists
./scripts/harvest/nara-ingest.sh --month=202602 --force-sync       # re-download base from S3
./scripts/harvest/nara-ingest.sh --skip-to-pipeline --harvest=/path/to/merged.avro
./scripts/harvest/nara-ingest.sh --month=202512,202602             # multi-month
```

`--force-sync` downloads only the **single latest** merged base harvest from S3
(~34 GB) rather than the full historical prefix. Only the most recent base is
needed since each merged harvest is a complete snapshot of all NARA records.

### schedule.sh - Query Schedules

Query hub ingest schedules from i3.conf. Located in `scripts/communication/`:

```bash
./scripts/communication/schedule.sh              # Full year schedule
./scripts/communication/schedule.sh feb          # February hubs
./scripts/communication/schedule.sh 2            # Month 2 hubs
./scripts/communication/schedule.sh virginias   # Single hub schedule
```

### fix-si.sh - Smithsonian Preprocessing

Preprocess Smithsonian XML files before harvest. Located in `scripts/harvest/`:

```bash
./scripts/harvest/fix-si.sh --list        # List available folders
./scripts/harvest/fix-si.sh 20260201      # Process specific folder
```

### community-webs-export.sh - Community Webs DB Export

Export Community Webs SQLite database to JSONL and ZIP for harvest. Internet Archive sends a SQLite database; this script runs the intermediate step (sqlite3 + jq + zip) automatically.

**Prerequisites:** `sqlite3`, `jq` (brew install sqlite3 jq / apt install sqlite3 jq)

```bash
./scripts/harvest/community-webs-export.sh                        # Auto-detect latest *.db
./scripts/harvest/community-webs-export.sh --db=/path/to/file.db  # Explicit DB path
./scripts/harvest/community-webs-export.sh --update-conf          # Also update i3.conf endpoint
./scripts/harvest/community-webs-export.sh --skip-validate        # Skip JSONL schema validation (not recommended)
```

Output: `$DPLA_DATA/community-webs/originalRecords/<YYYYMMDD>/community-webs-<timestamp>.zip`

The script validates the JSONL against the expected harvest schema (required `id` field) before zipping. Use `community-webs-ingest.sh` for the full flow.

### community-webs-ingest.sh - Community Webs Ingest

Orchestrates export + harvest + (optional) full pipeline. Located in `scripts/harvest/`:

```bash
./scripts/harvest/community-webs-ingest.sh              # Export + harvest only
./scripts/harvest/community-webs-ingest.sh --full       # Export + harvest + mapping + enrichment + jsonl
./scripts/harvest/community-webs-ingest.sh --skip-export  # Harvest only (endpoint must already point to ZIP)
./scripts/harvest/community-webs-ingest.sh --db=/path/to/db --update-conf
```

### delete-by-id.sh - Elasticsearch Delete

Delete records from Elasticsearch by DPLA ID. Located in `scripts/delete/`:

```bash
./scripts/delete/delete-by-id.sh <id1> <id2> ...
./scripts/delete/delete-by-id.sh -f ids-to-delete.txt
cat ids.txt | ./scripts/delete/delete-by-id.sh -f -

# Preview without deleting
DRY_RUN=true ./scripts/delete/delete-by-id.sh -f ids.txt
```

### delete-from-jsonl.sh - S3 JSONL Delete

Delete records from JSONL files in S3. Located in `scripts/delete/`:

```bash
./scripts/delete/delete-from-jsonl.sh --hub cdl -f ids-to-delete.txt
./scripts/delete/delete-from-jsonl.sh --hub cdl <id1> <id2> ...

# Preview without modifying
DRY_RUN=true ./scripts/delete/delete-from-jsonl.sh --hub cdl -f ids.txt
```

> **Note**: For better performance, use `delete-from-jsonl.py` instead.

## Testing

Run the test suite to verify scripts work correctly:

```bash
./scripts/tests/test-scripts.sh           # Full test suite
./scripts/tests/test-scripts.sh --quick   # Syntax checks only
./scripts/tests/test-scripts.sh --verbose # Show detailed output
```

Tests verify:
- All scripts have valid bash syntax
- All scripts source common.sh correctly
- Environment variable defaults work
- Cross-platform functions work (sed_i, get_script_dir)
- Help/usage outputs work
- No hardcoded `/Users/scott` paths remain

## Platform Notes

### macOS vs Ubuntu Differences

| Feature | macOS | Ubuntu | Solution |
|---------|-------|--------|----------|
| `sed -i` | `sed -i '' ...` | `sed -i ...` | Use `sed_i` function |
| `readlink -f` | Not available | Works | Use `get_script_dir` function |
| Java location | `/Library/Java/...` | `/usr/lib/jvm/...` | Use `setup_java` function |

### Java Detection

The `setup_java` function automatically detects Java:

- **macOS**: Uses `/usr/libexec/java_home` or searches common locations
- **Ubuntu**: Searches `/usr/lib/jvm/`, `/opt/java/`, or uses `which java`

Override by setting `JAVA_HOME` before running scripts.

## Python Orchestrator (Parallel Ingests)

For parallel hub processing, use the Python orchestrator instead of `auto-ingest.sh`:

```bash
# Activate venv
source ./venv/bin/activate

# Current month, sequential (default)
python -m scheduler.orchestrator.main

# Run 3 hubs concurrently
python -m scheduler.orchestrator.main --parallel=3

# Specific hubs
python -m scheduler.orchestrator.main --hub=mi,va,mn --parallel=3

# Preview execution plan
python -m scheduler.orchestrator.main --dry-run --parallel=3

# Retry failures from last run
python -m scheduler.orchestrator.main --retry-failed

# Check status
python -m scheduler.orchestrator.main --status
```

Resource budgeting is automatic: `--parallel=2` gives each hub `local[2]` and ~4g heap, `--parallel=3` gives `local[2]` and ~4g heap each. Per-hub status files are written to `logs/status/<hub>.status`.

### send-ingest-email.sh - Send Summary Emails

Sends ingest summary emails to hub contacts on demand. Useful for re-sending emails or sending summaries after manual ingests.

**Purpose:**
- Reads hub email from i3.conf (e.g., `nara.email = "contact@nara.gov"`)
- Finds the most recent mapping output directory
- Sends an email with:
  - Mapping summary (attempted, successful, failed counts)
  - Error/warning details from `_SUMMARY`
  - Pre-signed S3 links to full logs (7-day expiration)

**Usage:** (script in `scripts/communication/`)
```bash
# Latest mapping for a hub
./scripts/communication/send-ingest-email.sh maryland

# Skip confirmation prompt (for automation)
./scripts/communication/send-ingest-email.sh --yes nara

# Specific mapping directory
./scripts/communication/send-ingest-email.sh maryland /path/to/mapping/20260201_120000-maryland-MAP.avro

# Bulk send (with --yes to avoid multiple prompts)
./scripts/communication/send-ingest-email.sh --yes wisconsin
./scripts/communication/send-ingest-email.sh --yes p2p
./scripts/communication/send-ingest-email.sh --yes maryland
```

**Options:**
- `--yes` or `-y`: Skip the confirmation prompt and send immediately

**Requirements:**
- Hub must have email configured in i3.conf:
  ```hocon
  hub.email = "contact@example.com"
  # Multiple recipients supported:
  hub.email = "person1@example.com,person2@example.com"
  ```
- Mapping output with `_SUMMARY` file must exist
- Java 11+ and sbt must be available

**Email Content:**
- Subject: `DPLA Ingest Summary for [Provider] - [Month Year]`
- Body includes:
  - Full `_SUMMARY` content (harvest count, mapping stats, error breakdown)
  - Pre-signed S3 links to:
    - `_SUMMARY` file
    - Error logs ZIP (if there are failed records)

**Integration:**
- Orchestrator creates **email drafts** at `logs/hub-emails-<run_id>/` after runs
- Use this script to manually send emails from those drafts
- See [.claude/skills/send-email/](../.claude/skills/send-email/) for Claude Code skill

**Related:**
- Scala Emailer: `src/main/scala/dpla/ingestion3/utils/Emailer.scala`
- Orchestrator email drafts: `scheduler/orchestrator/notifications.py` (line 790+)

### Scheduling emails (monthly pre-scheduling notification)

One summary email to all contacts of hubs scheduled for a given month. Sent at the start of the month to notify hubs that ingests will run during the **last calendar week** of that month. Uses **i3.conf** for schedule (`<hub>.schedule.months`) and contacts (`<hub>.email`). CC on every send: ingest@dp.la, dominic@dp.la.

**Entry point:** `./venv/bin/python -m scheduler.orchestrator.scheduling_emails`

**Usage:**
```bash
# Preview: date range, hubs list, To/CC, and full body (no file, no send)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --dry-run

# Write draft to scheduler/emails/scheduling-YYYY-MM.txt
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --draft

# Send via SES (after preview)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --send
```

**Options:**
- `--month=N` — Target month 1–12 (default: current month).
- `--dry-run` — Print preview (date range, hubs included, hubs with no email noted) and full email; no draft or send.
- `--draft` — Print preview and write a single draft file to `scheduler/emails/scheduling-YYYY-MM.txt`.
- `--send` — Print preview and send via AWS SES. Requires boto3 and `--aws-profile dpla` (or `AWS_PROFILE`).
- `--i3-conf PATH` — Path to i3.conf (default: from env `I3_CONF`).
- `--aws-profile NAME` — AWS profile for SES (default: dpla).

**Preview:** Every run (dry-run, draft, send) prints a **Preview** block with the date range (e.g. "February 23 – February 28, 2026") and the list of hubs included; hubs without email in i3.conf are marked "(no email in i3.conf – add manually if needed)".

**Skill:** See [.cursor/skills/dpla-monthly-emails/SKILL.md](../.cursor/skills/dpla-monthly-emails/SKILL.md). The agent should always show the preview (hubs + date range) before sending.

### notify-harvest-failure.sh and send-harvest-failure-email.py - Harvest Failure Alerts

Invoked by `HarvestExecutor` when an OAI (or other) harvest fails. Sends **both** Slack and email so tech is notified even if one channel is unavailable.

**Email body is built in Scala:** `OaiHarvestException.buildEmailBody` (in `OaiResponse.scala`) produces the complete email body (hub name, human-readable error details, footer). `HarvestExecutor` passes this body as a third argument to `notify-harvest-failure.sh`, which forwards it to `send-harvest-failure-email.py`. The Python script sends the body as-is — no template wrapping. This makes Scala the single source of truth for notification content.

**Slack:** If `SLACK_WEBHOOK` is set, posts to #tech-alerts with hub name and error snippet (from `OaiHarvestException.getMessage`). Optional `SLACK_ALERT_USER_ID` adds an @mention.

**Email:** Always attempts to send to **tech@dp.la** via AWS SES using `scripts/communication/send-harvest-failure-email.py` (requires project venv and boto3; uses `AWS_PROFILE`, default `dpla`). Best-effort: if email fails (e.g. no credentials), a warning is printed and the script still exits 0.

**Usage:** Normally called by the JVM on harvest failure; can be run manually (script in `scripts/communication/`):
```bash
# With Scala-provided email body (3 args)
./scripts/communication/notify-harvest-failure.sh indiana "OAI Error: badArgument ..." "DPLA OAI Harvest Failure..."

# Without email body (2 args, backward-compat: Python wraps error in default template)
./scripts/communication/notify-harvest-failure.sh indiana "OAI Error: badArgument ..."
```

**Environment:** `SLACK_WEBHOOK`, `SLACK_ALERT_USER_ID`, `AWS_PROFILE` (for email), `I3_HOME` (default: derived from script path).

## Workflow Diagrams

### Standard Ingest Pipeline

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Harvest │ -> │ Mapping │ -> │ Enrich  │ -> │  JSONL  │
└─────────┘    └─────────┘    └─────────┘    └─────────┘
     │              │              │              │
     v              v              v              v
  harvest/       mapping/     enrichment/      jsonl/
```

### Script Relationships

```
auto-ingest.sh
     │
     ├── harvest.sh (or S3 download for file hubs)
     │
     └── remap.sh
           │
           └── IngestRemap (mapping + enrichment + jsonl)

ingest.sh
     │
     ├── HarvestEntry
     │
     └── IngestRemap

batch-ingest.sh
     │
     └── ingest.sh (for each hub)
```

## Updating This Documentation

When adding or modifying scripts:

1. **Document:** Add any new script to the Quick Reference table and, if non-trivial, to Script Details (purpose, usage, and env vars if relevant). Update existing entries when behavior changes.
2. **Tests:** For new or changed scripts, add or update tests in `scripts/tests/test-scripts.sh` (or under `scripts/tests/` as appropriate). Run the suite before committing:
   - `./scripts/tests/test-scripts.sh` (full), or `./scripts/tests/test-scripts.sh --quick` for syntax/quick checks.
3. Update environment variables in this doc if new ones are added.
4. Test on both macOS and Ubuntu if possible.
