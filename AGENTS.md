# DPLA Ingestion3 – Agent guide

Use this document when you are asked to **run ingests**, **report status**, or **handle errors** for the DPLA ingestion pipeline. It ties together runbooks, scripts, and notification policy.

---

## Running an ingest

1. **Start here:** Read [runbooks/README.md](runbooks/README.md) to see the runbook index and where hub configuration lives. If the runbooks directory is not yet present, use [scripts/SCRIPTS.md](scripts/SCRIPTS.md) and the harvest type mapping below.

2. **Identify the hub** (e.g. from the user request or schedule).

3. **Determine harvest type** from i3.conf (`$I3_CONF`, default `~/dpla/code/ingestion3-conf/i3.conf`): look up `<hub>.harvest.type` for that hub. Values: `localoai`, `api`, `file`, `nara.file.delta`.

4. **Follow the right runbook:**
   - `localoai` → [runbooks/05-standard-oai-ingests.md](runbooks/05-standard-oai-ingests.md)
   - `api` → [runbooks/06-standard-api-ingests.md](runbooks/06-standard-api-ingests.md)
   - `file` → [runbooks/04-file-based-imports.md](runbooks/04-file-based-imports.md); for Smithsonian or NARA use their dedicated runbooks.
   - `nara.file.delta` → [runbooks/02-nara.md](runbooks/02-nara.md)
   - Smithsonian (file) → [runbooks/03-smithsonian.md](runbooks/03-smithsonian.md)

5. **Run the scripts** indicated in that runbook (e.g. `./scripts/ingest.sh <hub>`, `./scripts/remap.sh <hub>`, or `./scripts/nara-ingest.sh` for NARA). See [scripts/SCRIPTS.md](scripts/SCRIPTS.md) for script options and usage.

6. **Verify** outputs (e.g. `_SUCCESS` markers, record counts) and run `./scripts/s3-sync.sh <hub>` when the runbook says so.

Do not run the standard ingest pipeline for NARA or Smithsonian without following their dedicated runbooks (NARA uses a delta merge workflow; Smithsonian requires preprocessing via `fix-si.sh`).

---

## Notifications and errors (Slack or email tech@dp.la)

**Policy:** All errors and important status updates must be reported to **Slack** or **email tech@dp.la** (or both). Prefer Slack when a webhook is configured; otherwise ensure tech@dp.la is notified.

**Treat as errors (must notify):**
- Feed or endpoint unreachable (e.g. OAI/API timeout, connection failure).
- Harvest failure (script or orchestrator harvest step failed).
- Mapping/remap or enrichment failure.
- S3 sync failure.
- Anomaly detection halting sync (e.g. critical record-count or failure-rate change).
- Any pipeline step that exits non-zero or is reported as failed by the orchestrator.

**Treat as status (should notify):**
- Ingest run started (which hubs, run id).
- Ingest run completed (per-hub success/failure counts, draft emails path if applicable).
- Anomaly warnings (sync proceeded but unexpected changes).
- Failures requiring attention (list of failed hubs, link or path to failure report).

### Post stage failures to #tech-alerts

For any **pipeline stage failure**, ensure a notification is posted to **Slack #tech-alerts** (or, if Slack is unavailable, email tech@dp.la with the same information).

**Stage-specific failures to report:**

| Stage | What to report |
|-------|----------------|
| **Harvest** | Feed unreachable, timeout, or harvest script/orchestrator step failed. |
| **Mapping/remap** | Mapping or IngestRemap step failed (e.g. non-zero exit, no output). |
| **Sync** | S3 sync failed or was blocked. |
| **Anomaly** | Anomaly detection halted sync (critical threshold); optionally mention warning-level anomalies in #tech-alerts. |

**How it works:** When using the orchestrator (`python -m scheduler.orchestrator.main`), [scheduler/orchestrator/notifications.py](scheduler/orchestrator/notifications.py) sends a Slack failure alert for the run (run id, failed hubs, report path). Configure the Slack webhook so that message posts to **#tech-alerts**. When running scripts manually, post a short message to #tech-alerts (or email tech@dp.la) with: hub name, stage that failed, and a one-line error or path to logs/report.

---

## Where notifications are implemented

- **Orchestrator** ([scheduler/orchestrator/](scheduler/orchestrator/)): When running the automated pipeline (`python -m scheduler.orchestrator.main`), it uses `notifications.py` to send:
  - **Slack:** Run start, per-stage progress (harvest/mapping/enrichment/jsonl/sync complete per hub), anomaly alerts, run completion (per-hub status), failure escalation (failed hubs + path to report). Requires `SLACK_WEBHOOK` in the environment (or in `.env`). The webhook should target **#tech-alerts** so all automated notifications land there.
  - If Slack is not configured, the orchestrator still writes escalation files under `data/escalations/`. In that case, email tech@dp.la with the failure summary or attach the failure report.

- **Scripts:** [scripts/send-ingest-email.sh](scripts/send-ingest-email.sh) sends the mapping summary to the hub’s contacts (from i3.conf). [scheduler/orchestrator/backlog_emails.py](scheduler/orchestrator/backlog_emails.py) CC’s tech@dp.la on backlog hub emails. Scripts do not send errors to Slack or tech@dp.la; when you run ingests manually via scripts and something fails, post the error to #tech-alerts or email tech@dp.la (and, if applicable, point to the failure report or logs).

**If you run an ingest and it fails:** (1) Classify the failure (harvest, mapping, sync, anomaly). (2) Post to #tech-alerts (if `SLACK_WEBHOOK` is set) or send an email to tech@dp.la with a short summary and, if available, the path to the failure report (e.g. `data/escalations/failures-<run_id>.md`) or the relevant log snippet.

---

## Status and reporting summary

The orchestrator sends per-stage Slack notifications as each hub progresses through the pipeline:

| Event | Slack (#tech-alerts) | Email tech@dp.la | Notes |
|-------|----------------------|-------------------|------|
| Run started | Yes | Optional | Run id, hubs list |
| Harvest complete | Yes | — | Hub name, record count, duration |
| Mapping complete | Yes | — | Hub name, attempted/successful/failed counts |
| Enrichment complete | Yes | — | Hub name, duration |
| JSONL export complete | Yes | — | Hub name, duration |
| Data synced to S3 | Yes | — | Hub name, duration |
| Anomaly (warning) | Yes | Optional | Sync proceeded |
| Anomaly (critical) | Yes | Recommended | Sync halted |
| Run completed | Yes | Optional | Per-hub status, draft emails path |
| Stage failure | Yes | Required if no Slack | Failed hubs, stage, report path |

Ensure every failure is reflected in #tech-alerts or in an email to tech@dp.la.

---

## Agent checklist for ingest runs

**Before running:**
- Confirm hub and harvest type; open the correct runbook (or SCRIPTS.md if runbooks are not yet available).
- If using the orchestrator, ensure `SLACK_WEBHOOK` is set (or plan to email tech@dp.la on failure).

**After a run:**
- If any hub failed: post failure summary to #tech-alerts or email tech@dp.la; include stage and reference escalation report if present.
- If the run completed: completion notification is sent by the orchestrator when applicable; if you ran only scripts, consider notifying status to #tech-alerts or tech@dp.la if that’s standard for your workflow.

**References:**
- Runbooks: [runbooks/README.md](runbooks/README.md)
- Scripts: [scripts/SCRIPTS.md](scripts/SCRIPTS.md)
- Config: i3.conf at `$I3_CONF` (default `~/dpla/code/ingestion3-conf/i3.conf`)
- Debug ingest failures: [.cursor/skills/dpla-ingest-debug/SKILL.md](.cursor/skills/dpla-ingest-debug/SKILL.md)

For a human-readable explanation of how this document is used and how an agent moves through it, see [docs/AGENTS-narrative.md](docs/AGENTS-narrative.md).

---

## Critical: OutputHelper path convention

**All `--output` arguments to Scala entry points must be `$DPLA_DATA` (the data root)**, never `$DPLA_DATA/$PROVIDER`.

`OutputHelper.scala` constructs paths as: `rootPath / shortName / activity / timestamp-shortName-schema`

So `--output=$DPLA_DATA` produces `$DPLA_DATA/nara/jsonl/20260210_103600-nara-MAP3_1.IndexRecord.jsonl` (correct). But `--output=$DPLA_DATA/nara` would produce `$DPLA_DATA/nara/nara/jsonl/...` (double-nested, WRONG).

This applies to all entry points: HarvestEntry, MappingEntry, EnrichEntry, JsonlEntry, IngestRemap.

---

## Java requirement

**Java 11+ is required** (the codebase uses `java.net.http.HttpClient` which was introduced in Java 11). Java 19 is known to work. Set `JAVA_HOME` in `.env` (or export it) so scripts and the orchestrator use the correct JDK:

```bash
# .env (gitignored)
JAVA_HOME=/Users/scott/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home
```

The `common.sh` `run_entry()` function conditionally adds `--add-opens` flags only when running Java 9+ (Java 8 does not recognise them and will fail). If you see `Unrecognized option: --add-opens`, your `JAVA_HOME` likely points to Java 8.

---

## JAR vs sbt execution

All scripts use `run_entry()` from `common.sh`, which automatically uses the fat JAR when it exists:

- **JAR** (`target/scala-2.13/ingestion3-assembly-0.0.1.jar`): ~15s faster startup, no orphan JVM problem, no sbt memory overhead, no project lock conflicts (enables parallel ingests). Build with `sbt assembly`.
- **sbt fallback**: Used when no JAR exists. Slower startup, forks a child JVM (can become orphan if parent is killed).

The JAR is preferred for all usage. Rebuild after code changes with `sbt assembly`.

**Important:** The `build.sbt` assembly merge strategy must discard `.SF`/`.DSA`/`.RSA` signature files from signed dependency JARs. Without this, the fat JAR will fail at runtime with `SecurityException: Invalid signature file digest` (Java 11+) or the misleading `Could not find or load main class` (Java 8). The current `build.sbt` already handles this.

---

## Verifying pipeline output

Each pipeline step writes a `_SUCCESS` marker file when complete. Check for it:

```bash
# Verify a step completed
ls $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_SUCCESS

# The _MANIFEST file contains record counts
cat $DPLA_DATA/<hub>/jsonl/<timestamped-dir>/_MANIFEST
```

Incomplete runs (directories with `_temporary` but no `_SUCCESS`) should be deleted before retrying.

---

## Resuming failed steps

If a pipeline fails partway through:

1. Check which steps completed (look for `_SUCCESS` files in `harvest/`, `mapping/`, `enrichment/`, `jsonl/`).
2. Re-run only the failed step and later steps. Example: if mapping succeeded but enrichment failed, run `./scripts/enrich.sh <hub>` then `./scripts/jsonl.sh <hub>`.
3. For the full pipeline (IngestRemap), it must be re-run from scratch since it does mapping+enrichment+jsonl in one Spark application.

---

## Parallel ingest execution

Use the Python orchestrator for parallel hub processing:

```bash
# Run 3 hubs concurrently
./venv/bin/python -m scheduler.orchestrator.main --parallel=3

# Specific hubs in parallel
./venv/bin/python -m scheduler.orchestrator.main --hub=mi,va,mn --parallel=3

# Preview without running
./venv/bin/python -m scheduler.orchestrator.main --dry-run --parallel=3
```

Resource budgeting (automatic):
- `--parallel=1`: local[4], 8g heap (default)
- `--parallel=2`: local[2-3], 4-6g heap each
- `--parallel=3`: local[2], 4g heap each

Per-hub status files: `logs/status/<hub>.status` (JSON, updated in real-time).

---

## Common error patterns and fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `Unrecognized option: --add-opens` | `JAVA_HOME` points to Java 8; scripts need Java 11+ | Set `JAVA_HOME` to a Java 11+ JDK in `.env` |
| `Could not find or load main class` | Fat JAR contains signed-dependency signature files | Rebuild with `sbt assembly` (build.sbt discards `.SF`/`.DSA`/`.RSA` files) |
| `SecurityException: Invalid signature file digest` | Same as above (Java 11+ gives a clearer message) | Same fix: rebuild with `sbt assembly` |
| `IOException: Failed to delete` with double-nested path | `--output` was `$DPLA_DATA/$PROVIDER` instead of `$DPLA_DATA` | Fix the output arg; delete the double-nested directory |
| Orphan Java processes after killing a script | sbt forks a child JVM; killing the parent leaves the child running | Use `kill_tree()` (built into all scripts via common.sh) or `pkill -f 'java.*ingestion'` |
| `_temporary` directories but no `_SUCCESS` | Incomplete Spark write (crash or kill) | Delete the incomplete timestamped directory and retry |
| sbt project lock conflict | Two sbt processes for the same project | Use the fat JAR (no sbt lock needed), or wait for the first to finish |
| Duplicate log lines | sbt writes to both stdout and stderr | Scripts pipe through dedup; check for multiple processes writing to same log |
