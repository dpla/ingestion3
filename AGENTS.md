# DPLA Ingestion3 – Agent guide

Use this document when you are asked to **run ingests**, **report status**, or **handle errors** for the DPLA ingestion pipeline. This document provides shared reference (environment, notifications, error patterns). For ingest procedures, use the rules and skills linked in the Ingest workflows section.

---

## Environment and build

**Before any of these:** building the JAR, running `./scripts/*`, running the orchestrator, running Python tools, or running AWS scripts that need project env — you must load the project environment. This is the **canonical checklist** for Cursor and Claude agents; .cursorrules and CLAUDE.md point here for the full list.

- **Load env:** From repo root run `source .env` so `JAVA_HOME`, `SLACK_WEBHOOK`, `DPLA_DATA`, `I3_CONF`, etc. are set. Scripts that source `common.sh` (e.g. `harvest.sh`, `ingest.sh`) automatically load `$I3_HOME/.env` when present, so `SLACK_WEBHOOK` is available for harvest-failure notifications even if you did not run `source .env` in the shell first.
- **Build the fat JAR:** From repo root run `source .env` then `sbt assembly`. Java 11+ is required (the codebase uses `java.net.http`). If you run `sbt assembly` without sourcing `.env`, the build may use the wrong JDK and fail (e.g. "package java.net.http does not exist"). Pipeline scripts (harvest.sh, ingest.sh, etc.) automatically run `sbt assembly` when the JAR is missing or when any Scala source is newer than the JAR, so the "harvest [hub]" skill uses current code without a separate build step.
- **Java:** 11+ required for build and run. Set `JAVA_HOME` in `.env` (see Java requirement section below).
- **Python:** Use `./venv/bin/python` or `source ./venv/bin/activate`; do not use system Python.
- **AWS:** Use `--profile dpla` for all `aws` commands.

**Why this checklist lives in AGENTS.md:** The full list of "before X do Y" rules is kept in one place so Cursor and Claude agents (and humans) follow the same process. .cursorrules and CLAUDE.md point here for the canonical checklist.

---

## Ingest workflows

| Task | Where to go |
|------|-------------|
| Run a single-hub ingest | [.claude/rules/ingestion.md](.claude/rules/ingestion.md) / dpla-run-ingest skill |
| Run orchestrator (parallel, scheduled) | [.claude/rules/orchestrator.md](.claude/rules/orchestrator.md) |
| Verify and notify on failure | [.claude/rules/notifications.md](.claude/rules/notifications.md) |
| Runbook index | [runbooks/README.md](runbooks/README.md) |

> Rules and skills are synced from `docs/ai-context/`. Edit there; run `./scripts/ai-context/sync.sh`.

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

- **Scripts:** [scripts/communication/send-ingest-email.sh](scripts/communication/send-ingest-email.sh) sends the mapping summary to the hub’s contacts (from i3.conf). [scheduler/orchestrator/backlog_emails.py](scheduler/orchestrator/backlog_emails.py) CC’s tech@dp.la on backlog hub emails. Scripts do not send errors to Slack or tech@dp.la; when you run ingests manually via scripts and something fails, post the error to #tech-alerts or email tech@dp.la (and, if applicable, point to the failure report or logs).

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

## Project conventions

- **Environment:** Source `.env` before running the orchestrator or pipeline scripts so `JAVA_HOME`, `SLACK_WEBHOOK`, `DPLA_DATA`, and other vars are set (e.g. `source .env` from repo root).
- **Python:** Use the virtualenv at `./venv/` when writing or running Python scripts.
- **AWS CLI:** Use `--profile dpla` for all AWS commands.
- **Shell scripts:** Write POSIX-compliant bash; avoid OS-specific flags (see [scripts/common.sh](scripts/common.sh) for portable helpers).
- **New or changed scripts:** Document in [scripts/SCRIPTS.md](scripts/SCRIPTS.md) (Quick Reference and, if needed, Script Details). Create or update tests in `scripts/tests/` and run `./scripts/tests/test-scripts.sh` when changing scripts.

For a human-readable explanation of how this document is used and how an agent moves through it, see [docs/AGENTS-narrative.md](docs/AGENTS-narrative.md).

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
