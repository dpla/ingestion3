# DPLA Ingestion — Golden Path

How to run hub ingests end-to-end, what notifications to expect, and how to monitor running jobs.

---

## Prerequisites

### 1. Java 11+

The ingestion pipeline requires Java 11 or later (`java.net.http.HttpClient`). Java 19 is the tested/recommended version.

```bash
# macOS: list available JDKs
/usr/libexec/java_home -V

# Verify
java -version
# → openjdk version "19.0.2" or similar (11+)
```

### 2. Environment file

```bash
cp .env.example .env
```

Edit `.env` and set:

```bash
# Slack webhook for #tech-alerts notifications
SLACK_WEBHOOK=https://hooks.slack.com/services/...

# Java 11+ required. Java 19 is recommended.
JAVA_HOME=/path/to/java-11-or-newer
```

### 3. Build the fat JAR

The pipeline runs via a self-contained JAR. Build it once (rebuild after any Scala code changes):

```bash
sbt assembly
# → target/scala-2.13/ingestion3-assembly-0.0.1.jar (~570MB)
```

### 4. Hub configuration

Hub definitions live in `i3.conf` (default: `~/dpla/code/ingestion3-conf/i3.conf`). Each hub has a harvest type (`localoai`, `api`, `file`, `nara.file.delta`), endpoint, and schedule.

---

## Running Ingests

### Standard: orchestrator (recommended)

The Python orchestrator manages the full pipeline — harvest, mapping, enrichment, JSONL export, anomaly detection, and S3 sync — with Slack notifications at each stage.

```bash
cd ~/dpla/code/ingestion3
source .env

# Run specific hubs
python3 -m scheduler.orchestrator.main --hub=wisconsin,p2p

# Run specific hubs in parallel (recommended: 2-3)
python3 -m scheduler.orchestrator.main --hub=wisconsin,p2p --parallel=2

# Run all hubs scheduled for the current month
python3 -m scheduler.orchestrator.main

# Run all hubs for a specific month
python3 -m scheduler.orchestrator.main --month=3

# Preview what would run (no changes)
python3 -m scheduler.orchestrator.main --dry-run

# Retry failed hubs from the last run
python3 -m scheduler.orchestrator.main --retry-failed

# Skip harvest (re-process existing harvested data)
python3 -m scheduler.orchestrator.main --hub=wisconsin --skip-harvest

# Skip S3 sync (process but don't upload)
python3 -m scheduler.orchestrator.main --hub=wisconsin --skip-s3-sync
```

### Long-running / unattended

Harvests can take 12–24 hours. Use one of these to survive terminal disconnection:

```bash
# tmux (recommended — can reattach later)
tmux new -s ingest
cd ~/dpla/code/ingestion3 && source .env
python3 -m scheduler.orchestrator.main --hub=wisconsin,p2p --parallel=2
# Ctrl-B, D to detach — tmux attach -t ingest to reattach

# nohup (fire and forget)
cd ~/dpla/code/ingestion3 && source .env
nohup python3 -m scheduler.orchestrator.main --hub=wisconsin,p2p --parallel=2 \
  > logs/orchestrator-$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

### Manual: individual scripts

For running a single pipeline step or debugging:

```bash
cd ~/dpla/code/ingestion3

./scripts/harvest.sh <hub>       # Harvest only
./scripts/mapping.sh <hub>       # Mapping only
./scripts/enrich.sh <hub>        # Enrichment only
./scripts/jsonl.sh <hub>         # JSONL export only
./scripts/remap.sh <hub>         # Mapping + enrichment + JSONL (combined)
./scripts/ingest.sh <hub>        # Full pipeline (harvest + remap)
./scripts/s3-sync.sh <hub>       # Sync to S3
```

> When running scripts manually and something fails, post the error to **#tech-alerts** in Slack or email **tech@dp.la**.

---

## Pipeline Stages

The orchestrator runs each hub through six stages in order:

| # | Stage | Script | What it does |
|---|-------|--------|--------------|
| 1 | Prepare | — | Download S3 data for file-based hubs (OAI/API hubs skip this) |
| 2 | Harvest | `harvest.sh` | Fetch records from OAI endpoint, API, or file source |
| 3 | Mapping | `mapping.sh` | Transform harvested records into DPLA MAP format |
| 4 | Enrichment | `enrich.sh` | Normalize and enrich DPLA MAP records |
| 5 | JSONL Export | `jsonl.sh` | Export enriched records to gzipped JSON Lines |
| 6 | S3 Sync | `s3-sync.sh` | Upload JSONL to S3 (with anomaly detection gate) |

If a stage fails, the orchestrator stops that hub, records the failure, and continues with the next hub (when running in parallel).

---

## Slack Notifications

All notifications are posted to **#tech-alerts** via the `SLACK_WEBHOOK` configured in `.env`.

### Per-stage notifications

| When | Slack message | Details included |
|------|--------------|------------------|
| Run started | :rocket: DPLA Ingest Started | Run ID, list of hubs |
| Harvest complete | :seedling: `hub` harvest complete | Record count, duration |
| Mapping complete | :world_map: `hub` mapping complete | Attempted, successful, failed counts, duration |
| Enrichment complete | :sparkles: `hub` enrichment complete | Duration |
| JSONL export complete | :package: `hub` JSONL export complete | Duration |
| S3 sync complete | :cloud: `hub` data synced to S3 | Duration |
| Run complete | :white_check_mark: or :warning: DPLA Ingest Complete | Per-hub summary, totals |

### Error and anomaly notifications

| When | Slack message | Details included |
|------|--------------|------------------|
| Stage failure | :x: Posted in #tech-alerts | Hub, stage, error message |
| Anomaly warning | :warning: Anomaly alert | Record count changes, threshold exceeded |
| Anomaly critical (sync halted) | :octagonal_sign: Sync blocked | What changed, why sync was halted |
| Run failures | Escalation report | List of failed hubs, failure stages, report path |

Escalation reports are also written to `~/dpla/data/escalations/failures-<run_id>.md`.

### Test notifications

Send test messages with a `[TEST]` prefix to verify Slack is configured:

```bash
python3 -m scheduler.orchestrator.main --dry-run-notify
```

---

## Monitoring Running Jobs

### Status command (instant, file-based)

The orchestrator writes per-hub status to `logs/status/<hub>.status` (JSON) in real time. The status reader is instant — no subprocess calls.

```bash
# Table view
./scripts/ingest-status.sh

# Auto-refreshing dashboard (every 30s)
./scripts/ingest-status.sh --watch

# Auto-refresh with custom interval
./scripts/ingest-status.sh --watch 10

# Specific hubs
./scripts/ingest-status.sh wisconsin p2p

# Verbose (includes stage history with per-stage durations)
./scripts/ingest-status.sh -v

# JSON output (for scripting)
./scripts/ingest-status.sh --json
```

Example output:

```
════════════════════════════════════════════════════════════════════════
  INGEST STATUS — 2026-02-10 16:30:00
════════════════════════════════════════════════════════════════════════

  🌾  wisconsin
  ────────────────────────────────────────
     Stage:          Harvesting  (2/6)
     Stage started:  08:15:00
     Time in stage:  8h 15m
     Total elapsed:  8h 15m
     Records:        142,000

  🗺️  p2p
  ────────────────────────────────────────
     Stage:          Mapping  (3/6)
     Stage started:  16:20:00
     Time in stage:  10m 00s
     Total elapsed:  2h 45m

════════════════════════════════════════════════════════════════════════
```

### Status fields

| Field | Meaning |
|-------|---------|
| Stage | Current pipeline stage (Harvesting, Mapping, etc.) |
| (N/6) | Stage progress — which stage out of 6 |
| Stage started | Clock time the current stage began |
| Time in stage | Wall-clock time spent in the current stage (computed live) |
| Total elapsed | Wall-clock time since the hub started processing |
| Records | Harvest record count (when available) |

### Raw status files

Each hub's status is a self-contained JSON file at `logs/status/<hub>.status`:

```bash
cat logs/status/wisconsin.status | python3 -m json.tool
```

Fields include: `status`, `stage_started_at`, `time_in_stage_seconds`, `total_elapsed_seconds`, `stage_index`, `total_stages`, `stage_history`, `harvest_records`, `error`, `failure_stage`.

### Health checks

The orchestrator logs periodic health-check lines for long-running stages (every 5 minutes by default), showing elapsed time, child JVM PID, and memory usage. These appear in the orchestrator's stdout/log. No process is automatically killed — long harvests (12–24h) are expected.

---

## Verifying Output

Each pipeline step writes a `_SUCCESS` marker file when complete:

```bash
# Check a step completed
ls ~/dpla/data/<hub>/harvest/<timestamped-dir>/_SUCCESS
ls ~/dpla/data/<hub>/mapping/<timestamped-dir>/_SUCCESS

# Record counts
cat ~/dpla/data/<hub>/harvest/<timestamped-dir>/_MANIFEST
cat ~/dpla/data/<hub>/mapping/<timestamped-dir>/_SUMMARY
```

Incomplete runs (directories with `_temporary` but no `_SUCCESS`) should be deleted before retrying.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Unrecognized option: --add-opens` | `JAVA_HOME` is pointing to Java 8. Set it to Java 11+ in `.env` |
| `Could not find or load main class` | Rebuild JAR: `sbt assembly` |
| `SecurityException: Invalid signature` | Rebuild JAR (signature files are now stripped automatically) |
| Double-nested output paths | Use `$DPLA_DATA` as `--output`, never `$DPLA_DATA/<hub>` |
| Orphan Java processes | `pkill -f 'java.*ingestion'` or use the fat JAR (avoids sbt forks) |
| `_temporary` dirs but no `_SUCCESS` | Delete incomplete dir and retry |
| sbt lock conflict | Use the fat JAR, or wait for the first sbt process to finish |

---

## File Reference

| File | Purpose |
|------|---------|
| `.env` | Local environment (`JAVA_HOME`, `SLACK_WEBHOOK`) |
| `.env.example` | Template for `.env` |
| `i3.conf` | Hub configuration (endpoints, schedules, harvest types) |
| `AGENTS.md` | Agent guide — runbooks, notification policy, error patterns |
| `scripts/SCRIPTS.md` | Script reference (all shell scripts with usage) |
| `scheduler/orchestrator/main.py` | Orchestrator entry point |
| `scheduler/orchestrator/status.py` | Status reader (file-based, no subprocesses) |
| `scheduler/orchestrator/notifications.py` | Slack notification logic |
| `scheduler/orchestrator/hub_processor.py` | Per-hub pipeline execution |
| `scheduler/orchestrator/state.py` | Run/hub state and per-stage timing |
| `scheduler/orchestrator/anomaly_detector.py` | Pre-sync anomaly detection |
| `logs/status/<hub>.status` | Live per-hub status files (JSON) |
| `logs/orchestrator_state.json` | Full orchestrator state across runs |
| `~/dpla/data/escalations/` | Failure reports |
