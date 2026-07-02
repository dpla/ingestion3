# DPLA Ingestion3 — Agent Guide

This document is the reference guide for running ingests, checking status, and handling errors in the DPLA ingestion pipeline. When in doubt about what to do in a given situation, start here.

---

## What this pipeline does

The ingestion3 pipeline moves metadata from DPLA's partner institutions into the DPLA search index. Each month, records are pulled from partner systems, standardized, and loaded into Elasticsearch so they appear on dp.la. The pipeline has five stages:

1. **Harvest** — fetches records from a partner's OAI feed, API, or file export
2. **Mapping** — converts partner metadata into DPLA's standard format (MAP)
3. **Enrichment** — normalizes dates, languages, types, and rights statements
4. **JSONL export** — writes the final records to compressed files and syncs to S3
5. **Indexing** — loads the S3 data into Elasticsearch via the sparkindexer EMR job

---

## Before running anything

Every time you run pipeline scripts, make sure these are in place first:

**1. Load the project environment**
From the repo root, run `source .env`. This sets up paths and credentials that all the scripts depend on (Java location, data directory, Slack webhook, AWS profile, etc.). Scripts like `harvest.sh` and `ingest.sh` will load this automatically, but it's good practice to run it manually too.

**2. Check your Java version**
Java 11 or higher is required. Set `JAVA_HOME` in `.env` to point to your Java 11+ installation. If you see errors like `package java.net.http does not exist`, Java is probably set to the wrong version.

**3. Use the right Python**
Use `./venv/bin/python` or run `source ./venv/bin/activate` before running any Python scripts. Don't use your system Python.

**4. Use the right AWS profile**
All AWS commands (S3, SSM, EMR, etc.) need `--profile dpla`.

---

## How to run ingests

The main way to run ingests is through the Python scripts in `ingest_python_scripts/`. These handle everything remotely via AWS SSM — you don't need to SSH into the EC2 directly.

| What you want to do | Script to use |
|---------------------|--------------|
| See which hubs are scheduled this month | `pre_ingest_check.py` |
| Check the EC2 is ready before a run | `hub_preflight.py` |
| Run a standard hub ingest | `launch_ingest.py` |
| Watch a running ingest | `check_ingest.py` |
| Run the NARA ingest | `nara/launch_nara.py` |
| Run the Smithsonian ingest | `smithsonian/launch_smithsonian.py` |
| Run the Community Webs ingest | `community-webs/launch_cw.py` |
| Verify all hubs ingested (run before indexer) | `postchecks.py` |
| Rebuild the search index (run after postchecks passes) | `launch_indexer.py` |
| Run post-index batch jobs (sitemaps, exports) | `post_indexer.py` |

Full details and step-by-step workflow in [`ingest_python_scripts/README.md`](ingest_python_scripts/README.md).

For the underlying shell scripts (used by the Python scripts, or for manual one-off runs), see [`scripts/SCRIPTS.md`](scripts/SCRIPTS.md).

---

## Ingest workflow reference

| Task | Where to go |
|------|-------------|
| Single-hub ingest | [.claude/rules/ingestion.md](.claude/rules/ingestion.md) |
| Parallel / scheduled runs | [.claude/rules/orchestrator.md](.claude/rules/orchestrator.md) |
| Failure notifications | [.claude/rules/notifications.md](.claude/rules/notifications.md) |
| All runbooks | [runbooks/README.md](runbooks/README.md) |

> Rules and skills are synced from `docs/ai-context/`. Edit there, then run `./scripts/ai-context/sync.sh` to apply changes.

---

## When something goes wrong

**Who to notify:** All errors go to **Slack #tech-alerts**. If Slack isn't set up, email **tech@dp.la** instead.

**What counts as an error (always notify):**
- A partner's feed or endpoint is unreachable
- Harvest failed — no records came through
- Mapping or enrichment step failed
- S3 sync failed
- The pipeline stopped because a record count dropped more than expected
- Any pipeline step that exited with an error

**What counts as a status update (notify when it happens):**
- An ingest run started (which hubs, what time)
- An ingest run finished (which succeeded, which failed)
- A warning about unexpected changes (but the sync still went through)

**When you're running scripts manually and something fails:**
1. Figure out which stage failed (harvest, mapping, enrichment, sync)
2. Post a short message to **#tech-alerts** with: hub name, stage that failed, and a brief description of the error or path to the log
3. If Slack isn't available, email tech@dp.la

---

## Notification summary

| What happened | Slack #tech-alerts | Email tech@dp.la |
|---------------|--------------------|------------------|
| Run started | Yes | Optional |
| Each stage completed (harvest, mapping, etc.) | Yes | — |
| A warning (sync went through but something was unusual) | Yes | Optional |
| A critical error (sync stopped) | Yes | Required if no Slack |
| Run completed | Yes | Optional |
| Stage failed | Yes | Required if no Slack |

When using the Python orchestrator (`python -m scheduler.orchestrator.main`), notifications are sent automatically. When running scripts manually, you're responsible for posting to Slack or emailing tech@dp.la.

---

## Common errors and fixes

| Error message | What it means | What to do |
|---------------|--------------|------------|
| `Unrecognized option: --add-opens` | Java version is too old (needs Java 11+) | Set `JAVA_HOME` to Java 11+ in `.env` |
| `Could not find or load main class` | The built JAR has a conflict with signed dependency files | Run `sbt assembly` again from repo root |
| `SecurityException: Invalid signature file digest` | Same as above (Java gives a clearer message here) | Run `sbt assembly` again from repo root |
| `IOException: Failed to delete` with a double-nested path | Output path was set incorrectly | Fix the output path and delete the duplicate directory |
| Orphan Java processes after stopping a script | sbt launched a child process that kept running | Run `pkill -f 'java.*ingestion'` to clean up |
| `_temporary` directories but no `_SUCCESS` file | A Spark write was interrupted | Delete the incomplete directory and rerun the stage |
| sbt project lock conflict | Two sbt processes tried to run at the same time | Use the fat JAR instead of sbt (it has no lock), or wait for the first process to finish |

---

## Build notes (for reference)

The pipeline runs from a pre-built "fat JAR" — a single file that bundles all the Scala code and its dependencies. Scripts build this automatically when needed, but you can also build it manually:

```bash
source .env
sbt assembly
```

The JAR is faster to start up than running through sbt directly, and it avoids some common issues with parallel ingests. After making any code changes in `src/`, rebuild with `sbt assembly` before running ingests.

---

## Project conventions

- **Environment:** Always `source .env` from repo root before running scripts
- **Python:** Use `./venv/bin/python` — not system Python
- **AWS:** Always use `--profile dpla`
- **Shell scripts:** Keep them cross-platform (macOS and Linux both run these). Use the helper functions in `scripts/common.sh` instead of OS-specific commands
- **New scripts:** Document them in `scripts/SCRIPTS.md` and add tests in `scripts/tests/`
