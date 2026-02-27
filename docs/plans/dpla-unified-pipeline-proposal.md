# DPLA Unified Pipeline Orchestration — Proposal

**Date:** February 2026
**Author:** DPLA Engineering
**Status:** Draft — for review by Director and Community Manager

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current State](#current-state)
3. [The Proposal](#the-proposal)
4. [Data Flow Architecture](#data-flow-architecture)
5. [Strategic Analysis](#strategic-analysis)
6. [Deep Technical Analysis](#deep-technical-analysis)
7. [Risk Assessment](#risk-assessment)
8. [Phased Rollout Plan](#phased-rollout-plan)
9. [Effort Estimates](#effort-estimates)
10. [Appendix A: Project Reference — ingestion3](#appendix-a-project-reference--ingestion3)
11. [Appendix B: Project Reference — sparkindexer](#appendix-b-project-reference--sparkindexer)
12. [Appendix C: Project Reference — ingest-wikimedia](#appendix-c-project-reference--ingest-wikimedia)
13. [Appendix D: EC2 Operational Snapshot](#appendix-d-ec2-operational-snapshot)

---

## Executive Summary

DPLA operates three independent software projects that together form a data pipeline: records are harvested and processed (ingestion3), indexed for search (sparkindexer), and uploaded to Wikimedia Commons (ingest-wikimedia). Today these projects run in isolation — on different machines, in different languages, with no coordination, no shared monitoring, and no unified view of progress.

This proposal recommends creating a lightweight fourth project (`dpla-pipeline`) to orchestrate, monitor, and document the three existing systems. The immediate deliverable is a **daily Wikimedia digest** that posts upload progress to Slack, giving the community manager visibility into a process that currently requires SSH access to inspect. The longer-term deliverable is automated coordination of the full monthly cycle: ingest all hubs, build a new search index, flip it live, and run Wikimedia uploads — with Slack notifications at every phase transition and automatic escalation on failure.

The total effort is estimated at 3–4 weeks of engineering time, broken into four independent phases that each deliver value on their own.

---

## Current State

### Three projects, three environments, no coordination

| Project | Language | Runs On | Cadence | Trigger | Monitoring |
|---------|----------|---------|---------|---------|------------|
| **ingestion3** | Scala + Python | Local machine | Monthly | Manual or orchestrator | Slack via orchestrator |
| **sparkindexer** | Scala/Spark | AWS EMR (temporary cluster) | Monthly, after ingestion | Manual script | None (check EMR console) |
| **ingest-wikimedia** | Python | Standalone EC2 | Continuous | Manual tmux sessions | SSH and check logs |

### How the pipeline works today

1. **Ingestion (monthly):** An engineer runs the ingestion3 orchestrator on their local machine. It harvests records from ~60 hub sources (OAI-PMH, API, file), maps them to the DPLA data model, enriches metadata, exports to JSONL, and syncs to S3. The orchestrator sends per-stage Slack notifications. This takes about a week to complete all hubs.

2. **Indexing (monthly, after ingestion):** An engineer manually runs `sparkindexer-emr.sh`, which builds a JAR, uploads it to S3, and launches an EMR cluster with 10 nodes. The cluster reads all JSONL from S3, transforms records, and writes them to a new Elasticsearch index. After ~2 hours, the engineer manually runs `flip-alias.sh` to point the `dpla_alias` to the new index. The DPLA API and website now serve updated data. There are no automated notifications.

3. **Wikimedia uploads (continuous):** On a dedicated EC2 instance, an engineer has started `uploader` commands inside tmux windows. These run for weeks or months, iterating through lists of DPLA IDs, fetching metadata from the DPLA API, downloading media from source institutions, and uploading to Wikimedia Commons. There is no monitoring, no daily reporting, and no automatic recovery from failures.

### What's missing

- **No visibility into Wikimedia progress.** The community manager cannot see upload statistics without SSH access.
- **No detection of stalled processes.** As of today, the minnesota uploader on the EC2 has been silently stuck for over 2 months (since Dec 20, 2025) due to a `ConnectionError` on an IIIF manifest fetch. Nobody knew.
- **No coordination between phases.** The indexer must wait for ingestion to finish. Wikimedia must wait for the alias flip (it reads from the DPLA API, which serves from the active ES index). These dependencies are managed by humans remembering to run things in order.
- **No unified status view.** Knowing "where are we in the monthly cycle" requires checking three different systems.
- **Manual intervention at every handoff.** Each phase transition requires an engineer to SSH in, check status, and start the next step.

---

## The Proposal

### Create `dpla-pipeline`: a fourth project for orchestration and documentation

Rather than embedding cross-project coordination into any one repository, we create a new lightweight Python project that serves as:

1. **A monitor** — daily Wikimedia digest to Slack, process health checks
2. **A coordinator** — automated phase transitions (ingestion → indexing → wikimedia)
3. **A documentation home** — cross-project reference, architecture diagrams, stakeholder briefs

### Why a fourth project (not extending ingestion3)?

The three projects run in fundamentally different environments with different operational cadences:

- **ingestion3** is monthly, run-and-done, on a local machine
- **sparkindexer** is monthly, run-and-done, on temporary EMR clusters
- **ingest-wikimedia** is long-running and continuous, on a persistent EC2 instance

Embedding wikimedia monitoring into ingestion3 would mean the local-machine orchestrator must reach across to the EC2. The ingestion3 orchestrator is designed for monthly batch runs, not continuous monitoring. Conversely, embedding indexer coordination into the wikimedia EC2 would couple indexing to a machine that has nothing to do with it.

A fourth project can be deployed wherever makes sense — on the wikimedia EC2 for the daily digest, on the local machine for the full pipeline run, or on a CI system in the future. It keeps each repo focused on its domain while providing the "glue" layer.

### What the fourth project is NOT

It does not replace the ingestion3 orchestrator. Phase 1 (monthly ingestion) continues to use ingestion3's existing Python orchestrator exactly as it does today. The fourth project coordinates *between* phases and adds monitoring that doesn't exist anywhere today.

---

## Data Flow Architecture

```
                        ┌──────────────────────────────────────────────────┐
                        │           PHASE 1: Monthly Ingestion             │
                        │              (ingestion3 project)                │
                        │                                                  │
                        │  Hub Sources ──→ Harvest ──→ Map ──→ Enrich     │
                        │  (OAI/API/File)              ──→ JSONL Export    │
                        │                              ──→ S3 Sync        │
                        └──────────────────────┬───────────────────────────┘
                                               │
                            JSONL files in s3://dpla-master-dataset/{hub}/jsonl/
                                               │
                        ┌──────────────────────▼───────────────────────────┐
                        │           PHASE 2: Indexing                      │
                        │            (sparkindexer project)                │
                        │                                                  │
                        │  S3 JSONL ──→ EMR Spark Job ──→ ES Index        │
                        │  (all hubs)   (10 nodes)        dpla-all-{ts}   │
                        │                          ──→ Flip dpla_alias     │
                        └──────────────────────┬───────────────────────────┘
                                               │
                                   DPLA API now serves fresh data
                                               │
                        ┌──────────────────────▼───────────────────────────┐
                        │           PHASE 3: Wikimedia Uploads             │
                        │          (ingest-wikimedia project)              │
                        │                                                  │
                        │  DPLA API ──→ Get eligible IDs                  │
                        │           ──→ Download media to S3              │
                        │               (s3://dpla-wikimedia/{partner}/)   │
                        │           ──→ Upload to Wikimedia Commons       │
                        └──────────────────────────────────────────────────┘

    Notifications (via dpla-pipeline):
    ┌─────────────────────────────────────────────────┐
    │  Slack #tech-alerts:                            │
    │    • Phase 1: per-stage progress (existing)     │
    │    • Phase 2: EMR started/complete, alias flip  │
    │    • Phase 3: daily digest, stalled processes   │
    │    • All phases: failure escalation              │
    └─────────────────────────────────────────────────┘
```

### Data handoff points

| From | To | Artifact | Location |
|------|----|----------|----------|
| ingestion3 | sparkindexer | JSONL files (gzipped, one dir per hub) | `s3://dpla-master-dataset/{hub}/jsonl/{timestamp}-{hub}-MAP3_1.IndexRecord.jsonl/` |
| sparkindexer | DPLA API | Elasticsearch index | `dpla-all-{timestamp}` on ES cluster, accessed via `dpla_alias` |
| DPLA API | ingest-wikimedia | Item metadata (JSON) | `https://api.dp.la/v2/items/{id}` |
| ingest-wikimedia | Wikimedia Commons | Media files + wikitext | Wikimedia Commons pages |
| ingest-wikimedia | S3 (staging) | Downloaded media + metadata | `s3://dpla-wikimedia/{partner}/images/{hash_dirs}/{dpla_id}/` |

### Critical dependency chain

```
All hubs ingested  ──→  Indexer can run  ──→  Alias flipped  ──→  Wikimedia sees fresh data
```

Running the indexer before all hubs are done means the new index will have stale data for unfinished hubs. Running wikimedia before the alias flip means it fetches metadata from the old index and may miss new records or process deleted ones.

---

## Strategic Analysis

### Benefits

**Immediate operational wins:**
- The daily Wikimedia digest gives the community manager daily visibility into upload progress without requiring engineering support or SSH access. This is the single most impactful deliverable.
- Stalled process detection prevents the kind of silent failure we're seeing today (minnesota stuck for 2+ months). A single daily health check would have caught this on day one.

**Organizational benefits:**
- A unified status view makes it possible to answer "where are we in the monthly cycle?" without checking three systems.
- Documentation of the full pipeline makes the system understandable to non-engineers and reduces bus factor — today the pipeline is fully understood by one engineer.
- Automated phase transitions reduce the manual work of running the monthly cycle. Each handoff that currently requires human intervention becomes automatic.

**Scalability benefits:**
- As DPLA adds more hubs or more Wikimedia-eligible partners, the coordination overhead grows. Automating phase transitions keeps the work constant regardless of scale.
- The orchestration layer provides a natural place to add new pipeline phases (e.g., thumbnail generation, analytics) without modifying the existing projects.

**AI/Agentic workflow benefits:**
- The documentation and structured configuration in the fourth project create ideal context for AI coding agents. An agent can read the pipeline overview, understand all three projects, and assist with operations.
- The daily digest establishes the pattern for machine-readable status that agents can query and act on.
- The phase coordinator provides the scaffolding for future autonomous operation — an agent (or scheduled job) triggers each phase based on the previous phase's completion status.

### Threats

**Increased complexity:**
- A fourth repo is one more thing to maintain, deploy, and keep in sync. If the coordination layer breaks, manual operation must remain possible as a fallback.
- Cross-project knowledge encoded in one place means that place becomes a new single point of failure for understanding.

**Environment sprawl:**
- The EC2 instance has been running for 311 days. Adding new scripts or services there increases the risk of configuration drift and makes the instance harder to rebuild.
- Different Python versions, virtual environments, and dependency sets across projects could cause subtle compatibility issues.

**Operational risk during transition:**
- Automating the alias flip is the highest-risk change. A bug could point the production search to an empty or partial index. The manual step (with its `read -p "Confirm?"` prompt) is a safety gate that automation must replace with equivalent validation.

### Opportunities

**Progressive automation:** Each phase of this proposal delivers standalone value. We don't need to commit to full automation upfront — the daily digest alone is worth building even if we never automate the indexer.

**Wikimedia partnership visibility:** A daily digest with concrete numbers (X images uploaded, Y GB transferred) provides material for stakeholder reports, grant applications, and Wikimedia partnership communications.

**Foundation for scheduling:** The phase coordinator is a natural extension point for a future scheduling system — e.g., "on the first Monday of each month, start the ingestion cycle; when it completes, trigger indexing; when that completes, refresh wikimedia ID lists."

**Cost optimization:** Automating the EMR indexer with proper monitoring can prevent runaway costs. Today, if an EMR cluster hangs, nobody knows until someone checks the AWS console. An automated monitor would detect this and terminate the cluster.

---

## Deep Technical Analysis

### ingestion3 — Edge Cases and Risks

**State persistence is not crash-safe.** The orchestrator writes state to a JSON file after each stage transition, but writes are not atomic. If the process crashes mid-write, the state file can be corrupted. On load failure, state is reset to empty, losing all run history. There is no locking, so concurrent orchestrator instances could corrupt state.

**Retry logic is harvest-only.** Harvest failures get up to 5 retries with exponential backoff, but mapping, enrichment, JSONL export, and S3 sync have no retry logic. A transient network error during S3 sync fails the entire hub.

**Anomaly detection can false-positive.** Thresholds are global (15% harvest drop = warning, 30% = critical halt). A legitimate change — say, a hub removes a collection — triggers a critical halt that blocks S3 sync and requires manual override. Thresholds are not configurable per-hub.

**S3 sync is not atomic.** `aws s3 sync` can be interrupted, leaving partial data in S3. The anomaly check runs before sync, not after. There is no verification that sync completed successfully.

**Incomplete run cleanup is manual.** If a pipeline step crashes, it may leave `_temporary` directories. The orchestrator does not clean these up on restart. Hubs can be stuck in intermediate states (e.g., `HARVESTING`) with no timeout or detection.

**Environment variables are implicit.** `DPLA_DATA`, `I3_CONF`, and `I3_HOME` are used throughout but not in `.env.example`. Missing variables cause silent failures rather than clear errors.

### sparkindexer — Edge Cases and Risks

**Index creation is not idempotent.** If an index with the same name already exists, creation throws a `RuntimeException` with no retry or cleanup. If the indexer fails mid-run, a partial index remains and must be manually deleted.

**ES write errors are silently swallowed.** `ElasticSearchWriter.saveRecords()` wraps writes in `Try()` but only prints the error — it does not throw or exit non-zero. A Spark job can report success even if significant ES write failures occurred.

**No pre-flight health checks.** The indexer does not verify that the ES cluster is healthy, that S3 paths exist, or that JSONL files are valid before beginning. Failures are discovered mid-run.

**Alias flip is manual and unvalidated.** `flip-alias.sh` finds the newest `dpla-all-*` index and flips the alias, but does not verify:
  - That the new index finished building (no `_SUCCESS` marker or completion check)
  - That the document count is reasonable (no anomaly detection)
  - That the index is serving queries correctly

The alias flip itself is atomic (ES `_aliases` API), but the script has a race condition between reading the current alias and executing the flip.

**EMR configuration is hardcoded.** Security groups (`sg-07459c7a`, etc.), subnet (`subnet-90afd9ba`), and instance profile (`sparkindexer-s3`) are hardcoded in `sparkindexer-emr.sh`. If any of these AWS resources are modified or deleted, cluster creation fails with potentially cryptic errors.

**Scala version mismatch.** sparkindexer uses Scala 2.12.18; ingestion3 uses Scala 2.13.15. While they don't share compiled artifacts, any future code sharing would require addressing this.

**Cost risk.** EMR with 10 instances runs ~$10–30/hour depending on instance type. If a job hangs or fails silently (ES write errors not propagated), the cluster auto-terminates only on step completion/failure. A hung Spark task could keep the cluster running.

### ingest-wikimedia — Edge Cases and Risks

**No checkpoint or resume capability.** If the uploader crashes, the entire ID list must be re-processed from the beginning. The SHA-1 deduplication means already-uploaded files are skipped (fast), but the process still iterates through every ID, re-fetching metadata from the DPLA API. For NARA with ~20,000 IDs and multi-page documents, this restart overhead is significant.

**Wikimedia session timeout is unhandled.** The ROADMAP explicitly flags this: "the user was logged out but the process still kept running." If the pywikibot session expires, uploads silently fail. There is no re-authentication logic. This may explain some failure patterns in long-running uploads.

**Stalled processes are invisible.** The minnesota uploader has been stuck since Dec 20, 2025, sleeping on a `ConnectionError` from `digitalcollections.hclib.org`. The process is alive (PID 3130639, state `S (sleeping)`) but not making progress. No monitoring detects this.

**Log files grow unbounded.** The active NARA upload log is 167MB. BPL logs total 4GB. There is no log rotation. Disk usage is manageable now (23GB of 100GB), but log accumulation over years will become an issue.

**The Tracker is in-memory only.** The `Tracker` class counts results (DOWNLOADED, UPLOADED, SKIPPED, FAILED, BYTES) but only dumps them to the log at the end of a run. If the process crashes, all tracking data for that run is lost. There is no persistent state file.

**IIIF manifest parsing is fragile.** The code handles IIIF v2 and partially handles v3. Regex-based URL maximization supports 0–3 prefix layers; servers with different URL structures will fail silently (returning empty strings). The ContentDM IIIF URL construction is a regex heuristic that may not match all ContentDM configurations.

**Banlist is loaded once at startup.** The 123-entry banlist (`dpla-id-banlist.txt`) prevents re-uploading copyright-deleted content. It is loaded into memory at process start and never refreshed. Changes require restarting the uploader.

**No parallel downloads.** The ROADMAP lists this as a desired feature. Currently, downloads are sequential — one item at a time, one media file at a time. For multi-page documents (NARA has many), this is a significant bottleneck.

**Partner-specific quirks:**
- NARA: Has a URL data quality issue (`https/` → `https:/`) that requires a runtime hack in the downloader
- ContentDM partners: IIIF URL construction from `isShownAt` uses heuristic regex matching
- BPL: Last upload run showed 2,171,647 skipped vs 36 uploaded — vast majority of items are duplicates or have zero-byte files in S3 (a data quality issue from earlier download runs)

### Cross-Project Integration Risks

**S3 path format coupling.** sparkindexer finds JSONL files by listing S3 prefixes matching `{provider}/jsonl/` and using string comparison on timestamps. If ingestion3 changes its output path format, timestamp format, or directory structure, the indexer breaks with no clear error.

**DPLA API as coupling point.** ingest-wikimedia depends on the DPLA API, which depends on the ES index, which depends on sparkindexer, which depends on ingestion3's S3 output. A failure anywhere in this chain affects wikimedia. Specifically:
- If the indexer creates a partial index and the alias flips to it, wikimedia may get 404s for items that were in the old index but missing from the new one.
- If ingestion3 produces bad JSONL (e.g., malformed JSON), the indexer may silently skip records, and wikimedia would then get 404s for those items.

**Environment variable fragmentation.** Each project has its own configuration mechanism:
- ingestion3: `.env` file + `i3.conf` (HOCON format)
- sparkindexer: command-line arguments + hardcoded values in scripts
- ingest-wikimedia: `config.toml` (partner secrets) + pywikibot `user-config.py`

A unified orchestrator must handle all three configuration systems.

---

## Risk Assessment

### High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Automated alias flip points to bad index | Production search serves wrong/partial data | Pre-flip validation: check document count against expected, compare to previous index, require minimum record count |
| Minnesota-style silent stalls go undetected | Partner uploads stop for months with no visibility | Daily digest with stall detection (the immediate deliverable) |
| EMR cluster cost overrun | Unexpected AWS charges | Automated cluster monitoring with timeout-based termination; cost alerts |

### Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| State file corruption in ingestion3 orchestrator | Lose track of run progress | Atomic writes (write to temp, rename); file locking for concurrent protection |
| Wikimedia session timeout during long upload | Uploads silently fail | Session health check; periodic re-authentication; stall detection in digest |
| S3 path format change breaks indexer | Indexer can't find JSONL files | Integration test that verifies S3 path format compatibility between projects |
| Partial S3 sync after anomaly check passes | Inconsistent data in production | Post-sync verification; anomaly recheck after sync |

### Low Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Scala version mismatch (2.12 vs 2.13) | No current impact; blocks future code sharing | Accept for now; align versions if code sharing is needed |
| Log disk exhaustion on EC2 | Uploads stop; instance unstable | Log rotation; disk usage alert in daily digest |
| IIIF manifest format changes | Some partner downloads fail | Monitor download failure rates; add new format support as needed |

---

## Phased Rollout Plan

### Phase 0: Documentation (Week 1, ~2 days)

**Deliverables:**
- This proposal document (already in progress)
- Cross-project reference: `docs/pipeline-overview.md`
- Wikimedia project reference: `docs/ingest-wikimedia-reference.md`
- Stakeholder brief: `docs/stakeholder-brief.md`

**Value:** Reduces bus factor. Gives the director and community manager a document to review. Provides AI agents with full context for future work. Costs nothing beyond writing time.

**Risk:** None. Pure documentation.

### Phase 1: Wikimedia Daily Digest (Week 1–2, ~3 days)

**Deliverables:**
- `wikimedia_digest.py` — Parses EC2 logs, detects stalled processes, posts Slack summary
- Cron setup on EC2 (install `cronie`, daily 9am ET job)
- Slack channel configuration

**Value:** Immediate visibility for the community manager. Catches stalled processes like the current minnesota hang. Establishes the notification pattern for later phases.

**Dependencies:** Slack webhook URL, EC2 SSH access.

**Risk:** Low. Read-only against logs; Slack posting is non-destructive. Worst case: the digest has a bug and posts garbage to Slack.

**Technical approach:**
- Read log files from the end backwards (they're 100MB+, don't read the whole thing)
- Parse timestamps from `[INFO] HH:MM:SS:` prefixed lines
- Count `Uploaded to`, `Skipping`, `Failed` patterns in the last 24 hours
- Check `/proc/{pid}/status` for running processes; compare last log timestamp to detect stalls
- Post via Slack Incoming Webhook (simple HTTP POST, no SDK needed)

### Phase 2: Project Scaffold and Notifications (Week 2, ~2 days)

**Deliverables:**
- `dpla-pipeline` project structure: `pyproject.toml`, README, config module, notification module
- Shared Slack notification helpers (reusable across all phases)
- Configuration management (paths to all three repos, ES host, S3 buckets, webhooks)

**Value:** Foundation for phases 3–4. The notification module is reused by every subsequent feature.

**Risk:** Low. No changes to existing systems.

### Phase 3: Indexer Orchestration (Week 2–3, ~5 days)

**Deliverables:**
- `indexer.py` — Automated EMR cluster creation, step monitoring, completion detection
- `alias_flip.py` — Automated alias flip with pre-flip validation (document count check, minimum record count, comparison to previous index)
- Slack notifications for indexer progress and alias flip
- Dry-run mode

**Value:** Eliminates the manual EMR launch and alias flip. Adds safety checks that don't exist today (pre-flip validation). Sends notifications that don't exist today.

**Dependencies:** AWS credentials with EMR create permission, ES cluster access.

**Risk:** Medium. The alias flip is the highest-risk automation. Mitigated by:
- Pre-flip validation (document count must be within 5% of previous index)
- Dry-run mode for testing
- Manual override available (existing `flip-alias.sh` still works)
- Rollback capability (flip alias back to previous index)

**Technical approach:**
- Replicate `sparkindexer-emr.sh` parameters in Python using `boto3` EMR client
- Poll `describe_step()` every 60 seconds until terminal state
- On success: query new index document count via ES API, compare to previous index
- If count is within threshold: execute alias flip via ES `_aliases` API
- If count is below threshold: alert Slack, do not flip, require manual intervention

### Phase 4: Wikimedia Coordination (Week 3–4, ~4 days)

**Deliverables:**
- `wikimedia_coordinator.py` — Determines eligible hubs, generates ID lists, invokes downloader/uploader
- Integration with ingest-wikimedia via subprocess (uses its own venv and CLI)
- Per-hub tracking and Slack status updates

**Value:** Replaces manual partner-by-partner wikimedia runs. Automatically refreshes ID lists after each monthly index flip. Coordinates runs across eligible partners.

**Dependencies:** ingest-wikimedia venv on EC2, DPLA API key.

**Risk:** Medium. The wikimedia uploader is a long-running process (days/weeks per partner). The coordinator must handle:
- Process monitoring (detect stalls, track progress)
- Graceful restart (re-running with the same ID list is safe due to SHA-1 dedup)
- Resource management (don't run too many uploaders in parallel on the EC2's 7.6GB RAM)

**Technical approach:**
- SSH to EC2 (or run locally on EC2) to invoke commands
- Use `get-ids-api` to generate fresh ID lists per partner after alias flip
- Launch `downloader` and `uploader` via subprocess in tmux windows
- Monitor progress by tailing log files (same technique as the daily digest)
- Report per-partner status to Slack

---

## Effort Estimates

### Size of lift with AI tooling

This project is well-suited to AI-assisted development. The codebase has thorough documentation (AGENTS.md, docs/ingestion/GOLDEN_PATH.md, SCRIPTS.md, ROADMAP.md), structured configuration, and clear patterns. AI agents can:
- Generate the daily digest script by analyzing the log format and process structure (already explored via SSH)
- Generate the EMR orchestration by translating `sparkindexer-emr.sh` to Python boto3 calls
- Generate the alias flip validation by reading `flip-alias.sh` and `Index.scala`
- Generate tests by following the existing test patterns in all three projects

**Estimated effort with AI assistance:**

| Phase | Without AI | With AI | Notes |
|-------|-----------|---------|-------|
| Phase 0: Documentation | 2 days | 0.5 days | AI can draft from code analysis; human reviews and refines |
| Phase 1: Daily Digest | 3 days | 1 day | Straightforward log parsing + Slack webhook; AI generates, human validates on EC2 |
| Phase 2: Scaffold | 2 days | 0.5 days | Boilerplate project setup; AI generates config/notification modules |
| Phase 3: Indexer | 5 days | 2–3 days | EMR creation well-documented; alias flip needs careful human review |
| Phase 4: Wikimedia | 4 days | 2 days | Subprocess coordination; most complexity is in error handling |
| **Total** | **16 days** | **6–7 days** | ~60% reduction with AI assistance |

### Where AI tooling is most effective

- **Log parsing and pattern matching** — the daily digest is almost entirely translatable from the EC2 exploration we already did
- **AWS API calls** — EMR cluster creation is a direct translation of the existing shell script to boto3
- **Boilerplate generation** — project scaffold, config loading, Slack notification helpers
- **Test generation** — following existing patterns

### Where human judgment is essential

- **Alias flip validation thresholds** — what document count delta is acceptable? This requires domain knowledge about expected index sizes
- **Failure escalation policy** — when should the system alert vs. block vs. proceed? These are business decisions
- **EC2 deployment** — cron setup, testing in the production environment, verifying the digest against real data
- **Review of cross-project assumptions** — S3 path format stability, API compatibility, config alignment

---

## Appendix A: Project Reference — ingestion3

**Repository:** `dpla/ingestion3` (Scala + Python)
**Location:** `/Users/scott/dpla/code/ingestion3`

### Pipeline stages

1. **Prepare** — Download S3 data for file-based hubs
2. **Harvest** — Extract records from OAI-PMH, API, or file sources (Scala/Spark)
3. **Mapping** — Transform to DPLA MAP data model (Scala/Spark)
4. **Enrichment** — Normalize and enrich metadata (Scala/Spark)
5. **JSONL Export** — Export enriched records to gzipped JSONL (Scala/Spark)
6. **S3 Sync** — Upload results to `s3://dpla-master-dataset/` with anomaly detection

### Orchestrator architecture

The Python orchestrator (`scheduler/orchestrator/`) provides:
- **Parallel execution** with configurable concurrency (semaphore-limited)
- **Three-phase processing**: S3 download → harvest/remap → S3 upload
- **State management**: JSON file at `logs/orchestrator_state.json`, per-hub status at `logs/status/{hub}.status`
- **Notifications**: Slack messages at each stage transition, failure escalation
- **Anomaly detection**: Compares current run to S3 baseline; halts sync on critical drops (>30% record drop, >40% failure rate)
- **Retry logic**: Harvest-only, 5 retries with exponential backoff

### Key configuration

- `i3.conf` (HOCON): Hub definitions (harvest type, endpoint, schedule, contacts)
- `.env`: `JAVA_HOME`, `SLACK_WEBHOOK`, `SLACK_TECH_WEBHOOK`, `SLACK_ALERT_USER_ID`
- Implicit: `DPLA_DATA` (default `~/dpla/data`), `I3_CONF` (default `~/dpla/code/ingestion3-conf/i3.conf`)

### Output artifacts

```
$DPLA_DATA/{hub}/
  harvest/{timestamp}-{hub}-HARVEST.avro/        (_SUCCESS, _MANIFEST)
  mapping/{timestamp}-{hub}-MAP.avro/            (_SUCCESS, _SUMMARY)
  enrichment/{timestamp}-{hub}-MAP4_0.EnrichRecord.avro/
  jsonl/{timestamp}-{hub}-MAP3_1.IndexRecord.jsonl/
```

S3 mirror: `s3://dpla-master-dataset/{hub-prefix}/jsonl/...`

### Build

```bash
source .env && sbt assembly
# → target/scala-2.13/ingestion3-assembly-0.0.1.jar (~570MB)
```

Requires Java 11+ (Java 19 recommended). Scala 2.13.15, Spark 3.5.5.

---

## Appendix B: Project Reference — sparkindexer

**Repository:** `dpla/sparkindexer` (Scala)
**Location:** `/Users/scott/dpla/code/sparkindexer`

### What it does

Reads JSONL files from S3, transforms records (remove legacy fields, add tags, add rights category, normalize provider), and writes to a new Elasticsearch index.

### Entry point

`dpla.ingestion3.indexer.IndexerMain` — 10 positional arguments:

| Arg | Description | Default |
|-----|-------------|---------|
| 0 | ES host | required |
| 1 | ES port | required |
| 2 | Index name base | required |
| 3 | Providers | `"all"` |
| 4 | Max timestamp | `"now"` |
| 5 | Shards | `5` |
| 6 | Replicas | `1` |
| 7 | Dataset bucket | `"dpla-master-dataset"` |
| 8 | Email addresses | `""` |
| 9 | Spark master | (EMR default) |

### How it finds data

`MasterDataset.buildPathList()` lists S3 prefixes in the dataset bucket:
1. List all top-level prefixes (provider directories)
2. Filter to requested providers (or all)
3. For each provider, list `{provider}/jsonl/` prefixes
4. Select the most recent directory before `maxTimestamp` (string comparison)
5. Return `s3a://` paths

### How it writes to ES

- Creates index with custom settings/mappings (custom analyzers, field mappings for DPLA schema)
- Initial settings: 0 replicas, 30s refresh interval (optimized for bulk write)
- Writes via ES Spark connector (`EsSpark.saveJsonToEs`) with upsert
- After completion: sets replicas to configured count

### Document tagging

`DocumentTagger` uses Apache Lucene Monitor to match documents against tag queries. Tag queries are JSON files in `/resources/tag_queries/`. Tags and sub-tags with exclusion logic are applied to records during indexing.

### Alias management

`scripts/flip-alias.sh`:
- Finds newest `dpla-all-*` index
- Checks current `dpla_alias` target
- Atomic flip via ES `_aliases` API (remove old + add new in one request)
- Requires `--execute` flag (dry-run by default)
- Prompts for confirmation before executing

### EMR execution

`sparkindexer-emr.sh` launches a 10-node EMR cluster:
- 1 master + 9 core nodes
- 2x250GB gp3 EBS per node (5TB total)
- Spark config: 24 executors, 12GB executor memory, 4 cores each
- Auto-terminates on step completion/failure
- JAR uploaded to `s3://dpla-sparkindexer/`

### Build

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 11) sbt assembly
# → target/scala-2.12/sparkindexer-assembly.jar
```

Scala 2.12.18, Spark 3.5.5. Note: different Scala version than ingestion3 (2.12 vs 2.13).

---

## Appendix C: Project Reference — ingest-wikimedia

**Repository:** `dpla/ingest-wikimedia` (Python 3.13+)
**Location:** `/Users/scott/dpla/code/ingest-wikimedia`

### What it does

Downloads media files from DPLA source institutions and uploads them to Wikimedia Commons with structured metadata.

### Two-phase workflow

**Phase A: Download** (`downloader IDS_FILE PARTNER API_KEY [--dry-run] [--verbose] [--overwrite] [--sleep N]`)
1. Read DPLA IDs from CSV file
2. For each ID: fetch metadata from DPLA API (`api.dp.la/v2/items/{id}`)
3. Validate eligibility (rights = "Unlimited Re-Use", has Wikidata IDs, not on banlist, has media)
4. Extract media URLs from `mediaMaster` (direct) or `iiifManifest` (parsed)
5. Download each media file to temp directory
6. Compute SHA-1 hash, detect MIME type
7. Upload to `s3://dpla-wikimedia/{partner}/images/{hash_dirs}/{dpla_id}/`

**Phase B: Upload** (`uploader IDS_FILE PARTNER [--dry-run] [--verbose]`)
1. Read DPLA IDs from CSV file
2. For each ID: retrieve metadata from S3
3. For each media file: download from S3, generate wikitext, check for duplicates (SHA-1), upload to Commons via pywikibot

### Eligible partners (13)

| Short name | Full name |
|------------|-----------|
| `bpl` | Digital Commonwealth |
| `georgia` | Digital Library of Georgia |
| `indiana` | Indiana Memory |
| `nara` | National Archives and Records Administration |
| `northwest-heritage` | Northwest Digital Heritage |
| `ohio` | Ohio Digital Network |
| `p2p` | Plains to Peaks Collective |
| `pa` | PA Digital |
| `texas` | The Portal to Texas History |
| `minnesota` | Minnesota Digital Library |
| `mwdl` | Mountain West Digital Library |
| `heartland` | Heartland Hub |

### S3 structure

```
s3://dpla-wikimedia/{partner}/images/{id[0]}/{id[1]}/{id[2]}/{id[3]}/{dpla_id}/
  dpla-map.json           # Full DPLA MAP record
  file-list.txt           # Newline-separated media URLs
  iiif.json               # IIIF manifest (if applicable)
  {ordinal}_{dpla_id}     # Media files (no extension; content-type in S3 metadata)
```

### Eligibility criteria

A record is eligible for Wikimedia upload if ALL of the following are true:
- `rightsCategory` is `"Unlimited Re-Use"`
- The provider (hub) is in the `DPLA_PARTNERS` list
- The provider has a Wikidata ID in `institutions_v2.json`
- The data provider has a Wikidata ID and `upload=True`
- The data provider has a non-empty name
- The record has `mediaMaster` or `iiifManifest` (media to download)
- The DPLA ID is not on the banlist (123 copyright-flagged IDs)

### Wikitext generation

Each uploaded file gets a Wikimedia Commons page with:
- Artwork template (creator, title, description, date, dimensions, medium)
- DPLA source template (data provider Wikidata, hub, URL, DPLA ID, local identifiers)
- Institution template (Wikidata)
- License template mapped from `edmRights` URI

### Configuration

- `config.toml`: Per-partner DPLA API secrets (gitignored)
- `user-config.py` / `user-password.py`: pywikibot Wikimedia credentials (gitignored)
- Provider data: fetched from GitHub (`dpla/ingestion3/.../institutions_v2.json`)

### Known limitations (from ROADMAP.md)

- No checkpoint/resume capability
- No parallel downloads
- Wikimedia session timeout unhandled (logged-out user, uploads silently fail)
- Banlist loaded once at startup
- IIIF v3 parsing incomplete
- No automated notifications or monitoring
- No `_SUMMARY` file generation

### Utility tools

| Tool | Purpose |
|------|---------|
| `get-ids-api` | Query DPLA API for eligible IDs per partner (sharded hex queries) |
| `get-incomplete-items` | Find items with incomplete downloads in S3 |
| `retirer` | Blank out ineligible items in S3 (set size to 0) |
| `remimer` | Fix MIME types for `binary/octet-stream` files |
| `sign` | Add SHA-1 checksums to unsigned S3 objects |
| `nuke` | Delete items from S3 |
| `nara-ids` | Generate filtered NARA ID list (faceted collection/language/format queries) |

---

## Appendix D: EC2 Operational Snapshot

**Instance:** `ec2-54-92-228-188.compute-1.amazonaws.com`
**Uptime:** 311 days (as of Feb 23, 2026)
**Resources:** 7.6GB RAM (1GB used), 100GB disk (23GB used)
**Python:** 3.13.2, uv installed
**Cron:** Not installed (`crontab: command not found`)

### Currently running

Two tmux windows in session `0`:

**Window 1: `nara-up`** (PID 2142748, running since Sep 2025)
- Command: `uploader resume.csv nara`
- Status: Active. Last upload at 03:08 UTC Feb 23.
- Queue: 16,643 IDs remaining (from `resume.csv`)
- Current log: `nara/logs/20250925-133435-nara-upload.log` (167MB)
- Stats from log: 884,396 uploaded, 73,932 skipped, ~3,093 warnings
- Rate: ~6,500 files/day

**Window 2: `minnesota-up`** (PID 3130639, running since Dec 2025)
- Command: `uploader minnesota.csv minnesota`
- Status: **STALLED.** Process alive (sleeping, `wait_woken`) but no log activity since Dec 20, 2025.
- Error: `ConnectionError` on `digitalcollections.hclib.org` (IIIF manifest fetch, connection reset by peer)
- Last known stats: 85,466 uploaded, 307,767 skipped out of ~399,447 IDs

### Partner status summary

| Partner | Last activity | Type | Notes |
|---------|--------------|------|-------|
| nara | Active now | upload | 884K uploaded, 16.6K remaining |
| minnesota | Stalled Dec 20 | upload | ConnectionError, needs restart |
| bpl | Dec 18 | retirer | Last upload: 36 uploaded, 2.1M skipped (zero-byte files) |
| georgia | Nov 30 | download | Completed |
| heartland | Dec 2 | upload | Completed |
| indiana | Nov 30 | download | Completed |
| mwdl | Nov 30 | download | Completed |
| northwest-heritage | Nov 30 | download | Completed |
| ohio | Nov 30 | download | Completed |
| p2p | Nov 30 | retirer | Completed |
| pa | Dec 2 | retirer | Completed |
| texas | Nov 7 | download | Completed |

### Log disk usage

Total logs: ~13GB across all partners. Largest: NARA (4.3GB), BPL (4.0GB), Ohio (1.5GB).

### Git state

On commit `2d24737` ("Bot headers fixes (#99)"). Remote: `git@github.com:dpla/ingest-wikimedia.git`.

### Directory structure per partner

Each partner has its own working directory:
```
{partner}/
  config.toml → ../config.toml (symlink)
  {partner}.csv                  # ID list
  logs/                          # Per-run log files
  apicache/                      # (some partners)
  pywikibot-DPLA_bot.lwp         # Session cookie
  user-config.py → ../user-config.py (symlink, some partners)
```

Process execution: uploaders/downloaders are launched manually inside tmux windows, running from within the partner's directory. The working directory matters because pywikibot reads `user-config.py` from the current directory.
