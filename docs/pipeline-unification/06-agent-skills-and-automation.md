# DPLA Pipeline Unification -- Agent Skills and Automation

**Audience:** Engineers, operations staff at receiving organization

**Reading time:** 30 minutes

**Context:** This document covers how DPLA's pipeline operations are documented as runnable procedures -- and how those procedures can be executed by either a human engineer or a coding assistant following the same steps. It describes what is available today, what is proposed, and how to implement new procedures.

---

## Contents

- [Documented Procedures and the Handoff Problem](#documented-procedures-and-the-handoff-problem)
  - [Key Terms](#key-terms)
  - [Where Procedures Live in This Codebase](#where-procedures-live-in-this-codebase)
- [What Can Be Done With Documented Procedures Today](#what-can-be-done-with-documented-procedures-today)
  - [ingestion3 (8 Cursor Skills, 6 Claude Code Rules, 2 Claude Code Skills)](#ingestion3-8-cursor-skills-6-claude-code-rules-2-claude-code-skills)
  - [sparkindexer (No Documented Procedures Yet)](#sparkindexer-no-documented-procedures-yet)
  - [ingest-wikimedia (No Documented Procedures Yet)](#ingest-wikimedia-no-documented-procedures-yet)
- [Proposed Procedures for sparkindexer](#proposed-procedures-for-sparkindexer)
  - [sparkindexer-build-and-upload](#sparkindexer-build-and-upload)
  - [sparkindexer-launch-emr](#sparkindexer-launch-emr)
  - [sparkindexer-check-index](#sparkindexer-check-index)
  - [sparkindexer-flip-alias](#sparkindexer-flip-alias)
  - [sparkindexer-delete-old-indices](#sparkindexer-delete-old-indices)
  - [sparkindexer-debug](#sparkindexer-debug)
- [Proposed Procedures for ingest-wikimedia](#proposed-procedures-for-ingest-wikimedia)
  - [wikimedia-check-status](#wikimedia-check-status)
  - [wikimedia-restart-partner](#wikimedia-restart-partner)
  - [wikimedia-refresh-ids](#wikimedia-refresh-ids)
  - [wikimedia-download-partner / wikimedia-upload-partner](#wikimedia-download-partner--wikimedia-upload-partner)
  - [wikimedia-daily-digest](#wikimedia-daily-digest)
  - [wikimedia-ec2-health](#wikimedia-ec2-health)
- [Cross-Project Procedures (Pipeline Coordinator)](#cross-project-procedures-pipeline-coordinator)
  - [pipeline-monthly-cycle](#pipeline-monthly-cycle)
  - [pipeline-status](#pipeline-status)
- [End-to-End Pipeline Flow](#end-to-end-pipeline-flow)
  - [Where Human Judgment Is Always Required](#where-human-judgment-is-always-required)
- [Implementing Procedures: Structure and Location](#implementing-procedures-structure-and-location)
  - [Skill File Structure](#skill-file-structure)
  - [File Locations](#file-locations)
  - [Testing Skills](#testing-skills)

---

## Documented Procedures and the Handoff Problem

DPLA's pipeline is bespoke software built over many years. When it is handed off to a new team, the challenge is not that the domain is obscure -- library metadata aggregation is well understood -- it is that the specific operational knowledge of *how* to run this particular pipeline is concentrated in the people who built it.

Documented procedures address this directly. A procedure file describes how to perform a specific task: what commands to run, in what order, what to check before proceeding, and what to do when something goes wrong. When that procedure is written carefully, it can be followed by a human engineer who has never seen the codebase before -- or by a coding assistant (such as Cursor or Claude Code) running in the same repository. The file is documentation and executable instructions at the same time.

This is not about replacing engineers with automation. It is about reducing the knowledge transfer burden. A team member can follow the procedure for "run the monthly ingest for Ohio" without needing to memorize harvest types, configuration keys, and script ordering. A coding assistant can do the same, freeing the engineer's attention for the judgment calls that actually require it.

### Key Terms

**Skill:** A procedure file (`SKILL.md`) that describes how to perform a specific task. Skills are both documentation a human can read and steps a coding assistant can follow.

**Rule:** Background context that is always active (e.g., "always use `--profile dpla` for AWS commands"). Rules are loaded automatically by the coding assistant when it runs in the repository.

**Gate:** A mandatory checkpoint within a skill where a precondition must be verified before proceeding. If the check fails, the process stops and reports. Gates are not optional steps.

### Where procedures live in this codebase

Engineers can use Cursor or Claude Code in this repository; skills and rules are loaded automatically when the tool runs from the repository root. The canonical runbook (environment, scripts, notifications) is [AGENTS.md](../../AGENTS.md). Procedure files live in the following locations:

| What | Where in this repo |
|------|--------------------|
| Runbook and environment guide | [AGENTS.md](../../AGENTS.md) |
| Cursor skills (ingestion3) | `.cursor/skills/dpla-*` (e.g. dpla-run-ingest, dpla-orchestrator, dpla-s3-and-aws) |
| Claude Code rules | `.claude/rules/*.md` (loaded automatically when using Claude Code) |
| Claude Code skills | `.claude/skills/*` |
| Rules index (which rule when) | [.claude/rules/README.md](../../.claude/rules/README.md) |

**Further reading (external documentation):**

- [Cursor -- Agent Skills](https://cursor.com/docs/context/skills): How Cursor discovers and uses `SKILL.md` procedure files.
- [Claude Code -- Skills](https://code.claude.com/docs/en/skills): How to extend Claude Code with skills.

---

## What Can Be Done With Documented Procedures Today

### ingestion3 (8 Cursor Skills, 6 Claude Code Rules, 2 Claude Code Skills)

| Task | Trigger Phrase | What the Agent Does |
|------|---------------|---------------------|
| Run a single-hub ingest | "run ingest for ohio" | Looks up harvest type, selects runbook, runs scripts, verifies output |
| Run the orchestrator | "run orchestrator for february" | Starts orchestrator with correct flags, monitors status, reports progress |
| Check ingest status | "what's the ingest status?" | Reads status files, summarizes per-hub progress |
| Sync data to S3 | "sync ohio to S3" | Runs `s3-sync.sh` with correct profile, verifies completion |
| Debug a failure | "why did georgia fail?" | Reads logs, identifies failure stage, suggests remediation |
| Send ingest email | "send email for ohio" | Reads contacts from i3.conf, generates summary, sends via SES |
| Verify ingest output | "verify the ohio ingest" | Checks `_SUCCESS` markers, reads `_MANIFEST`, compares to previous run |
| Add or modify a script | "create a script to check harvest freshness" | Follows POSIX bash conventions, uses `common.sh`, documents in SCRIPTS.md |

### sparkindexer (No Documented Procedures Yet)

sparkindexer has no skills, no rules, and no AGENTS.md. Anyone operating it -- human or coding assistant -- must be given explicit instructions each time. Part of this plan (Phase 3) is to create that documentation so sparkindexer can be operated from written procedures.

### ingest-wikimedia (No Documented Procedures Yet)

The same gap exists for ingest-wikimedia. Checking upload status, restarting a stalled uploader, and refreshing ID lists all require engineering knowledge that is not yet written down. Part of this plan (Phase 4) is to document these procedures. Without them, the Wikimedia stage is the most fragile to hand off.

---

## Proposed Procedures for sparkindexer

These procedures are created in Phase 3 (P3-9, P3-10) of the implementation roadmap. Once in place, anyone following them can build and deploy the indexer, launch the EMR cluster, validate the new index, and flip the production alias -- without needing to understand the internals of sparkindexer.

### sparkindexer-build-and-upload

**When to use:** Before launching an EMR indexing job, or when sparkindexer code has changed.

**Steps:**
1. Verify Java 11+ is available (check `JAVA_HOME`)
2. Run `sbt assembly` in the sparkindexer directory
3. Verify JAR created: `target/scala-2.12/sparkindexer-assembly.jar`
4. Upload to S3: `aws s3 cp ... s3://dpla-sparkindexer/ --profile dpla`
5. Verify upload: `aws s3 ls s3://dpla-sparkindexer/sparkindexer-assembly.jar --profile dpla`

**Failure modes:** sbt build failure (Java version, dependencies), S3 upload failure (credentials, network).

### sparkindexer-launch-emr

**When to use:** After all hubs are ingested and synced to S3, and the JAR is uploaded.

**Gate (Gate A):** Before launch, verify:
- JAR exists in S3
- Scheduled hubs have `_SUCCESS` markers in their S3 JSONL directories
- EMR config file exists and is valid

**Steps:**
1. Generate timestamped index name: `dpla-all-{yyyyMMdd-HHmmss}`
2. Launch EMR cluster from `emr-config.json`
3. Record cluster ID and step ID
4. Poll `aws emr describe-step` every 60 seconds
5. On COMPLETED: report success and new index name
6. On FAILED: report failure with EMR log details
7. Verify cluster terminated (cost protection)

### sparkindexer-check-index

**When to use:** After indexing completes, before flipping the alias.

**Steps:**
1. Query ES: `GET /{index-name}/_stats` -- report document count, size, shard status
2. Compare document count to current production index (via `dpla_alias`)
3. Flag if count differs by more than 2%
4. Spot-check: query 10 known DPLA IDs, verify expected fields
5. Check cluster health: `GET /_cluster/health`

### sparkindexer-flip-alias

**When to use:** After check-index passes all validations.

**Gate (Gate B):** All preconditions from [Gate B: Pre-Flip Gate](03-integration-contracts-and-gates.md#gate-b-pre-flip-gate-critical) must pass.

**Steps:**
1. Run validation (calls check-index internally)
2. If validation fails: stop and report. Do not flip.
3. Perform dry-run: log what would change without executing
4. In controlled mode: wait for human approval. In automated mode: proceed if all gates pass.
5. Execute alias flip via ES `_aliases` API (atomic remove + add)
6. Post-flip verification (Gate C): query API, verify results from new index
7. If post-flip check fails: automatic rollback within 5 minutes

### sparkindexer-delete-old-indices

**When to use:** Periodically, to clean up old indices.

**Steps:**
1. List all `dpla-all-*` indices
2. Identify current alias target (never delete)
3. Identify previous alias target (keep for rollback)
4. Propose deletion of others older than 7 days
5. Require explicit confirmation before deleting

### sparkindexer-debug

**When to use:** When an EMR job or indexing operation fails.

**Steps:**
1. Check EMR step status and error message
2. Read Spark driver logs from S3 (`s3://dpla-sparkindexer/logs/`)
3. Match against known patterns: S3 access denied, ES connection refused, out of memory, input not found
4. Suggest remediation

---

## Proposed Procedures for ingest-wikimedia

These procedures are created in Phase 4 (P4-8, P4-9) of the implementation roadmap. Once in place, anyone following them can check upload progress, restart stalled processes, and trigger the post-index ID refresh -- without SSH access to the server or knowledge of the process management setup.

### wikimedia-check-status

**When to use:** To check the current state of all Wikimedia upload processes.

**Steps:**
1. Check running processes: `ps aux | grep -E 'uploader|downloader'`
2. For each process: identify partner, read last 100 log lines, parse counts, check last activity timestamp
3. Flag stalled processes (no activity > 1 hour)
4. Report per-partner summary
5. Check disk usage, flag if > 80%

### wikimedia-restart-partner

**When to use:** When an uploader has stalled or crashed.

**Steps:**
1. If process exists: send SIGTERM (graceful shutdown, writes checkpoint)
2. Wait 30 seconds, then SIGKILL if needed
3. Verify checkpoint file exists
4. Restart via systemd: `systemctl restart wikimedia-uploader@{partner}`
5. Wait 30 seconds, verify process is running and producing output

### wikimedia-refresh-ids

**When to use:** After an alias flip, to generate fresh ID lists.

**Gate (Gate D):** Verify alias flip is confirmed, API is healthy, provider mapping is valid.

**Steps:**
1. For each eligible partner: run `get-ids-api {partner}`
2. Report count of IDs found
3. Compare to previous ID lists (flag significant changes)

### wikimedia-download-partner / wikimedia-upload-partner

**When to use:** To start or restart download/upload for a specific partner.

**Steps:**
1. Verify prerequisites (ID list, config, credentials)
2. Start via systemd
3. Monitor log for first 60 seconds to verify progress
4. Report initial status

### wikimedia-daily-digest

**When to use:** Automatically via cron (daily 9am ET), or on demand.

**Steps:**
1. For each partner with an active process: count uploads/skips/failures in last 24 hours, calculate rate, estimate completion
2. Check for stalled processes
3. Check disk and memory usage
4. Format and post Slack summary

### wikimedia-ec2-health

**When to use:** To check EC2 instance health.

**Steps:**
1. Check disk (`df -h`), flag if > 80%
2. Check memory (`free -m`), flag if < 500MB
3. List running processes
4. Check systemd service status
5. Verify cron is installed and digest job is scheduled

---

## Cross-Project Procedures (Pipeline Coordinator)

These procedures live in the pipeline coordinator project created in Phase 3. They are the top-level operating procedures -- the ones an engineer or coding assistant uses to run and monitor the full monthly cycle.

### pipeline-monthly-cycle

**When to use:** At the start of each monthly cycle.

**Steps:**
1. Read schedule from i3.conf
2. Start ingestion3 orchestrator
3. Monitor progress
4. When all hubs complete: check Gate A, trigger indexer
5. When indexing completes: run Gate B validation
6. If validation passes: execute alias flip with Gate C rollback
7. After flip: run Gate D, trigger wikimedia refresh
8. Post cycle summary to Slack

**Human gates:** Hub failure decisions, alias flip validation failures.

### pipeline-status

**When to use:** Anytime, for unified pipeline view.

**Output:**
- Ingestion: X/Y hubs complete, Z failed
- Indexing: not running / running / last completed {date}
- Search: index name, document count
- Wikimedia: N active uploaders, M stalled

---

## End-to-End Pipeline Flow

The diagram below shows how the monthly cycle flows when all procedures are in place. Human approval gates are shown explicitly -- these are the points where automation stops and waits for a decision.

```
START: "Run the monthly cycle for February"
  |
  v
STAGE 1: INGESTION (AUTO, ~1 week)
  |-- Hubs processed 3 at a time        [Slack: per-stage updates]
  |-- Failed hubs: Slack alert
  v
GATE A: Hub completion policy met?
  |-- YES: proceed
  |-- SOME FAILED: human decides
  v
STAGE 2: INDEXING (AUTO, ~2 hours)
  |-- Build JAR, upload, launch EMR     [Slack: "EMR started"]
  |-- Monitor until completion          [Slack: "EMR complete"]
  v
GATE B: Pre-flip validation passed?
  |-- YES: proceed to flip
  |-- NO: Slack alert, human investigates
  v
ALIAS FLIP (AUTO with rollback)
  |-- Dry-run, then execute             [Slack: "Alias flipped"]
  v
GATE C: Post-flip healthy?
  |-- YES: proceed
  |-- NO: automatic rollback            [Slack: "ROLLBACK"]
  v
GATE D: Wikimedia refresh preconditions met?
  |-- YES: proceed
  |-- NO: Slack alert
  v
STAGE 3: WIKIMEDIA (AUTO, continuous)
  |-- Refresh ID lists                  [Slack: "IDs refreshed"]
  |-- Restart uploaders
  |-- Daily digest                      [Slack: daily 9am ET]
  |-- Stall detection + auto-restart
  v
DONE: Cycle complete.
  [Slack: "February cycle complete. X hubs, Y records indexed, Z files uploaded."]
```

### Where Human Judgment Is Always Required

Documented procedures and automation handle the repeatable work. The following categories require human judgment and should never be delegated to an automated process:

1. **Hub failure triage:** When a hub fails, the decision to proceed without it affects what users see on dp.la. That is a judgment call about mission, not a technical check.
2. **Index validation anomalies:** If the new index differs significantly from production in record count or content, a human must investigate before the alias is flipped.
3. **Repeated Wikimedia stalls:** Recurring stalls usually indicate a problem with the source institution's server or media files. These require coordination with the partner, not just a restart.
4. **New partner onboarding:** Adding a new hub involves configuration, mapping, and coordination decisions that require domain knowledge.
5. **Cost anomalies:** An EMR cluster running longer than expected may indicate a runaway job. A human should review before incurring additional cost.

---

## Implementing Procedures: Structure and Location

### Skill File Structure

```markdown
# Skill Name

**When to use:** [trigger phrases and conditions]

## Prerequisites
- [environment requirements]
- [configuration requirements]

## Steps
1. [step with exact command]
2. [next step]

## Gates
- [mandatory checks before proceeding]

## Verification
- [how to check it worked]

## Troubleshooting
- [common failure] -> [fix]
```

### File Locations

| Platform | Location |
|----------|----------|
| Cursor skill | `{repo}/.cursor/skills/{skill-name}/SKILL.md` |
| Claude Code rule | `{repo}/.claude/rules/{rule-name}.md` |
| Claude Code skill | `{repo}/.claude/skills/{skill-name}/SKILL.md` |
| Agent guide | `{repo}/AGENTS.md` |

For in-repo examples and external documentation (Cursor, Claude Code), see [Where procedures live in this codebase](#where-procedures-live-in-this-codebase) above.

### Testing Skills

Every skill should be testable in dry-run mode:
1. Run with `--dry-run` to verify commands are correct
2. Run on a non-production target if possible
3. Have a human review the first real execution
4. Verify Slack notifications go to a test channel first
