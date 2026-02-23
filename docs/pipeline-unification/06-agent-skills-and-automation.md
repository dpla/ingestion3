# DPLA Pipeline Unification -- Agent Skills and Automation

**Audience:** Engineers, AI agents, operations staff at receiving organization
**Reading time:** 30 minutes

---

## How AI Agents Fit Into This Pipeline

This document describes how AI coding agents can operate the DPLA data pipeline with minimal human intervention. It covers what agents can do today, what new capabilities are proposed, and how skills should be implemented.

### Key Concepts

**Agent (AI agent):** Software that reads instructions and executes tasks. In DPLA's context, agents run inside Cursor, Claude Code, or Warp. You describe what to do in plain English, and the agent figures out the commands and runs them.

**Skill:** A procedure file (`SKILL.md`) that tells an agent how to perform a specific task. Skills are both documentation that humans can read and instructions that agents can follow.

**Rule:** Background context that is always active (e.g., "always use `--profile dpla` for AWS commands"). Rules are loaded automatically.

**Gate:** A mandatory checkpoint in a skill where the agent must verify preconditions before proceeding. If the check fails, the agent stops and reports -- it does not attempt to work around the failure.

---

## What Agents Can Do Today

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

### sparkindexer (No Agent Infrastructure)

No skills, no rules, no AGENTS.md. An agent must be told exactly what to do.

### ingest-wikimedia (No Agent Infrastructure)

Same situation. An agent cannot check upload status, restart a stalled uploader, or refresh ID lists without explicit commands from a human.

---

## Proposed Agent Skills for sparkindexer

These skills are created in Phase 3 (P3-9, P3-10) of the implementation roadmap.

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

**Gate (Gate B):** All preconditions from [03-integration-contracts-and-gates.md](03-integration-contracts-and-gates.md) must pass.

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

## Proposed Agent Skills for ingest-wikimedia

These skills are created in Phase 4 (P4-8, P4-9) of the implementation roadmap.

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

## Cross-Project Skills (Pipeline Coordinator)

These skills live in the pipeline coordinator project (created in Phase 3).

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

## End-to-End Agent Pipeline

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

### Where Human Judgment Is Still Required

1. **Hub failure triage:** When a hub fails, should the cycle continue without it?
2. **Index validation failure:** If the new index is significantly different, a human must investigate.
3. **Repeated wikimedia stalls:** Source institution issues require human investigation.
4. **New partner onboarding:** Configuration changes require human judgment.
5. **Cost anomalies:** EMR running longer than expected needs human review.

---

## Implementing Skills: Structure and Location

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

### Testing Skills

Every skill should be testable in dry-run mode:
1. Run with `--dry-run` to verify commands are correct
2. Run on a non-production target if possible
3. Have a human review the first real execution
4. Verify Slack notifications go to a test channel first
