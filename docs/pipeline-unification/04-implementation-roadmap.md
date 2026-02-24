# DPLA Pipeline Unification -- Implementation Roadmap

**Audience:** Engineers performing implementation, AI agents executing changes

**Reading time:** 45 minutes

**Context:** This is the work plan. Each step is discrete and independently testable. The phases correspond to maturity levels described below. Complete one phase's exit criteria before starting the next -- later phases build on assumptions established in earlier ones.

---

## Contents

- [How to Use This Document](#how-to-use-this-document)
- [Maturity Model](#maturity-model)
- [Phase 0: Integration Contracts and Documentation](#phase-0-integration-contracts-and-documentation)
  - [Steps](#steps)
  - [Exit Criteria for Phase 0](#exit-criteria-for-phase-0)
- [Phase 1: Foundational Reliability Fixes](#phase-1-foundational-reliability-fixes)
  - [ingestion3 Fixes](#ingestion3-fixes)
  - [sparkindexer Fixes](#sparkindexer-fixes)
  - [ingest-wikimedia Fixes](#ingest-wikimedia-fixes)
  - [Exit Criteria for Phase 1](#exit-criteria-for-phase-1)
- [Phase 2: Monitoring and Observability](#phase-2-monitoring-and-observability)
  - [Steps](#steps-1)
  - [Exit Criteria for Phase 2](#exit-criteria-for-phase-2)
- [Phase 3: Controlled Indexer Automation](#phase-3-controlled-indexer-automation)
  - [Steps](#steps-2)
  - [Controlled Execution Protocol](#controlled-execution-protocol)
  - [Exit Criteria for Phase 3](#exit-criteria-for-phase-3)
- [Phase 4: End-to-End Coordination](#phase-4-end-to-end-coordination)
  - [Steps](#steps-3)
  - [Exit Criteria for Phase 4](#exit-criteria-for-phase-4)
- [Dependency Graph](#dependency-graph)
- [For Agents: How to Work Through This Plan](#for-agents-how-to-work-through-this-plan)
- [Effort Summary](#effort-summary)
- [Graduation Protocol: When to Increase Automation](#graduation-protocol-when-to-increase-automation)

---

## How to Use This Document

This roadmap breaks the pipeline unification into discrete, independently testable steps organized into five phases. Each phase delivers standalone value. Each step within a phase has clear inputs, outputs, and verification criteria.

**For a human engineer:** Use this as a work tracking document. Mark steps as done as you complete them. The phases correspond roughly to weeks of calendar time, but pace depends on resource availability.

**For an AI agent implementing this plan:** Work through steps in order within each phase. Verify exit criteria before moving to the next phase. Each step references specific findings in [Technical Findings](05-technical-findings.md) where applicable. Do not skip phases -- later phases depend on earlier ones.

---

## Maturity Model

The phases map to a maturity progression. Understanding this helps you decide how far to go:

| Maturity Level | What It Means | Phases |
|----------------|---------------|--------|
| **Level 0: Documented** | Contracts written down, integration rules explicit | Phase 0 |
| **Level 1: Observable** | Failures detected quickly, reliability improved, monitoring in place | Phases 1--2 |
| **Level 2: Controlled** | Stage transitions automated with mandatory human approval at critical gates | Phase 3 |
| **Level 3: Coordinated** | End-to-end pipeline runs with minimal human intervention; humans approve only at high-stakes gates | Phase 4 |

Each level is independently valuable. Stopping at Level 1 (observable) is far better than the current state. Stopping at Level 2 (controlled) is sufficient for most operational needs.

---

## Phase 0: Integration Contracts and Documentation

**Goal:** Write down the rules that are currently implicit. No system changes -- only documentation and configuration.
**Duration:** ~1 week (can overlap with Phase 1 start)
**Risk:** None.

### Steps

| ID | Step | Depends On | Estimate | Verification |
|----|------|-----------|----------|--------------|
| P0-1 | Create this document suite | -- | Done | Documents exist and are reviewed |
| P0-2 | Create `hub-registry.json` with all hub name mappings | -- | 2 hours | Every hub in `i3.conf` has an entry; every entry has short_name, s3_prefix, display_name |
| P0-3 | Document S3 path format contract | -- | 1 hour | Contract matches actual S3 contents for 5+ hubs |
| P0-4 | Document DPLA API field contract | -- | 1 hour | Contract matches actual API response for 5+ items |
| P0-5 | Standardize `institutions_v2.json` branch references | Finding XP-H1 | 30 min | All GitHub raw URLs in ingest-wikimedia reference `main` branch |
| P0-6 | Add all required env vars to `.env.example` | Finding I3-M4 | 30 min | `.env.example` includes `DPLA_DATA`, `I3_CONF`, `I3_HOME` with comments |
| P0-7 | Remove hardcoded default paths from `config.py` | Finding I3-H4 | 30 min | `config.py` fails with clear error when `I3_HOME`/`I3_CONF` are not set |

### Exit Criteria for Phase 0

- [ ] `hub-registry.json` exists and covers all active hubs
- [ ] Integration contracts documented in `03-integration-contracts-and-gates.md`
- [ ] All `institutions_v2.json` references point to `main` branch
- [ ] `.env.example` is complete
- [ ] No hardcoded developer-specific paths in `config.py`

---

## Phase 1: Foundational Reliability Fixes

**Goal:** Fix the most dangerous silent failure patterns across all three systems. No new features -- only making existing code safer and more honest about failures.
**Duration:** ~2 weeks
**Risk:** Low. These are defensive changes. Each can be tested independently.

### ingestion3 Fixes

| ID | Step | Finding | Estimate | Verification |
|----|------|---------|----------|--------------|
| P1-1 | Atomic state file writes | I3-C1 | 2 hours | Kill orchestrator mid-write; restart; state file is intact |
| P1-2 | Division-by-zero guard in anomaly detector | I3-C2 | 30 min | Run anomaly check for a hub with no baseline; no crash |
| P1-3 | Sanitize hub names in shell commands | I3-C3 | 30 min | `subprocess.run()` uses list args, not `shell=True` |
| P1-4 | Thread-safe state updates for parallel execution | I3-H1 | 2 hours | Run 3 hubs in parallel; state file is consistent after all complete |
| P1-5 | Post-sync S3 verification | I3-H2 | 2 hours | After sync, verify `_SUCCESS` and `_MANIFEST` exist in S3; fail if missing |
| P1-6 | JAR build lock for parallel runs | I3-H3 | 1 hour | Start 3 ingests simultaneously; only one sbt build runs |
| P1-7 | Fix OAI iterator exception type | I3-M2 | 15 min | Iterator throws `NoSuchElementException` when exhausted |

### sparkindexer Fixes

| ID | Step | Finding | Estimate | Verification |
|----|------|---------|----------|--------------|
| P1-8 | Propagate ES write failures (exit non-zero) | SI-C1 | 2 hours | Simulate ES write failure; EMR step reports FAILED |
| P1-9 | Fix IndexerMain exit codes | SI-C2 | 1 hour | Any unhandled exception causes `System.exit(1)` |
| P1-10 | Extract hardcoded AWS IDs to config file | SI-C3 | 2 hours | `emr-config.json` exists; `sparkindexer-emr.sh` reads from it |
| P1-11 | Fix MasterDataset timestamp handling | SI-H1 | 1 hour | `maxTimestamp="now"` handled explicitly, not by string sort accident |
| P1-12 | Add pre-flight health checks before indexing | SI-H2 | 2 hours | Indexer verifies ES reachable, S3 has data, index name available |
| P1-13 | Fix alias flip race condition | SI-H3 | 1 hour | Flip script uses conditional `_aliases` API with expected current target |
| P1-14 | Fix resource leak in Index.scala | SI-H4 | 30 min | `getResourceAsStream` uses try-with-resources pattern |

### ingest-wikimedia Fixes

| ID | Step | Finding | Estimate | Verification |
|----|------|---------|----------|--------------|
| P1-15 | Add checkpoint/resume capability | WM-C2 | 4 hours | Kill uploader; restart; resumes from last checkpoint (not beginning) |
| P1-16 | Add pywikibot session health check | WM-C1 | 2 hours | Session re-authenticates after timeout; uploads continue |
| P1-17 | Add signal handler for graceful shutdown | WM-C3 | 1 hour | SIGTERM saves checkpoint and exits cleanly |
| P1-18 | Add structured error logging (failures.jsonl) | WM-H1 | 2 hours | Failed items written to `{partner}/logs/failures.jsonl` with error details |
| P1-19 | Periodic tracker state flush | WM-H2 | 1 hour | Tracker writes state to disk every 100 items; survives crash |
| P1-20 | Add config.toml error handling | WM-M4 | 30 min | Missing or malformed config.toml gives clear error message |

### Exit Criteria for Phase 1

- [ ] Orchestrator state survives a mid-write crash (P1-1)
- [ ] Anomaly detector handles first-ever hub and zero baselines (P1-2)
- [ ] No `shell=True` with user-derived input (P1-3)
- [ ] Parallel hub processing produces consistent state (P1-4)
- [ ] S3 sync verified after completion (P1-5)
- [ ] sparkindexer exits non-zero on ES write failures (P1-8, P1-9)
- [ ] EMR config is externalized, not hardcoded (P1-10)
- [ ] Wikimedia uploader resumes from checkpoint (P1-15)
- [ ] Wikimedia uploader re-authenticates on session timeout (P1-16)
- [ ] All changes have tests (unit or integration)

---

## Phase 2: Monitoring and Observability

**Goal:** Make the pipeline's state visible. Detect failures in minutes, not months. This is the phase most directly visible to non-engineering staff -- the daily Slack digest is the primary deliverable.
**Duration:** ~1 week
**Risk:** Low. Monitoring is read-only and non-destructive.

### Steps

| ID | Step | Depends On | Estimate | Verification |
|----|------|-----------|----------|--------------|
| P2-1 | Daily Wikimedia Slack digest | P1-15 (checkpoint gives structured state) | 4 hours | Digest posts to Slack at 9am ET with per-partner stats |
| P2-2 | Wikimedia stall detection | P2-1 | 1 hour | Process with no log activity for >1 hour flagged as stalled |
| P2-3 | Install cron on Wikimedia EC2 | -- | 30 min | `crontab -l` shows digest job |
| P2-4 | Set up log rotation on EC2 | Finding WM-H3 | 1 hour | logrotate config installed; logs rotate daily, 14-day retention |
| P2-5 | Create systemd services for uploaders/downloaders | -- | 3 hours | `systemctl status wikimedia-uploader@nara` shows running |
| P2-6 | Document EC2 rebuild procedure | -- | 2 hours | Step-by-step guide to provision and configure a fresh EC2 |
| P2-7 | Add CloudWatch agent for EC2 disk/memory | -- | 1 hour | CloudWatch dashboard shows disk% and available memory |
| P2-8 | Per-hub anomaly threshold overrides in i3.conf | Finding I3-M1 | 2 hours | Hub with custom threshold uses it; others use global default |
| P2-9 | Increase notification script timeout | Finding I3-M3 | 30 min | Timeout is 30s (up from 15s); timeout events are logged |

### Exit Criteria for Phase 2

- [ ] Daily Slack digest runs reliably for 7+ consecutive days
- [ ] Stalled processes detected within 1 hour of stalling
- [ ] EC2 has systemd services, log rotation, and CloudWatch monitoring
- [ ] EC2 rebuild procedure tested (can provision a fresh instance from the guide)
- [ ] At least one stalled process detected and reported by the new monitoring (can be simulated)

---

## Phase 3: Controlled Indexer Automation

**Goal:** Automate the indexer launch and alias flip with mandatory safety gates. Every high-stakes decision requires human approval. The alias flip -- which determines what millions of users see on dp.la -- must be validated before execution and reversible after.
**Duration:** ~2 weeks
**Risk:** Medium. Mitigated by pre-flip validation, dry-run mode, and automatic rollback.

### Steps

| ID | Step | Depends On | Estimate | Verification |
|----|------|-----------|----------|--------------|
| P3-1 | Pipeline coordinator scaffold | -- | 2 hours | Python project with config, notification module, CLI entry point |
| P3-2 | Shared notification module (Slack + SNS) | -- | 2 hours | Can send Slack messages and (optionally) publish to SNS |
| P3-3 | EMR launch automation (boto3) | P1-10 (config) | 4 hours | Can launch EMR cluster from config; returns cluster ID |
| P3-4 | EMR step monitoring (poll until terminal) | P3-3 | 2 hours | Polls every 60s; reports COMPLETED or FAILED with details |
| P3-5 | Pre-flip validation suite (Gate B) | P1-8, P1-9 | 4 hours | Checks count, health, sample queries; returns PASS/FAIL with evidence |
| P3-6 | Alias flip with dry-run mode | P1-13, P3-5 | 3 hours | Dry-run logs what would change; execute mode flips with confirmation |
| P3-7 | Post-flip health check with rollback (Gate C) | P3-6 | 2 hours | Canary queries run; automatic rollback on failure |
| P3-8 | CloudWatch alarm for EMR cost protection | -- | 1 hour | Alarm triggers if EMR runs >4 hours; SNS notification |
| P3-9 | Create sparkindexer AGENTS.md | -- | 2 hours | Agent can read AGENTS.md and understand how to build, deploy, run |
| P3-10 | Create sparkindexer agent skills | P3-9 | 4 hours | Skills for build, launch-emr, check-index, flip-alias, debug |

### Controlled Execution Protocol

During Phase 3, the indexer automation runs in **controlled mode**: every gate requires explicit human approval before proceeding. This means:

1. Automation launches EMR and monitors completion.
2. Pre-flip validation runs automatically and reports results.
3. **Human reviews the validation report and approves or blocks the flip.**
4. If approved, automation executes the flip and runs post-flip checks.
5. Post-flip rollback is automatic (no human needed -- speed matters here).

After at least two successful monthly cycles with controlled execution, the team can consider moving Gate B to automated approval (with documented confidence thresholds).

### Exit Criteria for Phase 3

- [ ] EMR can be launched and monitored via the coordinator (not just shell script)
- [ ] Pre-flip validation catches at least one simulated problem (e.g., low count, missing field)
- [ ] Alias flip works in dry-run and execute modes
- [ ] Post-flip rollback works when canary check is forced to fail
- [ ] At least one full indexing cycle completed with controlled gates
- [ ] sparkindexer has AGENTS.md and agent skills
- [ ] CloudWatch alarm tested (simulate long EMR run)

---

## Phase 4: End-to-End Coordination

**Goal:** Connect all three stages so that completing one automatically triggers the next. By this phase, the reliability and monitoring work of Phases 1--3 is in place -- this is the layer that ties it together. Humans retain approval authority at the highest-stakes gates.
**Duration:** ~2 weeks
**Risk:** Medium. Depends on all previous phases being in place. Each step is independently testable.

### Steps

| ID | Step | Depends On | Estimate | Verification |
|----|------|-----------|----------|--------------|
| P4-1 | Pre-indexer gate (Gate A) | P0-2 (hub registry) | 2 hours | Verifies hub completion in S3; blocks indexer if policy not met |
| P4-2 | Automated ID list refresh after alias flip (Gate D) | P3-7 | 2 hours | After successful flip + canary, refreshes ID lists for all partners |
| P4-3 | Uploader launch/restart via systemd | P2-5 | 2 hours | Coordinator can start/stop/restart uploaders via systemd |
| P4-4 | Stall detection and auto-restart with backoff | P2-2 | 3 hours | Stalled uploader auto-restarts; repeated stalls escalate to human |
| P4-5 | Per-partner progress tracking | P1-15 (checkpoint) | 3 hours | Coordinator reads checkpoint files; reports per-partner status |
| P4-6 | Monthly cycle coordinator | P4-1, P3-6, P4-2 | 4 hours | Single command starts ingestion -> waits -> indexing -> validation -> flip -> wikimedia refresh |
| P4-7 | Unified pipeline status command | P4-5 | 3 hours | One command shows: ingestion progress, indexer state, ES index info, wikimedia upload status |
| P4-8 | Create ingest-wikimedia AGENTS.md | -- | 2 hours | Agent can understand how to check status, restart, refresh IDs |
| P4-9 | Create ingest-wikimedia agent skills | P4-8 | 4 hours | Skills for check-status, restart, refresh-ids, daily-digest, ec2-health |

### Exit Criteria for Phase 4

- [ ] Full monthly cycle can be triggered with a single command
- [ ] Pre-indexer gate blocks when hubs are incomplete
- [ ] Wikimedia ID refresh happens automatically after alias flip
- [ ] Stalled uploaders detected and auto-restarted
- [ ] Pipeline status command provides unified view across all stages
- [ ] ingest-wikimedia has AGENTS.md and agent skills
- [ ] At least one full end-to-end cycle completed with minimal human intervention
- [ ] Manual override remains available at every gate

---

## Dependency Graph

The graph below shows which steps must be complete before others can begin. Phases 1 and 2 have significant overlap -- reliability work on each system is independent of reliability work on the others, so these can run in parallel.

```
Phase 0: Contracts
  P0-2 (hub registry) ──────────────────────────────────────────────> P4-1 (pre-indexer gate)
  P0-5 (branch refs) ──> Phase 1 can begin
  P0-6, P0-7 (env) ──> Phase 1 can begin

Phase 1: Reliability                         Phase 2: Monitoring
  P1-1..P1-7 (ingestion3) ─────────────────> P2-8, P2-9 (ingestion3 monitoring)
  P1-8..P1-14 (sparkindexer) ──────────────> P3-3..P3-10 (indexer automation)
  P1-15..P1-20 (wikimedia) ────────────────> P2-1..P2-7 (wikimedia monitoring)

Phase 3: Controlled Indexer                  Phase 4: Coordination
  P3-3 (EMR launch) ──> P3-4 (monitoring)
  P3-5 (validation) ──> P3-6 (flip)
  P3-6 (flip) ──> P3-7 (rollback) ─────────> P4-2 (wiki refresh)
  P3-7 (rollback) ──────────────────────────> P4-6 (monthly cycle)
```

---

## For Agents: How to Work Through This Plan

If you are an AI agent implementing this roadmap:

1. **Start with Phase 0.** These are documentation and configuration tasks. Low risk, high value.

2. **Within Phase 1, work by repository.** Pick one repo (e.g., ingestion3), complete all its Phase 1 steps, verify with tests, then move to the next repo. This minimizes context switching.

3. **For each step:**
   - Read the referenced finding in [Technical Findings](05-technical-findings.md) for file paths and proposed fix
   - Implement the fix
   - Write or update tests
   - Verify the step's criteria
   - Update the step status in this document

4. **Before moving to the next phase,** verify all exit criteria for the current phase. If any criterion is not met, address it before proceeding.

5. **When making cross-project changes,** check the contracts in [Integration Contracts and Gates](03-integration-contracts-and-gates.md). If your change affects any contract item, update the contract and verify all consumers.

6. **Test conservatively.** Use dry-run modes. Validate before flipping. Keep rollback options. The goal is stability over speed.

---

## Effort Summary

| Phase | Steps | Estimated Hours | Calendar Time |
|-------|-------|-----------------|---------------|
| Phase 0: Contracts | 7 | ~7 hours | 1 week |
| Phase 1: Reliability | 20 | ~28 hours | 2 weeks |
| Phase 2: Monitoring | 9 | ~15 hours | 1 week |
| Phase 3: Indexer | 10 | ~28 hours | 2 weeks |
| Phase 4: Coordination | 9 | ~25 hours | 2 weeks |
| **Total** | **55** | **~103 hours** | **~8 weeks** |

With AI assistance on implementation tasks, calendar time can be compressed significantly -- roughly 40--60% reduction for coding and configuration work. Testing and validation against real infrastructure compress less; plan the full calendar time for those steps.

---

## Graduation Protocol: When to Increase Automation

Moving from one maturity level to the next should be based on evidence, not schedule. Rushing graduation means building automation on top of uncertainty -- which is how quiet failures become large ones. These are the graduation criteria:

### Level 0 -> Level 1 (Documented -> Observable)

**Prerequisite:** All Phase 0 exit criteria met.
**Graduate when:** Phase 1 and Phase 2 exit criteria are met. No remaining CRITICAL findings in the open state.

### Level 1 -> Level 2 (Observable -> Controlled)

**Prerequisite:** All Phase 2 exit criteria met. Monitoring has been running for at least one full monthly cycle.
**Graduate when:**
- At least two consecutive monthly cycles completed with no missing status coverage for critical transitions
- No unresolved contract-mapping failures for hub naming or S3 path resolution
- Daily Slack digest has been running reliably for 30+ days
- All Phase 3 exit criteria met

### Level 2 -> Level 3 (Controlled -> Coordinated)

**Prerequisite:** All Phase 3 exit criteria met. Controlled execution has been used for at least two monthly cycles.
**Graduate when:**
- At least two full monthly cycles completed with controlled gates and no manual sequencing gaps
- All gate outputs include pass/fail evidence and remediation hints
- No gate failures required human override to fix an automation bug (as opposed to a genuine data issue)
- Team is confident in automatic post-flip rollback

### Moving Gates from Human-Approval to Automatic

Gate B (pre-flip validation) is the highest-stakes gate. It should remain human-approval for at least 3 monthly cycles. After that, consider automatic approval **only if:**
- All 3+ previous cycles passed Gate B on the first attempt
- The validation suite has caught at least one real issue (proving it works)
- Automatic rollback (Gate C) has been tested successfully
- The team has documented confidence thresholds for each check
