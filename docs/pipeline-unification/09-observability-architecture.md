# DPLA Pipeline Unification -- Observability and Monitoring Architecture

**Audience:** Director, Community Manager, Engineers, leadership at receiving organization

**Reading time:** 25 minutes (main document); appendices are engineer reference

**Status:** Proposal -- ready for review

**Context:** This document proposes the monitoring and notification layer that makes the pipeline visible to everyone with a stake in it -- engineers, community managers, hub contacts, and leadership. It describes what changes for each audience and what the proposed infrastructure costs.

---

## Contents

- [Why This Document Exists](#why-this-document-exists)
- [What Changes](#what-changes)
  - [For Hub Contacts](#for-hub-contacts)
  - [For Community Managers and Leadership](#for-community-managers-and-leadership)
  - [For Engineers](#for-engineers)
- [The Cost of Not Seeing](#the-cost-of-not-seeing)
  - [What the Proposed Architecture Costs](#what-the-proposed-architecture-costs)
- [Design Principles](#design-principles)
- [Implementation Phases](#implementation-phases)
  - [Phase 1: Structured Logging](#phase-1-structured-logging-1-2-weeks-0month)
  - [Phase 2: CloudWatch and Notifications](#phase-2-cloudwatch-and-notifications-2-weeks-7month)
  - [Phase 3: Visual Dashboard](#phase-3-visual-dashboard-1-week-3month)
  - [Phase 4: Slack Integration](#phase-4-slack-integration-2-3-weeks-2month)
- [Slack Channels and Commands](#slack-channels-and-commands)
  - [Channel Structure](#channel-structure)
  - [Slash Commands](#slash-commands)
  - [Scheduled Digests](#scheduled-digests)
- [Current Architecture vs. Greenfield: Investment Strategy](#current-architecture-vs-greenfield-investment-strategy)
  - [Build now -- carries forward completely](#build-now----carries-forward-completely)
  - [Build now with awareness -- minor rework later](#build-now-with-awareness----minor-rework-later)
  - [Wait for greenfield -- built-in to Fargate and Step Functions](#wait-for-greenfield----built-in-to-fargate-and-step-functions)
  - [Migration Path](#migration-path)
- [Relationship to Pipeline Unification Phases](#relationship-to-pipeline-unification-phases)
- [Summary](#summary)
- [Appendix A: Structured Logging Specification](#appendix-a-structured-logging-specification)
- [Appendix B: CloudWatch Configuration](#appendix-b-cloudwatch-configuration)
- [Appendix C: CloudWatch Logs Insights Query Examples](#appendix-c-cloudwatch-logs-insights-query-examples)
- [Appendix D: Slack Slash Command Architecture](#appendix-d-slack-slash-command-architecture)
- [Appendix E: Dashboard Widget Specification](#appendix-e-dashboard-widget-specification)
- [Appendix F: Greenfield-Only AWS Capabilities](#appendix-f-greenfield-only-aws-capabilities)

---

## Why This Document Exists

Throughout the pipeline unification process, one theme has surfaced repeatedly: **the pipeline's biggest operational risk is not that things break -- it's that breakages go undetected.** The Minnesota Wikimedia incident (two months of silent failure, discovered only by SSH-ing into a server) is not an anomaly. It is the default outcome of a system with no centralized monitoring.

Today, the only people who can answer "is the pipeline healthy?" are the engineers who built it, and even they must manually check log files, S3 listings, and terminal sessions to assemble an answer. Hub contacts who want to know "when was my data last ingested?" must email DPLA and wait for a human to look it up. Community managers preparing board updates must ask engineering for status. There is no self-service visibility for anyone.

This document proposes a monitoring and notification architecture that makes the pipeline **observable by everyone who has a stake in it** -- engineers, community managers, hub contacts, and leadership -- using AWS services DPLA already pays for and Slack as the universal interface.

---

## What Changes

### For Hub Contacts

Today, a hub contact who wants to know about their ingest must send an email to `tech@dp.la` and wait for someone to check. After this work:

- Type `/dpla status florida` in Slack and get an immediate answer: last ingest date, record counts, success rate, whether data is staged for the next index build.
- Watch `#dpla-ingest-status` to see when their hub's ingest completes, without asking anyone.
- Type `/dpla history florida` to see six months of ingest results at a glance.
- Ask `/dpla schedule` to see whether their hub is on this month's schedule and what its current status is.

### For Community Managers and Leadership

Today, getting a pipeline status update requires asking an engineer. After this work:

- A weekly digest arrives in `#dpla-ingest-status` every Monday: "34 of 58 hubs complete, 2 failures, on track for March 1 index build."
- `/dpla staged` shows which hubs have new data ready for the next index.
- `/dpla dashboard` returns a shareable URL to a visual status page -- suitable for board presentations or stakeholder check-ins.
- Failures post automatically to `#dpla-ingest-status` in plain English, not log excerpts.

### For Engineers

Today, debugging a pipeline issue means checking local log files, S3 directories, terminal sessions, and orchestrator state files across multiple machines. After this work:

- `/dpla failures` returns recent failures with the stage that failed, error classification, and a remediation hint.
- `/dpla logs florida harvest` pulls the last 20 structured log lines from CloudWatch -- no SSH, no file hunting.
- `/dpla health` runs an infrastructure check: EC2 status, disk usage, EMR state, Elasticsearch health.
- CloudWatch Logs Insights enables ad-hoc queries across the entire pipeline: "show me all errors for Florida in the last 24 hours" or "which stages started but never completed?"
- CloudWatch Alarms detect stalls, timeouts, and anomalies automatically and post to `#tech-alerts` before anyone has to notice.

---

## The Cost of Not Seeing

The observability gaps in the current pipeline have concrete costs. These are not hypothetical -- they reflect actual patterns from the past year of operations.

**Silent failures consume engineering time after the fact.** The Minnesota Wikimedia stall went undetected for two months. Diagnosing, restarting, and verifying the recovery required multiple hours of engineering work that would have been seconds if an alert had fired when the process stalled. Every silent failure follows this pattern: the cost of detection dwarfs the cost of the fix.

**Status inquiries interrupt engineering work.** When a hub contact asks "when was our data last ingested?" or a community manager asks "how is this month's cycle going?", an engineer must stop what they're doing, check S3 listings or log files, and compose a response. At ~60 hubs and several inquiries per month, this is a steady drain on engineering capacity that could be eliminated entirely by self-service status.

**Post-ingest revocations are expensive.** When a hub's data is ingested before it's ready (because there was no structured way to confirm readiness), the correction costs 5--8 hours of engineering time per incident. Visibility into hub status -- both for DPLA staff and for the hubs themselves -- reduces the frequency of these events.

**Lack of visibility erodes trust.** Hub contacts and community managers who cannot see what's happening assume the worst when things are quiet. Proactive status updates -- even "nothing has changed, everything is on track" -- build confidence in the pipeline and reduce the volume of inquiry emails.

### What the Proposed Architecture Costs

The entire observability layer -- CloudWatch Logs, metrics, alarms, dashboard, SNS notifications, Lambda slash commands, API Gateway, DynamoDB state store, and EventBridge scheduled rules -- costs approximately **$12 per month** in AWS services.

| Component | Monthly Cost | Notes |
|-----------|-------------|-------|
| CloudWatch Logs (~2GB/month) | ~$1.06 | Ingestion + retention |
| CloudWatch Custom Metrics (~15) | ~$4.50 | Pipeline health tracking |
| CloudWatch Alarms (~8) | ~$0.80 | Automated failure detection |
| CloudWatch Dashboard | $3.00 | Visual status, shareable URL |
| SNS Topics | $0.00 | Free tier (< 1000 msgs/month) |
| Lambda Functions | ~$1.00 | Slash commands + digests |
| API Gateway | ~$0.50 | Slack command endpoint |
| DynamoDB (on-demand) | ~$1.00 | Pipeline state store |
| EventBridge | $0.00 | Scheduled digest rules |
| **Total** | **~$12/month** | |

For context: one post-ingest revocation costs 5-8 hours of engineering time. One month of "can you check the status of our hub?" emails costs several hours of interruption. The monitoring infrastructure pays for itself in the first week of operation.

---

## Design Principles

Four principles guide this architecture:

1. **No new platforms.** Use AWS services DPLA already has access to (CloudWatch, SNS, S3, Lambda, SES, DynamoDB) and Slack, which the team already uses daily. No Datadog, no PagerDuty, no Grafana Cloud, no new vendor relationships.

2. **Slack as the universal interface.** Engineers get alerts in `#tech-alerts`. Non-engineers get status in `#dpla-ingest-status`. Slash commands let anyone ask questions on demand. Same platform, different channels, appropriate detail for each audience.

3. **Fix the data at the source.** The reason the pipeline is hard to monitor today is that its logs are unstructured text scattered across machines. The first and most important step is making every component emit structured, machine-readable logs with consistent fields. Everything downstream -- dashboards, alarms, slash commands, digests -- flows automatically from well-structured source data.

4. **Every phase stands alone.** Each implementation phase delivers usable value independently. If only Phase 1 is completed, the codebase is still better. If only Phases 1-2 are completed, CloudWatch provides monitoring without the Slack layer. No phase depends on a future phase to be useful.

---

## Implementation Phases

### Phase 1: Structured Logging (1-2 weeks, $0/month)

Every pipeline component switches from unstructured text logging to structured JSON. This is pure code quality work with no infrastructure cost and no new AWS services.

**What changes:** Log output across the Scala pipeline, the Python orchestrator, and the shell scripts gains a consistent JSON format with standardized fields (`timestamp`, `level`, `hub`, `run_id`, `stage`, `message`). Log rotation is configured everywhere to prevent unbounded file growth.

**Why it matters:** Structured logs are the foundation for everything else. CloudWatch Logs Insights can automatically parse JSON. Dashboards can filter by hub or stage. Alarms can trigger on error rates. Slash commands can return formatted log excerpts. None of this is possible with today's unstructured text. Even without any AWS integration, structured logs make local debugging faster -- you can filter by hub, by stage, by error level, using standard tools.

**What it enables:** Phases 2-4 all depend on structured logs. This phase also benefits engineers immediately: `grep` and `jq` work against JSON logs far more effectively than against unstructured text.

### Phase 2: CloudWatch and Notifications (~2 weeks, ~$7/month)

Logs flow to CloudWatch. Custom metrics track pipeline health. Alarms detect failures automatically. SNS provides reliable multi-channel notification delivery.

**What changes:** A CloudWatch agent ships logs from the machines running the pipeline to centralized log groups in AWS. The orchestrator emits custom metrics (records harvested, mapping failure rate, stage duration) to CloudWatch. Alarms fire when thresholds are breached (stage running too long, failure rate too high, Wikimedia uploader stalled, EC2 disk filling up). SNS topics replace the current direct Slack webhook calls, adding retry logic, delivery verification, and the ability to add email or other subscribers without code changes.

**Why it matters:** This is the transition from "check manually" to "get told automatically." An engineer no longer needs to SSH into a server to check if a process is running. A stalled Wikimedia upload triggers an alarm within an hour instead of going undetected for months. A harvest that exceeds its expected duration sends an alert before anyone has to wonder why it's taking so long.

**What it enables:** Phase 3 (dashboard) needs metrics to display. Phase 4 (Slack commands) needs CloudWatch Logs Insights to query. The alarms provide immediate value on their own -- they are the automated version of "someone periodically checks that things are okay."

### Phase 3: Visual Dashboard (~1 week, $3/month)

A single CloudWatch dashboard provides a visual overview of the entire pipeline. Shareable via URL without requiring AWS console access.

**What changes:** A dashboard is created with widgets showing: monthly cycle progress (hubs completed, failed, waiting), active processing status, stage performance (harvest duration, mapping success rate), infrastructure health (EC2 disk, EMR status), and Wikimedia upload progress. The dashboard can be shared as a read-only public URL, suitable for non-engineers.

**Why it matters:** This is the first time anyone -- not just engineers -- can see the pipeline's status at a glance. A community manager can open a URL and see "34 of 58 hubs complete, 2 failed, everything else on track." A director can include a dashboard screenshot in a board presentation. An engineer can spot patterns (which hubs are slow, which stages fail most often) without running queries.

### Phase 4: Slack Integration (2-3 weeks, ~$2/month)

Slash commands, scheduled digests, and a new `#dpla-ingest-status` channel make the pipeline visible to everyone in Slack.

**What changes:** A set of Lambda functions behind API Gateway handle Slack slash commands (`/dpla status`, `/dpla staged`, `/dpla schedule`, `/dpla history`, `/dpla failures`, `/dpla health`, `/dpla dashboard`). EventBridge rules trigger scheduled digests (daily ingest progress during the ingest window, weekly pipeline summary, daily Wikimedia progress). A new `#dpla-ingest-status` channel provides a human-readable, non-technical feed of pipeline activity that hub contacts can be invited to as Slack guests.

**Why it matters:** This is the phase that eliminates "can you check the status of our hub?" emails. Hub contacts, community managers, and leadership get self-service access to pipeline information in a tool they already use. Engineers get quick diagnostic commands that pull from CloudWatch instead of requiring manual log inspection.

---

## Slack Channels and Commands

### Channel Structure

| Channel | Who Watches It | What They See |
|---------|---------------|---------------|
| `#tech-alerts` | Engineers | Failures, critical alarms, safety gate decisions. Technical detail, immediate action needed. |
| `#dpla-ingest-status` | Community Managers, Hub contacts (as Slack guests), Engineers | Hub completions, monthly progress, weekly summaries, plain-English failure notices. Status-oriented, not action-oriented. |
| `#tech` | Engineers | Hub-complete notifications with @here, detailed technical status. Existing channel, enhanced content. |

`#dpla-ingest-status` is the key addition. It provides a feed that a non-technical person can follow to understand what the pipeline is doing without needing to interpret log output or ask an engineer. Hub contacts can be invited as single-channel Slack guests at no additional Slack cost.

### Slash Commands

**For anyone (hub contacts, community managers, engineers):**

| Command | What You Learn |
|---------|---------------|
| `/dpla status <hub>` | Last ingest date, record counts, success rate, whether data is staged for next index build |
| `/dpla staged` | Which hubs have new data staged this month |
| `/dpla schedule` | This month's schedule: who's ready, who's waiting, who's done |
| `/dpla history <hub>` | Last 6 months of ingest results for a hub |
| `/dpla counts <hub>` | Record count trend over recent months |
| `/dpla next-index` | When is the next index build and what data will it include |

**For engineers:**

| Command | What You Learn |
|---------|---------------|
| `/dpla pipeline` | Full pipeline status across all three systems |
| `/dpla failures` | Recent failures with stage, error classification, and remediation hint |
| `/dpla logs <hub> <stage>` | Recent structured log lines from CloudWatch |
| `/dpla health` | Infrastructure check: EC2, disk, memory, EMR, Elasticsearch |
| `/dpla dashboard` | Shareable link to the CloudWatch dashboard |
| `/dpla run <hub>` | Trigger a one-off ingest (requires engineer confirmation) |

### Scheduled Digests

| Digest | Channel | Schedule | Content |
|--------|---------|----------|---------|
| Daily ingest progress | `#dpla-ingest-status` | 9am ET weekdays during ingest window | Hubs completed yesterday, in progress, waiting |
| Weekly pipeline summary | `#dpla-ingest-status` | Monday 9am ET | Full month-to-date summary with hub counts |
| Daily Wikimedia progress | `#dpla-ingest-status` | 9am ET daily | Uploads per partner, stall alerts, overall progress |
| Failure digest | `#tech-alerts` | 6pm ET daily (only if failures exist) | Unresolved failures requiring attention |

---

## Current Architecture vs. Greenfield: Investment Strategy

The [Greenfield Vision](07-greenfield-vision.md) moves the pipeline onto Fargate containers orchestrated by Step Functions. This matters for observability planning because Fargate and Step Functions provide significant built-in monitoring capabilities that the current laptop/EC2/EMR architecture does not have. The question for every investment is: **does this work carry forward into the greenfield, or does it become throwaway?**

The answer divides cleanly into three categories.

### Build now -- carries forward completely

These investments deliver value today and transfer directly into any future architecture with zero rework. They define **what the data looks like** rather than how it moves, which makes them architecture-independent.

- **Structured JSON log format and field conventions.** Whether a Scala process runs on a laptop or inside a Fargate container, JSON logs with `hub`, `run_id`, and `stage` fields are JSON logs. In Fargate, the container's stdout goes directly to CloudWatch via a built-in log driver. If that output is already structured JSON, CloudWatch parses it automatically. The format defined now is the format used forever.

- **SNS topic structure.** The three notification topics (critical alerts, status updates, hub completions) and their Slack/email subscriptions work identically whether the publisher is a Python script on a laptop or a Step Functions state machine on AWS. SNS is the abstraction layer between "something happened" and "who needs to know." Defining the topics now means greenfield components publish to the same topics from day one.

- **CloudWatch metric names, namespaces, and alarms.** The metrics (RecordsHarvested, MappingFailureRate, StageDuration) are pipeline concepts, not infrastructure concepts. Alarms reference metrics. Whether the metric comes from a Python orchestrator or a Fargate container, the alarm definition is identical. The dashboard widgets that display these metrics are also unchanged.

- **DynamoDB state store, Lambda slash commands, EventBridge rules, and Slack channel topology.** These are all integration-layer components that interact with S3, DynamoDB, and CloudWatch -- not with the pipeline compute directly. They are 100% reusable regardless of whether the pipeline runs on a laptop or in Fargate.

**Effort:** ~3 weeks across Phases 1-4. **Rework in greenfield:** none.

### Build now with awareness -- minor rework later

These deliver real value in the current architecture but involve some infrastructure or code that changes when the greenfield arrives. Worth doing because the current operational pain is real and the rework cost is low.

- **CloudWatch agent installation.** This is the only way to get logs from a laptop and the Wikimedia EC2 into CloudWatch today. In Fargate, a built-in log driver replaces the agent entirely. The agent configuration is throwaway, but the log groups it writes to are reused -- Fargate task definitions simply point to the same groups. Rework: remove the agent when each component migrates.

- **Python orchestrator logging improvements.** If the orchestrator is eventually replaced by Step Functions, the Python logging code goes away. But the orchestrator will run for months or years before that happens, and structured logs make it debuggable in the meantime. The logging patterns and field conventions inform what Step Functions should emit via its native event system.

- **Dashboard infrastructure widgets.** Widgets showing EC2 disk usage or EMR cluster status need to be updated when those components move to Fargate (ECS task metrics) or EMR Serverless. Pipeline-specific widgets (records harvested, failure rates) are unchanged. Roughly 70% of the dashboard carries forward; 30% needs metric source updates.

- **Log rotation configuration.** Necessary now for the laptop and EC2. In Fargate, container logs are ephemeral and CloudWatch retention policies handle lifecycle. The logrotate configs are deleted when each component migrates.

**Effort:** ~2 weeks. **Rework in greenfield:** ~20-30% (agent removal, logrotate deletion, some dashboard widget updates).

### Wait for greenfield -- built-in to Fargate and Step Functions

These AWS capabilities are either unavailable in the current architecture or would require building poor substitutes for what the greenfield provides for free. The right move is to ensure that naming conventions, log formats, and topic structures established now are compatible with these capabilities when they come online.

- **ECS Container Insights.** Automatic per-task CPU, memory, network, and disk metrics with zero instrumentation. Available only when running on ECS/Fargate. No equivalent for a JVM on a laptop.

- **Step Functions visual execution console.** A graphical timeline of every pipeline execution -- which stages ran, how long each took, what failed, what the input and output were at each step. This is the single best debugging tool for pipeline issues. Building anything comparable for the current Python orchestrator would be a significant project; Step Functions provides it for free.

- **AWS X-Ray distributed tracing.** Instruments calls between services (Lambda to Fargate to S3 to Elasticsearch) and shows a service map with latency at each hop. Meaningful only when the pipeline is distributed across services, not when it runs as a single JVM on a laptop.

- **Fargate native log routing.** The built-in `awslogs` driver that replaces the CloudWatch agent entirely. One line in each Fargate task definition.

- **Step Functions + EventBridge for gate notifications.** Step Functions publishes events to EventBridge on every state transition. EventBridge rules route those events to the SNS topics defined in Phase 2 -- replacing all custom notification code. The topics and subscriptions carry forward; the publisher changes from Python code to a managed AWS service.

**Effort now:** none. **Design work now:** ensure Phase 1-4 naming conventions are compatible (they are by construction).

### The Strategic Principle

Invest now in **what the data looks like** -- log formats, field conventions, metric names, topic structures, channel topology. These are permanent. Invest cautiously in **how the data moves** -- agents, custom emission code, log rotation. These are interim. Wait for greenfield on **what the infrastructure provides for free** -- Container Insights, Step Functions console, X-Ray, native log routing. These cannot be meaningfully replicated today.

### Migration Path

When each component migrates to Fargate, the observability transition follows this pattern:

1. **Pre-migration:** Component runs on laptop/EC2, logs ship via CloudWatch agent to log group `/dpla/pipeline/<component>/`.
2. **Migration day:** Fargate task definition specifies the built-in log driver pointing to the same log group. CloudWatch agent removed from the old host. Downstream consumers (dashboards, alarms, Logs Insights queries, slash commands) see no change -- same log group, same JSON format, same fields.
3. **Post-migration:** Enable Container Insights for automatic infrastructure metrics. Remove custom metrics for infrastructure health (CPU, memory). Pipeline-specific metrics continue unchanged. Update the ~30% of dashboard widgets that referenced EC2/EMR metrics.
4. **Step Functions migration:** Add EventBridge rules matching Step Functions events, routing to existing SNS topics. Remove Python notification code. Slash commands that queried orchestrator state now query the Step Functions API.

Each component migrates independently. The log group is the bridge -- it stays the same throughout.

---

## Relationship to Pipeline Unification Phases

This observability architecture maps onto the phases defined in the [Implementation Roadmap](04-implementation-roadmap.md):

| Unification Phase | This Document | How They Connect |
|-------------------|---------------|-----------------|
| Phase 1: Reliability Fixes | Phase 1 (Structured Logging) | Structured logging is the foundation for reliability -- you cannot fix what you cannot see |
| Phase 2: Monitoring | Phase 2 (CloudWatch) + Phase 3 (Dashboard) | Specific AWS tooling to implement the monitoring goals defined in the roadmap |
| Phase 3: Indexer Automation | Phase 2 (Alarms) | EMR monitoring, cost protection alarms, and safety gate notifications |
| Phase 4: End-to-End | Phase 4 (Slack Integration) | Slash commands and digests are the user-facing expression of end-to-end coordination |
| [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md) | Phase 4 (Slash Commands) | Hub status commands and self-service integrate with the readiness layer |

The observability work can proceed in parallel with other unification phases. Phase 1 (structured logging) is independent of all other work. Phases 2-4 build on each other but do not block other unification efforts.

---

## Summary

The pipeline's biggest operational risk is undetected failure. Today, the only way to know what's happening is to ask an engineer to check manually. This proposal adds a monitoring layer that costs $12/month, requires no new vendor relationships, and makes the pipeline visible to everyone who has a stake in it.

The work is organized so that the most important step (structured logging) is also the cheapest, the fastest, and the one with the longest shelf life. Each subsequent phase adds value independently. The architecture is designed so that 80-100% of the work carries forward into the greenfield vision, and the remaining 20% is well-understood interim infrastructure that gets simpler -- not more complex -- when the greenfield arrives.

The end state: a hub contact types `/dpla status florida` in Slack and knows, in five seconds, exactly where their data stands. A community manager opens a dashboard URL and sees the full monthly cycle at a glance. An engineer gets an alert the moment something stalls, with enough context to fix it without a diagnostic expedition. Nobody SSHes into a server to check if a process is running.

---
---

## Appendix A: Structured Logging Specification

**Target:** Every pipeline component emits JSON-formatted log lines with a consistent field schema.

### Log Schema

Every log line must include these fields:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `timestamp` | ISO 8601 string | When the event occurred | `"2026-02-23T14:30:00Z"` |
| `level` | string | Log level | `"INFO"`, `"WARN"`, `"ERROR"` |
| `logger` | string | Source component/class | `"dpla.ingestion3.harvesters.oai"` |
| `message` | string | Human-readable description | `"Harvest page complete"` |
| `hub` | string | Hub short name (when applicable) | `"florida"` |
| `run_id` | string | Unique identifier for this pipeline run | `"20260223-florida"` |
| `stage` | string | Pipeline stage | `"harvest"`, `"mapping"`, `"enrichment"`, `"jsonl"`, `"sync"` |

Additional context fields are encouraged (e.g., `records`, `set`, `token`, `error`, `duration_ms`) but not required.

### Example Output

```json
{"timestamp":"2026-02-23T14:30:00Z","level":"INFO","logger":"dpla.ingestion3.harvesters.oai","message":"Harvest page complete","hub":"florida","run_id":"20260223-florida","stage":"harvest","records":500,"set":"fsu","token":"abc123"}
```

### Scala Implementation

**Files to change:**

- [`src/main/resources/log4j2.properties`](../../src/main/resources/log4j2.properties) -- Switch from `PatternLayout` to `JsonTemplateLayout`. This layout is built into Log4j2 (no new dependency required). Configure a `RollingFileAppender` with size/time-based rotation alongside the console appender.

- Entry points ([`HarvestExecutor.scala`](../../src/main/scala/dpla/ingestion3/executors/HarvestExecutor.scala), `MappingExecutor.scala`, `EnrichExecutor.scala`, `JsonlExecutor.scala`) -- At the start of each entry point, set MDC (Mapped Diagnostic Context) fields for `hub`, `run_id`, and `stage`. These propagate automatically to all log statements within that execution context.

### Python Implementation

**Files to change:**

- [`scheduler/orchestrator/main.py`](../../scheduler/orchestrator/main.py) -- Configure the root logger with a JSON formatter (use `python-json-logger` or a simple custom formatter). Set `run_id` and `hub` in a `logging.LoggerAdapter` or via `extra` dicts.

- [`scheduler/orchestrator/notifications.py`](../../scheduler/orchestrator/notifications.py) -- Replace `print()` statements with `logger.info()`, `logger.warning()`, `logger.error()`.

- [`scheduler/orchestrator/state.py`](../../scheduler/orchestrator/state.py) -- Replace `print()` statements with appropriate log-level calls.

Use `logging.handlers.RotatingFileHandler` for rotation (10MB max, 5 backups).

### Shell Script Implementation

**File to change:**

- [`scripts/common.sh`](../../scripts/common.sh) -- Add a `log_json()` function that writes a JSON line to a `.jsonl` sidecar file alongside the existing human-readable log output. Update `log_info()`, `log_warn()`, and `log_error()` to call `log_json()` in addition to writing to the terminal/log file.

Configure `logrotate` for the `logs/` directory (daily rotation, 14-day retention, compress).

---

## Appendix B: CloudWatch Configuration

### Log Group Structure

```
/dpla/pipeline/
    ingestion3/harvest/       # Scala harvest logs
    ingestion3/mapping/       # Scala mapping logs
    ingestion3/enrichment/    # Scala enrichment logs
    ingestion3/jsonl/         # Scala JSONL export logs
    orchestrator/             # Python orchestrator logs
    sparkindexer/             # EMR/Spark indexer logs
    wikimedia/downloader/     # EC2 wikimedia downloader
    wikimedia/uploader/       # EC2 wikimedia uploader
    scripts/                  # Shell script structured logs
```

**Retention:** 30 days for operational logs, 90 days for error-level logs.

In the greenfield architecture, Fargate task definitions specify these same log groups via the `awslogs` log driver. The group hierarchy is preserved; only the log stream names change (from agent-generated names to ECS task IDs).

### Custom Metrics

| Metric | Namespace | Dimensions | Emitted By |
|--------|-----------|------------|------------|
| `RecordsHarvested` | `DPLA/Pipeline` | Hub, RunId | Orchestrator |
| `RecordsMapped` | `DPLA/Pipeline` | Hub, RunId | Orchestrator |
| `MappingFailureRate` | `DPLA/Pipeline` | Hub, RunId | Orchestrator |
| `StageDuration` | `DPLA/Pipeline` | Hub, Stage | Orchestrator |
| `S3SyncStatus` | `DPLA/Pipeline` | Hub | s3-sync.sh |
| `AnomalyScore` | `DPLA/Pipeline` | Hub | Anomaly detector |
| `WikimediaUploadsPerHour` | `DPLA/Wikimedia` | Partner | EC2 uploader |
| `WikimediaStallMinutes` | `DPLA/Wikimedia` | Partner | EC2 health check |
| `EMRStepDuration` | `DPLA/Indexer` | StepName | EMR monitoring script |

Emit via boto3 `PutMetricData` in the current architecture. In the greenfield, pipeline-specific metrics can switch to CloudWatch Embedded Metric Format (structured JSON log lines with a special `_aws` field that CloudWatch automatically extracts as metrics).

### Alarms

| Alarm | Metric | Threshold | Notification |
|-------|--------|-----------|-------------|
| Pipeline stage timeout | `StageDuration` | > 4 hours (harvest) or > 2 hours (other stages) | SNS `dpla-pipeline-critical` |
| High mapping failure rate | `MappingFailureRate` | > 15% for any hub | SNS `dpla-pipeline-critical` |
| Wikimedia stall | `WikimediaStallMinutes` | > 60 minutes | SNS `dpla-pipeline-critical` |
| EMR cost protection | EMR step runtime | > 4 hours | SNS `dpla-pipeline-critical` |
| EC2 disk full | CloudWatch agent `disk_used_percent` | > 80% | SNS `dpla-pipeline-critical` |

### SNS Topic Structure

```
dpla-pipeline-critical      -> Slack #tech-alerts + email tech@dp.la
dpla-pipeline-status        -> Slack #dpla-ingest-status
dpla-pipeline-hub-complete  -> Slack #tech + email to hub contacts
```

SNS provides built-in retry with exponential backoff, dead-letter queues for failed deliveries, delivery status logging, and the ability to add subscribers without code changes. This replaces the current direct Slack webhook POST pattern in [`notifications.py`](../../scheduler/orchestrator/notifications.py), which has no retry logic and no delivery verification.

---

## Appendix C: CloudWatch Logs Insights Query Examples

Once structured JSON logs are flowing to CloudWatch, Logs Insights provides ad-hoc querying at no additional cost beyond CloudWatch Logs.

```sql
-- All errors for a specific hub in the last 24 hours
fields @timestamp, level, hub, message, error
| filter hub = "florida" and level = "ERROR"
| sort @timestamp desc
| limit 50

-- Harvest duration by hub for the current month
fields hub, @timestamp, message
| filter message like /Harvest complete/
| stats max(@timestamp) - min(@timestamp) as duration by hub

-- Error rate trend by hub over last 7 days
fields hub, level
| filter level = "ERROR"
| stats count(*) as errors by hub, bin(1d)

-- Find silent failures (stages that started but never completed)
fields hub, stage, message
| filter message like /started/ or message like /complete/
| stats count(*) as events by hub, stage
| filter events = 1
```

These queries replace the current pattern of SSH-ing into a machine and manually inspecting log files. They also enable pattern analysis that is impractical with manual inspection (e.g., "which hubs have had increasing error rates over the past three months?").

---

## Appendix D: Slack Slash Command Architecture

Slash commands are implemented as Lambda functions behind API Gateway, deployed as a separate project from ingestion3 (as recommended in [docs/slack-slash-commands-planning.md](../slack-slash-commands-planning.md)).

### Implementation Pattern

1. Slack sends an HTTP POST to the API Gateway endpoint (must respond within 3 seconds).
2. The Lambda validates the Slack signing secret, parses the command and arguments.
3. For fast queries (S3 listings, DynamoDB reads), the Lambda returns the response inline.
4. For slower queries (CloudWatch Logs Insights, multi-S3-listing aggregation), the Lambda acknowledges immediately and uses the `response_url` to post the result asynchronously.

### Data Sources by Command

| Command | Data Source | Greenfield Change |
|---------|------------|-------------------|
| `/dpla status <hub>` | S3 listing + `_MANIFEST` files | None |
| `/dpla staged` | S3 `dpla-master-dataset` listing | None |
| `/dpla schedule` | DynamoDB readiness store | None |
| `/dpla history <hub>` | S3 directory timestamps + `_MANIFEST` files | None |
| `/dpla counts <hub>` | S3 `_MANIFEST` files | None |
| `/dpla next-index` | DynamoDB / orchestrator state | Query Step Functions instead of DynamoDB |
| `/dpla pipeline` | CloudWatch metrics + DynamoDB | Query Step Functions execution status |
| `/dpla failures` | CloudWatch Logs Insights query | May also query Step Functions failed executions |
| `/dpla logs <hub> <stage>` | CloudWatch Logs | None (same log groups) |
| `/dpla health` | CloudWatch alarms + EC2 API | EC2 API becomes ECS API |
| `/dpla dashboard` | CloudWatch dashboard URL | None |
| `/dpla run <hub>` | Lambda -> orchestrator | Lambda -> Step Functions StartExecution |

Commands in the top half of the table query S3 and DynamoDB exclusively -- they are 100% reusable in any architecture. Commands in the bottom half query infrastructure-specific APIs and need minor updates when the greenfield arrives.

### Infrastructure

| Component | Purpose | Estimated Cost |
|-----------|---------|---------------|
| API Gateway (HTTP API) | Slack slash command endpoint | ~$0.50/month |
| Lambda (single function with routing, or one per command) | Command handlers + digest generators | ~$1.00/month |
| DynamoDB (on-demand) | Pipeline state, hub readiness, run history | ~$1.00/month |
| EventBridge (scheduled rules) | Daily/weekly digest triggers | $0.00 |

### Slack App Setup

1. Create app at [api.slack.com/apps](https://api.slack.com/apps).
2. Add Slash Command: `/dpla` with Request URL = API Gateway endpoint.
3. Set `SLACK_SIGNING_SECRET` for request verification.
4. Install app to workspace.
5. Optionally add command descriptions for subcommands in Slack's command configuration.

---

## Appendix E: Dashboard Widget Specification

### Row 1: Monthly Cycle Overview

- **Number widget:** "Hubs Completed This Month" -- count of hubs with successful JSONL export in current month.
- **Number widget:** "Hubs Failed" -- count of hubs with any stage failure.
- **Number widget:** "Hubs Waiting" -- scheduled but not yet started.

### Row 2: Current Activity

- **Table widget:** Active hub processing -- hub name, current stage, duration, status.
- **Alarm status widget:** All pipeline alarms (green/red).

### Row 3: Stage Performance

- **Bar chart:** Harvest duration per hub (last run).
- **Bar chart:** Mapping success rate per hub (last run).
- **Line chart:** Total records harvested per month over last 6 months (trend).

### Row 4: Infrastructure

- **Line chart:** EC2 disk usage and available memory (wikimedia server). In greenfield: ECS task CPU/memory.
- **Number widget:** EMR cluster status (running/idle/terminated). In greenfield: EMR Serverless application status.

### Row 5: Wikimedia

- **Line chart:** Uploads per hour over last 7 days.
- **Table widget:** Per-partner upload progress (partner name, total uploaded, last upload timestamp, rate).

### Sharing

CloudWatch dashboards support sharing via public read-only URL (no AWS login required). A Lambda function can generate a time-scoped URL and post it to Slack on demand (`/dpla dashboard`) or on a schedule.

---

## Appendix F: Greenfield-Only AWS Capabilities

These capabilities become available when the pipeline migrates to Fargate and Step Functions. They require no custom implementation -- they are built into the AWS services. The work in Phases 1-4 is designed to be compatible with all of them.

| Capability | What It Provides | Activation |
|------------|-----------------|------------|
| **ECS Container Insights** | Automatic per-task CPU, memory, network, disk I/O metrics; service map | Enable in ECS cluster settings (one line of OpenTofu) |
| **Step Functions Visual Console** | Graphical execution timeline with per-state timing, input/output, failure drill-down | Automatic with any Step Functions state machine |
| **AWS X-Ray Tracing** | Distributed tracing across Lambda, Fargate, S3, OpenSearch; service map with latency | Enable X-Ray flag in task definitions and Lambda configs |
| **Fargate `awslogs` Driver** | Zero-config log shipping from container stdout/stderr to CloudWatch Logs | One block in each Fargate task definition |
| **Step Functions + EventBridge** | Native state-change events; replaces custom notification code | EventBridge rules routing to existing SNS topics |
| **OpenSearch Dashboards** | Built-in search analytics, query performance, index health visualizations | Part of OpenSearch Serverless (if adopted) |
| **EMR Serverless Metrics** | Built-in application-level metrics (driver/executor CPU, memory, I/O) | Automatic with EMR Serverless |

The design principle throughout this document: **define the interfaces now (log formats, metric names, topic structures, channel topology) so these capabilities plug in cleanly when the infrastructure arrives.**
