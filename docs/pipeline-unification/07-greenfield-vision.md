# DPLA Pipeline Unification -- Greenfield Vision

**Audience:** Leadership at receiving organization, architects planning long-term

**Reading time:** 15 minutes

**Context:** This document describes the ideal end state -- what the pipeline would look like if built from scratch today. It is not a rewrite plan. It is a reference for incremental decisions and a starting point for any organization that chooses to rebuild rather than extend.

---

## Contents

- [Purpose](#purpose)
- [The Pipeline in One Paragraph](#the-pipeline-in-one-paragraph)
- [Greenfield Architecture](#greenfield-architecture)
  - [Design Principles](#design-principles)
  - [Technology Choices](#technology-choices)
  - [Architecture Diagram](#architecture-diagram)
  - [Key Differences from Current State](#key-differences-from-current-state)
- [Hub Readiness and Scheduling](#hub-readiness-and-scheduling)
- [What This Means for a Receiving Organization](#what-this-means-for-a-receiving-organization)
  - [If You Maintain the Current System](#if-you-maintain-the-current-system)
  - [If You Rebuild](#if-you-rebuild)
  - [Recommended Approach: Incremental Migration](#recommended-approach-incremental-migration)
- [Infrastructure as Code: OpenTofu Strategy](#infrastructure-as-code-opentofu-strategy)
  - [Stack Isolation](#stack-isolation)
  - [Why Segregated from Existing IaC](#why-segregated-from-existing-iac)
- [Cost Comparison](#cost-comparison)
- [Summary](#summary)

---

## Purpose

This document describes what DPLA's data pipeline would look like if built from scratch today, unconstrained by the current implementation. It is an aspirational target state: a north star that informs incremental decisions. When a choice comes up during implementation ("should we do X or Y?"), this document helps answer: "which option moves us closer to where we'd want to be if starting fresh?"

This vision also serves a practical handoff purpose. If another organization takes over the pipeline, they may choose to rebuild rather than maintain. This document gives them a clear picture of the ideal end state and a realistic estimate of what that investment involves.

---

## The Pipeline in One Paragraph

DPLA's pipeline harvests records from ~60 library and museum sources monthly, transforms them into a common schema, builds a search index that serves dp.la, and uploads eligible media to Wikimedia Commons. It handles approximately 40 million records, approximately 2TB of data, and continuous media uploads that run for weeks at a time. The mission it serves -- making America's cultural heritage accessible to everyone -- requires it to run reliably every month.

---

## Greenfield Architecture

The six principles below are not aspirational ideals -- they are the concrete properties that distinguish a modern data pipeline from the current one. Each principle addresses a specific operational cost that the current system carries.

### Design Principles

1. **Single orchestration plane.** One system coordinates all stages, manages state, and owns the execution lifecycle.
2. **Explicit contracts at every boundary.** Data formats, naming conventions, and handoff rules are defined in versioned schemas, not implicit in code.
3. **Observable by default.** Every stage emits structured events. Failures are detected in seconds, not months.
4. **Restartable and idempotent.** Every operation can be interrupted and resumed. Running the same operation twice produces the same result.
5. **Infrastructure as code.** All cloud resources are defined in declarative configuration, reproducible from scratch.
6. **Minimal language diversity.** Prefer one or two languages for the entire pipeline to reduce cognitive overhead and dependency management.

### Technology Choices

| Component | Greenfield Choice | Rationale |
|-----------|-------------------|-----------|
| **Language** | Python 3.12+ everywhere | Unified ecosystem. Libraries for OAI-PMH, Elasticsearch, S3, Wikimedia. Strong async support. |
| **Orchestration** | AWS Step Functions | Native state machine with retries, timeouts, parallel execution, and audit trail. No custom coordinator to maintain. |
| **Compute (batch)** | AWS Fargate (ECS) | Serverless containers. No instances to patch. Pay per task. Scale to zero between monthly runs. |
| **Compute (indexing)** | AWS Fargate or EMR Serverless | Spark on EMR Serverless (pay per query) or rewrite indexing as a streaming Fargate task using the Elasticsearch bulk API directly. |
| **Compute (wikimedia)** | AWS Fargate with EBS | Long-running tasks with persistent storage. Auto-restart on failure. No SSH required. |
| **Data format** | Apache Parquet | Columnar, schema-enforced, splittable. Better than JSONL for analytics and validation. JSONL as an export format for ES. |
| **Schema registry** | JSON Schema or Avro schema in a shared repo | Enforced at write time. Breaking changes detected before deployment. |
| **Search** | Amazon OpenSearch Serverless | Managed, serverless Elasticsearch-compatible. No cluster sizing, patching, or capacity planning. |
| **Monitoring** | CloudWatch + Slack via SNS | Native AWS monitoring. Structured logs. Alarms with automatic escalation. |
| **IaC** | OpenTofu | Open-source Terraform-compatible. All infrastructure reproducible. State isolation per environment. |
| **CI/CD** | GitHub Actions | Build, test, deploy from pull requests. No separate CI system. |
| **Configuration** | Single config repo | Hub definitions, partner metadata, thresholds, credentials (encrypted) in one versioned repository. |

### Architecture Diagram

```
                    ┌─────────────────────────────────────────────────────┐
                    │              AWS Step Functions                      │
                    │         (Monthly Pipeline State Machine)             │
                    │                                                      │
                    │   ┌──────────┐    ┌──────────┐    ┌──────────┐     │
                    │   │ Ingest   │───>│ Index    │───>│ Wikimedia│     │
                    │   │ Stage    │    │ Stage    │    │ Stage    │     │
                    │   └────┬─────┘    └────┬─────┘    └────┬─────┘     │
                    │        │               │               │           │
                    └────────│───────────────│───────────────│───────────┘
                             │               │               │
                    ┌────────▼─────┐  ┌──────▼───────┐  ┌───▼──────────┐
                    │ Fargate Task │  │ Fargate Task  │  │ Fargate Task │
                    │ per hub      │  │ (or EMR       │  │ per partner  │
                    │              │  │  Serverless)  │  │ (long-run)   │
                    │ • harvest    │  │ • read Parquet│  │ • get IDs    │
                    │ • transform  │  │ • transform   │  │ • download   │
                    │ • validate   │  │ • bulk write  │  │ • upload     │
                    │ • write S3   │  │ • validate    │  │ • checkpoint │
                    └──────────────┘  └──────────────┘  └──────────────┘
                             │               │               │
                    ┌────────▼───────────────▼───────────────▼──────────┐
                    │                      S3                            │
                    │  ┌──────────────┐  ┌───────────┐  ┌────────────┐ │
                    │  │ Raw (Parquet)│  │ Processed │  │ Media      │ │
                    │  │ per hub      │  │ (JSONL    │  │ per partner│ │
                    │  │              │  │  for ES)  │  │            │ │
                    │  └──────────────┘  └─────┬─────┘  └────────────┘ │
                    └──────────────────────────│────────────────────────┘
                                               │
                    ┌──────────────────────────▼────────────────────────┐
                    │            OpenSearch Serverless                   │
                    │                                                    │
                    │  Alias: dpla → current index                      │
                    │  Previous index retained for rollback             │
                    │                                                    │
                    └──────────────────────────┬────────────────────────┘
                                               │
                    ┌──────────────────────────▼────────────────────────┐
                    │                   DPLA API                        │
                    │              (api.dp.la/v2/)                      │
                    └──────────────────────────────────────────────────┘
```

### Key Differences from Current State

| Aspect | Current | Greenfield |
|--------|---------|------------|
| Orchestration | Manual + Python scripts | AWS Step Functions state machine |
| Compute | Laptop, EMR, bare EC2 | Fargate (serverless containers) |
| Coordination | Implicit (human sequences stages) | Explicit (state machine transitions) |
| Monitoring | Partial Slack from ingestion only | Full CloudWatch + SNS to Slack/email |
| Recovery | Manual restart from scratch | Automatic checkpoint + resume |
| Search | Self-managed Elasticsearch | OpenSearch Serverless (managed) |
| Infrastructure | Manual setup, hardcoded IDs | OpenTofu, reproducible from scratch |
| Languages | Scala + Python + bash | Python only |
| Data format | JSONL + Avro | Parquet (with JSONL export for ES) |
| Configuration | 3 separate config systems | 1 config repo with JSON Schema |
| Hub scheduling | Manual email + guessing | Hub-triggered ingests with one-click readiness |
| Hub communication | Freeform email replies | Structured status API + S3 deposit monitoring |
| Post-ingest review | None (revocations after the fact) | Optional approve/reject in mapping summary email |

---

## Hub Readiness and Scheduling

One of the largest sources of operational overhead in the current pipeline is not technical -- it is the monthly communication cycle with ~60 hub contacts. Today, DPLA sends a scheduling email and guesses when hubs are ready. Hubs reply by freeform email to flag delays or problems. File-based hubs deposit data in S3 buckets that must be manually checked. Post-ingest revocations (hubs discovering problems after harvest) cost 5-8 hours of engineering time each.

A detailed beta proposal for a **Hub Readiness Layer** exists in the [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md). In a greenfield architecture, this layer would be a first-class component:

**Hub Status API** (API Gateway + Lambda + DynamoDB): Hubs interact via one-click links in monthly scheduling emails. "Ready -- start my ingest" triggers the pipeline. "Skip this month" flags the hub as inactive. Status is stored in DynamoDB and consumed by the orchestrator (Step Functions, in greenfield).

**S3 Deposit Monitoring** (EventBridge + S3 Event Notifications): For file-based hubs, S3 event notifications trigger the ingest automatically when new data is detected. No polling required -- the greenfield infrastructure manages this via OpenTofu.

**Review-Before-Publish Gate** (integrated into Step Functions): An optional step in the per-hub state machine. After harvest and mapping, the hub receives the standard mapping summary email enhanced with approve/reject links. The Step Functions state machine pauses at a "wait for callback" state until the hub approves, rejects, or the auto-approve window expires. This is native Step Functions capability -- no custom wait logic needed.

**Hub-Partitioned Indexing** (separate investigation): In the greenfield, consider per-hub Elasticsearch indices with a spanning alias, enabling surgical hub removal without full re-indexing. This is flagged for technical investigation in the [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md#technical-investigation-hub-partitioned-indexing) and is particularly relevant for hubs like CDL that send frequent item-level delete requests.

The hub readiness layer is architecture-independent. The beta proposal works with the current Python orchestrator on a laptop. In a greenfield build, the same concepts (status API, S3 monitoring, review gates) map directly onto Step Functions + EventBridge + Lambda with stronger guarantees and no custom coordination code.

---

## What This Means for a Receiving Organization

A receiving organization faces a choice: maintain the current system with incremental improvements, or rebuild toward the greenfield vision. Both paths are viable. The right choice depends on engineering capacity, timeline, and appetite for risk. The sections below describe what each path involves.

### If You Maintain the Current System

The incremental improvements in this plan's Phases 0--4 make the current system significantly more operable:
- Failures are detected quickly
- Stage transitions are automated with safety gates
- Documentation covers the full pipeline
- Agent skills enable operation without deep codebase knowledge

This is the recommended path for the near term.

### If You Rebuild

A greenfield rebuild would:
1. Eliminate the Scala dependency (the biggest barrier to contributor onboarding)
2. Eliminate the laptop-as-server anti-pattern
3. Provide full infrastructure reproducibility
4. Reduce operational complexity (fewer languages, fewer deployment models)
5. Enable true serverless operation (no persistent instances to manage)

Estimated effort for a full rebuild: 3--6 months with a 2-person team, assuming the current system continues to run in parallel during development.

### Recommended Approach: Incremental Migration

Rather than a big-bang rebuild, consider migrating one stage at a time:

**Step 1: Migrate Wikimedia to Fargate** (lowest risk, highest operational benefit). The EC2 instance is the most fragile component. Moving uploaders to Fargate tasks with persistent EBS and CloudWatch monitoring eliminates the "SSH to check status" anti-pattern. The Python codebase moves with minimal changes.

**Step 2: Migrate Ingestion to Fargate** (medium risk, eliminates laptop dependency). Rewrite harvest/transform in Python (or containerize the existing Scala/Spark pipeline). Run as Fargate tasks triggered by Step Functions. Hub configuration moves to a shared config repo.

**Step 3: Migrate Indexing** (highest risk, touches search). Replace EMR with EMR Serverless or a direct Elasticsearch bulk-write Fargate task. This requires the most careful testing since it affects the production search.

**Step 4: Replace Elasticsearch with OpenSearch Serverless** (optional, cost-dependent). Eliminates cluster management. Depends on whether OpenSearch Serverless meets performance and cost requirements for ~40M documents.

Each step can be done independently. The Step Functions orchestration layer works with both old and new implementations -- it just calls different compute targets.

---

## Infrastructure as Code: OpenTofu Strategy

For a greenfield or incremental migration, all infrastructure should be defined in OpenTofu (open-source Terraform-compatible). The goal is that any engineer can reproduce the entire pipeline environment from a clean AWS account by running a set of well-defined scripts -- no manual console steps, no institutional memory required.

### Stack Isolation

```
orchestration/          # Step Functions, EventBridge, Lambda glue
  shared/               # SNS topics, IAM roles, VPC
  staging/              # Non-production pipeline
  production/           # Production pipeline

compute/                # Fargate task definitions, ECS clusters
  ingestion/
  indexing/
  wikimedia/

data/                   # S3 buckets, OpenSearch, config
  storage/
  search/
```

Each stack has its own state file. Changes to one stack cannot accidentally affect another.

### Why Segregated from Existing IaC

The existing `tofu` repository manages DPLA's application infrastructure (ECS services, CodePipeline, etc.). The pipeline orchestration layer should be a separate stack because:
- Different change cadence (pipeline changes monthly; app deploys weekly)
- Different blast radius (pipeline failure affects data freshness; app failure affects availability)
- Different ownership (pipeline may transfer to a different org than the app)
- Simpler handoff (one self-contained stack to transfer)

---

## Cost Comparison

The greenfield architecture costs modestly more per month but eliminates the largest non-dollar costs: manual infrastructure management, the laptop dependency, and the SSH-to-check-status pattern for Wikimedia. The table below compares current and greenfield monthly costs.

| Component | Current Monthly | Greenfield Monthly | Notes |
|-----------|----------------|--------------------|-------|
| Ingestion compute | $0 (laptop) | ~$50 (Fargate, ~168 task-hours/month) | Fargate pricing for hub processing |
| Indexing compute | ~$30 (EMR) | ~$20 (EMR Serverless) | Pay-per-query pricing |
| Wikimedia compute | ~$50 (EC2) | ~$80 (Fargate + EBS) | Continuous tasks cost more on Fargate |
| Search | ~$200 (managed ES, estimated) | ~$150--300 (OpenSearch Serverless) | Depends on query volume |
| Orchestration | $0 | ~$5 (Step Functions + Lambda) | Negligible at this scale |
| Storage (S3) | ~$75 | ~$75 | No change |
| Monitoring | $0 | ~$10 (CloudWatch) | Minimal at this scale |
| Hub readiness layer | $0 (manual email) | ~$3 (API GW + Lambda + DynamoDB) | Hub status API, S3 monitoring |
| **Total** | **~$355** | **~$393--543** | Greenfield adds ~$38--188/month |

The greenfield cost is modestly higher but eliminates all manual infrastructure management and the laptop dependency. The primary cost driver is whether OpenSearch Serverless or a self-managed ES cluster is used.

---

## Summary

The greenfield vision is not the immediate plan -- it is the long-term direction. The incremental work in Phases 0--4 makes the current system safe and operable. The greenfield vision shows where those incremental improvements naturally lead, and helps a receiving organization decide whether to continue improving the current system or invest in a rebuild.

The key insight: **the contracts and gates defined in Phase 0 and enforced in Phases 1--4 are the same contracts that a greenfield system would need.** Getting the contracts right now means any future architecture -- whether it's the current Scala+Python+EMR stack or a Python+Fargate+Step Functions stack -- will work correctly at the boundaries.

Similarly, the [Hub Readiness Layer](08-hub-readiness-automation.md) is designed to be architecture-independent. The hub-facing API (one-click links, review gates), S3 deposit monitoring, and i3.conf configuration all transfer directly to a greenfield architecture. In a Step Functions build, the review-before-publish gate becomes a native "wait for callback" state -- simpler and more reliable than custom polling. Building the hub readiness layer now, against the current system, validates the workflow and hub relationships before committing to a greenfield infrastructure investment.
