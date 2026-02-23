# DPLA Pipeline Unification -- Greenfield Vision

**Audience:** Leadership at receiving organization, architects planning long-term
**Reading time:** 15 minutes

---

## Purpose

This document describes what the DPLA data pipeline would look like if built from scratch today, unconstrained by the current implementation. It is an aspirational target state -- not a rewrite plan, but a north star that informs incremental decisions. When a choice comes up during implementation ("should we do X or Y?"), this document helps answer: "which option moves us closer to where we'd want to be if starting fresh?"

This vision also serves a practical handoff purpose: if another organization takes over the pipeline, they may choose to rebuild rather than maintain. This document gives them a clear picture of the ideal end state.

---

## The Pipeline in One Paragraph

A metadata aggregation pipeline that harvests records from ~60 library/museum sources monthly, transforms them into a common schema, builds a search index, serves an API, and uploads eligible media to Wikimedia Commons. The pipeline handles ~40 million records, ~2TB of data, and continuous media uploads running for weeks at a time.

---

## Greenfield Architecture

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

---

## What This Means for a Receiving Organization

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

For a greenfield or incremental migration, all infrastructure should be managed via OpenTofu (open-source Terraform-compatible):

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

| Component | Current Monthly | Greenfield Monthly | Notes |
|-----------|----------------|--------------------|-------|
| Ingestion compute | $0 (laptop) | ~$50 (Fargate, ~168 task-hours/month) | Fargate pricing for hub processing |
| Indexing compute | ~$30 (EMR) | ~$20 (EMR Serverless) | Pay-per-query pricing |
| Wikimedia compute | ~$50 (EC2) | ~$80 (Fargate + EBS) | Continuous tasks cost more on Fargate |
| Search | ~$200 (managed ES, estimated) | ~$150--300 (OpenSearch Serverless) | Depends on query volume |
| Orchestration | $0 | ~$5 (Step Functions + Lambda) | Negligible at this scale |
| Storage (S3) | ~$75 | ~$75 | No change |
| Monitoring | $0 | ~$10 (CloudWatch) | Minimal at this scale |
| **Total** | **~$355** | **~$390--540** | Greenfield adds ~$35--185/month |

The greenfield cost is modestly higher but eliminates all manual infrastructure management and the laptop dependency. The primary cost driver is whether OpenSearch Serverless or a self-managed ES cluster is used.

---

## Summary

The greenfield vision is not the immediate plan -- it's the long-term direction. The incremental work in Phases 0--4 makes the current system safe and operable. The greenfield vision shows where those incremental improvements naturally lead, and helps a receiving organization decide whether to continue improving the current system or invest in a rebuild.

The key insight: **the contracts and gates defined in Phase 0 and enforced in Phases 1--4 are the same contracts that a greenfield system would need.** Getting the contracts right now means any future architecture -- whether it's the current Scala+Python+EMR stack or a Python+Fargate+Step Functions stack -- will work correctly at the boundaries.
