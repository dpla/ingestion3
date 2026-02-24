# DPLA Pipeline Unification Plan

**Last updated:** February 2026

**Status:** First draft

---

## Contents

- [What DPLA Is and What This Pipeline Enables](#what-dpla-is-and-what-this-pipeline-enables)
- [What This Document Set Is For](#what-this-document-set-is-for)
- [What This Costs Today](#what-this-costs-today)
- [Document Guide](#document-guide)
- [How to Read These Documents](#how-to-read-these-documents)
- [Documentation for Engineers: Procedures and Coding Assistants](#documentation-for-engineers-procedures-and-coding-assistants)
- [Related Documents](#related-documents)

---

## What DPLA Is and What This Pipeline Enables

DPLA -- the Digital Public Library of America -- makes the collections of America's libraries, archives, and museums freely available to everyone. Through dp.la, anyone can search tens of millions of records spanning photographs, manuscripts, maps, audio recordings, and more, drawn from approximately 60 partner institutions (called "hubs") across the country. These hubs range from large research universities and state libraries to regional historical societies and specialized archives. DPLA does not hold these items -- it aggregates the metadata that describes them and makes that metadata searchable in one place.

The data pipeline is the infrastructure that makes this possible. Once a month, it reaches out to hubs, pulls in their latest records, transforms them into a common format, and loads them into the search index that powers dp.la. It also identifies records with openly-licensed media and uploads them to Wikimedia Commons, placing them on Wikipedia and other Wikimedia projects. Without the pipeline running reliably, dp.la goes stale. Without dp.la, DPLA's collections are effectively invisible to most of the people the mission is meant to serve.

---

## What This Document Set Is For

This folder documents the full DPLA data pipeline and makes the case for investing in its reliability, observability, and long-term operability. It was created to serve four purposes:

1. **Simplify the pipeline end-to-end** by adding coordination, monitoring, and reliability across the three existing systems -- so that the monthly cycle runs predictably and failures are detected in minutes, not months.
2. **Enable handoff** by packaging the pipeline so a future team or organization can understand, operate, and extend it without relying on the institutional memory of the engineers who built it.
3. **Support operation under constrained resources** by providing documented procedures and coding-assistant tooling (e.g., Cursor, Claude Code) that allow a small team to run the pipeline without deep codebase expertise. See [Documentation for engineers: procedures and coding assistants](#documentation-for-engineers-procedures-and-coding-assistants) below.
4. **Communicate to funders and leadership** the operational investment required to sustain this infrastructure, and what improvement to that infrastructure yields in mission impact and reduced operational cost.

The plan is designed for incremental implementation. Each phase delivers standalone value. A future implementer -- human or coding assistant following the documented procedures -- can work through the implementation roadmap step by step without needing to understand the entire plan upfront.

---

## What This Costs Today

The pipeline works. It has run monthly for years, and the records it produces are used by researchers, educators, and the public every day. The costs it carries are not the result of poor engineering decisions -- they are the natural accumulation of solving real problems incrementally, under nonprofit resource constraints, over time.

Those costs are worth naming explicitly:

- **Engineering time to run the monthly cycle.** Running ingestion, indexing, and Wikimedia uploads requires approximately 15--20 hours of engineering attention per month -- not because the work is complex, but because the three stages have no coordination layer. Each must be coordinated manually, started manually, monitored manually, and handed off manually.
- **Engineering time lost to silent failures.** When something breaks undetected, diagnosing and recovering takes far longer than it would have if an alert had fired at the moment of failure.
- **Partner coordination overhead.** DPLA sends scheduling emails, waits for replies, guesses when hubs are ready, and occasionally discovers after the fact that a hub's data was not ready -- requiring the affected ingest to be deleted and re-run. Each post-ingest revocation costs 5--8 hours of engineering time.
- **Institutional knowledge concentration.** The complete picture of how to run the monthly cycle -- which hubs to process, in what order, what to do when something fails, how to flip the search index -- is held primarily by one engineer. That knowledge is not a failure to document; it is the predictable result of a small team building and operating sophisticated infrastructure.

This document directly addresses each of these costs.

---

## Document Guide

| # | Document | Audience | What You'll Learn | Reading Time |
|---|----------|----------|-------------------|--------------|
| 01 | [Executive Summary](01-executive-summary.md) | Leadership | What the pipeline does, why it matters, what it costs today, what changes, key risks | 15 min |
| 02 | [System Architecture](02-system-architecture.md) | Technical managers, architects, engineers | How the three systems connect, data flow, the monthly cycle, current and target AWS topology | 30 min |
| 03 | [Integration Contracts and Gates](03-integration-contracts-and-gates.md) | Engineers, architects, operations staff | Formal rules for how the systems communicate, mandatory safety gates, change management | 20 min |
| 04 | [Implementation Roadmap](04-implementation-roadmap.md) | Engineers, AI agents performing implementation | Phased plan with discrete testable steps, exit criteria, dependency graph | 45 min |
| 05 | [Technical Findings](05-technical-findings.md) | Engineers, AI agents executing code changes | Every technical issue found across all three repos, with severity, file paths, and proposed fixes | 60+ min (reference) |
| 06 | [Agent Skills and Automation](06-agent-skills-and-automation.md) | Engineers, operations staff, AI agents planning changes | Documented procedures and skills; what coding assistants can do today; proposed new skills | 30 min |
| 07 | [Greenfield Vision](07-greenfield-vision.md) | Leadership, architects planning long-term | What the pipeline would look like if built from scratch today; aspirational target state | 15 min |
| 08 | [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md) | Engineers, architects, leadership | Beta proposal: hub-controlled ingest triggers, S3 deposit monitoring, tiered confirmation workflows | 25 min |
| 09 | [Observability and Monitoring Architecture](09-observability-architecture.md) | Operations staff, architects, engineers, leadership | Phased plan for centralized logging, CloudWatch monitoring, Slack integration, and self-service pipeline visibility | 25 min |

---

## How to Read These Documents

**If you are a non-technical audience:** Read the [Executive Summary](01-executive-summary.md). It defines every technical term it uses and is written for a non-engineering audience. If you want to understand the monitoring and visibility improvements in concrete terms, read the [Why This Document Exists](09-observability-architecture.md#why-this-document-exists) and [What Changes](09-observability-architecture.md#what-changes) sections of the [Observability Architecture](09-observability-architecture.md) as well.

**If you are evaluating this pipeline for partnership or funding:** Read the [Executive Summary](01-executive-summary.md) for the full picture, then the [What This Costs Today](#what-this-costs-today) section above. The investment required to implement this plan is documented in the cost and timeline sections of the [Executive Summary](01-executive-summary.md).

**If you are an engineer at the receiving organization:** Start with [System Architecture](02-system-architecture.md) for the system overview, then [Integration Contracts and Gates](03-integration-contracts-and-gates.md) for the contracts that must not be broken. Use [Implementation Roadmap](04-implementation-roadmap.md) as your implementation guide and [Technical Findings](05-technical-findings.md) as your work queue.

**If you are implementing changes (human or coding assistant):** Read [Integration Contracts and Gates](03-integration-contracts-and-gates.md) first -- it defines the contracts you must not violate. Use [Implementation Roadmap](04-implementation-roadmap.md) as your task list, verifying exit criteria before moving to the next phase. Use [Technical Findings](05-technical-findings.md) as a reference for specific code changes. Read [Agent Skills and Automation](06-agent-skills-and-automation.md) for procedures, skills, and where they live in the codebase.

**If you are planning a greenfield rebuild:** Read [Greenfield Vision](07-greenfield-vision.md) for the aspirational architecture, then [System Architecture](02-system-architecture.md) for the constraints of the current state. Read [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md) for the hub scheduling and communication layer that should be part of any rebuild.

**If you are focused on hub relations and scheduling:** Read [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md) for the beta proposal on hub readiness automation -- hub-triggered ingests, S3 deposit monitoring, and optional review-before-publish integrated into the existing mapping summary email.

**If you want to understand pipeline monitoring and Slack integration:** Read [Observability Architecture](09-observability-architecture.md) for the observability architecture -- structured logging, CloudWatch monitoring, Slack slash commands, and the investment strategy for current vs. greenfield architecture.

---

## Documentation for Engineers: Procedures and Coding Assistants

When engineering resources are constrained, **documented procedures** allow a small team -- or a coding assistant that follows the same procedures -- to operate and extend the pipeline without deep codebase expertise. This is for **internal** use: the domain is well understood, and the tools are there to reduce the burden on any individual engineer.

**In this repo:**

| What | Where |
|------|--------|
| Runbook and environment guide | [AGENTS.md](../../AGENTS.md) |
| Cursor skills (ingestion3) | `.cursor/skills/dpla-*` (e.g. dpla-run-ingest, dpla-orchestrator, dpla-s3-and-aws) |
| Claude Code rules | `.claude/rules/*.md` (loaded automatically when using Claude Code in this repo) |
| Claude Code skills | `.claude/skills/*` |
| Rules index (which rule when) | [.claude/rules/README.md](../../.claude/rules/README.md) |

**External documentation:**

- [Cursor -- Agent Skills](https://cursor.com/docs/context/skills): How Cursor discovers and uses `SKILL.md` procedure files.
- [Claude Code -- Skills](https://code.claude.com/docs/en/skills): How to extend Claude Code with skills.

Full context, proposed skills for sparkindexer and ingest-wikimedia, and how to add new skills are in [Agent Skills and Automation](06-agent-skills-and-automation.md).

---

## Related Documents

- [../dpla-unified-pipeline-proposal.md](../dpla-unified-pipeline-proposal.md) -- Original unification proposal
- [../../AGENTS.md](../../AGENTS.md) -- ingestion3 agent guide (environment, runbooks, notifications)
- [../../GOLDEN_PATH.md](../../GOLDEN_PATH.md) -- ingestion3 standard workflow
- [../../scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) -- ingestion3 script reference

---
