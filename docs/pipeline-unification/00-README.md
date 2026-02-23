# DPLA Pipeline Unification -- Synthesized Plan

**Last updated:** February 2026
**Status:** Final draft -- ready for implementation handoff
**Origin:** Synthesized from two independent analyses (Opus 4.6 and Codex) plus the original unification proposal

---

## What This Folder Contains

This folder documents the full DPLA data pipeline -- three independent software projects that together harvest library and museum records, make them searchable on dp.la, and upload eligible media to Wikimedia Commons. These documents were created to support two goals:

1. **Simplify the pipeline end-to-end** by adding coordination, monitoring, and reliability across the three existing systems.
2. **Enable handoff** by packaging the pipeline so a future team or organization can understand, operate, and maintain it.

The plan is designed for incremental implementation. Each phase delivers standalone value. Each step within a phase is independently testable. A future agent (AI or human) can work through the implementation roadmap step by step without needing to understand the entire plan upfront.

## Document Guide

| # | Document | Audience | What You'll Learn | Reading Time |
|---|----------|----------|-------------------|--------------|
| 01 | [Executive Summary](01-executive-summary.md) | Director, Community Manager, leadership at receiving org | What the pipeline does, why it's being unified, what changes, key risks, strategic principles | 15 min |
| 02 | [System Architecture](02-system-architecture.md) | Technical managers, architects, engineers | How the three systems connect, data flow, the monthly cycle, current and target AWS topology | 30 min |
| 03 | [Integration Contracts and Gates](03-integration-contracts-and-gates.md) | Engineers, architects, operations staff | Formal rules for how the systems communicate, mandatory safety gates, change management | 20 min |
| 04 | [Implementation Roadmap](04-implementation-roadmap.md) | Engineers, AI agents performing implementation | Phased plan with discrete testable steps, exit criteria, dependency graph | 45 min |
| 05 | [Technical Findings](05-technical-findings.md) | Engineers, AI agents executing code changes | Every technical issue found across all three repos, with severity, file paths, and proposed fixes | 60+ min (reference) |
| 06 | [Agent Skills and Automation](06-agent-skills-and-automation.md) | Engineers, AI agents, operations staff | What AI agents can do today, proposed new skills, end-to-end agent pipeline | 30 min |
| 07 | [Greenfield Vision](07-greenfield-vision.md) | Leadership at receiving org, architects planning long-term | What the pipeline would look like if built from scratch today; aspirational target state | 15 min |

## How to Read These Documents

**If you are a Director or Community Manager:** Read 01 only. It defines every technical term it uses. If you want more detail on a specific topic, 02 provides architecture context.

**If you are an engineer at the receiving organization:** Start with 02 for the big picture, then 03 for the rules that must not be broken. Use 04 as your implementation guide and 05 as your work queue.

**If you are an AI agent implementing changes:** Read 03 first -- it defines the contracts you must not violate. Use 04 as your task list -- work through steps in order, verifying exit criteria before moving to the next phase. Use 05 as a reference for specific code changes. Read 06 for your role in the pipeline.

**If you are planning a greenfield rebuild:** Read 07 for the aspirational architecture, then 02 for the constraints of the current state that inform migration.

## Synthesis Notes

This document set was produced by reviewing two independent analyses:

- **Opus analysis** (files `01-04` original series): Detailed technical findings with file paths, line numbers, and proposed code fixes. Strong on implementation specifics, agent skills, and cost analysis. Phased into 5 concrete work phases (0-4) with task-level estimates.

- **Codex analysis** (files `CODEX-01` through `CODEX-05`): Architecture-first reassessment with formal safety gates, integration contracts, risk/opportunity matrix, and OpenTofu IaC proposal. Strong on governance, change management, and phase acceptance criteria.

The synthesis follows these principles:
- **Safer implementation**: When the plans differ, prefer the approach with smaller steps and independent testability.
- **Orchestration-first**: Keep existing domain engines; add coordination, not rewrites.
- **Contracts before code**: Formalize integration rules before building automation on top of them.
- **Observability before autonomy**: See what's happening before automating decisions.
- **Explicit gates**: Every high-impact transition has a pass/fail gate with rollback.

The Codex source files (`CODEX-*.md`) remain in this directory for reference.

## Related Documents

- [../dpla-unified-pipeline-proposal.md](../dpla-unified-pipeline-proposal.md) -- The original proposal that seeded both analyses
- [../../AGENTS.md](../../AGENTS.md) -- ingestion3 agent guide (environment, runbooks, notifications)
- [../../GOLDEN_PATH.md](../../GOLDEN_PATH.md) -- ingestion3 standard workflow
- [../../scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) -- ingestion3 script reference

## Version History

| Date | Change | Author |
|------|--------|--------|
| Feb 2026 | Original Opus analysis: red-team findings, phased implementation, agent skills | DPLA Engineering |
| Feb 2026 | Independent Codex reassessment: architecture, risk matrix, contracts, IaC proposal | DPLA Engineering |
| Feb 2026 | Synthesized final plan: merged analyses, added greenfield vision, refined phase model | DPLA Engineering |
