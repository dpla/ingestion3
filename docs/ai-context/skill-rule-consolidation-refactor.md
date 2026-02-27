# Skill/Rule Consolidation Refactor — Change Log

This document records the changes made during the plan refinement (Feb 2025) to support future refactoring. It serves as a reference when planning or resuming the AI context consolidation work.

---

## What Was Done

### 1. docs/plans/ai-context-refactor.md

| Change | Reason |
|--------|--------|
| **DEPRECATED_ prefix → archive approach** | Cursor's skill format requires `name` to match parent folder; prefixed dirs may still load or create mismatches. Moving to `docs/ai-context/archive/` guarantees tools won't load old content. |
| **Removed duplicate dpla-s3-ops.md** from file layout | Plan listed dpla-s3-ops twice (merged skill and wrapper). Clarified: one file serves both roles. |
| **Added (Cursor-only)** labels to rule-wrapping skills | Makes it explicit that dpla-run-ingest, dpla-orchestrator, etc. are Cursor-only; Claude gets rules directly. |
| **validation.md** | Changed from "May fold into dpla-verify-and-notify or dpla-ingest-debug" to "Always-loaded rule (keep separate)". Validation must remain an always-loaded rule. |
| **Rule-to-skill composition section** | Documented how sync generates output: Cursor skills must be self-contained; Claude gets rules + skills; sync is destructive. |
| **Phased rollout** | Replaced monolithic steps with 7-phase table; each phase has validation before proceeding. |
| **AGENTS.md and CLAUDE.md updates** | Added explicit section with the edits to apply during reference-update phase. |

### 2. docs/ai-context/INSTRUCTIONS.md

| Change | Reason |
|--------|--------|
| **"When to read"** in How to Use | Added onboarding vs resuming distinction; point to README first for new contributors. |
| **Link validation** | Clarified that sync should rewrite relative links when emitting to .cursor/.claude. |
| **Sync idempotence** | New "Sync script behavior" section: sync overwrites targets; removal of source file should delete generated output. |
| **Remove deprecated → Remove archive** | Aligned terminology with plan (archive, not DEPRECATED_). |

### 3. docs/ai-context/MIGRATION.md

| Change | Reason |
|--------|--------|
| **One Example section** | run-ingest → ingestion before/after table for quick lookup. |
| **Skill Distribution table** | Cursor vs Claude: which skills and rules each tool gets. |
| **AGENTS.md and CLAUDE.md Specific Edits** | Exact edits for AGENTS.md (Ingest workflows table, sync note) and CLAUDE.md (rule list, sync note). |

### 4. docs/ai-context/README.md

| Change | Reason |
|--------|--------|
| **Quick Start** | 4-step flow (Edit → Sync → Verify → Commit) for contributors. |
| **Deprecated → Archived** | Terminology aligned with plan. |
| **Cursor-only note** | Explains that rule-wrappers are Cursor-only; Claude uses rules directly. |

---

## Decisions Captured

1. **Archive, not rename:** Use `docs/ai-context/archive/` instead of `DEPRECATED_` prefix for old content.
2. **validation.md stays a rule:** Do not fold into skills; keep as always-loaded rule.
3. **dpla-s3-ops is one file:** Merged skill + wrapper; no duplication in layout.
4. **Sync is destructive:** Idempotent; removes generated files when source is removed.
5. **Phased rollout:** 7 phases with validation gates; reduces risk of large-bang migration.

---

## Files Modified (Phase 5, completed)

- **AGENTS.md** — Ingest workflows table updated (run-ingest → ingestion, verify-and-notify → notifications); sync note added.
- **CLAUDE.md** — Rule list updated to new names; sync note added.

---

## References

- Main plan: [docs/plans/ai-context-refactor.md](../plans/ai-context-refactor.md)
- Future work: [docs/ai-context/INSTRUCTIONS.md](INSTRUCTIONS.md)
- Migration mapping: [docs/ai-context/MIGRATION.md](MIGRATION.md)
