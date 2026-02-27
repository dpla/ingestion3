# AI Context Refactor Plan

**Purpose:** Create a single canonical source for AI instructions (`docs/ai-context/`), sync to Cursor and Claude via thin wrappers, and clearly mark deprecated files for migration.

**Plan file:** `docs/plans/ai-context-refactor.md`
**Future work context:** `docs/ai-context/INSTRUCTIONS.md`
**Change log (plan refinements):** `docs/ai-context/skill-rule-consolidation-refactor.md`

---

## Part 1: Naming Conventions

All skills and rules in `docs/ai-context/` follow these conventions:

### Rules (always-loaded workflows)

Rules are **domain groups** with consistent `{domain}.md` or `{domain}-{qualifier}.md` patterns:

- **Pattern:** `{domain}.md` or `{domain}-{qualifier}.md`
- **Examples:** `ingestion.md`, `orchestrator.md`, `aws-tools.md`, `notifications.md`, `validation.md`, `script-standards.md`
- **Rules:**
  - Lowercase, hyphen-separated
  - Domain or group name (ingestion, aws-tools, script-standards)
  - No `dpla-` prefix (rules are project-scoped by location)

### Skills (on-demand invocable commands)

- **Pattern:** `dpla-{domain}-{action}.md` or `dpla-{noun}.md`
- **Examples:** `dpla-ingest-status.md`, `dpla-hub-info.md`, `dpla-s3-ops.md`, `send-email.md`
- **Rules:**
  - Prefix `dpla-` for DPLA-specific workflows (ingest, hub, orchestrator, etc.)
  - Exception: Generic utilities keep short names (`send-email.md`, not `dpla-send-email.md`)
  - Lowercase, hyphen-separated
  - Descriptive: `dpla-s3-ops` (find + sync + check), not `dpla-s3-latest` or `dpla-s3-and-aws`

### File layout

```
docs/ai-context/
├── README.md
├── MIGRATION.md              # Old -> New mapping (this plan references it)
├── INSTRUCTIONS.md           # Future work context (TODOs, not for implementation)
├── rules/
│   ├── aws-tools.md          # S3 sync, S3 latest, AWS ops (was s3-and-aws)
│   ├── ingestion.md          # Run ingest, harvest, remap (was run-ingest)
│   ├── orchestrator.md
│   ├── notifications.md      # Slack/email, what to report (from verify-and-notify)
│   ├── validation.md         # Verify outputs, escalation (from verify-and-notify)
│   └── script-standards.md   # POSIX, common.sh, SCRIPTS.md (was script-workflow)
└── skills/
    ├── dpla-ingest-status.md
    ├── dpla-hub-info.md
    ├── dpla-monitor-ingest-remap.md
    ├── dpla-oai-harvest-watch.md
    ├── dpla-staged-report.md
    ├── dpla-s3-ops.md        # Merged s3-latest + dpla-s3-and-aws; also skill wrapper for aws-tools rule
    ├── send-email.md
    ├── dpla-monthly-emails.md
    ├── dpla-community-webs-ingest.md
    ├── dpla-ingest-debug.md
    ├── dpla-run-ingest.md    # Skill wrapper for ingestion rule (Cursor-only)
    ├── dpla-orchestrator.md  # Skill wrapper for orchestrator rule (Cursor-only)
    ├── dpla-verify-and-notify.md  # Skill wrapper for notifications (Cursor-only)
    └── dpla-script-workflow.md    # Skill wrapper for script-standards rule (Cursor-only)
```

---

## Part 2: Archive Old Files (Not DEPRECATED_ Prefix)

**Avoid `DEPRECATED_` prefix:** Cursor's skill format requires `name` in SKILL.md to match the parent folder name. Renaming folders to `DEPRECATED_<name>` would either require changing the skill name (breaking discovery) or create a mismatch (uncertain behavior). Tools may still load prefixed directories.

**Preferred approach:** Move existing `.cursor/` and `.claude/` content to an archive folder **outside** the tool discovery path. Tools will not load archived content; sync generates fresh outputs.

### Archive strategy

| Location | Old path | Archive path |
|----------|----------|--------------|
| .cursor/skills | .cursor/skills/dpla-*/ | docs/ai-context/archive/cursor-skills/ |
| .claude/rules | .claude/rules/*.md | docs/ai-context/archive/claude-rules/ |
| .claude/skills | .claude/skills/*/ | docs/ai-context/archive/claude-skills/ |

**Steps:**
1. Create `docs/ai-context/archive/{cursor-skills,claude-rules,claude-skills}/`
2. Move (don't rename) existing content into archive subdirs, preserving structure
3. Run sync to generate new `.cursor/` and `.claude/` content
4. Later: delete archive after migration is verified (see INSTRUCTIONS.md)

---

## Part 3: Old Skill/Rule → New Skill/Rule Mapping

Use this mapping to investigate breaking changes after the refactor. When updating references across the repo, replace old paths with new ones.

### Cursor skills

| Old (deprecated) | New (docs/ai-context) | Sync target |
|------------------|-----------------------|-------------|
| .cursor/skills/dpla-run-ingest/ | docs/ai-context/rules/ingestion.md + skills/dpla-run-ingest.md | .cursor/skills/dpla-run-ingest/ |
| .cursor/skills/dpla-orchestrator/ | docs/ai-context/rules/orchestrator.md + skills/dpla-orchestrator.md | .cursor/skills/dpla-orchestrator/ |
| .cursor/skills/dpla-verify-and-notify/ | docs/ai-context/rules/notifications.md + skills/dpla-verify-and-notify.md | .cursor/skills/dpla-verify-and-notify/ |
| .cursor/skills/dpla-script-workflow/ | docs/ai-context/rules/script-standards.md + skills/dpla-script-workflow.md | .cursor/skills/dpla-script-workflow/ |
| .cursor/skills/dpla-s3-and-aws/ | docs/ai-context/rules/aws-tools.md + skills/dpla-s3-ops.md | .cursor/skills/dpla-s3-ops/ |
| .cursor/skills/dpla-ingest-status/ | docs/ai-context/skills/dpla-ingest-status.md | .cursor/skills/dpla-ingest-status/ |
| .cursor/skills/dpla-hub-info/ | docs/ai-context/skills/dpla-hub-info.md | .cursor/skills/dpla-hub-info/ |
| .cursor/skills/dpla-monitor-ingest-remap/ | docs/ai-context/skills/dpla-monitor-ingest-remap.md | .cursor/skills/dpla-monitor-ingest-remap/ |
| .cursor/skills/dpla-oai-harvest-watch/ | docs/ai-context/skills/dpla-oai-harvest-watch.md | .cursor/skills/dpla-oai-harvest-watch/ |
| .cursor/skills/dpla-staged-report/ | docs/ai-context/skills/dpla-staged-report.md | .cursor/skills/dpla-staged-report/ |
| .cursor/skills/dpla-monthly-emails/ | docs/ai-context/skills/dpla-monthly-emails.md | .cursor/skills/dpla-monthly-emails/ |
| .cursor/skills/dpla-community-webs-ingest/ | docs/ai-context/skills/dpla-community-webs-ingest.md | .cursor/skills/dpla-community-webs-ingest/ |
| .cursor/skills/dpla-ingest-debug/ | docs/ai-context/skills/dpla-ingest-debug.md | .cursor/skills/dpla-ingest-debug/ |

### Claude rules

| Old (deprecated) | New (docs/ai-context) | Sync target |
|------------------|-----------------------|-------------|
| .claude/rules/orchestrator.md | docs/ai-context/rules/orchestrator.md | .claude/rules/orchestrator.md |
| .claude/rules/run-ingest.md | docs/ai-context/rules/ingestion.md | .claude/rules/ingestion.md |
| .claude/rules/s3-and-aws.md | docs/ai-context/rules/aws-tools.md | .claude/rules/aws-tools.md |
| .claude/rules/verify-and-notify.md | docs/ai-context/rules/notifications.md, rules/validation.md | .claude/rules/notifications.md, .claude/rules/validation.md |
| .claude/rules/script-workflow.md | docs/ai-context/rules/script-standards.md | .claude/rules/script-standards.md |

### Claude skills

| Old (deprecated) | New (docs/ai-context) | Sync target |
|------------------|-----------------------|-------------|
| .claude/skills/dpla-ingest-status/ | docs/ai-context/skills/dpla-ingest-status.md | .claude/skills/dpla-ingest-status/ |
| .claude/skills/dpla-hub-info/ | docs/ai-context/skills/dpla-hub-info.md | .claude/skills/dpla-hub-info/ |
| .claude/skills/dpla-monitor-ingest-remap/ | docs/ai-context/skills/dpla-monitor-ingest-remap.md | .claude/skills/dpla-monitor-ingest-remap/ |
| .claude/skills/dpla-oai-harvest-watch/ | docs/ai-context/skills/dpla-oai-harvest-watch.md | .claude/skills/dpla-oai-harvest-watch/ |
| .claude/skills/dpla-staged-report/ | docs/ai-context/skills/dpla-staged-report.md | .claude/skills/dpla-staged-report/ |
| .claude/skills/s3-latest/ | docs/ai-context/skills/dpla-s3-ops.md (merged) | .claude/skills/dpla-s3-ops/ |
| .claude/skills/send-email/ | docs/ai-context/skills/send-email.md | .claude/skills/send-email/ |

### Rule-to-skill wrappers

| Rule | Skill wrapper |
|------|---------------|
| ingestion.md | dpla-run-ingest |
| aws-tools.md | dpla-s3-ops |
| notifications.md | dpla-verify-and-notify |
| validation.md | Always-loaded rule (keep separate; used by dpla-verify-and-notify and dpla-ingest-debug) |
| script-standards.md | dpla-script-workflow |
| orchestrator.md | dpla-orchestrator |

### Rule-to-skill composition (how sync generates output)

- **Claude:** Rules are always loaded from `.claude/rules/`; skills are on-demand from `.claude/skills/`. Rule-wrapping skills (dpla-run-ingest, etc.) are **Cursor-only** — Claude gets the rules directly.
- **Cursor:** Cursor loads skills only; it does not auto-load `.claude/rules/`. Rule-wrapping skills must be **self-contained**: the generated SKILL.md includes enough workflow detail to run without the rule file. The sync script produces `skills/<name>.md` → `.cursor/skills/<name>/SKILL.md` with full content.
- **Sync behavior:** For rule-wrappers, the skill source file should embed or summarize the rule so Cursor users get the same behavior. When a source file is removed from docs/ai-context/, sync should delete the corresponding generated output (sync is destructive/overwrites targets).

### Content migration notes

When populating docs/ai-context/: **aws-tools.md** — Merge s3-and-aws + s3-latest (find latest in S3). **ingestion.md** — From run-ingest.md. **notifications.md** — From verify-and-notify: "What to Report", "Where to Notify", "Test notifications". **validation.md** — From verify-and-notify: "Verification" section, escalation reports, _SUCCESS checks.

### Files that reference old paths (for breaking-change audit)

| File | References |
|------|------------|
| AGENTS.md | .claude/rules/run-ingest.md, dpla-run-ingest (update to ingestion.md) |
| CLAUDE.md | .claude/rules/, .claude/skills/ |
| .claude/rules/README.md | .cursor/skills/dpla-* |
| .claude/rules/run-ingest.md | .cursor/skills/dpla-ingest-debug/SKILL.md (→ ingestion.md) |
| .claude/skills/dpla-ingest-status/SKILL.md | dpla-ingest-debug |
| .cursor/skills/dpla-ingest-status/SKILL.md | dpla-ingest-debug |
| .cursor/skills/dpla-run-ingest/SKILL.md | dpla-ingest-debug |
| runbooks/08-community-webs.md | .cursor/skills/dpla-community-webs-ingest/SKILL.md |
| scripts/SCRIPTS.md | .claude/skills/send-email/, .cursor/skills/dpla-monthly-emails/ |
| docs/plans/ingest-pipeline-decomposition.md | .cursor/skills/dpla-*, .claude/skills/* |
| docs/plans/ingest-pipeline-decomposition-context.md | .cursor/skills/*, .claude/skills/* |
| docs/pipeline-unification/*.md | .cursor/skills/, .claude/skills/ |

### AGENTS.md and CLAUDE.md updates (Phase 5)

When updating references, apply these edits:

- **AGENTS.md:** In the Ingest workflows table, change `run-ingest.md` → `ingestion.md`; add a note that rules/skills are synced from `docs/ai-context/`.
- **CLAUDE.md:** Update the Ingest and script workflows section to list new rule names (ingestion, aws-tools, script-standards, notifications, validation, orchestrator); add note that rules and skills are generated from `docs/ai-context/` via sync.

See MIGRATION.md for the full list of edits.

---

## Part 4: Implementation Scope

**Implemented:**
- Create `docs/ai-context/` structure with naming conventions
- Archive existing .cursor and .claude content (move to docs/ai-context/archive/)
- Create `docs/ai-context/MIGRATION.md` with the full old→new mapping
- Create `docs/ai-context/INSTRUCTIONS.md` with future work TODOs
- Create sync script `scripts/ai-context/sync.sh` (idempotent, destructive, `--dry-run`)
- Add sync tests `scripts/tests/test-ai-context-sync.sh` (105 tests)
- Update AGENTS.md, CLAUDE.md, READMEs to point to docs/ai-context and sync workflow
- Pre-commit hook (`hooks/pre-commit`) — auto-syncs on commit; install with `./hooks/install.sh`
- Watch mode (`scripts/ai-context/watch.sh`) — fswatch/inotifywait live sync on save

**Not in scope (TODOs / future work):**
- CI job for sync validation
- Updating all cross-references in the project
- Adding adapters for Codex, Lightweave, or other tools

---

## Part 5: For Beginners — Why and Impact

### Why

Multiple AI tools (Cursor, Claude Code) each have their own config. Editing the same instructions in several places causes drift. A single source (`docs/ai-context/`) plus sync keeps everything consistent.

### What we're doing

- **Canonical source:** `docs/ai-context/` — all AI instructions live here
- **Archived files:** Old .cursor and .claude content is moved to `docs/ai-context/archive/` (outside tool discovery)
- **Sync:** `./scripts/ai-context/sync.sh` generates .cursor and .claude outputs from docs/ai-context
- **Naming:** Consistent rules (e.g. ingestion, aws-tools, script-standards) and skills (e.g. dpla-ingest-status)

### Impact on tools

| Tool | Edit | Read |
|------|------|------|
| Cursor | docs/ai-context/ only | .cursor/skills/<name>/ (generated from sync) |
| Claude Code | docs/ai-context/ only | .claude/rules/, .claude/skills/ (generated) |

---

## Part 6: For Agents — Implementation Steps (Phased Rollout)

Use a phased approach to validate each step before proceeding.

| Phase | Steps | Validation |
|-------|-------|------------|
| **1** | Create `docs/ai-context/` with README.md, MIGRATION.md, INSTRUCTIONS.md; add `rules/` and `skills/` subdirs | Directory structure exists |
| **2** | Populate from current files (copy content from .cursor/.claude into docs/ai-context/) | Manual diff of source vs copied |
| **3** | Create `scripts/ai-context/sync.sh`; run sync to generate outputs (e.g. to temp dirs for testing) | Script runs; output structure correct |
| **4** | Move existing .cursor/ and .claude/ content to `docs/ai-context/archive/`; run sync to generate new outputs | Test Cursor and Claude with generated files |
| **5** | Update AGENTS.md, CLAUDE.md, .claude/rules/README.md, .claude/skills/README.md, and all cross-references (see MIGRATION.md) | Grep for old paths; none remain |
| **6** | Add `scripts/tests/test-ai-context-sync.sh` | Tests pass |
| **7** | (Later) Delete archive after migration verified | — |

**Detailed steps:**
1. Create `docs/ai-context/` with README.md, MIGRATION.md, INSTRUCTIONS.md
2. Create `docs/ai-context/rules/` and `docs/ai-context/skills/` subdirs
3. Populate docs/ai-context/ with migrated content (copy from archive after creating it, or from current .cursor/.claude)
4. Create `scripts/ai-context/sync.sh`
5. Create `docs/ai-context/archive/{cursor-skills,claude-rules,claude-skills}/`
6. Move existing .cursor/skills/* into archive/cursor-skills/, .claude/rules/* into archive/claude-rules/, .claude/skills/* into archive/claude-skills/
7. Run sync to generate new .cursor and .claude content
8. Add scripts/tests/test-ai-context-sync.sh
9. Update AGENTS.md, CLAUDE.md, .claude/rules/README.md, .claude/skills/README.md (see MIGRATION.md for AGENTS.md/CLAUDE.md edits)

---

## Part 7: Future Work

See `docs/ai-context/INSTRUCTIONS.md` for remaining TODOs. Summary:

- [x] ~~Add pre-commit hook to run sync when docs/ai-context/** changes~~ (done: `hooks/pre-commit`)
- [x] ~~Add watch mode (scripts/ai-context/watch.sh) for local dev~~ (done: `scripts/ai-context/watch.sh`)
- [x] ~~Sync idempotence~~ (done: sync.sh is destructive and idempotent)
- [ ] Add CI job to validate sync (fail if generated files are stale)
- [ ] Update all cross-references (runbooks, SCRIPTS.md, docs/plans/*.md, etc.) to new paths
- [ ] Remove archive (docs/ai-context/archive/) after migration is verified
- [ ] Add adapter for Codex / Lightweave if needed
- [ ] Snapshot tests (approval-style diff of generated vs committed)
- [ ] Link validation (rewrite relative links during sync)
