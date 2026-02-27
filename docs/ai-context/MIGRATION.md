# AI Context Migration — Old to New Mapping

Use this file to investigate breaking changes and update references after the refactor.

## One Example (run-ingest → ingestion)

| Before | After |
|--------|-------|
| `.claude/rules/run-ingest.md` | `docs/ai-context/rules/ingestion.md` → syncs to `.claude/rules/ingestion.md` |
| `dpla-run-ingest` skill (Cursor) | Same name; source: `docs/ai-context/skills/dpla-run-ingest.md` + `rules/ingestion.md` |
| AGENTS.md link: `run-ingest.md` | `ingestion.md` |

## Skill Distribution (Cursor vs Claude)

| Location | Cursor | Claude |
|----------|--------|--------|
| Rule-wrappers (dpla-run-ingest, dpla-orchestrator, dpla-verify-and-notify, dpla-script-workflow, dpla-s3-ops) | Yes (skills) | No — Claude gets rules directly |
| Rules (ingestion, orchestrator, notifications, validation, aws-tools, script-standards) | No | Yes (always loaded) |
| Other skills (dpla-ingest-status, dpla-hub-info, send-email, etc.) | Yes | Yes |

## Cursor Skills

| Old (deprecated) | New canonical | Sync target |
|------------------|---------------|-------------|
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

## Claude Rules

| Old (deprecated) | New canonical | Sync target |
|------------------|---------------|-------------|
| .claude/rules/orchestrator.md | docs/ai-context/rules/orchestrator.md | .claude/rules/orchestrator.md |
| .claude/rules/run-ingest.md | docs/ai-context/rules/ingestion.md | .claude/rules/ingestion.md |
| .claude/rules/s3-and-aws.md | docs/ai-context/rules/aws-tools.md | .claude/rules/aws-tools.md |
| .claude/rules/verify-and-notify.md | docs/ai-context/rules/notifications.md, rules/validation.md | .claude/rules/notifications.md, .claude/rules/validation.md |
| .claude/rules/script-workflow.md | docs/ai-context/rules/script-standards.md | .claude/rules/script-standards.md |

## Claude Skills

| Old (deprecated) | New canonical | Sync target |
|------------------|---------------|-------------|
| .claude/skills/dpla-ingest-status/ | docs/ai-context/skills/dpla-ingest-status.md | .claude/skills/dpla-ingest-status/ |
| .claude/skills/dpla-hub-info/ | docs/ai-context/skills/dpla-hub-info.md | .claude/skills/dpla-hub-info/ |
| .claude/skills/dpla-monitor-ingest-remap/ | docs/ai-context/skills/dpla-monitor-ingest-remap.md | .claude/skills/dpla-monitor-ingest-remap/ |
| .claude/skills/dpla-oai-harvest-watch/ | docs/ai-context/skills/dpla-oai-harvest-watch.md | .claude/skills/dpla-oai-harvest-watch/ |
| .claude/skills/dpla-staged-report/ | docs/ai-context/skills/dpla-staged-report.md | .claude/skills/dpla-staged-report/ |
| .claude/skills/s3-latest/ | docs/ai-context/skills/dpla-s3-ops.md (merged) | .claude/skills/dpla-s3-ops/ |
| .claude/skills/send-email/ | docs/ai-context/skills/send-email.md | .claude/skills/send-email/ |

## Files Referencing Old Paths (audit for breaking changes)

| File | References to update |
|------|----------------------|
| AGENTS.md | .claude/rules/run-ingest.md → ingestion.md, dpla-run-ingest |
| CLAUDE.md | .claude/rules/, .claude/skills/ |
| .claude/rules/README.md | .cursor/skills/dpla-* |
| .claude/rules/run-ingest.md (→ ingestion.md) | .cursor/skills/dpla-ingest-debug/SKILL.md |
| runbooks/08-community-webs.md | .cursor/skills/dpla-community-webs-ingest/SKILL.md |
| scripts/SCRIPTS.md | .claude/skills/send-email/, .cursor/skills/dpla-monthly-emails/ |
| docs/plans/ingest-pipeline-decomposition.md | .cursor/skills/*, .claude/skills/* |
| docs/plans/ingest-pipeline-decomposition-context.md | .cursor/skills/*, .claude/skills/* |
| docs/pipeline-unification/00-README.md | .cursor/skills/, .claude/skills/ |
| docs/pipeline-unification/06-agent-skills-and-automation.md | .cursor/skills/, .claude/skills/ |

## AGENTS.md and CLAUDE.md Specific Edits

### AGENTS.md

- **Ingest workflows table (lines ~25–29):** Change `run-ingest.md` → `ingestion.md`, `verify-and-notify.md` → `notifications.md` (and optionally link validation.md).
- Add note: "Rules and skills are synced from `docs/ai-context/`. Edit there; run `./scripts/ai-context/sync.sh`."

### CLAUDE.md

- **Ingest and script workflows section:** Update rule list from `orchestrator, run-ingest, script-workflow, verify-and-notify, s3-and-aws` to `orchestrator, ingestion, script-standards, notifications, validation, aws-tools`.
- Add note: "Rules and skills are generated from `docs/ai-context/`. Edit there; run `./scripts/ai-context/sync.sh`."
