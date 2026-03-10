# AI Context — Canonical Source for Agent Instructions

All AI agent rules and skills (Cursor, Claude Code, etc.) are authored here. Run `./scripts/ai-context/sync.sh` to generate tool-specific outputs in `.cursor/` and `.claude/`.

## Quick Start (for contributors)

1. **Edit:** `docs/ai-context/rules/ingestion.md` or `docs/ai-context/skills/dpla-hub-info.md`
2. **Commit:** The pre-commit hook runs sync automatically and stages generated files
3. **Verify:** Check `.cursor/skills/` and `.claude/` for updated files

That's it — the pre-commit hook handles sync for you.

## Automation

### Pre-commit hook (always active)

When you commit changes to `docs/ai-context/rules/` or `docs/ai-context/skills/`, the git pre-commit hook automatically runs `sync.sh` and re-stages the generated `.cursor/` and `.claude/` files. No manual sync needed.

**Install (first time or after clone):**

```bash
./hooks/install.sh
```

### Watch mode (optional, for active editing)

For live sync while editing (Cursor/Claude pick up changes immediately):

```bash
# Requires: brew install fswatch (macOS) or apt install inotify-tools (Linux)
./scripts/ai-context/watch.sh
```

This watches `docs/ai-context/{rules,skills}/` and runs sync on every save.

### Manual sync

```bash
./scripts/ai-context/sync.sh            # Full sync
./scripts/ai-context/sync.sh --dry-run  # Preview changes
```

## Layout

- **rules/** — Always-loaded domain groups (ingestion, orchestrator, aws-tools, notifications, validation, script-standards)
- **skills/** — On-demand invocable commands (dpla-ingest-status, dpla-hub-info, etc.)
- **archive/** — Old .cursor and .claude content preserved for reference
- **MIGRATION.md** — Old → New path mapping for breaking-change investigation
- **INSTRUCTIONS.md** — Remaining future work TODOs

## Naming Conventions

- **Rules:** Domain groups — `{domain}.md` or `{domain}-{qualifier}.md` (e.g. ingestion.md, aws-tools.md, script-standards.md)
- **Skills:** `dpla-{domain}-{action}.md` or generic `send-email.md`

See [docs/plans/ai-context-refactor.md](../plans/ai-context-refactor.md) for full conventions.

## Cursor vs Claude distribution

- **Rules** go to `.claude/rules/` only (Claude auto-loads them). Cursor gets rule content via rule-wrapping skills.
- **Rule-wrapping skills** (dpla-run-ingest, dpla-orchestrator, dpla-verify-and-notify, dpla-script-workflow, dpla-s3-ops) go to `.cursor/skills/` only — they're Cursor-only because Claude gets the rules directly.
- **Shared skills** (dpla-ingest-status, dpla-hub-info, send-email, etc.) go to both `.cursor/skills/` and `.claude/skills/`.
