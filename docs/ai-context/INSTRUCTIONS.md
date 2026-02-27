# AI Context — Future Work

Remaining TODOs for the AI context refactor. Items are removed as they're implemented.

---

## TODOs

### Sync automation

- [ ] **CI validation:** Add GitHub Actions job that runs sync and fails if `git diff .cursor .claude` is non-empty when docs/ai-context or scripts/ai-context change.

### Migration and references

- [ ] **Update cross-references:** Update all files that reference old paths (see MIGRATION.md mapping). Key files: runbooks/08-community-webs.md, scripts/SCRIPTS.md, docs/plans/*.md, docs/pipeline-unification/*.md.
- [ ] **Remove archive:** After migration is verified and all references updated, delete `docs/ai-context/archive/`.

### New tools

- [ ] **Adapter for Codex:** If Codex is adopted, add adapter in sync.sh (or scripts/ai-context/adapters/codex.sh) to emit Codex-format config from docs/ai-context.
- [ ] **Adapter for Lightweave:** Same pattern as Codex.

### Testing

- [ ] **Snapshot tests:** Optionally add approval-style test: run sync, diff output against committed .cursor and .claude. When format changes intentionally, update snapshots.
- [ ] **Link validation:** Verify relative links in generated files resolve correctly from their target locations. Sync should rewrite paths when emitting to .cursor/.claude.

---

## Completed

- [x] **Pre-commit hook:** `hooks/pre-commit` — runs sync when docs/ai-context/ changes are staged; re-stages generated files. Install with `./hooks/install.sh`.
- [x] **Watch mode:** `scripts/ai-context/watch.sh` — uses fswatch (macOS) or inotifywait (Linux) to run sync on file save.
- [x] **Idempotence:** sync.sh overwrites targets; repeated runs produce identical output; removal of source deletes generated output.
- [x] **Sync script:** `scripts/ai-context/sync.sh` with `--dry-run` support.
- [x] **Sync tests:** `scripts/tests/test-ai-context-sync.sh` (105 tests).
- [x] **Archive:** Old content moved to `docs/ai-context/archive/`.
- [x] **AGENTS.md / CLAUDE.md updates:** References updated to new rule names.

---

## How to Use This File

- **When resuming work:** Read this file to see what follow-up items remain.
- **When planning a sprint:** Pull relevant TODOs into the implementation plan.
- **When completing a TODO:** Move it to the Completed section.
