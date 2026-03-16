#!/usr/bin/env bash
set -euo pipefail

# Sync canonical AI context from docs/ai-context/ to .cursor/ and .claude/ outputs.
#
# Usage:
#   ./scripts/ai-context/sync.sh              # Full sync (destructive)
#   ./scripts/ai-context/sync.sh --dry-run    # Show what would change
#
# Sync is destructive: generated targets are removed and recreated from source.
# Files in .cursor/skills/ and .claude/{rules,skills}/ that do not correspond
# to a source in docs/ai-context/ are deleted.

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC_DIR="$REPO_ROOT/docs/ai-context"
CURSOR_SKILLS="$REPO_ROOT/.cursor/skills"
CLAUDE_RULES="$REPO_ROOT/.claude/rules"
CLAUDE_SKILLS="$REPO_ROOT/.claude/skills"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

CHANGES=0

log() { echo "[sync] $*"; }
log_dry() { echo "[dry-run] $*"; }

action() {
  CHANGES=$((CHANGES + 1))
  if $DRY_RUN; then
    log_dry "$@"
  else
    log "$@"
  fi
}

# --- Rule-wrapping skills (Cursor-only) ---
# These skills exist only in .cursor/skills/ (not in .claude/).
# The source is docs/ai-context/skills/<name>.md which is self-contained.
CURSOR_ONLY_SKILLS=(
  dpla-run-ingest
  dpla-orchestrator
  dpla-verify-and-notify
  dpla-script-workflow
  dpla-s3-ops
)

# --- Skills that go to both Cursor and Claude ---
SHARED_SKILLS=(
  dpla-ingest-status
  dpla-hub-info
  dpla-monitor-ingest-remap
  dpla-oai-harvest-watch
  dpla-staged-report
  dpla-monthly-emails
  dpla-community-webs-ingest
  dpla-ingest-debug
  dpla-takedown
  send-email
)

# --- Rules (Claude-only, always loaded) ---
RULES=(
  ingestion
  orchestrator
  aws-tools
  notifications
  validation
  script-standards
)

# ============================================================
# Step 1: Sync rules → .claude/rules/
# ============================================================
log "Syncing rules to $CLAUDE_RULES/"

# Remove old rule files that are not in the new set (but keep README.md)
if [[ -d "$CLAUDE_RULES" ]]; then
  for f in "$CLAUDE_RULES"/*.md; do
    [[ -f "$f" ]] || continue
    base="$(basename "$f" .md)"
    [[ "$base" == "README" ]] && continue
    found=false
    for rule in "${RULES[@]}"; do
      if [[ "$base" == "$rule" ]]; then
        found=true
        break
      fi
    done
    if ! $found; then
      action "DELETE $f (no matching source rule)"
      $DRY_RUN || rm -f "$f"
    fi
  done
fi

# Copy each rule
for rule in "${RULES[@]}"; do
  src="$SRC_DIR/rules/${rule}.md"
  dst="$CLAUDE_RULES/${rule}.md"
  if [[ ! -f "$src" ]]; then
    log "WARNING: source not found: $src"
    continue
  fi
  if [[ -f "$dst" ]] && diff -q "$src" "$dst" >/dev/null 2>&1; then
    continue  # unchanged
  fi
  action "COPY $src → $dst"
  $DRY_RUN || cp "$src" "$dst"
done

# ============================================================
# Step 2: Sync shared skills → .cursor/skills/ and .claude/skills/
# ============================================================
log "Syncing shared skills to $CURSOR_SKILLS/ and $CLAUDE_SKILLS/"

for skill in "${SHARED_SKILLS[@]}"; do
  src="$SRC_DIR/skills/${skill}.md"
  if [[ ! -f "$src" ]]; then
    log "WARNING: source not found: $src"
    continue
  fi

  # Cursor: .cursor/skills/<skill>/SKILL.md
  cursor_dir="$CURSOR_SKILLS/$skill"
  cursor_dst="$cursor_dir/SKILL.md"
  if [[ -f "$cursor_dst" ]] && diff -q "$src" "$cursor_dst" >/dev/null 2>&1; then
    : # unchanged
  else
    action "COPY $src → $cursor_dst"
    if ! $DRY_RUN; then
      mkdir -p "$cursor_dir"
      cp "$src" "$cursor_dst"
    fi
  fi

  # Claude: .claude/skills/<skill>/SKILL.md
  claude_dir="$CLAUDE_SKILLS/$skill"
  claude_dst="$claude_dir/SKILL.md"
  if [[ -f "$claude_dst" ]] && diff -q "$src" "$claude_dst" >/dev/null 2>&1; then
    : # unchanged
  else
    action "COPY $src → $claude_dst"
    if ! $DRY_RUN; then
      mkdir -p "$claude_dir"
      cp "$src" "$claude_dst"
    fi
  fi
done

# ============================================================
# Step 3: Sync Cursor-only skills → .cursor/skills/
# ============================================================
log "Syncing Cursor-only skills to $CURSOR_SKILLS/"

for skill in "${CURSOR_ONLY_SKILLS[@]}"; do
  src="$SRC_DIR/skills/${skill}.md"
  if [[ ! -f "$src" ]]; then
    log "WARNING: source not found: $src"
    continue
  fi

  cursor_dir="$CURSOR_SKILLS/$skill"
  cursor_dst="$cursor_dir/SKILL.md"
  if [[ -f "$cursor_dst" ]] && diff -q "$src" "$cursor_dst" >/dev/null 2>&1; then
    : # unchanged
  else
    action "COPY $src → $cursor_dst"
    if ! $DRY_RUN; then
      mkdir -p "$cursor_dir"
      cp "$src" "$cursor_dst"
    fi
  fi
done

# ============================================================
# Step 4: Clean up stale targets (destructive sync)
# ============================================================
log "Cleaning stale targets..."

# Build newline-delimited list of expected Cursor skill names (POSIX-portable)
EXPECTED_CURSOR="$(printf '%s\n' "${SHARED_SKILLS[@]}" "${CURSOR_ONLY_SKILLS[@]}")"

# Remove unexpected Cursor skill dirs
if [[ -d "$CURSOR_SKILLS" ]]; then
  for d in "$CURSOR_SKILLS"/*/; do
    [[ -d "$d" ]] || continue
    name="$(basename "$d")"
    if ! echo "$EXPECTED_CURSOR" | grep -Fqx "$name"; then
      action "DELETE $d (no matching source skill)"
      $DRY_RUN || rm -rf "$d"
    fi
  done
fi

# Build newline-delimited list of expected Claude skill names
EXPECTED_CLAUDE="$(printf '%s\n' "${SHARED_SKILLS[@]}")"

# Remove unexpected Claude skill dirs
if [[ -d "$CLAUDE_SKILLS" ]]; then
  for d in "$CLAUDE_SKILLS"/*/; do
    [[ -d "$d" ]] || continue
    name="$(basename "$d")"
    if ! echo "$EXPECTED_CLAUDE" | grep -Fqx "$name"; then
      action "DELETE $d (no matching source skill)"
      $DRY_RUN || rm -rf "$d"
    fi
  done
fi

# ============================================================
# Step 5: Generate .claude/rules/README.md
# ============================================================
readme_dst="$CLAUDE_RULES/README.md"
readme_content="# Claude Code rules (synced from docs/ai-context/)

These rule files are **generated** by \`./scripts/ai-context/sync.sh\` from \`docs/ai-context/rules/\`. Do not edit directly; edit the source and re-run sync.

**Environment:** Before any of: building the JAR (\`sbt assembly\`), running the orchestrator, or running pipeline scripts — run \`source .env\` from repo root so \`JAVA_HOME\` (Java 11+), \`SLACK_WEBHOOK\`, \`DPLA_DATA\`, \`I3_CONF\`, etc. are set. Full checklist: [AGENTS.md](../../AGENTS.md) § Environment and build.

| Rule file | Use when |
|-----------|----------|
| ingestion.md | Running a single-hub ingest (harvest, remap, full pipeline) |
| orchestrator.md | Running or monitoring the Python orchestrator (parallel ingest, status) |
| aws-tools.md | S3 sync, finding latest S3 data; always \`--profile dpla\` |
| notifications.md | Posting failures to #tech-alerts or emailing tech@dp.la |
| validation.md | Verifying pipeline output (_SUCCESS markers, escalation reports) |
| script-standards.md | Adding or modifying scripts; document in SCRIPTS.md, run tests |

**Source:** \`docs/ai-context/\` — edit there, then run \`./scripts/ai-context/sync.sh\`.
"

readme_tmp="$(mktemp)"
printf '%s' "$readme_content" > "$readme_tmp"
if [[ -f "$readme_dst" ]] && diff -q "$readme_tmp" "$readme_dst" >/dev/null 2>&1; then
  : # unchanged
else
  if [[ -f "$readme_dst" ]]; then
    action "UPDATE $readme_dst"
  else
    action "CREATE $readme_dst"
  fi
  $DRY_RUN || cp "$readme_tmp" "$readme_dst"
fi
rm -f "$readme_tmp"

# ============================================================
# Summary
# ============================================================
if $DRY_RUN; then
  if [[ $CHANGES -eq 0 ]]; then
    log "Dry run complete: no changes needed."
  else
    log "Dry run complete: $CHANGES change(s) would be made."
  fi
else
  if [[ $CHANGES -eq 0 ]]; then
    log "Sync complete: everything up to date."
  else
    log "Sync complete: $CHANGES change(s) applied."
  fi
fi
