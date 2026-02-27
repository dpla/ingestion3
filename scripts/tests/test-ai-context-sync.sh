#!/usr/bin/env bash
set -euo pipefail

# Tests for scripts/ai-context/sync.sh
#
# Usage:
#   ./scripts/tests/test-ai-context-sync.sh
#   ./scripts/tests/test-ai-context-sync.sh --verbose

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYNC="$REPO_ROOT/scripts/ai-context/sync.sh"

VERBOSE=false
[[ "${1:-}" == "--verbose" ]] && VERBOSE=true

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); $VERBOSE && echo "  PASS: $1" || true; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

echo "=== AI Context Sync Tests ==="

# -------------------------------------------------------
# Test 1: sync.sh exists and is executable
# -------------------------------------------------------
echo "Test 1: sync.sh exists and is executable"
if [[ -x "$SYNC" ]]; then
  pass "sync.sh is executable"
else
  fail "sync.sh is not executable"
fi

# -------------------------------------------------------
# Test 2: Source files exist
# -------------------------------------------------------
echo "Test 2: Source files exist"
for rule in ingestion orchestrator aws-tools notifications validation script-standards; do
  if [[ -f "$REPO_ROOT/docs/ai-context/rules/${rule}.md" ]]; then
    pass "rules/${rule}.md exists"
  else
    fail "rules/${rule}.md missing"
  fi
done

for skill in dpla-run-ingest dpla-orchestrator dpla-verify-and-notify dpla-script-workflow \
             dpla-s3-ops dpla-ingest-status dpla-hub-info dpla-monitor-ingest-remap \
             dpla-oai-harvest-watch dpla-staged-report dpla-monthly-emails \
             dpla-community-webs-ingest dpla-ingest-debug send-email; do
  if [[ -f "$REPO_ROOT/docs/ai-context/skills/${skill}.md" ]]; then
    pass "skills/${skill}.md exists"
  else
    fail "skills/${skill}.md missing"
  fi
done

# -------------------------------------------------------
# Test 3: Dry run produces no changes (idempotence)
# -------------------------------------------------------
echo "Test 3: Sync is idempotent (dry-run shows no changes)"
dry_output="$("$SYNC" --dry-run 2>&1)" || true
if echo "$dry_output" | grep -q "no changes needed"; then
  pass "dry-run reports no changes"
else
  fail "dry-run reports changes (sync may be out of date)"
  $VERBOSE && echo "$dry_output"
fi

# -------------------------------------------------------
# Test 4: Claude rules are generated
# -------------------------------------------------------
echo "Test 4: Claude rules are generated"
for rule in ingestion orchestrator aws-tools notifications validation script-standards; do
  dst="$REPO_ROOT/.claude/rules/${rule}.md"
  src="$REPO_ROOT/docs/ai-context/rules/${rule}.md"
  if [[ -f "$dst" ]]; then
    pass ".claude/rules/${rule}.md exists"
    if diff -q "$src" "$dst" >/dev/null 2>&1; then
      pass ".claude/rules/${rule}.md matches source"
    else
      fail ".claude/rules/${rule}.md differs from source"
    fi
  else
    fail ".claude/rules/${rule}.md missing"
  fi
done

# -------------------------------------------------------
# Test 5: Old rules are removed
# -------------------------------------------------------
echo "Test 5: Old rules are removed"
for old in run-ingest s3-and-aws script-workflow verify-and-notify; do
  if [[ -f "$REPO_ROOT/.claude/rules/${old}.md" ]]; then
    fail ".claude/rules/${old}.md still exists (should be removed)"
  else
    pass ".claude/rules/${old}.md removed"
  fi
done

# -------------------------------------------------------
# Test 6: Cursor skills are generated
# -------------------------------------------------------
echo "Test 6: Cursor skills are generated"
for skill in dpla-run-ingest dpla-orchestrator dpla-verify-and-notify dpla-script-workflow \
             dpla-s3-ops dpla-ingest-status dpla-hub-info dpla-monitor-ingest-remap \
             dpla-oai-harvest-watch dpla-staged-report dpla-monthly-emails \
             dpla-community-webs-ingest dpla-ingest-debug send-email; do
  dst="$REPO_ROOT/.cursor/skills/${skill}/SKILL.md"
  src="$REPO_ROOT/docs/ai-context/skills/${skill}.md"
  if [[ -f "$dst" ]]; then
    pass ".cursor/skills/${skill}/SKILL.md exists"
    if diff -q "$src" "$dst" >/dev/null 2>&1; then
      pass ".cursor/skills/${skill}/SKILL.md matches source"
    else
      fail ".cursor/skills/${skill}/SKILL.md differs from source"
    fi
  else
    fail ".cursor/skills/${skill}/SKILL.md missing"
  fi
done

# -------------------------------------------------------
# Test 7: Claude shared skills are generated
# -------------------------------------------------------
echo "Test 7: Claude shared skills are generated"
for skill in dpla-ingest-status dpla-hub-info dpla-monitor-ingest-remap \
             dpla-oai-harvest-watch dpla-staged-report dpla-monthly-emails \
             dpla-community-webs-ingest dpla-ingest-debug send-email; do
  dst="$REPO_ROOT/.claude/skills/${skill}/SKILL.md"
  src="$REPO_ROOT/docs/ai-context/skills/${skill}.md"
  if [[ -f "$dst" ]]; then
    pass ".claude/skills/${skill}/SKILL.md exists"
    if diff -q "$src" "$dst" >/dev/null 2>&1; then
      pass ".claude/skills/${skill}/SKILL.md matches source"
    else
      fail ".claude/skills/${skill}/SKILL.md differs from source"
    fi
  else
    fail ".claude/skills/${skill}/SKILL.md missing"
  fi
done

# -------------------------------------------------------
# Test 8: Cursor-only skills are NOT in Claude
# -------------------------------------------------------
echo "Test 8: Cursor-only skills are not in Claude"
for skill in dpla-run-ingest dpla-orchestrator dpla-verify-and-notify dpla-script-workflow dpla-s3-ops; do
  if [[ -d "$REPO_ROOT/.claude/skills/${skill}" ]]; then
    fail ".claude/skills/${skill}/ exists (should be Cursor-only)"
  else
    pass ".claude/skills/${skill}/ not present in Claude"
  fi
done

# -------------------------------------------------------
# Test 9: Stale targets are removed
# -------------------------------------------------------
echo "Test 9: Stale targets are removed"
if [[ -d "$REPO_ROOT/.cursor/skills/dpla-s3-and-aws" ]]; then
  fail ".cursor/skills/dpla-s3-and-aws/ still exists"
else
  pass ".cursor/skills/dpla-s3-and-aws/ removed"
fi
if [[ -d "$REPO_ROOT/.claude/skills/s3-latest" ]]; then
  fail ".claude/skills/s3-latest/ still exists"
else
  pass ".claude/skills/s3-latest/ removed"
fi

# -------------------------------------------------------
# Test 10: Skills have valid frontmatter
# -------------------------------------------------------
echo "Test 10: Skills have valid frontmatter (name field)"
for skill_file in "$REPO_ROOT"/docs/ai-context/skills/*.md; do
  name="$(basename "$skill_file" .md)"
  if head -5 "$skill_file" | grep -q "^name:"; then
    pass "${name} has name in frontmatter"
  else
    fail "${name} missing name in frontmatter"
  fi
done

# -------------------------------------------------------
# Test 11: Markdown links resolve to existing files
# -------------------------------------------------------
echo "Test 11: Markdown links resolve correctly"
link_broken=0
for f in "$REPO_ROOT"/.claude/rules/*.md "$REPO_ROOT"/.claude/skills/*/SKILL.md "$REPO_ROOT"/.cursor/skills/*/SKILL.md "$REPO_ROOT"/.claude/rules/README.md; do
  [[ -f "$f" ]] || continue
  dir="$(dirname "$f")"
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    filepath="${path%%#*}"
    [[ -z "$filepath" ]] && continue
    [[ "$filepath" == mailto:* ]] && continue
    [[ "$filepath" == http:* ]] && continue
    [[ "$filepath" == https:* ]] && continue
    # Resolve path relative to file's directory (cd handles ../ correctly)
    if (cd "$dir" 2>/dev/null && [ -e "$filepath" ]); then
      pass "Link OK in $(basename "$f")"
    else
      fail "Broken link in $f: ]($path)"
      link_broken=$((link_broken + 1))
    fi
  done < <(grep -oE '\]\([^)]+\)' "$f" 2>/dev/null | sed 's/](\(.*\))/\1/' || true)
done 2>/dev/null || true

if [[ $link_broken -eq 0 ]]; then
  pass "All markdown links resolve"
else
  fail "$link_broken broken link(s) found"
fi

# -------------------------------------------------------
# Summary
# -------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
