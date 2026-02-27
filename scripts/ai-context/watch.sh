#!/usr/bin/env bash
set -euo pipefail

# Watch docs/ai-context/ for changes and auto-run sync.
#
# Usage:
#   ./scripts/ai-context/watch.sh
#
# Requires fswatch (macOS: brew install fswatch) or inotifywait (Linux: apt install inotify-tools).
# Runs sync.sh whenever a .md file in docs/ai-context/rules/ or docs/ai-context/skills/ changes.

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SYNC="$REPO_ROOT/scripts/ai-context/sync.sh"
WATCH_DIRS=(
  "$REPO_ROOT/docs/ai-context/rules"
  "$REPO_ROOT/docs/ai-context/skills"
)

if ! [[ -x "$SYNC" ]]; then
  echo "Error: sync.sh not found or not executable at $SYNC" >&2
  exit 1
fi

echo "[watch] Watching docs/ai-context/{rules,skills}/ for changes..."
echo "[watch] Press Ctrl-C to stop."

if command -v fswatch >/dev/null 2>&1; then
  # macOS (or anywhere fswatch is installed)
  # --one-per-batch: emit one event per batch of changes (debounce)
  # -e '.*' -i '\.md$': only trigger on .md files
  fswatch --one-per-batch -e '.*' -i '\.md$' "${WATCH_DIRS[@]}" | while read -r _; do
    echo ""
    echo "[watch] Change detected — running sync..."
    "$SYNC"
    echo "[watch] Waiting for changes..."
  done
elif command -v inotifywait >/dev/null 2>&1; then
  # Linux
  while true; do
    inotifywait -r -e modify,create,delete,move --include '\.md$' "${WATCH_DIRS[@]}" 2>/dev/null
    echo ""
    echo "[watch] Change detected — running sync..."
    "$SYNC"
    echo "[watch] Waiting for changes..."
  done
else
  echo "Error: Neither fswatch nor inotifywait found." >&2
  echo "  macOS:  brew install fswatch" >&2
  echo "  Linux:  sudo apt install inotify-tools" >&2
  exit 1
fi
