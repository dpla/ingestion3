#!/usr/bin/env bash
set -euo pipefail

# Install git hooks from hooks/ into .git/hooks/.
# Safe to run multiple times — overwrites existing hooks from this repo.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_SRC="$REPO_ROOT/hooks"
HOOKS_DST="$REPO_ROOT/.git/hooks"

for hook in "$HOOKS_SRC"/*; do
  name="$(basename "$hook")"
  [[ "$name" == "install.sh" ]] && continue
  [[ "$name" == "README.md" ]] && continue
  cp "$hook" "$HOOKS_DST/$name"
  chmod +x "$HOOKS_DST/$name"
  echo "Installed $name"
done
