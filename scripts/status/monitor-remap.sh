#!/usr/bin/env bash
# Monitor a running manual remap pipeline, polling mapping/enrichment/jsonl stage outputs
# Usage: monitor-remap.sh <hub>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
[[ -f "$REPO_ROOT/.env" ]] && source "$REPO_ROOT/.env" || { echo "WARN: $REPO_ROOT/.env not found; DPLA_DATA may be wrong" >&2; }
source "$SCRIPT_DIR/../common.sh"

hub="${1:?Usage: monitor-remap.sh <hub>}"

while true; do
  echo ""
  date
  for step in mapping enrichment jsonl; do
    latest=$(find_latest_data "$hub" "$step" 2>/dev/null || true)
    if [[ -z "$latest" ]]; then
      echo "$step: (no output yet)"
      continue
    fi
    if [[ -f "$latest/_SUCCESS" ]]; then
      echo "$step: done  $(basename "$latest")"
    else
      echo "$step: running $(basename "$latest")"
    fi
  done
  sleep 30
done
