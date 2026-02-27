#!/usr/bin/env bash
# Report which hubs have new JSONL staged in S3 for a given month
# Usage: staged-report.sh [month] [--slack] [--json]
#   month: numeric month (e.g. 2 for February); defaults to current month

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
I3_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$I3_HOME"

month="${1:-}"
shift || true

if [[ -n "$month" ]]; then
  "$I3_HOME/venv/bin/python" -m scheduler.orchestrator.staged_report --month="$month" "$@"
else
  "$I3_HOME/venv/bin/python" -m scheduler.orchestrator.staged_report "$@"
fi
