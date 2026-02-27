#!/usr/bin/env bash
# Run an OAI harvest and watch set-by-set progress with ETA
# Usage: oai-harvest-watch.sh <hub>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
I3_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"
[[ -f "$I3_HOME/.env" ]] && source "$I3_HOME/.env" || { echo "WARN: $I3_HOME/.env not found" >&2; }
source "$SCRIPT_DIR/../common.sh"

hub="${1:?Usage: oai-harvest-watch.sh <hub>}"
log="$I3_HOME/logs/harvest-${hub}-$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$(dirname "$log")"

"$I3_HOME/scripts/harvest.sh" "$hub" 2>&1 | tee "$log"
"$I3_HOME/venv/bin/python" "$I3_HOME/scripts/status/watch-oai-harvest.py" \
  --log="$log" --conf="$I3_CONF" --hub="$hub"
