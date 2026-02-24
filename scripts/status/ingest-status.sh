#!/usr/bin/env bash
#
# Quick ingest status check — reads .status files, no subprocess overhead.
#
# Usage:
#   ./ingest-status.sh                   # Table view
#   ./ingest-status.sh --json            # Raw JSON
#   ./ingest-status.sh wisconsin p2p     # Specific hubs
#   ./ingest-status.sh -v                # With stage history
#   ./ingest-status.sh --watch           # Auto-refresh (30s)
#   ./ingest-status.sh --watch 10        # Auto-refresh (10s)
#

# Repo root (script lives in scripts/status/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
I3_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$I3_HOME"
python3 -m scheduler.orchestrator.status "$@"
