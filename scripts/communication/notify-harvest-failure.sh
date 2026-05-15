#!/usr/bin/env bash
# notify-harvest-failure.sh - Send email notification on harvest failure
#
# Usage: ./scripts/notify-harvest-failure.sh <hub> "<error message>" ["<email body>"]
#
# Args:
#   $1  Hub short name (required)
#   $2  Error summary (required)
#   $3  Complete email body built by the Scala pipeline (optional; when omitted
#       the Python emailer wraps $2 in a default template)
#
# Sends to tech@dp.la via AWS SES. Slack alerts are handled by the Tech Reports
# bot and do not need to be sent here.
#
# Environment:
#   AWS_PROFILE  - AWS profile for SES email (default: dpla)
#   I3_HOME      - Repo root (default: derived from script location)

set -e

HUB="${1:?Usage: notify-harvest-failure.sh <hub> <error> [<email_body>]}"
ERROR_MSG="${2:-Unknown error}"
EMAIL_BODY="${3:-}"

# Repo root for venv and Python helper (script lives in scripts/communication/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
I3_HOME="${I3_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
export I3_HOME

# Send email to tech@dp.la (best-effort)
# When EMAIL_BODY is provided (from Scala), pass it as arg 3 so the Python
# script uses it as-is. Otherwise the Python script wraps ERROR_MSG in a
# default template.
SEND_EMAIL_SCRIPT="$I3_HOME/scripts/communication/send-harvest-failure-email.py"
PYTHON="${I3_HOME}/venv/bin/python"
if [ -x "$PYTHON" ] && [ -f "$SEND_EMAIL_SCRIPT" ]; then
    ARGS=("$HUB" "$ERROR_MSG")
    [ -n "$EMAIL_BODY" ] && ARGS+=("$EMAIL_BODY")
    "$PYTHON" "$SEND_EMAIL_SCRIPT" "${ARGS[@]}" 2>/dev/null || \
        echo "Warning: Failed to send harvest failure email to tech@dp.la" >&2
else
    echo "Could not run send-harvest-failure-email.py (missing venv or script). Notify tech@dp.la manually." >&2
fi
