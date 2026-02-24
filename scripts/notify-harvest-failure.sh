#!/usr/bin/env bash
# notify-harvest-failure.sh - Send Slack and email notification on harvest failure
#
# Usage: ./scripts/notify-harvest-failure.sh <hub> "<error message>" ["<email body>"]
#
# Args:
#   $1  Hub short name (required)
#   $2  Error summary for Slack code block (required)
#   $3  Complete email body built by the Scala pipeline (optional; when omitted
#       the Python emailer wraps $2 in a default template)
#
# Sends to both Slack (#tech-alerts) and tech@dp.la (email via AWS SES).
#
# Environment:
#   SLACK_WEBHOOK        - Slack webhook URL for #tech-alerts (required for Slack)
#   SLACK_ALERT_USER_ID  - Slack user ID to mention, e.g. U01234ABCD (optional)
#   AWS_PROFILE          - AWS profile for SES email (default: dpla)
#   I3_HOME              - Repo root (default: derived from script location)

set -e

HUB="${1:?Usage: notify-harvest-failure.sh <hub> <error> [<email_body>]}"
ERROR_MSG="${2:-Unknown error}"
EMAIL_BODY="${3:-}"

# Repo root for venv and Python helper
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
I3_HOME="${I3_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
export I3_HOME

# Truncate very long error messages for Slack
MAX_LEN=1500
if [ "${#ERROR_MSG}" -gt "$MAX_LEN" ]; then
    ERROR_MSG_SLACK="${ERROR_MSG:0:$MAX_LEN}..."
else
    ERROR_MSG_SLACK="$ERROR_MSG"
fi

# Build the mention string
MENTION=""
if [ -n "${SLACK_ALERT_USER_ID:-}" ]; then
    MENTION="<@${SLACK_ALERT_USER_ID}> "
fi

# 1. Send to Slack when webhook is set
if [ -n "${SLACK_WEBHOOK:-}" ]; then
    PAYLOAD=$(cat <<EOJSON
{
  "text": ":x: *Harvest Failure: ${HUB}*\n${MENTION}OAI harvest failure needs attention\n\n\`\`\`\n${ERROR_MSG_SLACK}\n\`\`\`"
}
EOJSON
)

    if curl -s -o /dev/null -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        -d "$PAYLOAD" "$SLACK_WEBHOOK" | grep -q "200"; then
        echo "Harvest failure notification sent to Slack for ${HUB}"
    else
        echo "Warning: Failed to send Slack notification for ${HUB}" >&2
    fi
else
    echo "SLACK_WEBHOOK not set. Harvest failure for ${HUB}:" >&2
    echo "$ERROR_MSG" >&2
    echo "" >&2
fi

# 2. Always send email to tech@dp.la (best-effort)
# When EMAIL_BODY is provided (from Scala), pass it as arg 3 so the Python
# script uses it as-is. Otherwise the Python script wraps ERROR_MSG in a
# default template.
SEND_EMAIL_SCRIPT="$I3_HOME/scripts/send-harvest-failure-email.py"
PYTHON="${I3_HOME}/venv/bin/python"
if [ -x "$PYTHON" ] && [ -f "$SEND_EMAIL_SCRIPT" ]; then
    if [ -n "$EMAIL_BODY" ]; then
        "$PYTHON" "$SEND_EMAIL_SCRIPT" "$HUB" "$ERROR_MSG" "$EMAIL_BODY" 2>/dev/null || \
            echo "Warning: Failed to send harvest failure email to tech@dp.la" >&2
    else
        "$PYTHON" "$SEND_EMAIL_SCRIPT" "$HUB" "$ERROR_MSG" 2>/dev/null || \
            echo "Warning: Failed to send harvest failure email to tech@dp.la" >&2
    fi
else
    echo "Could not run send-harvest-failure-email.py (missing venv or script). Notify tech@dp.la manually." >&2
fi
