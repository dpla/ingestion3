#!/usr/bin/env bash
# ingest-watchdog.sh — cron watchdog for unexpected ingest process deaths
#
# Run every 5 minutes via crontab (as ec2-user). Detects ingests that were
# killed without a clean exit (e.g. SIGKILL of the whole process group, which
# bypasses bash EXIT traps) by checking for in-progress status files with no
# corresponding running ingest.sh process.
#
# On detecting a dead ingest:
#   - Sends a Slack alert to #tech-alerts via SLACK_BOT_TOKEN
#   - Marks the status as "killed" to prevent repeat alerts on subsequent runs
#
# Usage: add to ec2-user's crontab:
#   */5 * * * * /home/ec2-user/ingestion3/scripts/ingest-watchdog.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
I3_HOME="$(dirname "$SCRIPT_DIR")"
STATUS_DIR="$I3_HOME/logs/status"
ENV_FILE="$I3_HOME/.env"

# Statuses that indicate an ingest is actively in progress
ACTIVE_STATUSES=(harvesting remapping enriching jsonl syncing)

# Minimum age (seconds) of a status file before alerting — provides a buffer
# for the brief gap between one JVM step completing and the next starting.
MIN_AGE_SECS=300  # 5 minutes

[[ -d "$STATUS_DIR" ]] || exit 0

# Load credentials (provides SLACK_BOT_TOKEN, SLACK_CHANNEL)
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"

slack_alert() {
    local msg
    msg=$(printf '%b' "$1")
    local token="${SLACK_BOT_TOKEN:-}"
    local channel="${SLACK_CHANNEL:-C02HEU2L3}"
    [[ -z "$token" ]] && return 0
    local payload
    payload=$(python3 -c "
import json, sys
print(json.dumps({'channel': sys.argv[1], 'text': sys.argv[2]}))" \
        "$channel" "$msg") || return 0
    curl -s -X POST "https://slack.com/api/chat.postMessage" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload" > /dev/null || true
}

for status_file in "$STATUS_DIR"/*.status; do
    [[ -f "$status_file" ]] || continue

    hub=$(python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
print(d.get('hub', ''))" "$status_file" 2>/dev/null) || continue

    status=$(python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
print(d.get('status', ''))" "$status_file" 2>/dev/null) || continue

    # Skip terminal statuses (complete, failed, killed, etc.)
    is_active=false
    for s in "${ACTIVE_STATUSES[@]}"; do
        [[ "$status" == "$s" ]] && is_active=true && break
    done
    $is_active || continue

    # Skip if status file is too recent — avoids false positives during the
    # brief gap between one JVM step completing and the next starting.
    file_mtime=$(stat -c '%Y' "$status_file" 2>/dev/null) || continue
    age=$(( $(date +%s) - file_mtime ))
    (( age >= MIN_AGE_SECS )) || continue

    # Skip if the ingest process is still running for this hub
    if pgrep -f "ingest.sh $hub" > /dev/null 2>&1; then
        continue
    fi

    # In-progress status with no running process and stale timestamp — alert.
    age_min=$(( age / 60 ))
    slack_alert ":skull: *$hub ingest process was killed* | stage: \`$status\` | last updated: ${age_min}m ago\nNo ingest.sh process found — likely SIGKILL. Manual restart required."

    # Mark as killed so subsequent cron runs don't re-alert for this ingest.
    python3 - "$status_file" << 'PYEOF'
import json, sys, datetime
path = sys.argv[1]
with open(path) as f:
    d = json.load(f)
d['status'] = 'killed'
d['updated_at'] = datetime.datetime.utcnow().isoformat()
d['error'] = 'Process killed without clean exit (watchdog detected)'
with open(path, 'w') as f:
    json.dump(d, f, indent=2)
PYEOF

done
