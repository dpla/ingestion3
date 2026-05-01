#!/usr/bin/env bash
# ingest-watchdog.sh — cron watchdog for unexpected ingest process deaths
#
# Run every 5 minutes via crontab (as ec2-user). Detects ingests that were
# killed without a clean exit (e.g. SIGKILL of the whole process group, which
# bypasses bash EXIT traps) by checking for in-progress status files with no
# corresponding running ingest process.
#
# On detecting a dead ingest:
#   - Sends a Slack alert to #tech-alerts via SLACK_BOT_TOKEN
#   - Touches a sentinel file to prevent repeat alerts on subsequent runs
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

# Hubs that should get periodic "still running" progress updates while in an
# active stage. These tend to be multi-hour jobs (smithsonian harvest is 3-4h)
# where the operator benefits from a heartbeat rather than radio silence.
HEARTBEAT_HUBS=(smithsonian)

# How often to send a heartbeat for HEARTBEAT_HUBS while the ingest is alive.
HEARTBEAT_INTERVAL_SECS=$((60 * 60))  # 1 hour

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

is_heartbeat_hub() {
    local hub="$1"
    for h in "${HEARTBEAT_HUBS[@]}"; do
        [[ "$hub" == "$h" ]] && return 0
    done
    return 1
}

# Pull the most recent "Harvested X% (N / total) of records from FILE" line
# from any of the candidate log files for this hub. Returns empty if no
# progress line is found yet (e.g., harvest just started).
latest_progress_line() {
    local hub="$1"
    local candidates=(
        "$I3_HOME/data/${hub}-ingest.log"
        "$I3_HOME/data/${hub}-harvest.log"
        "$I3_HOME/data/si-harvest.log"          # smithsonian's bare-harvest log
        "$I3_HOME/data/si-pipeline.log"
    )
    for log in "${candidates[@]}"; do
        if [[ -f "$log" ]]; then
            # Most recent "Harvested" line, stripped of timestamp+log noise.
            local line
            line=$(grep -E "Harvested [0-9]" "$log" 2>/dev/null \
                   | tail -1 \
                   | sed -E 's/^[0-9]{2}:[0-9]{2}:[0-9]{2} *INFO *\[[^]]*\] *//')
            if [[ -n "$line" ]]; then
                echo "$line"
                return 0
            fi
        fi
    done
}

for status_file in "$STATUS_DIR"/*.status; do
    [[ -f "$status_file" ]] || continue

    read -r hub status < <(python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
print(d.get('hub', ''), d.get('status', ''))" "$status_file" 2>/dev/null) || continue
    [[ -n "$hub" && -n "$status" ]] || continue

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

    # Skip if any process for this hub is still running. Different hubs use
    # different launcher conventions:
    #   - ingest.sh <hub>             — standard launcher
    #   - <hub>-ingest.sh             — hub-specific scripts (community-webs)
    #   - harvest.sh <hub>            — bare harvest invocations (smithsonian)
    #   - java ... --name=<hub>       — the actual JVM, most reliable signal
    proc_pattern="ingest\.sh $hub|${hub}-ingest\.sh|harvest\.sh $hub|java.*--name=$hub"
    if pgrep -f "$proc_pattern" > /dev/null 2>&1; then
        # Process is alive. For heartbeat-eligible hubs (long-running like
        # smithsonian), send a periodic Slack progress update so the operator
        # knows things are still moving instead of waiting hours in silence.
        if is_heartbeat_hub "$hub"; then
            heartbeat_sentinel="$STATUS_DIR/$hub.heartbeat"
            last_heartbeat=0
            [[ -f "$heartbeat_sentinel" ]] && \
                last_heartbeat=$(stat -c '%Y' "$heartbeat_sentinel" 2>/dev/null || echo 0)
            now=$(date +%s)
            if (( now - last_heartbeat >= HEARTBEAT_INTERVAL_SECS )); then
                progress=$(latest_progress_line "$hub" || true)
                age_min=$(( age / 60 ))
                msg=":hourglass: *$hub still running* — stage \`$status\`, last status update ${age_min}m ago"
                if [[ -n "$progress" ]]; then
                    msg+="\nLatest progress: $progress"
                fi
                slack_alert "$msg"
                touch "$heartbeat_sentinel"
            fi
        fi
        continue
    fi

    # Deduplicate alerts using a sentinel file.  The sentinel is touched after
    # the first alert for a given death event.  A new ingest updates the status
    # file's mtime, making the sentinel older → watchdog re-arms automatically.
    sentinel="$STATUS_DIR/$hub.watchdog-alerted"
    if [[ -f "$sentinel" ]]; then
        sentinel_mtime=$(stat -c '%Y' "$sentinel" 2>/dev/null) || sentinel_mtime=0
        (( sentinel_mtime >= file_mtime )) && continue
    fi

    # In-progress status with no running process and stale timestamp — alert.
    age_min=$(( age / 60 ))
    slack_alert ":skull: *$hub ingest process was killed* | stage: \`$status\` | last updated: ${age_min}m ago\nNo ingest process found — likely SIGKILL. Manual restart required."

    # Touch sentinel so subsequent cron runs skip this death event.
    touch "$sentinel"

done