#!/usr/bin/env bash
#
# drain-queue.sh - The box-wide ingest queue's only executor (issue #786).
#
# Runs queued jobs (from enqueue-ingest.sh) one at a time, oldest first:
#   - one drain loop at a time (drain.lock)
#   - one pipeline at a time, holding the run slot (run.lock) for the whole run
#   - holds while an index rebuild or post-index batch is in progress
#     (INDEX_GUARD, set by 4_launch_indexer.py / 5_post_indexer.py, with an
#     active-EMR-cluster check as a backstop)
#   - on startup, re-queues a job a crash or reboot interrupted (once; a job
#     interrupted twice is marked failed so it can't loop)
#
# Meant to run forever under systemd (dpla-ingest-queue.service). Linux only:
# needs flock, setsid and /proc.
#
# Usage: drain-queue.sh            (normally started by systemd)
#
# Environment:
#   QUEUE_DIR            queue root (default $DPLA_DATA/queue)
#   QUEUE_POLL_SECONDS   how often to look for new jobs (default 15)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
source "$SCRIPT_DIR/../common.sh"
# shellcheck source=queue-lib.sh
source "$SCRIPT_DIR/queue-lib.sh"

POLL_SECONDS="${QUEUE_POLL_SECONDS:-15}"
HOLD_POLL_SECONDS=60
ORPHAN_POLL_SECONDS=30
MAX_ATTEMPTS=2

case "${1:-}" in
    -h|--help) sed -n '3,21p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
esac

require_command flock "flock is required (util-linux)"
require_command setsid "setsid is required (util-linux)"
q_init || die "Can't create the queue at $QUEUE_DIR"

# --- Singleton --------------------------------------------------------------
exec 8>"$QUEUE_DIR/drain.lock"
if ! flock -n 8; then
    log_info "Another drain loop already holds $QUEUE_DIR/drain.lock; exiting."
    exit 0
fi

ts() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

# Is <pid> the pipeline process for <job_id>? Checked through the job id the
# drain loop puts in the pipeline's environment, so a reused PID after a
# reboot is never mistaken for the job.
proc_is_job() {
    local pid="$1" id="$2"
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
    [ -r "/proc/$pid/environ" ] || return 1
    tr '\0' '\n' < "/proc/$pid/environ" | grep -qx "INGEST_JOB_ID=$id"
}

# Pipeline command and log path for a job. Sets CMD (array) and LOG.
build_command() {
    local id="$1" job="$2" hub kind args_file a
    hub=$(q_get "$job" HUB)
    kind=$(q_get "$job" KIND)
    args_file="${job%.job}.args"
    local args=()
    if [ -f "$args_file" ]; then
        while IFS= read -r a || [ -n "$a" ]; do
            args+=("$a")
        done < "$args_file"
    fi
    case "$kind" in
        standard)
            CMD=(bash "$I3_HOME/scripts/ingest.sh" "$hub" ${args[@]+"${args[@]}"})
            # Same path the launchers use, so check_ingest.py keeps working.
            LOG="$DPLA_DATA/$hub-ingest.log" ;;
        nara)
            CMD=(bash "$I3_HOME/scripts/harvest/nara-ingest.sh" ${args[@]+"${args[@]}"})
            LOG="$QUEUE_DIR/logs/$id.log" ;;
        cw)
            CMD=(bash "$I3_HOME/scripts/harvest/community-webs-ingest.sh" ${args[@]+"${args[@]}"})
            LOG="$QUEUE_DIR/logs/$id.log" ;;
        *)
            return 1 ;;
    esac
}

# --- Startup sweep: jobs left in running/ -----------------------------------
sweep_running() {
    local f id hub pid attempts
    for f in "$QUEUE_DIR"/running/*.job; do
        [ -e "$f" ] || continue
        id=$(basename "$f" .job)
        hub=$(q_get "$f" HUB)
        pid=$(q_get "$f" PID)

        if proc_is_job "$pid" "$id"; then
            # The drain loop restarted (e.g. a deploy) but the pipeline kept
            # running. Wait for it rather than starting anything alongside it.
            log_info "Job $id ($hub) is still running as PID $pid; waiting for it to finish."
            slack_notify ":hourglass: Queue restarted while *$hub* was running; waiting for it before starting anything else.\nJob: \`$id\`" || true
            while proc_is_job "$pid" "$id"; do sleep "$ORPHAN_POLL_SECONDS"; done
            q_set "$f" FINISHED "$(ts)"
            q_set "$f" EXIT "unknown"
            q_set "$f" REASON "drain loop restarted while it ran; exit code not captured"
            q_move "$id" running failed
            slack_notify ":grey_question: *$hub* finished while the queue was restarting, so its result wasn't captured. Check its log and the hub's Slack messages.\nJob: \`$id\` Log: \`$(q_get "$QUEUE_DIR/failed/$id.job" LOG)\`" || true
            continue
        fi

        attempts=$(q_get "$f" ATTEMPTS)
        attempts=$(( ${attempts:-0} + 1 ))
        q_set "$f" ATTEMPTS "$attempts"
        if [ "$attempts" -ge "$MAX_ATTEMPTS" ]; then
            q_set "$f" FINISHED "$(ts)"
            q_set "$f" EXIT "interrupted"
            q_set "$f" REASON "interrupted $attempts times (crash, OOM or reboot); not retried again"
            q_move "$id" running failed
            slack_notify ":x: *$hub* was interrupted again (crash, OOM or reboot), so it was marked failed instead of re-queued.\nJob: \`$id\`" || true
        else
            q_move "$id" running queued
            slack_notify ":recycle: *$hub* was interrupted (crash, OOM or reboot) and has been re-queued at the front.\nJob: \`$id\`" || true
        fi
    done
}

# --- Run one job ------------------------------------------------------------
run_job() {
    local id="$1" job="$QUEUE_DIR/running/$1.job" hub requester src rc state waiting
    hub=$(q_get "$job" HUB)
    requester=$(q_get "$job" REQUESTER)
    src=$(q_get "$job" SOURCE)

    CMD=(); LOG=""
    if ! build_command "$id" "$job"; then
        q_set "$job" FINISHED "$(ts)"
        q_set "$job" EXIT "rejected"
        q_set "$job" REASON "unknown job kind: $(q_get "$job" KIND)"
        q_move "$id" running failed
        slack_notify ":x: Queue job for *$hub* has an unknown kind and was not run.\nJob: \`$id\`" || true
        return
    fi

    # The run slot. Blocks if a break-glass direct run holds it.
    exec 9>"$QUEUE_DIR/run.lock"
    flock 9

    q_set "$job" STARTED "$(ts)"
    q_set "$job" LOG "$LOG"
    export INGEST_JOB_ID="$id" INGEST_REQUESTER="$requester" INGEST_SOURCE="$src"
    # setsid --wait: the pipeline starts as its own session leader (so
    # ingest.sh doesn't re-exec itself under setsid and return early), and we
    # still get its real exit code. 8>&- keeps the drain lock out of the
    # pipeline, so a restarted drain loop can take it while the pipeline runs.
    # Fresh log per run, like the launchers' `> <hub>-ingest.log`.
    setsid --wait "${CMD[@]}" > "$LOG" 2>&1 < /dev/null 8>&- &
    local pid=$!
    q_set "$job" PID "$pid"
    unset INGEST_JOB_ID INGEST_REQUESTER INGEST_SOURCE

    log_info "Started $id ($hub) as PID $pid; log $LOG"
    slack_notify ":arrow_forward: *$hub* started from the queue (queued by $requester, $src).\nJob: \`$id\` Log: \`$LOG\`" || true

    wait "$pid"
    rc=$?
    flock -u 9
    exec 9>&-

    q_set "$job" FINISHED "$(ts)"
    q_set "$job" EXIT "$rc"
    waiting=$(q_list_queued | wc -l | tr -d ' ')
    if [ "$rc" -eq 0 ]; then
        state=done
        q_move "$id" running done
        slack_notify ":checkered_flag: *$hub* queue job finished (exit 0). $waiting job(s) still queued.\nJob: \`$id\`" || true
    else
        state=failed
        q_move "$id" running failed
        slack_notify ":x: *$hub* queue job failed (exit $rc). $waiting job(s) still queued; the queue carries on.\nJob: \`$id\` Log: \`$LOG\`" || true
    fi
    log_info "Job $id ($hub) $state, exit $rc"
}

# --- Main loop ----------------------------------------------------------------
sweep_running
log_info "Drain loop started (queue $QUEUE_DIR, polling every ${POLL_SECONDS}s)"

HELD_REASON=""
EMR_WARNED=0
while true; do
    NEXT=$(q_list_queued | head -1)
    if [ -z "$NEXT" ]; then
        sleep "$POLL_SECONDS"
        continue
    fi

    # Index guard: INDEX_GUARD first, active EMR clusters as a backstop.
    REASON=$(q_index_busy) || {
        case "$REASON" in
            EXPIRED:*)
                slack_notify ":warning: The queue's index guard (${REASON#EXPIRED:}) expired without being cleared, so it was removed. If an index rebuild is still running, set it again with scripts/queue/index-guard.sh." || true ;;
        esac
        REASON=""
    }
    if [ -z "$REASON" ]; then
        EMR=$(q_emr_busy); EMR_RC=$?
        if [ "$EMR_RC" -eq 0 ]; then
            REASON="active EMR cluster(s): $EMR"
        elif [ "$EMR_RC" -eq 2 ] && [ "$EMR_WARNED" -eq 0 ]; then
            log_warn "Can't list EMR clusters from this box; relying on INDEX_GUARD only."
            EMR_WARNED=1
        fi
    fi
    if [ -n "$REASON" ]; then
        if [ "$REASON" != "$HELD_REASON" ]; then
            WAITING=$(q_list_queued | wc -l | tr -d ' ')
            slack_notify ":hourglass: Ingest queue holding: $REASON. $WAITING job(s) waiting." || true
            HELD_REASON="$REASON"
        fi
        sleep "$HOLD_POLL_SECONDS"
        continue
    fi
    if [ -n "$HELD_REASON" ]; then
        slack_notify ":arrow_forward: Index work finished; the ingest queue is running again." || true
        HELD_REASON=""
    fi

    # Claim the job. Fails only if it was removed in the meantime.
    if ! q_move "$NEXT" queued running 2>/dev/null; then
        continue
    fi
    run_job "$NEXT"
done
