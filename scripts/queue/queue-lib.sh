#!/usr/bin/env bash
#
# queue-lib.sh - Shared helpers for the box-wide ingest queue (issue #786).
#
# Sourced by enqueue-ingest.sh, drain-queue.sh and index-guard.sh; not run
# directly. Source common.sh first (it sets DPLA_DATA, I3_CONF, slack_notify).
#
# Layout under $QUEUE_DIR (default $DPLA_DATA/queue, i.e. on EBS so it
# survives stop/start and reboot):
#
#   queued/<job_id>.job     waiting to run, oldest first (job ids sort by time)
#   running/<job_id>.job    the job the drain loop is running (at most one)
#   done/<job_id>.job       exited 0
#   failed/<job_id>.job     exited non-zero, or outcome unknown
#   <state>/<job_id>.args   the pipeline arguments, one per line
#   tmp/                    staging area; files are moved into queued/ whole
#   logs/                   per-job logs for kinds without a standard log path
#   drain.lock              held by the single drain loop
#   run.lock                held around each pipeline run (the run slot)
#   INDEX_GUARD             present while an index rebuild/batch is running
#
# A job's state is the directory it is in. Moving a file between directories
# on the same filesystem is atomic, so a job is never in two states at once.
#
# A .job file is KEY=value lines, values quoted with printf %q.

QUEUE_DIR="${QUEUE_DIR:-${DPLA_DATA:-$HOME/dpla/data}/queue}"
QUEUE_STATES="queued running done failed"

# Create the queue directories if they don't exist yet.
q_init() {
    local d
    for d in $QUEUE_STATES tmp logs; do
        mkdir -p "$QUEUE_DIR/$d" || return 1
    done
}

# A new job id: nanosecond timestamp (fixed width, so ids sort in enqueue
# order) plus the hub name for readability.
q_new_job_id() {
    local hub="$1" ns
    ns=$(python3 -c 'import time; print(time.time_ns())') || return 1
    printf '%019d-%s\n' "$ns" "$hub"
}

# Write KEY=value to a job file, replacing an existing KEY.
# Usage: q_set <job_file> <KEY> <value>
q_set() {
    local file="$1" key="$2" value="$3" tmp
    tmp="${file}.tmp.$$"
    { grep -v "^${key}=" "$file" 2>/dev/null || true
      printf '%s=%q\n' "$key" "$value"
    } > "$tmp" && mv "$tmp" "$file"
}

# Read KEY from a job file (empty if unset).
# Usage: q_get <job_file> <KEY>
q_get() {
    local file="$1" key="$2" line
    line=$(grep "^${key}=" "$file" 2>/dev/null | tail -1) || true
    [ -z "$line" ] && return 0
    # Values were written with printf %q; eval in a subshell to unquote.
    ( eval "printf '%s\n' ${line#*=}" )
}

# Path of a job's file in whichever state it is in (empty if not found).
# Usage: q_find <job_id>
q_find() {
    local id="$1" s
    for s in $QUEUE_STATES; do
        if [ -f "$QUEUE_DIR/$s/$id.job" ]; then
            printf '%s\n' "$QUEUE_DIR/$s/$id.job"
            return 0
        fi
    done
    return 1
}

# Queued job ids, oldest first.
q_list_queued() {
    local f
    for f in "$QUEUE_DIR"/queued/*.job; do
        [ -e "$f" ] || continue
        basename "$f" .job
    done | sort
}

# 1-based position of a queued job, counting a running job as position 0.
# Usage: q_position <job_id>
q_position() {
    local id="$1" n=0 q
    while IFS= read -r q; do
        n=$((n + 1))
        if [ "$q" = "$id" ]; then
            printf '%s\n' "$n"
            return 0
        fi
    done < <(q_list_queued)
    return 1
}

# Move a job (and its .args) to another state.
# Usage: q_move <job_id> <from_state> <to_state>
q_move() {
    local id="$1" from="$2" to="$3"
    mv "$QUEUE_DIR/$from/$id.job" "$QUEUE_DIR/$to/$id.job" || return 1
    if [ -f "$QUEUE_DIR/$from/$id.args" ]; then
        mv "$QUEUE_DIR/$from/$id.args" "$QUEUE_DIR/$to/$id.args"
    fi
}

# Is an index rebuild or post-index batch in progress? Prints the reason and
# returns 0 if so. Removes an expired INDEX_GUARD (with a warning) so a
# crashed indexer can't hold the queue forever.
q_index_busy() {
    local guard="$QUEUE_DIR/INDEX_GUARD" now expires note
    if [ -f "$guard" ]; then
        now=$(date +%s)
        expires=$(q_get "$guard" EXPIRES)
        note=$(q_get "$guard" NOTE)
        if [ -n "$expires" ] && [ "$now" -lt "$expires" ] 2>/dev/null; then
            printf 'index guard set (%s), expires %s\n' "$note" \
                "$(date -u -d "@$expires" '+%Y-%m-%d %H:%M UTC' 2>/dev/null || echo "$expires")"
            return 0
        fi
        rm -f "$guard"
        printf 'EXPIRED:%s\n' "$note"
        return 1
    fi
    return 1
}

# Backstop for the index guard: an active sparkindexer or monthlybatch EMR
# cluster. Prints the cluster ids and returns 0 if one is active. If the EMR
# call itself fails (no permission, no network), returns 2 so the caller can
# warn once and rely on INDEX_GUARD alone.
q_emr_busy() {
    local out
    out=$(aws emr list-clusters --active --region us-east-1 \
        --query "Clusters[?Name=='sparkindexer' || Name=='monthlybatch'].Id" \
        --output text 2>/dev/null) || return 2
    out=$(printf '%s' "$out" | tr -s '[:space:]' ' ' | sed 's/^ //; s/ $//')
    if [ -n "$out" ] && [ "$out" != "None" ]; then
        printf '%s\n' "$out"
        return 0
    fi
    return 1
}
