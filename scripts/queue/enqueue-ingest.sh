#!/usr/bin/env bash
#
# enqueue-ingest.sh - Add a hub ingest to the box-wide queue (issue #786).
#
# The drain loop (drain-queue.sh, run by systemd) runs queued jobs one at a
# time, in the order they were added, and never while an index rebuild is in
# progress. This script only writes the job; it returns as soon as the job is
# queued and prints its job id and position.
#
# Usage:
#   enqueue-ingest.sh <hub> [--requester <name>] [--source <source>] [-- <pipeline args...>]
#
# Everything after "--" is passed to the pipeline script unchanged:
#   standard hubs   scripts/ingest.sh <hub> <args>        e.g. -- --resume-from mapping
#   nara            scripts/harvest/nara-ingest.sh <args>  e.g. -- --month=202605
#   community-webs  scripts/harvest/community-webs-ingest.sh <args>  e.g. -- --skip-export
#
# Rejected: a hub with no harvest.type in i3.conf, and Smithsonian (its
# download/preprocess steps aren't a single script yet; use
# launch_smithsonian.py).
#
# Examples:
#   enqueue-ingest.sh bpl --requester zoe --source manual
#   enqueue-ingest.sh nara --source gha -- --month=202605
#
# Exit codes: 0 queued, 1 rejected or error.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
source "$SCRIPT_DIR/../common.sh"
# shellcheck source=queue-lib.sh
source "$SCRIPT_DIR/queue-lib.sh"

usage() {
    sed -n '3,27p' "$0" | sed 's/^# \{0,1\}//'
}

HUB=""
REQUESTER="${GHA_ACTOR:-${USER:-unknown}}"
SOURCE="manual"
PIPELINE_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --requester) REQUESTER="${2:?--requester needs a value}"; shift 2 ;;
        --source) SOURCE="${2:?--source needs a value}"; shift 2 ;;
        --) shift; PIPELINE_ARGS=("$@"); break ;;
        -*) die "Unknown option: $1 (pipeline options go after --)" ;;
        *)
            [ -z "$HUB" ] || die "Only one hub per job (got '$HUB' and '$1')"
            HUB="$1"; shift ;;
    esac
done

[ -n "$HUB" ] || { usage; exit 1; }
case "$HUB" in
    *[!a-z0-9_-]*) die "Invalid hub name: $HUB" ;;
esac

# --- Validate the hub and pick the pipeline script ---------------------------
[ -r "$I3_CONF" ] || die "Can't read i3.conf at $I3_CONF"
if ! grep -q "^${HUB}\.harvest\.type" "$I3_CONF"; then
    die "Unknown hub '$HUB': no ${HUB}.harvest.type in $I3_CONF. Nothing queued."
fi
HARVEST_TYPE=$(get_harvest_type "$HUB")

case "$HUB:$HARVEST_TYPE" in
    smithsonian:*|si:*)
        die "Smithsonian isn't queueable yet (its download and preprocessing aren't one script). Use special_cases/smithsonian/launch_smithsonian.py." ;;
    nara:*|*:nara.file.delta) KIND="nara" ;;
    community-webs:*)         KIND="cw" ;;
    *)                        KIND="standard" ;;
esac

# --- Write the job -----------------------------------------------------------
q_init || die "Can't create the queue at $QUEUE_DIR"
JOB_ID=$(q_new_job_id "$HUB") || die "Couldn't create a job id"

TMP_JOB="$QUEUE_DIR/tmp/$JOB_ID.job"
TMP_ARGS="$QUEUE_DIR/tmp/$JOB_ID.args"
: > "$TMP_JOB" || die "Queue at $QUEUE_DIR isn't writable (disk full?). Nothing queued."
q_set "$TMP_JOB" JOB_ID "$JOB_ID"
q_set "$TMP_JOB" HUB "$HUB"
q_set "$TMP_JOB" KIND "$KIND"
q_set "$TMP_JOB" REQUESTER "$REQUESTER"
q_set "$TMP_JOB" SOURCE "$SOURCE"
q_set "$TMP_JOB" ENQUEUED "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
: > "$TMP_ARGS"
if [ ${#PIPELINE_ARGS[@]} -gt 0 ]; then
    printf '%s\n' "${PIPELINE_ARGS[@]}" > "$TMP_ARGS"
fi
# Args file first, so the drain loop never sees a .job without its .args.
mv "$TMP_ARGS" "$QUEUE_DIR/queued/$JOB_ID.args" \
    && mv "$TMP_JOB" "$QUEUE_DIR/queued/$JOB_ID.job" \
    || die "Couldn't move the job into the queue. Nothing queued."

POSITION=$(q_position "$JOB_ID" || echo "?")
RUNNING=$(ls "$QUEUE_DIR"/running/*.job 2>/dev/null | wc -l | tr -d ' ')
ARGS_SHOWN=""
[ ${#PIPELINE_ARGS[@]} -gt 0 ] && ARGS_SHOWN=" ${PIPELINE_ARGS[*]}"

echo "QUEUED $JOB_ID"
echo "  hub:       $HUB ($KIND)$ARGS_SHOWN"
echo "  position:  $POSITION in queue$( [ "$RUNNING" -gt 0 ] && echo ', behind 1 running job')"
echo "  requester: $REQUESTER ($SOURCE)"

SLACK_ARGS=""
[ -n "$ARGS_SHOWN" ] && SLACK_ARGS=" \`${ARGS_SHOWN# }\`"
slack_notify ":inbox_tray: *$HUB ingest queued*${SLACK_ARGS} by $REQUESTER ($SOURCE), position $POSITION\nJob: \`$JOB_ID\`" || true

# Warn (don't fail) if nothing is draining the queue.
if command -v systemctl >/dev/null 2>&1; then
    if ! systemctl is-active --quiet dpla-ingest-queue 2>/dev/null; then
        log_warn "The queue service (dpla-ingest-queue) isn't running, so this job won't start until it is. See scripts/queue/install-queue-service.sh."
    fi
fi
exit 0
