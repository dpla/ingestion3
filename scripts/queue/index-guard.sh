#!/usr/bin/env bash
#
# index-guard.sh - Hold the ingest queue during an index rebuild (issue #786).
#
# While the guard is set, the drain loop (drain-queue.sh) starts no new
# pipelines; a pipeline already running finishes. The guard expires on its own
# so a crashed indexer can't hold the queue forever.
#
# 4_launch_indexer.py sets it before launching the sparkindexer cluster;
# 5_post_indexer.py refreshes it while the batch runs and clears it at the end.
#
# Usage:
#   index-guard.sh set <hours> [note]   set (or extend) the guard
#   index-guard.sh clear                remove it
#   index-guard.sh status               print it; exit 0 if set, 1 if not
#
# Example:
#   index-guard.sh set 16 "sparkindexer launched by zoe"

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
source "$SCRIPT_DIR/../common.sh"
# shellcheck source=queue-lib.sh
source "$SCRIPT_DIR/queue-lib.sh"

GUARD="$QUEUE_DIR/INDEX_GUARD"

usage() { sed -n '3,18p' "$0" | sed 's/^# \{0,1\}//'; }

case "${1:-}" in
    set)
        hours="${2:-}"
        case "$hours" in
            ''|*[!0-9]*) die "set needs a whole number of hours, e.g. index-guard.sh set 16 \"note\"" ;;
        esac
        [ "$hours" -ge 1 ] && [ "$hours" -le 72 ] || die "hours must be between 1 and 72"
        note="${3:-index rebuild}"
        q_init || die "Can't create the queue at $QUEUE_DIR"
        tmp="$GUARD.tmp.$$"
        : > "$tmp" || die "Can't write $GUARD"
        q_set "$tmp" EXPIRES "$(( $(date +%s) + hours * 3600 ))"
        q_set "$tmp" NOTE "$note"
        q_set "$tmp" SET_AT "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        mv "$tmp" "$GUARD" || die "Can't write $GUARD"
        echo "GUARD SET for ${hours}h: $note"
        ;;
    clear)
        if [ -f "$GUARD" ]; then
            rm -f "$GUARD" && echo "GUARD CLEARED"
        else
            echo "GUARD NOT SET"
        fi
        ;;
    status)
        if reason=$(q_index_busy); then
            echo "GUARD SET: $reason"
            exit 0
        fi
        echo "GUARD NOT SET"
        exit 1
        ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 1 ;;
esac
