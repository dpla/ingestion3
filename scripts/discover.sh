#!/usr/bin/env bash
# discover.sh - Find record ids a Primo hub has added recently, and bank them.
#
# Hubs whose records can only be harvested by id (see README_INGESTS.md) need a
# way to learn about ids they do not already hold. Primo's `newrecords` facet
# provides one, but only reaches back a fixed window -- 90 days at most -- so a
# record added more than a window before the next harvest is invisible to it,
# permanently.
#
# This runs on its own schedule, far more often than the harvests, so the window
# is never the binding constraint. It writes ids only; the records themselves are
# fetched by the next full harvest.
#
# Usage:
#   ./scripts/discover.sh                # every hub with discovery enabled
#   ./scripts/discover.sh mississippi    # one hub
#   ./scripts/discover.sh all --no-s3    # skip the S3 upload (local testing)
#   ./scripts/discover.sh ohio --if-enabled   # no-op if ohio is not on this path
#
# Exit status is the contract for the scheduler: non-zero means at least one hub
# failed or returned an incomplete window.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

setup_java "2g" || die "Failed to setup Java environment"

HUB="all"
SYNC_S3=true
IF_ENABLED=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-s3)   SYNC_S3=false; shift ;;
        # For callers that name a hub without knowing whether it is on this
        # harvest path -- skip it instead of failing.
        --if-enabled) IF_ENABLED="--ifEnabled"; shift ;;
        -h|--help)
            cat <<'USAGE'
discover.sh - Bank record ids a Primo hub has added recently.

Usage:
  ./scripts/discover.sh                     every hub with discovery enabled
  ./scripts/discover.sh mississippi         one hub
  ./scripts/discover.sh ohio --if-enabled   no-op if ohio is not on this path
  ./scripts/discover.sh all --no-s3         skip the S3 upload (local testing)

Writes ids only; the records are fetched by the next full harvest.
Exits non-zero if any hub failed or returned an incomplete window.
USAGE
            exit 0 ;;
        -*)        die "Unknown option: $1" ;;
        *)         HUB="$1"; shift ;;
    esac
done

# One discovery run at a time. Two concurrent runs would hit the same partner
# endpoint twice as fast for no benefit, and could write two id files for the
# same window. Waits rather than fails: the caller (a schedule, or a harvest
# about to start) wants the ids, not an error. Nothing re-enters this script, so
# no re-entrancy escape hatch is needed.
LOCK_FILE="${TMPDIR:-/tmp}/i3-discover.lock"
if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCK_FILE"
    if ! flock -w 1800 9; then
        die "Timed out waiting for another discovery run to finish ($LOCK_FILE)"
    fi
fi

LOG_DIR="$I3_HOME/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/discover-${HUB}-$(date +%Y%m%d_%H%M%S).log"

echo "=============================================="
echo " DPLA id discovery"
echo "=============================================="
echo "Hub(s):  $HUB"
echo "Output:  $DPLA_DATA"
echo "Config:  $I3_CONF"
echo "Log:     $LOG_FILE"
echo ""

STATUS=0
run_entry dpla.ingestion3.entries.ingest.DiscoverIdsEntry \
    --output="$DPLA_DATA" \
    --conf="$I3_CONF" \
    --name="$HUB" $IF_ENABLED 2>&1 | tee "$LOG_FILE" || STATUS=$?

# Upload whatever was written even when the run reported trouble: a partial set
# of ids is still ids, and discarding them would put those records back outside
# the window.
#
# Via s3-sync.sh rather than a local `aws s3 sync`: it owns the destination
# bucket, the OS X junk-file excludes, and resolve_s3_prefix -- the hub-key to
# S3-prefix mapping (hathi -> hathitrust, tn -> tennessee). Writing the ids under
# the local directory name would silently misplace any hub whose two names differ.
if [[ "$SYNC_S3" == "true" ]]; then
    for dir in "$DPLA_DATA"/*/discovery; do
        [[ -d "$dir" ]] || continue
        hub_name=$(basename "$(dirname "$dir")")
        if [[ "$HUB" != "all" && "$hub_name" != "$HUB" ]]; then
            continue
        fi
        log_info "Syncing $hub_name discovery ids to S3"
        if ! "$SCRIPT_DIR/s3-sync.sh" "$hub_name" discovery; then
            log_error "S3 sync failed for $hub_name"
            # The ids exist locally, but EBS is not durable across an instance
            # replacement -- treat an unsynced run as failed so someone retries.
            STATUS=1
        fi
    done
fi

if [[ $STATUS -ne 0 ]]; then
    summary=$(grep -E "INCOMPLETE|FAILED|did not complete" "$LOG_FILE" | head -5 || true)
    slack_notify ":warning: *DPLA id discovery failed* (hub: \`$HUB\`, exit $STATUS)\n\`\`\`${summary:-see $LOG_FILE}\`\`\`"
    log_error "Discovery finished with errors; see $LOG_FILE"
    exit "$STATUS"
fi

# Deliberately silent on success. Every hub on this path adds records most weeks,
# so a success notification would be ~52 messages a year that say nothing -- and
# a channel people stop reading is worse than no channel.
log_success "Discovery complete"
