#!/usr/bin/env bash
# i3-harvest - Run DPLA ingestion3 harvest

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (4g for harvest)
setup_java "4g" || die "Failed to setup Java environment"

if [ -z "$1" ]; then
    echo "Usage: harvest.sh <provider-name>"
    echo "Example: ./harvest.sh harvard"
    echo ""
    echo "Available providers: artstor, bhl, community-webs, ct, florida, georgia,"
    echo "  getty, gpo, harvard, hathi, heartland, ia, il, indiana, jh3, lc,"
    echo "  maryland, mi, minnesota, mississippi, mt, mwdl, nara, digitalnc,"
    echo "  njde, northwest-heritage, nypl, ohio, oklahoma, p2p, pa, david-rumsey,"
    echo "  scdl, sd, smithsonian, texas, tennessee, txdl, virginias, vt, wisconsin"
    exit 1
fi

PROVIDER="$1"
OUTPUT="$DPLA_DATA"

trap 'err=$?
    [[ -n "${HARVEST_CONF:-}" && "${HARVEST_CONF}" != "${I3_CONF:-}" ]] && rm -f "$HARVEST_CONF"
    if [[ $err -ne 0 ]]; then
        write_hub_status "$PROVIDER" failed --error="Exit $err" || true
    fi' EXIT
# Without explicit SIGTERM/SIGINT traps, bash exits immediately on those signals
# and skips the EXIT trap above. These traps call exit() to ensure EXIT fires.
trap 'exit 130' INT
trap 'exit 143' TERM
write_hub_status "$PROVIDER" harvesting

# File hubs: harvest the newest delivery in the hub's bucket (see
# resolve_file_delivery in common.sh). Fails if there's nothing to harvest or
# an older delivery was already harvested.
_fd_rc=0
resolve_file_delivery "$PROVIDER" || _fd_rc=$?
case "$_fd_rc" in
    0) if [[ "$FILE_DELIVERY_RESULT" == "ok" ]]; then log_info "Harvesting delivery: $FILE_DELIVERY_URI"; fi ;;
    2) die "Nothing to harvest for $PROVIDER: $FILE_DELIVERY_REASON" ;;
    3) die "No new delivery for $PROVIDER: newest is $FILE_DELIVERY_URI (not this month), already harvested by $FILE_DELIVERY_LAST. Set FILE_DELIVERY=$FILE_DELIVERY_URI to re-harvest it." ;;
    *) die "Couldn't pick a delivery for $PROVIDER (exit $_fd_rc)" ;;
esac

echo "Using Java: $JAVA_HOME"
echo "Provider: $PROVIDER"
echo "Output: $OUTPUT"
echo "Config: $I3_CONF"
echo "OAI log:  $I3_HOME/logs/oai-harvest-$PROVIDER-<timestamp>.log"
echo ""

run_entry dpla.ingestion3.entries.ingest.HarvestEntry \
    --output="$OUTPUT" \
    --conf="$HARVEST_CONF" \
    --name="$PROVIDER" \
    --sparkMaster="$SPARK_MASTER"

write_hub_status "$PROVIDER" complete
