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
    echo "  getty, gpo, harvard, hathi, heartland, ia, il, indiana, jhn, lc,"
    echo "  maryland, mi, minnesota, mississippi, mt, mwdl, nara, digitalnc,"
    echo "  njde, northwest-heritage, nypl, ohio, oklahoma, p2p, pa, david-rumsey,"
    echo "  scdl, sd, smithsonian, texas, tennessee, txdl, virginias, vt, wisconsin"
    exit 1
fi

PROVIDER="$1"
OUTPUT="$DPLA_DATA"

trap 'err=$?; if [[ $err -ne 0 ]]; then write_hub_status "$PROVIDER" failed --error="Exit $err"; fi' EXIT
write_hub_status "$PROVIDER" harvesting

echo "Using Java: $JAVA_HOME"
echo "Provider: $PROVIDER"
echo "Output: $OUTPUT"
echo "Config: $I3_CONF"
echo ""

run_entry dpla.ingestion3.entries.ingest.HarvestEntry \
    --output="$OUTPUT" \
    --conf="$I3_CONF" \
    --name="$PROVIDER" \
    --sparkMaster="$SPARK_MASTER"

write_hub_status "$PROVIDER" complete
