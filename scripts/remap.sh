#!/usr/bin/env bash
# i3-remap - Run mapping → enrichment → jsonl on existing harvest data
# Use this when you already have harvested data and want to re-process it

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (8g for remap)
setup_java "8g" || die "Failed to setup Java environment"

if [ -z "$1" ]; then
    echo "Usage: remap.sh <provider-name> [input-path]"
    echo ""
    echo "Runs mapping → enrichment → jsonl pipeline on harvested data."
    echo "If input-path is not specified, uses the most recent harvest from:"
    echo "  \$DPLA_DATA/<provider>/harvest"
    echo ""
    echo "Example: ./remap.sh maryland"
    exit 1
fi

PROVIDER="$1"
INPUT="${2:-$(find_latest_data "$PROVIDER" "harvest" 2>/dev/null || echo "$DPLA_DATA/$PROVIDER/harvest")}"
OUTPUT="$DPLA_DATA"

trap 'err=$?; if [[ $err -ne 0 ]]; then write_hub_status "$PROVIDER" failed --error="Exit $err"; fi' EXIT
write_hub_status "$PROVIDER" remapping

echo "=============================================="
echo "  IngestRemap: $PROVIDER"
echo "=============================================="
echo "Input:   $INPUT"
echo "Output:  $OUTPUT/$PROVIDER"
echo "Config:  $I3_CONF"
echo "=============================================="
echo ""

run_ingest_remap "$INPUT" "$OUTPUT" "$I3_CONF" "$PROVIDER"

write_hub_status "$PROVIDER" complete

echo ""
echo "=============================================="
echo "  Remap completed!"
echo "=============================================="
echo "Output files:"
echo "  Mapping:    $OUTPUT/$PROVIDER/mapping"
echo "  Enrichment: $OUTPUT/$PROVIDER/enrichment"
echo "  JSON-L:     $OUTPUT/$PROVIDER/jsonl"
echo ""
