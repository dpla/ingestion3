#!/usr/bin/env bash
# i3-ingest - Run full DPLA ingestion3 pipeline (harvest → mapping → enrichment → jsonl)
# This is the "fire and forget" script for a complete provider ingest

set -e  # Exit on any error

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (8g default memory)
setup_java "8g" || die "Failed to setup Java environment"

usage() {
    echo "Usage: ingest.sh <provider-name> [options]"
    echo ""
    echo "Options:"
    echo "  --skip-harvest    Skip harvest step (use existing harvest data)"
    echo "  --harvest-only    Only run harvest step"
    echo "  --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./ingest.sh maryland              # Full pipeline"
    echo "  ./ingest.sh maryland --skip-harvest  # Use existing harvest"
    echo "  ./ingest.sh maryland --harvest-only  # Only harvest"
    echo ""
    echo "Available providers: artstor, bhl, community-webs, ct, florida, georgia,"
    echo "  getty, gpo, harvard, hathi, heartland, ia, il, indiana, jhn, lc,"
    echo "  maryland, mi, minnesota, mississippi, mt, mwdl, nara, digitalnc,"
    echo "  njde, northwest-heritage, nypl, ohio, oklahoma, p2p, pa, david-rumsey,"
    echo "  scdl, sd, smithsonian, texas, tennessee, txdl, virginias, vt, wisconsin"
    exit 1
}

if [ -z "$1" ] || [ "$1" == "--help" ]; then
    usage
fi

PROVIDER="$1"
shift

SKIP_HARVEST=false
HARVEST_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-harvest)
            SKIP_HARVEST=true
            shift
            ;;
        --harvest-only)
            HARVEST_ONLY=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Setup paths
PROVIDER_DATA="$DPLA_DATA/$PROVIDER"
HARVEST_DIR="$PROVIDER_DATA/harvest"

echo ""
echo "=============================================="
echo "  DPLA Ingestion3 Pipeline"
echo "=============================================="
echo "Provider:     $PROVIDER"
echo "Data Dir:     $PROVIDER_DATA"
echo "Config:       $I3_CONF"
echo "Spark Master: $SPARK_MASTER"
echo "Java:         $JAVA_HOME"
echo "=============================================="
echo ""

START_TIME=$(date +%s)

cd "$I3_HOME"

# Step 1: Harvest
if [ "$SKIP_HARVEST" = false ]; then
    print_step "Step 1/2: Harvesting $PROVIDER..."

    sbt -java-home "$JAVA_HOME" "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
        --output=$HARVEST_DIR \
        --conf=$I3_CONF \
        --name=$PROVIDER \
        --sparkMaster=$SPARK_MASTER"

    # Note: set -e ensures we exit on sbt failure
    log_info "Harvest complete"
else
    print_step "Skipping harvest (using existing data)..."
fi

if [ "$HARVEST_ONLY" = true ]; then
    log_info "Harvest-only mode - stopping here"
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo ""
    echo "=============================================="
    echo "  Harvest completed in $((DURATION / 60))m $((DURATION % 60))s"
    echo "=============================================="
    exit 0
fi

# Step 2: Mapping + Enrichment + JSON-L (IngestRemap)
print_step "Step 2/2: Mapping → Enrichment → JSON-L..."

run_ingest_remap "$HARVEST_DIR" "$PROVIDER_DATA" "$I3_CONF" "$PROVIDER"

# Note: set -e ensures we exit on sbt failure

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=============================================="
echo "  Pipeline completed successfully!"
echo "  Duration: $((DURATION / 60))m $((DURATION % 60))s"
echo "=============================================="
echo ""
echo "Output files:"
echo "  Harvest:    $HARVEST_DIR"
echo "  Mapping:    $PROVIDER_DATA/mapping"
echo "  Enrichment: $PROVIDER_DATA/enrichment"
echo "  JSON-L:     $PROVIDER_DATA/jsonl"
echo ""
