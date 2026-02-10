#!/usr/bin/env bash
# i3-enrich - Run DPLA ingestion3 enrichment step
# Enriches and normalizes DPLA MAP records

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (8g for enrichment)
setup_java "8g" || die "Failed to setup Java environment"

if [ -z "$1" ]; then
    echo "Usage: enrich.sh <provider-name> [input-path]"
    echo ""
    echo "If input-path is not specified, uses the most recent mapping from:"
    echo "  \$DPLA_DATA/<provider>/mapping"
    echo ""
    echo "Example: ./enrich.sh maryland"
    exit 1
fi

PROVIDER="$1"
INPUT="${2:-$(find_latest_data "$PROVIDER" "mapping" 2>/dev/null || echo "$DPLA_DATA/$PROVIDER/mapping")}"
# --output must be the DATA ROOT, not the provider dir.
# OutputHelper builds: root/shortName/enrichment/timestamp-shortName-schema
OUTPUT="$DPLA_DATA"

echo "Enrichment: $PROVIDER"
echo "Input:      $INPUT"
echo "Output:     $OUTPUT/$PROVIDER/enrichment/"
echo ""

run_entry dpla.ingestion3.entries.ingest.EnrichEntry \
    --input="$INPUT" \
    --output="$OUTPUT" \
    --name="$PROVIDER" \
    --sparkMaster="$SPARK_MASTER"
