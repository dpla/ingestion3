#!/usr/bin/env bash
# i3-mapping - Run DPLA ingestion3 mapping step
# Transforms harvested records into DPLA MAP format

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (8g for mapping)
setup_java "8g" || die "Failed to setup Java environment"

if [ -z "$1" ]; then
    echo "Usage: mapping.sh <provider-name> [input-path]"
    echo ""
    echo "If input-path is not specified, uses the most recent harvest from:"
    echo "  \$DPLA_DATA/<provider>/harvest"
    echo ""
    echo "Example: ./mapping.sh maryland"
    exit 1
fi

PROVIDER="$1"
INPUT="${2:-$(find_latest_data "$PROVIDER" "harvest" 2>/dev/null || echo "$DPLA_DATA/$PROVIDER/harvest")}"
# --output must be the DATA ROOT, not the provider dir.
# OutputHelper builds: root/shortName/mapping/timestamp-shortName-schema
OUTPUT="$DPLA_DATA"

echo "Mapping: $PROVIDER"
echo "Input:   $INPUT"
echo "Output:  $OUTPUT/$PROVIDER/mapping/"
echo ""

run_entry dpla.ingestion3.entries.ingest.MappingEntry \
    --input="$INPUT" \
    --output="$OUTPUT" \
    --name="$PROVIDER" \
    --sparkMaster="$SPARK_MASTER"
