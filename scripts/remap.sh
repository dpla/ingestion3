#!/bin/bash
# i3-remap - Run mapping → enrichment → jsonl on existing harvest data
# Use this when you already have harvested data and want to re-process it

set -e

# Java configuration
JAVA_HOME_PATH="/Users/scott/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home"
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"
export SBT_OPTS="-Xms2g -Xmx8g -XX:+UseG1GC"

# Ingestion3 configuration
I3_HOME="${I3_HOME:-/Users/scott/dpla/code/ingestion3}"
I3_CONF="${I3_CONF:-/Users/scott/dpla/code/ingestion3-conf/i3.conf}"
DPLA_DATA="${DPLA_DATA:-/Users/scott/dpla/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

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
INPUT="${2:-$DPLA_DATA/$PROVIDER/harvest}"
OUTPUT="$DPLA_DATA"

echo "=============================================="
echo "  IngestRemap: $PROVIDER"
echo "=============================================="
echo "Input:   $INPUT"
echo "Output:  $OUTPUT/$PROVIDER"
echo "Config:  $I3_CONF"
echo "=============================================="
echo ""

cd "$I3_HOME" && sbt -java-home "$JAVA_HOME_PATH" "runMain dpla.ingestion3.entries.ingest.IngestRemap \
    --input=$INPUT \
    --output=$OUTPUT \
    --conf=$I3_CONF \
    --name=$PROVIDER \
    --sparkMaster=$SPARK_MASTER"

echo ""
echo "=============================================="
echo "  Remap completed!"
echo "=============================================="
echo "Output files:"
echo "  Mapping:    $OUTPUT/$PROVIDER/mapping"
echo "  Enrichment: $OUTPUT/$PROVIDER/enrichment"
echo "  JSON-L:     $OUTPUT/$PROVIDER/jsonl"
echo ""
