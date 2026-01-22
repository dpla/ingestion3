#!/bin/bash
# i3-enrich - Run DPLA ingestion3 enrichment step
# Enriches and normalizes DPLA MAP records

set -e

# Java configuration
JAVA_HOME_PATH="/Users/scott/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home"
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"
export SBT_OPTS="-Xms2g -Xmx8g -XX:+UseG1GC"

# Ingestion3 configuration
I3_HOME="${I3_HOME:-/Users/scott/dpla/code/ingestion3}"
DPLA_DATA="${DPLA_DATA:-/Users/scott/dpla/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

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
INPUT="${2:-$DPLA_DATA/$PROVIDER/mapping}"
OUTPUT="$DPLA_DATA/$PROVIDER"

echo "Enrichment: $PROVIDER"
echo "Input:      $INPUT"
echo "Output:     $OUTPUT/enrichment"
echo ""

cd "$I3_HOME" && sbt -java-home "$JAVA_HOME_PATH" "runMain dpla.ingestion3.entries.ingest.EnrichEntry \
    --input=$INPUT \
    --output=$OUTPUT \
    --name=$PROVIDER \
    --sparkMaster=$SPARK_MASTER"
