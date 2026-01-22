#!/bin/bash
# i3-mapping - Run DPLA ingestion3 mapping step
# Transforms harvested records into DPLA MAP format

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
    echo "Usage: mapping.sh <provider-name> [input-path]"
    echo ""
    echo "If input-path is not specified, uses the most recent harvest from:"
    echo "  \$DPLA_DATA/<provider>/harvest"
    echo ""
    echo "Example: ./mapping.sh maryland"
    exit 1
fi

PROVIDER="$1"
INPUT="${2:-$DPLA_DATA/$PROVIDER/harvest}"
OUTPUT="$DPLA_DATA/$PROVIDER"

echo "Mapping: $PROVIDER"
echo "Input:   $INPUT"
echo "Output:  $OUTPUT/mapping"
echo ""

cd "$I3_HOME" && sbt -java-home "$JAVA_HOME_PATH" "runMain dpla.ingestion3.entries.ingest.MappingEntry \
    --input=$INPUT \
    --output=$OUTPUT \
    --name=$PROVIDER \
    --sparkMaster=$SPARK_MASTER"
