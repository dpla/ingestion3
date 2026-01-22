#!/bin/bash
# i3-harvest - Run DPLA ingestion3 harvest

# Java configuration - ensure we use Java 11+
JAVA_HOME_PATH="/Users/scott/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home"
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"

# SBT JVM options
export SBT_OPTS="-Xms1g -Xmx4g -XX:+UseG1GC"

# Ingestion3 configuration
I3_HOME="${I3_HOME:-/Users/scott/dpla/code/ingestion3}"
I3_CONF="${I3_CONF:-/Users/scott/dpla/code/ingestion3-conf/i3.conf}"
DPLA_DATA="${DPLA_DATA:-/Users/scott/dpla/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

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

echo "Using Java: $JAVA_HOME"
echo "Provider: $PROVIDER"
echo "Output: $OUTPUT"
echo "Config: $I3_CONF"
echo ""

cd "$I3_HOME" && sbt -java-home "$JAVA_HOME_PATH" "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
    --output=$OUTPUT \
    --conf=$I3_CONF \
    --name=$PROVIDER \
    --sparkMaster=$SPARK_MASTER"
