#!/usr/bin/env bash
# send-ingest-email.sh - Send ingest summary email to hub contacts
#
# Usage:
#   ./scripts/send-ingest-email.sh <hub-name>
#   ./scripts/send-ingest-email.sh <hub-name> <mapping-dir>
#
# This script:
#   1. Finds the most recent mapping output for the hub
#   2. Reads the _SUMMARY file
#   3. Sends an email to the hub's configured contacts

set -euo pipefail

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (4g for email)
setup_java "4g" || die "Failed to setup Java environment"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <hub-name> [mapping-dir]"
    echo ""
    echo "Examples:"
    echo "  $0 maryland"
    echo "  $0 maryland /Users/scott/dpla/data/maryland/mapping/20260201_120000-maryland-MAP.avro"
    exit 1
fi

HUB="$1"
MAPPING_DIR="${2:-}"

# Get hub info using common.sh functions
PROVIDER_NAME=$(get_provider_name "$HUB")
EMAIL=$(get_hub_email "$HUB")

if [[ -z "$EMAIL" ]]; then
    echo "ERROR: No email configured for hub '$HUB' in i3.conf"
    echo "Add: ${HUB}.email = \"contact@example.com\""
    exit 1
fi

# Find mapping directory if not specified
if [[ -z "$MAPPING_DIR" ]]; then
    MAPPING_BASE="${DPLA_DATA}/${HUB}/mapping"
    if [[ ! -d "$MAPPING_BASE" ]]; then
        echo "ERROR: No mapping directory found at $MAPPING_BASE"
        exit 1
    fi
    MAPPING_DIR=$(ls -td "${MAPPING_BASE}"/*/ 2>/dev/null | head -1)
    if [[ -z "$MAPPING_DIR" ]]; then
        echo "ERROR: No mapping output found in $MAPPING_BASE"
        exit 1
    fi
fi

# Verify _SUMMARY file exists
SUMMARY_FILE="${MAPPING_DIR}/_SUMMARY"
if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "ERROR: Summary file not found: $SUMMARY_FILE"
    exit 1
fi

echo "=============================================="
echo "  Send Ingest Summary Email"
echo "=============================================="
echo "Hub:          $HUB"
echo "Provider:     $PROVIDER_NAME"
echo "Recipients:   $EMAIL"
echo "Mapping:      $MAPPING_DIR"
echo "Summary:      $SUMMARY_FILE"
echo "=============================================="
echo ""

# Display summary preview
echo "Summary Preview:"
echo "---"
head -30 "$SUMMARY_FILE"
echo "..."
echo "---"
echo ""

read -p "Send email? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Send email using the Scala Emailer
echo "Sending email..."

cd "$I3_HOME"

# Use sbt to invoke the Emailer
sbt -java-home "$JAVA_HOME" "runMain dpla.ingestion3.utils.Emailer \
    $MAPPING_DIR \
    $HUB"

if [[ $? -eq 0 ]]; then
    echo ""
    echo "Email sent successfully to: $EMAIL"
else
    echo ""
    echo "ERROR: Failed to send email"
    exit 1
fi
