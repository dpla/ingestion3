#!/usr/bin/env bash
# send-ingest-email.sh - Send ingest summary email to hub contacts
#
# Usage:
#   ./scripts/send-ingest-email.sh <hub-name>
#   ./scripts/send-ingest-email.sh <hub-name> <mapping-dir>
#   ./scripts/send-ingest-email.sh --yes <hub-name>
#   ./scripts/send-ingest-email.sh --email-override test@example.com <hub-name>
#
# Options:
#   --yes, -y                   Skip confirmation prompt and send immediately
#   --email-override <address>  Send to this address instead of the hub's configured
#                               contacts. Useful for testing before sending to the
#                               real hub. (e.g. --email-override ingest@dp.la)
#
# This script:
#   1. Finds the most recent mapping output for the hub
#   2. Reads the _SUMMARY file
#   3. Sends an email to the hub's configured contacts (or override address)

set -euo pipefail

# Source common configuration (common.sh is in scripts/ root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# Setup Java environment (4g for email)
setup_java "4g" || die "Failed to setup Java environment"

# Parse options
SKIP_CONFIRM=false
EMAIL_OVERRIDE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --yes|-y)
            SKIP_CONFIRM=true
            shift
            ;;
        --email-override)
            EMAIL_OVERRIDE="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--yes] [--email-override <address>] <hub-name> [mapping-dir]"
    echo ""
    echo "Options:"
    echo "  --yes, -y                   Skip confirmation prompt"
    echo "  --email-override <address>  Override recipient (for testing)"
    echo ""
    echo "Examples:"
    echo "  $0 maryland"
    echo "  $0 --yes nara"
    echo "  $0 --email-override ingest@dp.la --yes nara   # test run"
    echo "  $0 maryland /Users/scott/dpla/data/maryland/mapping/20260201_120000-maryland-MAP.avro"
    exit 1
fi

HUB="$1"
MAPPING_DIR="${2:-}"
MERGE_SUMMARY_PATH=""

# Get hub info using common.sh functions
PROVIDER_NAME=$(get_provider_name "$HUB")
EMAIL=$(get_hub_email "$HUB")

if [[ -z "$EMAIL" ]] && [[ -z "$EMAIL_OVERRIDE" ]]; then
    echo "ERROR: No email configured for hub '$HUB' in i3.conf"
    echo "Add: ${HUB}.email = \"contact@example.com\""
    exit 1
fi

# Apply override if set — replaces the hub's configured address entirely
if [[ -n "$EMAIL_OVERRIDE" ]]; then
    echo "⚠️  EMAIL OVERRIDE ACTIVE — sending to $EMAIL_OVERRIDE instead of configured contacts"
    EMAIL="$EMAIL_OVERRIDE"
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
if [[ "$HUB" == "nara" ]]; then
echo "(NARA merge stats will be auto-detected and included)"
fi
echo "=============================================="
echo ""

# Display summary preview
echo "Summary Preview:"
echo "---"
head -30 "$SUMMARY_FILE"
echo "..."
echo "---"
echo ""

# Confirmation prompt (skip if --yes)
if [[ "$SKIP_CONFIRM" == "false" ]]; then
    read -p "Send email? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
else
    echo "Sending (--yes flag provided)..."
fi

# For NARA: find the most recent merge _SUMMARY.txt and include delete stats
if [[ "$HUB" == "nara" ]]; then
    NARA_HARVEST_DIR="${DPLA_DATA}/nara/harvest"
    if [[ -d "$NARA_HARVEST_DIR" ]]; then
        LATEST_HARVEST=$(ls -td "${NARA_HARVEST_DIR}"/*-nara-OriginalRecord.avro 2>/dev/null | head -1)
        if [[ -n "$LATEST_HARVEST" && -f "$LATEST_HARVEST/_LOGS/_SUMMARY.txt" ]]; then
            MERGE_SUMMARY_PATH="$LATEST_HARVEST/_LOGS/_SUMMARY.txt"
            echo "Merge summary:  $MERGE_SUMMARY_PATH"
        else
            echo "WARNING: No merge _SUMMARY.txt found under $NARA_HARVEST_DIR — delete stats will be omitted from email."
        fi
    fi
fi
echo ""

# Send email using the Scala Emailer
echo "Sending email..."

cd "$I3_HOME"

# Build the sbt command. For NARA, pass --merge-summary so delete stats appear in the email.
EMAILER_ARGS="$MAPPING_DIR $HUB"
if [[ -n "$MERGE_SUMMARY_PATH" ]]; then
    EMAILER_ARGS="$EMAILER_ARGS --merge-summary $MERGE_SUMMARY_PATH"
fi
if [[ -n "$EMAIL_OVERRIDE" ]]; then
    EMAILER_ARGS="$EMAILER_ARGS --email-override $EMAIL_OVERRIDE"
fi

sbt -java-home "$JAVA_HOME" "runMain dpla.ingestion3.utils.Emailer $EMAILER_ARGS"

if [[ $? -eq 0 ]]; then
    echo ""
    echo "Email sent successfully to: $EMAIL"
else
    echo ""
    echo "ERROR: Failed to send email"
    exit 1
fi
