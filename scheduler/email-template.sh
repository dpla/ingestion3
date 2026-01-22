#!/bin/bash
# email-template.sh - Generate personalized emails from template for scheduled ingests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULE_FILE="${SCRIPT_DIR}/schedule.json"
TEMPLATE_FILE="${SCRIPT_DIR}/email-template.txt"
OUTPUT_DIR="${SCRIPT_DIR}/emails"

# Get current month (1-12)
CURRENT_MONTH=$(date +%-m)
CURRENT_YEAR=$(date +%Y)
MONTH_NAMES=(January February March April May June July August September October November December)
CURRENT_MONTH_NAME=${MONTH_NAMES[$((CURRENT_MONTH - 1))]}

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed."
    exit 1
fi

# Check if template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "ERROR: email-template.txt not found at $TEMPLATE_FILE"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Generating email notifications for ${CURRENT_MONTH_NAME} ${CURRENT_YEAR}..."
echo ""

# Find hubs that need ingests this month
HUBS_TO_INGEST=$(jq -r --arg month "$CURRENT_MONTH" '
    .hubs[] |
    select(.schedule.months[] | tonumber == ($month | tonumber)) |
    "\(.provider_code)|\(.name)|\(.schedule.frequency)|\(.schedule.week // "")|\(.schedule.notes // "")|\(.dashboard_url)|\(.contacts | join(","))"
' "$SCHEDULE_FILE")

if [ -z "$HUBS_TO_INGEST" ]; then
    echo "No hubs scheduled for ingestion in ${CURRENT_MONTH_NAME}."
    exit 0
fi

# Month name mapping for display
MONTH_DISPLAY_NAMES=(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)
MONTH_DISPLAY=${MONTH_DISPLAY_NAMES[$((CURRENT_MONTH - 1))]}

EMAIL_COUNT=0
while IFS='|' read -r provider_code hub_name frequency week notes dashboard_url contacts; do
    EMAIL_COUNT=$((EMAIL_COUNT + 1))

    # Convert month numbers to names
    MONTHS_JSON=$(jq -r --arg code "$provider_code" '
        .hubs[] |
        select(.provider_code == $code) |
        .schedule.months |
        map({
            "1": "January", "2": "February", "3": "March", "4": "April",
            "5": "May", "6": "June", "7": "July", "8": "August",
            "9": "September", "10": "October", "11": "November", "12": "December"
        }[tostring]) |
        join(", ")
    ' "$SCHEDULE_FILE" | tr -d '\n')

    # Generate email filename
    SAFE_NAME=$(echo "$hub_name" | tr ' ' '_' | tr -cd '[:alnum:]_')
    EMAIL_FILE="${OUTPUT_DIR}/${provider_code}-${CURRENT_YEAR}-${MONTH_DISPLAY}.txt"

    # Escape special characters for sed
    ESCAPED_HUB_NAME=$(echo "$hub_name" | sed 's/[[\.*^$()+?{|]/\\&/g')
    ESCAPED_NOTES=$(echo "$notes" | sed 's/[[\.*^$()+?{|]/\\&/g')
    ESCAPED_DASHBOARD_URL=$(echo "$dashboard_url" | sed 's/[[\.*^$()+?{|]/\\&/g')
    ESCAPED_MONTHS=$(echo "$MONTHS_JSON" | sed 's/[[\.*^$()+?{|]/\\&/g')

    # Read template and substitute variables
    {
        sed "s|{{MONTH}}|${CURRENT_MONTH_NAME}|g" "$TEMPLATE_FILE" | \
        sed "s|{{YEAR}}|${CURRENT_YEAR}|g" | \
        sed "s|{{HUB_NAME}}|${ESCAPED_HUB_NAME}|g" | \
        sed "s|{{FREQUENCY}}|${frequency}|g" | \
        sed "s|{{MONTHS}}|${ESCAPED_MONTHS}|g" | \
        sed "s|{{DASHBOARD_URL}}|${ESCAPED_DASHBOARD_URL}|g" | \
        sed "s|{{NOTES}}|${ESCAPED_NOTES}|g" | \
        sed "s|{{WEEK}}|${week}|g"
    } > "$EMAIL_FILE"

    # Remove conditional blocks if empty (using different delimiter)
    if [ -z "$week" ]; then
        sed -i '' '/{{#WEEK}}/,/{{{\/WEEK}}}/d' "$EMAIL_FILE" 2>/dev/null || \
        sed -i '/{{#WEEK}}/,/{{{\/WEEK}}}/d' "$EMAIL_FILE"
    fi

    if [ -z "$notes" ]; then
        sed -i '' '/{{#NOTES}}/,/{{{\/NOTES}}}/d' "$EMAIL_FILE" 2>/dev/null || \
        sed -i '/{{#NOTES}}/,/{{{\/NOTES}}}/d' "$EMAIL_FILE"
    fi

    # Remove conditional blocks if empty
    if [ -z "$week" ]; then
        sed -i '' '/{{#WEEK}}/,/{{{\/WEEK}}}/d' "$EMAIL_FILE" 2>/dev/null || \
        sed -i '/{{#WEEK}}/,/{{{\/WEEK}}}/d' "$EMAIL_FILE"
    fi

    if [ -z "$notes" ]; then
        sed -i '' '/{{#NOTES}}/,/{{{\/NOTES}}}/d' "$EMAIL_FILE" 2>/dev/null || \
        sed -i '/{{#NOTES}}/,/{{{\/NOTES}}}/d' "$EMAIL_FILE"
    fi

    echo "  Generated: ${EMAIL_FILE}"
    echo "    To: ${contacts}"
    echo ""

done <<< "$HUBS_TO_INGEST"

echo "=================================================================="
echo "Generated ${EMAIL_COUNT} email(s) in ${OUTPUT_DIR}/"
echo ""
echo "To send emails, review the files and use your email client or:"
echo "  for file in ${OUTPUT_DIR}/*.txt; do"
echo "    # Extract recipient from filename or contacts in schedule.json"
echo "    # Send email using mail command or your email service"
echo "  done"
