#!/bin/bash
# schedule.sh - Generate monthly ingest commands based on schedule.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULE_FILE="${SCRIPT_DIR}/schedule.json"
I3_HOME="${I3_HOME:-$(dirname "$SCRIPT_DIR")}"

# Get current month (1-12)
CURRENT_MONTH=$(date +%-m)
CURRENT_YEAR=$(date +%Y)
MONTH_NAMES=(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)
CURRENT_MONTH_NAME=${MONTH_NAMES[$((CURRENT_MONTH - 1))]}

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed."
    echo "Install with: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
fi

# Check if schedule.json exists
if [ ! -f "$SCHEDULE_FILE" ]; then
    echo "ERROR: schedule.json not found at $SCHEDULE_FILE"
    exit 1
fi

echo "Monthly Ingest Schedule for ${CURRENT_MONTH_NAME} ${CURRENT_YEAR}"
echo "=================================================================="
echo ""

# Find hubs that need ingests this month
HUBS_TO_INGEST=$(jq -r --arg month "$CURRENT_MONTH" '
    .hubs[] |
    select(.schedule.months[] | tonumber == ($month | tonumber)) |
    "\(.provider_code)|\(.name)|\(.schedule.frequency)|\(.schedule.week // "")"
' "$SCHEDULE_FILE")

if [ -z "$HUBS_TO_INGEST" ]; then
    echo "No hubs scheduled for ingestion in ${CURRENT_MONTH_NAME}."
    exit 0
fi

# Count hubs
HUB_COUNT=$(echo "$HUBS_TO_INGEST" | wc -l | tr -d ' ')
echo "Found ${HUB_COUNT} hub(s) scheduled for ${CURRENT_MONTH_NAME}:"
echo ""

# Generate ingest commands
COMMANDS_FILE="${SCRIPT_DIR}/ingest-commands-${CURRENT_YEAR}-${CURRENT_MONTH_NAME}.sh"
{
    echo "#!/bin/bash"
    echo "# Auto-generated ingest commands for ${CURRENT_MONTH_NAME} ${CURRENT_YEAR}"
    echo "# Generated on $(date)"
    echo ""
    echo "set -euo pipefail"
    echo ""
    echo "I3_HOME=\"${I3_HOME}\""
    echo "cd \"\$I3_HOME\""
    echo ""
} > "$COMMANDS_FILE"

while IFS='|' read -r provider_code hub_name frequency week; do
    echo "  - ${hub_name} (${provider_code})"
    echo "    Frequency: ${frequency}"
    if [ -n "$week" ]; then
        echo "    Week: ${week}"
    fi

    # Generate command - NARA uses a special script due to delta merge process
    {
        echo ""
        echo "# Ingest: ${hub_name}"
        echo "echo \"Starting ingest for ${hub_name}...\""
        if [ "$week" = "last" ]; then
            echo "# Scheduled for last week of month"
        fi
        
        if [ "$provider_code" = "nara" ]; then
            echo "# NARA requires delta ingest with ZIP file from NARA"
            echo "# See README_NARA.md for details"
            echo "# ./nara-ingest.sh /path/to/nara-export.zip"
            echo "echo \"NARA requires manual delta ingest. Run: ./nara-ingest.sh /path/to/nara-export.zip\""
        else
            echo "./ingest.sh ${provider_code}"
        fi
        echo "if [ \$? -eq 0 ]; then"
        echo "    echo \"✓ ${hub_name} ingest completed successfully\""
        echo "else"
        echo "    echo \"✗ ${hub_name} ingest failed\""
        echo "    exit 1"
        echo "fi"
    } >> "$COMMANDS_FILE"

done <<< "$HUBS_TO_INGEST"

chmod +x "$COMMANDS_FILE"

echo ""
echo "=================================================================="
echo "Generated ingest commands: ${COMMANDS_FILE}"
echo ""
echo "To execute all ingests for this month, run:"
echo "  ${COMMANDS_FILE}"
echo ""
echo "Or run individual ingests:"
while IFS='|' read -r provider_code hub_name frequency week; do
    if [ "$provider_code" = "nara" ]; then
        echo "  ./nara-ingest.sh <nara-export.zip>  # ${hub_name} (requires ZIP from NARA)"
    else
        echo "  ./ingest.sh ${provider_code}  # ${hub_name}"
    fi
done <<< "$HUBS_TO_INGEST"
