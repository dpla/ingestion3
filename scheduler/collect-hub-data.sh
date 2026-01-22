#!/bin/bash
# collect-hub-data.sh - Extract hub metadata from Atlassian Confluence dashboard pages

set -euo pipefail

BASE_URL="https://digitalpubliclibraryofamerica.atlassian.net"
OUTPUT_FILE="scheduler/hub-data-raw.json"
SUMMARY_FILE="scheduler/hub-data-summary.txt"

# List of hub dashboard URLs (relative paths)
HUB_URLS=(
    "/wiki/spaces/CT/pages/113573915/David+Rumsey+Map+Collection+Dashboard"
    "/wiki/spaces/CT/pages/113672258/Internet+Archive+Dashboard"
    "/wiki/spaces/CT/pages/113868824/Harvard+Library+Dashboard"
    "/wiki/spaces/CT/pages/113901613/National+Archives+and+Records+Administration+Dashboard"
    "/wiki/spaces/CT/pages/149192718/Sunshine+State+Digital+Network+Dashboard"
    "/wiki/spaces/CT/pages/177963022/OKHub+Dashboard"
    "/wiki/spaces/CT/pages/235143169/Ohio+Digital+Network+Dashboard"
    "/wiki/spaces/CT/pages/276889601/District+Digital+Dashboard"
    "/wiki/spaces/CT/pages/391675905/Plains+to+Peaks+Dashboard"
    "/wiki/spaces/CT/pages/693075973/Template+Dashboard"
    "/wiki/spaces/CT/pages/84938455/New+York+Public+Library+Dashboard"
    "/wiki/spaces/CT/pages/84970940/Library+of+Congress+Dashboard"
    "/wiki/spaces/CT/pages/84971116/NCDHC+Dashboard"
    "/wiki/spaces/CT/pages/84971302/Indiana+Memory+Dashboard"
    "/wiki/spaces/CT/pages/84971401/DLG+Dashboard"
    "/wiki/spaces/CT/pages/85141812/BHL+Dashboard"
    "/wiki/spaces/CT/pages/85417652/MDL+Dashboard"
    "/wiki/spaces/CT/pages/85417844/Illinois+Digital+Heritage+Dashboard"
    "/wiki/spaces/CT/pages/85462286/Heartland+Hub+Dashboard"
    "/wiki/spaces/CT/pages/85463029/Portal+to+Texas+History+Dashboard"
    "/wiki/spaces/CT/pages/85627860/Recollection+Wisconsin+Dashboard"
    "/wiki/spaces/CT/pages/85628036/South+Carolina+Digital+Library+Dashboard"
    "/wiki/spaces/CT/pages/85628206/Digital+Commonwealth+Dashboard"
    "/wiki/spaces/CT/pages/85628441/Smithsonian+Institution+Dashboard"
    "/wiki/spaces/CT/pages/85920546/Digital+Library+of+Tennessee+Dashboard"
    "/wiki/spaces/CT/pages/85929619/Michigan+Service+Hub+Dashboard"
    "/wiki/spaces/CT/pages/85929877/Mountain+West+Digital+Library+Dashboard"
    "/wiki/spaces/CT/pages/85930296/Getty+Dashboard"
    "/wiki/spaces/CT/pages/85930880/USC+Dashboard"
    "/wiki/spaces/CT/pages/862945312/Orbis+Cascade+Alliance+Dashboard"
    "/wiki/spaces/CT/pages/863207453/Digital+Virginias+Dashboard"
    "/wiki/spaces/CT/pages/86430746/CDL+Dashboard"
    "/wiki/spaces/CT/pages/86496527/Digital+Maine+Dashboard"
    "/wiki/spaces/CT/pages/86496913/HathiTrust+Dashboard"
    "/wiki/spaces/CT/pages/86498117/Big+Sky+Country+Digital+Network+Dashboard"
    "/wiki/spaces/CT/pages/86498373/Digital+Maryland+Dashboard"
    "/wiki/spaces/CT/pages/86543081/PA+Digital+Dashboard"
    "/wiki/spaces/CT/pages/88075716/GPO+Dashboard"
)

# Function to extract hub name from URL
extract_hub_name() {
    local url="$1"
    # Extract name from URL path, replace + with spaces, remove "Dashboard"
    echo "$url" | sed 's|.*/pages/[0-9]*/||' | sed 's/+Dashboard//' | sed 's/+/\ /g' | sed 's/Dashboard$//' | xargs
}

# Function to extract emails from HTML content
extract_emails() {
    local html="$1"
    # Extract mailto: links
    echo "$html" | grep -oE 'mailto:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' | sed 's/mailto://' | sort -u
    # Extract plain email addresses (common patterns)
    echo "$html" | grep -oE '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' | grep -vE '^(window|http|https|data|javascript)' | sort -u
}

# Function to extract feed information
extract_feed_info() {
    local html="$1"
    local feed_type=""
    local feed_url=""

    # Look for OAI-PMH indicators
    if echo "$html" | grep -qiE '(oai-pmh|oai\.pmh|OAI)' ; then
        feed_type="OAI-PMH"
        # Try to extract OAI endpoint
        feed_url=$(echo "$html" | grep -oE 'https?://[^"'\''\s<>]+oai[^"'\''\s<>]*' | head -1 || echo "")
    fi

    # Look for API indicators
    if echo "$html" | grep -qiE '(api|endpoint|json|xml)' && [ -z "$feed_type" ]; then
        feed_type="API"
    fi

    # Look for file-based indicators
    if echo "$html" | grep -qiE '(file|ftp|sftp|s3|bucket)' && [ -z "$feed_type" ]; then
        feed_type="File-based"
    fi

    echo "$feed_type|$feed_url"
}

# Function to extract metadata format
extract_metadata_format() {
    local html="$1"
    local format=""

    # Common metadata formats
    if echo "$html" | grep -qiE '\b(MODS|mods)\b' ; then
        format="MODS"
    elif echo "$html" | grep -qiE '\b(Dublin Core|DC|dublincore)\b' ; then
        format="Dublin Core"
    elif echo "$html" | grep -qiE '\b(MARC|marc)\b' ; then
        format="MARC"
    elif echo "$html" | grep -qiE '\b(EDM|Europeana)\b' ; then
        format="EDM"
    elif echo "$html" | grep -qiE '\b(JSON|json)\b' ; then
        format="JSON"
    elif echo "$html" | grep -qiE '\b(XML|xml)\b' ; then
        format="XML"
    fi

    echo "$format"
}

# Function to extract schedule information
extract_schedule() {
    local html="$1"
    local schedule_text=""

    # Look for schedule patterns
    if echo "$html" | grep -qiE '(ingested|schedule|quarterly|monthly|bi-annually|bi-monthly)' ; then
        schedule_text=$(echo "$html" | grep -iE '(ingested|schedule|quarterly|monthly|bi-annually|bi-monthly)[^<]*' | head -1 | sed 's/<[^>]*>//g' | xargs)
    fi

    echo "$schedule_text"
}

# Main collection loop
echo "[" > "$OUTPUT_FILE"
echo "Collecting hub data from ${#HUB_URLS[@]} dashboard pages..." > "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

first=true
for url_path in "${HUB_URLS[@]}"; do
    full_url="${BASE_URL}${url_path}"
    hub_name=$(extract_hub_name "$url_path")

    echo "Processing: $hub_name"
    echo "  URL: $full_url" >> "$SUMMARY_FILE"

    # Fetch page content
    html_content=$(curl -sL "$full_url" || echo "")

    if [ -z "$html_content" ]; then
        echo "  ERROR: Failed to fetch page" >> "$SUMMARY_FILE"
        continue
    fi

    # Extract data
    emails=($(extract_emails "$html_content" | head -5))
    feed_info=$(extract_feed_info "$html_content")
    feed_type=$(echo "$feed_info" | cut -d'|' -f1)
    feed_url=$(echo "$feed_info" | cut -d'|' -f2)
    metadata_format=$(extract_metadata_format "$html_content")
    schedule_text=$(extract_schedule "$html_content")

    # Build JSON entry
    if [ "$first" = false ]; then
        echo "," >> "$OUTPUT_FILE"
    fi
    first=false

    {
        echo "  {"
        echo "    \"name\": \"$hub_name\","
        echo "    \"dashboard_url\": \"$full_url\","
        echo "    \"contacts\": ["
        if [ ${#emails[@]} -gt 0 ]; then
            for i in "${!emails[@]}"; do
                if [ $i -gt 0 ]; then
                    echo ","
                fi
                echo -n "      \"${emails[$i]}\""
            done
            echo ""
        fi
        echo "    ],"
        echo "    \"feed_type\": \"$feed_type\","
        echo "    \"feed_url\": \"$feed_url\","
        echo "    \"metadata_format\": \"$metadata_format\","
        echo "    \"schedule_text\": \"$schedule_text\","
        echo "    \"notes\": \"\""
        echo -n "  }"
    } >> "$OUTPUT_FILE"

    # Add to summary
    echo "  Contacts: ${#emails[@]} email(s) found" >> "$SUMMARY_FILE"
    if [ ${#emails[@]} -gt 0 ]; then
        printf "    - %s\n" "${emails[@]}" >> "$SUMMARY_FILE"
    fi
    echo "  Feed Type: $feed_type" >> "$SUMMARY_FILE"
    echo "  Metadata Format: $metadata_format" >> "$SUMMARY_FILE"
    echo "  Schedule: $schedule_text" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"

    # Be nice to the server
    sleep 1
done

echo "" >> "$OUTPUT_FILE"
echo "]" >> "$OUTPUT_FILE"

echo ""
echo "Data collection complete!"
echo "Raw data: $OUTPUT_FILE"
echo "Summary: $SUMMARY_FILE"
