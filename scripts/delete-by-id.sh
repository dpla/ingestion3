#!/usr/bin/env bash
# delete-by-id.sh - Delete DPLA records from Elasticsearch by DPLA ID
#
# Usage:
#   ./delete-by-id.sh <id1> [id2] [id3] ...
#   ./delete-by-id.sh -f <file-with-ids>
#
# Examples:
#   ./delete-by-id.sh 57d000d66004c73c5e31ea7dc7f57201 83851552c94d5b8372f99f8d635fc23e
#   ./delete-by-id.sh -f ids-to-delete.txt
#   cat ids.txt | ./delete-by-id.sh -f -
#
# Environment variables (optional):
#   ES_HOST      - Elasticsearch host (default: search-prod1.internal.dp.la:9200)
#   ES_INDEX     - Elasticsearch index name (default: auto-resolved from dpla_alias)
#   ES_ALIAS     - Alias to resolve index from (default: dpla_alias)
#   DRY_RUN      - Set to "true" to preview without deleting

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Configuration - can be overridden with environment variables
ES_HOST="${ES_HOST:-search-prod1.internal.dp.la:9200}"
ES_ALIAS="${ES_ALIAS:-dpla_alias}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_CONFIRM="false"

# Function to resolve alias to actual index name
resolve_alias() {
    local alias="$1"
    local response
    
    response=$(curl -s "http://$ES_HOST/_alias/$alias")
    
    # Check for error
    if echo "$response" | grep -q '"error"'; then
        echo "Error: Could not resolve alias '$alias'" >&2
        echo "$response" >&2
        return 1
    fi
    
    # Extract the index name (first key in the JSON response)
    local index_name
    index_name=$(echo "$response" | python3 -c "import sys, json; print(list(json.load(sys.stdin).keys())[0])" 2>/dev/null)
    
    if [[ -z "$index_name" ]]; then
        echo "Error: Could not parse index name from alias response" >&2
        return 1
    fi
    
    echo "$index_name"
}

usage() {
    echo "Usage: $0 [options] <id1> [id2] [id3] ..."
    echo "       $0 [options] -f <file-with-ids>"
    echo "       cat ids.txt | $0 -f -"
    echo ""
    echo "Delete DPLA records from Elasticsearch by DPLA ID."
    echo ""
    echo "Options:"
    echo "  -f <file>    Read IDs from file (one per line), use '-' for stdin"
    echo "  -y           Skip confirmation prompt"
    echo "  -h, --help   Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  ES_HOST      Elasticsearch host (default: search-prod1.internal.dp.la:9200)"
    echo "  ES_INDEX     Elasticsearch index name (default: auto-resolved from dpla_alias)"
    echo "  ES_ALIAS     Alias to resolve index from (default: dpla_alias)"
    echo "  DRY_RUN      Set to 'true' to preview the query without executing"
    echo ""
    echo "Examples:"
    echo "  $0 57d000d66004c73c5e31ea7dc7f57201"
    echo "  $0 -f ids-to-delete.txt"
    echo "  $0 -y -f ids-to-delete.txt              # Skip confirmation"
    echo "  ES_INDEX=dpla-all-20260202-164030 $0 -f ids.txt  # Skip alias resolution"
    echo "  DRY_RUN=true $0 -f ids.txt"
    exit 1
}

# Parse arguments
IDS=()
FROM_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -f)
            FROM_FILE="$2"
            shift 2
            ;;
        -y)
            SKIP_CONFIRM="true"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            IDS+=("$1")
            shift
            ;;
    esac
done

# Read IDs from file if specified
if [[ -n "$FROM_FILE" ]]; then
    if [[ "$FROM_FILE" == "-" ]]; then
        # Read from stdin
        while IFS= read -r line; do
            # Skip empty lines and comments
            line=$(echo "$line" | sed 's/#.*//' | tr -d '[:space:]')
            [[ -n "$line" ]] && IDS+=("$line")
        done
    elif [[ -f "$FROM_FILE" ]]; then
        while IFS= read -r line; do
            # Skip empty lines and comments
            line=$(echo "$line" | sed 's/#.*//' | tr -d '[:space:]')
            [[ -n "$line" ]] && IDS+=("$line")
        done < "$FROM_FILE"
    else
        echo "Error: File not found: $FROM_FILE" >&2
        exit 1
    fi
fi

# Check we have at least one ID
if [[ ${#IDS[@]} -eq 0 ]]; then
    echo "Error: No IDs provided" >&2
    usage
fi

# Build the JSON array of IDs
ID_JSON=""
for id in "${IDS[@]}"; do
    if [[ -n "$ID_JSON" ]]; then
        ID_JSON="$ID_JSON,"
    fi
    ID_JSON="$ID_JSON\"$id\""
done

# Resolve index name from alias if ES_INDEX not explicitly set
if [[ -z "$ES_INDEX" ]]; then
    echo "Resolving index from alias '$ES_ALIAS'..."
    if ! ES_INDEX=$(resolve_alias "$ES_ALIAS"); then
        echo "Error: Failed to resolve alias to index name" >&2
        exit 1
    fi
    if [[ -z "$ES_INDEX" ]]; then
        echo "Error: Alias resolved to empty index name" >&2
        exit 1
    fi
    echo "Resolved to index: $ES_INDEX"
    echo ""
fi

# Build the query
QUERY=$(cat <<EOF
{
  "query": {
    "terms": {
      "id": [$ID_JSON]
    }
  }
}
EOF
)

echo "Elasticsearch Delete-by-Query"
echo "=============================="
echo "Host:   $ES_HOST"
echo "Index:  $ES_INDEX"
echo "Alias:  $ES_ALIAS"
echo "IDs:    ${#IDS[@]} record(s)"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would execute:"
    echo ""
    echo "curl -XPOST \"http://$ES_HOST/$ES_INDEX/_delete_by_query\" \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '$QUERY'"
    echo ""
    echo "IDs to delete:"
    printf '%s\n' "${IDS[@]}"
    exit 0
fi

# Confirm before deleting
if [[ "$SKIP_CONFIRM" != "true" ]]; then
    echo "About to delete ${#IDS[@]} record(s). Continue? [y/N] "
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Execute the delete
echo "Deleting records..."
RESPONSE=$(curl -s -XPOST "http://$ES_HOST/$ES_INDEX/_delete_by_query" \
    -H 'Content-Type: application/json' \
    -d "$QUERY")

echo ""
echo "Response:"
echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

# Extract deleted count if possible
DELETED=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('deleted', 'unknown'))" 2>/dev/null || echo "unknown")
echo ""
echo "Deleted: $DELETED record(s)"
