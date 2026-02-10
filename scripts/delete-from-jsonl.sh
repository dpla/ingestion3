#!/usr/bin/env bash
# delete-from-jsonl.sh - Delete DPLA records from JSONL files in S3
#
# NOTE: Consider using delete-from-jsonl.py instead - it's more efficient
#       (uses boto3, parallel processing, and avoids subprocess overhead)
#
# Usage:
#   ./delete-from-jsonl.sh --hub <hub> -f <file-with-ids>
#   ./delete-from-jsonl.sh --hub <hub> <id1> [id2] [id3] ...
#
# Examples:
#   ./delete-from-jsonl.sh --hub cdl -f ids-to-delete.txt
#   ./delete-from-jsonl.sh --hub cdl 57d000d66004c73c5e31ea7dc7f57201
#
# Environment variables (optional):
#   S3_BUCKET    - S3 bucket name (default: dpla-master-dataset)
#   DRY_RUN      - Set to "true" to preview without modifying S3

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Configuration
S3_BUCKET="${S3_BUCKET:-dpla-master-dataset}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_CONFIRM="false"
HUB=""

usage() {
    echo "Usage: $0 --hub <hub> [options] <id1> [id2] [id3] ..."
    echo "       $0 --hub <hub> [options] -f <file-with-ids>"
    echo ""
    echo "Delete DPLA records from JSONL files stored in S3."
    echo ""
    echo "Required:"
    echo "  --hub <hub>  Hub/provider short name (e.g., cdl, mdl, pa)"
    echo ""
    echo "Options:"
    echo "  -f <file>    Read IDs from file (one per line), use '-' for stdin"
    echo "  -y           Skip confirmation prompt"
    echo "  -h, --help   Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  S3_BUCKET    S3 bucket name (default: dpla-master-dataset)"
    echo "  DRY_RUN      Set to 'true' to preview without modifying S3"
    echo ""
    echo "Examples:"
    echo "  $0 --hub cdl -f ids-to-delete.txt"
    echo "  $0 --hub cdl -y -f ids.txt                 # Skip confirmation"
    echo "  DRY_RUN=true $0 --hub cdl -f ids.txt       # Preview only"
    echo ""
    echo "The script will:"
    echo "  1. Find the most recent JSONL export in s3://\$S3_BUCKET/<hub>/jsonl/"
    echo "  2. Download, filter, and re-upload each JSONL file"
    echo "  3. Records with matching IDs in _source.id will be removed"
    echo ""
    echo "NOTE: For better performance, use delete-from-jsonl.py instead."
    exit 1
}

# Find the most recent JSONL export directory for a hub
find_latest_jsonl() {
    local hub="$1"
    local s3_path="s3://$S3_BUCKET/$hub/jsonl/"

    # List directories and get the most recent one (sorted by name, which includes timestamp)
    local latest
    latest=$(aws s3 ls "$s3_path" | grep 'PRE' | awk '{print $2}' | sort -r | head -1 | tr -d '/')

    if [[ -z "$latest" ]]; then
        echo "Error: No JSONL exports found in $s3_path" >&2
        return 1
    fi

    echo "$latest"
}

# Parse arguments
IDS=()
FROM_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --hub)
            HUB="$2"
            shift 2
            ;;
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

# Validate hub is provided
if [[ -z "$HUB" ]]; then
    echo "Error: Hub (--hub) is required" >&2
    usage
fi

# Read IDs from file if specified
if [[ -n "$FROM_FILE" ]]; then
    if [[ "$FROM_FILE" == "-" ]]; then
        while IFS= read -r line; do
            line=$(echo "$line" | sed 's/#.*//' | tr -d '[:space:]')
            [[ -n "$line" ]] && IDS+=("$line")
        done
    elif [[ -f "$FROM_FILE" ]]; then
        while IFS= read -r line; do
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

# Create a temporary directory for processing
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Create a file with IDs for grep
ID_FILE="$TEMP_DIR/ids.txt"
printf '%s\n' "${IDS[@]}" > "$ID_FILE"

# Find the latest JSONL export
echo "Finding latest JSONL export for hub '$HUB'..."
LATEST_EXPORT=$(find_latest_jsonl "$HUB")
if [[ $? -ne 0 ]] || [[ -z "$LATEST_EXPORT" ]]; then
    echo "Error: Could not find JSONL export" >&2
    exit 1
fi

S3_JSONL_PATH="s3://$S3_BUCKET/$HUB/jsonl/$LATEST_EXPORT"
echo "Found: $S3_JSONL_PATH"
echo ""

# List the JSONL files
echo "Listing JSONL files..."
JSONL_FILES=$(aws s3 ls "$S3_JSONL_PATH/" | grep -E '\.gz$|\.jsonl$' | awk '{print $4}')

if [[ -z "$JSONL_FILES" ]]; then
    echo "Error: No JSONL files found in $S3_JSONL_PATH" >&2
    exit 1
fi

FILE_COUNT=$(echo "$JSONL_FILES" | wc -l | tr -d ' ')

echo ""
echo "S3 JSONL Delete"
echo "==============="
echo "Bucket:     $S3_BUCKET"
echo "Hub:        $HUB"
echo "Export:     $LATEST_EXPORT"
echo "Path:       $S3_JSONL_PATH"
echo "Files:      $FILE_COUNT file(s)"
echo "IDs:        ${#IDS[@]} record(s) to delete"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would process the following files:"
    echo "$JSONL_FILES" | head -20
    [[ $FILE_COUNT -gt 20 ]] && echo "... and $((FILE_COUNT - 20)) more"
    echo ""
    echo "IDs to delete:"
    printf '%s\n' "${IDS[@]}" | head -20
    [[ ${#IDS[@]} -gt 20 ]] && echo "... and $((${#IDS[@]} - 20)) more"
    exit 0
fi

# Confirm before proceeding
if [[ "$SKIP_CONFIRM" != "true" ]]; then
    echo "This will modify $FILE_COUNT file(s) in S3. Continue? [y/N] "
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Create a Python filter script for efficient processing
FILTER_SCRIPT="$TEMP_DIR/filter_jsonl.py"
cat > "$FILTER_SCRIPT" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Filter JSONL records, removing those with IDs in the exclusion set."""
import sys
import json
import gzip
from pathlib import Path

def main():
    if len(sys.argv) != 4:
        print("Usage: filter_jsonl.py <input_file> <output_file> <ids_file>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    ids_path = sys.argv[3]

    # Load IDs to exclude
    with open(ids_path, 'r') as f:
        exclude_ids = set(line.strip() for line in f if line.strip())

    # Determine if files are gzipped
    is_gzipped = input_path.endswith('.gz')

    before_count = 0
    after_count = 0

    # Open input and output with appropriate compression
    open_read = gzip.open if is_gzipped else open
    open_write = gzip.open if is_gzipped else open

    with open_read(input_path, 'rt', encoding='utf-8') as infile, \
         open_write(output_path, 'wt', encoding='utf-8') as outfile:
        for line in infile:
            before_count += 1
            line = line.rstrip('\n')
            if not line:
                continue
            try:
                record = json.loads(line)
                record_id = record.get('_source', {}).get('id', '')
                if record_id not in exclude_ids:
                    outfile.write(line + '\n')
                    after_count += 1
            except json.JSONDecodeError:
                # Keep malformed lines
                outfile.write(line + '\n')
                after_count += 1

    removed = before_count - after_count
    print(json.dumps({
        'before': before_count,
        'after': after_count,
        'removed': removed
    }))

if __name__ == '__main__':
    main()
PYTHON_EOF

# Process each file
TOTAL_REMOVED=0
PROCESSED=0

echo "Processing files..."
echo ""

for file in $JSONL_FILES; do
    PROCESSED=$((PROCESSED + 1))
    echo "[$PROCESSED/$FILE_COUNT] Processing $file..."

    LOCAL_FILE="$TEMP_DIR/$file"
    FILTERED_FILE="$TEMP_DIR/filtered_$file"

    # Download the file
    aws s3 cp "$S3_JSONL_PATH/$file" "$LOCAL_FILE" --quiet

    # Filter the file using Python (much faster than line-by-line bash)
    RESULT=$(python3 "$FILTER_SCRIPT" "$LOCAL_FILE" "$FILTERED_FILE" "$ID_FILE")

    BEFORE_COUNT=$(echo "$RESULT" | python3 -c "import sys, json; print(json.load(sys.stdin)['before'])")
    AFTER_COUNT=$(echo "$RESULT" | python3 -c "import sys, json; print(json.load(sys.stdin)['after'])")
    REMOVED=$(echo "$RESULT" | python3 -c "import sys, json; print(json.load(sys.stdin)['removed'])")

    TOTAL_REMOVED=$((TOTAL_REMOVED + REMOVED))

    if [[ $REMOVED -gt 0 ]]; then
        echo "  Removed $REMOVED record(s) (was $BEFORE_COUNT, now $AFTER_COUNT)"
        # Upload the filtered file back to S3
        aws s3 cp "$FILTERED_FILE" "$S3_JSONL_PATH/$file" --quiet
        echo "  Uploaded to S3"
    else
        echo "  No matching records found"
    fi

    # Clean up local files to save space
    rm -f "$LOCAL_FILE" "$FILTERED_FILE"
done

echo ""
echo "========================================"
echo "Complete!"
echo "Total records removed: $TOTAL_REMOVED"
echo "Files processed: $PROCESSED"
