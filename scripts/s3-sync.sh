#!/usr/bin/env bash
# Sync hub data to S3 destination
# Usage: ./scripts/s3-sync.sh <hub-name> [subdir]
# Example: ./scripts/s3-sync.sh ohio enriched
# Example: ./scripts/s3-sync.sh ohio (syncs entire hub directory)

set -e

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <hub-name> [subdir]"
    echo "Example: $0 ohio enriched"
    echo "Example: $0 ohio (syncs entire hub directory)"
    exit 1
fi

HUB=$1
SUBDIR=${2:-""}
SOURCE_BASE="$DPLA_DATA"
DEST_BUCKET="s3://dpla-master-dataset"
S3_PREFIX=$(resolve_s3_prefix "$HUB")

# Construct source and destination paths
if [ -z "$SUBDIR" ]; then
    SOURCE_PATH="${SOURCE_BASE}/${HUB}/"
    DEST_PATH="${DEST_BUCKET}/${S3_PREFIX}/"
else
    SOURCE_PATH="${SOURCE_BASE}/${HUB}/${SUBDIR}/"
    DEST_PATH="${DEST_BUCKET}/${S3_PREFIX}/${SUBDIR}/"
fi

# No need to expand ~ since we use $DPLA_DATA

# When syncing the jsonl subdir, warn if the latest local snapshot has 0 records.
# This is a warning only — the hard block lives in ingest.sh. Direct callers of
# s3-sync.sh should ensure their data is valid before invoking this script.
if [[ "$SUBDIR" == "jsonl" ]]; then
    LATEST_LOCAL=$(ls -1d "${SOURCE_PATH}"*/ 2>/dev/null | sort | tail -1)
    if [[ -n "$LATEST_LOCAL" && -f "${LATEST_LOCAL}_MANIFEST" ]]; then
        SNAP_COUNT=$(read_manifest_count "${LATEST_LOCAL}_MANIFEST")
        if [[ "$SNAP_COUNT" -eq 0 ]]; then
            log_warn "Latest local JSONL snapshot has 0 records per _MANIFEST — verify data before syncing"
        fi
    fi
fi

echo "Syncing ${SOURCE_PATH} to ${DEST_PATH}"

# Run aws s3 sync, excluding OSX system files
aws s3 sync "${SOURCE_PATH}" "${DEST_PATH}" \
    --exclude ".DS_Store" \
    --exclude "._*" \
    --exclude ".AppleDouble" \
    --exclude ".LSOverride" \
    --exclude "Icon?" \
    --exclude ".Trashes" \
    --exclude ".fseventsd" \
    --exclude ".Spotlight-V100" \
    --exclude ".TemporaryItems"

echo "Sync complete!"
