#!/usr/bin/env bash
#
# community-webs-export.sh - Export Community Webs SQLite DB to JSONL and ZIP
#
# Internet Archive Community Webs sends a SQLite database. This script exports
# the DB to JSONL (one JSON object per line), validates against the expected
# schema, and zips the result for harvest.
#
# Prerequisites: sqlite3, jq
#   brew install sqlite3 jq   # macOS
#   apt install sqlite3 jq    # Ubuntu
#
# Usage:
#   ./scripts/community-webs-export.sh [--db=/path/to/file.db]
#
#   --db=/path/to/file.db  Explicit DB path (default: latest *.db in originalRecords/)
#   --skip-validate        Skip JSONL schema validation (not recommended)
#
# Output: $DPLA_DATA/community-webs/originalRecords/<YYYYMMDD>/community-webs-<timestamp>.zip
#
# The newest ZIP is also copied to the top of originalRecords/, replacing the
# previous one. The harvester reads only that level, so community-webs.harvest.endpoint
# stays fixed at originalRecords/ and a re-export can't double the records.
#
# See scripts/SCRIPTS.md for full documentation.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# Configuration (I3_HOME set by common.sh)
CW_ORIGINALS="${DPLA_DATA}/community-webs/originalRecords"
COMMON_DIR="${COMMON_DIR:-$(get_common_dir)}"

# Defaults
DB_PATH=""
SKIP_VALIDATE=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Export Community Webs SQLite DB to JSONL and ZIP for harvest."
    echo ""
    echo "Options:"
    echo "  --db=PATH        Explicit DB file path"
    echo "  --skip-validate  Skip JSONL schema validation (not recommended)"
    echo "  --help, -h       Show this help"
    echo ""
    echo "Default: auto-detect latest *.db in $CW_ORIGINALS"
    echo "Output:  \$DPLA_DATA/community-webs/originalRecords/<YYYYMMDD>/community-webs-<timestamp>.zip"
    echo ""
    echo "Example:"
    echo "  $0"
    echo "  $0 --db=$CW_ORIGINALS/20260204_dpla_export.db"
    exit 1
}

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --db=*)
            DB_PATH="${arg#*=}"
            ;;
        --skip-validate)
            SKIP_VALIDATE=true
            ;;
        --help|-h)
            usage
            ;;
        *)
            log_error "Unknown option: $arg"
            usage
            ;;
    esac
done

# Find latest *.db if not specified (portable: ls -t works on macOS and Linux)
find_latest_db() {
    local dir="$1"
    if [[ ! -d "$dir" ]]; then
        return 1
    fi
    local latest
    latest=$(ls -t "$dir"/*.db 2>/dev/null | head -1) || true
    [[ -n "$latest" ]] && echo "$latest"
}

# Validate DB has ait table
validate_db() {
    local db="$1"
    if [[ ! -f "$db" ]]; then
        log_error "DB file not found: $db"
        return 1
    fi
    local tables
    tables=$(sqlite3 "$db" "SELECT name FROM sqlite_master WHERE type='table' AND name='ait';" 2>/dev/null || true)
    if [[ -z "$tables" ]]; then
        log_error "DB does not have 'ait' table: $db"
        return 1
    fi
    return 0
}

# Main
main() {
    require_command sqlite3 "Install sqlite3: brew install sqlite3"
    require_command jq "Install jq: brew install jq"

    if [[ -z "$DB_PATH" ]]; then
        DB_PATH=$(find_latest_db "$CW_ORIGINALS")
        if [[ -z "$DB_PATH" ]]; then
            log_error "No *.db file found in $CW_ORIGINALS"
            log_info "Place the Community Webs export DB there or use --db=/path/to/file.db"
            exit 1
        fi
        log_info "Using DB: $DB_PATH"
    fi

    validate_db "$DB_PATH" || exit 1

    DATESTAMP=$(date +%Y%m%d)
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    OUT_DIR="$CW_ORIGINALS/$DATESTAMP"
    mkdir -p "$OUT_DIR"

    TMP_JSON=$(mktemp)
    TMP_JSONL=$(mktemp)
    trap "rm -f '$TMP_JSON' '$TMP_JSONL'" EXIT

    print_step "Exporting DB to JSON..."
    sqlite3 "$DB_PATH" --json "SELECT * FROM ait" > "$TMP_JSON" || die "sqlite3 export failed"

    RECORD_COUNT=$(jq 'length' "$TMP_JSON" 2>/dev/null || echo "0")
    if [[ "$RECORD_COUNT" -eq 0 ]]; then
        log_error "No records in ait table"
        exit 1
    fi
    log_info "Exported $RECORD_COUNT records"

    print_step "Converting to JSONL..."
    jq -c '.[]' "$TMP_JSON" > "$TMP_JSONL" || die "jq conversion failed"

    if [[ "$SKIP_VALIDATE" != "true" ]]; then
        print_step "Validating JSONL schema..."
        VALIDATE_SCRIPT="$SCRIPT_DIR/community-webs-validate-jsonl.py"
        if [[ -f "$VALIDATE_SCRIPT" ]]; then
            PYTHON="${I3_HOME}/venv/bin/python"
            [[ -x "$PYTHON" ]] || PYTHON="python3"
            "$PYTHON" "$VALIDATE_SCRIPT" "$TMP_JSONL" || die "JSONL validation failed"
            log_info "Validation passed"
        else
            log_warn "Validation script not found: $VALIDATE_SCRIPT (skipping validation)"
        fi
    fi

    ZIP_NAME="community-webs-$TIMESTAMP.zip"
    ZIP_PATH="$OUT_DIR/$ZIP_NAME"
    JSONL_FINAL="$OUT_DIR/community-webs.jsonl"
    cp "$TMP_JSONL" "$JSONL_FINAL"
    print_step "Creating ZIP..."
    (cd "$OUT_DIR" && zip -j "$ZIP_PATH" "community-webs.jsonl") || die "zip failed"
    rm -f "$JSONL_FINAL"

    log_success "Export complete: $ZIP_PATH"

    # Publish for harvest: the temp name doesn't end in .zip, so the harvester
    # never sees a partial copy.
    cp "$ZIP_PATH" "$CW_ORIGINALS/.$ZIP_NAME.tmp"
    find "$CW_ORIGINALS" -maxdepth 1 -type f -name "*.zip" -exec rm -f {} +
    mv "$CW_ORIGINALS/.$ZIP_NAME.tmp" "$CW_ORIGINALS/$ZIP_NAME"
    log_info "Ready for harvest: $CW_ORIGINALS/$ZIP_NAME"

    echo ""
    echo "Next: ./scripts/harvest.sh community-webs"
    echo "Or:   ./scripts/harvest/community-webs-ingest.sh --skip-export"
}

main "$@"
