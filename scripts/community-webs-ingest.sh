#!/usr/bin/env bash
#
# community-webs-ingest.sh - Community Webs ingest: export DB to ZIP, harvest, optionally full pipeline
#
# Automates the Community Webs workflow:
#   1. Export SQLite DB to JSONL and ZIP (or --skip-export to use existing)
#   2. Update i3.conf endpoint if needed
#   3. Harvest community-webs
#   4. (Optional) Run full pipeline: mapping + enrichment + jsonl
#
# Usage:
#   ./scripts/community-webs-ingest.sh [options]
#
# Options:
#   --db=PATH       Explicit DB path (passed to community-webs-export.sh)
#   --skip-export   Use existing ZIP; endpoint must already point to it
#   --full          Run full pipeline (harvest + map + enrich + jsonl)
#   --update-conf   Update i3.conf with export output directory
#   --help, -h      Show help
#
# Example:
#   ./scripts/community-webs-ingest.sh
#   ./scripts/community-webs-ingest.sh --full --update-conf
#   ./scripts/community-webs-ingest.sh --skip-export  # Use existing ZIP
#
# See scripts/SCRIPTS.md for full documentation.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Defaults
SKIP_EXPORT=false
FULL_PIPELINE=false
UPDATE_CONF=false
DB_PATH=""

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Community Webs ingest: export DB -> harvest -> (optional) full pipeline"
    echo ""
    echo "Options:"
    echo "  --db=PATH       Explicit DB path for export"
    echo "  --skip-export   Use existing ZIP; i3.conf endpoint must point to it"
    echo "  --full          Run full pipeline (harvest + map + enrich + jsonl)"
    echo "  --update-conf   Update i3.conf with export output directory"
    echo "  --help, -h      Show this help"
    echo ""
    echo "Example:"
    echo "  $0                    # Export + harvest only"
    echo "  $0 --full             # Export + harvest + map + enrich + jsonl"
    echo "  $0 --skip-export      # Harvest only (existing ZIP)"
    exit 1
}

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --db=*)
            DB_PATH="${arg#*=}"
            ;;
        --skip-export)
            SKIP_EXPORT=true
            ;;
        --full)
            FULL_PIPELINE=true
            ;;
        --update-conf)
            UPDATE_CONF=true
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

main() {
    setup_java "4g" || die "Failed to setup Java"

    if [[ "$SKIP_EXPORT" != "true" ]]; then
        print_step "Exporting Community Webs DB to JSONL and ZIP..."
        local export_args=()
        [[ -n "$DB_PATH" ]] && export_args+=(--db="$DB_PATH")
        [[ "$UPDATE_CONF" == "true" ]] && export_args+=(--update-conf)
        "$SCRIPT_DIR/community-webs-export.sh" "${export_args[@]}" || die "Export failed"
    else
        log_info "Skipping export (using existing ZIP)"
        if [[ -z "${I3_CONF:-}" ]] || [[ ! -f "$I3_CONF" ]]; then
            log_error "I3_CONF not set or file not found; harvest endpoint unknown"
            exit 1
        fi
    fi

    print_step "Harvesting community-webs..."
    "$SCRIPT_DIR/harvest.sh" community-webs || die "Harvest failed"

    if [[ "$FULL_PIPELINE" == "true" ]]; then
        print_step "Running full pipeline (mapping + enrichment + jsonl)..."
        "$SCRIPT_DIR/ingest.sh" community-webs --skip-harvest || die "Pipeline failed"
        log_success "Community Webs ingest complete"
        echo ""
        echo "Output: $DPLA_DATA/community-webs/"
        echo "  mapping/    $DPLA_DATA/community-webs/mapping/"
        echo "  enrichment: $DPLA_DATA/community-webs/enrichment/"
        echo "  jsonl:      $DPLA_DATA/community-webs/jsonl/"
    else
        log_success "Community Webs harvest complete"
        echo ""
        echo "Next: ./scripts/ingest.sh community-webs --skip-harvest  (mapping + enrichment + jsonl)"
    fi
}

main "$@"
