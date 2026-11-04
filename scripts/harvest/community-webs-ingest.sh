#!/usr/bin/env bash
#
# community-webs-ingest.sh - Community Webs ingest, paused for review before sending
#
#   1. Export the SQLite DB to JSONL and ZIP (or --skip-export to reuse the current ZIP)
#   2. Harvest, then mapping + enrichment + jsonl + safety check
#   3. Stop and post the counts to Slack
#   4. After review, --send syncs the jsonl to S3 and emails the hub contacts
#
# Usage:
#   ./scripts/harvest/community-webs-ingest.sh --db=/path/to/file.db
#   ./scripts/harvest/community-webs-ingest.sh --skip-export
#   ./scripts/harvest/community-webs-ingest.sh --send
#   ./scripts/harvest/community-webs-ingest.sh --send --force
#
# Options:
#   --db=PATH       Explicit DB path (passed to community-webs-export.sh)
#   --skip-export   Reuse the current ZIP in originalRecords/
#   --send          Sync the reviewed snapshot to S3 and email the hub contacts
#   --force         With --send: send the newest snapshot even though the run
#                   did not finish cleanly (e.g. the >5% safety check blocked it)
#   --help, -h      Show help
#
# See scripts/SCRIPTS.md for full documentation.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

HUB="community-webs"
CW_ORIGINALS="${DPLA_DATA}/${HUB}/originalRecords"
READY_FILE="${DPLA_DATA}/${HUB}/.ready-for-review"

# Defaults
SKIP_EXPORT=false
SEND=false
FORCE=false
DB_PATH=""
STAGE="startup"
ALERT_ON_EXIT=true

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Community Webs ingest: export -> harvest -> map -> enrich -> jsonl, then pause for review"
    echo ""
    echo "Options:"
    echo "  --db=PATH       Explicit DB path for export"
    echo "  --skip-export   Reuse the current ZIP in originalRecords/"
    echo "  --send          Sync the reviewed snapshot to S3 and email the hub contacts"
    echo "  --force         With --send: send the newest snapshot even if the run did not finish cleanly"
    echo "  --help, -h      Show this help"
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
        --send)
            SEND=true
            ;;
        --force)
            FORCE=true
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

if [[ "$FORCE" == "true" && "$SEND" != "true" ]]; then
    log_error "--force only applies with --send"
    usage
fi

trap 'err=$?
      if [[ $err -ne 0 && "$ALERT_ON_EXIT" == "true" ]]; then
          slack_notify ":x: *$HUB ingest FAILED* during $STAGE (exit $err)" || true
      fi' EXIT

# The harvester lists ZIPs directly under the endpoint, so anything else in i3.conf
# silently harvests the wrong delivery or 0 records.
check_endpoint() {
    local endpoint
    endpoint=$(grep "^${HUB}\.harvest\.endpoint" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || true)
    [[ -d "$endpoint" ]] || die "$HUB.harvest.endpoint is not a directory: '$endpoint'"
    [[ "$(cd "$endpoint" && pwd -P)" == "$(cd "$CW_ORIGINALS" && pwd -P)" ]] \
        || die "$HUB.harvest.endpoint must be $CW_ORIGINALS/ (found: $endpoint)"
}

run_ingest() {
    setup_java "4g" || die "Failed to setup Java"
    rm -f "$READY_FILE"
    mkdir -p "$CW_ORIGINALS"
    check_endpoint

    if [[ "$SKIP_EXPORT" != "true" ]]; then
        STAGE="export"
        slack_notify ":arrow_forward: *$HUB export started*" || true
        print_step "Exporting Community Webs DB to JSONL and ZIP..."
        local export_args=()
        [[ -n "$DB_PATH" ]] && export_args+=(--db="$DB_PATH")
        "$SCRIPT_DIR/community-webs-export.sh" ${export_args[@]+"${export_args[@]}"} || die "Export failed"
    else
        log_info "Skipping export (using existing ZIP)"
    fi

    local zip_path zip_count exported
    zip_count=$(find "$CW_ORIGINALS" -maxdepth 1 -type f -name "*.zip" | wc -l | tr -d ' ')
    [[ "$zip_count" -eq 1 ]] || die "Expected 1 ZIP directly in $CW_ORIGINALS, found $zip_count"
    zip_path=$(find "$CW_ORIGINALS" -maxdepth 1 -type f -name "*.zip")
    exported=$(unzip -p "$zip_path" | wc -l | tr -d ' ')
    slack_notify ":white_check_mark: *$HUB export ready* ($exported records in $(basename "$zip_path")) — starting harvest" || true

    STAGE="harvest"
    print_step "Harvesting community-webs..."
    "$SCRIPTS_ROOT/harvest.sh" "$HUB" || die "Harvest failed"

    local harvest_dir harvest_count
    harvest_dir=$(find_latest_data "$HUB" harvest) || die "No harvest output found"
    harvest_count=$(read_manifest_count "$harvest_dir/_MANIFEST")
    if [[ "$harvest_count" -eq 0 ]]; then
        die "Harvest produced 0 records from $zip_path"
    fi
    slack_notify ":white_check_mark: *$HUB harvest complete* ($harvest_count records)" || true

    # ingest.sh posts its own stage and failure alerts from here on
    STAGE="pipeline"
    ALERT_ON_EXIT=false
    print_step "Running pipeline (mapping + enrichment + jsonl + safety check)..."
    "$SCRIPTS_ROOT/ingest.sh" "$HUB" --skip-harvest --skip-s3-sync --skip-email || die "Pipeline failed"
    ALERT_ON_EXIT=true

    STAGE="review handoff"
    local jsonl_dir map_dir jsonl_count
    jsonl_dir=$(find_latest_data "$HUB" jsonl) || die "No jsonl output found"
    map_dir=$(find_latest_data "$HUB" mapping) || die "No mapping output found"
    jsonl_count=$(read_manifest_count "$jsonl_dir/_MANIFEST")
    printf '%s\n%s\n' "$jsonl_dir" "$map_dir" > "$READY_FILE"

    slack_notify ":pause_button: *$HUB ready for review* — nothing sent yet\nExported: $exported | Harvested: $harvest_count | JSON-L: $jsonl_count\nSnapshot: \`$(basename "$jsonl_dir")\`\nAnswer the prompt in your terminal, or run \`launch_cw.py --send\`" || true
    log_success "Community Webs ingest complete — ready for review"
    echo ""
    echo "Snapshot: $jsonl_dir ($jsonl_count records)"
    echo "Nothing has been sent. To sync to S3 and email the hub: launch_cw.py --send"
}

run_send() {
    STAGE="send"
    local jsonl_dir map_dir
    if [[ -f "$READY_FILE" ]]; then
        jsonl_dir=$(sed -n 1p "$READY_FILE")
        map_dir=$(sed -n 2p "$READY_FILE")
    elif [[ "$FORCE" == "true" ]]; then
        jsonl_dir=$(find_latest_data "$HUB" jsonl) || die "No jsonl output found"
        map_dir=$(find_latest_data "$HUB" mapping) || die "No mapping output found"
        log_warn "No clean run waiting for review; --force is sending the newest snapshot"
    else
        die "No run is waiting for review. If the safety check blocked it and you have reviewed the drop, use --send --force."
    fi
    [[ -f "$jsonl_dir/_SUCCESS" ]] || die "Snapshot is incomplete (no _SUCCESS): $jsonl_dir"

    local jsonl_ts jsonl_count
    jsonl_ts=$(basename "$jsonl_dir")
    jsonl_count=$(read_manifest_count "$jsonl_dir/_MANIFEST")
    [[ "$jsonl_count" -gt 0 ]] || die "Snapshot has 0 records: $jsonl_dir"

    STAGE="S3 sync"
    print_step "Syncing $jsonl_ts to S3..."
    "$SCRIPTS_ROOT/s3-sync.sh" "$HUB" jsonl
    slack_notify ":white_check_mark: *$HUB synced to S3* ($jsonl_count records)\nSnapshot: \`$jsonl_ts\`" || true

    # The sync is safe to repeat, so a failed email can be retried with --send
    STAGE="partner email (S3 sync already done; rerun --send to retry)"
    print_step "Emailing hub contacts..."
    "$SCRIPTS_ROOT/communication/send-ingest-email.sh" --yes "$HUB" "$map_dir"
    rm -f "$READY_FILE"
    slack_notify ":email: *$HUB partner email sent*" || true
    log_success "Community Webs sent: $jsonl_ts ($jsonl_count records)"
}

if [[ "$SEND" == "true" ]]; then
    run_send
else
    run_ingest
fi
