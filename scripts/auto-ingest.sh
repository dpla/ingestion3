#!/usr/bin/env bash
# auto-ingest.sh - Fully automated DPLA hub ingestion pipeline
#
# This script automates the entire ingestion process:
#   1. Determines hubs scheduled for the current month
#   2. For file hubs: downloads latest data from S3, preprocesses if needed
#   3. Runs full pipeline: harvest → remap (mapping+enrichment+jsonl)
#   4. Syncs results to S3 destination
#   5. Sends email notifications with mapping summaries
#
# Key behaviors learned from production runs:
#   - Processes hubs sequentially to avoid sbt conflicts
#   - Exit code 1 doesn't always mean failure - verifies actual output
#   - Smithsonian requires xmll preprocessing and endpoint update
#   - Retries failed harvests up to MAX_RETRIES times
#   - Tracks timeout failures separately
#
# Usage:
#   ./scripts/auto-ingest.sh                    # Process all scheduled hubs
#   ./scripts/auto-ingest.sh --hub=maryland     # Process single hub
#   ./scripts/auto-ingest.sh --month=3          # Process March hubs
#   ./scripts/auto-ingest.sh --dry-run          # Show what would be processed
#   ./scripts/auto-ingest.sh --no-email         # Skip email notifications
#   ./scripts/auto-ingest.sh --no-s3-sync       # Skip S3 sync
#   ./scripts/auto-ingest.sh --skip-harvest     # Skip harvest, run remap only

set -uo pipefail  # Don't use -e, we handle errors manually

# =============================================================================
# Configuration
# =============================================================================

# Source common configuration (but don't auto-init since we manage paths here)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_NO_INIT=1 source "$SCRIPT_DIR/common.sh"

# Now set up our paths
I3_HOME="${I3_HOME:-$(dirname "$SCRIPT_DIR")}"
I3_CONF="${I3_CONF:-$HOME/dpla/code/ingestion3-conf/i3.conf}"
DPLA_DATA="${DPLA_DATA:-$HOME/dpla/data}"
AWS_PROFILE="${AWS_PROFILE:-dpla}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

# Setup Java (8g for general ingestion)
setup_java "8g" || { echo "Failed to setup Java environment"; exit 1; }

# Retry configuration
MAX_RETRIES=5
RETRY_DELAY=30  # seconds between retries

# xmll jar location
XMLL_JAR="${XMLL_JAR:-$HOME/xmll-assembly-0.1.jar}"

# Logging
LOG_DIR="${I3_HOME}/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/auto-ingest-${TIMESTAMP}.log"
STATUS_FILE="${LOG_DIR}/auto-ingest-${TIMESTAMP}.status"

# Setup colors from common.sh
setup_colors

# =============================================================================
# Logging Functions
# =============================================================================

log() {
    local level="$1"
    shift
    local msg="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $msg" | tee -a "$LOG_FILE"
}

log_info() { log "INFO" "$@"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$LOG_FILE"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $*" | tee -a "$LOG_FILE"; }
log_debug() { [[ "${DEBUG:-false}" == "true" ]] && log "DEBUG" "$@"; }

print_step() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}" | tee -a "$LOG_FILE"
}

print_status() {
    echo -e "${CYAN}[STATUS]${NC} $*" | tee -a "$LOG_FILE"
}

# =============================================================================
# Status Tracking
# =============================================================================

declare -A HUB_STATUS
declare -A HUB_RETRIES
declare -A HUB_ERRORS

update_status() {
    local hub="$1"
    local status="$2"
    local detail="${3:-}"
    HUB_STATUS[$hub]="$status"
    echo "$hub=$status${detail:+ ($detail)}" >> "$STATUS_FILE"
    print_status "$hub: $status${detail:+ - $detail}"
}

print_all_status() {
    echo ""
    echo "=============================================="
    echo "  Current Hub Status"
    echo "=============================================="
    for hub in "${!HUB_STATUS[@]}"; do
        local status="${HUB_STATUS[$hub]}"
        local color="$NC"
        case "$status" in
            "complete") color="$GREEN" ;;
            "failed"|"timeout") color="$RED" ;;
            "running"*) color="$CYAN" ;;
            "pending") color="$YELLOW" ;;
        esac
        echo -e "  ${color}${hub}${NC}: $status"
    done
    echo "=============================================="
    echo ""
}

# =============================================================================
# Hub Configuration
# =============================================================================

# Map hub names to S3 source buckets for file-based harvests
get_s3_source_bucket() {
    local hub="$1"
    case "$hub" in
        florida) echo "dpla-hub-fl" ;;
        smithsonian) echo "dpla-hub-si" ;;
        vt) echo "dpla-hub-vt" ;;
        ct) echo "dpla-hub-ct" ;;
        georgia) echo "dpla-hub-ga" ;;
        heartland) echo "dpla-hub-mo" ;;
        ohio) echo "dpla-hub-ohio" ;;
        txdl) echo "dpla-hub-tdl" ;;
        northwest-heritage) echo "dpla-hub-northwest-heritage" ;;
        nara) echo "dpla-hub-nara" ;;
        *) echo "" ;;  # No S3 source bucket for OAI/API hubs
    esac
}

# Get harvest type from i3.conf
get_harvest_type() {
    local hub="$1"
    grep "^${hub}\.harvest\.type" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo "oai"
}

# Get hub email contacts from i3.conf
get_hub_email() {
    local hub="$1"
    grep "^${hub}\.email" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo ""
}

# Get hub provider name from i3.conf
get_provider_name() {
    local hub="$1"
    grep "^${hub}\.provider" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo "$hub"
}

# Get S3 destination from i3.conf
get_s3_destination() {
    local hub="$1"
    grep "^${hub}\.s3_destination" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo ""
}

# Get harvest endpoint from i3.conf
get_harvest_endpoint() {
    local hub="$1"
    grep "^${hub}\.harvest\.endpoint" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo ""
}

# Get scheduled months from i3.conf
get_scheduled_months() {
    local hub="$1"
    grep "^${hub}\.schedule\.months" "$I3_CONF" 2>/dev/null | \
        sed 's/.*\[//' | sed 's/\].*//' | tr ',' ' ' || echo ""
}

# Get all hubs with schedule (not as-needed, not on-hold)
get_scheduled_hubs() {
    grep '\.schedule\.frequency' "$I3_CONF" | \
        grep -v '^#' | \
        grep -v 'as-needed' | \
        grep -v 'on-hold' | \
        sed 's/\.schedule\.frequency.*//' | \
        sort -u
}

# Check if hub is scheduled for a month
is_scheduled_for_month() {
    local hub="$1"
    local target_month="$2"
    local months=$(get_scheduled_months "$hub")
    for m in $months; do
        if [[ "$m" == "$target_month" ]]; then
            return 0
        fi
    done
    return 1
}

# =============================================================================
# Output Verification Functions
# =============================================================================

# Check if harvest produced output (more reliable than exit code)
verify_harvest_output() {
    local hub="$1"
    local harvest_dir="${DPLA_DATA}/${hub}/harvest"

    if [[ ! -d "$harvest_dir" ]]; then
        log_debug "No harvest directory for $hub"
        return 1
    fi

    # Check for any .avro directories
    local avro_dirs=$(find "$harvest_dir" -maxdepth 1 -type d -name "*.avro" 2>/dev/null | wc -l)
    if [[ "$avro_dirs" -gt 0 ]]; then
        log_debug "Found $avro_dirs harvest output directories for $hub"
        return 0
    fi

    return 1
}

# Check if remap produced final jsonl output
verify_remap_output() {
    local hub="$1"
    local jsonl_dir="${DPLA_DATA}/${hub}/jsonl"

    if [[ ! -d "$jsonl_dir" ]]; then
        return 1
    fi

    local jsonl_dirs=$(find "$jsonl_dir" -maxdepth 1 -type d -name "*.jsonl" 2>/dev/null | wc -l)
    if [[ "$jsonl_dirs" -gt 0 ]]; then
        return 0
    fi

    return 1
}

# Get record count from harvest summary if available
get_harvest_record_count() {
    local hub="$1"
    local harvest_dir="${DPLA_DATA}/${hub}/harvest"
    local latest=$(ls -td "${harvest_dir}"/*/ 2>/dev/null | head -1)

    if [[ -n "$latest" && -f "${latest}/_SUCCESS" ]]; then
        # Try to count records from the parquet files
        echo "completed"
    else
        echo "unknown"
    fi
}

# =============================================================================
# S3 Data Download Functions
# =============================================================================

download_s3_data() {
    local hub="$1"
    local source_bucket=$(get_s3_source_bucket "$hub")

    if [[ -z "$source_bucket" ]]; then
        log_info "Hub $hub uses OAI/API harvest, no S3 download needed"
        return 0
    fi

    log_info "Downloading data for $hub from s3://$source_bucket/"

    local local_dir="${DPLA_DATA}/${hub}/originalRecords"
    mkdir -p "$local_dir"

    # List bucket contents and find most recent data
    local bucket_contents=$(aws s3 ls "s3://${source_bucket}/" --profile "$AWS_PROFILE" 2>/dev/null)

    if [[ -z "$bucket_contents" ]]; then
        log_error "No data found in s3://$source_bucket/ or bucket access denied"
        return 1
    fi

    # Check for date-formatted folders first (most common pattern)
    local latest_folder=$(echo "$bucket_contents" | grep "PRE" | awk '{print $2}' | tr -d '/' | sort -r | head -1)

    if [[ -n "$latest_folder" ]]; then
        local download_dir="${local_dir}/${latest_folder}"
        mkdir -p "$download_dir"
        log_info "Downloading folder: $latest_folder"
        aws s3 sync "s3://${source_bucket}/${latest_folder}/" "$download_dir/" --profile "$AWS_PROFILE"
        echo "$download_dir" > "${local_dir}/.latest_endpoint"
    else
        # No folders, look for files
        local latest_file=$(echo "$bucket_contents" | grep -E "\.json|\.xml|\.zip|\.jsonl" | sort -k1,2 | tail -1 | awk '{print $4}')

        if [[ -z "$latest_file" ]]; then
            log_error "No recognizable data files found in s3://$source_bucket/"
            return 1
        fi

        # Extract date from filename or use today
        local date_folder=$(echo "$latest_file" | grep -oE '[0-9]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1)
        [[ -z "$date_folder" ]] && date_folder=$(date +%Y%m%d)

        local download_dir="${local_dir}/${date_folder}"
        mkdir -p "$download_dir"
        log_info "Downloading file: $latest_file to $download_dir"
        aws s3 cp "s3://${source_bucket}/${latest_file}" "$download_dir/" --profile "$AWS_PROFILE"

        # Zip JSONL files if needed (file harvester expects .zip)
        if [[ "$latest_file" == *.jsonl ]]; then
            log_info "Zipping JSONL file for harvester..."
            (cd "$download_dir" && zip "${latest_file}.zip" "$latest_file")
        fi

        echo "$download_dir" > "${local_dir}/.latest_endpoint"
    fi

    log_success "Downloaded to: $(cat ${local_dir}/.latest_endpoint)"
    return 0
}

# =============================================================================
# Smithsonian Preprocessing
# =============================================================================

preprocess_smithsonian() {
    local data_dir="$1"

    if [[ ! -f "$XMLL_JAR" ]]; then
        log_error "xmll jar not found at $XMLL_JAR"
        log_info "Clone https://github.com/dpla/xmll and run 'sbt assembly'"
        return 1
    fi

    log_info "Preprocessing Smithsonian files with xmll..."
    log_info "Data directory: $data_dir"

    mkdir -p "${data_dir}/fixed" "${data_dir}/xmll"

    # Step 1: Recompress gzip files (SI exports have gzip issues)
    log_info "Step 1: Recompressing gzip files..."
    local recompress_count=0
    for f in "${data_dir}"/*.xml.gz; do
        if [[ -f "$f" ]]; then
            local basename=$(basename "$f")
            if [[ ! -f "${data_dir}/fixed/$basename" ]]; then
                log_debug "Recompressing: $basename"
                gunzip -dck "$f" | gzip > "${data_dir}/fixed/$basename" 2>/dev/null || {
                    log_warn "Failed to recompress $basename, copying original"
                    cp "$f" "${data_dir}/fixed/$basename"
                }
                ((recompress_count++))
            fi
        fi
    done
    log_info "Recompressed $recompress_count files"

    # Step 2: Run xmll to shred large XML files into line-delimited format
    log_info "Step 2: Running xmll shredder..."
    local xmll_count=0
    for f in "${data_dir}/fixed"/*.xml.gz; do
        if [[ -f "$f" ]]; then
            local basename=$(basename "$f")
            if [[ ! -f "${data_dir}/xmll/$basename" ]]; then
                log_info "Processing with xmll: $basename"
                java -Xmx4g -jar "$XMLL_JAR" doc "$f" "${data_dir}/xmll/${basename}" 2>&1 | tee -a "$LOG_FILE" || {
                    log_warn "xmll failed for $basename, will try with original"
                }
                ((xmll_count++))
            fi
        fi
    done
    log_info "Processed $xmll_count files with xmll"

    # Step 3: Update i3.conf to point to xmll directory
    local xmll_dir="${data_dir}/xmll/"
    local current_endpoint=$(get_harvest_endpoint "smithsonian")

    if [[ "$current_endpoint" != "$xmll_dir" ]]; then
        log_info "Updating smithsonian.harvest.endpoint to: $xmll_dir"
        # Use sed to update the endpoint in i3.conf
        if [[ -n "$current_endpoint" ]]; then
            sed_i "s|smithsonian.harvest.endpoint = \".*\"|smithsonian.harvest.endpoint = \"${xmll_dir}\"|" "$I3_CONF"
        else
            echo "smithsonian.harvest.endpoint = \"${xmll_dir}\"" >> "$I3_CONF"
        fi
    fi

    log_success "Smithsonian preprocessing complete"
    return 0
}

# =============================================================================
# Pipeline Execution Functions
# =============================================================================

run_harvest_with_retry() {
    local hub="$1"
    local max_retries="${2:-$MAX_RETRIES}"
    local attempt=1

    while [[ $attempt -le $max_retries ]]; do
        log_info "Harvest attempt $attempt/$max_retries for $hub"
        update_status "$hub" "harvesting" "attempt $attempt"

        cd "$I3_HOME"

        # Run harvest and capture output
        local harvest_log="${LOG_DIR}/${hub}-harvest-${attempt}.log"
        ./scripts/harvest.sh "$hub" > "$harvest_log" 2>&1
        local exit_code=$?

        # Append to main log
        cat "$harvest_log" >> "$LOG_FILE"

        # Check for actual output (exit code alone is unreliable)
        if verify_harvest_output "$hub"; then
            log_success "Harvest completed for $hub (verified output exists)"
            return 0
        fi

        # Check for specific error patterns
        if grep -q "OutOfMemoryError" "$harvest_log" 2>/dev/null; then
            log_error "OutOfMemoryError for $hub - may need preprocessing"
            HUB_ERRORS[$hub]="OutOfMemoryError"
            return 1
        fi

        if grep -q "Connection.*timed out\|SocketTimeoutException\|Read timed out" "$harvest_log" 2>/dev/null; then
            log_warn "Timeout detected for $hub (attempt $attempt)"
            HUB_RETRIES[$hub]=$attempt
            ((attempt++))
            if [[ $attempt -le $max_retries ]]; then
                log_info "Waiting ${RETRY_DELAY}s before retry..."
                sleep "$RETRY_DELAY"
            fi
            continue
        fi

        # Exit code 0 but no output usually means OAI feed issue
        if [[ $exit_code -eq 0 ]]; then
            log_warn "Harvest exited OK but no output for $hub - possible OAI feed issue"
            HUB_ERRORS[$hub]="no_output"
        fi

        ((attempt++))
        if [[ $attempt -le $max_retries ]]; then
            sleep "$RETRY_DELAY"
        fi
    done

    log_error "Harvest failed after $max_retries attempts for $hub"
    return 1
}

run_remap() {
    local hub="$1"

    log_info "Running remap (mapping → enrichment → jsonl) for $hub"
    update_status "$hub" "remapping"

    cd "$I3_HOME"

    local remap_log="${LOG_DIR}/${hub}-remap.log"
    ./scripts/remap.sh "$hub" > "$remap_log" 2>&1
    local exit_code=$?

    cat "$remap_log" >> "$LOG_FILE"

    # Verify output exists (exit code 1 often still produces valid output)
    if verify_remap_output "$hub"; then
        log_success "Remap completed for $hub (verified jsonl output exists)"
        return 0
    fi

    # Check for specific errors
    if grep -q "NullPointerException.*InputHelper" "$remap_log" 2>/dev/null; then
        log_error "No harvest data found for $hub - run harvest first"
        HUB_ERRORS[$hub]="no_harvest_data"
        return 1
    fi

    if [[ $exit_code -ne 0 ]]; then
        log_error "Remap failed for $hub (exit code: $exit_code)"
        return 1
    fi

    return 1
}

sync_to_s3() {
    local hub="$1"

    log_info "Syncing $hub data to S3..."
    update_status "$hub" "syncing"

    cd "$I3_HOME"

    if ./scripts/s3-sync.sh "$hub" >> "$LOG_FILE" 2>&1; then
        log_success "S3 sync completed for $hub"
        return 0
    else
        log_warn "S3 sync failed for $hub (non-fatal)"
        return 1
    fi
}

# =============================================================================
# Email Functions
# =============================================================================

send_email_notification() {
    local hub="$1"
    local mapping_dir="${DPLA_DATA}/${hub}/mapping"
    local email=$(get_hub_email "$hub")

    if [[ -z "$email" ]]; then
        log_info "No email configured for $hub, skipping notification"
        return 0
    fi

    # Find the most recent mapping directory
    local latest_mapping=$(ls -td "${mapping_dir}"/*/ 2>/dev/null | head -1)

    if [[ -z "$latest_mapping" ]]; then
        log_warn "No mapping output found for $hub, skipping email"
        return 1
    fi

    local summary_file="${latest_mapping}/_SUMMARY"

    if [[ ! -f "$summary_file" ]]; then
        log_warn "Summary file not found: $summary_file"
        return 1
    fi

    log_info "Sending email notification for $hub to $email"

    # For now, just log that we would send email
    # The actual Emailer requires AWS SES setup
    log_info "Email would be sent with summary from: $summary_file"

    return 0
}

# =============================================================================
# Full Pipeline for Single Hub
# =============================================================================

process_hub() {
    local hub="$1"
    local skip_email="${2:-false}"
    local skip_s3="${3:-false}"
    local skip_harvest="${4:-false}"
    local start_time=$(date +%s)

    local provider_name=$(get_provider_name "$hub")
    local harvest_type=$(get_harvest_type "$hub")

    echo ""
    echo "=============================================="
    echo "  Processing: $provider_name ($hub)"
    echo "  Harvest Type: $harvest_type"
    echo "  Started: $(date)"
    echo "=============================================="

    update_status "$hub" "starting"

    # Step 1: Download S3 data if file harvest
    if [[ "$harvest_type" == "file" && "$skip_harvest" != "true" ]]; then
        print_step "Step 1/4: Downloading S3 data..."
        if ! download_s3_data "$hub"; then
            log_error "Failed to download S3 data for $hub"
            update_status "$hub" "failed" "S3 download failed"
            return 1
        fi

        # Special preprocessing for Smithsonian
        if [[ "$hub" == "smithsonian" ]]; then
            local data_dir="${DPLA_DATA}/${hub}/originalRecords"
            local latest_dir=$(cat "${data_dir}/.latest_endpoint" 2>/dev/null)
            if [[ -n "$latest_dir" ]]; then
                if ! preprocess_smithsonian "$latest_dir"; then
                    log_error "Smithsonian preprocessing failed"
                    update_status "$hub" "failed" "preprocessing failed"
                    return 1
                fi
            fi
        fi
    else
        print_step "Step 1/4: S3 download skipped (OAI/API harvest or --skip-harvest)"
    fi

    # Step 2: Harvest
    if [[ "$skip_harvest" != "true" ]]; then
        print_step "Step 2/4: Running harvest..."
        if ! run_harvest_with_retry "$hub"; then
            log_error "Harvest failed for $hub"
            update_status "$hub" "failed" "harvest failed"
            return 1
        fi
    else
        print_step "Step 2/4: Harvest skipped (--skip-harvest)"
        if ! verify_harvest_output "$hub"; then
            log_error "No existing harvest data for $hub"
            update_status "$hub" "failed" "no harvest data"
            return 1
        fi
    fi

    # Step 3: Remap (mapping + enrichment + jsonl)
    print_step "Step 3/4: Running remap (mapping → enrichment → jsonl)..."
    if ! run_remap "$hub"; then
        log_error "Remap failed for $hub"
        update_status "$hub" "failed" "remap failed"
        return 1
    fi

    # Step 4: S3 Sync
    if [[ "$skip_s3" != "true" ]]; then
        print_step "Step 4/4: Syncing to S3..."
        sync_to_s3 "$hub" || true  # Non-fatal
    else
        print_step "Step 4/4: S3 sync skipped (--no-s3-sync)"
    fi

    # Send email notification
    if [[ "$skip_email" != "true" ]]; then
        send_email_notification "$hub" || true  # Non-fatal
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    update_status "$hub" "complete" "$((duration / 60))m $((duration % 60))s"
    log_success "Pipeline completed for $hub in $((duration / 60))m $((duration % 60))s"

    return 0
}

# =============================================================================
# Main
# =============================================================================

main() {
    local dry_run=false
    local skip_email=false
    local skip_s3=false
    local skip_harvest=false
    local single_hub=""
    local target_month=$(date +%-m)

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                dry_run=true
                shift
                ;;
            --no-email)
                skip_email=true
                shift
                ;;
            --no-s3-sync)
                skip_s3=true
                shift
                ;;
            --skip-harvest)
                skip_harvest=true
                shift
                ;;
            --hub=*)
                single_hub="${1#*=}"
                shift
                ;;
            --month=*)
                target_month="${1#*=}"
                shift
                ;;
            --debug)
                DEBUG=true
                shift
                ;;
            --help)
                cat << EOF
Usage: $0 [options]

Automated DPLA hub ingestion pipeline.

Options:
  --hub=<name>      Process single hub (e.g., --hub=maryland)
  --month=<1-12>    Process hubs for specific month (default: current)
  --dry-run         Show what would be processed without running
  --skip-harvest    Skip harvest step, run remap on existing data
  --no-email        Skip email notifications
  --no-s3-sync      Skip S3 sync after processing
  --debug           Enable debug logging
  --help            Show this help

Examples:
  $0                          # Process all hubs scheduled for current month
  $0 --hub=maryland           # Process only Maryland
  $0 --month=2                # Process February hubs
  $0 --hub=ia --skip-harvest  # Remap existing IA harvest data
  $0 --dry-run                # Preview what would run

Log files: ${LOG_DIR}/
EOF
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done

    echo ""
    echo "=============================================="
    echo "  DPLA Automated Ingestion Pipeline"
    echo "  $(date)"
    echo "=============================================="
    echo "Log file: $LOG_FILE"
    echo "Status file: $STATUS_FILE"
    echo ""

    local hubs_to_process=()

    if [[ -n "$single_hub" ]]; then
        hubs_to_process=("$single_hub")
    else
        # Get hubs scheduled for target month
        while IFS= read -r hub; do
            [[ -z "$hub" ]] && continue
            if is_scheduled_for_month "$hub" "$target_month"; then
                hubs_to_process+=("$hub")
            fi
        done < <(get_scheduled_hubs)
    fi

    if [[ ${#hubs_to_process[@]} -eq 0 ]]; then
        log_info "No hubs scheduled for month $target_month"
        exit 0
    fi

    # Initialize status tracking
    for hub in "${hubs_to_process[@]}"; do
        HUB_STATUS[$hub]="pending"
    done

    echo "Hubs to process (${#hubs_to_process[@]} total):"
    for hub in "${hubs_to_process[@]}"; do
        local provider=$(get_provider_name "$hub")
        local harvest_type=$(get_harvest_type "$hub")
        echo "  - $hub ($provider) [$harvest_type]"
    done
    echo ""

    if [[ "$dry_run" == "true" ]]; then
        echo "Dry run mode - no processing will be done"
        print_all_status
        exit 0
    fi

    # Process each hub SEQUENTIALLY to avoid sbt conflicts
    local success_count=0
    local fail_count=0
    local failed_hubs=()

    log_info "Processing hubs sequentially to avoid sbt conflicts..."

    for hub in "${hubs_to_process[@]}"; do
        if process_hub "$hub" "$skip_email" "$skip_s3" "$skip_harvest"; then
            ((success_count++))
        else
            ((fail_count++))
            failed_hubs+=("$hub")
        fi

        # Brief pause between hubs to let sbt clean up
        sleep 5
    done

    # Final summary
    echo ""
    echo "=============================================="
    echo "  Pipeline Complete"
    echo "=============================================="
    echo "Successful: $success_count"
    echo "Failed: $fail_count"
    if [[ ${#failed_hubs[@]} -gt 0 ]]; then
        echo "Failed hubs: ${failed_hubs[*]}"
        echo ""
        echo "Error details:"
        for hub in "${failed_hubs[@]}"; do
            echo "  $hub: ${HUB_ERRORS[$hub]:-unknown error}"
        done
    fi
    echo ""
    echo "Log file: $LOG_FILE"
    echo "Status file: $STATUS_FILE"
    echo ""

    print_all_status

    # Exit with error if any failures
    [[ $fail_count -gt 0 ]] && exit 1
    exit 0
}

main "$@"
