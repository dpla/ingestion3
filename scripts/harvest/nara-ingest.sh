#!/usr/bin/env bash
# nara-ingest.sh - Automated NARA Delta Ingest Pipeline
#
# Automates the full NARA delta ingest process:
#   1. Preprocessing - Extracts ZIPs by export group, separates deletes, recompresses
#   2. S3 Sync - Downloads base harvest from S3 if missing
#   3. Delta Harvest - Runs NaraDeltaHarvester on preprocessed data
#   4. Merge - Merges delta with base using NaraMergeUtil (zero-persistence strategy)
#   5. Pipeline - Runs mapping, enrichment, and JSON-L export via IngestRemap
#   6. S3 Sync - Uploads final outputs to S3
#
# NARA delivers data as multiple ZIP files organized by "export group" (e.g., 17.115).
# Each month's delivery is a directory of ZIPs at:
#   ~/dpla/data/nara/originalRecords/YYYYMM/
#
# See docs/ingestion/README_NARA.md for full documentation.
#
# Usage:
#   ./nara-ingest.sh --month=202601
#   ./nara-ingest.sh --month=202512 --skip-pipeline
#   ./nara-ingest.sh --month=202601 --base=/path/to/previous-merged.avro
#   ./nara-ingest.sh --skip-to-pipeline --harvest=/path/to/merged-harvest.avro
#   ./nara-ingest.sh --month=202512,202601   # multi-month (pipeline on last only)

set -e  # Exit on any error

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# S3 configuration
S3_BUCKET="s3://dpla-master-dataset"
S3_NARA_PATH="$S3_BUCKET/nara"

# NARA-specific paths
NARA_DATA="$DPLA_DATA/nara"
NARA_HARVEST="$NARA_DATA/harvest"
NARA_ORIGINALS="$NARA_DATA/originalRecords"

# Datestamp for output directories (YYYYMMDD format required by IngestRemap)
OUTPUT_DATESTAMP=$(date +%Y%m%d)

# ============================================================================
# Memory Configuration
#
# CRITICAL: NARA is ~18.8M records / 34 GB of Avro. These settings are tuned
# to avoid OOM on machines with 16-18 GB RAM. See docs/ingestion/README_NARA.md for details.
#
# - Merge: uses local[1] to minimize concurrent task memory. The zero-persistence
#   strategy in NaraMergeUtil only needs ~3 GB of heap.
# - Pipeline: uses local[4] for CPU-intensive XML mapping. Each task needs ~1-2 GB.
# ============================================================================

# Per-step memory and parallelism (overridable via environment)
HARVEST_SBT_OPTS="${HARVEST_SBT_OPTS:--Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
HARVEST_SPARK_MASTER="${HARVEST_SPARK_MASTER:-local[1]}"

MERGE_SBT_OPTS="${MERGE_SBT_OPTS:--Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
MERGE_SPARK_MASTER="${MERGE_SPARK_MASTER:-local[1]}"

PIPELINE_SBT_OPTS="${PIPELINE_SBT_OPTS:--Xms4g -Xmx14g -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
PIPELINE_SPARK_MASTER="${PIPELINE_SPARK_MASTER:-local[4]}"

# ============================================================================
# Output Functions
# ============================================================================

print_header() {
    echo ""
    echo -e "${CYAN}=============================================="
    echo -e "  $1"
    echo -e "==============================================${NC}"
    echo ""
}

print_error() { log_error "$@"; }
print_info() { log_info "$@"; }
print_success() { log_success "$@"; }

# ============================================================================
# Usage
# ============================================================================

usage() {
    cat <<'USAGE'
Usage: nara-ingest.sh [options]

Automated NARA delta ingest pipeline.

Options:
  --month=YYYYMM[,YYYYMM,...]
                    Month(s) to process. Comma-separated for multi-month.
                    Maps to ~/dpla/data/nara/originalRecords/YYYYMM/
                    Multi-month runs pipeline only on the final month.

  --base=<path>     Use specific base harvest Avro dir (skips S3 sync)
  --force-sync      Force fresh download of base harvest from S3
  --datestamp=YYYYMMDD
                    Override output datestamp (default: today).
                    Used for the final merged harvest directory name.

  --skip-preprocess Skip preprocessing (use existing delta/ directory)
  --skip-pipeline   Stop after merge (don't run mapping/enrichment/jsonl)
  --skip-to-pipeline
                    Skip directly to pipeline using existing merged harvest
  --harvest=<path>  Specify harvest path for --skip-to-pipeline

  --dry-run         Show what would be done without executing
  --help            Show this help message

Environment Variables:
  HARVEST_SBT_OPTS       JVM opts for harvest step  (default: -Xmx12g)
  HARVEST_SPARK_MASTER   Spark master for harvest    (default: local[1])
  MERGE_SBT_OPTS         JVM opts for merge step     (default: -Xmx12g)
  MERGE_SPARK_MASTER     Spark master for merge      (default: local[1])
  PIPELINE_SBT_OPTS      JVM opts for pipeline step  (default: -Xmx14g)
  PIPELINE_SPARK_MASTER  Spark master for pipeline   (default: local[4])

Examples:
  # Single month, full pipeline
  ./nara-ingest.sh --month=202601

  # Two months: Dec merge-only, Jan merge + pipeline
  ./nara-ingest.sh --month=202512,202601

  # Resume pipeline on existing merged harvest
  ./nara-ingest.sh --skip-to-pipeline --harvest=~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro

  # Single month, stop after merge (no mapping/enrichment)
  ./nara-ingest.sh --month=202601 --skip-pipeline

  # Use a specific base instead of S3
  ./nara-ingest.sh --month=202601 --base=~/dpla/data/nara/harvest/202512_000000-nara-OriginalRecord.avro
USAGE
    exit 1
}

# ============================================================================
# Utility Functions
# ============================================================================

setup_java_env() {
    # Find Java 11+ (prefer 19+)
    if [ -z "$JAVA_HOME" ]; then
        if [ -x /usr/libexec/java_home ]; then
            JAVA_HOME=$(/usr/libexec/java_home 2>/dev/null || true)
        fi
        # Fallback: scan common macOS locations
        if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
            for d in "$HOME/Library/Java/JavaVirtualMachines"/*/Contents/Home \
                     /Library/Java/JavaVirtualMachines/*/Contents/Home; do
                if [ -d "$d" ]; then
                    JAVA_HOME="$d"
                    break
                fi
            done
        fi
    fi
    if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
        print_error "JAVA_HOME not set and could not auto-detect. Set JAVA_HOME to a Java 11+ JDK."
        exit 1
    fi
    export JAVA_HOME
    print_info "Using Java: $JAVA_HOME"
}

find_latest_local_base() {
    # Find the most recent harvest Avro directory (sorted by name, newest first)
    ls -td "$NARA_HARVEST"/*-nara-OriginalRecord.avro 2>/dev/null | head -1
}

elapsed_since() {
    local start=$1
    local now=$(date +%s)
    local dur=$((now - start))
    printf "%dm %ds" $((dur / 60)) $((dur % 60))
}

# Install an ERR trap that sends a Slack failure notification for any unhandled
# exit. Set TRAP_HANDLED=true before any explicit exit that already sends its
# own notification to prevent double-posting.
_setup_error_trap() {
    TRAP_HANDLED=false
    trap '
        if [ "$TRAP_HANDLED" = false ]; then
            slack_notify ":x: *nara ingest FAILED* (unexpected error — check log)"
        fi
    ' ERR
}

# ============================================================================
# Prerequisite Checks
# ============================================================================

check_prerequisites() {
    print_step "Checking prerequisites..."

    local missing=()
    command -v aws >/dev/null 2>&1 || missing+=("aws (AWS CLI)")
    command -v java >/dev/null 2>&1 || missing+=("java")
    command -v sbt >/dev/null 2>&1 || missing+=("sbt")
    command -v unzip >/dev/null 2>&1 || missing+=("unzip")
    command -v tar >/dev/null 2>&1 || missing+=("tar")

    if [ ${#missing[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing[*]}"
        exit 1
    fi

    java -version 2>&1 | head -1

    if [ ! -f "$I3_CONF" ]; then
        print_error "Configuration file not found: $I3_CONF"
        exit 1
    fi

    print_success "All prerequisites satisfied"
}

# ============================================================================
# S3 Sync
# ============================================================================

sync_base_from_s3() {
    local force="${1:-false}"
    local local_base
    local_base=$(find_latest_local_base)

    if [ -n "$local_base" ] && [ "$force" != "true" ]; then
        print_info "Local base harvest found: $local_base"
        print_info "Skipping S3 sync (use --force-sync to override)"
        BASE_HARVEST="$local_base"
        return 0
    fi

    print_step "Downloading latest NARA base harvest from S3..."
    mkdir -p "$NARA_HARVEST"

    # Find the single latest merged harvest on S3 rather than syncing the entire
    # prefix. The prefix accumulates all historical harvests (400+ GB going back
    # to 2017); only the most recent one is needed as the base for the merge.
    # Note: aws s3 ls returns at most 1000 keys per call; NARA has ~40 historical
    # entries so this is well within limits, but worth knowing if entries grow.
    local latest_s3_key
    latest_s3_key=$(aws s3 ls "$S3_NARA_PATH/harvest/" \
        | awk '{print $NF}' \
        | grep -E '^[0-9]{8}_[0-9]{6}-nara-OriginalRecord\.avro/$' \
        | sort \
        | tail -1 \
        | sed 's|/$||')

    if [ -z "$latest_s3_key" ]; then
        print_error "No base harvest found on S3 at $S3_NARA_PATH/harvest/"
        exit 1
    fi

    print_info "Latest S3 base: $latest_s3_key"
    slack_notify ":arrow_down: *nara S3 base download starting* — \`$latest_s3_key\`"
    aws s3 sync "$S3_NARA_PATH/harvest/$latest_s3_key/" "$NARA_HARVEST/$latest_s3_key/"

    # We already know the local path from the key; no need to re-scan the directory.
    BASE_HARVEST="$NARA_HARVEST/$latest_s3_key"

    local base_size
    base_size=$(du -sh "$BASE_HARVEST" | cut -f1)
    print_success "Download complete. Base: $BASE_HARVEST ($base_size)"
    slack_notify ":white_check_mark: *nara S3 base download complete* — \`$latest_s3_key\` ($base_size)"
}

sync_outputs_to_s3() {
    print_step "Syncing outputs to S3..."

    local merged_dir
    merged_dir=$(basename "$MERGED_HARVEST")

    print_info "Syncing harvest..."
    aws s3 sync "$MERGED_HARVEST" "$S3_NARA_PATH/harvest/$merged_dir/"

    print_info "Syncing mapping..."
    aws s3 sync "$NARA_DATA/mapping/" "$S3_NARA_PATH/mapping/"

    print_info "Syncing enrichment..."
    aws s3 sync "$NARA_DATA/enrichment/" "$S3_NARA_PATH/enrichment/"

    print_info "Syncing jsonl..."
    aws s3 sync "$NARA_DATA/jsonl/" "$S3_NARA_PATH/jsonl/"

    print_success "S3 sync complete"
}

# ============================================================================
# Preprocessing
#
# NARA delivers ZIPs organized by export group (e.g., 17.115_DESC_0001.zip).
# Each export group may have multiple ZIPs that need to be combined into a
# single tar.gz. Delete files (*NaidsList.xml) are separated out.
# ============================================================================

preprocess_month() {
    local month="$1"
    local src_dir="$NARA_ORIGINALS/$month"
    local dest_dir="$NARA_DATA/delta/$month"

    print_step "Preprocessing month $month..."

    if [ ! -d "$src_dir" ]; then
        print_error "Original records directory not found: $src_dir"
        exit 1
    fi

    mkdir -p "$dest_dir/deletes"

    # Step 1: Move delete files (NaidsList.xml) to deletes/
    local delete_count=0
    for f in "$src_dir"/*_NaidsList.xml "$src_dir"/*NaidsList*.xml; do
        [ -f "$f" ] || continue
        cp "$f" "$dest_dir/deletes/"
        ((delete_count++)) || true
    done
    print_info "Copied $delete_count delete file(s) to $dest_dir/deletes/"

    # Step 2: Identify unique export groups from ZIP filenames
    # Naming pattern: DATE_EXPORTGROUP_DESC_SEQNUM.zip
    # e.g., 2026-02-07_17.115_DESC_0001.zip -> export group = 17.115
    local groups=()
    for zip in "$src_dir"/*.zip; do
        [ -f "$zip" ] || continue
        local basename
        basename=$(basename "$zip")
        # Extract export group: second underscore-delimited field
        # 2026-02-07_17.115_DESC_0001.zip -> 17.115
        local group
        group=$(echo "$basename" | sed -E 's/^[^_]+_([0-9]+\.[0-9]+)_.*/\1/')
        if [ -n "$group" ] && [[ ! " ${groups[*]} " =~ " ${group} " ]]; then
            groups+=("$group")
        fi
    done

    if [ ${#groups[@]} -eq 0 ]; then
        print_error "No ZIP files with recognized export group pattern found in $src_dir"
        exit 1
    fi

    print_info "Found ${#groups[@]} export group(s): ${groups[*]}"

    # Step 3: For each export group, unzip all ZIPs and create a single tar.gz
    local total_xml=0
    for group in "${groups[@]}"; do
        local work_dir="$dest_dir/_work_${group}"
        mkdir -p "$work_dir"

        local zip_count=0
        for zip in "$src_dir"/*_${group}_DESC_*.zip; do
            [ -f "$zip" ] || continue
            unzip -q -o "$zip" -d "$work_dir/"
            ((zip_count++)) || true
        done

        if [ "$zip_count" -eq 0 ]; then
            print_info "  Skipping group $group (no matching ZIPs)"
            rm -rf "$work_dir"
            continue
        fi

        # Remove any delete files that ended up in the work dir
        find "$work_dir" -name "*NaidsList*" -delete 2>/dev/null || true

        # Count XML data files
        local xml_count
        xml_count=$(find "$work_dir" -name "*.xml" -type f | wc -l | tr -d ' ')
        total_xml=$((total_xml + xml_count))

        # Create tar.gz for this export group
        local archive="$dest_dir/${group}_nara_delta.tar.gz"
        (cd "$work_dir" && tar -czf "$archive" *.xml 2>/dev/null) || {
            # If *.xml glob fails, try find-based approach
            (cd "$work_dir" && find . -name "*.xml" -print0 | tar -czf "$archive" --null -T -)
        }

        local archive_size
        archive_size=$(du -h "$archive" | cut -f1)
        print_info "  Group $group: $xml_count XML files -> $archive ($archive_size)"

        rm -rf "$work_dir"
    done

    print_success "Preprocessing complete: $total_xml total XML files in ${#groups[@]} groups"

    # Verify at least one tar.gz was created
    local tgz_count
    tgz_count=$(find "$dest_dir" -maxdepth 1 -name "*.tar.gz" | wc -l | tr -d ' ')
    if [ "$tgz_count" -eq 0 ]; then
        print_error "No tar.gz archives created. Check ZIP file naming patterns."
        exit 1
    fi
}

# ============================================================================
# Delta Harvest
# ============================================================================

update_nara_config() {
    local delta_dir="$1"
    print_step "Updating i3.conf for NARA delta path: $delta_dir"
    sed_i "s|nara.harvest.delta.update = .*|nara.harvest.delta.update = \"$delta_dir/\"|" "$I3_CONF"
    sed_i "s|nara.harvest.endpoint = .*|nara.harvest.endpoint = \"$delta_dir/\"|" "$I3_CONF"
    print_info "Updated i3.conf nara paths -> $delta_dir/"
}

run_delta_harvest() {
    local month="$1"
    local delta_dir="$NARA_DATA/delta/$month"

    print_step "Running NARA delta harvest for $month..."

    # Update i3.conf to point to this month's delta directory
    update_nara_config "$delta_dir"

    cd "$I3_HOME"
    export SBT_OPTS="$HARVEST_SBT_OPTS"

    sbt -java-home "$JAVA_HOME" \
        "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
        --output=$delta_dir/harvest \
        --conf=$I3_CONF \
        --name=nara \
        --sparkMaster=$HARVEST_SPARK_MASTER"

    # Find the harvested output (nested under nara/harvest/)
    DELTA_HARVEST=$(find "$delta_dir/harvest" -type d -name "*-nara-OriginalRecord.avro" 2>/dev/null | sort | tail -1)

    if [ -z "$DELTA_HARVEST" ]; then
        print_error "Delta harvest failed - no output found in $delta_dir/harvest/"
        exit 1
    fi

    local avro_count
    avro_count=$(find "$DELTA_HARVEST" -name "*.avro" -type f | wc -l | tr -d ' ')
    print_success "Delta harvest complete: $DELTA_HARVEST ($avro_count part files)"
    slack_notify ":white_check_mark: *nara delta harvest complete* ($avro_count part files) — starting merge"
}

# ============================================================================
# Merge
# ============================================================================

run_merge() {
    local month="$1"
    local output_datestamp="$2"
    local delta_dir="$NARA_DATA/delta/$month"

    print_step "Running NARA merge for $month..."

    local output_path="$NARA_HARVEST/${output_datestamp}_000000-nara-OriginalRecord.avro"

    print_info "Base harvest:  $BASE_HARVEST"
    print_info "Delta harvest: $DELTA_HARVEST"
    print_info "Deletes dir:   $delta_dir/deletes/"
    print_info "Output path:   $output_path"

    slack_notify ":hourglass: *nara merge starting* — delta into \`$(basename "$BASE_HARVEST")\`"

    cd "$I3_HOME"
    export SBT_OPTS="$MERGE_SBT_OPTS"

    sbt -java-home "$JAVA_HOME" \
        "runMain dpla.ingestion3.utils.NaraMergeUtil \
        $BASE_HARVEST \
        $DELTA_HARVEST \
        $delta_dir/deletes/ \
        $output_path \
        $MERGE_SPARK_MASTER"

    # Verify merge output
    if [ ! -d "$output_path" ]; then
        print_error "Merge failed - output not found: $output_path"
        exit 1
    fi

    # Display merge summary
    local summary_file="$output_path/_LOGS/_SUMMARY.txt"
    if [ -f "$summary_file" ]; then
        echo ""
        echo "=== Merge Summary ($month) ==="
        cat "$summary_file"
        echo "=============================="
    fi

    MERGED_HARVEST="$output_path"
    print_success "Merge complete: $MERGED_HARVEST"
    if [ "${SKIP_PIPELINE:-false}" = true ]; then
        slack_notify ":white_check_mark: *nara merge complete* — \`$(basename "$MERGED_HARVEST")\`"
    else
        slack_notify ":white_check_mark: *nara merge complete* — \`$(basename "$MERGED_HARVEST")\` — starting pipeline"
    fi
}

# ============================================================================
# Pipeline (Mapping -> Enrichment -> JSON-L)
# ============================================================================

# Run safety check against previous S3 snapshot and sync JSONL to S3 if it passes.
# NARA's JSONL output is a full merged dataset (~18.8M records), so the same
# >5% drop check used for full providers is valid here.
run_post_pipeline() {
    local jsonl_dir
    jsonl_dir=$(find "$NARA_DATA/jsonl" -maxdepth 1 -type d -name "*-nara-*.IndexRecord.jsonl" | sort | tail -1)
    if [ -z "$jsonl_dir" ]; then
        print_error "No JSONL output found under $NARA_DATA/jsonl/"
        exit 1
    fi
    local jsonl_ts
    jsonl_ts=$(basename "$jsonl_dir")
    local new_count
    new_count=$(read_manifest_count "$jsonl_dir/_MANIFEST")
    slack_notify ":white_check_mark: *nara JSONL export complete* (${new_count} records) — running safety check"

    local s3_prefix
    s3_prefix=$(resolve_s3_prefix "nara")
    local prev_snap
    prev_snap=$(aws s3 ls "s3://dpla-master-dataset/${s3_prefix}/jsonl/" 2>/dev/null \
        | awk '{print $NF}' | sed 's|/||g' | sort | grep -v "^$" | tail -1)
    local prev_count=0
    if [ -n "$prev_snap" ]; then
        prev_count=$(aws s3 cp "s3://dpla-master-dataset/${s3_prefix}/jsonl/${prev_snap}/_MANIFEST" - 2>/dev/null \
            | read_manifest_count /dev/stdin)
    fi

    # delta_line is used both in the safety-check failure message and the final
    # completion notification. Declare it here so the completion message can reuse
    # it without a second python3 call. Falls back to a simple count when there is
    # no previous snapshot (first-ever ingest).
    local delta_line
    delta_line="Records: ${new_count}"

    if [ "$prev_count" -gt 0 ]; then
        local safety_result _safety_out
        _safety_out=$(python3 -c "
new  = $new_count
prev = $prev_count
drop = (prev - new) / prev * 100
delta = new - prev
sign = '+' if delta >= 0 else ''
print('FAIL' if drop > 5 else 'OK')
print(f'New: {new:,} ({sign}{delta:,} vs prev) | Change: {-drop:+.1f}%')
")
        safety_result=$(echo "$_safety_out" | head -1)
        delta_line=$(echo "$_safety_out" | tail -1)
        if [ "$safety_result" = "FAIL" ]; then
            print_error "Safety check FAILED — >5% record drop. $delta_line"
            slack_notify ":x: *nara ingest FAILED* — safety check blocked S3 sync\n$delta_line\nSnapshot \`$jsonl_ts\` was NOT synced to S3."
            TRAP_HANDLED=true
            write_hub_status "nara" failed
            exit 1
        fi
        print_success "Safety check passed: $delta_line"
    fi

    print_step "Syncing JSONL to S3..."
    aws s3 sync "$jsonl_dir/" "s3://dpla-master-dataset/${s3_prefix}/jsonl/${jsonl_ts}/"
    print_success "S3 sync complete: s3://dpla-master-dataset/${s3_prefix}/jsonl/${jsonl_ts}/"

    slack_notify ":white_check_mark: *nara ingest complete*\nNew snapshot: \`$jsonl_ts\`\n${delta_line}\nS3: \`s3://dpla-master-dataset/${s3_prefix}/jsonl/${jsonl_ts}/\`"
    TRAP_HANDLED=true
    write_hub_status "nara" complete
}

run_pipeline() {
    print_step "Running mapping, enrichment, and JSON-L pipeline..."
    print_info "Input harvest: $MERGED_HARVEST"
    print_info "Spark master:  $PIPELINE_SPARK_MASTER"
    print_info "SBT_OPTS:      $PIPELINE_SBT_OPTS"
    print_info ""
    print_info "NOTE: This step processes ~18.8M XML records and takes ~10 hours."
    print_info "Monitor with: ps aux | grep IngestRemap"
    slack_notify ":hourglass: *nara pipeline starting* — mapping → enrichment → jsonl (~10 hours)"

    cd "$I3_HOME"
    export SBT_OPTS="$PIPELINE_SBT_OPTS"

    sbt -java-home "$JAVA_HOME" \
        "runMain dpla.ingestion3.entries.ingest.IngestRemap \
        --input=$MERGED_HARVEST \
        --output=$NARA_DATA \
        --conf=$I3_CONF \
        --name=nara \
        --sparkMaster=$PIPELINE_SPARK_MASTER"

    print_success "Pipeline complete"
    echo ""
    echo "Output:"
    echo "  Harvest:    $MERGED_HARVEST"
    echo "  Mapping:    $NARA_DATA/mapping/"
    echo "  Enrichment: $NARA_DATA/enrichment/"
    echo "  JSON-L:     $NARA_DATA/jsonl/"
}

# ============================================================================
# Main
# ============================================================================

main() {
    # Parse arguments
    MONTHS=""
    FORCE_SYNC=false
    EXPLICIT_BASE=""
    SKIP_PREPROCESS=false
    SKIP_PIPELINE=false
    SKIP_TO_PIPELINE=false
    EXPLICIT_HARVEST=""
    DRY_RUN=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --month=*)
                MONTHS="${1#*=}"
                shift
                ;;
            --base=*)
                EXPLICIT_BASE="${1#*=}"
                shift
                ;;
            --force-sync)
                FORCE_SYNC=true
                shift
                ;;
            --datestamp=*)
                OUTPUT_DATESTAMP="${1#*=}"
                shift
                ;;
            --skip-preprocess)
                SKIP_PREPROCESS=true
                shift
                ;;
            --skip-pipeline)
                SKIP_PIPELINE=true
                shift
                ;;
            --skip-to-pipeline)
                SKIP_TO_PIPELINE=true
                shift
                ;;
            --harvest=*)
                EXPLICIT_HARVEST="${1#*=}"
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help)
                usage
                ;;
            -*)
                print_error "Unknown option: $1"
                usage
                ;;
            *)
                print_error "Unexpected argument: $1"
                usage
                ;;
        esac
    done

    print_header "NARA Delta Ingest Pipeline"

    # Setup Java
    setup_java_env

    # Check prerequisites
    check_prerequisites

    START_TIME=$(date +%s)

    # -----------------------------------------------------------------------
    # Mode 1: Skip to pipeline (use existing merged harvest)
    # -----------------------------------------------------------------------
    if [ "$SKIP_TO_PIPELINE" = true ]; then
        if [ -n "$EXPLICIT_HARVEST" ]; then
            MERGED_HARVEST="$EXPLICIT_HARVEST"
        else
            MERGED_HARVEST=$(find_latest_local_base)
        fi

        if [ -z "$MERGED_HARVEST" ] || [ ! -d "$MERGED_HARVEST" ]; then
            print_error "No merged harvest found. Use --harvest=<path>"
            exit 1
        fi

        _setup_error_trap

        print_info "Resuming pipeline with: $MERGED_HARVEST"
        slack_notify ":arrow_forward: *nara ingest resuming* — pipeline from \`$(basename "$MERGED_HARVEST")\`"
        run_pipeline
        run_post_pipeline

        END_TIME=$(date +%s)
        print_header "NARA Pipeline Complete"
        echo "Duration: $(elapsed_since $START_TIME)"
        exit 0
    fi

    # -----------------------------------------------------------------------
    # Mode 2: Full or partial ingest (requires --month)
    # -----------------------------------------------------------------------
    if [ -z "$MONTHS" ]; then
        print_error "No --month specified. Use --month=YYYYMM or --skip-to-pipeline."
        usage
    fi

    # Split comma-separated months into array
    IFS=',' read -ra MONTH_ARRAY <<< "$MONTHS"

    echo "Configuration:"
    echo "  Month(s):        ${MONTH_ARRAY[*]}"
    echo "  Output Datestamp: $OUTPUT_DATESTAMP"
    echo "  Data Dir:         $NARA_DATA"
    echo "  Config:           $I3_CONF"
    echo "  Java:             $JAVA_HOME"
    echo ""
    echo "  Harvest:  SBT_OPTS=$HARVEST_SBT_OPTS  Spark=$HARVEST_SPARK_MASTER"
    echo "  Merge:    SBT_OPTS=$MERGE_SBT_OPTS  Spark=$MERGE_SPARK_MASTER"
    echo "  Pipeline: SBT_OPTS=$PIPELINE_SBT_OPTS  Spark=$PIPELINE_SPARK_MASTER"
    echo ""

    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] Would process months: ${MONTH_ARRAY[*]}"
        echo "[DRY RUN] Pipeline on final month only (unless --skip-pipeline)"
        exit 0
    fi

    _setup_error_trap

    if [ "$SKIP_PIPELINE" = true ]; then
        slack_notify ":arrow_forward: *nara ingest started* | months: ${MONTH_ARRAY[*]} | delta harvest → merge (--skip-pipeline)"
    else
        slack_notify ":arrow_forward: *nara ingest started* | months: ${MONTH_ARRAY[*]} | delta harvest → merge → pipeline → s3"
    fi

    # Step 1: Get base harvest
    if [ -n "$EXPLICIT_BASE" ]; then
        if [ ! -d "$EXPLICIT_BASE" ]; then
            print_error "Specified base harvest not found: $EXPLICIT_BASE"
            exit 1
        fi
        BASE_HARVEST="$EXPLICIT_BASE"
        print_info "Using explicit base harvest: $BASE_HARVEST"
    else
        sync_base_from_s3 "$FORCE_SYNC"
    fi

    # Step 2: Process each month
    local total_months=${#MONTH_ARRAY[@]}
    local month_idx=0

    for month in "${MONTH_ARRAY[@]}"; do
        ((month_idx++)) || true
        local is_last_month=false
        [ "$month_idx" -eq "$total_months" ] && is_last_month=true

        print_header "Processing Month $month ($month_idx/$total_months)"

        # Determine output datestamp for this month's merge
        # For intermediate months, use YYYYMM format (6 digit)
        # For the final month, use full YYYYMMDD format (8 digit - required by IngestRemap)
        local merge_datestamp
        if [ "$is_last_month" = true ]; then
            merge_datestamp="$OUTPUT_DATESTAMP"
        else
            merge_datestamp="$month"
        fi

        # 2a. Preprocess
        if [ "$SKIP_PREPROCESS" != true ]; then
            # Check if already preprocessed (tar.gz files exist)
            local existing_tgz
            existing_tgz=$(find "$NARA_DATA/delta/$month" -maxdepth 1 -name "*.tar.gz" 2>/dev/null | wc -l | tr -d ' ')
            if [ "$existing_tgz" -gt 0 ]; then
                print_info "Month $month already preprocessed ($existing_tgz tar.gz files). Skipping."
            else
                preprocess_month "$month"
            fi
        else
            print_info "Skipping preprocessing (--skip-preprocess)"
        fi

        # 2b. Delta Harvest
        # Check if already harvested
        local existing_harvest
        existing_harvest=$(find "$NARA_DATA/delta/$month/harvest" -type d -name "*-nara-OriginalRecord.avro" 2>/dev/null | sort | tail -1)
        if [ -n "$existing_harvest" ]; then
            print_info "Delta harvest already exists: $existing_harvest. Skipping harvest."
            DELTA_HARVEST="$existing_harvest"
        else
            run_delta_harvest "$month"
        fi

        # 2c. Merge
        # Check if already merged
        local existing_merge="$NARA_HARVEST/${merge_datestamp}_000000-nara-OriginalRecord.avro"
        if [ -d "$existing_merge" ] && [ -f "$existing_merge/_LOGS/_SUMMARY.txt" ]; then
            print_info "Merge output already exists: $existing_merge. Skipping merge."
            MERGED_HARVEST="$existing_merge"
        else
            run_merge "$month" "$merge_datestamp"
        fi

        # Use this month's merged output as the base for the next month
        BASE_HARVEST="$MERGED_HARVEST"

        print_success "Month $month complete ($(elapsed_since $START_TIME) elapsed)"
    done

    # Step 3: Pipeline (only on the final month's merged output)
    if [ "$SKIP_PIPELINE" != true ]; then
        run_pipeline
        run_post_pipeline
    else
        print_info "Skipping pipeline (--skip-pipeline)"
        print_info "Final merged harvest: $MERGED_HARVEST"
    fi

    END_TIME=$(date +%s)
    print_header "NARA Ingest Complete"
    echo "Duration: $(elapsed_since $START_TIME)"
    echo ""
    echo "Final harvest:  $MERGED_HARVEST"
    if [ "$SKIP_PIPELINE" != true ]; then
        echo "Mapping:        $NARA_DATA/mapping/"
        echo "Enrichment:     $NARA_DATA/enrichment/"
        echo "JSON-L:         $NARA_DATA/jsonl/"
    fi
    echo ""
    echo "Next steps:"
    echo "  1. Inspect merge logs:  cat $MERGED_HARVEST/_LOGS/_SUMMARY.txt"
    echo ""
}

# Run main
main "$@"
