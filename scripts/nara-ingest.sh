#!/bin/bash
# nara-ingest.sh - Automated NARA Delta Ingest Pipeline
#
# This script automates the NARA ingest process:
#   1. S3 Sync - Downloads base harvest from S3 if missing/outdated
#   2. Preprocessing - Extracts ZIP, separates deletes, recompresses
#   3. Delta Harvest - Runs NaraDeltaHarvester on new data
#   4. Merge - Merges delta with base using NaraMergeUtil
#   5. Pipeline - Runs mapping, enrichment, and JSON-L export
#
# Usage:
#   ./nara-ingest.sh /path/to/nara-export.zip
#   ./nara-ingest.sh /path/to/nara-export.zip --force-sync
#   ./nara-ingest.sh /path/to/nara-export.zip --base=/path/to/base.avro
#   ./nara-ingest.sh --skip-to-pipeline  # Use existing merged harvest

set -e  # Exit on any error

# ============================================================================
# Configuration
# ============================================================================

# Java configuration
JAVA_HOME_PATH="/Users/scott/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home"
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"

# SBT JVM options - Optimized for large NARA dataset (~14.5M records)
export SBT_OPTS="-Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Ingestion3 configuration
I3_HOME="${I3_HOME:-/Users/scott/dpla/code/ingestion3}"
I3_CONF="${I3_CONF:-/Users/scott/dpla/code/ingestion3-conf/i3.conf}"
DPLA_DATA="${DPLA_DATA:-/Users/scott/dpla/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

# S3 configuration
S3_BUCKET="s3://dpla-master-dataset"
S3_NARA_PATH="$S3_BUCKET/nara"

# NARA-specific paths
NARA_DATA="$DPLA_DATA/nara"
NARA_HARVEST="$NARA_DATA/harvest"
DATESTAMP=$(date +%Y%m%d)
NARA_DELTA="$NARA_DATA/delta/$DATESTAMP"

# Spark configuration for large datasets
SPARK_CONF="--conf spark.driver.memory=8g \
  --conf spark.executor.memory=8g \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.default.parallelism=200 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.autoBroadcastJoinThreshold=-1"

# ============================================================================
# Colors and Output Functions
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${CYAN}=============================================="
    echo -e "  $1"
    echo -e "==============================================${NC}"
    echo ""
}

print_step() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1" >&2
}

print_info() {
    echo -e "${YELLOW}INFO:${NC} $1"
}

print_success() {
    echo -e "${GREEN}SUCCESS:${NC} $1"
}

# ============================================================================
# Usage
# ============================================================================

usage() {
    echo "Usage: nara-ingest.sh <nara-export.zip> [options]"
    echo ""
    echo "Automated NARA delta ingest pipeline."
    echo ""
    echo "Arguments:"
    echo "  <nara-export.zip>     Path to NARA export ZIP file"
    echo ""
    echo "Options:"
    echo "  --force-sync          Force download of base harvest from S3"
    echo "  --base=<path>         Use specific base harvest (skips S3 sync)"
    echo "  --skip-to-pipeline    Skip to mapping/enrichment (use existing merged harvest)"
    echo "  --skip-pipeline       Stop after merge (don't run mapping/enrichment)"
    echo "  --dry-run             Show what would be done without executing"
    echo "  --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./nara-ingest.sh ~/downloads/nara-delta-20260115.zip"
    echo "  ./nara-ingest.sh ~/downloads/nara-delta.zip --force-sync"
    echo "  ./nara-ingest.sh ~/downloads/nara-delta.zip --base=~/nara/harvest/old.avro"
    echo ""
    exit 1
}

# ============================================================================
# Validation Functions
# ============================================================================

check_prerequisites() {
    print_step "Checking prerequisites..."
    
    local missing=()
    
    # Check for required commands
    command -v aws >/dev/null 2>&1 || missing+=("aws (AWS CLI)")
    command -v java >/dev/null 2>&1 || missing+=("java")
    command -v sbt >/dev/null 2>&1 || missing+=("sbt")
    command -v unzip >/dev/null 2>&1 || missing+=("unzip")
    command -v tar >/dev/null 2>&1 || missing+=("tar")
    
    if [ ${#missing[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing[*]}"
        exit 1
    fi
    
    # Check Java version
    java -version 2>&1 | head -1
    
    # Check AWS credentials (if S3 sync needed)
    if [ "$SKIP_S3_SYNC" != "true" ] && [ -z "$EXPLICIT_BASE" ]; then
        if ! aws sts get-caller-identity >/dev/null 2>&1; then
            print_error "AWS credentials not configured. Run 'aws configure' or set AWS_PROFILE"
            exit 1
        fi
        print_info "AWS credentials verified"
    fi
    
    # Check config file
    if [ ! -f "$I3_CONF" ]; then
        print_error "Configuration file not found: $I3_CONF"
        exit 1
    fi
    
    print_success "All prerequisites satisfied"
}

validate_input_file() {
    local input="$1"
    
    if [ -z "$input" ]; then
        print_error "No input file specified"
        usage
    fi
    
    if [ ! -f "$input" ]; then
        print_error "Input file not found: $input"
        exit 1
    fi
    
    if [[ ! "$input" == *.zip ]]; then
        print_error "Input file must be a ZIP file: $input"
        exit 1
    fi
    
    print_info "Input file validated: $input"
}

# ============================================================================
# S3 Sync Functions
# ============================================================================

find_latest_local_base() {
    # Find the most recent harvest avro directory
    local latest=$(ls -td "$NARA_HARVEST"/*-nara-OriginalRecord.avro 2>/dev/null | head -1)
    echo "$latest"
}

sync_from_s3() {
    local force="${1:-false}"
    local local_base=$(find_latest_local_base)
    
    if [ -n "$local_base" ] && [ "$force" != "true" ]; then
        print_info "Local base harvest found: $local_base"
        print_info "Skipping S3 sync (use --force-sync to override)"
        BASE_HARVEST="$local_base"
        return 0
    fi
    
    print_step "Syncing NARA base harvest from S3..."
    mkdir -p "$NARA_HARVEST"
    
    # Sync harvest directory from S3
    aws s3 sync "$S3_NARA_PATH/harvest/" "$NARA_HARVEST/" \
        --exclude "*" \
        --include "*-nara-OriginalRecord.avro/*" \
        --no-progress
    
    # Find the synced base
    BASE_HARVEST=$(find_latest_local_base)
    
    if [ -z "$BASE_HARVEST" ]; then
        print_error "No base harvest found after S3 sync"
        exit 1
    fi
    
    print_success "S3 sync complete. Base harvest: $BASE_HARVEST"
}

# ============================================================================
# Preprocessing Functions
# ============================================================================

preprocess_nara_export() {
    local input="$1"
    local work_dir="$NARA_DELTA"
    
    print_step "Preprocessing NARA export..."
    
    # Create working directories
    mkdir -p "$work_dir/deletes"
    mkdir -p "$work_dir/extracted"
    
    # Extract ZIP file
    print_info "Extracting ZIP file..."
    unzip -q "$input" -d "$work_dir/extracted"
    
    # Count files before processing
    local total_files=$(find "$work_dir/extracted" -type f -name "*.xml" | wc -l | tr -d ' ')
    print_info "Found $total_files XML files in export"
    
    # Move and rename delete files
    print_info "Processing delete files..."
    local delete_count=0
    while IFS= read -r -d '' file; do
        local basename=$(basename "$file")
        mv "$file" "$work_dir/deletes/${DATESTAMP}_${basename}"
        ((delete_count++)) || true
    done < <(find "$work_dir/extracted" -name "deletes_*.xml" -print0)
    
    print_info "Moved $delete_count delete file(s) to $work_dir/deletes/"
    
    # Count remaining data files
    local data_files=$(find "$work_dir/extracted" -type f -name "*.xml" | wc -l | tr -d ' ')
    print_info "Remaining data files: $data_files"
    
    if [ "$data_files" -eq 0 ]; then
        print_error "No data files found after removing deletes"
        exit 1
    fi
    
    # Create tar.gz from remaining files
    print_info "Creating compressed archive..."
    tar -czf "$work_dir/${DATESTAMP}_nara_delta.tar.gz" -C "$work_dir/extracted" .
    
    # Verify archive was created
    if [ ! -f "$work_dir/${DATESTAMP}_nara_delta.tar.gz" ]; then
        print_error "Failed to create tar.gz archive"
        exit 1
    fi
    
    local archive_size=$(du -h "$work_dir/${DATESTAMP}_nara_delta.tar.gz" | cut -f1)
    print_success "Created archive: ${DATESTAMP}_nara_delta.tar.gz ($archive_size)"
    
    # Clean up extracted files to save space
    rm -rf "$work_dir/extracted"
    
    DELTA_ARCHIVE="$work_dir/${DATESTAMP}_nara_delta.tar.gz"
    DELETES_DIR="$work_dir/deletes"
}

# ============================================================================
# Harvest Functions
# ============================================================================

run_delta_harvest() {
    print_step "Running NARA delta harvest..."
    
    cd "$I3_HOME"
    
    # Run the delta harvester
    # Note: This requires i3.conf to be configured with nara.harvest.delta.update path
    sbt -java-home "$JAVA_HOME_PATH" \
        "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
        --output=$NARA_DELTA/harvest \
        --conf=$I3_CONF \
        --name=nara \
        --sparkMaster=$SPARK_MASTER"
    
    # Find the harvested output
    DELTA_HARVEST=$(ls -td "$NARA_DELTA/harvest"/*-nara-OriginalRecord.avro 2>/dev/null | head -1)
    
    if [ -z "$DELTA_HARVEST" ]; then
        print_error "Delta harvest failed - no output found"
        exit 1
    fi
    
    local record_count=$(find "$DELTA_HARVEST" -name "*.avro" | wc -l | tr -d ' ')
    print_success "Delta harvest complete: $DELTA_HARVEST (${record_count} part files)"
}

# ============================================================================
# Merge Functions
# ============================================================================

run_merge() {
    print_step "Running NARA merge utility..."
    
    local output_path="$NARA_HARVEST/${DATESTAMP}_000000-nara-OriginalRecord.avro"
    
    print_info "Base harvest: $BASE_HARVEST"
    print_info "Delta harvest: $DELTA_HARVEST"
    print_info "Deletes dir: $DELETES_DIR"
    print_info "Output path: $output_path"
    
    cd "$I3_HOME"
    
    sbt -java-home "$JAVA_HOME_PATH" \
        "runMain dpla.ingestion3.utils.NaraMergeUtil \
        $BASE_HARVEST \
        $DELTA_HARVEST \
        $DELETES_DIR \
        $output_path \
        $SPARK_MASTER"
    
    # Verify merge output
    if [ ! -d "$output_path" ]; then
        print_error "Merge failed - output not found: $output_path"
        exit 1
    fi
    
    # Display merge summary
    local summary_file="$output_path/_LOGS/_SUMMARY.txt"
    if [ -f "$summary_file" ]; then
        echo ""
        echo "=== Merge Summary ==="
        cat "$summary_file"
        echo "===================="
    fi
    
    MERGED_HARVEST="$output_path"
    print_success "Merge complete: $MERGED_HARVEST"
}

# ============================================================================
# Pipeline Functions (Mapping, Enrichment, JSON-L)
# ============================================================================

run_pipeline() {
    print_step "Running mapping, enrichment, and JSON-L pipeline..."
    
    cd "$I3_HOME"
    
    # Use the merged harvest as input
    local harvest_input="$MERGED_HARVEST"
    
    # Run IngestRemap (mapping -> enrichment -> jsonl)
    sbt -java-home "$JAVA_HOME_PATH" \
        "runMain dpla.ingestion3.entries.ingest.IngestRemap \
        --input=$harvest_input \
        --output=$NARA_DATA \
        --conf=$I3_CONF \
        --name=nara \
        --sparkMaster=$SPARK_MASTER"
    
    print_success "Pipeline complete"
    
    echo ""
    echo "Output files:"
    echo "  Harvest:    $harvest_input"
    echo "  Mapping:    $NARA_DATA/mapping"
    echo "  Enrichment: $NARA_DATA/enrichment"
    echo "  JSON-L:     $NARA_DATA/jsonl"
}

# ============================================================================
# Main
# ============================================================================

main() {
    # Parse arguments
    INPUT_FILE=""
    FORCE_SYNC=false
    EXPLICIT_BASE=""
    SKIP_TO_PIPELINE=false
    SKIP_PIPELINE=false
    DRY_RUN=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force-sync)
                FORCE_SYNC=true
                shift
                ;;
            --base=*)
                EXPLICIT_BASE="${1#*=}"
                shift
                ;;
            --skip-to-pipeline)
                SKIP_TO_PIPELINE=true
                shift
                ;;
            --skip-pipeline)
                SKIP_PIPELINE=true
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
                INPUT_FILE="$1"
                shift
                ;;
        esac
    done
    
    print_header "NARA Delta Ingest Pipeline"
    
    echo "Configuration:"
    echo "  Datestamp:    $DATESTAMP"
    echo "  Data Dir:     $NARA_DATA"
    echo "  Config:       $I3_CONF"
    echo "  Spark Master: $SPARK_MASTER"
    echo "  Java:         $JAVA_HOME"
    echo ""
    
    START_TIME=$(date +%s)
    
    # Check prerequisites
    check_prerequisites
    
    if [ "$SKIP_TO_PIPELINE" = true ]; then
        # Skip to pipeline using existing merged harvest
        MERGED_HARVEST=$(find_latest_local_base)
        if [ -z "$MERGED_HARVEST" ]; then
            print_error "No merged harvest found for --skip-to-pipeline"
            exit 1
        fi
        print_info "Using existing harvest: $MERGED_HARVEST"
        run_pipeline
    else
        # Full process
        validate_input_file "$INPUT_FILE"
        
        # Step 1: S3 Sync (get base harvest)
        if [ -n "$EXPLICIT_BASE" ]; then
            if [ ! -d "$EXPLICIT_BASE" ]; then
                print_error "Specified base harvest not found: $EXPLICIT_BASE"
                exit 1
            fi
            BASE_HARVEST="$EXPLICIT_BASE"
            print_info "Using explicit base harvest: $BASE_HARVEST"
        else
            sync_from_s3 "$FORCE_SYNC"
        fi
        
        # Step 2: Preprocess
        preprocess_nara_export "$INPUT_FILE"
        
        # Step 3: Delta Harvest
        run_delta_harvest
        
        # Step 4: Merge
        run_merge
        
        # Step 5: Pipeline (optional)
        if [ "$SKIP_PIPELINE" != true ]; then
            run_pipeline
        else
            print_info "Skipping pipeline (--skip-pipeline)"
        fi
    fi
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    print_header "NARA Ingest Complete"
    echo "Duration: $((DURATION / 60))m $((DURATION % 60))s"
    echo ""
}

# Run main
main "$@"
