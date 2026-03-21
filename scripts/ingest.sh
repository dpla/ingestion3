#!/usr/bin/env bash
# i3-ingest - Run full DPLA ingestion3 pipeline (harvest → mapping → enrichment → jsonl → S3 sync)
# This is the "fire and forget" script for a complete provider ingest
#
# IMPORTANT: --output must be $DPLA_DATA (data root), not $DPLA_DATA/$PROVIDER.
# OutputHelper builds root/shortName/activity/..., so provider subdir would double-nest.

set -eo pipefail  # Exit on any error; propagate failures through pipes

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java environment (8g default memory)
setup_java "8g" || die "Failed to setup Java environment"

usage() {
    echo "Usage: ingest.sh <provider-name> [options]"
    echo ""
    echo "Options:"
    echo "  --skip-harvest          Skip harvest step (use existing harvest data)"
    echo "  --harvest-only          Only run harvest step"
    echo "  --skip-s3-sync          Skip S3 sync step (pipeline stops at JSONL)"
    echo "  --resume-from <step>    Resume from a specific step, reusing existing outputs"
    echo "                          for all preceding steps. Valid steps: mapping,"
    echo "                          enrichment, jsonl. Implies --skip-harvest."
    echo "  --help                  Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./ingest.sh maryland                          # Full pipeline"
    echo "  ./ingest.sh maryland --skip-harvest           # Use existing harvest"
    echo "  ./ingest.sh maryland --harvest-only           # Only harvest"
    echo "  ./ingest.sh maryland --skip-s3-sync           # Skip S3 upload"
    echo "  ./ingest.sh maryland --resume-from enrichment # Skip harvest+mapping, reuse"
    echo "                                                #   existing mapping output"
    echo "  ./ingest.sh maryland --resume-from jsonl      # Skip to JSONL, reuse existing"
    echo "                                                #   enrichment output"
    echo ""
    echo "Available providers: artstor, bhl, community-webs, ct, florida, georgia,"
    echo "  getty, gpo, harvard, hathi, heartland, ia, il, indiana, jhn, lc,"
    echo "  maryland, mi, minnesota, mississippi, mt, mwdl, nara, digitalnc,"
    echo "  njde, northwest-heritage, nypl, ohio, oklahoma, p2p, pa, david-rumsey,"
    echo "  scdl, sd, smithsonian, texas, tennessee, txdl, virginias, vt, wisconsin"
    exit 1
}

if [ -z "$1" ] || [ "$1" == "--help" ]; then
    usage
fi

PROVIDER="$1"
shift

SKIP_HARVEST=false
HARVEST_ONLY=false
SKIP_S3_SYNC=false
RESUME_FROM=""

# Minimum free disk space (GB) required before starting each step.
# Enrichment needs more headroom — it expands enriched Avro alongside the mapping input.
DISK_MIN_GB=20
DISK_MIN_ENRICHMENT_GB=30

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-harvest)
            SKIP_HARVEST=true
            shift
            ;;
        --harvest-only)
            HARVEST_ONLY=true
            shift
            ;;
        --skip-s3-sync)
            SKIP_S3_SYNC=true
            shift
            ;;
        --resume-from)
            RESUME_FROM="${2:-}"
            if [[ ! "$RESUME_FROM" =~ ^(mapping|enrichment|jsonl)$ ]]; then
                log_error "--resume-from requires a step: mapping, enrichment, or jsonl"
                usage
            fi
            SKIP_HARVEST=true
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Block delta providers — they require a dedicated script that runs a merge step
# before mapping. Running ingest.sh on a delta harvest produces only the delta
# records, not the full merged dataset.
HARVEST_TYPE=$(get_harvest_type "$PROVIDER")
if [[ "$HARVEST_TYPE" == *"delta"* ]]; then
    die "Provider '$PROVIDER' uses delta harvesting ($HARVEST_TYPE) and cannot be run with ingest.sh. Use the dedicated script instead (e.g. scripts/harvest/nara-ingest.sh)."
fi

# Setup paths
PROVIDER_DATA="$DPLA_DATA/$PROVIDER"
HARVEST_DIR="$PROVIDER_DATA/harvest"
INGEST_LOG="$DPLA_DATA/${PROVIDER}-ingest.log"
# Truncate at run start so heartbeat reads don't pick up stale harvest progress
# from previous ingest runs of the same provider. Skip truncation when resuming
# mid-pipeline so the existing log context is preserved.
if [[ -z "$RESUME_FROM" && "$SKIP_HARVEST" = false ]]; then
    : > "$INGEST_LOG"
fi

# Track provider for status file (ingest-status.sh); trap writes failed + notifies on error
INGEST_PROVIDER="$PROVIDER"
TRAP_HANDLED=false  # set to true when we handle notification+status explicitly before exit
trap 'err=$?
     stop_heartbeat 2>/dev/null || true
     if [[ $err -ne 0 && -n "${INGEST_PROVIDER:-}" && "$TRAP_HANDLED" != "true" ]]; then
         slack_notify ":x: *$INGEST_PROVIDER ingest FAILED* (exit $err)"
         write_hub_status "$INGEST_PROVIDER" failed --error="Exit $err" || true
     fi' EXIT
# Without explicit SIGTERM/SIGINT traps, bash exits immediately on those signals
# and skips the EXIT trap above. These traps call exit() to ensure EXIT fires.
trap 'exit 130' INT
trap 'exit 143' TERM

echo ""
echo "=============================================="
echo "  DPLA Ingestion3 Pipeline"
echo "=============================================="
echo "Provider:     $PROVIDER"
echo "Data Dir:     $PROVIDER_DATA"
echo "Config:       $I3_CONF"
echo "Spark Master: $SPARK_MASTER"
echo "Java:         $JAVA_HOME"
echo "=============================================="
echo ""

START_TIME=$(date +%s)

cd "$I3_HOME"

if [[ -n "$RESUME_FROM" ]]; then
    slack_notify ":arrow_forward: *$PROVIDER ingest resuming from $RESUME_FROM*"
else
    slack_notify ":arrow_forward: *$PROVIDER ingest started* | harvest → map → enrich → jsonl → s3"
fi

# Step 1: Harvest
if [ "$SKIP_HARVEST" = false ]; then
    check_disk_space $DISK_MIN_GB
    write_hub_status "$PROVIDER" harvesting
    print_step "Step 1/5: Harvesting $PROVIDER..."

    HARVEST_LOG="$I3_HOME/logs/harvest-${PROVIDER}-$(date +%Y%m%d_%H%M%S).log"
    mkdir -p "$I3_HOME/logs"
    start_heartbeat "$PROVIDER" "harvest" "$HARVEST_LOG"

    SBT_OPTS="-Xmx15g"
    run_entry dpla.ingestion3.entries.ingest.HarvestEntry \
        --output="$DPLA_DATA" \
        --conf="$I3_CONF" \
        --name="$PROVIDER" \
        --sparkMaster="$SPARK_MASTER" 2>&1 | tee "$HARVEST_LOG"

    stop_heartbeat
    HARVEST_TS_DIR=$(find_latest_data "$PROVIDER" harvest)
    log_info "Harvest complete: $(basename "$HARVEST_TS_DIR")"

    HARVEST_RECORD_COUNT=$(read_manifest_count "$HARVEST_TS_DIR/_MANIFEST")
    if [[ "$HARVEST_RECORD_COUNT" -eq 0 ]]; then
        slack_notify ":x: *$PROVIDER ingest FAILED* — harvest produced 0 records. Check harvester config and source data."
        write_hub_status "$PROVIDER" failed --error="Harvest produced 0 records"
        TRAP_HANDLED=true
        exit 1
    fi

    slack_notify ":white_check_mark: *$PROVIDER harvest complete* (${HARVEST_RECORD_COUNT} records) — starting mapping"
else
    print_step "Skipping harvest (using existing data)..."
    HARVEST_TS_DIR=$(find_latest_data "$PROVIDER" harvest) \
        || die "No existing harvest data for $PROVIDER — run without --skip-harvest first"
fi

if [ "$HARVEST_ONLY" = true ]; then
    write_hub_status "$PROVIDER" complete
    log_info "Harvest-only mode - stopping here"
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo ""
    echo "=============================================="
    echo "  Harvest completed in $((DURATION / 60))m $((DURATION % 60))s"
    echo "=============================================="
    exit 0
fi

# Step 2: Mapping
if [[ "$RESUME_FROM" =~ ^(enrichment|jsonl)$ ]]; then
    print_step "Skipping mapping (--resume-from $RESUME_FROM)..."
    MAP_TS_DIR=$(find_latest_data "$PROVIDER" mapping) \
        || die "No existing mapping data for $PROVIDER — cannot resume from $RESUME_FROM"
    log_info "Reusing mapping: $(basename "$MAP_TS_DIR")"
else
    check_disk_space $DISK_MIN_GB
    write_hub_status "$PROVIDER" remapping
    print_step "Step 2/5: Mapping $PROVIDER..."

    start_heartbeat "$PROVIDER" "mapping" "$INGEST_LOG" 3600
    SBT_OPTS="-Xmx15g -Dspark.sql.parquet.enableVectorizedReader=false"
    run_entry dpla.ingestion3.entries.ingest.MappingEntry \
        --output="$DPLA_DATA" \
        --conf="$I3_CONF" \
        --name="$PROVIDER" \
        --input="$HARVEST_TS_DIR" \
        --sparkMaster="$SPARK_MASTER"
    stop_heartbeat

    MAP_TS_DIR=$(find_latest_data "$PROVIDER" mapping)
    log_info "Mapping complete: $(basename "$MAP_TS_DIR")"

    MAP_RECORD_COUNT=$(read_manifest_count "$MAP_TS_DIR/_MANIFEST")
    if [[ "$MAP_RECORD_COUNT" -eq 0 ]]; then
        slack_notify ":x: *$PROVIDER ingest FAILED* — mapping produced 0 records. Check mapper and source data."
        write_hub_status "$PROVIDER" failed --error="Mapping produced 0 records"
        TRAP_HANDLED=true
        exit 1
    fi

    slack_notify ":white_check_mark: *$PROVIDER mapping complete* (${MAP_RECORD_COUNT} records) — starting enrichment"
fi

# Step 3: Enrichment
if [[ "$RESUME_FROM" == "jsonl" ]]; then
    print_step "Skipping enrichment (--resume-from jsonl)..."
    ENRICH_TS_DIR=$(find_latest_data "$PROVIDER" enrichment) \
        || die "No existing enrichment data for $PROVIDER — cannot resume from jsonl"
    log_info "Reusing enrichment: $(basename "$ENRICH_TS_DIR")"
else
    check_disk_space $DISK_MIN_ENRICHMENT_GB
    print_step "Step 3/5: Enriching $PROVIDER..."

    start_heartbeat "$PROVIDER" "enrichment" "$INGEST_LOG" 3600
    SBT_OPTS="-Xmx18g"
    run_entry dpla.ingestion3.entries.ingest.EnrichEntry \
        --output="$DPLA_DATA" \
        --conf="$I3_CONF" \
        --name="$PROVIDER" \
        --input="$MAP_TS_DIR" \
        --sparkMaster="$SPARK_MASTER"
    stop_heartbeat

    ENRICH_TS_DIR=$(find_latest_data "$PROVIDER" enrichment)
    log_info "Enrichment complete: $(basename "$ENRICH_TS_DIR")"

    ENRICH_RECORD_COUNT=$(read_manifest_count "$ENRICH_TS_DIR/_MANIFEST")
    if [[ "$ENRICH_RECORD_COUNT" -eq 0 ]]; then
        slack_notify ":x: *$PROVIDER ingest FAILED* — enrichment produced 0 records."
        write_hub_status "$PROVIDER" failed --error="Enrichment produced 0 records"
        TRAP_HANDLED=true
        exit 1
    fi

    slack_notify ":white_check_mark: *$PROVIDER enrichment complete* (${ENRICH_RECORD_COUNT} records) — starting JSONL export"
fi

# Step 4: JSONL export
check_disk_space $DISK_MIN_GB
print_step "Step 4/5: JSONL export for $PROVIDER..."

start_heartbeat "$PROVIDER" "jsonl export" "$INGEST_LOG" 3600
SBT_OPTS="-Xmx12g"
run_entry dpla.ingestion3.entries.ingest.JsonlEntry \
    --output="$DPLA_DATA" \
    --conf="$I3_CONF" \
    --name="$PROVIDER" \
    --input="$ENRICH_TS_DIR" \
    --sparkMaster="local[1]"
stop_heartbeat

JSONL_TS_DIR=$(find_latest_data "$PROVIDER" jsonl)
JSONL_TS=$(basename "$JSONL_TS_DIR")
log_info "JSONL export complete: $JSONL_TS"
slack_notify ":white_check_mark: *$PROVIDER JSONL export complete* — starting S3 sync"

# Read record count from JSONL manifest
JSONL_RECORD_COUNT=$(read_manifest_count "$JSONL_TS_DIR/_MANIFEST")

# Safety check: flag any ingest where the new record count has dropped >5% vs the
# last S3 snapshot. This catches bad harvests (empty results, partial pulls, etc.)
# before the partner email goes out. Skipped on first-ever ingest (no prev snapshot).
S3_PREFIX=$(resolve_s3_prefix "$PROVIDER")
PREV_SNAP=$(aws s3 ls "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/" 2>/dev/null \
    | awk '{print $NF}' | sed 's|/||g' | sort | tail -1) || true
PREV_COUNT=0
if [ -n "$PREV_SNAP" ]; then
    PREV_COUNT=$(aws s3 cp "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/${PREV_SNAP}/_MANIFEST" - 2>/dev/null \
        | read_manifest_count /dev/stdin)
fi

SAFETY_OK=true
if [ "$PREV_COUNT" -gt 0 ]; then
    _SAFETY_OUT=$(python3 -c "
new  = $JSONL_RECORD_COUNT
prev = $PREV_COUNT
drop = (prev - new) / prev * 100
print('FAIL' if drop > 5 else 'OK')
print(f'New: {new:,} | Prev: {prev:,} | Change: {-drop:+.1f}%')
")
    SAFETY_RESULT=$(echo "$_SAFETY_OUT" | head -1)
    DELTA_LINE=$(echo "$_SAFETY_OUT" | tail -1)
    if [ "$SAFETY_RESULT" = "FAIL" ]; then
        SAFETY_OK=false
        log_warn "Safety check FAILED — >5% record drop. $DELTA_LINE"
        slack_notify ":warning: *$PROVIDER safety check FAILED* — >5% record drop\n$DELTA_LINE\nNew snapshot: \`$JSONL_TS\`\nPartner email and S3 sync suppressed."
    fi
fi

# Abort before S3 sync if safety check failed — do not replace the previous
# snapshot with a potentially bad dataset.
if [ "$SAFETY_OK" = false ]; then
    slack_notify ":x: *$PROVIDER ingest FAILED* — safety check blocked S3 sync\n$DELTA_LINE\nSnapshot \`$JSONL_TS\` was NOT synced to S3."
    write_hub_status "$PROVIDER" failed
    TRAP_HANDLED=true
    exit 1
fi

# Send partner summary email only if safety check passed
HUB_EMAIL=$(get_hub_email "$PROVIDER")
EMAIL_NOTE=""
if [ -n "$HUB_EMAIL" ]; then
    SBT_OPTS="-Xmx4g"
    if run_entry dpla.ingestion3.utils.Emailer \
           "$MAP_TS_DIR" "$PROVIDER" "$I3_CONF"; then
        EMAIL_NOTE="\nPartner email sent to $HUB_EMAIL"
        log_info "Partner email sent to $HUB_EMAIL"
    else
        log_warn "Partner email failed (non-fatal)"
    fi
fi

# Step 5: S3 sync (unless skipped)
if [ "$SKIP_S3_SYNC" = false ]; then
    write_hub_status "$PROVIDER" syncing
    print_step "Step 5/5: Syncing $PROVIDER to S3..."
    "$SCRIPT_DIR/s3-sync.sh" "$PROVIDER" jsonl
    log_info "S3 sync complete"

    # Compute summary stats (S3_PREFIX, PREV_COUNT, and record count already resolved above)
    NEW_COUNT="$JSONL_RECORD_COUNT"
    TOTAL_SIZE=$(aws s3 ls --summarize --recursive \
        "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/${JSONL_TS}/" 2>/dev/null \
        | grep "Total Size" | awk '{print $NF}' || echo "0")
    SUMMARY=$(python3 -c "
new  = int('${NEW_COUNT}'  or 0)
prev = int('${PREV_COUNT}' or 0)
size_mb = int('${TOTAL_SIZE}' or 0) / 1_048_576
delta = new - prev
sign = '+' if delta >= 0 else ''
print(f'Records: {new:,} ({sign}{delta:,} vs prev) | Size: {size_mb:.1f} MB')
")

    slack_notify "*$PROVIDER ingest complete* :white_check_mark:\nNew snapshot: \`$JSONL_TS\`\n$SUMMARY\nS3: \`s3://dpla-master-dataset/${S3_PREFIX}/jsonl/${JSONL_TS}/\`${EMAIL_NOTE}"
else
    print_step "Skipping S3 sync (--skip-s3-sync)"
    slack_notify "*$PROVIDER ingest complete* :white_check_mark: _(S3 sync skipped)_\nJSONL: \`$JSONL_TS\`${EMAIL_NOTE}"
fi

write_hub_status "$PROVIDER" complete
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=============================================="
echo "  Pipeline completed successfully!"
echo "  Duration: $((DURATION / 60))m $((DURATION % 60))s"
echo "=============================================="
echo ""
echo "Output files:"
echo "  Harvest:    $HARVEST_TS_DIR"
echo "  Mapping:    $MAP_TS_DIR"
echo "  Enrichment: $ENRICH_TS_DIR"
echo "  JSON-L:     $JSONL_TS_DIR"
if [ "$SKIP_S3_SYNC" = false ]; then
    echo "  S3:         synced to dpla-master-dataset/$S3_PREFIX"
fi
echo ""
