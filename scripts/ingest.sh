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
    echo "  --skip-harvest    Skip harvest step (use existing harvest data)"
    echo "  --harvest-only    Only run harvest step"
    echo "  --skip-s3-sync    Skip S3 sync step (pipeline stops at JSONL)"
    echo "  --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./ingest.sh maryland              # Full pipeline (harvest + remap + S3 sync)"
    echo "  ./ingest.sh maryland --skip-harvest  # Use existing harvest"
    echo "  ./ingest.sh maryland --harvest-only  # Only harvest"
    echo "  ./ingest.sh maryland --skip-s3-sync  # Skip S3 upload"
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
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Setup paths
PROVIDER_DATA="$DPLA_DATA/$PROVIDER"
HARVEST_DIR="$PROVIDER_DATA/harvest"

# Track provider for status file (ingest-status.sh); trap writes failed + notifies on error
INGEST_PROVIDER="$PROVIDER"
trap 'err=$?
     stop_heartbeat 2>/dev/null || true
     if [[ $err -ne 0 && -n "${INGEST_PROVIDER:-}" ]]; then
         write_hub_status "$INGEST_PROVIDER" failed --error="Exit $err"
         slack_notify ":x: *$INGEST_PROVIDER ingest FAILED* (exit $err)"
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

slack_notify ":arrow_forward: *$PROVIDER ingest started* | harvest → map → enrich → jsonl → s3"

# Step 1: Harvest
if [ "$SKIP_HARVEST" = false ]; then
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
    slack_notify ":white_check_mark: *$PROVIDER harvest complete* — starting mapping"
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
write_hub_status "$PROVIDER" remapping
print_step "Step 2/5: Mapping $PROVIDER..."

SBT_OPTS="-Xmx12g"
run_entry dpla.ingestion3.entries.ingest.MappingEntry \
    --output="$DPLA_DATA" \
    --conf="$I3_CONF" \
    --name="$PROVIDER" \
    --input="$HARVEST_TS_DIR" \
    --sparkMaster="$SPARK_MASTER"

MAP_TS_DIR=$(find_latest_data "$PROVIDER" mapping)
log_info "Mapping complete: $(basename "$MAP_TS_DIR")"
slack_notify ":white_check_mark: *$PROVIDER mapping complete* — starting enrichment"

# Step 3: Enrichment
print_step "Step 3/5: Enriching $PROVIDER..."

SBT_OPTS="-Xmx18g"
run_entry dpla.ingestion3.entries.ingest.EnrichEntry \
    --output="$DPLA_DATA" \
    --conf="$I3_CONF" \
    --name="$PROVIDER" \
    --input="$MAP_TS_DIR" \
    --sparkMaster="$SPARK_MASTER"

ENRICH_TS_DIR=$(find_latest_data "$PROVIDER" enrichment)
log_info "Enrichment complete: $(basename "$ENRICH_TS_DIR")"
slack_notify ":white_check_mark: *$PROVIDER enrichment complete* — starting JSONL export"

# Step 4: JSONL export
print_step "Step 4/5: JSONL export for $PROVIDER..."

SBT_OPTS="-Xmx12g"
run_entry dpla.ingestion3.entries.ingest.JsonlEntry \
    --output="$DPLA_DATA" \
    --conf="$I3_CONF" \
    --name="$PROVIDER" \
    --input="$ENRICH_TS_DIR" \
    --sparkMaster="local[1]"

JSONL_TS_DIR=$(find_latest_data "$PROVIDER" jsonl)
JSONL_TS=$(basename "$JSONL_TS_DIR")
log_info "JSONL export complete: $JSONL_TS"
slack_notify ":white_check_mark: *$PROVIDER JSONL export complete* — starting S3 sync"

# Send partner summary email (based on mapping data; independent of S3 sync)
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

    # Compute record delta vs previous snapshot
    S3_PREFIX=$(resolve_s3_prefix "$PROVIDER")
    NEW_COUNT=$(aws s3 cp "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/${JSONL_TS}/_MANIFEST" - 2>/dev/null \
        | grep "^Record count:" | awk '{print $NF}' || echo "0")
    PREV_SNAP=$(aws s3 ls "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/" 2>/dev/null \
        | awk '{print $NF}' | sed 's|/||g' | sort | grep -v "^$" | tail -2 | head -1)
    PREV_COUNT=$(aws s3 cp "s3://dpla-master-dataset/${S3_PREFIX}/jsonl/${PREV_SNAP}/_MANIFEST" - 2>/dev/null \
        | grep "^Record count:" | awk '{print $NF}' || echo "0")
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
