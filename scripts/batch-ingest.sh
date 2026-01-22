#!/bin/bash
# i3-batch-ingest - Run full pipeline for multiple providers
# Fire and forget batch processing

set -e

I3_HOME="${I3_HOME:-/Users/scott/dpla/code/ingestion3}"
LOG_DIR="${LOG_DIR:-/Users/scott/dpla/logs}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

usage() {
    echo "Usage: batch-ingest.sh [options] <provider1> [provider2] ..."
    echo ""
    echo "Options:"
    echo "  --skip-harvest    Skip harvest step for all providers"
    echo "  --parallel N      Run N providers in parallel (default: 1)"
    echo "  --help            Show this help"
    echo ""
    echo "Examples:"
    echo "  ./batch-ingest.sh maryland virginia wisconsin"
    echo "  ./batch-ingest.sh --skip-harvest maryland virginia"
    echo "  ./batch-ingest.sh --parallel 2 maryland virginia wisconsin ohio"
    exit 1
}

if [ -z "$1" ] || [ "$1" == "--help" ]; then
    usage
fi

SKIP_HARVEST=""
PARALLEL=1
PROVIDERS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-harvest)
            SKIP_HARVEST="--skip-harvest"
            shift
            ;;
        --parallel)
            PARALLEL="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            PROVIDERS+=("$1")
            shift
            ;;
    esac
done

if [ ${#PROVIDERS[@]} -eq 0 ]; then
    echo -e "${RED}ERROR:${NC} No providers specified"
    usage
fi

mkdir -p "$LOG_DIR"

echo ""
echo "=============================================="
echo "  DPLA Batch Ingest"
echo "=============================================="
echo "Providers: ${PROVIDERS[*]}"
echo "Parallel:  $PARALLEL"
echo "Logs:      $LOG_DIR"
echo "=============================================="
echo ""

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY_LOG="$LOG_DIR/batch_${TIMESTAMP}_summary.log"

run_provider() {
    local provider=$1
    local log_file="$LOG_DIR/${TIMESTAMP}_${provider}.log"

    echo "Starting: $provider (log: $log_file)"

    if "$I3_HOME/ingest.sh" "$provider" $SKIP_HARVEST > "$log_file" 2>&1; then
        echo -e "${GREEN}✓${NC} $provider completed successfully"
        echo "$provider: SUCCESS" >> "$SUMMARY_LOG"
        return 0
    else
        echo -e "${RED}✗${NC} $provider failed (see $log_file)"
        echo "$provider: FAILED" >> "$SUMMARY_LOG"
        return 1
    fi
}

export -f run_provider
export I3_HOME SKIP_HARVEST LOG_DIR TIMESTAMP SUMMARY_LOG RED GREEN NC

START_TIME=$(date +%s)

if [ "$PARALLEL" -gt 1 ]; then
    echo "Running $PARALLEL providers in parallel..."
    printf '%s\n' "${PROVIDERS[@]}" | xargs -P "$PARALLEL" -I {} bash -c 'run_provider "$@"' _ {}
else
    for provider in "${PROVIDERS[@]}"; do
        run_provider "$provider"
    done
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=============================================="
echo "  Batch Complete"
echo "  Duration: $((DURATION / 60))m $((DURATION % 60))s"
echo "=============================================="
echo ""
echo "Summary:"
cat "$SUMMARY_LOG"
echo ""
