#!/usr/bin/env bash
#
# jsonl.sh - Run DPLA ingestion3 JSON-L export step
#
# Exports enriched records to gzipped JSON-L format for Elasticsearch indexing.
# Includes single-instance locking, ETA estimation, per-provider logging, and
# clean shutdown (kill_tree prevents orphan JVMs).
#
# All sbt/Java output goes to the log file. Watch progress with:
#   tail -f logs/jsonl-<provider>.log
#
# Usage:
#   ./scripts/jsonl.sh <provider-name> [input-path]
#
# Examples:
#   ./scripts/jsonl.sh nara                       # auto-find latest enrichment
#   ./scripts/jsonl.sh maryland /specific/path    # explicit input path
#
# Key detail: --output must be $DPLA_DATA (the data root), NOT $DPLA_DATA/$PROVIDER.
# OutputHelper builds: root / shortName / jsonl / timestamp-shortName-schema
# So --output=$DPLA_DATA → .../$PROVIDER/jsonl/... (correct)
#    --output=$DPLA_DATA/$PROVIDER → .../$PROVIDER/$PROVIDER/jsonl/... (WRONG)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
if [[ -z "${1:-}" ]]; then
    echo "Usage: jsonl.sh <provider-name> [input-path]"
    echo ""
    echo "If input-path is not specified, uses the most recent enrichment from:"
    echo "  \$DPLA_DATA/<provider>/enrichment/"
    echo ""
    echo "Examples:"
    echo "  ./scripts/jsonl.sh nara"
    echo "  ./scripts/jsonl.sh maryland /path/to/enrichment/dir"
    exit 1
fi

PROVIDER="$1"

# ---------------------------------------------------------------------------
# Resolve input: explicit path or auto-find latest enrichment
# ---------------------------------------------------------------------------
if [[ -n "${2:-}" ]]; then
    ENRICHMENT_INPUT="$2"
else
    ENRICHMENT_INPUT=$(find_latest_data "$PROVIDER" "enrichment") || true
    if [[ -z "$ENRICHMENT_INPUT" ]]; then
        die "No enrichment data found for '$PROVIDER' in $DPLA_DATA/$PROVIDER/enrichment/"
    fi
fi

if [[ ! -d "$ENRICHMENT_INPUT" ]]; then
    die "Enrichment input not found: $ENRICHMENT_INPUT"
fi

# --output must be the DATA ROOT (not provider dir); see header comment.
OUTPUT_ROOT="$DPLA_DATA"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup_java "8g" || die "Failed to setup Java environment"
mkdir -p "$I3_HOME/logs"

LOG_FILE="$I3_HOME/logs/jsonl-${PROVIDER}.log"
LOCK_FILE="$I3_HOME/logs/jsonl-${PROVIDER}.lock"
ENTRY_PID=""

export SPARK_MASTER="${SPARK_MASTER:-local[4]}"

# ---------------------------------------------------------------------------
# Single-instance lock (per provider)
# ---------------------------------------------------------------------------
if [[ -f "$LOCK_FILE" ]]; then
    existing=$(cat "$LOCK_FILE" 2>/dev/null || true)
    if [[ -n "$existing" ]] && kill -0 "$existing" 2>/dev/null; then
        die "Another JSON-L export for '$PROVIDER' is already running (PID $existing)."
    fi
    rm -f "$LOCK_FILE"
fi
echo $$ > "$LOCK_FILE"

# ---------------------------------------------------------------------------
# Cleanup: remove lock and kill process tree on exit/signal
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -n "$ENTRY_PID" ]] && kill -0 "$ENTRY_PID" 2>/dev/null; then
        echo "[$(date '+%H:%M:%S')] Shutting down (killing process tree rooted at $ENTRY_PID)..." >> "$LOG_FILE" 2>/dev/null || true
        kill_tree "$ENTRY_PID"
        sleep 1
        # Force-kill anything still alive
        kill -9 "$ENTRY_PID" 2>/dev/null || true
        local stragglers
        stragglers=$(pgrep -P "$ENTRY_PID" 2>/dev/null) || true
        for s in $stragglers; do kill -9 "$s" 2>/dev/null || true; done
    fi
    rm -f "$LOCK_FILE"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Clean incomplete previous runs (dirs without _SUCCESS)
# ---------------------------------------------------------------------------
jsonl_dir="$OUTPUT_ROOT/$PROVIDER/jsonl"
if [[ -d "$jsonl_dir" ]]; then
    for d in "$jsonl_dir"/202*; do
        [[ -d "$d" && ! -f "$d/_SUCCESS" ]] && rm -rf "$d"
    done 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# ETA estimate
# ---------------------------------------------------------------------------
nparts=$(find "$ENRICHMENT_INPUT" -maxdepth 1 -name 'part-*.avro' 2>/dev/null | wc -l | tr -d ' ')
[[ -z "$nparts" || "$nparts" -eq 0 ]] && nparts="unknown"

if [[ "$nparts" != "unknown" ]]; then
    # Extract core count from SPARK_MASTER (e.g. local[4] → 4)
    cores=$(echo "$SPARK_MASTER" | sed -n 's/local\[\([0-9]*\)\]/\1/p')
    [[ -z "$cores" || "$cores" -eq 0 ]] && cores=4
    # ~50s per partition is a conservative estimate
    est_min=$(( nparts * 50 / cores / 60 ))
    if [[ "$PLATFORM" == "macos" ]]; then
        eta=$(date -v+"${est_min}M" '+%H:%M' 2>/dev/null || echo "~${est_min}m")
    else
        eta=$(date -d "+${est_min} minutes" '+%H:%M' 2>/dev/null || echo "~${est_min}m")
    fi
else
    eta="unknown"
fi

# ---------------------------------------------------------------------------
# Log header (to terminal AND log file)
# ---------------------------------------------------------------------------
header="=== JSON-L export: $PROVIDER ===
  Started:    $(date '+%Y-%m-%d %H:%M:%S')
  Input:      $ENRICHMENT_INPUT
  Output:     $OUTPUT_ROOT/$PROVIDER/jsonl/
  Partitions: $nparts
  Spark:      $SPARK_MASTER
  Memory:     8g heap
  ETA:        ~$eta
  PID:        $$
  Log:        $LOG_FILE
==========================="

echo "$header"               # terminal
echo "$header" > "$LOG_FILE" # log (overwrite for clean per-run log)
echo ""
echo "Watching progress: tail -f $LOG_FILE"
echo ""

# ---------------------------------------------------------------------------
# Run JsonlEntry
# ---------------------------------------------------------------------------
# Background the entry point so we can capture the PID for kill_tree cleanup.
run_entry dpla.ingestion3.entries.ingest.JsonlEntry \
    --input="$ENRICHMENT_INPUT" \
    --output="$OUTPUT_ROOT" \
    --name="$PROVIDER" \
    --sparkMaster="$SPARK_MASTER" \
    >> "$LOG_FILE" 2>&1 &

ENTRY_PID=$!
echo "[$(date '+%H:%M:%S')] Process launched (PID $ENTRY_PID), waiting..." >> "$LOG_FILE"

wait "$ENTRY_PID"
rc=$?
ENTRY_PID=""  # prevent cleanup from killing already-exited process

if [[ $rc -eq 0 ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] JSON-L export for '$PROVIDER' completed successfully." | tee -a "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] JSON-L export for '$PROVIDER' FAILED (exit $rc)." | tee -a "$LOG_FILE"
    exit $rc
fi
