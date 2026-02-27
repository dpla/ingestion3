#!/bin/bash
# Monitor IngestRemap pipeline and report stage completions/failures
# Usage: ./monitor-pipeline.sh <terminal-file> <spark-pid>

TERM_FILE="$1"
SPARK_PID="$2"
POLL_INTERVAL=120  # seconds

if [ -z "$TERM_FILE" ] || [ -z "$SPARK_PID" ]; then
  echo "Usage: $0 <terminal-file> <spark-pid>"
  exit 1
fi

LAST_STAGE=""
LAST_LINE_COUNT=0
MAPPING_DONE=false
ENRICH_DONE=false
JSONL_DONE=false

echo "=== Pipeline Monitor Started ==="
echo "Terminal file: $TERM_FILE"
echo "Spark PID: $SPARK_PID"
echo "Poll interval: ${POLL_INTERVAL}s"
echo ""

while true; do
  # Check if process is still alive
  if ! kill -0 "$SPARK_PID" 2>/dev/null; then
    echo ""
    echo "============================================"
    echo ">>> PROCESS $SPARK_PID HAS EXITED <<<"
    echo "============================================"
    
    # Check exit code from terminal file
    EXIT_CODE=$(grep "exit_code:" "$TERM_FILE" 2>/dev/null | tail -1 | awk '{print $2}')
    if [ -n "$EXIT_CODE" ]; then
      if [ "$EXIT_CODE" = "0" ]; then
        echo ">>> EXIT CODE: 0 (SUCCESS) <<<"
      else
        echo ">>> EXIT CODE: $EXIT_CODE (FAILURE) <<<"
      fi
    fi
    
    # Show last few meaningful lines
    echo ""
    echo "Last output:"
    grep -E "(Record count|Summary|success|error|Exception|Mapping|Enrichment|Jsonl|completed|Total time)" "$TERM_FILE" 2>/dev/null | tail -20
    echo ""
    
    # Check output dirs
    echo "Output directories:"
    for dir in mapping enrichment jsonl; do
      count=$(find ~/dpla/data/nara/$dir -maxdepth 2 -name "*.parquet" -o -name "*.jsonl" -o -name "*.avro" -o -name "_SUCCESS" 2>/dev/null | wc -l)
      echo "  nara/$dir: $count files"
    done
    
    echo ""
    echo "=== Pipeline Monitor Stopped ==="
    break
  fi

  # Get current line count
  CUR_LINE_COUNT=$(wc -l < "$TERM_FILE" 2>/dev/null)
  
  # Check for stage completions
  CURRENT_STAGES=$(grep -E "finished in|ResultStage.*finished|Mapping complete|Enrichment complete|Record count" "$TERM_FILE" 2>/dev/null | tail -5)
  
  # Check for mapping completion (parquet write stage finishing)  
  MAPPING_STAGE=$(grep -E "parquet at MappingExecutor.*finished|MAP4_0.*MAPRecord|MappingSummary|Mapping complete|Record count.*mapping" "$TERM_FILE" 2>/dev/null | tail -1)
  if [ -n "$MAPPING_STAGE" ] && [ "$MAPPING_DONE" = false ]; then
    echo ""
    echo "============================================"
    echo ">>> MAPPING STAGE COMPLETED <<<"
    echo ">>> $(date) <<<"
    echo "============================================"
    echo "$MAPPING_STAGE"
    MAPPING_DONE=true
  fi

  # Check for enrichment completion
  ENRICH_STAGE=$(grep -E "EnrichRecord|EnrichmentSummary|Enrichment complete|Record count.*enrich" "$TERM_FILE" 2>/dev/null | tail -1)
  if [ -n "$ENRICH_STAGE" ] && [ "$ENRICH_DONE" = false ]; then
    echo ""
    echo "============================================"
    echo ">>> ENRICHMENT STAGE COMPLETED <<<"
    echo ">>> $(date) <<<"
    echo "============================================"
    echo "$ENRICH_STAGE"
    ENRICH_DONE=true
  fi

  # Check for JSONL completion
  JSONL_STAGE=$(grep -E "IndexRecord|JsonlSummary|Jsonl complete|Record count.*jsonl" "$TERM_FILE" 2>/dev/null | tail -1)
  if [ -n "$JSONL_STAGE" ] && [ "$JSONL_DONE" = false ]; then
    echo ""
    echo "============================================"
    echo ">>> JSON-L STAGE COMPLETED <<<"
    echo ">>> $(date) <<<"
    echo "============================================"
    echo "$JSONL_STAGE"
    JSONL_DONE=true
  fi

  # Check for errors
  ERRORS=$(grep -E "(Exception|OutOfMemoryError|error.*Nonzero exit)" "$TERM_FILE" 2>/dev/null | tail -1)
  if [ -n "$ERRORS" ]; then
    echo ""
    echo "============================================"
    echo ">>> ERROR DETECTED <<<"
    echo ">>> $(date) <<<"  
    echo "============================================"
    echo "$ERRORS"
    # Don't break - let it continue monitoring until process exits
  fi

  # Periodic status (every 10 min = every 5th poll)
  POLL_COUNT=${POLL_COUNT:-0}
  POLL_COUNT=$((POLL_COUNT + 1))
  if [ $((POLL_COUNT % 5)) -eq 0 ]; then
    CPU=$(ps -o pcpu= -p "$SPARK_PID" 2>/dev/null | tr -d ' ')
    MEM=$(ps -o rss= -p "$SPARK_PID" 2>/dev/null | tr -d ' ')
    MEM_GB=$(echo "scale=1; ${MEM:-0} / 1048576" | bc 2>/dev/null)
    ELAPSED=$(ps -o etime= -p "$SPARK_PID" 2>/dev/null | tr -d ' ')
    
    # Count completed tasks in current write stage
    TASKS=$(grep -c "Finished task.*in stage 3" "$TERM_FILE" 2>/dev/null)
    TASKS=$((TASKS / 2))  # Each task has 2 log lines
    
    echo "[$(date '+%H:%M')] Status: CPU=${CPU}% MEM=${MEM_GB}GB Elapsed=${ELAPSED} MappingTasks=${TASKS}/286 Lines=${CUR_LINE_COUNT}"
  fi

  sleep "$POLL_INTERVAL"
done
