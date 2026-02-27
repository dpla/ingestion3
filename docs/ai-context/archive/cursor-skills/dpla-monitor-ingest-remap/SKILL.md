---
name: dpla-monitor-ingest-remap
description: Monitor a running IngestRemap (remap.sh / ingest.sh step 2) or orchestrator remap stages. Use when user asks whether mapping/enrichment/jsonl is done yet, to monitor remap progress, or to check which stage is currently running.
---

# dpla-monitor-ingest-remap
Prefer orchestrator status when available; otherwise monitor the latest mapping/enrichment/jsonl output dirs and `_SUCCESS` markers.

## Orchestrator runs (recommended)
```bash
set -a
source .env
set +a

./scripts/status/ingest-status.sh --watch

# One hub only
./scripts/status/ingest-status.sh <hub> --watch
```

## Manual remap/ingest runs
### 1) Capture output (optional but recommended)
```bash
set -a
source .env
set +a

HUB=<hub>
LOG="logs/remap-${HUB}-$(date +%Y%m%d_%H%M%S).log"

# If you're running remap directly:
./scripts/remap.sh "$HUB" 2>&1 | tee "$LOG"

# Or if you're running full ingest and want to watch step 2:
# ./scripts/ingest.sh "$HUB" 2>&1 | tee "$LOG"
```

### 2) Poll latest output dirs + `_SUCCESS`
```bash
set -a
source .env
set +a

HUB=<hub>
DATA_ROOT="${DPLA_DATA:-$HOME/dpla/data}"

while true; do
  echo ""
  date
  for step in mapping enrichment jsonl; do
    step_dir="$DATA_ROOT/$HUB/$step"
    latest=$(ls -1d "$step_dir"/*/ 2>/dev/null | sort -r | head -1 || true)
    latest="${latest%/}"

    if [[ -z "$latest" ]]; then
      echo "$step: (no output yet)"
      continue
    fi

    if [[ -f "$latest/_SUCCESS" ]]; then
      echo "$step: SUCCESS $(basename "$latest")"
    else
      echo "$step: RUNNING $(basename "$latest")"
    fi
  done
  sleep 30
done
```

## Notes
- If you see `_temporary` but no `_SUCCESS`, the write is incomplete (job still running or failed).