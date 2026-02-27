---
name: dpla-monitor-ingest-remap
description: Monitor a running IngestRemap (remap.sh / ingest.sh step 2) or orchestrator remap stages. Use when user asks whether mapping/enrichment/jsonl is done yet, to monitor remap progress, or to check which stage is currently running.
---

# dpla-monitor-ingest-remap
## Orchestrator
```bash
set -a
source .env
set +a

./scripts/status/ingest-status.sh --watch
```

## Manual remap
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
