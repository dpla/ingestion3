---
name: dpla-oai-harvest-watch
description: Watch an OAI harvest log and report set-by-set progress + ETA (for hubs using harvest.setlist). Use when user asks to watch OAI harvest progress, track sets, estimate completion, or monitor a long OAI harvest.
---

# dpla-oai-harvest-watch
Uses `scripts/status/watch-oai-harvest.py` to parse `ListRecords&set=...` transitions in a harvest log file.

## Workflow
1) Run the harvest with output captured to a log file.
2) Run the watcher against that log.

## Commands
```bash
set -a
source .env
set +a

HUB=<hub>
LOG="logs/harvest-${HUB}-$(date +%Y%m%d_%H%M%S).log"
CONF="${I3_CONF:-$HOME/dpla/code/ingestion3-conf/i3.conf}"

# Start harvest with log capture
./scripts/harvest.sh "$HUB" 2>&1 | tee "$LOG"

# In another terminal: watch set progress (total auto-parsed from i3.conf when possible)
./venv/bin/python scripts/status/watch-oai-harvest.py --log="$LOG" --conf="$CONF" --hub="$HUB"

# If the hub does not have harvest.setlist in i3.conf, you can provide a manual total:
# ./venv/bin/python scripts/status/watch-oai-harvest.py --log="$LOG" --total=<n>
```

## Notes
- This works best for OAI hubs that paginate by `set=` (i.e., a `harvest.setlist` in i3.conf).
- If the harvest output does not contain `ListRecords&set=...`, use orchestrator status instead: `./scripts/status/ingest-status.sh --watch`.
