---
name: dpla-oai-harvest-watch
description: Watch an OAI harvest log and report set-by-set progress + ETA (for hubs using harvest.setlist). Use when user asks to watch OAI harvest progress, track sets, or estimate completion.
---

# dpla-oai-harvest-watch
```bash
set -a
source .env
set +a

HUB=<hub>
LOG="logs/harvest-${HUB}-$(date +%Y%m%d_%H%M%S).log"
CONF="${I3_CONF:-$HOME/dpla/code/ingestion3-conf/i3.conf}"

./scripts/harvest.sh "$HUB" 2>&1 | tee "$LOG"
./venv/bin/python scripts/status/watch-oai-harvest.py --log="$LOG" --conf="$CONF" --hub="$HUB"
```
