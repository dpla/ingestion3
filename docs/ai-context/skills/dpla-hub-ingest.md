---
description: Run a full DPLA hub ingest (harvest → map → enrich → JSONL → S3 sync) on the ingest EC2 instance. Use when user says "ingest <hub>", "harvest <hub>", "run <hub> ingest", "<hub> hub ingest", "<hub> pipeline", "run <hub>", or "ingest all hubs".
allowedTools:
  - Bash(aws *)
  - Bash(python3*)
  - Bash(curl*)
  - Bash(sleep*)
  - Bash(grep*)
  - Bash(base64*)
  - Bash(bash*)
  - Bash(source*)
  - Bash(nohup*)
  - Bash(CMDID=*)
  - Bash(CMD_ID=*)
  - Bash(PARAMS=*)
  - Bash(LAUNCH_PARAMS=*)
  - Bash(SCRIPT_B64=*)
  - Bash(SCRIPT_NAME=*)
  - Bash(STATE=*)
  - Bash(NEW_COUNT=*)
  - Bash(PREV_COUNT=*)
  - Bash(DELTA_LINE=*)
  - Bash(TOTAL_SIZE=*)
  - Bash(NEW_SNAP=*)
  - Bash(PREV_SNAP=*)
  - Bash(HARVEST_TS=*)
  - Bash(MAP_TS=*)
  - Bash(ENRICH_TS=*)
  - Bash(JSONL_TS=*)
  - Bash(HUB=*)
  - Bash(DB=*)
  - Bash(CW_DB_PATH=*)
  - Bash(cat *)
  - Bash(chmod *)
  - Bash(chown *)
  - Bash(mkdir *)
  - Bash(ls *)
  - Bash(tail *)
  - Bash(head *)
  - Bash(df *)
  - Bash(ps *)
  - Bash(sed *)
  - Bash(awk *)
  - Bash(sort *)
  - Bash(wc *)
  - Bash(wait)
  - Bash(wait *)
---

# DPLA Hub Ingest

## Purpose

Orchestrate a full harvest-to-S3-sync ingest for any DPLA hub on the ingest EC2 instance. Covers: starting the EC2 instance, running the pipeline via SSM (harvest → mapping → enrichment → JSONL → S3 sync), verifying results, and stopping the instance.

**This ingest does NOT rebuild the Elasticsearch index or affect the live dp.la site.** Index rebuilds (sparkindexer) are a separate operation done after all monthly ingests complete.

## Trigger Phrases

- "Ingest `<hub>`" / "Harvest `<hub>`" / "Run `<hub>` ingest"
- "Run the `<hub>` pipeline" / "Start `<hub>` harvest"
- "Ingest all hubs" / "Run this month's ingests"

## EC2 Ingest Instance

| Field | Value |
|-------|-------|
| Instance ID | `i-0a0def8581efef783` |
| Name | `ingest` |
| Type | m8g.2xlarge (8 vCPU, 32GB RAM, aarch64/Graviton) |
| OS | Amazon Linux 2023 |
| Private IP | `172.30.2.13` |
| VPC | `vpc-b36ab3d6` |
| SSH user | `ec2-user` (NOT ubuntu) |
| Connectivity | SSM Session Manager (no SSH key or bastion needed) |
| AWS access | EC2 instance role `ingestion3-spark` (no IAM keys needed) |
| Cost | ~$0.36/hr — always stop when done |

The instance is normally **stopped** between ingests to save cost (~$0.36/hr running).

## EC2 Environment Layout

All paths under `/home/ec2-user/`:

| Path | Contents |
|------|----------|
| `ingestion3/` | Repo (main branch), Java/SBT via `mise` |
| `ingestion3-conf/` | Hub configuration (`i3.conf`) |
| `ingestion3/.env` | DPLA_DATA, I3_CONF, SLACK_WEBHOOK, JAVA_HOME |
| `data/` | `$DPLA_DATA` — all pipeline output |
| `data/<hub>/harvest/` | Raw harvested records (Avro) |
| `data/<hub>/mapping/` | Mapped records (Avro) |
| `data/<hub>/enrichment/` | Enriched records (Avro) |
| `data/<hub>/jsonl/` | Final JSONL for indexing |

**Java**: Temurin 11.0.29 via mise.
**SBT**: 1.11.7 via mise (on PATH in login shells).

## SSM Command Pattern

Every remote command uses this pattern — always run as `ec2-user` with a **login shell** (required for mise/Java/SBT to be on PATH):

**Send command:**
```bash
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 7200 \
  --parameters '{"commands":["sudo -u ec2-user bash -lc \"<COMMAND>\""]}' \
  --query 'Command.CommandId' --output text)
echo "Command ID: $CMDID"
```

**Poll for completion** (repeat until Status is not `InProgress`):
```bash
aws ssm get-command-invocation \
  --command-id "$CMDID" \
  --instance-id i-0a0def8581efef783 \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
  --output json
```

**Important**: Sleep 8–15 seconds before polling, and between retries. SSM results are not always immediately available.

**Always build `--parameters` JSON with python3** — never use inline `'{"commands":[...]}'`. Inline quoting silently breaks when the command contains single quotes (e.g. `grep '^hub\.'`), `$!`, backticks, or nested double quotes, producing misleading "Error parsing parameter" or empty grep results. python3 handles all escaping correctly:
```bash
PARAMS=$(python3 -c "
import json
cmd = 'sudo -u ec2-user bash -lc \"<COMMAND>\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 7200 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
```

**Polling loop** — use POSIX `[ ]` not `[[ ]]`; `[[ != ]]` triggers a zsh parse error on the local Mac:
```bash
sleep 15
while true; do
  STATUS=$(aws ssm get-command-invocation \
    --command-id "$CMDID" --instance-id i-0a0def8581efef783 \
    --query 'Status' --output text 2>/dev/null)
  echo "$(date '+%H:%M:%S') — $STATUS"
  if [ "$STATUS" != "InProgress" ] && [ "$STATUS" != "Pending" ]; then
    aws ssm get-command-invocation \
      --command-id "$CMDID" --instance-id i-0a0def8581efef783 \
      --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
      --output json
    break
  fi
  sleep 30
done
```

## Hub Configuration Reference

Hub config lives in `i3.conf` on the EC2 at `/home/ec2-user/ingestion3-conf/i3.conf`.

Before running, check the hub's harvest type:
```bash
grep "^<hub>\.harvest\.type" "$I3_CONF"
```

Key harvest types and their network requirements:

| Type | Notes |
|------|-------|
| `localoai` | OAI-PMH over HTTP/HTTPS. Most hubs. |
| `api` | REST API (e.g. MDL/SD uses `metl.lib.umn.edu`). EC2-reachable. Slow to respond — use 60s+ curl timeout. |
| `file` | **community-webs**: DB export pre-processing runs entirely on EC2 (see Community Webs Pre-processing section). **Other file hubs**: `harvest.endpoint` references `/Users/scott/...` paths from a previous operator — require manual staging to EC2 before harvest. |
| `nara.file.delta` | NARA-specific delta file format. Complex — consult README_NARA.md. |

**Note:** **maryland** and **getty** have known IP blocks on EC2 — harvest locally on your Mac (2–3GB disk, 16GB+ RAM) or ask their IT to allowlist `52.2.32.179`. Always pre-flight test the endpoint from EC2 (see Step 3) before starting.

## Community Webs Pre-processing

Community Webs is a `file`-type hub — Internet Archive sends a SQLite database (`.db`) that must be exported to a JSONL ZIP on the EC2 before harvest. **Everything runs on EC2 via SSM** — no local Mac steps needed.

Run these steps **after Step 3** (verify EC2 environment) and **before Step 4** (harvest), whenever hub is `community-webs`.

### Step CW1: Get DB file location

If not provided by the user, ask:

> "Where is the Community Webs `.db` file? Provide an S3 URI (e.g. `s3://dpla-uploads/community-webs/export.db`) or a path already on the EC2 (e.g. `/home/ec2-user/data/community-webs/originalRecords/export.db`)."

### Step CW2: Check prerequisites on EC2

```bash
sudo -u ec2-user bash -lc "
  sqlite3 --version 2>&1 || sudo dnf install -y sqlite
  jq --version 2>&1 || sudo dnf install -y jq
  echo PREREQS_OK"
```

### Step CW3: Stage DB to EC2 (if S3 URI)

If the user provided an S3 URI, download it:

```bash
sudo -u ec2-user bash -lc "
  mkdir -p /home/ec2-user/data/community-webs/originalRecords
  aws s3 cp <S3_URI> /home/ec2-user/data/community-webs/originalRecords/cw-export.db
  echo STAGE_OK"
```

If already on EC2, just verify: `sudo -u ec2-user bash -lc "ls -lh <EC2_PATH>"`

Save the confirmed EC2 path as `CW_DB_PATH`.

### Step CW4: Export DB → JSONL → ZIP and update i3.conf

Write the export script locally, upload to EC2 via base64, and run it. The script exports the `ait` table, validates all records have an `id` field, ZIPs the JSONL, and updates `i3.conf` — all in one shot.

```bash
cat > /tmp/cw-export.sh << 'SCRIPT_END'
#!/bin/bash -l
set -euo pipefail

DB="$1"
CONF=/home/ec2-user/ingestion3-conf/i3.conf
ORIGINALS=/home/ec2-user/data/community-webs/originalRecords
DATESTAMP=$(date +%Y%m%d)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR="${ORIGINALS}/${DATESTAMP}"
mkdir -p "$OUT_DIR"

echo "DB: $DB"
echo "--- Exporting DB ---"
sqlite3 "$DB" --json "SELECT * FROM ait" > "$OUT_DIR/tmp.json"
RECORD_COUNT=$(jq 'length' "$OUT_DIR/tmp.json")
echo "Exported $RECORD_COUNT records"

echo "--- Converting to JSONL ---"
jq -c '.[]' "$OUT_DIR/tmp.json" > "$OUT_DIR/community-webs.jsonl"

echo "--- Validating ---"
JSONL_PATH="$OUT_DIR/community-webs.jsonl" python3 -c "
import json, sys, os
errors, skipped = 0, 0
with open(os.environ['JSONL_PATH']) as f:
    for i, line in enumerate(f, 1):
        line = line.strip()
        if not line: continue
        try:
            rec = json.loads(line)
            if rec.get('status') == 'deleted':
                skipped += 1
            elif 'id' not in rec:
                print(f'Line {i}: missing id field')
                errors += 1
        except Exception as e:
            print(f'Line {i}: {e}')
            errors += 1
if errors:
    print(f'Validation FAILED: {errors} errors')
    sys.exit(1)
print(f'Validation passed ({skipped} deleted records skipped)')
"

echo "--- Creating ZIP ---"
ZIP_NAME="community-webs-${TIMESTAMP}.zip"
(cd "$OUT_DIR" && zip -j "$ZIP_NAME" community-webs.jsonl)
rm -f "$OUT_DIR/community-webs.jsonl" "$OUT_DIR/tmp.json"

echo "--- Updating i3.conf ---"
sed -i "s|community-webs\.harvest\.endpoint.*|community-webs.harvest.endpoint = \"${OUT_DIR}/\"|" "$CONF"
grep "community-webs.harvest.endpoint" "$CONF"

echo "OUT_DIR=${OUT_DIR}"
echo "RECORD_COUNT=${RECORD_COUNT}"
echo "EXPORT_SUCCESS"
SCRIPT_END

SCRIPT_B64=$(base64 < /tmp/cw-export.sh)
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 60 \
  --parameters "{\"commands\":[\"echo '${SCRIPT_B64}' | base64 -d > /home/ec2-user/cw-export.sh && chmod +x /home/ec2-user/cw-export.sh && echo WRITE_OK\"]}" \
  --query 'Command.CommandId' --output text)
# Poll until Status=Success, verify output contains WRITE_OK
```

Then run it — use `python3` to build the `--parameters` JSON to safely handle the path:

```bash
PARAMS=$(python3 -c "
import json
db = '<CW_DB_PATH>'
cmd = 'sudo -u ec2-user bash -l /home/ec2-user/cw-export.sh ' + db
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 300 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
# Poll until Status=Success — export takes ~1–3 minutes depending on DB size
```

From the output, capture:
- `OUT_DIR` — the ZIP directory (i3.conf is already updated; no further action needed)
- `RECORD_COUNT` — sanity-check that it's in the expected range (community-webs typically 8k–15k)

Verify `EXPORT_SUCCESS` is present before continuing.

### Proceed to Step 4

`i3.conf` is already updated. Continue with Step 4 (harvest) using `community-webs` as the hub name — harvest will read from the ZIP automatically.

## Full Procedure

### Step 0: Identify the Hub and Check Memory

**First**, read `~/.claude/memory/hub-ingest-memory.md` and surface any content under the hub's section. If persistent notes exist, state them clearly before proceeding — they may affect how this ingest should be run. If no entry exists yet for this hub, proceed normally; one may be created after the run.

The memory file uses `## hub-name` markdown sections to organize per-hub content. If the file or directory doesn't exist yet, proceed normally — it will be created when the first memory entry is written in Step 6. If the file cannot be read, log a warning and continue without persistent notes.

Confirm the hub key (e.g. `sd`, `maryland`, `indiana`) and check its config:

```bash
grep "^<hub>\." "$I3_CONF"
```

Note the `harvest.type` and `harvest.endpoint`. For `file` harvests, check that the file path exists and confirm with the user before proceeding.

**Check for test hub status:**
```bash
grep "^<hub>\.status" "$I3_CONF"
```

If the result is `<hub>.status = test`, this is a **test hub**. Announce this clearly and set `IS_TEST_HUB=true` to carry through the remaining steps:

> "⚠️ **`<hub>` is a test hub** (`status = test` in i3.conf). This ingest will:
> - Run the full harvest → mapping → enrichment → JSONL pipeline normally
> - **Skip S3 sync** — data stays on EC2 only and cannot be picked up by sparkindexer
> - **Skip the partner summary email**
> - Still generate all summaries and logs for manual review
>
> See `docs/ingestion/README_TEST_HUBS.md` for full conventions."

For test hubs, also check for the mapper in the experimental subpackage:
```bash
ls src/main/scala/dpla/ingestion3/mappers/providers/experimental/
```

If hub is `community-webs`, run the Community Webs Pre-processing steps (see above) after Step 3 and before Step 4.

#### New Hub or First S3 File Harvest — Pre-flight Checklist

Run this checklist before Step 2 whenever:
- The hub has never been ingested before, OR
- The hub's `harvest.endpoint` has changed (new S3 bucket, new path), OR
- The hub's `harvest.type` is `file` with an S3 endpoint

**1. Verify S3 bucket access from your local machine:**
```bash
aws s3 ls s3://<bucket>/<path>/ | head -10
```
Confirm you can see the expected files. If access is denied, the EC2 role may also lack permissions — check the S3 bucket policy.

**2. Inspect the actual file formats in the bucket:**
```bash
aws s3 ls s3://<bucket>/<path>/ | awk '{print $NF}' | sed 's/.*\.//' | sort | uniq -c | sort -rn
```
This shows the file extensions present (e.g. `xml.gz`, `zip`, `jsonl`). Confirm the harvester supports them:

| File Extension | Harvester | Supported? |
|---|---|---|
| `.zip` (OAI-PMH XML inside) | `OaiFileHarvester` | ✅ |
| `.xml.gz` (OAI-PMH XML, gzipped) | `OaiFileHarvester` | ✅ |
| `.zip` (JSONL inside) | `JsonFileHarvester` | ✅ |
| `.jsonl` | `JsonFileHarvester` | ✅ |

If unsure which harvester the hub uses, grep the Scala source: `grep -r "<hub>" src/main/scala/dpla/ingestion3/executors/` or check the mapper for the hub.

**3. Confirm the i3.conf endpoint path:**
```bash
grep "<hub>\.harvest" "$I3_CONF"
```
The endpoint must point to the **folder containing the files** (e.g. `s3://dpla-hub-ohio/2026-03-20/`), not a parent bucket root. If it needs updating, use python3 to avoid quoting issues:
```bash
python3 -c "
content = open(os.environ["I3_CONF"]).read()
content = content.replace('<hub>.harvest.endpoint = \"<old>\"', '<hub>.harvest.endpoint = \"<new>\"')
open(os.environ["I3_CONF"], 'w').write(content)
print('Updated:', content[content.find('<hub>.harvest.endpoint'):content.find('<hub>.harvest.endpoint')+60])
"
```

**4. Count the files:**
```bash
aws s3 ls s3://<bucket>/<path>/ | wc -l
```
Sanity-check this matches what the hub contact said. For Ohio-style `.xml.gz` feeds, each file typically contains thousands of records — 434 files × ~1,800 records = ~780K total.

### Step 1: Pre-flight — Verify Endpoint Reachability

For `localoai` hubs, test the OAI endpoint **locally first**:
```bash
curl -s --max-time 15 "<harvest.endpoint>?verb=Identify" | head -5
```

For `api` hubs (e.g. MDL), test with a longer timeout:
```bash
curl -s --max-time 60 "<harvest.endpoint>?<harvest.query>&rows=1" | head -3
```

If the endpoint is unreachable locally → **stop**, the hub is down.
If the endpoint works locally but needs EC2 verification → proceed to Step 2, then test from EC2 (Step 3).

### Step 2: Start the EC2 Instance

Check current state first — skip start if already running:
```bash
STATE=$(aws ec2 describe-instances --instance-ids i-0a0def8581efef783 \
  --query 'Reservations[0].Instances[0].State.Name' --output text)
echo "Current state: $STATE"
```

If not `running`:
```bash
aws ec2 start-instances --instance-ids i-0a0def8581efef783
aws ec2 wait instance-running --instance-ids i-0a0def8581efef783
echo "Instance running"
```

Wait 30 seconds for SSM agent, then verify SSM connectivity:
```bash
sleep 30
aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=i-0a0def8581efef783" \
  --query 'InstanceInformationList[0].PingStatus' --output text
```

Expected: `Online`. If not yet online, retry after 15 seconds (SSM agent takes ~30–60s after boot).

### Step 3: Verify EC2 Environment

#### Step 3a: Pull latest ingestion3

Always do this at the start of every ingest session. The EC2 has no auto-update mechanism and can drift behind main, which can cause failures (e.g. `Emailer.main` not found). Use `git fetch + reset --hard` — **not** `git pull`. The EC2 repo can accumulate local commits (debug changes, temp fixes) that cause `git pull` to fail with "divergent branches":

```bash
PARAMS=$(python3 -c "
import json
cmd = 'sudo -u ec2-user bash -lc \"cd /home/ec2-user/ingestion3 && git fetch https://github.com/dpla/ingestion3.git main && git reset --hard FETCH_HEAD && git log --oneline -1\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 60 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
```

Expected output: the latest commit hash and message. If this fails (network issue), investigate before proceeding — do not run an ingest on stale code.

#### Step 3b: Check environment

Run two SSM commands (splitting avoids single-quote escaping issues with grep patterns in python3 JSON):

```bash
# Command 1: Java, SBT, disk
PARAMS=$(python3 -c "
import json
cmd = 'sudo -u ec2-user bash -lc \"echo === Java === && java -version 2>&1 && echo === SBT === && sbt --version 2>&1 | tail -2 && echo === Disk === && df -h / | tail -1\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" --timeout-seconds 60 \
  --parameters "$PARAMS" --query 'Command.CommandId' --output text)
# Poll (use loop from SSM Command Pattern above)

# Command 2: hub config + prior S3 snapshots
PARAMS=$(python3 -c "
import json
hub = '<hub>'
cmd = f'sudo -u ec2-user bash -lc \"grep {hub}\\\\. /home/ec2-user/ingestion3-conf/i3.conf && aws s3 ls s3://dpla-master-dataset/{hub}/jsonl/ | tail -3\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" --timeout-seconds 30 \
  --parameters "$PARAMS" --query 'Command.CommandId' --output text)
# Poll
```

For `localoai` hubs, also test the endpoint from EC2:
```bash
curl -s --max-time 15 "<endpoint>?verb=Identify" | head -3
```

For `api` hubs, use a 60-second timeout:
```bash
curl -s --max-time 60 "<endpoint>?<query>&rows=1" | head -2
```

**If endpoint is unreachable from EC2 but reachable locally** → firewall or IP block issue. Stop the instance and run the harvest locally instead (see EC2-blocked hubs note above).

### Step 4: Launch ingest.sh

**Always use `ingest.sh` — never run individual SBT steps manually.** The script handles the full pipeline (harvest → mapping → enrichment → JSONL → S3 sync) with Slack notifications at every step, hub status tracking, 0-record abort, and safety checks. Running steps manually bypasses all of this.

Launch it as a background process so SSM doesn't time out on long ingests.

The snippet below reads `IS_TEST_HUB` (set earlier if this is a test hub) and automatically appends `--skip-s3-sync` when true, preventing output from ever reaching S3:

```bash
IS_TEST_HUB=${IS_TEST_HUB:-false}
PARAMS=$(python3 -c "
import json
hub = '<hub>'
is_test = "${IS_TEST_HUB}".lower() == 'true'
extra = ' --skip-s3-sync' if is_test else ''
cmd = f'sudo -u ec2-user bash -lc \"nohup bash /home/ec2-user/ingestion3/scripts/ingest.sh {hub}{extra} > /home/ec2-user/data/{hub}-ingest.log 2>&1 </dev/null &\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 30 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
```

After SSM returns Success (just confirming the launch), verify the process is running:

```bash
PARAMS=$(python3 -c "
import json
cmd = 'sudo -u ec2-user bash -lc \"ps aux | grep ingest.sh | grep -v grep\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" --timeout-seconds 15 \
  --parameters "$PARAMS" --query 'Command.CommandId' --output text)
```

**Monitor via Slack** — `ingest.sh` posts to #tech-alerts at each milestone (started → harvest complete → mapping complete → enrichment complete → JSONL complete → complete or failed). You can also tail the log:

```bash
PARAMS=$(python3 -c "
import json
hub = '<hub>'
cmd = f'sudo -u ec2-user bash -lc \"tail -30 /home/ec2-user/data/{hub}-ingest.log\"'
print(json.dumps({'commands': [cmd]}))
")
CMDID=$(aws ssm send-command --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" --timeout-seconds 15 \
  --parameters "$PARAMS" --query 'Command.CommandId' --output text)
```

**To resume from a failed step** (e.g. if mapping failed and harvest data is intact):
```bash
IS_TEST_HUB=${IS_TEST_HUB:-false}
PARAMS=$(python3 -c "
import json
hub = '<hub>'
is_test = "${IS_TEST_HUB}".lower() == 'true'
extra = ' --skip-s3-sync' if is_test else ''
cmd = f'sudo -u ec2-user bash -lc \"nohup bash /home/ec2-user/ingestion3/scripts/ingest.sh {hub}{extra} --resume-from mapping > /home/ec2-user/data/{hub}-ingest.log 2>&1 </dev/null &\"'
print(json.dumps({'commands': [cmd]}))
")
```

Valid `--resume-from` values: `mapping`, `enrichment`, `jsonl`.

### Step 5: Stop EC2 and Verify

Wait for the Slack `:white_check_mark: *<hub> ingest complete*` notification. Then stop the instance:

```bash
aws ec2 stop-instances --instance-ids i-0a0def8581efef783
```

`ingest.sh` handles result verification, safety checks, partner email, and the final Slack notification internally — no additional steps needed.

**Test hubs:** `ingest.sh` will skip S3 sync (due to `--skip-s3-sync`). After the Slack completion notification, also confirm the JSONL output is present on EC2:
```bash
ls -lh /home/ec2-user/data/<hub>/jsonl/
```
Do **not** send the partner summary email. The mapping summary and logs are available on EC2 for manual review. If you want to share results with the partner contact, retrieve the summary manually and send it yourself.

### Step 6: Update Ingest Memory

After the ingest completes (whether fully successful, partially recovered, or abandoned after investigation):

1. Review everything that happened during this run.
2. Identify anything **new or worth preserving** — for example:
   - A changed endpoint, new S3 path, or updated file format that had to be determined
   - An API performance issue, rate limiting, or connectivity problem encountered and resolved
   - A mapping error, anomaly, or unexpected record count — and how it was handled
   - A TODO or open question to raise with the partner in future
   - Any special circumstance that should be remembered to run this hub correctly next time
3. If anything notable occurred, draft a brief summary and **ask the user to confirm** it is accurate or if they want to add or change anything.
4. Once confirmed, append to `~/.claude/memory/hub-ingest-memory.md` under the hub's section (create the section if it doesn't exist):
   - **Persistent Notes** — for things to always surface next run (config, quirks, endpoint paths, partner context)
   - **Run Log** — for timestamped noteworthy events (use today's date)

**Memory file format:**
```markdown
## hub-name

### Persistent Notes
- Endpoint changed to `s3://new-bucket/path/` as of 2026-03
- API rate-limits at ~100 req/min; harvest takes ~45 min
- Partner prefers notifications to alternate-contact@example.org

### Run Log
- **2026-03-26**: First successful S3 file harvest. Confirmed OaiFileHarvester supports .xml.gz format.
- **2026-02-15**: Mapping error in subject field resolved by updating mapper to handle nested arrays.
```

**Do not create a memory entry for routine runs where nothing new happened.**

---

### Manual Pipeline Steps (Fallback / Debugging Only)

Use these only if `ingest.sh` is unavailable or you need to re-run a single step in isolation.

#### Failure Notification Pattern

If any step fails, post to Slack **before stopping**. Do NOT stop the EC2 — logs are still needed.

```bash
source ~/.claude/secrets/dpla.env
curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"C02HEU2L3\",\"text\":\":x: *<hub> <step> FAILED* | check \`/home/ec2-user/data/<hub>-<step>.log\`\"}"
```

#### Run Harvest (Manual)

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx15g sbt \"runMain dpla.ingestion3.entries.ingest.HarvestEntry \
    --output /home/ec2-user/data/ \
    --conf /home/ec2-user/ingestion3-conf/i3.conf \
    --name <hub> \
    --sparkMaster local[*]\" \
  > /home/ec2-user/data/<hub>-harvest.log 2>&1 && echo HARVEST_SUCCESS || echo HARVEST_FAILED
"
```

Use `--timeout-seconds 7200` on the SSM send-command (harvests can take 20–90+ minutes depending on hub size).

**Poll** until Status is `Success`, then check the output for `HARVEST_SUCCESS`. If `HARVEST_FAILED`: post failure notification (see above) and stop.

After completion, capture the harvest output timestamp in one command:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/harvest/ | head -1"
```

This returns the most recent directory name (format: `YYYYMMDD_HHMMSS-<hub>-OriginalRecord.avro`). Save this as `HARVEST_TIMESTAMP`.

#### Run Mapping (Manual)

Use `MappingEntry` — mapping only, no partner email. The partner summary email is sent separately in Step 9.5, **after** the safety check passes. Do NOT use `IngestRemap` here — it fires the partner email immediately during mapping, before S3 sync and the safety check.

Use the harvest timestamp from Step 4:

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx12g sbt \"runMain dpla.ingestion3.entries.ingest.MappingEntry \
    --output /home/ec2-user/data/ \
    --conf /home/ec2-user/ingestion3-conf/i3.conf \
    --name <hub> \
    --input /home/ec2-user/data/<hub>/harvest/<HARVEST_TIMESTAMP>/ \
    --sparkMaster local[*]\" \
  > /home/ec2-user/data/<hub>-remap.log 2>&1 && echo REMAP_SUCCESS || echo REMAP_FAILED
"
```

If `REMAP_FAILED`: post failure notification (see above) and stop.

After completion, capture the mapping output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/mapping/ | head -1"
```

Save this as `MAPPING_TIMESTAMP`.

#### Run Enrichment (Manual)

Use the mapping timestamp from Step 5:

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx18g sbt \"runMain dpla.ingestion3.entries.ingest.EnrichEntry \
    --output /home/ec2-user/data/ \
    --conf /home/ec2-user/ingestion3-conf/i3.conf \
    --name <hub> \
    --input /home/ec2-user/data/<hub>/mapping/<MAPPING_TIMESTAMP>/ \
    --sparkMaster local[*]\" \
  > /home/ec2-user/data/<hub>-enrich.log 2>&1 && echo ENRICH_SUCCESS || echo ENRICH_FAILED
"
```

If `ENRICH_FAILED`: post failure notification (see above) and stop.

After completion, capture the enrichment output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/enrichment/ | head -1"
```

Save this as `ENRICH_TIMESTAMP`.

#### Run JSONL Export (Manual)

Use the enrichment timestamp from Step 6:

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx12g sbt \"runMain dpla.ingestion3.entries.ingest.JsonlEntry \
    --output /home/ec2-user/data/ \
    --conf /home/ec2-user/ingestion3-conf/i3.conf \
    --name <hub> \
    --input /home/ec2-user/data/<hub>/enrichment/<ENRICH_TIMESTAMP>/ \
    --sparkMaster local[1]\" \
  > /home/ec2-user/data/<hub>-jsonl.log 2>&1 && echo JSONL_SUCCESS || echo JSONL_FAILED
"
```

Note `local[1]` (single thread) — this is intentional for JSONL export.

Use `--timeout-seconds 3600` on the SSM send-command (JSONL export can take 5–30 minutes).

If `JSONL_FAILED`: post failure notification (see above) and stop.

After completion, capture the JSONL output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/jsonl/ | head -1"
```

Save this as `JSONL_TIMESTAMP`.

#### Sync to S3 (Manual)

Use the JSONL timestamp from Step 7:

```bash
sudo -u ec2-user bash -lc "
  aws s3 sync \
    /home/ec2-user/data/<hub>/jsonl/<JSONL_TIMESTAMP>/ \
    s3://dpla-master-dataset/<hub>/jsonl/<JSONL_TIMESTAMP>/ \
  && echo SYNC_SUCCESS || echo SYNC_FAILED
"
```

Use `--timeout-seconds 3600` on the SSM send-command (S3 sync is usually fast but can take 10–20 minutes for large hubs).

This adds a new timestamped snapshot. It does **not** overwrite or delete any prior snapshots. If `SYNC_FAILED`: post failure notification (see above) and stop.

#### Verify Results (Manual)

Check that the new snapshot appears in S3 and run an automated safety check:

```bash
# List all snapshots — new one should appear at the bottom
aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ | sort | tail -5
```

Then get the two most recent snapshot names and run the safety check using record counts from `_MANIFEST`:

```bash
NEW_SNAP=<JSONL_TIMESTAMP>
PREV_SNAP=$(aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ \
  | awk '{print $NF}' | sed 's|/||g' | sort | tail -2 | head -1)

NEW_COUNT=$(aws s3 cp s3://dpla-master-dataset/<hub>/jsonl/${NEW_SNAP}/_MANIFEST - \
  | grep "^Record count:" | awk '{print $NF}')
PREV_COUNT=$(aws s3 cp s3://dpla-master-dataset/<hub>/jsonl/${PREV_SNAP}/_MANIFEST - \
  | grep "^Record count:" | awk '{print $NF}')

python3 -c "
new, prev = ${NEW_COUNT:-0}, ${PREV_COUNT:-0}
drop = (prev - new) / prev * 100 if prev else 0
print(f'New: {new:,} records | Prev: {prev:,} records | Change: {drop:+.1f}%')
if drop > 5:
    print('WARNING: >5% record drop — STOP and investigate before proceeding.')
else:
    print('OK: record count within acceptable range.')
"
```

**If the check prints WARNING**: do NOT stop the EC2. Do NOT proceed to Step 9.5 — the partner email must not be sent for a failed/suspect ingest. Post to Slack with the counts, then wait for direction from the user:

```bash
source ~/.claude/secrets/dpla.env
curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"C02HEU2L3\",\"text\":\":warning: *<hub> safety check FAILED* — >5% record drop\nNew: ${NEW_COUNT} | Prev: ${PREV_COUNT}\nNew snapshot: \`${NEW_SNAP}\`\nPartner email suppressed. Investigating.\"}"
```

#### Send Partner Summary Email (Manual)

Only run this if the safety check in Step 9 passed (printed `OK`). This sends the mapping summary and logs to the hub contact via AWS SES, from `DPLA Bot <tech@dp.la>`.

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx4g sbt \"runMain dpla.ingestion3.utils.Emailer \
    /home/ec2-user/data/<hub>/mapping/<MAPPING_TIMESTAMP>/ \
    <hub> \
    /home/ec2-user/ingestion3-conf/i3.conf\" \
  > /home/ec2-user/data/<hub>-email.log 2>&1 && echo EMAIL_SUCCESS || echo EMAIL_FAILED
"
```

#### Stop EC2 (Manual)

Only after verification is complete and results look good:

```bash
aws ec2 stop-instances --instance-ids i-0a0def8581efef783
```

#### Notify via Slack (Manual)

Post a completion summary to Slack #tech-alerts:

```bash
source ~/.claude/secrets/dpla.env

# Read record counts from new and previous snapshots
NEW_SNAP=<JSONL_TIMESTAMP>
PREV_SNAP=$(aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ \
  | awk '{print $NF}' | sed 's|/||g' | sort | tail -2 | head -1)

NEW_COUNT=$(aws s3 cp s3://dpla-master-dataset/<hub>/jsonl/${NEW_SNAP}/_MANIFEST - \
  | grep "^Record count:" | awk '{print $NF}')
PREV_COUNT=$(aws s3 cp s3://dpla-master-dataset/<hub>/jsonl/${PREV_SNAP}/_MANIFEST - \
  | grep "^Record count:" | awk '{print $NF}')

TOTAL_SIZE=$(aws s3 ls --summarize --recursive \
  s3://dpla-master-dataset/<hub>/jsonl/${NEW_SNAP}/ \
  | grep "Total Size" | awk '{print $NF}')

DELTA_LINE=$(python3 -c "
new, prev = ${NEW_COUNT:-0}, ${PREV_COUNT:-0}
total_mb = ${TOTAL_SIZE:-0} / 1_048_576
delta = new - prev
sign = '+' if delta >= 0 else ''
print(f'Records: {new:,} ({sign}{delta:,} vs prev) | Size: {total_mb:.1f} MB')
")

curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"C02HEU2L3\",\"text\":\"*<hub> ingest complete* :white_check_mark:\nNew snapshot: \`${NEW_SNAP}\`\n${DELTA_LINE}\nS3: \`s3://dpla-master-dataset/<hub>/jsonl/${NEW_SNAP}/\`\"}"
```

## Batch Mode

Hubs **cannot run in parallel** on this instance — two enrichment steps alone would require 36 GB, exceeding the 32 GB RAM ceiling. Always run sequentially.

### Step B1: Derive Hub List

When the user says "run [month] ingests" (e.g. "run February ingests", "run March ingests"), use this script to derive the list from `i3.conf`. It skips on-hold hubs, flags maryland and file-type hubs, and excludes `nara` (complex delta format, manual only).

```python
#!/usr/bin/env python3
import os, re, sys
from datetime import datetime

CONF = os.environ["I3_CONF"]
EC2_BLOCKED_HUBS = {"maryland", "getty"}
MONTH_NAMES = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

arg = sys.argv[1].lower() if len(sys.argv) > 1 else str(datetime.now().month)
month = int(arg) if arg.isdigit() else MONTH_NAMES.get(arg, datetime.now().month)

conf = open(CONF).read()
hubs = {}
for line in conf.splitlines():
    m = re.match(r'^([\w-]+)\.harvest\.type\s*=\s*"([^"]+)"', line)
    if m:
        hubs[m.group(1)] = {"type": m.group(2), "months": [], "status": None}
for line in conf.splitlines():
    m = re.match(r'^([\w-]+)\.schedule\.months\s*=\s*\[([^\]]*)\]', line)
    if m and m.group(1) in hubs:
        hubs[m.group(1)]["months"] = [int(x) for x in m.group(2).split(",") if x.strip()]
    m = re.match(r'^([\w-]+)\.schedule\.status\s*=\s*"([^"]+)"', line)
    if m and m.group(1) in hubs:
        hubs[m.group(1)]["status"] = m.group(2)

print(f"\nHubs scheduled for month {month}:\n")
runnable = []
for hub, info in sorted(hubs.items()):
    if month not in info["months"]:
        continue
    if info["status"] and "on-hold" in info["status"]:
        print(f"  SKIP {hub:25s} [on-hold: {info['status']}]")
        continue
    if info["type"] == "nara.file.delta":
        print(f"  SKIP {hub:25s} [nara delta — manual only]")
        continue
    if hub in EC2_BLOCKED_HUBS:
        print(f"  WARN {hub:25s} [blocked from EC2 — run locally]")
        continue
    if info["type"] == "file":
        if hub == "community-webs":
            print(f"  CW   {hub:25s} [DB export pre-processing needed on EC2 — see Step CW]")
        else:
            print(f"  WARN {hub:25s} [file harvest — verify files staged on EC2 first]")
        runnable.append(hub)
        continue
    print(f"  OK   {hub:25s} [{info['type']}]")
    runnable.append(hub)

print(f"\nEC2-runnable: {' '.join(runnable)}")
```

Run as: `python3 hub-list.py february` (or `python3 hub-list.py 2`). Review the output, confirm with the user, then proceed.

### Step B2: Choose Orchestration Mode

**Short run (≤ ~4 hours total):** Orchestrate directly — run Steps 4–11 in sequence, Claude stays active and posts a Slack notification on completion.

**Long run (estimated > 4 hours, OR 6+ hubs):** Write a self-contained script to the EC2 and launch it in the background. Claude's job is setup and launch; the script handles the rest and posts its own Slack updates. This applies to:
- Any single large hub (e.g. **minnesota ~1.1M records ≈ 8–9 hours**, smithsonian, ia, hathi)
- Multi-hub batches of 6+ hubs

**Slack channel for background scripts:**
- **#tech-alerts** (`C02HEU2L3`): use `DPLA_SLACK_BOT_TOKEN` — embed it directly in the script (preferred for single-hub runs)
- **#network**: use `SLACK_WEBHOOK` from the EC2's `.env` — already available without embedding tokens (convenient for multi-hub batch runs)

### Step B3: Long Run — Write and Launch Script

There are two script templates depending on whether this is a single large hub or a multi-hub batch.

#### Write the script to a local temp file, then upload via base64

**Use a quoted heredoc (`<< 'SCRIPTEOF'`) to write the script, then inject the token with `sed`.** Do NOT pass Python code through `python3 -c "..."` to generate scripts — bash processes the double-quoted argument first and strips `\"` → `"`, breaking any JSON in the script's curl calls.

```bash
cat > /tmp/<hub>-ingest.sh << 'SCRIPTEOF'
#!/bin/bash -l
set -euo pipefail
...
SLACK_TOKEN="__TOKEN__"
...
SCRIPTEOF

TOKEN=$(python3 -c "
import re, os
with open(os.path.expanduser('~/.claude/secrets/dpla.env')) as f:
    content = f.read()
token = re.search(r\"DPLA_SLACK_BOT_TOKEN='?([^'\\n]+)'?\", content).group(1).strip(\"'\")
print(token)
")
sed -i '' "s/__TOKEN__/$TOKEN/" /tmp/<hub>-ingest.sh
echo "Script written."
```

**Why `<< 'SCRIPTEOF'` is safe:** Single-quoting the heredoc delimiter prevents all bash expansion in the body. The prior concern about heredocs (backslash stripping) only applies to *unquoted* heredocs in zsh.

Then upload via base64:
```bash
SCRIPT_B64=$(base64 < /tmp/<hub>-ingest.sh)
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 60 \
  --parameters "{\"commands\":[\"echo '${SCRIPT_B64}' | base64 -d > /home/ec2-user/<hub>-ingest.sh && chmod +x /home/ec2-user/<hub>-ingest.sh && echo WRITE_OK\"]}" \
  --query 'Command.CommandId' --output text)
# Poll until Status=Success, verify output contains WRITE_OK
```

#### Single-hub script template (posts to #tech-alerts)

Use this for large single hubs (minnesota, smithsonian, ia, hathi, etc.). The `__SLACK_TOKEN__` placeholder is replaced by the `sed` step in the write block above.

```bash
#!/bin/bash -l
set -euo pipefail

HUB="<hub>"
DATA=/home/ec2-user/data
I3=/home/ec2-user/ingestion3
CONF=/home/ec2-user/ingestion3-conf/i3.conf
LOG=$DATA/<hub>-ingest.log
SLACK_TOKEN="__SLACK_TOKEN__"
CHANNEL="C02HEU2L3"

slack() {
  local payload
  payload=$(python3 -c "import json,sys; print(json.dumps({'channel':'$CHANNEL','text':sys.argv[1]}))" "$1")
  curl -s -X POST "https://slack.com/api/chat.postMessage" \
    -H "Authorization: Bearer $SLACK_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload" > /dev/null
}
log() { echo "[$(date -u +%H:%M:%SZ)] $1" >> $LOG; }
BT=$(printf '\x60')

log "=== $HUB ingest starting ==="
slack ":arrow_forward: *$HUB ingest started* | harvest -> map -> enrich -> jsonl -> s3"

cd $I3

# Harvest
log "--- Harvest starting ---"
SBT_OPTS=-Xmx15g sbt "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
  --output $DATA/ --conf $CONF --name $HUB --sparkMaster local[*]" \
  >> $LOG 2>&1 && log "Harvest OK" || { log "Harvest FAILED"; slack ":x: *$HUB harvest FAILED* | check ${BT}$LOG${BT}"; exit 1; }
HARVEST_TS=$(ls -t $DATA/$HUB/harvest/ | head -1)
slack ":white_check_mark: *$HUB harvest complete* | ${BT}$HARVEST_TS${BT}"

# Mapping (MappingEntry only — partner email sent after safety check, not here)
log "--- Mapping starting ---"
SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.MappingEntry \
  --output $DATA/ --conf $CONF --name $HUB \
  --input $DATA/$HUB/harvest/$HARVEST_TS/ --sparkMaster local[*]" \
  >> $LOG 2>&1 && log "Mapping OK" || { log "Mapping FAILED"; slack ":x: *$HUB mapping FAILED* | check ${BT}$LOG${BT}"; exit 1; }
MAP_TS=$(ls -t $DATA/$HUB/mapping/ | head -1)
slack ":white_check_mark: *$HUB mapping complete* | ${BT}$MAP_TS${BT}"

# Enrichment
log "--- Enrichment starting ---"
SBT_OPTS=-Xmx18g sbt "runMain dpla.ingestion3.entries.ingest.EnrichEntry \
  --output $DATA/ --conf $CONF --name $HUB \
  --input $DATA/$HUB/mapping/$MAP_TS/ --sparkMaster local[*]" \
  >> $LOG 2>&1 && log "Enrichment OK" || { log "Enrichment FAILED"; slack ":x: *$HUB enrichment FAILED* | check ${BT}$LOG${BT}"; exit 1; }
ENRICH_TS=$(ls -t $DATA/$HUB/enrichment/ | head -1)
slack ":white_check_mark: *$HUB enrichment complete* | ${BT}$ENRICH_TS${BT}"

# JSONL
log "--- JSONL export starting ---"
SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.JsonlEntry \
  --output $DATA/ --conf $CONF --name $HUB \
  --input $DATA/$HUB/enrichment/$ENRICH_TS/ --sparkMaster local[1]" \
  >> $LOG 2>&1 && log "JSONL OK" || { log "JSONL FAILED"; slack ":x: *$HUB JSONL FAILED* | check ${BT}$LOG${BT}"; exit 1; }
JSONL_TS=$(ls -t $DATA/$HUB/jsonl/ | head -1)
slack ":white_check_mark: *$HUB JSONL complete* | ${BT}$JSONL_TS${BT}"

# S3 sync
log "--- S3 sync starting ---"
aws s3 sync $DATA/$HUB/jsonl/$JSONL_TS/ s3://dpla-master-dataset/$HUB/jsonl/$JSONL_TS/ \
  >> $LOG 2>&1 && log "S3 sync OK" || { log "S3 sync FAILED"; slack ":x: *$HUB S3 sync FAILED*"; exit 1; }

# Safety check — compare new vs previous record count
PREV_SNAP=$(aws s3 ls s3://dpla-master-dataset/$HUB/jsonl/ \
  | awk '{print $NF}' | sed 's|/||g' | sort | tail -2 | head -1)
NEW_COUNT=$(aws s3 cp s3://dpla-master-dataset/$HUB/jsonl/${JSONL_TS}/_MANIFEST - 2>/dev/null \
  | grep "^Record count:" | awk '{print $NF}')
PREV_COUNT=$(aws s3 cp s3://dpla-master-dataset/$HUB/jsonl/${PREV_SNAP}/_MANIFEST - 2>/dev/null \
  | grep "^Record count:" | awk '{print $NF}')
TOTAL_SIZE=$(aws s3 ls --summarize --recursive s3://dpla-master-dataset/$HUB/jsonl/${JSONL_TS}/ \
  | grep "Total Size" | awk '{print $NF}')
SAFETY=$(python3 -c "
new, prev = ${NEW_COUNT:-0}, ${PREV_COUNT:-0}
total_mb = ${TOTAL_SIZE:-0} / 1_048_576
delta = new - prev
sign = '+' if delta >= 0 else ''
drop = (prev - new) / prev * 100 if prev else 0
print(f'Records: {new:,} ({sign}{delta:,} vs prev) | Size: {total_mb:.1f} MB')
print('SAFETY_FAIL' if drop > 5 else 'SAFETY_OK')
")
DELTA_LINE=$(echo "$SAFETY" | head -1)
SAFETY_STATUS=$(echo "$SAFETY" | tail -1)
log "$DELTA_LINE ($SAFETY_STATUS)"

if [ "$SAFETY_STATUS" = "SAFETY_FAIL" ]; then
  log "Safety check FAILED — partner email suppressed"
  slack ":warning: *$HUB safety check FAILED* — >5% record drop\n${DELTA_LINE}\nNew snapshot: ${BT}${JSONL_TS}${BT}\nPartner email suppressed. Investigate before proceeding."
  exit 1
fi

# Partner summary email (only after safety check passes)
log "--- Sending partner summary email ---"
SBT_OPTS=-Xmx4g sbt "runMain dpla.ingestion3.utils.Emailer \
  $DATA/$HUB/mapping/$MAP_TS/ $HUB $CONF" \
  >> $LOG 2>&1 && log "Partner email sent" || log "Partner email FAILED (non-fatal)"

# Final Slack summary
slack "*$HUB ingest complete* :white_check_mark:\nNew snapshot: ${BT}${JSONL_TS}${BT}\n${DELTA_LINE}\nS3: ${BT}s3://dpla-master-dataset/$HUB/jsonl/${JSONL_TS}/${BT}"
```

#### Multi-hub batch script template (posts to #network via SLACK_WEBHOOK)

Use this for 6+ hub batch runs. The `SLACK_WEBHOOK` is already in `/home/ec2-user/ingestion3/.env` and posts to #network.

```bash
#!/bin/bash -l
source /home/ec2-user/ingestion3/.env
HUBS="$*"
LOG=/home/ec2-user/batch-ingest.log
I3=/home/ec2-user/ingestion3
DATA=/home/ec2-user/data
CONF=/home/ec2-user/ingestion3-conf/i3.conf

slack() {
  local payload
  payload=$(python3 -c "import json,sys; print(json.dumps({'text':sys.argv[1]}))" "$1")
  curl -s -X POST "$SLACK_WEBHOOK" -H "Content-Type: application/json" -d "$payload" > /dev/null
}
log()   { echo "[$(date -u +%H:%M:%SZ)] $1" >> $LOG; }
BT=$(printf '\x60')

PASSED=(); FAILED=()
cd $I3

for HUB in $HUBS; do
  log "=== Starting $HUB ==="

  SBT_OPTS=-Xmx15g sbt "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
    --output $DATA/ --conf $CONF --name $HUB --sparkMaster local[*]" \
    >> $LOG 2>&1 && log "$HUB harvest OK" || { log "$HUB harvest FAILED"; FAILED+=("$HUB/harvest"); slack ":x: *$HUB harvest FAILED*"; continue; }
  HARVEST_TS=$(ls -t $DATA/$HUB/harvest/ | head -1)

  # Mapping (MappingEntry only — partner email sent after safety check)
  SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.MappingEntry \
    --output $DATA/ --conf $CONF --name $HUB --input $DATA/$HUB/harvest/$HARVEST_TS/ --sparkMaster local[*]" \
    >> $LOG 2>&1 && log "$HUB mapping OK" || { log "$HUB mapping FAILED"; FAILED+=("$HUB/mapping"); slack ":x: *$HUB mapping FAILED*"; continue; }
  MAP_TS=$(ls -t $DATA/$HUB/mapping/ | head -1)

  SBT_OPTS=-Xmx18g sbt "runMain dpla.ingestion3.entries.ingest.EnrichEntry \
    --output $DATA/ --conf $CONF --name $HUB --input $DATA/$HUB/mapping/$MAP_TS/ --sparkMaster local[*]" \
    >> $LOG 2>&1 && log "$HUB enrichment OK" || { log "$HUB enrichment FAILED"; FAILED+=("$HUB/enrichment"); slack ":x: *$HUB enrichment FAILED*"; continue; }
  ENRICH_TS=$(ls -t $DATA/$HUB/enrichment/ | head -1)

  SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.JsonlEntry \
    --output $DATA/ --conf $CONF --name $HUB --input $DATA/$HUB/enrichment/$ENRICH_TS/ --sparkMaster local[1]" \
    >> $LOG 2>&1 && log "$HUB jsonl OK" || { log "$HUB jsonl FAILED"; FAILED+=("$HUB/jsonl"); slack ":x: *$HUB JSONL FAILED*"; continue; }
  JSONL_TS=$(ls -t $DATA/$HUB/jsonl/ | head -1)

  aws s3 sync $DATA/$HUB/jsonl/$JSONL_TS/ s3://dpla-master-dataset/$HUB/jsonl/$JSONL_TS/ \
    >> $LOG 2>&1 && log "$HUB S3 sync OK" || { log "$HUB S3 sync FAILED"; FAILED+=("$HUB/s3"); slack ":x: *$HUB S3 sync FAILED*"; continue; }

  # Safety check
  PREV_SNAP=$(aws s3 ls s3://dpla-master-dataset/$HUB/jsonl/ \
    | awk '{print $NF}' | sed 's|/||g' | sort | tail -2 | head -1)
  NEW_COUNT=$(aws s3 cp s3://dpla-master-dataset/$HUB/jsonl/$JSONL_TS/_MANIFEST - 2>/dev/null | grep "^Record count:" | awk '{print $NF}')
  PREV_COUNT=$(aws s3 cp s3://dpla-master-dataset/$HUB/jsonl/$PREV_SNAP/_MANIFEST - 2>/dev/null | grep "^Record count:" | awk '{print $NF}')
  SAFETY_STATUS=$(python3 -c "
new, prev = ${NEW_COUNT:-0}, ${PREV_COUNT:-0}
drop = (prev - new) / prev * 100 if prev else 0
print('SAFETY_FAIL' if drop > 5 else 'SAFETY_OK')
")
  if [ "$SAFETY_STATUS" = "SAFETY_FAIL" ]; then
    log "$HUB safety check FAILED — partner email suppressed"
    FAILED+=("$HUB/safety-check")
    slack ":warning: *$HUB safety check FAILED* — >5% record drop. Partner email suppressed."
    continue
  fi

  # Partner summary email (only after safety check passes)
  SBT_OPTS=-Xmx4g sbt "runMain dpla.ingestion3.utils.Emailer \
    $DATA/$HUB/mapping/$MAP_TS/ $HUB $CONF" \
    >> $LOG 2>&1 && log "$HUB partner email sent" || log "$HUB partner email FAILED (non-fatal)"

  log "$HUB complete — $NEW_COUNT records"
  PASSED+=("$HUB ($NEW_COUNT records)")
  slack ":white_check_mark: *$HUB ingest complete* — ${NEW_COUNT} records | ${BT}$JSONL_TS${BT}"
done

PASS_STR=$(IFS=", "; echo "${PASSED[*]}")
FAIL_STR=$(IFS=", "; echo "${FAILED[*]}")
log "=== Batch done. Passed: ${#PASSED[@]}  Failed: ${#FAILED[@]} ==="
MSG="*Batch ingest complete* :checkered_flag:\n:white_check_mark: ${#PASSED[@]} passed: $PASS_STR"
[ ${#FAILED[@]} -gt 0 ] && MSG="$MSG\n:x: ${#FAILED[@]} failed: $FAIL_STR"
slack "$MSG"
```

#### Launch the script (nohup, runs in background)

**Use `python3` to generate the `--parameters` JSON** to avoid "Error parsing parameter" from shell metacharacters (`$!`, backticks, nested quotes all cause this error):

```bash
SCRIPT_NAME="<hub>-ingest.sh"   # or batch-ingest.sh
# bash expands ${SCRIPT_NAME} before python3 sees the string — no need to pass it as an argument
LAUNCH_PARAMS=$(python3 -c "
import json
cmd = 'sudo -u ec2-user bash -lc \"nohup /home/ec2-user/${SCRIPT_NAME} > /dev/null 2>&1 &\"'
print(json.dumps({'commands': [cmd]}))
")

CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 172800 \
  --parameters "$LAUNCH_PARAMS" \
  --query 'Command.CommandId' --output text)
# This command returns quickly — the script runs in the background
```

**Why `> /dev/null`?** The script writes its own log via `$LOG` — the outer redirect is just discarded. **Do NOT redirect to a log file in the nohup launch** (e.g. `> /home/ec2-user/data/my.log`) — SSM runs as ssm-user (root), which creates the file owned by root. The ec2-user script then can't append to it and fails immediately with `tee: permission denied`. If you need an outer redirect, pre-create the file as ec2-user first, or wrap the entire nohup inside `sudo -u ec2-user bash -lc "..."`.

**Do NOT try to capture `$!`** — the SSM command returns before the background process starts, so `$!` is empty or unavailable. Including `$!` in the command string also causes "Error parsing parameter: Invalid JSON: Invalid \escape" from the AWS CLI JSON parser.

**After launching, do TWO verification checks:**

**Check 1 — immediate (10s):** Confirm the script launched and logged its start:
```bash
PARAMS=$(python3 -c "import json; print(json.dumps({'commands': ['tail -5 /home/ec2-user/data/<hub>-ingest.log']}))")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 30 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
# Poll after 8s — look for "=== <hub> ingest starting ===" in output
```
Expected: the starting log message. If the log is empty or doesn't exist, the script failed to launch — alert immediately.

**Check 2 — after 5 minutes:** Confirm the harvest has actually started (sbt/java are running and the log is growing). If the script died early (e.g. due to a broken slack function, missing dependency, or set -e trigger), it will have exited before logging "--- Harvest starting ---". Do NOT skip this check — a silent early exit is indistinguishable from a running harvest without it.

```bash
sleep 300
PARAMS=$(python3 -c "import json; print(json.dumps({'commands': ['tail -10 /home/ec2-user/data/<hub>-ingest.log && ps aux | grep -E \"sbt|java\" | grep -v grep | wc -l']}))")
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 30 \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)
# Poll after 8s
```

Expected: log contains "--- Harvest starting ---" or later steps, AND at least one sbt/java process is running.

**If the log still only shows the starting line and no java processes are running**, the script died silently (likely due to a broken slack function or other early failure). Post a fallback Slack alert locally and investigate:

```bash
source ~/.claude/secrets/dpla.env
MSG_JSON=$(python3 -c "import json; print(json.dumps({'channel':'C02HEU2L3','text':':x: *<hub> ingest script died silently* — no harvest activity after 5 min. Check /home/ec2-user/data/<hub>-ingest.log on EC2.'}))")
curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$MSG_JSON"
```

### Step B4: Monitor Progress

At any time, check the log:

```bash
CMDID=$(aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --timeout-seconds 30 \
  --parameters '{"commands":["sudo -u ec2-user tail -30 /home/ec2-user/batch-ingest.log"]}' \
  --query 'Command.CommandId' --output text)
# Poll then read StandardOutputContent
```

### Step B5: Stop EC2 After Batch

The batch script does **not** stop the EC2 — do that manually after the Slack summary arrives:

```bash
aws ec2 stop-instances --instance-ids i-0a0def8581efef783
```

## SBT Memory Settings Reference

| Step | SBT_OPTS | Spark Master | Notes |
|------|----------|-------------|-------|
| Harvest | `-Xmx15g` | `local[*]` | |
| Mapping | `-Xmx12g` | `local[*]` | |
| Enrichment | `-Xmx18g` | `local[*]` | Highest memory step |
| JSONL | `-Xmx12g` | `local[1]` | Single thread intentional |

## Typical Run Times

Scale is roughly linear with record count. Use SD as a baseline for api/MDL hubs.

### SD (~96k records)

| Step | Time |
|------|------|
| Harvest (api/MDL) | ~25 min |
| Mapping | ~3 min |
| Enrichment | ~10 min |
| JSONL | ~8 min |
| S3 sync | ~1 min |
| **Total** | **~47 min** |

### Minnesota (~1.1M records — 11× SD)

| Step | Estimated Time |
|------|----------------|
| Harvest (api/MDL) | ~4.5–5 hours |
| Mapping | ~30 min |
| Enrichment | ~1.5–2 hours |
| JSONL | ~1–1.5 hours |
| S3 sync | ~10–15 min |
| **Total** | **~8–9 hours** |

Minnesota is the largest MDL hub (full collection minus SD). Always use the background script for it.

Smaller hubs (file, localoai with small collections) will be faster than SD. Other large hubs (smithsonian, ia, hathi) may be comparable to or larger than Minnesota.

## Error Handling

### Harvest Failure
1. Check the log: `tail -30 /home/ec2-user/data/<hub>-harvest.log`
2. For `localoai`: verify OAI endpoint is up and reachable from EC2
3. For `api`: verify the API endpoint with a longer curl timeout (60s+)
4. For maryland and getty: EC2 harvest is blocked — run harvest locally instead
5. **Do NOT stop the EC2 instance** until investigated

### Mapping / Enrichment / JSONL Failure
1. Check the log: `tail -30 /home/ec2-user/data/<hub>-<step>.log`
2. Look for `OutOfMemoryError` → increase SBT_OPTS heap
3. Look for `_SUCCESS` files inside output dirs — missing means the stage failed mid-run
4. Stages can be re-run individually with explicit input timestamps — no need to re-harvest
5. **Do NOT stop the EC2 instance** until resolved

### Duplicate Output Directories
Occasionally a step produces two output directories (e.g. from a prior failed run). **Always use the most recent timestamp** (sorted lexicographically, the last one). The older partial directory can be ignored.

### S3 Sync Failure
1. Verify AWS identity: `aws sts get-caller-identity`
2. Verify S3 access: `aws s3 ls s3://dpla-master-dataset/<hub>/`
3. Retry the sync command — it is idempotent
4. **Do NOT stop the EC2 instance** until sync succeeds

### General Rule
On **any** failure: do NOT stop the EC2 instance until the issue is investigated and either resolved or explicitly abandoned by the user.

## Fallback: Using .bashrc Shell Functions

The EC2's `.bashrc` defines simplified wrapper functions as an alternative to direct SBT invocations:

```bash
i3-harvest <hub>                         # harvest
i3-remap <hub>                           # map (input: entire harvest dir)
i3-enrich <hub> <mapping_timestamp_dir>  # enrich
i3-jsonl <hub> <enrichment_timestamp_dir># jsonl
sync-s3 <hub>                            # s3 sync (entire hub data dir)
```

Use these if direct SBT invocations fail or for quick manual runs. Note that `i3-remap` takes the whole harvest directory (not a specific timestamp) and `sync-s3` syncs the entire hub data directory to S3 (not just the latest JSONL snapshot).

## Future Improvements

The EC2 now has the latest ingestion3 (pulled via HTTPS from `https://github.com/dpla/ingestion3.git`), which includes `scripts/*.sh`. A potential simplification would be to replace the direct sbt invocations in this skill with `./scripts/ingest.sh <hub>` — but only after validating that script handles all the edge cases (memory flags, timestamp capture, safety check, email timing) the same way this skill does.

## Safety Rules

- Always verify record counts in Step 9. A drop >5% must be flagged before stopping.
- This ingest does **NOT** rebuild the Elasticsearch index or change what is live on dp.la.
- Keep the EC2 running until verification is complete, then stop to save cost.
- Do not modify `i3.conf`, `.env`, or Scala source code unless explicitly requested. Exception: for `community-webs`, Step CW4 updates `community-webs.harvest.endpoint` in `i3.conf` as part of the required ingest flow.
- Always refresh ingestion3 at the start of each session (Step 3a) using `git fetch + reset --hard` — the EC2 has no auto-update and can drift behind main, causing failures (e.g. missing entry points). Never use `git pull`; local commits can accumulate and cause divergent-branch failures.
- For multi-hub batches: run all harvests/ingests first, then request index rebuild separately.
- Memory system (Steps 0 and 6): Persistent notes are stored in `~/.claude/memory/hub-ingest-memory.md` outside the repository. Always review hub notes at Step 0 before proceeding — they may affect how the ingest should be run. Update the file after any notable run in Step 6. Do not create entries for routine successful runs.
