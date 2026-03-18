---
description: Run a full DPLA hub ingest (harvest → map → enrich → JSONL → S3 sync) on the ingest EC2 instance. Use when user says "ingest <hub>", "harvest <hub>", "run <hub> ingest", "<hub> hub ingest", "<hub> pipeline", "run <hub>", or "ingest all hubs".
allowedTools:
  - Bash(aws ec2 start-instances*)
  - Bash(aws ec2 stop-instances*)
  - Bash(aws ec2 describe-instances*)
  - Bash(aws ec2 wait*)
  - Bash(aws ssm send-command*)
  - Bash(aws ssm get-command-invocation*)
  - Bash(aws ssm describe-instance-information*)
  - Bash(aws s3 ls*)
  - Bash(aws s3 sync*)
  - Bash(aws s3api*)
  - Bash(aws sts get-caller-identity*)
  - Bash(curl*)
  - Bash(sleep*)
  - Bash(grep*)
  - Bash(python3*)
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

## Hub Configuration Reference

Hub config lives in `i3.conf` on the EC2 at `/home/ec2-user/ingestion3-conf/i3.conf`.

Before running, check the hub's harvest type:
```bash
grep "^<hub>\.harvest\.type" /Users/dominic/Documents/GitHub/ingestion3-conf/i3.conf
```

Key harvest types and their network requirements:
| Type | Notes |
|------|-------|
| `localoai` | OAI-PMH over HTTP/HTTPS. Most hubs. **CONTENTdm-hosted endpoints are blocked from EC2** (see below). |
| `api` | REST API (e.g. MDL/SD uses `metl.lib.umn.edu`). EC2-reachable. Slow to respond — use 60s+ curl timeout. |
| `file` | Pre-staged local file. Path in `harvest.endpoint` references `/Users/scott/...` — these were run locally by a previous operator. Requires the file to be present on the EC2 or staged to S3 first. |
| `nara.file.delta` | NARA-specific delta file format. Complex — consult README_NARA.md. |

### CONTENTdm Block (Important)

OCLC's CONTENTdm hosting infrastructure (`132.174.3.1`) **blocks connections from AWS IP ranges**. Affected hubs whose endpoints resolve to this IP cannot be harvested from the EC2:

- **maryland** (`collections.digitalmaryland.org`)
- **bpl** — verify before running
- **scdl** — verify before running
- **digitalnc** — verify before running

For these hubs, either:
1. Run the harvest locally on your Mac (2–3GB disk, 16GB+ RAM needed)
2. Contact the hub's IT team to allowlist DPLA's NAT IP: `52.2.32.179`

Always pre-flight test the endpoint from EC2 (see Step 3) before starting.

## Full Procedure

### Step 0: Identify the Hub

Confirm the hub key (e.g. `sd`, `maryland`, `indiana`) and check its config:

```bash
grep "^<hub>\." /Users/dominic/Documents/GitHub/ingestion3-conf/i3.conf
```

Note the `harvest.type` and `harvest.endpoint`. For `file` harvests, check that the file path exists and confirm with the user before proceeding.

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

Run these checks via SSM to confirm everything is ready:

```bash
# All in one SSM command — check Java, SBT, config, S3, disk
sudo -u ec2-user bash -lc "
  echo '=== Java ===' && java -version 2>&1
  echo '=== SBT ===' && sbt --version 2>&1 | tail -2
  echo '=== Hub config ===' && grep '^<hub>\.' /home/ec2-user/ingestion3-conf/i3.conf
  echo '=== Disk ===' && df -h / | tail -1
  echo '=== Prior S3 ingests ===' && aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ | tail -3
"
```

For `localoai` hubs, also test the endpoint from EC2:
```bash
curl -s --max-time 15 "<endpoint>?verb=Identify" | head -3
```

For `api` hubs, use a 60-second timeout:
```bash
curl -s --max-time 60 "<endpoint>?<query>&rows=1" | head -2
```

**If endpoint is unreachable from EC2 but reachable locally** → CONTENTdm block or firewall issue. Stop the instance and run the harvest locally instead.

### Step 4: Run Harvest

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

**Poll** until Status is `Success`, then check the output for `HARVEST_SUCCESS`.

After completion, capture the harvest output timestamp in one command:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/harvest/ | head -1"
```

This returns the most recent directory name (format: `YYYYMMDD_HHMMSS-<hub>-OriginalRecord.avro`). Save this as `HARVEST_TIMESTAMP`.

### Step 5: Run Mapping

Use the harvest timestamp from Step 4:

```bash
sudo -u ec2-user bash -lc "
  cd /home/ec2-user/ingestion3 &&
  SBT_OPTS=-Xmx12g sbt \"runMain dpla.ingestion3.entries.ingest.IngestRemap \
    --output /home/ec2-user/data/ \
    --conf /home/ec2-user/ingestion3-conf/i3.conf \
    --name <hub> \
    --input /home/ec2-user/data/<hub>/harvest/<HARVEST_TIMESTAMP>/ \
    --sparkMaster local[*]\" \
  > /home/ec2-user/data/<hub>-remap.log 2>&1 && echo REMAP_SUCCESS || echo REMAP_FAILED
"
```

After completion, capture the mapping output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/mapping/ | head -1"
```

Save this as `MAPPING_TIMESTAMP`.

### Step 6: Run Enrichment

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

After completion, capture the enrichment output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/enrichment/ | head -1"
```

Save this as `ENRICH_TIMESTAMP`.

### Step 7: Run JSONL Export

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

After completion, capture the JSONL output timestamp:
```bash
sudo -u ec2-user bash -lc "ls -t /home/ec2-user/data/<hub>/jsonl/ | head -1"
```

Save this as `JSONL_TIMESTAMP`.

### Step 8: Sync to S3

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

This adds a new timestamped snapshot. It does **not** overwrite or delete any prior snapshots.

### Step 9: Verify Results

Check that the new snapshot appears in S3 and run an automated safety check:

```bash
# List all snapshots — new one should appear at the bottom
aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ | sort | tail -5
```

Then get the two most recent snapshot names and run the safety check:

```bash
NEW_SNAP=<JSONL_TIMESTAMP>
PREV_SNAP=$(aws s3 ls s3://dpla-master-dataset/<hub>/jsonl/ \
  | awk '{print $NF}' | sed 's|/||g' | sort | tail -2 | head -1)

NEW_SIZE=$(aws s3 ls --summarize --recursive \
  s3://dpla-master-dataset/<hub>/jsonl/${NEW_SNAP}/ \
  | grep "Total Size" | awk '{print $NF}')
PREV_SIZE=$(aws s3 ls --summarize --recursive \
  s3://dpla-master-dataset/<hub>/jsonl/${PREV_SNAP}/ \
  | grep "Total Size" | awk '{print $NF}')

python3 -c "
new, prev = $NEW_SIZE, $PREV_SIZE
drop = (prev - new) / prev * 100 if prev else 0
print(f'New: {new:,} bytes | Prev: {prev:,} bytes | Change: {drop:+.1f}%')
if drop > 5:
    print('WARNING: >5% size drop — STOP and investigate before proceeding.')
else:
    print('OK: size within acceptable range.')
"
```

**If the check prints WARNING**: do NOT stop the EC2. Alert the user with the numbers and wait for direction.

### Step 10: Stop the EC2 Instance

Only after verification is complete and results look good:

```bash
aws ec2 stop-instances --instance-ids i-0a0def8581efef783
```

### Step 11: Notify via Slack

Post a completion summary to Slack #tech-alerts:

```bash
# Extract token directly — source fails due to spaces in other dpla.env values
DPLA_SLACK_BOT_TOKEN=$(grep '^DPLA_SLACK_BOT_TOKEN=' ~/.claude/secrets/dpla.env | cut -d'=' -f2-)

# Get object count and total size from the new snapshot
SUMMARY=$(aws s3 ls --summarize --recursive \
  s3://dpla-master-dataset/<hub>/jsonl/<JSONL_TIMESTAMP>/)
OBJ_COUNT=$(echo "$SUMMARY" | grep "Total Objects" | awk '{print $NF}')
TOTAL_SIZE=$(echo "$SUMMARY" | grep "Total Size" | awk '{print $NF}')
TOTAL_MB=$(python3 -c "print(f'{${TOTAL_SIZE:-0} / 1_048_576:.1f} MB')")

curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"C02HEU2L3\",\"text\":\"*<hub> ingest complete* :white_check_mark:\nNew snapshot: \`<JSONL_TIMESTAMP>\`\nFiles: ${OBJ_COUNT} | Size: ${TOTAL_MB}\nS3: \`s3://dpla-master-dataset/<hub>/jsonl/<JSONL_TIMESTAMP>/\`\"}"
```

## SBT Memory Settings Reference

| Step | SBT_OPTS | Spark Master | Notes |
|------|----------|-------------|-------|
| Harvest | `-Xmx15g` | `local[*]` | |
| Mapping | `-Xmx12g` | `local[*]` | |
| Enrichment | `-Xmx18g` | `local[*]` | Highest memory step |
| JSONL | `-Xmx12g` | `local[1]` | Single thread intentional |

## Typical Run Times (SD as baseline — ~96k records)

| Step | Time |
|------|------|
| Harvest (api/MDL) | ~25 min |
| Mapping | ~3 min |
| Enrichment | ~10 min |
| JSONL | ~8 min |
| S3 sync | ~1 min |
| **Total** | **~47 min** |

Smaller hubs (file, localoai with small collections) will be faster. Large hubs (smithsonian, ia, hathi) will be significantly longer.

## Error Handling

### Harvest Failure
1. Check the log: `tail -30 /home/ec2-user/data/<hub>-harvest.log`
2. For `localoai`: verify OAI endpoint is up and reachable from EC2
3. For `api`: verify the API endpoint with a longer curl timeout (60s+)
4. For CONTENTdm endpoints: if blocked from EC2, run harvest locally instead
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

The EC2 runs an older ingestion3 version without `scripts/*.sh`. Upgrading would add:
- Status tracking and automatic Slack notifications per step
- Better error handling with structured output
- `./scripts/ingest.sh <hub>` as a single command for the full pipeline

To upgrade: add a GitHub deploy key to the EC2, `git pull`, then update this skill to use `./scripts/ingest.sh <hub>`.

## Safety Rules

- Always verify record counts in Step 9. A drop >5% must be flagged before stopping.
- This ingest does **NOT** rebuild the Elasticsearch index or change what is live on dp.la.
- Keep the EC2 running until verification is complete, then stop to save cost.
- Do not modify `i3.conf`, `.env`, or code unless explicitly requested.
- Do not `git pull` or update code unless explicitly requested.
- For multi-hub batches: run all harvests/ingests first, then request index rebuild separately.
