---
description: Run a full Maryland hub ingest (harvest → map → enrich → JSONL → S3 sync) on the DPLA ingest EC2 instance. Use when user says "ingest maryland", "harvest maryland", "run maryland ingest", "maryland hub ingest", "maryland pipeline", or "run maryland".
---

# Maryland Hub Ingest

## Purpose

Orchestrate a full harvest-to-S3-sync ingest for the **Maryland** hub (`Digital Maryland`) on the DPLA ingest EC2 instance. This covers: starting the EC2 instance, running the pipeline via SSM commands (harvest → mapping → enrichment → JSONL → S3 sync), verifying results, and stopping the instance.

## When to Use

- "Ingest maryland"
- "Harvest maryland"
- "Run maryland ingest"
- "Maryland hub ingest"
- "Run the maryland pipeline"
- "Start maryland harvest"

## Maryland Hub Configuration

| Field | Value |
|-------|-------|
| Provider name | Digital Maryland |
| Hub key | `maryland` |
| Contact | Tracy Thompson <tthompson@prattlibrary.org> |
| Harvest type | `localoai` (OAI-PMH) |
| OAI endpoint | `http://collections.digitalmaryland.org/oai/oai.php` |
| Metadata prefix | `oai_qdc` (Qualified Dublin Core) |
| OAI verb | `ListRecords` |
| Schedule | Quarterly (March, June, September, December) |
| S3 destination | `s3://dpla-master-dataset/maryland/` |

## EC2 Ingest Instance

| Field | Value |
|-------|-------|
| Instance ID | `i-0a0def8581efef783` |
| Name | ingest |
| Type | m8g.2xlarge (8 vCPU, 32GB RAM, aarch64/Graviton) |
| OS | Amazon Linux 2023 |
| Private IP | 172.30.2.13 |
| VPC | vpc-b36ab3d6 |
| SSH user | `ec2-user` (NOT ubuntu) |
| SSH key | `~/.ssh/aws_jenkins.pem` |
| Bastion | 54.165.106.96 (Tailscale instance, same VPC) |

The instance is usually **stopped** to save costs. Start before use, stop when done.

## EC2 Environment Layout

All paths are under `/home/ec2-user/`:

| Path | Contents |
|------|----------|
| `ingestion3/` | Repo (main branch). Contains `sbt` launcher, `conf/i3.conf` (symlinked from ingestion3-conf). Java/SBT via `mise`. |
| `ingestion3-conf/` | Hub configuration (i3.conf) |
| `ingestion3/.env` | Environment variables (DPLA_DATA, I3_CONF, SLACK_WEBHOOK, JAVA_HOME) |
| `data/` | `$DPLA_DATA` — harvest/mapping/enrichment/jsonl output |
| `pyoaiharvester/` | OAI harvester tool |
| `.bashrc` | Shell functions: `i3-harvest`, `i3-remap`, `i3-enrich`, `i3-jsonl`, `sync-s3` |

**AWS access**: via EC2 instance role `ingestion3-spark` (no IAM keys needed).
**Java**: Temurin 11 via mise (`/home/ec2-user/.local/share/mise/installs/java/temurin-11.0.29+7`).
**SBT**: 1.11.7 via mise. The repo also bundles `./sbt` and `sbt-launch.jar` for direct use.

## Execution Method

All commands run remotely via `aws ssm send-command`. The pattern for every command is:

```bash
aws ssm send-command \
  --instance-ids i-0a0def8581efef783 \
  --document-name "AWS-RunShellScript" \
  --parameters '{"commands":["sudo -u ec2-user bash -lc \"<COMMAND>\""]}' \
  --output json --query 'Command.CommandId'
```

Then retrieve output:

```bash
aws ssm get-command-invocation \
  --command-id "<COMMAND_ID>" \
  --instance-id i-0a0def8581efef783 \
  --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
  --output json
```

**IMPORTANT**: Always use `sudo -u ec2-user bash -lc` to run as ec2-user with a login shell (activates mise for Java/SBT).

## Full Procedure

### Step 1: Pre-flight — Check OAI Endpoint

Before starting the EC2 instance, verify the OAI endpoint is responding (run locally):

```bash
curl -s --max-time 15 "http://collections.digitalmaryland.org/oai/oai.php?verb=Identify" | head -20
```

Should return XML with `<repositoryName>CONTENTdm Server Repository</repositoryName>`. If it fails, stop — the harvest will fail.

### Step 2: Start the EC2 Instance

```bash
aws ec2 start-instances --instance-ids i-0a0def8581efef783
aws ec2 wait instance-running --instance-ids i-0a0def8581efef783
```

Wait 30 seconds for SSM agent to start, then confirm SSM connectivity:

```bash
aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=i-0a0def8581efef783" \
  --query 'InstanceInformationList[0].PingStatus' --output text
```

Expected: `Online`. Retry after 15 seconds if not yet online.

### Step 3: Verify the Environment

Run via SSM (all as `ec2-user` login shell):

1. **Java**: `java -version` → must show 11+
2. **SBT**: `cd /home/ec2-user/ingestion3 && ./sbt --version` → must show 1.x
3. **i3.conf**: `cat /home/ec2-user/ingestion3-conf/i3.conf | grep ^maryland` → verify config present
4. **.env**: `cat /home/ec2-user/ingestion3/.env` → verify DPLA_DATA, I3_CONF, SLACK_WEBHOOK, JAVA_HOME
5. **AWS access**: `aws sts get-caller-identity` → should show role `ingestion3-spark`
6. **S3 access**: `aws s3 ls s3://dpla-master-dataset/maryland/jsonl/ | tail -3` → should list prior ingests
7. **Disk**: `df -h /home/ec2-user/data/` → need at least 10GB free
8. **OAI reachable from EC2**: `curl -s --max-time 15 "http://collections.digitalmaryland.org/oai/oai.php?verb=Identify" | head -5`

If .env is missing, create it:
```
DPLA_DATA=/home/ec2-user/data/
I3_CONF=/home/ec2-user/ingestion3-conf/i3.conf
SLACK_WEBHOOK=<webhook from local ~/.claude/secrets or ingestion3/.env>
JAVA_HOME=/home/ec2-user/.local/share/mise/installs/java/temurin-11.0.29+7
```

### Step 4: Run the Ingest Pipeline

The pipeline uses shell functions defined in ec2-user's `.bashrc`. Run each step via SSM.

**4a. Harvest** (OAI-PMH harvest — this is the longest step):

```bash
cd /home/ec2-user/ingestion3 && i3-harvest maryland
```

This runs: `SBT_OPTS=-Xmx15g sbt "runMain dpla.ingestion3.entries.ingest.HarvestEntry --output /home/ec2-user/data/ --conf /home/ec2-user/ingestion3/conf/i3.conf --name maryland --sparkMaster local[*]"`

**IMPORTANT**: This can take 10-60+ minutes. Use a long SSM command timeout or run in background. The SSM `send-command` default timeout is 3600s (1 hour). For longer harvests, set `--timeout-seconds 7200`.

After harvest, verify:
```bash
ls /home/ec2-user/data/maryland/harvest/
```
Should show a new timestamped directory. Check for `_SUCCESS` marker inside it.

**4b. Remap** (mapping + enrichment + JSONL in one step):

```bash
cd /home/ec2-user/ingestion3 && i3-remap maryland
```

This runs: `SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.IngestRemap --output /home/ec2-user/data/ --conf /home/ec2-user/ingestion3/conf/i3.conf --name maryland --input /home/ec2-user/data/maryland/harvest/ --sparkMaster local[*]"`

After remap, verify:
```bash
ls /home/ec2-user/data/maryland/mapping/
ls /home/ec2-user/data/maryland/enrichment/
ls /home/ec2-user/data/maryland/jsonl/
```

All should have new timestamped directories with `_SUCCESS` markers.

Check the mapping summary:
```bash
cat /home/ec2-user/data/maryland/mapping/*/_SUMMARY
```

**4c. S3 Sync** (upload JSONL to master dataset):

```bash
cd /home/ec2-user/ingestion3 && sync-s3 maryland
```

This runs: `aws s3 sync /home/ec2-user/data/maryland/ s3://dpla-master-dataset/maryland/`

### Step 5: Verify Results

**Check S3 for the new JSONL snapshot**:
```bash
aws s3 ls s3://dpla-master-dataset/maryland/jsonl/ | sort | tail -5
```

Verify a new timestamped folder appeared. The most recent should match today's date.

**Compare record counts** with the previous ingest. Check the sizes of the last two JSONL snapshots:
```bash
aws s3 ls --summarize --recursive s3://dpla-master-dataset/maryland/jsonl/<NEW_TIMESTAMP>/ | tail -2
aws s3 ls --summarize --recursive s3://dpla-master-dataset/maryland/jsonl/<PREV_TIMESTAMP>/ | tail -2
```

**SAFETY CHECK**: If the new ingest has >5% fewer records or significantly smaller total size than the previous one, **STOP and alert the user**. This may indicate a harvest problem, endpoint change, or data loss.

### Step 6: Stop the EC2 Instance

Only after verification is complete and the user confirms results are acceptable:

```bash
aws ec2 stop-instances --instance-ids i-0a0def8581efef783
```

### Step 7: Notifications

Post a summary to Slack #tech-alerts using the bot token (from local machine):

```bash
source ~/.claude/secrets/dpla.env
curl -s -X POST "https://slack.com/api/chat.postMessage" \
  -H "Authorization: Bearer $DPLA_SLACK_BOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"channel":"C02HEU2L3","text":"Maryland ingest complete. <RECORD_COUNT> records synced to S3."}'
```

## Error Handling

### Harvest Failure
1. Check OAI endpoint: `curl -s "http://collections.digitalmaryland.org/oai/oai.php?verb=Identify"`
2. Check harvest output dir for partial data or error logs
3. Common causes: OAI endpoint down, network timeout, malformed XML response
4. **Do NOT stop the EC2 instance** until investigated

### Mapping/Enrichment/JSONL Failure
1. Check for `_SUCCESS` markers — missing means that stage failed
2. Check for Java OutOfMemory errors (increase `SBT_OPTS` heap if needed)
3. Can retry individual stages:
   - Mapping only: `i3-remap maryland` (reruns mapping + enrichment + jsonl)
   - Enrichment only: `i3-enrich maryland <mapping_timestamp_dir>`
   - JSONL only: `i3-jsonl maryland <enrichment_timestamp_dir>`
4. **Do NOT stop the EC2 instance** until resolved

### S3 Sync Failure
1. Verify AWS identity: `aws sts get-caller-identity`
2. Verify bucket access: `aws s3 ls s3://dpla-master-dataset/maryland/`
3. Retry: `sync-s3 maryland`
4. **Do NOT stop the EC2 instance** until sync succeeds

### General Rule
On **any** failure: do NOT stop the EC2 instance until the issue is investigated and either resolved or explicitly abandoned by the user.

## Safety Rules

- This skill handles **Maryland only**. Do not run for other hubs without explicit instruction.
- Always verify record counts. A drop >5% must be flagged.
- This ingest does **NOT** rebuild the Elasticsearch index or change what is live on dp.la. Index rebuilds (sparkindexer) are separate, done after all monthly ingests complete.
- Keep the EC2 instance running until verification is complete, then stop to save costs.
- Do not modify i3.conf, .env, or any configuration unless explicitly requested.
- Do not `git pull` or update code unless explicitly requested.

## Credentials Reference

| Credential | Location |
|------------|----------|
| EC2 SSH key | `~/.ssh/aws_jenkins.pem` (local machine, fallback only) |
| AWS S3 access | EC2 instance role `ingestion3-spark` (no keys needed) |
| Slack webhook | `ingestion3/.env` on EC2 |
| Slack bot token | `~/.claude/secrets/dpla.env` on local machine (`DPLA_SLACK_BOT_TOKEN`) |
| OAI endpoint | `http://collections.digitalmaryland.org/oai/oai.php` (public, no auth) |

No secrets are stored in this skill file.

## Future Improvements

The EC2 instance runs an older version of ingestion3 that uses `.bashrc` shell functions (`i3-harvest`, `i3-remap`, `sync-s3`) instead of the newer `scripts/*.sh` pipeline. The newer pipeline adds status tracking, automatic Slack notifications, and better error handling. To upgrade:
1. Set up a GitHub deploy key on the EC2 instance
2. `git pull` the ingestion3 repo to get the `scripts/` directory
3. Update this skill to use `./scripts/ingest.sh maryland` instead of the shell functions
