# DPLA Ingest Pipeline — Runbook

Python scripts for running the DPLA monthly ingest and indexing pipeline from your local machine via AWS SSM. All scripts communicate with the ingest EC2 instance — you never need to SSH in directly.

---

## First-Time Setup

```bash
python3 onboarding.py
```

Prompts for your local repo paths (`ingestion3`, `ingestion3-conf`, `batch-process-dpla-index`) and saves them to `.env` in the ingestion3 root (gitignored). Also verifies your AWS credentials, NARA profile, `i3.conf`, and DPLA API key.

Re-run at any time. Use `--update` to change saved paths.

---

## Prerequisites

- Python 3.9+
- AWS CLI installed and authenticated (`~/.aws/credentials`)
- `[nara]` profile in `~/.aws/credentials` (obtain credentials from Dominic)
- `sbt` installed locally for batch JAR builds (`brew install sbt`)
- `~/.dpla-secrets.env` locally with `DPLA_API_KEY`
- `~/.dpla-secrets.env` on the ingest EC2 with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL`

---

## Monthly Workflow

Each month runs in roughly this order. Special-case hubs (Smithsonian, NARA, Community Webs) run their own scripts but fit into the same overall sequence.

```
1. Pre-flight checks    →  prechecks.py          (per hub, before each ingest)
2. Provider ingests     →  launch_ingest.py       (all standard hubs)
3. Smithsonian          →  smithsonian/launch_smithsonian.py
4. Community Webs       →  community-webs/launch_cw.py
5. NARA                 →  nara/copy_nara.py  →  nara/launch_nara.py
6. Index rebuild        →  launch_indexer.py
7. Post-index batch     →  post_indexer.py
8. Verify               →  postchecks.py
```

---

## Standard Hub Ingest

### Step 1 — Pre-flight checks

```bash
python3 prechecks.py --hub <hub>
```

Checks the EC2 instance state, repo freshness on EC2 (`ingestion3` + `ingestion3-conf`), JAR freshness, disk space, and reachability of the hub's harvest endpoint. Starts the instance automatically if it's stopped.

Run for every hub before launching. Without a hub argument, just checks box state:

```bash
python3 prechecks.py
```

Flag options:
```bash
python3 prechecks.py --hub njde --endpoint-only   # just check the endpoint
python3 prechecks.py --hub njde --skip-endpoint   # skip the endpoint check
python3 prechecks.py --no-start                   # don't auto-start EC2 if stopped
```

---

### Step 2 — Launch ingest

```bash
python3 launch_ingest.py <hub>
```

Runs `ingest.sh <hub>` on EC2 in the background via SSM. For `file`-type hubs (Ohio, Georgia, Florida, etc.) it lists available S3 deliveries and asks you to confirm the endpoint before launching.

To resume a failed run partway through:

```bash
python3 launch_ingest.py <hub> --resume-from mapping     # or enrichment / jsonl
```

---

### Step 3 — Monitor

```bash
python3 check_ingest.py --hub <hub>
python3 check_ingest.py --hub <hub> --watch    # auto-refresh every 30s
python3 check_ingest.py --hub <hub> --watch 60 # custom interval
```

Shows process status, completed stages with record counts, current stage, disk usage, and recent log lines.

---

### Step 4 — Launch indexer

Once all hubs for the month are ingested:

```bash
python3 launch_indexer.py
```

Runs pre-flight checks (no existing cluster, snapshot ages, batch output path, JAR freshness), launches the sparkindexer EMR cluster, monitors it with an hourly Slack heartbeat, then walks you through the Elasticsearch alias swap. Cluster takes **6–9 hours**.

```bash
python3 launch_indexer.py --cluster-id j-XXXXX   # resume monitoring an existing cluster
python3 launch_indexer.py --alias-swap-only       # skip straight to alias swap
python3 launch_indexer.py --verify-only           # just verify the API count
python3 launch_indexer.py --skip-preflight        # skip pre-flight checks
```

---

### Step 5 — Post-indexer

Immediately after the alias swap completes:

```bash
python3 post_indexer.py
```

Checks/rebuilds the batch JAR, launches the monthlybatch EMR cluster, and runs four Spark steps in sequence: `ParquetDump` → `JsonlDump` → `MqReports` → `Sitemap`. On completion it runs hub stats on EC2 and triggers the hub sitemaps GitHub Actions workflow. Cluster takes **3–5 hours**.

```bash
python3 post_indexer.py --cluster-id j-XXXXX   # resume monitoring
python3 post_indexer.py --skip-preflight        # skip JAR check
```

---

### Step 6 — Verify

```bash
python3 postchecks.py                    # current month
python3 postchecks.py --month 202604     # specific month (YYYYMM)
```

Reads `i3.conf` to get the hub list scheduled for that month, checks snapshot dates and record counts in S3, verifies the provider export, hub stats, and sitemaps, and hits the live API for a total record count.

---

## Special Cases

### NARA

NARA delivers delta files bimonthly (Feb, Apr, Jun, Aug, Oct, Dec) via their own S3 bucket (`s3://ngc-storage01`), which requires separate NARA-issued credentials. The process is three steps.

**Step 1 — Stage files**

```bash
python3 nara/copy_nara.py --month 202604
```

Runs on EC2 via SSM. Downloads ZIPs from `ngc-storage01` using the `[nara]` AWS profile, uploads them to `s3://dpla-hub-nara/raw_ingest_files/<YYYYMM>/`, then moves them to the EC2 ingest directory so `nara-ingest.sh` doesn't need to re-download them.

> NARA credentials live in `~/.aws/credentials` on EC2 under `[nara]`. Keys are in 1Password: **"ingest@dp.la NARA keys"**. If they're expired, request new ones from NARA.

**Step 2 — Launch**

```bash
python3 nara/launch_nara.py --month 202604
```

Runs a preflight check that files are staged on EC2, then launches `nara-ingest.sh` in the background. NARA is a large delta ingest — **expect several hours**.

**Step 3 — Monitor**

```bash
python3 nara/check_nara.py
python3 nara/check_nara.py --watch
```

Shows process, completed stages, latest merged harvest record count, disk usage, and recent log lines.

---

### Smithsonian

Smithsonian delivers files to `s3://dpla-hub-si` bimonthly (Feb, Apr, Jun, Aug, Oct, Dec). It requires a preprocessing step (`fix-si.sh`) before the standard pipeline and has optional human review checkpoints at each stage.

```bash
python3 smithsonian/launch_smithsonian.py
```

Stages run in order with a confirmation prompt between each:

1. **Download** — syncs from `s3://dpla-hub-si` to EC2
2. **Preprocess** — runs `fix-si.sh` to normalize raw files *(checkpoint)*
3. **Harvest** — runs `harvest.sh smithsonian` *(checkpoint)*
4. **Mapping** — runs `ingest.sh smithsonian --mapping-only` *(checkpoint)*
5. **Pipeline** — runs `ingest.sh smithsonian --resume-from enrichment`

```bash
python3 smithsonian/launch_smithsonian.py --auto              # skip checkpoints
python3 smithsonian/launch_smithsonian.py --date 2026-04-03   # specific delivery date
python3 smithsonian/launch_smithsonian.py --start-at mapping  # resume from a stage
```

Monitor with:

```bash
python3 smithsonian/check_status_smithsonian.py
python3 smithsonian/check_status_smithsonian.py --watch
```

---

### Community Webs

Internet Archive delivers a SQLite `.db` file directly (not via S3). You receive the file and pass it to the script.

```bash
python3 community-webs/launch_cw.py --db ~/Downloads/community-webs.db --full
```

This:
1. Uploads the `.db` to `s3://dpla-scratch/community-webs/`
2. Downloads it to EC2 at a temp path
3. Runs `community-webs-ingest.sh --db=<path> --full` on EC2: export → harvest → mapping → enrichment → jsonl → S3

Without `--full`, only export and harvest run. Re-run with `--full` and `--skip-export` to continue:

```bash
python3 community-webs/launch_cw.py --full --skip-export
```

**Skip flags for resuming after a failure:**

```bash
--skip-upload    # .db already in s3://dpla-scratch/community-webs/
--skip-export    # ZIP already on EC2, skip straight to harvest
```

Monitor with:

```bash
python3 community-webs/check_cw.py
python3 community-webs/check_cw.py --watch
```

---

## Key AWS Resources

| Resource | Value |
|---|---|
| Ingest EC2 | `i-0a0def8581efef783` |
| Search EC2 (search-prod1) | `i-00bbdfe0a6ff6cf78` |
| Master dataset bucket | `s3://dpla-master-dataset/` |
| Provider export bucket | `s3://dpla-provider-export/` |
| Sparkindexer JAR | `s3://dpla-sparkindexer/sparkindexer-assembly.jar` |
| Batch JAR | `s3://dpla-monthly-batch/batch-process-dpla-index-assembly.jar` |
| EMR logs | `s3://aws-logs-283408157088-us-east-1/elasticmapreduce/` |
| Sitemaps | `s3://sitemaps.dp.la/sitemap/` |
| Dashboard analytics | `s3://dashboard-analytics/` |
| NARA raw files | `s3://dpla-hub-nara/raw_ingest_files/` |
| CW staging | `s3://dpla-scratch/community-webs/` |

---

## Slack Notifications

Scripts send notifications to `#tech-alerts` (failures) and `#tech` (completions) via the ingest EC2. Configure webhooks in `.env`:

```
SLACK_WEBHOOK=           # #tech-alerts
SLACK_TECH_WEBHOOK=      # #tech (hub-complete with @here)
SLACK_ALERT_USER_ID=     # your Slack member ID for @mention on failures
```

---

## Troubleshooting

**Clock skew / `InvalidSignatureException` from AWS CLI**
AWS rejects requests when your system clock is off. Fix with:
```bash
sudo sntp -sS time.apple.com
```
Then re-run or use `--resume` / `--cluster-id` to pick up where you left off.

**Ingest failed mid-run**
Use `--resume-from <stage>` in `launch_ingest.py` to restart from mapping, enrichment, or jsonl without re-harvesting.

**Cluster monitoring crashes**
Your IAM user needs EMR read permissions. Resume with `--cluster-id j-XXXXX` once permissions are sorted.

**Slack notifications not sending**
Verify `~/.dpla-secrets.env` exists on the ingest EC2 with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL` set.

**`sbt assembly` fails**
Make sure `sbt` is installed (`brew install sbt`) and `batch-process-dpla-index` is cloned at the path saved in your `.env`.

**NARA credentials expired**
The `[nara]` profile in `~/.aws/credentials` on EC2 uses time-limited keys. Request new ones from NARA and update the profile.
