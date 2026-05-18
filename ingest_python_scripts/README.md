# DPLA Ingest Pipeline — Runbook

Python scripts for running the DPLA monthly ingest and indexing pipeline from a local machine via AWS SSM. All scripts communicate with the ingest EC2 instance — you never need to SSH in directly.

---

## Prerequisites

- Python 3.9+
- AWS CLI installed and authenticated (`~/.aws/credentials`)
- `[nara]` profile in `~/.aws/credentials` (obtain credentials from Dominic)
- `sbt` installed locally for batch JAR builds (`brew install sbt`)
- `~/.dpla-secrets.env` locally with `DPLA_API_KEY`
- `~/.dpla-secrets.env` on the ingest EC2 with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL`

---

## First-Time Setup

```bash
python3 onboarding.py
```

Prompts for your local repo paths (`ingestion3`, `ingestion3-conf`, `batch-process-dpla-index`) and saves them to `.env` in the ingestion3 root (gitignored). Also verifies your AWS credentials, NARA profile, `i3.conf`, and DPLA API key.

Re-run at any time. Use `--update` to change saved paths.

---

## Monthly Workflow

Each month runs in roughly this order. Special-case hubs (Smithsonian, NARA, Community Webs) run their own scripts but fit into the same overall sequence.

```
1. Pre-flight checks    →  prechecks.py          (per hub, before each ingest)
2. Provider ingests     →  launch_ingest.py       (all standard hubs)
3. Special Cases        →  <hub_name>/launch_hub.py
6. Index rebuild        →  launch_indexer.py
7. Post-index batch     →  post_indexer.py
8. Verify               →  postchecks.py
```

---

## Standard Hub Ingest

### Step 1 — Pre-flight checks

```bash
python3 prechecks.py 
```

Prompts the user for the hub endpoint to check, Checks the EC2 instance state, repo freshness on EC2 (`ingestion3` + `ingestion3-conf`), JAR freshness, disk space, and reachability of the hub's harvest endpoint. Starts the instance automatically if it's stopped.

Run for every hub before launching.

Flag options:
```bash
python3 prechecks.py --endpoint-only   # just check the endpoint
python3 prechecks.py --skip-endpoint   # skip the endpoint check
python3 prechecks.py --no-start                   # don't auto-start EC2 if stopped
```

---

### Step 2 — Launch ingest

```bash
python3 launch_ingest.py
```

Prompts the user for the hub to run. Runs `ingest.sh <hub>` on EC2 in the background via SSM. For `file`-type hubs (Ohio, Georgia, Florida, etc.) it lists available S3 deliveries and asks you to confirm the endpoint before launching.

To resume a failed run partway through:

```bash
python3 launch_ingest.py <hub> --resume-from mapping     # or enrichment / jsonl
```

---

### Step 3 — Monitor

```bash
python3 check_ingest.py 
python3 check_ingest.py --watch    # auto-refresh every 30s
python3 check_ingest.py --watch 60 # custom interval
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

> NARA credentials live in `~/.aws/credentials` on EC2 under `[nara]`. If they're expired, email tech@dp.la.

**Step 2 — Full Pipeline**

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

Internet Archive delivers a SQLite `.db` file directly (not via S3). File recieved to tech@dp.la and downloaded to local machine.

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

## Slack Notifications

Scripts send notifications to `#tech-alerts` via the ingest EC2. Configure webhooks in `.env`:

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
Make sure `sbt` is installed (`brew install sbt` on Mac) and `batch-process-dpla-index` is cloned at the path saved in your `.env`.

**NARA credentials expired**
The `[nara]` profile in `~/.aws/credentials` on EC2 uses time-limited keys. Request new ones from NARA and update the profile.
