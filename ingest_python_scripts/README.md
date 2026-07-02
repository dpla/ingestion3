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
0. Monthly hub list     →  pre_ingest_check.py          (list all hubs for the month)

── repeat steps 1–3 for each hub ──────────────────────────────────────────────
1. Pre-flight checks    →  hub_preflight.py              (per hub, before each ingest)
2. Provider ingests     →  launch_ingest.py              (standard hubs)
               OR          nara/launch_nara.py           (NARA)
               OR          smithsonian/launch_smithsonian.py  (Smithsonian)
               OR          community-webs/launch_cw.py   (Community Webs)
3. Monitor              →  check_ingest.py               (watch until complete)
── end repeat ──────────────────────────────────────────────────────────────────

4. Verify all hubs done →  postchecks.py                 (confirm every hub ingested before indexing)
5. Index rebuild        →  launch_indexer.py             (only after postchecks passes)
6. Post-index batch     →  post_indexer.py
```

---

## Standard Hub Ingest

### Step 0 — Monthly hub list

```bash
python3 pre_ingest_check.py              # current month
python3 pre_ingest_check.py --month 5    # specific month
```

Lists all hubs scheduled for the month from `i3.conf`, with harvest types, special-case notes, and on-hold status. Run this at the start of each ingest month to plan the run order.

---

### Step 1 — Pre-flight checks

```bash
python3 hub_preflight.py
```

Prompts the user for the hub endpoint to check. Checks the EC2 instance state, repo freshness on EC2 (`ingestion3` + `ingestion3-conf`), JAR freshness, disk space, and reachability of the hub's harvest endpoint. Starts the instance automatically if it's stopped.

Run for every hub before launching.

Flag options:
```bash
python3 hub_preflight.py --hub <hub> --endpoint-only   # just check the endpoint
python3 hub_preflight.py --hub <hub> --skip-endpoint   # skip the endpoint check
python3 hub_preflight.py --no-start                    # don't auto-start EC2 if stopped
```

---

### Step 2 — Launch ingest

Each hub has one launch script. **Do not pass special-case hub names to `launch_ingest.py`** — it will error and tell you which script to use instead.

#### Standard hubs

```bash
python3 launch_ingest.py
```

Prompts the user for the hub to run. Runs `ingest.sh <hub>` on EC2 in the background via SSM. For `file`-type hubs (Ohio, Georgia, Florida, etc.) it lists available S3 deliveries and asks you to confirm the endpoint before launching.

To resume a failed run partway through:

```bash
python3 launch_ingest.py <hub> --resume-from mapping     # or enrichment / jsonl
```

#### Digital Virginias

Digital Virginias publishes metadata across [multiple GitHub repositories](https://github.com/dplava). Run a staging script first to clone all repos and zip the output on EC2, then run the standard ingest:

```bash
python3 virginias/virginias_download.py
python3 launch_ingest.py virginias
```

#### NARA

NARA delivers delta files bimonthly (Feb, Apr, Jun, Aug, Oct, Dec) via their own S3 bucket (`s3://ngc-storage01`), which requires separate NARA-issued credentials.

**Stage files first:**

```bash
python3 nara/copy_nara.py --month 202604
```

Downloads ZIPs from `ngc-storage01` using the `[nara]` AWS profile and stages them on EC2. NARA credentials live in `~/.aws/credentials` under `[nara]`. If they're expired, email tech@dp.la.

**Then launch:**

```bash
python3 nara/launch_nara.py --month 202604
```

Runs pre-flight, then launches `nara-ingest.sh` in the background. NARA is a large delta ingest — **expect several hours**.

**Monitor:**

```bash
python3 nara/check_nara.py
python3 nara/check_nara.py --watch
```

#### Smithsonian

Smithsonian delivers files to `s3://dpla-hub-si` bimonthly (Feb, Apr, Jun, Aug, Oct, Dec). It requires a preprocessing step before the standard pipeline.

```bash
python3 smithsonian/launch_smithsonian.py
```

Stages run in order with a confirmation prompt between each: Download → Preprocess (`fix-si.sh`) → Harvest → Mapping → Pipeline.

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

#### Community Webs

Internet Archive delivers a SQLite `.db` file directly to tech@dp.la (not via S3). Download it locally first, then:

```bash
python3 community-webs/launch_cw.py --db ~/Downloads/community-webs.db --full
```

This uploads the `.db` to S3, downloads it to EC2, then runs the full pipeline: export → harvest → mapping → enrichment → jsonl → S3. Without `--full`, only export and harvest run.

```bash
python3 community-webs/launch_cw.py --full --skip-export   # resume after export
--skip-upload    # .db already in s3://dpla-scratch/community-webs/
--skip-export    # ZIP already on EC2, skip straight to harvest
```

Monitor with:

```bash
python3 community-webs/check_cw.py
python3 community-webs/check_cw.py --watch
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

### Step 4 — Verify all hubs are ingested

Before launching the indexer, confirm every hub for the month completed successfully:

```bash
python3 postchecks.py                    # current month
python3 postchecks.py --month 202604     # specific month (YYYYMM)
```

Reads `i3.conf` to get the hub list scheduled for that month, checks snapshot dates and record counts in S3, verifies the provider export. **Do not proceed to the indexer until this passes.**

---

### Step 5 — Launch indexer

Once postchecks confirms all hubs are ingested:

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

### Step 6 — Post-indexer

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


