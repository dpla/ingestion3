# DPLA Ingest Pipeline — Runbook

All scripts communicate with the ingest EC2 instance via AWS SSM — you never need to SSH in directly.

Run the scripts from this folder (`cd ingest_pipeline_python`). The numbered scripts are the monthly sequence, in order; `special_cases/` has the hubs with their own pipelines, and `helper_scripts/` has monitoring, setup and batch tools.

---

## How to Run an Ingest

Three paths are available depending on how much control you need:

| Method | When to use |
|--------|-------------|
| **1. Manual via SSM** | Debugging, one-off runs, or when you need to run a command directly on the box |
| **2. Manual via Python scripts** | Normal monthly operations — run from your Mac, full visibility, interactive prompts |
| **3. GitHub Actions** | Hands-off launches — trigger from the GitHub UI; EC2 does the work |

### 1. Manual via SSM

Send commands directly to the EC2 instance:

```bash
# Ad-hoc ingest (replace <hub> with the hub short name)
aws ssm send-command \
  --instance-ids i-XXXXX \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -u ec2-user bash -l -c \"nohup bash /home/ec2-user/ingestion3/scripts/ingest.sh <hub> > /home/ec2-user/data/<hub>-ingest.log 2>&1 </dev/null &\""]' \
  --region us-east-1
```

Use SSM for one-off commands, quick checks, or when the Python scripts aren't available.

### 2. Manual via Python scripts

Run from your Mac — see the sections below for each step of the monthly workflow.

```bash
python3 2_preflight.py --hub bpl              # pre-flight
python3 3_launch_ingest.py bpl                # launch a hub
python3 helper_scripts/check_ingest.py bpl    # monitor it
```

### 3. GitHub Actions

Two workflows are available under **Actions → Launch Hub Ingest / Monthly Hub Batch**:

| Workflow | Purpose | File |
|----------|---------|------|
| **Launch Hub Ingest** | Manual single-hub trigger | `.github/workflows/ingest-hub.yml` |
| **Monthly Hub Batch** | Reads i3.conf, fires all scheduled standard hubs sequentially; special-case hubs (NARA, Smithsonian, Community Webs) are excluded and must be run separately | `.github/workflows/ingest-monthly.yml` |

**Launch Hub Ingest** inputs:
- `hub` (required) — short name, e.g. `bpl`, `ohio`, `nara`, `smithsonian`
- `resume_from` (optional) — resume standard hubs from `harvest|mapping|enrichment|jsonl|delivery`
- `override` (optional) — NARA: `YYYYMM`; Smithsonian: `YYYY-MM-DD`; Community Webs: `YYYYMMDD_HHMMSS`

**Monthly Hub Batch** inputs:
- `month` (optional, 1-12) — defaults to current month
- `dry_run` — print hub list without launching

Both workflows require the `INGEST_INSTANCE_ID` secret and OIDC auth scoped to `main`.
The batch fires all hubs as a sequential background job on EC2 — the GHA step completes
immediately; Slack notifies as each hub finishes.

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
python3 helper_scripts/onboarding.py
```

Prompts for your local repo paths (`ingestion3`, `ingestion3-conf`, `batch-process-dpla-index`) and saves them to `.env` in the ingestion3 root (gitignored). Also verifies your AWS credentials, NARA profile, `i3.conf`, and DPLA API key.

Re-run at any time. Use `--update` to change saved paths.

---

## Monthly Workflow

```
1. 1_prechecks.py        list every hub scheduled for the month
2. 2_preflight.py        check one hub before ingesting it
3. 3_launch_ingest.py    launch that hub once preflight passes
4.                       repeat 2 and 3 for each hub on the list
5. 4_launch_indexer.py   rebuild the index and swap the alias, once every hub is in
6. 5_post_indexer.py     batch exports, hub stats and sitemaps, right after the swap
```

Afterwards, `6_postchecks.py` verifies the month's outputs. Special-case hubs (NARA, Smithsonian, Community Webs, Virginias) use their scripts in `special_cases/` in place of steps 2–3; preflight tells you when a hub is one of them.

---

### Step 1 — Monthly hub list

```bash
python3 1_prechecks.py              # current month
python3 1_prechecks.py --month 5    # specific month
```

Lists all hubs scheduled for the month from `i3.conf`, with harvest types, special-case notes, and on-hold status. Run this at the start of each ingest month to plan the run order.

---

### Step 2 — Preflight (per hub)

```bash
python3 2_preflight.py --hub <hub>
```

Checks the EC2 instance state, repo freshness on EC2 (`ingestion3` + `ingestion3-conf`), JAR freshness, disk space, and reachability of the hub's harvest endpoint. Starts the instance automatically if it's stopped. Without `--hub` it prompts for one. For special-case hubs it says which runbook to follow instead.

Flag options:
```bash
python3 2_preflight.py --hub <hub> --endpoint-only   # just check the endpoint
python3 2_preflight.py --hub <hub> --skip-endpoint   # skip the endpoint check
python3 2_preflight.py --no-start                    # don't auto-start EC2 if stopped
```

---

### Step 3 — Launch the ingest (once preflight passes)

```bash
python3 3_launch_ingest.py <hub>
```

Runs `ingest.sh <hub>` on EC2 in the background via SSM. Without a hub it prompts for one. For `file`-type hubs (Ohio, Georgia, Florida, etc.) it lists available S3 deliveries and asks you to confirm the endpoint before launching.

To resume a failed run partway through:

```bash
python3 3_launch_ingest.py <hub> --resume-from mapping     # or enrichment / jsonl
```

To run without the partner summary email at the end (for example, to review the counts before the hub hears about them):

```bash
python3 3_launch_ingest.py <hub> --skip-email
```

This passes `--skip-email` to `ingest.sh` on EC2, so the EC2 checkout of ingestion3 must include it. It combines with `--resume-from` and the other options.

To send the email later, run `send-ingest-email.sh` on EC2 as `ec2-user` from the ingestion3 checkout, since it reads the hub's mapping output there (`/home/ec2-user/data/<hub>/mapping/`, newest run unless you pass a mapping dir). The hub is the first argument. Without `--yes` it shows the recipients and summary and asks `Send email? (y/n)`, which needs an interactive shell (e.g. an SSM session):

```bash
sudo -u ec2-user bash -lc "cd /home/ec2-user/ingestion3 && source .env && bash scripts/communication/send-ingest-email.sh <hub>"
```

Through `aws ssm send-command` there is no terminal to answer the prompt, so add `--yes`, which sends immediately to the hub's i3.conf contacts:

```bash
aws ssm send-command --profile dpla --instance-ids <ingest-instance-id> --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -u ec2-user bash -lc \"cd /home/ec2-user/ingestion3 && source .env && bash scripts/communication/send-ingest-email.sh --yes <hub>\""]'
```

**Monitoring a running ingest:**

```bash
python3 helper_scripts/check_ingest.py <hub>
```

Shows process status, completed stages with record counts, current stage, disk usage, and recent log lines.

---

### Step 4 — Repeat preflight and launch for each hub

Hubs can't run in parallel on the ingest EC2 (memory), so wait for one to finish before launching the next.

---

### Step 5 — Launch the indexer

Once all hubs for the month are ingested:

```bash
python3 4_launch_indexer.py
```

Runs pre-flight checks (no existing cluster, snapshot ages, batch output path, JAR freshness), launches the sparkindexer EMR cluster, monitors it with an hourly Slack heartbeat, then walks you through the Elasticsearch alias swap. Cluster takes **6–9 hours**.

```bash
python3 4_launch_indexer.py --cluster-id j-XXXXX   # resume monitoring an existing cluster
python3 4_launch_indexer.py --alias-swap-only       # skip straight to alias swap
python3 4_launch_indexer.py --verify-only           # just verify the API count
python3 4_launch_indexer.py --skip-preflight        # skip pre-flight checks
```

---

### Step 6 — Post-indexer

Immediately after the alias swap completes:

```bash
python3 5_post_indexer.py
```

Checks/rebuilds the batch JAR, launches the monthlybatch EMR cluster, and runs four Spark steps in sequence: `ParquetDump` → `JsonlDump` → `MqReports` → `Sitemap`. On completion it runs hub stats and the aviation/bws sitemaps on EC2, then runs the dpla-frontend GitHub Actions workflow that builds the provider-based hub sitemaps and waits for its result. A final Slack summary names any failed or stale output. Cluster takes **3–5 hours**.

```bash
python3 5_post_indexer.py --cluster-id j-XXXXX   # resume monitoring
python3 5_post_indexer.py --skip-preflight        # skip JAR check
```

---

### Verify

```bash
python3 6_postchecks.py                    # current month
python3 6_postchecks.py --month 202604     # specific month (YYYYMM)
```

Reads `i3.conf` to get the hub list scheduled for that month, checks snapshot dates and record counts in S3, verifies the provider export, hub stats, and sitemaps, and hits the live API for a total record count.

---

## Special Cases

### NARA

NARA delivers delta files bimonthly (Feb, Apr, Jun, Aug, Oct, Dec) via their own S3 bucket (`s3://ngc-storage01`), which requires separate NARA-issued credentials. The process is three steps.

**Step 1 — Stage files**

```bash
python3 special_cases/nara/copy_nara.py --month 202604
```

Runs on EC2 via SSM. Downloads ZIPs from `ngc-storage01` using the `[nara]` AWS profile, uploads them to `s3://dpla-hub-nara/raw_ingest_files/<YYYYMM>/`, then moves them to the EC2 ingest directory so `nara-ingest.sh` doesn't need to re-download them.

> NARA credentials live in `~/.aws/credentials` on EC2 under `[nara]`. If they're expired, email tech@dp.la.

**Step 2 — Full Pipeline**

```bash
python3 special_cases/nara/launch_nara.py --month 202604
```

Runs a preflight check that files are staged on EC2, then launches `nara-ingest.sh` in the background. NARA is a large delta ingest — **expect several hours**.

**Step 3 — Monitor**

```bash
python3 special_cases/nara/check_nara.py
python3 special_cases/nara/check_nara.py --watch
```

Shows process, completed stages, latest merged harvest record count, disk usage, and recent log lines.

---

### Smithsonian

Smithsonian delivers files to `s3://dpla-hub-si` bimonthly (Feb, Apr, Jun, Aug, Oct, Dec). It requires a preprocessing step (`fix-si.sh`) before the standard pipeline and has optional human review checkpoints at each stage.

```bash
python3 special_cases/smithsonian/launch_smithsonian.py
```

Stages run in order with a confirmation prompt between each:

1. **Download** — syncs from `s3://dpla-hub-si` to EC2
2. **Preprocess** — runs `fix-si.sh` to normalize raw files *(checkpoint)*
3. **Harvest** — runs `harvest.sh smithsonian` *(checkpoint)*
4. **Mapping** — runs `ingest.sh smithsonian --mapping-only` *(checkpoint)*
5. **Pipeline** — runs `ingest.sh smithsonian --resume-from enrichment`

```bash
python3 special_cases/smithsonian/launch_smithsonian.py --auto              # skip checkpoints
python3 special_cases/smithsonian/launch_smithsonian.py --date 2026-04-03   # specific delivery date
python3 special_cases/smithsonian/launch_smithsonian.py --start-at mapping  # resume from a stage
```

Monitor with:

```bash
python3 special_cases/smithsonian/check_status_smithsonian.py
python3 special_cases/smithsonian/check_status_smithsonian.py --watch
```

---

### Community Webs

Internet Archive delivers a SQLite `.db` file directly (not via S3). File recieved to tech@dp.la and downloaded to local machine.

```bash
python3 special_cases/community-webs/copy_cw.py ~/Downloads/community-webs.sqlite3
python3 special_cases/community-webs/launch_cw.py
```

`copy_cw.py` checks the file, shows its record count next to the previous one, and copies it to `s3://dpla-hub-community-webs/`.

`launch_cw.py` then:
1. Finds the newest copied `.db` and asks once
2. Downloads it to EC2 at a temp path
3. Runs `community-webs-ingest.sh` on EC2: export → harvest → mapping → enrichment → jsonl → safety check
4. Waits, shows the counts and contacts, and asks before syncing to S3 and emailing the hub

Slack reports each stage. If the safety check blocked the run, step 4 says so and asks the same question. Without a terminal (GitHub Actions) it stops after starting the run.

If you answer no or close the terminal, send later:

```bash
python3 special_cases/community-webs/launch_cw.py --send
python3 special_cases/community-webs/launch_cw.py --send --force   # the safety check blocked it and you've reviewed the drop
```

To retry after a failure, run `launch_cw.py` again. `--timestamp` picks a specific copied `.db`; a file that already finished is refused without it.

Monitor with:

```bash
python3 special_cases/community-webs/check_cw.py
python3 special_cases/community-webs/check_cw.py --watch
```
---

### Virginias

Digital Virginias publishes its metadata across the `dplava` GitHub repos.

```bash
python3 special_cases/virginias/virginias_download.py
```

Downloads the repos to EC2 and updates `virginias.harvest.endpoint` in your local `i3.conf`. Commit and push the conf, then run steps 2–3 for `virginias`; preflight offers to pull the box's repos if they're behind.

---

## Folder Layout

| Path | What's there |
|---|---|
| `1_prechecks.py` … `6_postchecks.py` | The monthly sequence, in order |
| `special_cases/` | NARA, Smithsonian, Community Webs, Virginias |
| `helper_scripts/check_ingest.py` | Monitor a running ingest |
| `helper_scripts/hub_conf.py` | Not run directly: lets the numbered scripts import the shared i3.conf helpers from `2_preflight.py` |
| `helper_scripts/onboarding.py` | First-time setup |
| `helper_scripts/run_monthly_batch.py` | Launch all of a month's standard hubs in sequence (used by the Monthly Hub Batch workflow) |
| `helper_scripts/update_file_endpoints.py` | Point file hubs' i3.conf endpoints at their newest S3 delivery (used by the Update Hub File Endpoints workflow) |

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
Use `--resume-from <stage>` in `3_launch_ingest.py` to restart from mapping, enrichment, or jsonl without re-harvesting.

**Cluster monitoring crashes**
Your IAM user needs EMR read permissions. Resume with `--cluster-id j-XXXXX` once permissions are sorted.

**Slack notifications not sending**
Verify `~/.dpla-secrets.env` exists on the ingest EC2 with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL` set.

**`sbt assembly` fails**
Make sure `sbt` is installed (`brew install sbt` on Mac) and `batch-process-dpla-index` is cloned at the path saved in your `.env`.

**NARA credentials expired**
The `[nara]` profile in `~/.aws/credentials` on EC2 uses time-limited keys. Request new ones from NARA and update the profile.


