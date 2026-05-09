# ingestion3/run — DPLA Operations Scripts

Python scripts for running the DPLA monthly data pipeline: provider ingests, index rebuild, and post-index batch jobs.

---

## Prerequisites

- AWS CLI installed and authenticated (`~/.aws/credentials`)
- `gh` CLI installed and authenticated (`gh auth status`)
- Python 3.9+
- `sbt` installed locally (for batch JAR builds — `brew install sbt`)
- `~/.dpla-secrets.env` on the ingest EC2 with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL`
- `~/.dpla-secrets.env` locally with `DPLA_API_KEY` (for index verification)

---

## Monthly Workflow

The full monthly process runs in this order:

```
1. Provider ingests        →  launch_ingest.py (per hub)
2. Smithsonian ingest      →  smithsonian/launch_smithsonian.py
3. Community Webs ingest   →  community-webs/launch_cw.py
4. Index rebuild           →  launch_indexer.py
5. Post-index batch jobs   →  post_indexer.py
6. NARA ingest             →  launch_nara.py  (must run after post-index)
```

---

## Scripts

### `prechecks.py`
Runs pre-flight checks before launching anything. Verifies the ingest EC2 is reachable, the ingestion3 JAR is present, and the environment is healthy.

```bash
python3 prechecks.py
```

---

### `launch_ingest.py`
Launches an ingest for a single provider on the ingest EC2 (`i-0a0def8581efef783`) via SSM. Runs the full ingest pipeline: harvest → mapping → enrichment → JSONL → S3 sync.

```bash
python3 launch_ingest.py <provider>
# e.g.
python3 launch_ingest.py maryland
python3 launch_ingest.py virginia
```

Logs stream back to your terminal. The ingest runs remotely — you can Ctrl+C and it keeps going on EC2.

---

### `check_ingest.py`
Checks the status of a running or recently completed ingest. Shows which pipeline stages have completed and whether the process is still running.

```bash
python3 check_ingest.py <provider>
```

---

### `smithsonian/launch_smithsonian.py`
Launches the Smithsonian ingest. Smithsonian has a custom multi-unit harvest process and runs separately from the standard `launch_ingest.py` flow.

```bash
python3 smithsonian/launch_smithsonian.py
```

Check status with:
```bash
python3 smithsonian/check_status_smithsonian.py
```

---

### `community-webs/launch_cw.py`
Launches the Community Webs ingest.

```bash
python3 community-webs/launch_cw.py
```

Check status with:
```bash
python3 community-webs/check_cw.py
```

---

### `launch_indexer.py`
Runs the monthly Elasticsearch index rebuild. Reads all hub JSONL snapshots from `s3://dpla-master-dataset/`, indexes everything into a new `dpla-all-*` index on search-prod1, then walks you through the alias swap to make it live on dp.la.

**Pre-flight checks (automatic):**
- No existing sparkindexer cluster running
- Hub snapshot ages (warns if any hub > 45 days old)
- Batch output path clear
- sparkindexer JAR freshness (offers to rebuild if stale)

**Full run:**
```bash
python3 launch_indexer.py
```

**Resume monitoring an existing cluster:**
```bash
python3 launch_indexer.py --cluster-id j-XXXXXXXXXXXXX
```

**Skip straight to alias swap:**
```bash
python3 launch_indexer.py --alias-swap-only
```

**Just verify the API count (with delta vs old index):**
```bash
python3 launch_indexer.py --verify-only
```

**Skip pre-flight checks:**
```bash
python3 launch_indexer.py --skip-preflight
```

The spark-indexer step takes **6–9 hours** after the cluster reaches RUNNING state. Slack heartbeats post every hour. When the cluster finishes, the script pauses for you to confirm the alias swap before making anything live.

---

### `post_indexer.py`
Runs immediately after `launch_indexer.py` completes the alias swap. Handles all post-index batch processing:

1. **Batch JAR check** — verifies `s3://dpla-monthly-batch/batch-process-dpla-index-assembly.jar` is fresh. Offers to rebuild locally via `sbt assembly` if stale.
2. **Launch monthlybatch EMR cluster** — runs 4 Spark steps sequentially:
   - `ParquetDump` — writes parquet snapshot to `s3://dpla-provider-export/`
   - `JsonlDump` — writes JSONL dump to `s3://dpla-provider-export/`
   - `MqReports` — generates metadata quality reports to `s3://dashboard-analytics/`
   - `Sitemap` — generates sitemaps to `s3://sitemaps.dp.la/sitemap/`
3. **Hub stats** — runs `generate_hub_stats.py` on the ingest EC2
4. **Hub sitemaps** — triggers `generate-hub-sitemaps.yml` GitHub Actions workflow in `dpla-frontend`
5. **S3 verification** — confirms all output buckets have data

```bash
python3 post_indexer.py
```

**Resume monitoring an existing cluster:**
```bash
python3 post_indexer.py --cluster-id j-XXXXXXXXXXXXX
```

**Skip JAR check:**
```bash
python3 post_indexer.py --skip-preflight
```

Note: the batch JAR is built from the `batch-process-dpla-index` repo, which must be cloned locally at `/Users/zoe/Documents/Repos/batch-process-dpla-index`. The cluster takes **3–5 hours** and posts hourly Slack heartbeats.

---

### `launch_nara.py`
Launches the NARA ingest. **Must run after `post_indexer.py` completes** — NARA depends on the parquet output written by the batch cluster.

```bash
python3 launch_nara.py
```

Check status / supporting scripts in `nara/`:
```bash
python3 nara/check_nara.py
python3 nara/copy_nara.py
```

---

## Slack Notifications

All scripts send Slack notifications to the configured channel via the ingest EC2 (token lives in `~/.dpla-secrets.env` on the instance). Notifications are sent for:

- Cluster launched / running / terminating / complete / failed
- Hourly heartbeat while running
- Alias swap complete
- API record count with delta vs old index
- Post-index steps complete
- Hub stats and sitemaps triggered
- S3 output verification

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

---

## Troubleshooting

**Ingest failed mid-run (e.g. S3 sync step)**
If the ingest process died after the JSONL export but before completing the remaining steps, manually re-run the S3 sync and then run the recovery script:
```bash
# Re-run S3 sync on EC2 via SSM, then:
python3 finish_mi_ingest.py   # Michigan example — adapt for other providers
```

**Cluster monitoring crashes (AccessDeniedException)**
Your IAM user needs EMR read permissions. Ask an admin to run:
```bash
aws iam attach-user-policy --user-name <your-username> \
  --policy-arn arn:aws:iam::283408157088:policy/zoe-emr-readwrite
```
Resume monitoring with `--cluster-id`.

**Slack notifications not sending**
Notifications are sent via the ingest EC2. Verify `~/.dpla-secrets.env` exists on the instance with `SLACK_BOT_TOKEN` and `SLACK_CHANNEL` set.

**sbt assembly fails locally**
Make sure `sbt` is installed (`brew install sbt`) and the `batch-process-dpla-index` repo is cloned at `/Users/zoe/Documents/Repos/batch-process-dpla-index`.