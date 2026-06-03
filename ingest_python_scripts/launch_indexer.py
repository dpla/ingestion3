#!/usr/bin/env python3
"""
DPLA Sparkindexer launch script.

Runs pre-flight checks, launches the sparkindexer EMR cluster, monitors it,
then walks you through the Elasticsearch alias swap when it's done.

Usage:
    python3 launch_indexer.py

Pre-flight checks:
  1. No existing sparkindexer EMR cluster running
  2. Hub snapshot ages (warns on any >45 days old)
  3. Batch output path doesn't already exist (deletes if confirmed)
  4. sparkindexer JAR freshness

After cluster finishes:
  - Pauses for your confirmation before swapping the ES alias live
  - Verifies API count after swap
  - Reminds you to clean up old indices

Prerequisites:
    - AWS CLI installed and authenticated (~/.aws/credentials or instance role)
    - IAM: emr:CreateCluster, emr:DescribeCluster, ssm:SendCommand on search-prod1
"""

import base64
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

# ---------- config ----------
def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    )
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    creds = cfg.get("AWS_SHARED_CREDENTIALS_FILE")
    if creds:
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", creds)
    return cfg

_env = _load_dotenv()

REGION             = "us-east-1"
ES_INSTANCE_ID     = _env.get("ES_INSTANCE_ID", "")
INGEST_INSTANCE_ID = _env.get("INGEST_INSTANCE_ID", "")
ES_HOST            = _env.get("ES_HOST", "")
AWS_ACCOUNT_ID     = _env.get("AWS_ACCOUNT_ID", "")
EMR_LOG_URI        = f"s3://aws-logs-{AWS_ACCOUNT_ID}-us-east-1/elasticmapreduce/"
# Parse hostname and port from ES_HOST for sparkindexer step args
_es_stripped = ES_HOST.replace("https://", "").replace("http://", "").rstrip("/")
_es_parts    = _es_stripped.rsplit(":", 1)
ES_HOST_NAME = _es_parts[0]
ES_PORT      = _es_parts[1] if len(_es_parts) > 1 else "9200"
S3_DATASET        = "dpla-master-dataset"
S3_BATCH_BASE     = "dpla-provider-export"
SPARKINDEXER_JAR  = "s3://dpla-sparkindexer/sparkindexer-assembly.jar"
SPARKINDEXER_DIR  = "/home/ec2-user/sparkindexer"
SPARKINDEXER_LOCAL_JAR = f"{SPARKINDEXER_DIR}/target/scala-2.12/sparkindexer-assembly.jar"
STALE_DAYS        = 45
POLL_SECONDS      = 300   # check cluster every 5 minutes


# ---------- Slack ----------
def slack_notify(msg):
    """Post to Slack via EC2 using common.sh's slack_notify — identical behaviour to ingest.sh."""
    encoded = base64.b64encode(msg.encode()).decode()
    script = (
        "source /home/ec2-user/ingestion3/scripts/common.sh\n"
        f"slack_notify \"$(echo {encoded} | base64 -d)\""
    )
    cmd = base64.b64encode(script.encode()).decode()
    try:
        ssm_run(
            INGEST_INSTANCE_ID,
            f"sudo -u ec2-user bash -lc 'echo {cmd} | base64 -d | bash'",
            timeout_seconds=30,
            poll_seconds=3,
        )
    except Exception:
        pass  # Slack failure is never fatal


# ---------- AWS helpers ----------
def aws(args, check=True):
    profile = [] if any(a.startswith("--profile") for a in args) else ["--profile", "dpla"]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(instance_id, shell_cmd, timeout_seconds=60, poll_seconds=5):
    """Run a shell command on an EC2 instance via SSM. Returns stdout."""
    params = json.dumps({"commands": [shell_cmd]})
    cmd_id = aws([
        "ssm", "send-command",
        "--instance-ids", instance_id,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", str(timeout_seconds),
        "--parameters", params,
        "--region", REGION,
        "--query", "Command.CommandId",
        "--output", "text",
    ])
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", instance_id,
                      "--region", REGION,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", instance_id, "--region", REGION,
               "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", instance_id, "--region", REGION,
               "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


# ---------- UI helpers ----------
def step(n, title):
    print()
    print("=" * 70)
    print(f"  STEP {n}: {title}")
    print("=" * 70)


def ok(msg):   print(f"  [GOOD]    {msg}")
def warn(msg): print(f"  [CAUTION] {msg}")
def bad(msg):  print(f"  [BAD]     {msg}")
def info(msg): print(f"  {msg}")


def confirm(msg, default_yes=True):
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        answer = input(f"\n  {msg}{suffix}").strip().lower()
    except EOFError:
        answer = ""
    ok_answer = answer not in ("n", "no") if default_yes else answer in ("y", "yes")
    if not ok_answer:
        sys.exit("Aborted.")


# ---------- pre-flight checks ----------

def check_no_running_cluster():
    step(1, "Check for existing sparkindexer EMR cluster")
    out = aws([
        "emr", "list-clusters",
        "--cluster-states", "STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING",
        "--query", "Clusters[?Name=='sparkindexer'].{Name:Name,Id:Id,State:Status.State}",
        "--output", "json",
        "--region", REGION,
    ])
    clusters = json.loads(out)
    if clusters:
        for c in clusters:
            bad(f"Cluster already running: {c['Id']} ({c['State']})")
        sys.exit("Cannot launch — a sparkindexer cluster is already active. Wait for it to finish or terminate it.")
    ok("No active sparkindexer cluster.")


def check_hub_snapshot_ages():
    step(2, "Check hub snapshot ages (warn if >45 days old)")
    out = aws(["s3", "ls", f"s3://{S3_DATASET}/", "--region", REGION])
    hubs = [line.strip().rstrip("/").split()[-1] for line in out.splitlines() if line.strip().endswith("/")]

    now = datetime.now(timezone.utc)
    stale = []
    for hub in sorted(hubs):
        ls = aws(["s3", "ls", f"s3://{S3_DATASET}/{hub}/jsonl/", "--region", REGION], check=False)
        if not ls.strip():
            continue
        lines = [l for l in ls.splitlines() if l.strip()]
        if not lines:
            continue
        latest = sorted(lines)[-1]
        # Date is first field: 2026-03-21
        date_str = latest.strip().split()[0]
        try:
            snap_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            age_days = (now - snap_date).days
            if age_days > STALE_DAYS:
                stale.append((hub, age_days, date_str))
        except ValueError:
            pass

    if stale:
        warn(f"{len(stale)} hub(s) with snapshots older than {STALE_DAYS} days:")
        for hub, age, date in stale:
            info(f"    {hub:<25} {age} days old  (last: {date})")
        confirm("Proceed anyway?", default_yes=False)
    else:
        ok(f"All hub snapshots are within {STALE_DAYS} days.")


def check_batch_output():
    step(3, "Check for existing batch output")
    now = datetime.now()
    batch_path = f"s3://{S3_BATCH_BASE}/{now.year}/{now.month:02d}/all.parquet/"
    info(f"Checking: {batch_path}")
    ls = aws(["s3", "ls", batch_path, "--region", REGION], check=False)
    if ls.strip():
        warn(f"Batch output already exists at {batch_path}")
        info("Spark will refuse to write if this path exists.")
        confirm("Delete it and continue?", default_yes=False)
        aws(["s3", "rm", batch_path, "--recursive", "--region", REGION])
        ok("Deleted existing batch output.")
    else:
        ok("No existing batch output — clear to launch.")


def rebuild_jar():
    """Build the sparkindexer JAR on the ingest EC2 and upload it to S3."""
    info(f"Starting JAR rebuild on {INGEST_INSTANCE_ID} — this takes ~5-10 minutes...")
    slack_notify(":hammer: *sparkindexer JAR rebuild started* — running `sbt assembly` on ingest EC2 (~5–10 min).")

    # Step 1: sbt assembly — long timeout, poll every 15 s
    # Must run as ec2-user login shell so sbt is on PATH (SSM runs non-login by default)
    info("Running: cd sparkindexer && sbt assembly ...")
    try:
        out = ssm_run(
            INGEST_INSTANCE_ID,
            f"sudo -u ec2-user bash -l -c 'cd {SPARKINDEXER_DIR} && sbt assembly'",
            timeout_seconds=900,
            poll_seconds=15,
        )
    except RuntimeError as e:
        bad(f"sbt assembly failed:\n{e}")
        slack_notify(":x: *sparkindexer JAR rebuild FAILED* — `sbt assembly` error on ingest EC2.")
        sys.exit(1)

    # Print last 30 lines of sbt output (it's verbose)
    lines = out.strip().splitlines()
    if len(lines) > 30:
        print(f"\n  ... (showing last 30 of {len(lines)} lines)\n")
        print("\n".join(f"  {l}" for l in lines[-30:]))
    else:
        print("\n".join(f"  {l}" for l in lines))

    # Confirm sbt reported success
    if "[success]" not in out.lower():
        bad("sbt output does not contain [success] — build may have failed.")
        confirm("Continue anyway and upload whatever JAR is there?", default_yes=False)
    else:
        ok("sbt assembly succeeded.")

    # Step 2: upload JAR to S3 (also needs login shell for aws CLI path)
    info(f"Uploading to {SPARKINDEXER_JAR} ...")
    try:
        ssm_run(
            INGEST_INSTANCE_ID,
            f"sudo -u ec2-user bash -l -c 'aws s3 cp {SPARKINDEXER_LOCAL_JAR} {SPARKINDEXER_JAR}'",
            timeout_seconds=120,
            poll_seconds=5,
        )
    except RuntimeError as e:
        bad(f"S3 upload failed:\n{e}")
        slack_notify(":x: *sparkindexer JAR S3 upload FAILED* — built OK but couldn't upload.")
        sys.exit(1)

    ok("JAR uploaded to S3.")

    # Step 3: verify the new JAR is visible on S3
    new_ls = aws(["s3", "ls", SPARKINDEXER_JAR, "--region", REGION], check=False)
    if new_ls.strip():
        parts = new_ls.strip().split()
        info(f"S3 JAR: {parts[0]} {parts[1]}  ({parts[2]} bytes)")
        ok("JAR rebuild and upload complete.")
        slack_notify(f":white_check_mark: *sparkindexer JAR rebuilt and uploaded to S3* :tada:\nModified: {parts[0]} {parts[1]}")
    else:
        warn("Could not verify JAR on S3 after upload — check manually.")


def check_jar_freshness():
    step(4, "Check sparkindexer JAR freshness")
    out = aws(["s3", "ls", SPARKINDEXER_JAR, "--region", REGION], check=False)
    if not out.strip():
        bad(f"JAR not found at {SPARKINDEXER_JAR}")
        sys.exit("Cannot launch — JAR is missing. Build and upload it first.")
    # Parse date from ls output: "2026-04-15 10:23:11  12345678 sparkindexer-assembly.jar"
    parts = out.strip().split()
    if len(parts) >= 2:
        try:
            jar_date = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M:%S")
            age_days = (datetime.utcnow() - jar_date).days
            info(f"JAR last modified: {parts[0]} ({age_days} days ago)")
            if age_days > 30:
                warn(f"JAR is {age_days} days old — may be stale.")
                print()
                print("  Options:")
                print("    r) Rebuild JAR on EC2 and upload to S3 (takes ~5-10 min)")
                print("    p) Proceed with existing JAR")
                print("    a) Abort")
                try:
                    choice = input("  Choice [r/p/a]: ").strip().lower()
                except EOFError:
                    choice = "a"
                if choice == "r":
                    rebuild_jar()
                elif choice == "p":
                    ok("Proceeding with existing JAR.")
                else:
                    sys.exit("Aborted.")
            else:
                ok(f"JAR is {age_days} days old — looks fresh.")
        except ValueError:
            warn("Could not parse JAR date — check manually.")
    else:
        warn("Could not parse JAR listing — check manually.")


# ---------- launch cluster ----------

def launch_cluster():
    step(5, "Launch sparkindexer EMR cluster")
    confirm("All pre-flight checks passed. Launch the cluster now?")

    cluster_id = aws([
        "emr", "create-cluster",
        "--auto-terminate",
        "--applications", "Name=Hadoop", "Name=Hive", "Name=Pig", "Name=Hue", "Name=Spark",
        "--ebs-root-volume-size", "75",
        "--ec2-attributes", json.dumps({
            "KeyName": "general",
            "InstanceProfile": "sparkindexer-s3",
            "SubnetId": "subnet-90afd9ba",
            "ServiceAccessSecurityGroup": "sg-07459c7a",
            "EmrManagedSlaveSecurityGroup": "sg-0a459c77",
            "EmrManagedMasterSecurityGroup": "sg-08459c75",
        }),
        "--service-role", "EMR_Default_Role_v2",
        "--enable-debugging",
        "--release-label", "emr-7.10.0",
        "--log-uri", EMR_LOG_URI,
        "--tags", "for-use-with-amazon-emr-managed-policies=true",
        "--steps", json.dumps([{
            "Args": [
                "spark-submit", "--deploy-mode", "cluster",
                "--driver-memory", "18G",
                "--executor-memory", "18G",
                "--num-executors", "4",
                "--executor-cores", "2",
                "--class", "dpla.ingestion3.indexer.IndexerMain",
                SPARKINDEXER_JAR,
                ES_HOST_NAME, ES_PORT, "dpla-all", "all", "now",
                "3", "1", "dpla-master-dataset", "tech@dp.la",
            ],
            "Type": "CUSTOM_JAR",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "Jar": "command-runner.jar",
            "Properties": "",
            "Name": "spark-indexer",
        }]),
        "--name", "sparkindexer",
        "--instance-groups", json.dumps([
            {
                "InstanceCount": 9,
                "InstanceGroupType": "CORE",
                "InstanceType": "m6g.2xlarge",
                "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 250, "VolumeType": "gp3"}, "VolumesPerInstance": 2}]},
                "Name": "Core - 2",
            },
            {
                "InstanceCount": 1,
                "InstanceGroupType": "MASTER",
                "InstanceType": "m6g.2xlarge",
                "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 250, "VolumeType": "gp3"}, "VolumesPerInstance": 2}]},
                "Name": "Master - 1",
            },
        ]),
        "--scale-down-behavior", "TERMINATE_AT_TASK_COMPLETION",
        "--region", REGION,
        "--query", "ClusterId",
        "--output", "text",
    ])

    print(f"\n  Cluster ID: {cluster_id}")
    print(f"  Logs: {EMR_LOG_URI}{cluster_id}/")
    slack_notify(f":rocket: *sparkindexer cluster launched* — `{cluster_id}`\nMonitoring every {POLL_SECONDS // 60} min. spark-indexer step takes ~6–9 hours.")
    return cluster_id


# ---------- monitor cluster ----------

def monitor_cluster(cluster_id):
    step(6, f"Monitoring cluster {cluster_id}")
    info("Polling every 5 minutes. spark-indexer step takes ~6-9 hours once RUNNING.")
    info("Press Ctrl+C to stop monitoring (cluster keeps running).\n")

    # Seed last_state with the current cluster state so that resuming an
    # already-running cluster doesn't re-fire the RUNNING Slack notification.
    _seed = aws(["emr", "describe-cluster", "--cluster-id", cluster_id,
                 "--query", "Cluster.Status.State", "--output", "text",
                 "--region", REGION], check=False).strip()
    last_state = _seed if _seed else None
    # Use the cluster's actual ready time so elapsed % is correct when resuming.
    _ready = aws(["emr", "describe-cluster", "--cluster-id", cluster_id,
                  "--query", "Cluster.Status.Timeline.ReadyDateTime",
                  "--output", "text", "--region", REGION], check=False).strip()
    try:
        from datetime import timezone
        start_time = datetime.fromisoformat(_ready).astimezone(timezone.utc).timestamp() if _ready and _ready != "None" else time.time()
    except (ValueError, TypeError):
        start_time = time.time()
    last_heartbeat = time.time()
    HEARTBEAT_INTERVAL = 3600  # 1 hour
    try:
        while True:
            out = aws([
                "emr", "describe-cluster",
                "--cluster-id", cluster_id,
                "--query", "Cluster.{State:Status.State,StateDetail:Status.StateChangeReason.Message}",
                "--output", "json",
                "--region", REGION,
            ])
            data = json.loads(out)
            state = data.get("State", "UNKNOWN")
            detail = data.get("StateDetail") or ""

            elapsed_s  = time.time() - start_time
            elapsed_h  = elapsed_s / 3600
            EXPECTED_H = 7.5  # midpoint of 6-9h window
            pct        = min(int(elapsed_s / (EXPECTED_H * 3600) * 100), 99) if state == "RUNNING" else 0
            bar_fill   = int(pct / 5)
            bar        = "█" * bar_fill + "░" * (20 - bar_fill)
            ts         = datetime.now().strftime("%H:%M:%S")

            if state != last_state:
                print(f"  [{ts}] {state}  {detail}")
                last_state = state
                if state == "RUNNING":
                    slack_notify(f":large_green_circle: *sparkindexer RUNNING* — `{cluster_id}`\nspark-indexer step now executing (~6–9 hours).")
                elif state == "TERMINATING":
                    slack_notify(f":hourglass: *sparkindexer TERMINATING* — `{cluster_id}`\n{detail or 'wrapping up...'}")

            if state == "RUNNING":
                print(f"\r  [{ts}] {state}  [{bar}] ~{pct}%  ({elapsed_h:.1f}h elapsed)", end="", flush=True)

            # Hourly heartbeat while RUNNING
            if state == "RUNNING" and time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                print()
                slack_notify(f":beating_heart: *sparkindexer still running* — `{cluster_id}`\nElapsed: {elapsed_h:.1f}h (~{pct}% through expected window)")
                last_heartbeat = time.time()

            if state == "TERMINATED":
                # Check if it terminated successfully
                out2 = aws([
                    "emr", "describe-cluster",
                    "--cluster-id", cluster_id,
                    "--query", "Cluster.Status.StateChangeReason.Code",
                    "--output", "text",
                    "--region", REGION,
                ])
                if out2.strip() == "ALL_STEPS_COMPLETED":
                    ok("Cluster terminated successfully — all steps completed.")
                    slack_notify(f":white_check_mark: *sparkindexer COMPLETE* — `{cluster_id}`\nAll steps completed. Ready for ES alias swap.")
                    return True
                else:
                    bad(f"Cluster terminated with reason: {out2.strip()}")
                    info(f"Check logs: {EMR_LOG_URI}{cluster_id}/")
                    slack_notify(f":x: *sparkindexer FAILED* — `{cluster_id}`\nReason: {out2.strip()}\nLogs: `{EMR_LOG_URI}{cluster_id}/`")
                    return False

            if state in ("TERMINATING", "TERMINATED_WITH_ERRORS"):
                bad(f"Cluster {state}: {detail}")
                info(f"Check logs: {EMR_LOG_URI}{cluster_id}/")
                slack_notify(f":x: *sparkindexer {state}* — `{cluster_id}`\n{detail}\nLogs: `{EMR_LOG_URI}{cluster_id}/`")
                return False

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\n  Monitoring stopped. Cluster {cluster_id} is still running.")
        print(f"  Re-run this script with --cluster-id {cluster_id} to resume monitoring.")
        sys.exit(0)


# ---------- alias swap ----------

def do_alias_swap():
    step(7, "Elasticsearch alias swap")

    # Find current indices
    info("Fetching current dpla-all-* indices from search-prod1...")
    out = ssm_run(
        ES_INSTANCE_ID,
        f"curl -s '{ES_HOST}/_cat/indices/dpla-all-*?s=creation.date:desc&v&h=index,docs.count,creation.date.string' 2>/dev/null | head -10",
        timeout_seconds=60,
    )
    print(f"\n{out.rstrip()}\n")

    # Find current alias target
    alias_out = ssm_run(
        ES_INSTANCE_ID,
        f"curl -s '{ES_HOST}/_alias/dpla_alias' 2>/dev/null",
        timeout_seconds=60,
    )
    try:
        alias_data = json.loads(alias_out)
        current_live = list(alias_data.keys())[0] if alias_data else None
    except (json.JSONDecodeError, IndexError):
        current_live = None

    if current_live:
        info(f"Current live index (dpla_alias): {current_live}")
    else:
        warn("Could not determine current live index.")

    try:
        new_index = input("\n  Enter the NEW index name to make live: ").strip()
    except EOFError:
        sys.exit("No input provided.")
    if not new_index:
        sys.exit("No index name entered.")

    old_index = current_live or input("  Enter the OLD index name to remove alias from: ").strip()

    # Fetch old index doc count from ES before swapping
    old_count = None
    try:
        count_out = ssm_run(
            ES_INSTANCE_ID,
            f"curl -s '{ES_HOST}/{old_index}/_count' 2>/dev/null",
            timeout_seconds=30,
        )
        old_count = json.loads(count_out).get("count")
        if old_count is not None:
            info(f"Old index doc count: {old_count:,}")
    except Exception:
        warn("Could not fetch old index doc count.")

    print(f"\n  OLD index: {old_index}  ({f'{old_count:,} docs' if old_count else 'count unknown'})")
    print(f"  NEW index: {new_index}")
    print(f"  This will swap dpla_alias from {old_index} → {new_index} and make it LIVE on dp.la.")
    confirm("Confirm alias swap?", default_yes=False)

    swap_payload = json.dumps({
        "actions": [
            {"remove": {"index": old_index, "alias": "dpla_alias"}},
            {"add":    {"index": new_index, "alias": "dpla_alias"}},
        ]
    })
    swap_cmd = f"curl -s -X POST '{ES_HOST}/_aliases' -H 'Content-Type: application/json' -d '{swap_payload}' 2>/dev/null"
    result = ssm_run(ES_INSTANCE_ID, swap_cmd, timeout_seconds=60)
    print(f"\n  Response: {result.strip()}")

    if '"acknowledged":true' in result:
        ok("Alias swap successful.")
        slack_notify(f":arrows_counterclockwise: *ES alias swap complete*\nOld: `{old_index}`\nNew: `{new_index}` (now live on dp.la)")
    else:
        bad("Alias swap may have failed — check the response above.")
        slack_notify(f":warning: *ES alias swap may have failed* — check search-prod1\nAttempted: `{old_index}` → `{new_index}`")
        return old_index, new_index, old_count

    return old_index, new_index, old_count


def load_api_key():
    """Load DPLA_API_KEY from ~/.dpla-secrets.env if present."""
    secrets_path = os.path.expanduser("~/.dpla-secrets.env")
    if os.path.exists(secrets_path):
        with open(secrets_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DPLA_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("DPLA_API_KEY", "")


def verify_api(old_count=None):
    step(8, "Verify API reflects new index")
    info("Checking DPLA API record count...")

    api_key = load_api_key()
    if not api_key:
        warn("DPLA_API_KEY not found in ~/.dpla-secrets.env or environment.")
        info("Set it to verify the API count, or check manually:")
        info("  curl 'https://api.dp.la/v2/items?api_key=YOUR_KEY&page_size=0'")
        return

    try:
        import urllib.request
        req = urllib.request.Request(
            "https://api.dp.la/v2/items?page_size=0",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        count = data.get("count")
        if count is None:
            warn("No count in API response — check manually.")
            return

        count_str = f"{count:,}"
        if old_count is not None:
            delta     = count - old_count
            sign      = "+" if delta >= 0 else ""
            delta_str = f"{sign}{delta:,}"
            delta_pct = f"{sign}{delta / old_count * 100:.1f}%" if old_count else "N/A"
            info(f"Old index docs : {old_count:,}")
            info(f"New index docs : {count_str}  (Δ {delta_str} / {delta_pct})")
            slack_msg = (
                f":tada: *Index rebuild complete* — API live\n"
                f"New: {count_str} records  (Δ {delta_str} / {delta_pct} vs old index)"
            )
        else:
            info(f"API total records: {count_str}")
            slack_msg = f":tada: *Index rebuild complete* — API live\nRecords: {count_str}"

        ok("API is responding with the new index.")
        slack_notify(slack_msg)

    except Exception as e:
        warn(f"Could not verify API: {e}")
        info("Check manually: curl 'https://api.dp.la/v2/items?api_key=YOUR_KEY&page_size=0'")


# ---------- main ----------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="DPLA sparkindexer launch script")
    parser.add_argument("--cluster-id", help="Resume monitoring an existing cluster")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip pre-flight checks")
    parser.add_argument("--alias-swap-only", action="store_true", help="Skip launch, just do alias swap")
    parser.add_argument("--verify-only", action="store_true", help="Just check API count, show delta, and Slack notify")
    args = parser.parse_args()

    print("\nDPLA SPARKINDEXER")
    print(f"Region: {REGION}")
    print(f"Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.verify_only:
        verify_api()
        return

    if args.alias_swap_only:
        old_index, new_index, old_count = do_alias_swap()
        verify_api(old_count=old_count)
        print()
        print("=" * 70)
        print("  Alias swap complete!")
        print(f"  Old index: {old_index}")
        print(f"  New index: {new_index} (now live)")
        print(f"  Remember to delete old indices — keep current + 1 prior for rollback.")
        print("=" * 70)
        return

    if args.cluster_id:
        # Resume monitoring existing cluster
        print(f"\nResuming monitoring for cluster: {args.cluster_id}")
        success = monitor_cluster(args.cluster_id)
        if not success:
            sys.exit(1)
    else:
        # Full flow
        if not args.skip_preflight:
            check_no_running_cluster()
            check_hub_snapshot_ages()
            check_batch_output()
            check_jar_freshness()
        else:
            print("\n  Pre-flight checks skipped.")

        cluster_id = launch_cluster()
        success = monitor_cluster(cluster_id)
        if not success:
            sys.exit(1)

    # Alias swap (always pauses for confirmation)
    old_index, new_index, old_count = do_alias_swap()
    verify_api(old_count=old_count)

    print()
    print("=" * 70)
    print("  Indexer complete!")
    print(f"  New index: {new_index} (live)")
    print(f"  Old index: {old_index} (keep for rollback)")
    print()
    print("  Next: delete indices older than the previous one.")
    print("  Keep: current live + one prior. Delete everything else.")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
