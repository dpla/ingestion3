#!/usr/bin/env python3
"""
DPLA Post-Indexer: monthly batch jobs.

Run immediately after launch_indexer.py completes the alias swap.

Steps:
  1. Check / rebuild batch JAR  (s3://dpla-monthly-batch/)
  2. Launch monthlybatch EMR cluster  (parquet → jsonl → mq → sitemap)
  3. Monitor cluster with hourly heartbeat
  4. On completion: terminate cluster, run hub stats, trigger sitemaps
  5. Verify S3 outputs

Usage:
    python3 post_indexer.py
    python3 post_indexer.py --cluster-id j-XXXXX   # resume monitoring
    python3 post_indexer.py --skip-preflight        # skip JAR check
"""

import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime

# ---------- config ----------
REGION             = "us-east-1"

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
INGEST_INSTANCE_ID = _env.get("INGEST_INSTANCE_ID", "")
AWS_ACCOUNT_ID     = _env.get("AWS_ACCOUNT_ID", "")
EMR_LOG_URI        = f"s3://aws-logs-{AWS_ACCOUNT_ID}-us-east-1/elasticmapreduce/"

BATCH_JAR_BUCKET   = "s3://dpla-monthly-batch/"
BATCH_JAR_NAME     = "batch-process-dpla-index-assembly.jar"
BATCH_JAR_S3       = f"s3://dpla-monthly-batch/{BATCH_JAR_NAME}"
BATCH_JAR_EMR      = BATCH_JAR_S3  # same for EMR step args
BATCH_REPO_DIR     = _env.get("BATCH_REPO_DIR",
                               os.path.expanduser("~/Documents/Repos/batch-process-dpla-index"))
BATCH_LOCAL_JAR    = f"{BATCH_REPO_DIR}/target/scala-2.12/{BATCH_JAR_NAME}"

MASTER_DATASET     = "dpla-master-dataset"
PARQUET_OUT        = "s3a://dpla-provider-export/"
JSONL_OUT          = "s3a://dpla-provider-export/"
MQ_OUT             = "s3a://dashboard-analytics/"
SITEMAP_OUT        = "s3a://sitemaps.dp.la/sitemap/"
SITEMAP_ROOT       = "https://dp.la/sitemap/"

POLL_SECONDS       = 300   # 5 min
HEARTBEAT_INTERVAL = 3600  # 1 hour
STALE_JAR_DAYS     = 30


# ---------- Slack (via EC2 common.sh — same as ingest.sh) ----------
def slack_notify(msg):
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


# ---------- Step 1: JAR check ----------

def rebuild_jar():
    """Build batch JAR locally and upload to S3."""
    info(f"Building batch JAR locally in {BATCH_REPO_DIR} (~5-10 min)...")
    slack_notify(":hammer: *batch JAR rebuild started* — running `sbt assembly` locally.")

    result = subprocess.run(
        ["sbt", "clean", "assembly"],
        cwd=BATCH_REPO_DIR,
        text=True,
        capture_output=False,  # stream output live to terminal
    )
    if result.returncode != 0:
        bad("sbt assembly failed.")
        slack_notify(":x: *batch JAR rebuild FAILED*")
        sys.exit(1)
    ok("sbt assembly succeeded.")

    info(f"Uploading to {BATCH_JAR_S3} ...")
    try:
        aws(["s3", "cp", BATCH_LOCAL_JAR, BATCH_JAR_S3])
    except RuntimeError as e:
        bad(f"S3 upload failed:\n{e}")
        sys.exit(1)

    ok("Batch JAR uploaded to S3.")
    slack_notify(f":white_check_mark: *batch JAR rebuilt and uploaded* — `{BATCH_JAR_S3}`")


def check_jar_freshness():
    step(1, "Check batch JAR freshness")
    out = aws(["s3", "ls", BATCH_JAR_S3, "--region", REGION], check=False)
    if not out.strip():
        bad(f"JAR not found at {BATCH_JAR_S3}")
        confirm("Build and upload it now?", default_yes=False)
        rebuild_jar()
        return

    parts = out.strip().split()
    try:
        jar_date = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M:%S")
        age_days = (datetime.now() - jar_date).days
        info(f"JAR last modified: {parts[0]} ({age_days} days ago)")
        if age_days > STALE_JAR_DAYS:
            warn(f"JAR is {age_days} days old — may be stale.")
            print()
            print("  Options:")
            print("    r) Rebuild JAR on EC2 and upload to S3 (~5-10 min)")
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
    except (ValueError, IndexError):
        warn("Could not parse JAR date — check manually.")


# ---------- Step 2: launch cluster ----------

def launch_cluster():
    step(2, "Launch monthlybatch EMR cluster")
    info("Steps: parquet → jsonl → mq → sitemap")
    info("Cluster launched from EC2 via SSM (instance role has required IAM permissions).")
    confirm("Launch the batch cluster now?")

    # Build steps and cluster config as JSON, then pass to aws emr create-cluster
    # via SSM so it runs under the EC2 instance role (avoids iam:PassRole issues for local user)
    emr_steps = json.dumps([
        {
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--class", "dpla.batch_process_dpla_index.processes.ParquetDump",
                     BATCH_JAR_S3, MASTER_DATASET, PARQUET_OUT],
            "Type": "CUSTOM_JAR", "ActionOnFailure": "CANCEL_AND_WAIT",
            "Jar": "command-runner.jar", "Properties": "", "Name": "parquet",
        },
        {
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--class", "dpla.batch_process_dpla_index.processes.JsonlDump",
                     BATCH_JAR_S3, MASTER_DATASET, JSONL_OUT],
            "Type": "CUSTOM_JAR", "ActionOnFailure": "CANCEL_AND_WAIT",
            "Jar": "command-runner.jar", "Properties": "", "Name": "jsonl",
        },
        {
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--class", "dpla.batch_process_dpla_index.processes.MqReports",
                     BATCH_JAR_S3, PARQUET_OUT, MQ_OUT],
            "Type": "CUSTOM_JAR", "ActionOnFailure": "CANCEL_AND_WAIT",
            "Jar": "command-runner.jar", "Properties": "", "Name": "mq",
        },
        {
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "--class", "dpla.batch_process_dpla_index.processes.Sitemap",
                     BATCH_JAR_S3, PARQUET_OUT, SITEMAP_OUT, SITEMAP_ROOT],
            "Type": "CUSTOM_JAR", "ActionOnFailure": "CANCEL_AND_WAIT",
            "Jar": "command-runner.jar", "Properties": "", "Name": "sitemap",
        },
    ])
    ec2_attrs = json.dumps({
        "EmrManagedMasterSecurityGroup": "sg-08459c75",
        "EmrManagedSlaveSecurityGroup":  "sg-0a459c77",
        "InstanceProfile": "sparkindexer-s3",
        "KeyName": "general",
        "ServiceAccessSecurityGroup": "sg-07459c7a",
        "SubnetId": "subnet-90afd9ba",
    })
    instance_groups = json.dumps([
        {
            "InstanceCount": 8,
            "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 250, "VolumeType": "gp3"}, "VolumesPerInstance": 2}]},
            "InstanceGroupType": "CORE", "InstanceType": "r8g.xlarge", "Name": "Core - 2",
        },
        {
            "InstanceCount": 1,
            "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp3"}, "VolumesPerInstance": 2}]},
            "InstanceGroupType": "MASTER", "InstanceType": "m8g.xlarge", "Name": "Master - 1",
        },
    ])
    configurations = json.dumps([{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}])

    # Write JSON args to local temp files to avoid subprocess quoting issues
    tmp_files = []
    def write_tmp(content):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        f.write(content)
        f.close()
        tmp_files.append(f.name)
        return f.name

    steps_f  = write_tmp(emr_steps)
    ec2_f    = write_tmp(ec2_attrs)
    ig_f     = write_tmp(instance_groups)
    conf_f   = write_tmp(configurations)

    try:
        cluster_id = aws([
            "emr", "create-cluster",
            "--no-auto-terminate",
            "--auto-scaling-role", "EMR_AutoScaling_DefaultRole",
            "--applications", "Name=Hadoop", "Name=Hive", "Name=Spark",
            "--ebs-root-volume-size", "100",
            "--configurations", f"file://{conf_f}",
            "--ec2-attributes", f"file://{ec2_f}",
            "--service-role", "EMR_Default_Role_v2",
            "--enable-debugging",
            "--release-label", "emr-7.10.0",
            "--log-uri", EMR_LOG_URI,
            "--tags", "for-use-with-amazon-emr-managed-policies=true",
            "--steps", f"file://{steps_f}",
            "--name", "monthlybatch",
            "--instance-groups", f"file://{ig_f}",
            "--scale-down-behavior", "TERMINATE_AT_TASK_COMPLETION",
            "--region", REGION,
            "--query", "ClusterId",
            "--output", "text",
        ]).strip()
    finally:
        for f in tmp_files:
            try:
                os.unlink(f)
            except OSError:
                pass

    if not cluster_id.startswith("j-"):
        raise RuntimeError(f"Unexpected cluster ID output: {cluster_id}")

    print(f"\n  Cluster ID: {cluster_id}")
    print(f"  Logs: {EMR_LOG_URI}{cluster_id}/")
    slack_notify(
        f":rocket: *monthlybatch cluster launched* — `{cluster_id}`\n"
        f"Steps: parquet → jsonl → mq → sitemap. Polling every {POLL_SECONDS // 60} min."
    )
    return cluster_id


# ---------- Step 3: monitor ----------

def get_step_statuses(cluster_id):
    """Returns list of {{Name, State}} newest-first."""
    out = aws([
        "emr", "list-steps",
        "--cluster-id", cluster_id,
        "--query", "Steps[*].{Name:Name,State:Status.State}",
        "--output", "json",
        "--region", REGION,
    ])
    return json.loads(out)


def monitor_cluster(cluster_id):
    step(3, f"Monitoring cluster {cluster_id}")
    info("Polling every 5 min. Steps run sequentially: parquet → jsonl → mq → sitemap.")
    info("Cluster stays WAITING when done (no-auto-terminate) — script will terminate it.")
    info("Press Ctrl+C to stop monitoring (cluster keeps running).\n")

    last_state       = None
    last_steps_str   = None
    start_time       = time.time()
    last_heartbeat   = time.time()

    try:
        while True:
            out = aws([
                "emr", "describe-cluster",
                "--cluster-id", cluster_id,
                "--query", "Cluster.{State:Status.State,Detail:Status.StateChangeReason.Message}",
                "--output", "json",
                "--region", REGION,
            ])
            data   = json.loads(out)
            state  = data.get("State", "UNKNOWN")
            detail = data.get("Detail") or ""

            steps      = get_step_statuses(cluster_id)
            steps_str  = " → ".join(f"{s['Name']}:{s['State']}" for s in reversed(steps))

            if state != last_state or steps_str != last_steps_str:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] {state}  |  {steps_str}")
                prev_state     = last_state
                last_state     = state
                last_steps_str = steps_str
                if state == "RUNNING" and prev_state != "RUNNING":
                    slack_notify(f":large_green_circle: *monthlybatch RUNNING* — `{cluster_id}`")

            # Hourly heartbeat
            if state == "RUNNING" and time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                elapsed_h    = (time.time() - start_time) / 3600
                current_step = next((s["Name"] for s in reversed(steps) if s["State"] == "RUNNING"), "?")
                slack_notify(
                    f":beating_heart: *monthlybatch still running* — `{cluster_id}`\n"
                    f"Elapsed: {elapsed_h:.1f}h | Current step: {current_step}"
                )
                last_heartbeat = time.time()

            # WAITING = all steps finished (cluster kept alive by --no-auto-terminate)
            if state == "WAITING":
                failed    = [s["Name"] for s in steps if s["State"] in ("FAILED", "CANCELLED")]
                completed = [s["Name"] for s in steps if s["State"] == "COMPLETED"]

                if failed:
                    bad(f"Steps failed/cancelled: {', '.join(failed)}")
                    slack_notify(
                        f":x: *monthlybatch steps FAILED* — `{cluster_id}`\n"
                        f"Failed: {', '.join(failed)}\n"
                        f"Logs: `{EMR_LOG_URI}{cluster_id}/`"
                    )
                    terminate_cluster(cluster_id)
                    return False

                if len(completed) == len(steps):
                    ok(f"All {len(steps)} steps completed: {', '.join(completed)}")
                    slack_notify(
                        f":white_check_mark: *monthlybatch steps complete* — `{cluster_id}`\n"
                        f"All done: {', '.join(completed)}"
                    )
                    terminate_cluster(cluster_id)
                    return True

            # Unexpected early termination
            if state == "TERMINATED":
                code = aws([
                    "emr", "describe-cluster", "--cluster-id", cluster_id,
                    "--query", "Cluster.Status.StateChangeReason.Code",
                    "--output", "text", "--region", REGION,
                ], check=False).strip()
                if code == "ALL_STEPS_COMPLETED":
                    ok("Cluster terminated — all steps completed.")
                    return True
                else:
                    bad(f"Cluster terminated unexpectedly: {code} — {detail}")
                    slack_notify(f":x: *monthlybatch TERMINATED unexpectedly* — `{cluster_id}`\nCode: {code}")
                    return False

            if state == "TERMINATED_WITH_ERRORS":
                bad(f"Cluster terminated with errors: {detail}")
                slack_notify(f":x: *monthlybatch TERMINATED WITH ERRORS* — `{cluster_id}`\n{detail}")
                return False

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\n  Monitoring stopped. Cluster {cluster_id} is still running.")
        print(f"  Re-run with --cluster-id {cluster_id} to resume.")
        sys.exit(0)


def terminate_cluster(cluster_id):
    info(f"Terminating cluster {cluster_id}...")
    aws(["emr", "terminate-clusters", "--cluster-ids", cluster_id, "--region", REGION])
    ok("Cluster termination requested.")


# ---------- Step 4: hub stats ----------

def run_hub_stats():
    step(4, "Run hub stats")
    info("Updating ingestion3 on EC2 (fetch + reset --hard)...")
    try:
        ssm_run(
            INGEST_INSTANCE_ID,
            "sudo -u ec2-user bash -lc 'cd /home/ec2-user/ingestion3 && git fetch origin && git reset --hard origin/main'",
            timeout_seconds=60,
            poll_seconds=5,
        )
        ok("ingestion3 updated on EC2.")
    except RuntimeError as e:
        warn(f"git fetch/reset failed (proceeding anyway):\n{e}")

    info("Running generate_hub_stats.py on ingest EC2 (up to 3 min)...")
    try:
        out = ssm_run(
            INGEST_INSTANCE_ID,
            (
                "sudo -u ec2-user bash -lc '"
                "cd /home/ec2-user/ingestion3 && "
                "python3 scripts/generate_hub_stats.py"
                "' 2>&1"
            ),
            timeout_seconds=180,
            poll_seconds=10,
        )
        print(out[-2000:] if len(out) > 2000 else out)
        ok("Hub stats complete.")
        slack_notify(":bar_chart: *hub stats generated* :white_check_mark:")
    except RuntimeError as e:
        bad(f"Hub stats failed:\n{e}")
        slack_notify(":warning: *hub stats FAILED* — check ingest EC2")


# ---------- Step 5: sitemaps ----------

def trigger_sitemaps():
    step(5, "Trigger hub sitemaps GitHub workflow")
    import shutil
    gh = shutil.which("gh") or "/opt/homebrew/bin/gh"
    info(f"Running: {gh} workflow run generate-hub-sitemaps.yml --repo dpla/dpla-frontend --ref main")
    result = subprocess.run(
        [gh, "workflow", "run", "generate-hub-sitemaps.yml",
         "--repo", "dpla/dpla-frontend", "--ref", "main"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        ok("Sitemaps workflow triggered.")
        slack_notify(":world_map: *hub sitemaps workflow triggered* :white_check_mark:")
    else:
        bad(f"Failed to trigger sitemaps workflow:\n{result.stderr.strip()}")
        info("Make sure `gh` CLI is installed and authenticated.")
        slack_notify(":warning: *hub sitemaps workflow trigger FAILED* — run manually")


# ---------- Step 6: verify outputs ----------

def check_s3_file_today(s3_path, label):
    """Check that a specific S3 file exists and was modified today. Returns True/False."""
    today = datetime.now().strftime("%Y-%m-%d")
    ls = aws(["s3", "ls", s3_path, "--region", REGION], check=False)
    if not ls.strip():
        bad(f"{label}: NOT FOUND at {s3_path}")
        return False
    # Date is first field: "2026-05-09 15:11:55  573082 hub_stats.json"
    file_date = ls.strip().split()[0]
    if file_date == today:
        ok(f"{label}: {file_date} ✓")
        return True
    else:
        bad(f"{label}: found but last modified {file_date} (expected {today})")
        return False


def verify_outputs():
    step(6, "Verify S3 batch outputs")
    now   = datetime.now()
    today = now.strftime("%Y-%m-%d")
    year  = now.year
    month = f"{now.month:02d}"

    all_good = True

    # Provider export — check year/month prefix exists
    export_path = f"s3://dpla-provider-export/{year}/{month}/"
    ls = aws(["s3", "ls", export_path, "--region", REGION], check=False)
    if ls.strip():
        ok(f"Provider export: found at {export_path} ✓")
    else:
        bad(f"Provider export: EMPTY at {export_path}")
        all_good = False

    # Sitemaps — check _MANIFEST is from today
    if not check_s3_file_today("s3://sitemaps.dp.la/sitemap/_MANIFEST", "Sitemap _MANIFEST"):
        all_good = False

    # Hub stats — check both files are from today
    if not check_s3_file_today("s3://dashboard-analytics/hub-stats/hub_stats.json", "hub_stats.json"):
        all_good = False
    if not check_s3_file_today("s3://dashboard-analytics/hub-stats/hub_stats_bws.json", "hub_stats_bws.json"):
        all_good = False

    if all_good:
        slack_notify(
            f":tada: *post-indexer complete* — all outputs verified for {today} :white_check_mark:\n"
            f"• Provider export: `{export_path}`\n"
            f"• Sitemaps: fresh\n"
            f"• Hub stats: fresh"
        )
    else:
        slack_notify(":warning: *post-indexer done but some S3 outputs are missing or stale* — check manually.")


# ---------- main ----------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="DPLA post-indexer batch jobs")
    parser.add_argument("--cluster-id",     help="Resume monitoring an existing monthlybatch cluster")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip JAR freshness check")
    parser.add_argument("--verify-only",    action="store_true", help="Just run S3 output verification")
    args = parser.parse_args()

    print("\nDPLA POST-INDEXER")
    print(f"Region: {REGION}")
    print(f"Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.verify_only:
        verify_outputs()
        return

    if args.cluster_id:
        print(f"\nResuming monitoring for cluster: {args.cluster_id}")
        success = monitor_cluster(args.cluster_id)
    else:
        if not args.skip_preflight:
            check_jar_freshness()
        cluster_id = launch_cluster()
        success    = monitor_cluster(cluster_id)

    if not success:
        print()
        bad("Batch cluster did not complete successfully.")
        confirm("Run hub stats and sitemaps anyway?", default_yes=False)

    run_hub_stats()
    trigger_sitemaps()
    verify_outputs()

    print()
    print("=" * 70)
    print("  Post-indexer complete!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
