#!/usr/bin/env python3
"""
Community Webs (Internet Archive) ingest orchestrator.

Internet Archive delivers a SQLite .db file. This script:
  1. Uploads your local .db file to s3://dpla-scratch/community-webs/
  2. Downloads it on EC2
  3. Exports SQLite → JSONL → ZIP
  4. Places the ZIP in the standard harvest directory
  5. Prompts you to update i3.conf endpoint
  6. Runs the full ingest pipeline via ingest.sh

Usage:
    python3 launch_cw.py --db ~/Downloads/community-webs.db

Skip flags (for resuming a failed run):
    --skip-upload       .db already in s3://dpla-scratch/community-webs/
    --skip-preprocess   ZIP already on EC2 (requires --zip-dir)
    --zip-dir           EC2 path to dir containing the ZIP (use with --skip-preprocess)

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation,
           s3:PutObject/GetObject on dpla-scratch
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

# ---------- config ----------
INSTANCE_ID        = "i-0a0def8581efef783"
REPO_PATH          = "/home/ec2-user/ingestion3"
INGEST_SCRIPT      = f"{REPO_PATH}/scripts/ingest.sh"
HUB                = "community-webs"
S3_STAGING         = "s3://dpla-scratch/community-webs"
HARVEST_BASE       = "/home/ec2-user/data/community-webs/originalRecords"

PIPELINE_TIMEOUT_S = 10800  # 3 hours


# ---------- AWS / SSM helpers ----------
def aws(args):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})
    cmd_id  = aws([
        "ssm", "send-command",
        "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", str(timeout_seconds),
        "--parameters", params,
        "--query", "Command.CommandId",
        "--output", "text",
    ])
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ssm_bg(shell_cmd, log_path):
    """Fire a long-running command as a nohup background job. Returns PID."""
    launch = f"nohup bash -c {json.dumps(shell_cmd)} > {log_path} 2>&1 & echo $!"
    out = ssm_run(launch, timeout_seconds=60)
    return out.strip().split()[-1]


def wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=None):
    """Poll until process exits, tailing the log each cycle."""
    start = time.time()
    while True:
        time.sleep(poll_seconds)
        alive = ssm_run(
            f"ps -p {pid} -o pid= 2>/dev/null || echo dead",
            timeout_seconds=30,
        ).strip()
        log_tail = ssm_run(
            f"[ -f {log_path} ] && tail -20 {log_path} || echo '(no log yet)'",
            timeout_seconds=30,
        ).rstrip()
        print("\n" + "─" * 60)
        print(log_tail)
        if alive in ("dead", ""):
            break
        if timeout_seconds and (time.time() - start) > timeout_seconds:
            raise RuntimeError(f"Process {pid} timed out after {timeout_seconds}s")


# ---------- UI helpers ----------
def step(n, title):
    print()
    print("=" * 70)
    print(f"  STEP {n}: {title}")
    print("=" * 70)


def confirm(msg, default_yes=True):
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        answer = input(f"\n  {msg}{suffix}").strip().lower()
    except EOFError:
        answer = ""
    ok = answer not in ("n", "no") if default_yes else answer in ("y", "yes")
    if not ok:
        sys.exit("Aborted.")


# ---------- stages ----------

def stage_upload(db_path, timestamp):
    step(1, "Upload .db to S3")
    s3_key  = f"{timestamp}-community-webs.db"
    s3_dest = f"{S3_STAGING}/{s3_key}"
    size_mb = os.path.getsize(db_path) / 1024 / 1024
    print(f"  Local:  {db_path}  ({size_mb:.1f} MB)")
    print(f"  S3:     {s3_dest}")
    confirm(f"Upload {size_mb:.1f} MB to S3?")
    result = subprocess.run(
        ["aws", "s3", "cp", db_path, s3_dest, "--no-progress"], text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"S3 upload failed: {result.stderr.strip()}")
    print(f"  ✓ Uploaded → {s3_dest}")
    return s3_dest


def stage_preprocess(s3_db, timestamp):
    step(2, "Download .db & export to JSONL → ZIP on EC2")
    work_dir = f"{HARVEST_BASE}/{timestamp}"
    db_path  = f"{work_dir}/community-webs.db"
    zip_name = f"community-webs-{timestamp}.zip"
    zip_path = f"{work_dir}/{zip_name}"
    log_path = f"{work_dir}/preprocess.log"

    print(f"  Work dir: {work_dir}")
    ssm_run(f"mkdir -p {work_dir}", timeout_seconds=30)

    print(f"  Downloading .db from S3...")
    ssm_run(f"aws s3 cp {s3_db} {db_path}", timeout_seconds=600)

    # Run the export as a background job — sqlite3 + jq can take a while on large DBs
    print(f"  Exporting SQLite → JSONL → ZIP (background)...")
    print(f"  Log: {log_path}")
    export_cmd = (
        f"cd {work_dir} && "
        f"echo 'Exporting SQLite...' && "
        f"sqlite3 {db_path} --json 'SELECT * FROM ait' > tmp.json && "
        f"echo 'Flattening with jq...' && "
        f"jq -c '.[]' tmp.json > community-webs.jsonl && "
        f"RECORD_COUNT=$(wc -l < community-webs.jsonl) && "
        f"echo \"Records: $RECORD_COUNT\" && "
        f"echo 'Zipping...' && "
        f"zip -j {zip_name} community-webs.jsonl && "
        f"echo 'Cleaning up tmp files...' && "
        f"rm -f tmp.json community-webs.jsonl && "
        f"echo 'Done. ZIP: {zip_path}'"
    )
    pid = ssm_bg(export_cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=3600)

    # Show results
    result = ssm_run(
        f"echo '--- work dir ---' && ls -lh {work_dir}/ && "
        f"echo '--- record count ---' && grep 'Records:' {log_path} || echo '(not found in log)'",
        timeout_seconds=30,
    )
    print(f"\n{result.rstrip()}")

    confirm("Export looks good? Continue?")
    return work_dir, zip_path


def stage_conf_update(work_dir):
    step(3, "Update i3.conf endpoint")
    print(f"  The ZIP is at: {work_dir}/")
    print()
    print(f"  Update community-webs.harvest.endpoint in i3.conf to:")
    print(f"    \"{work_dir}\"")
    print()
    print("  Then push to origin/master and pull on the box:")
    print("    git add i3.conf && git commit -m 'Update community-webs endpoint' && git push")
    print("    (prechecks.py will pull it if you run it with --auto-pull)")
    print()
    confirm("i3.conf updated, pushed, and pulled on EC2?")


def stage_pipeline(timestamp):
    step(4, "Full ingest pipeline — harvest → mapping → enrichment → JSONL → S3")
    log_path = "/tmp/cw-ingest.log"
    cmd = f"bash {INGEST_SCRIPT} {HUB}"

    print(f"  Log: {log_path}")
    pid = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=PIPELINE_TIMEOUT_S)
    print("\n  Pipeline complete.")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Community Webs ingest orchestrator")
    parser.add_argument("--db", required=True,
                        help="Path to local community-webs .db file from Internet Archive")
    parser.add_argument("--timestamp",
                        help="Override timestamp (YYYYMMDD_HHMMSS). Auto-generated if omitted.")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (.db already at s3://dpla-scratch/community-webs/)")
    parser.add_argument("--skip-preprocess", action="store_true",
                        help="Skip export/zip (already done — requires --zip-dir)")
    parser.add_argument("--zip-dir",
                        help="EC2 path to directory containing the ZIP (use with --skip-preprocess)")
    args = parser.parse_args()

    db_path = os.path.expanduser(args.db)
    if not args.skip_upload and not os.path.isfile(db_path):
        sys.exit(f".db file not found: {db_path}")
    if args.skip_preprocess and not args.zip_dir:
        sys.exit("--skip-preprocess requires --zip-dir")

    timestamp = args.timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_db     = f"{S3_STAGING}/{timestamp}-community-webs.db"

    print(f"\nCOMMUNITY WEBS INGEST — {timestamp}")
    print(f"Instance:  {INSTANCE_ID}")
    print(f"Local .db: {db_path}")
    print(f"S3 stage:  {s3_db}\n")

    if not args.skip_upload:
        stage_upload(db_path, timestamp)
    else:
        print("Step 1: Upload — skipped.")

    if not args.skip_preprocess:
        work_dir, zip_path = stage_preprocess(s3_db, timestamp)
    else:
        work_dir = args.zip_dir
        print(f"Step 2: Preprocess — skipped. Using: {work_dir}")

    stage_conf_update(work_dir)
    stage_pipeline(timestamp)

    print()
    print("=" * 70)
    print(f"  Community Webs ingest complete — {timestamp}!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
