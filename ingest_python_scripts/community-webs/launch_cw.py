#!/usr/bin/env python3
"""
Community Webs (Internet Archive) ingest orchestrator.

Internet Archive delivers a SQLite .db file. This script:
  1. Uploads your local .db file to s3://dpla-scratch/community-webs/
  2. Downloads it to EC2 at a temp path
  3. Runs community-webs-ingest.sh --db=<path> on EC2
     (handles export → harvest, and optionally full pipeline)

After this script completes, run check_cw.py to monitor progress.

Usage:
    python3 launch_cw.py --db ~/Downloads/community-webs.db
    python3 launch_cw.py --db ~/Downloads/community-webs.db --full
    python3 launch_cw.py --db ~/Downloads/community-webs.db --full --update-conf

Skip flags (for resuming a failed run):
    --skip-upload     .db already in s3://dpla-scratch/community-webs/
    --skip-export     ZIP already on EC2 (community-webs-ingest.sh --skip-export)

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation,
           s3:PutObject/GetObject on dpla-scratch
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime

# ---------- config ----------
def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..", ".env")
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
INSTANCE_ID = _env.get("INGEST_INSTANCE_ID", "")
REGION        = "us-east-1"
REPO_PATH     = "/home/ec2-user/ingestion3"
CW_SCRIPT     = f"{REPO_PATH}/scripts/community-webs-ingest.sh"
S3_STAGING    = "s3://dpla-scratch/community-webs"
DATA_ROOT     = "/home/ec2-user/data"
INGEST_LOG    = f"{DATA_ROOT}/community-webs-ingest.log"


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
        "--region", REGION,
        "--query", "Command.CommandId",
        "--output", "text",
    ])
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
                      "--region", REGION,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--region", REGION,
               "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--region", REGION,
               "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ssm_bg(shell_cmd, log_path):
    """Fire a long-running command as a nohup background job. Returns PID."""
    launch = f"nohup bash -c {json.dumps(shell_cmd)} > {log_path} 2>&1 & echo $!"
    out = ssm_run(launch, timeout_seconds=60)
    return out.strip().split()[-1]


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


def stage_download(s3_db, timestamp):
    step(2, "Download .db to EC2")
    ec2_db = f"/tmp/community-webs-{timestamp}.db"
    print(f"  S3:  {s3_db}")
    print(f"  EC2: {ec2_db}")
    ssm_run(f"aws s3 cp {s3_db} {ec2_db} --region {REGION}", timeout_seconds=600)
    print(f"  ✓ Downloaded → {ec2_db}")
    return ec2_db


def stage_ingest(ec2_db, timestamp, full_pipeline, update_conf, skip_export):
    step(3, "Run community-webs-ingest.sh on EC2")

    script_args = [f"--db={ec2_db}"]
    if skip_export:
        script_args.append("--skip-export")
    if update_conf:
        script_args.append("--update-conf")
    if full_pipeline:
        script_args.append("--full")

    cmd = f"bash {CW_SCRIPT} {' '.join(script_args)}"
    print(f"  Command: {cmd}")
    print(f"  Log:     {INGEST_LOG}")
    print()
    if full_pipeline:
        print("  This will run: export → harvest → mapping → enrichment → jsonl → S3")
    else:
        print("  This will run: export → harvest only")
        print("  Re-run with --full to continue to mapping/enrichment/jsonl.")
    confirm("Launch Community Webs ingest now?")

    pid = ssm_bg(cmd, INGEST_LOG)
    print(f"  PID: {pid}")
    print(f"\n  Monitor with: python3 check_cw.py --watch")
    print(f"  Or tail log:  python3 launch_cw.py --resume")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Community Webs ingest orchestrator")
    parser.add_argument("--db",
                        help="Path to local community-webs .db file from Internet Archive")
    parser.add_argument("--full", action="store_true",
                        help="Run full pipeline (export + harvest + mapping + enrichment + jsonl + S3)")
    parser.add_argument("--update-conf", action="store_true",
                        help="Pass --update-conf to community-webs-ingest.sh")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (.db already at s3://dpla-scratch/community-webs/)")
    parser.add_argument("--skip-export", action="store_true",
                        help="Pass --skip-export to community-webs-ingest.sh (ZIP already on EC2)")
    parser.add_argument("--timestamp",
                        help="Override timestamp (YYYYMMDD_HHMMSS). Auto-generated if omitted.")
    parser.add_argument("--resume", action="store_true",
                        help="Re-attach to an existing running ingest log")
    args = parser.parse_args()

    # Resume mode — just tail the log
    if args.resume:
        print(f"\n  Tailing {INGEST_LOG} (Ctrl+C to stop)...")
        pid_out = ssm_run(
            "pgrep -f 'community-webs-ingest.sh\\|ingest.sh community-webs\\|harvest.sh community-webs' || echo ''",
            timeout_seconds=30,
        ).strip()
        if pid_out:
            print(f"  Running process(es): PID {pid_out}")
        else:
            print("  No running ingest process found — showing final log output.")
        out = ssm_run(f"tail -50 {INGEST_LOG}", timeout_seconds=30)
        print(out)
        return

    timestamp = args.timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_db     = f"{S3_STAGING}/{timestamp}-community-webs.db"

    if not args.skip_upload and not args.skip_export:
        if not args.db:
            sys.exit("--db is required unless --skip-upload or --skip-export is set.")
        db_path = os.path.expanduser(args.db)
        if not os.path.isfile(db_path):
            sys.exit(f".db file not found: {db_path}")

    print(f"\nCOMMUNITY WEBS INGEST — {timestamp}")
    print(f"Instance: {INSTANCE_ID}")
    if not args.skip_upload and not args.skip_export:
        print(f"Local .db: {os.path.expanduser(args.db)}")
    print(f"S3 stage: {s3_db}")
    print(f"Log:      {INGEST_LOG}")

    ec2_db = None

    if not args.skip_upload and not args.skip_export:
        stage_upload(os.path.expanduser(args.db), timestamp)
        ec2_db = stage_download(s3_db, timestamp)
    elif args.skip_upload and not args.skip_export:
        ec2_db = stage_download(s3_db, timestamp)
    # if skip_export, ec2_db doesn't matter (community-webs-ingest.sh ignores --db)

    stage_ingest(
        ec2_db or "",
        timestamp,
        full_pipeline=args.full,
        update_conf=args.update_conf,
        skip_export=args.skip_export,
    )


if __name__ == "__main__":
    main()
