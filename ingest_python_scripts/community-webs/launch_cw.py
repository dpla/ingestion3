#!/usr/bin/env python3
"""
Community Webs (Internet Archive) ingest orchestrator.

Internet Archive delivers a SQLite .db file. This script:
  1. Uploads your local .db file to s3://dpla-hub-community-webs/
  2. Downloads it to EC2 at a temp path
  3. Runs community-webs-ingest.sh on EC2: export -> harvest -> mapping ->
     enrichment -> jsonl -> safety check, then stops for review

Nothing reaches S3 or the hub contacts until you run --send. Slack reports each
stage and posts the counts when the run is ready for review.

Usage:
    python3 launch_cw.py ~/Downloads/community-webs.sqlite3
    python3 launch_cw.py --send             # after review: S3 sync + partner email
    python3 launch_cw.py --send --force     # send despite a blocked safety check

Rerunning the same command starts again from the export; it can't double records.

Other flags:
    --skip-upload     .db already in s3://dpla-hub-community-webs/ (newest is used)
    --timestamp       With --skip-upload: use this staged .db
    --resume          Show the running process and the log tail

After launching, run check_cw.py to monitor progress.

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation,
           s3:PutObject/GetObject/ListBucket on dpla-hub-community-webs
"""

import argparse
import base64
import json
import os
import shlex
import sqlite3
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
_env_file_exists = os.path.exists(os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
))
INSTANCE_ID = os.environ.get("INGEST_INSTANCE_ID") or _env.get("INGEST_INSTANCE_ID", "")
AWS_PROFILE: str | None = (
    os.environ.get("AWS_PROFILE")
    or _env.get("AWS_PROFILE")
    or ("dpla" if _env_file_exists else None)
)
REGION        = "us-east-1"
REPO_PATH     = "/home/ec2-user/ingestion3"
CW_SCRIPT     = f"{REPO_PATH}/scripts/harvest/community-webs-ingest.sh"
S3_STAGING    = "s3://dpla-hub-community-webs"
DATA_ROOT     = "/home/ec2-user/data"
INGEST_LOG    = f"{DATA_ROOT}/community-webs-ingest.log"
READY_FILE    = f"{DATA_ROOT}/community-webs/.ready-for-review"
S3_JSONL      = "s3://dpla-master-dataset/community-webs/jsonl"


# ---------- AWS / SSM helpers ----------
def aws(args):
    profile = [] if any(a.startswith("--profile") for a in args) else (["--profile", AWS_PROFILE] if AWS_PROFILE else [])
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
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
    # setsid: the job must outlive this SSM command. The exit code file tells a
    # fast finish apart from a crash.
    wrapped = f"{shell_cmd}; echo $? > {log_path}.exitcode"
    launch = (f"rm -f {log_path}.exitcode; setsid nohup bash -c {shlex.quote(wrapped)} "
              f"> {log_path} 2>&1 < /dev/null & echo $!")
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


# ---------- Auto-detect staged db ----------

def detect_staged_db():
    """Find the newest *-community-webs.db staged in S3_STAGING.

    Errors out if:
      - no .db files found in the staging prefix
      - the ingest log shows this specific db was already processed successfully
    Returns the timestamp string (YYYYMMDD_HHMMSS) of the newest unprocessed db.
    """
    import re as _re
    ls_out = aws(["s3", "ls", f"{S3_STAGING}/"])
    db_entries = [
        line for line in ls_out.splitlines()
        if line.strip().endswith("-community-webs.db")
    ]
    if not db_entries:
        sys.exit(
            f"\n  [BAD] No *-community-webs.db files found in {S3_STAGING}/.\n"
            "  Upload a .db first or pass --timestamp explicitly."
        )

    # Lines are: "2026-08-19 02:30:00  12345678 20260819_023000-community-webs.db"
    # Parse and validate the filename timestamp for each entry, then sort by it
    # so re-uploaded older files don't shadow a newer db.
    valid = []
    for line in db_entries:
        fname = line.split()[-1]
        m = _re.match(r"^(\d{8}_\d{6})-community-webs\.db$", fname)
        if m:
            try:
                datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
                valid.append((m.group(1), fname))
            except ValueError:
                pass  # skip filenames with structurally valid but impossible dates
    if not valid:
        sys.exit(
            f"\n  [BAD] No validly-named *-community-webs.db files found in {S3_STAGING}/.\n"
            "  Expected format: YYYYMMDD_HHMMSS-community-webs.db"
        )
    valid.sort(key=lambda x: x[0])
    timestamp, filename = valid[-1]

    # Check if this specific db was already successfully ingested by looking for
    # its EC2 path and a success marker in INGEST_LOG. The log is overwritten on
    # each run, so a match means this db was the most recent run and it completed.
    ec2_db_path = f"/tmp/community-webs-{timestamp}.db"
    log_check = ssm_run(
        f"grep -q {ec2_db_path} {INGEST_LOG} 2>/dev/null "
        f"&& grep -q 'Community Webs.*complete' {INGEST_LOG} 2>/dev/null "
        f"&& echo DONE || echo NOT_DONE",
        timeout_seconds=30,
    ).strip()

    if log_check == "DONE":
        sys.exit(
            f"\n  [BAD] DB {timestamp} appears already ingested.\n"
            f"  The ingest log shows a successful run for this db.\n"
            f"  Pass --timestamp {timestamp} explicitly to re-run anyway."
        )

    print(f"  Auto-detected staged db: {timestamp}")
    return timestamp


# ---------- stages ----------

def count_records(db_path):
    """Count ait rows locally so a bad file fails before anything is uploaded."""
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            return conn.execute("SELECT count(*) FROM ait").fetchone()[0]
    except sqlite3.Error as e:
        sys.exit(f"{db_path} is not a readable Community Webs export ({e}).")


def previous_count():
    """Record count of the newest snapshot in S3, or None if it can't be read."""
    try:
        snapshots = sorted(
            line.split()[-1] for line in aws(["s3", "ls", f"{S3_JSONL}/"]).splitlines()
            if line.strip().endswith(".jsonl/")
        )
        if not snapshots:
            return None
        manifest = aws(["s3", "cp", f"{S3_JSONL}/{snapshots[-1]}_MANIFEST", "-"])
    except RuntimeError:
        return None
    for line in manifest.splitlines():
        if line.startswith("Record count:"):
            return int(line.split()[-1].replace(",", ""))
    return None


def stage_upload(db_path, timestamp):
    s3_dest = f"{S3_STAGING}/{timestamp}-community-webs.db"
    result = subprocess.run(
        ["aws", "s3", "cp", db_path, s3_dest, "--no-progress"]
        + (["--profile", AWS_PROFILE] if AWS_PROFILE else []),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"S3 upload failed: {result.stderr.strip()}")
    print(f"  ✓ Uploaded → {s3_dest}")
    return s3_dest


def stage_download(s3_db, timestamp):
    ec2_db = f"/tmp/community-webs-{timestamp}.db"
    ssm_run(f"aws s3 cp {s3_db} {ec2_db} --region {REGION}", timeout_seconds=600)
    print(f"  ✓ Downloaded → {ec2_db}")
    return ec2_db


def stage_ingest(ec2_db):
    cmd = f"bash {CW_SCRIPT} --db={ec2_db}"
    pid = ssm_bg(cmd, INGEST_LOG)
    print(f"  ✓ Started on EC2 (PID {pid})")

    # A PID only means the fork worked — the job can still die instantly (bad
    # path, missing tool) while the launch looks successful. On failure show the
    # log and exit non-zero so CI goes red.
    time.sleep(5)
    try:
        check = ssm_run(
            f"if ps -p {pid} >/dev/null 2>&1; then echo INGEST_RUNNING; "
            f"elif [ \"$(cat {INGEST_LOG}.exitcode 2>/dev/null)\" = 0 ]; then echo INGEST_DONE; "
            f"else echo INGEST_NOT_RUNNING; tail -20 {INGEST_LOG} 2>&1; fi",
            timeout_seconds=30,
        )
    except RuntimeError as e:
        check = ""
        print(f"  Warning: could not verify the ingest is running ({e}) — check the log on EC2.")
    if "INGEST_NOT_RUNNING" in check:
        sys.exit(f"Community Webs ingest (PID {pid}) exited right after launch. Log tail ({INGEST_LOG}):\n{check}")

    print("\n  Slack will post each stage, then the counts when it's ready for review.")
    print("  Nothing goes to S3 or the hub contacts until you run: launch_cw.py --send")
    print("  Monitor with: python3 check_cw.py --watch")


def stage_send(force):
    """After review: sync the snapshot to S3 and email the hub contacts."""
    state = ssm_run(
        f"cat {READY_FILE} 2>/dev/null | head -1; "
        f"grep '^community-webs.email' {REPO_PATH}-conf/i3.conf | sed 's/.*= *//'",
        timeout_seconds=30,
    )
    print("\nCOMMUNITY WEBS SEND")
    print(f"  From EC2:\n{state}")
    if force:
        print("  --force: sends the newest snapshot even if the safety check blocked it.")
    confirm("Sync to S3 and email the contacts above?", default_yes=False)
    flags = "--send --force" if force else "--send"
    print(ssm_run(f"bash {CW_SCRIPT} {flags} 2>&1 | tail -15; exit ${{PIPESTATUS[0]}}",
                  timeout_seconds=1800))


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Community Webs ingest orchestrator")
    parser.add_argument("db", nargs="?",
                        help="Path to local community-webs .db/.sqlite3 file from Internet Archive")
    parser.add_argument("--send", action="store_true",
                        help="After review: sync the snapshot to S3 and email the hub contacts")
    parser.add_argument("--force", action="store_true",
                        help="With --send: send the newest snapshot even if the safety check blocked it")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (.db already at s3://dpla-hub-community-webs/)")
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

    if args.force and not args.send:
        parser.error("--force only applies with --send")
    if args.send:
        stage_send(args.force)
        return

    if args.skip_upload and not args.timestamp:
        timestamp = detect_staged_db()
    else:
        timestamp = args.timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_db = f"{S3_STAGING}/{timestamp}-community-webs.db"

    print(f"\nCOMMUNITY WEBS INGEST — {timestamp}")
    if args.skip_upload:
        print(f"  File:     {s3_db} (already staged)")
    else:
        if not args.db:
            parser.error("give the path to the .db file, or use --send, --resume or --skip-upload")
        db_path = os.path.expanduser(args.db)
        if not os.path.isfile(db_path):
            sys.exit(f".db file not found: {db_path}")
        size_mb = os.path.getsize(db_path) / 1024 / 1024
        prev = previous_count()
        print(f"  File:     {db_path} ({size_mb:.1f} MB)")
        print(f"  Records:  {count_records(db_path):,} (previous in S3: {f'{prev:,}' if prev is not None else 'unknown'})")
    print("  Will run: export → harvest → map → enrich → jsonl → safety check, then stop for review")
    confirm("Start?")

    if not args.skip_upload:
        stage_upload(db_path, timestamp)
    ec2_db = stage_download(s3_db, timestamp)
    stage_ingest(ec2_db)


if __name__ == "__main__":
    main()
