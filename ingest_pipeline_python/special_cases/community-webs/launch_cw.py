#!/usr/bin/env python3
"""
Community Webs (Internet Archive) ingest, using the newest file from copy_cw.py.

  1. Finds the newest *-community-webs.db in s3://dpla-hub-community-webs/
  2. Downloads it to EC2 at a temp path
  3. Runs community-webs-ingest.sh on EC2: export -> harvest -> mapping ->
     enrichment -> jsonl -> safety check
  4. Waits, shows the counts, and asks before syncing to S3 and emailing the hub

The .db is already staged in s3://dpla-hub-community-webs/ (copy_cw.py). Nothing
is published to s3://dpla-master-dataset/ or emailed to the hub contacts unless
you say yes. Slack reports each stage. Without a terminal (GitHub Actions) it starts the run and exits at step 3.

Usage:
    python3 special_cases/community-webs/copy_cw.py ~/Downloads/community-webs.sqlite3
    python3 special_cases/community-webs/launch_cw.py

Rerunning starts again from the export; it can't double records.

Other flags:
    --timestamp       Use this copied .db instead of the newest
    --send            Answer the sync and email question later, or retry a failed email
    --send --force    Same, for a run blocked by the safety check
    --resume          Show the running process and the log tail

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation,
           s3:GetObject/ListBucket on dpla-hub-community-webs
"""

import argparse
import base64
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime

# ---------- config ----------
def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../..", ".env")
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
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".env")
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
RUN_FILE      = f"{DATA_ROOT}/community-webs/.last-run"
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


def ask(msg, default_yes=True):
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        answer = input(f"\n  {msg}{suffix}").strip().lower()
    except EOFError:
        answer = ""
    return answer not in ("n", "no") if default_yes else answer in ("y", "yes")


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
            "  Run copy_cw.py first or pass --timestamp explicitly."
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


def stage_download(s3_db, timestamp):
    ec2_db = f"/tmp/community-webs-{timestamp}.db"
    ssm_run(f"aws s3 cp {s3_db} {ec2_db} --region {REGION}", timeout_seconds=600)
    print(f"  ✓ Downloaded → {ec2_db}")
    return ec2_db


def ensure_not_running():
    """Refuse early if an ingest or send is running, before the download can
    overwrite a .db it's exporting or the launch truncates its log. The script's
    own lock still makes the final call."""
    # Matches only bash running the script (an ingest or a --send), not editors,
    # tail or grep that merely mention its name.
    busy = ssm_run(
        "pgrep -f '^(/usr/bin/|/bin/)?bash [^ ]*community-webs-ingest\\.sh( |$)' || true",
        timeout_seconds=30,
    ).strip()
    if busy:
        sys.exit(f"A Community Webs ingest or send is already running on EC2 (PID {busy}). "
                 "Wait for it, or check it with: launch_cw.py --resume")


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

    return pid


def wait_for_ingest(pid):
    """Poll EC2 until the job ends. Returns its exit code, or None if it vanished."""
    print("\n  Waiting for the run to finish. Slack reports each stage.")
    print("  Ctrl+C is safe: the run keeps going, and launch_cw.py --send picks up from there.")
    while True:
        time.sleep(30)
        try:
            state = ssm_run(
                f"if [ -f {INGEST_LOG}.exitcode ]; then echo EXIT $(cat {INGEST_LOG}.exitcode); "
                f"elif ps -p {pid} >/dev/null 2>&1; then echo RUNNING; else echo GONE; fi",
                timeout_seconds=60,
            ).split()
        except RuntimeError as e:
            print(f"  Warning: could not check the run ({e}); trying again.")
            continue
        if state[:1] == ["GONE"]:
            return None
        if state[:1] == ["EXIT"] and len(state) == 2:
            return int(state[1])


def review_and_send(pid):
    """Same-terminal review: ask about the sync and email once the run ends."""
    try:
        code = wait_for_ingest(pid)
    except KeyboardInterrupt:
        print("\n  Stopped waiting; the run continues on EC2. Afterwards: launch_cw.py --send")
        return
    log_tail = ssm_run(f"tail -20 {INGEST_LOG} 2>&1", timeout_seconds=30)
    if code == 0:
        stage_send(force=False)
        return
    blocked = ssm_run(f"grep -h 'Safety check FAILED' {INGEST_LOG} 2>/dev/null | tail -1",
                      timeout_seconds=30).strip()
    if code is not None and blocked:
        print(f"\n  SAFETY CHECK BLOCKED THIS RUN\n  {blocked}")
        stage_send(force=True)
        return
    sys.exit(f"Community Webs ingest failed (exit {code}). Log tail ({INGEST_LOG}):\n{log_tail}")


def stage_send(force):
    """After review: sync the snapshot to S3 and email the hub contacts."""
    # Same source the EC2 script will send from: the reviewed run, or with
    # --force the last run's record. Check it before showing anything.
    source = f"head -1 {READY_FILE} 2>/dev/null"
    if force:
        source += f" || head -1 {RUN_FILE} 2>/dev/null"
    state = ssm_run(
        f"S=$({source}); "
        f"if [ -z \"$S\" ]; then echo NO_RUN; exit 0; fi; "
        f"echo \"Snapshot: $S\"; grep 'Record count' \"$S/_MANIFEST\"; "
        f"echo Contacts:; grep '^community-webs.email' {REPO_PATH}-conf/i3.conf | sed 's/.*= *//'",
        timeout_seconds=30,
    )
    if state.strip() == "NO_RUN":
        if force:
            sys.exit("No record of the last run's outputs on EC2; rerun the ingest.")
        sys.exit("No run is waiting for review. If the safety check blocked it and "
                 "you've reviewed the drop, use: launch_cw.py --send --force")
    prev = previous_count()
    print("\nCOMMUNITY WEBS SEND")
    print(f"{state}")
    print(f"Previous in S3: {f'{prev:,}' if prev is not None else 'unknown'}")
    if force:
        print("  This sends the last run's snapshot even though the safety check blocked it.")
    if not ask("Sync to S3 and email the contacts above?", default_yes=False):
        later = "--send --force" if force else "--send"
        print(f"  Nothing sent. To send later: launch_cw.py {later}")
        return
    flags = "--send --force" if force else "--send"
    print(ssm_run(f"bash {CW_SCRIPT} {flags} 2>&1 | tail -15; exit ${{PIPESTATUS[0]}}",
                  timeout_seconds=1800))


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Community Webs ingest orchestrator")
    parser.add_argument("--send", action="store_true",
                        help="After review: sync the snapshot to S3 and email the hub contacts")
    parser.add_argument("--force", action="store_true",
                        help="With --send: send the newest snapshot even if the safety check blocked it")
    parser.add_argument("--timestamp",
                        help="Use this copied .db (YYYYMMDD_HHMMSS) instead of the newest")
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

    timestamp = args.timestamp or detect_staged_db()
    s3_db = f"{S3_STAGING}/{timestamp}-community-webs.db"
    interactive = sys.stdin.isatty()

    print(f"\nCOMMUNITY WEBS INGEST — {timestamp}")
    print(f"  File:     {s3_db}")
    print("  Will run: export → harvest → map → enrich → jsonl → safety check, then ask before sending")
    ensure_not_running()
    if interactive and not ask("Start?"):
        sys.exit("Aborted.")

    ec2_db = stage_download(s3_db, timestamp)
    pid = stage_ingest(ec2_db)

    if interactive:
        review_and_send(pid)
    else:
        print("\n  Started. Slack posts the counts when it's ready for review.")
        print("  Nothing is published to dpla-master-dataset or emailed until you run: launch_cw.py --send")


if __name__ == "__main__":
    main()
