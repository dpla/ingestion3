#!/usr/bin/env python3
"""
DPLA NARA Ingest Launcher

Orchestrates the NARA monthly delta ingest:
  1. Runs nara-ingest.sh on EC2 as a background nohup job
  2. Tails the log in real time (Ctrl+C stops tailing — job keeps running)

Prerequisites:
  - copy_nara.py must have already run for this month:
      • ZIPs copied to s3://dpla-hub-nara/raw_ingest_files/<YYYYMM>/
      • ZIPs staged at ~/data/nara/originalRecords/<YYYYMM>/ on EC2
  - AWS CLI authenticated locally with SSM access

Usage:
    python3 nara/launch_nara.py --month 202605
    python3 nara/launch_nara.py --month 202605 --skip-pipeline     # merge only, no pipeline
    python3 nara/launch_nara.py --skip-to-pipeline                 # resume from latest merged harvest
    python3 nara/launch_nara.py --skip-to-pipeline --harvest /path/to/harvest.avro
    python3 nara/launch_nara.py --resume --month 202605            # re-attach to existing log
    python3 nara/launch_nara.py --resume --log /home/ec2-user/nara-ingest-202605.log

Zero-delete gate:
    After the merge, nara-ingest.sh checks whether any records were actually
    deleted. If 0 valid deletes are found, it halts (non-interactively) and sends
    a Slack notification. This script detects that halt and prompts you here:

        Y = I've verified 0 deletes is expected — continue to pipeline
        N = Abort so I can investigate before re-running

    To bypass the gate upfront (if you already know this delivery has no deletes):
        python3 nara/launch_nara.py --month 202605 --skip-delete-check
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
REGION         = "us-east-1"
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

NARA_SCRIPT    = "/home/ec2-user/ingestion3/scripts/harvest/nara-ingest.sh"

# EC2 paths — nara-ingest.sh uses ~/data/ (not ~/dpla/data/)
DATA_ROOT      = "/home/ec2-user/data"
NARA_DATA      = f"{DATA_ROOT}/nara"
NARA_ORIGINALS = f"{NARA_DATA}/originalRecords"

LOG_DIR        = "/home/ec2-user"
POLL_SECONDS   = 60   # log tail interval


# ---------- AWS / SSM helpers ----------

def aws(args, check=True):
    profile = [] if any(a.startswith("--profile") for a in args) else ["--profile", "dpla"]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    """Run a shell command on EC2 via SSM as ec2-user. Returns stdout."""
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
    """Launch a long-running command as a background nohup job. Returns PID.

    On completion the process writes its exit code to <log_path>.exitcode so
    tail_log can detect failures even when monitoring resumes after the process
    has already died.
    """
    exit_file = f"{log_path}.exitcode"
    wrapper = (
        f"bash -c {json.dumps(shell_cmd)} > {log_path} 2>&1; "
        f"echo $? > {exit_file}"
    )
    launch = f"nohup bash -c {json.dumps(wrapper)} </dev/null >/dev/null 2>&1 & echo $!"
    out    = ssm_run(launch, timeout_seconds=60)
    return out.strip().split()[-1]


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
    ok_resp = answer not in ("n", "no") if default_yes else answer in ("y", "yes")
    if not ok_resp:
        sys.exit("Aborted.")


# ---------- Step 1: pre-flight check ----------

def preflight_check(month):
    """Verify that copy_nara.py has already staged the files on EC2."""
    step(1, f"Pre-flight: verify files staged on EC2 ({month})")
    ingest_dir = f"{NARA_ORIGINALS}/{month}"
    count = ssm_run(
        f"ls {ingest_dir} 2>/dev/null | wc -l || echo 0",
        timeout_seconds=30,
    ).strip()
    if count == "0":
        sys.exit(
            f"\n  [BAD] No files found at {ingest_dir} on EC2.\n"
            f"  Run copy_nara.py first — it downloads the NARA delivery and stages\n"
            f"  the ZIPs at that path, ready for nara-ingest.sh.\n"
            f"  Run: python3 copy_nara.py   (then re-run this script)"
        )
    ok(f"{count} file(s) staged at {ingest_dir}")


# ---------- Step 2: launch nara-ingest.sh ----------

def launch_ingest(month, log_path, extra_args=""):
    step(2, "Launch nara-ingest.sh on EC2")  # Step 1 = preflight check
    cmd = f"bash {NARA_SCRIPT} --month={month} {extra_args}".strip()
    info(f"Command: {cmd}")
    info(f"Log:     {log_path}")
    print()
    print("  This will take ~12-14 hours total:")
    print("    • Preprocess + delta harvest:  ~30 min")
    print("    • Merge (~18.8M records):      ~1-2 hours")
    print("    • Pipeline (map/enrich/jsonl): ~10 hours")
    confirm("Launch NARA ingest now?")

    pid = ssm_bg(cmd, log_path)
    ok(f"Launched. PID: {pid}")
    return pid


def launch_skip_to_pipeline(harvest_path, log_path, skip_delete_check=False):
    step(2, "Launch nara-ingest.sh --skip-to-pipeline on EC2")
    flags = ["--skip-to-pipeline"]
    if harvest_path:
        flags.append(f"--harvest={harvest_path}")
    if skip_delete_check:
        flags.append("--skip-delete-check")
    cmd = f"bash {NARA_SCRIPT} {' '.join(flags)}"
    info(f"Command: {cmd}")
    info(f"Log:     {log_path}")
    print()
    print("  Pipeline only (~10 hours): mapping → enrichment → JSONL → S3")
    confirm("Launch NARA pipeline now?")

    pid = ssm_bg(cmd, log_path)
    ok(f"Launched. PID: {pid}")
    return pid


# ---------- Zero-delete gate ----------

def extract_harvest_from_log(log_path):
    """Pull the merged harvest path from the log's gate-halt message."""
    try:
        out = ssm_run(
            f"grep -oE -- '--harvest=[^ ]+' {log_path} | tail -1 || true",
            timeout_seconds=30,
        ).strip()
        if out.startswith("--harvest="):
            return out[len("--harvest="):]
    except Exception:
        pass
    # Fallback: scan for any OriginalRecord.avro path in the log
    try:
        out = ssm_run(
            f"grep -oE '/[^ ]+OriginalRecord\\.avro' {log_path} | tail -1 || true",
            timeout_seconds=30,
        ).strip()
        return out or None
    except Exception:
        return None


def handle_delete_gate_halt(log_path):
    """Called when tail_log detects the script halted at the zero-delete gate.

    Shows a summary, prompts the operator to continue or abort, and if they
    confirm, relaunches with --skip-to-pipeline --skip-delete-check.
    """
    print()
    warn("=" * 66)
    warn("  ZERO-DELETE GATE — nara-ingest.sh halted after merge")
    warn("=" * 66)
    print()
    warn("The merge completed but 0 records were actually deleted.")
    warn("A Slack notification was sent. Review before continuing:")
    print()
    info("  • Was a delete file (NAC_DESC_Deletes_*.xml) present in the delivery?")
    info("  • Check: cat <merged-harvest>/_LOGS/_SUMMARY.txt")
    info("  • If no deletes are expected for this delivery, continue.")
    print()

    harvest_path = extract_harvest_from_log(log_path)
    if harvest_path:
        info(f"  Merged harvest: {harvest_path}")
    else:
        warn("  Could not detect merged harvest path from log — you'll need to set --harvest manually.")

    print()
    confirm("Continue to pipeline despite 0 deletes? (confirm you've verified this)", default_yes=False)

    # Relaunch as a new background job
    label    = datetime.now().strftime("%Y%m%d-%H%M%S")
    new_log  = f"{LOG_DIR}/nara-ingest-resume-{label}.log"
    pid      = launch_skip_to_pipeline(harvest_path, new_log, skip_delete_check=True)
    tail_log(pid, new_log)


# ---------- Step 3: tail log ----------

def tail_log(pid, log_path):
    step(3, "Monitoring (Ctrl+C to stop tailing — job keeps running on EC2)")  # Step 2 = launch
    info(f"Log: {log_path}")
    info(f"PID: {pid}")
    info(f"Resume:  python3 nara/launch_nara.py --resume --log {log_path}")
    info(f"Status:  python3 check_nara.py\n")

    last_lines = 0
    try:
        while True:
            alive = ssm_run(
                f"ps -p {pid} -o pid= 2>/dev/null || echo dead",
                timeout_seconds=30,
            ).strip()

            total_lines = int(ssm_run(
                f"[ -f {log_path} ] && wc -l < {log_path} || echo 0",
                timeout_seconds=30,
            ).strip() or "0")

            if total_lines > last_lines:
                new = total_lines - last_lines
                tail = ssm_run(f"tail -{new} {log_path}", timeout_seconds=30).rstrip()
                if tail:
                    print(tail)
                last_lines = total_lines

            if alive in ("dead", ""):
                print()
                log_tail = ssm_run(f"tail -40 {log_path}", timeout_seconds=30).strip()
                print(log_tail)

                # Read sidecar exit code written by ssm_bg wrapper
                exit_file  = f"{log_path}.exitcode"
                exit_raw   = ssm_run(
                    f"cat {exit_file} 2>/dev/null || echo missing",
                    timeout_seconds=30,
                ).strip()
                if exit_raw == "missing":
                    bad("Exit code sidecar not found — process may have been killed or sidecar write failed.")
                    raise RuntimeError("NARA ingest exit code unknown; check the log manually.")
                try:
                    exit_code = int(exit_raw)
                except ValueError:
                    exit_code = 1

                if exit_code != 0:
                    # Check if this was a zero-delete gate halt before raising
                    if "zero-delete gate" in log_tail.lower():
                        handle_delete_gate_halt(log_path)
                        return
                    bad(f"NARA ingest FAILED (exit {exit_code}). See log: {log_path}")
                    raise RuntimeError(f"nara-ingest.sh exited {exit_code}")

                ok(f"Process exited cleanly (exit 0).")
                return

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\n  Monitoring stopped. NARA ingest is still running on EC2.")
        print(f"  Resume:  python3 nara/launch_nara.py --resume --log {log_path}")
        print(f"  Status:  python3 check_nara.py")


def resume_log(log_path):
    """Re-attach to an existing log/process."""
    step(1, f"Resuming: {log_path}")
    pid_out = ssm_run(
        "pgrep -f 'nara-ingest.sh' || echo ''",
        timeout_seconds=30,
    ).strip()

    if not pid_out:
        info("No running nara-ingest.sh process found — showing final log output.")
        print(ssm_run(f"tail -50 {log_path}", timeout_seconds=30).strip())
    else:
        pid = pid_out.strip().split()[-1]
        ok(f"Found running process: PID {pid}")
        tail_log(pid, log_path)


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="DPLA NARA ingest launcher")
    parser.add_argument("--month",
                        help="Month to process (YYYYMM)")
    parser.add_argument("--skip-preflight",   action="store_true",
                        help="Skip EC2 file check (use if you know files are staged)")
    parser.add_argument("--skip-pipeline",    action="store_true",
                        help="Run harvest+merge only, skip mapping/enrichment/jsonl")
    parser.add_argument("--skip-to-pipeline", action="store_true",
                        help="Skip to pipeline using existing merged harvest")
    parser.add_argument("--harvest",
                        help="Explicit merged harvest path (use with --skip-to-pipeline)")
    parser.add_argument("--base",
                        help="Explicit base harvest avro path")
    parser.add_argument("--force-sync",       action="store_true",
                        help="Force fresh S3 download of base harvest on EC2")
    parser.add_argument("--skip-delete-check", action="store_true",
                        help="Bypass the zero-delete gate (use when 0 deletes is expected)")
    parser.add_argument("--resume",           action="store_true",
                        help="Re-attach to an existing running ingest log")
    parser.add_argument("--log",
                        help="Log path to tail (use with --resume)")
    args = parser.parse_args()

    print("\nDPLA NARA INGEST LAUNCHER")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"EC2:  {INSTANCE_ID}")

    # ── Resume mode ──────────────────────────────────────────────────────────
    if args.resume:
        log_path = args.log
        if not log_path:
            if not args.month:
                sys.exit("\n  --resume requires --log <path> or --month YYYYMM")
            log_path = f"{LOG_DIR}/nara-ingest-{args.month}.log"
        resume_log(log_path)
        return

    # ── Skip-to-pipeline mode ─────────────────────────────────────────────
    if args.skip_to_pipeline:
        label    = args.month or datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = f"{LOG_DIR}/nara-ingest-{label}.log"
        pid      = launch_skip_to_pipeline(
            args.harvest, log_path,
            skip_delete_check=args.skip_delete_check,
        )
        tail_log(pid, log_path)
        return

    # ── Normal mode ───────────────────────────────────────────────────────
    if not args.month:
        sys.exit("\n  [BAD] --month YYYYMM is required.")
    if not re.match(r"^\d{6}$", args.month):
        sys.exit(f"\n  [BAD] --month must be 6 digits (YYYYMM), got: {args.month!r}")

    month    = args.month
    log_path = f"{LOG_DIR}/nara-ingest-{month}.log"

    print(f"Month: {month}")
    print(f"Log:   {log_path}")

    # Step 1: Pre-flight
    if not args.skip_preflight:
        preflight_check(month)
    else:
        info("Skipping pre-flight check (--skip-preflight).")

    # Step 2: Launch
    extra = []
    if args.skip_pipeline:
        extra.append("--skip-pipeline")
    if args.base:
        extra.append(f"--base={args.base}")
    if args.force_sync:
        extra.append("--force-sync")
    if args.skip_delete_check:
        extra.append("--skip-delete-check")

    pid = launch_ingest(month, log_path, extra_args=" ".join(extra))

    # Step 3: Tail
    tail_log(pid, log_path)


if __name__ == "__main__":
    main()
