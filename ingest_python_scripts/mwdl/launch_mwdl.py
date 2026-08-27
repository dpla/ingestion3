#!/usr/bin/env python3
"""
DPLA MWDL Ingest Launcher

Orchestrates the full MWDL Primo VE ingest:
  1. pre-harvest/mwdl-prefix-explorer.py  — builds title-prefix query buckets (mwdl-prefixes.json)
  2. pre-harvest/mwdl-harvest.py          — harvests all records using those buckets (mwdl-harvest.jsonl)
  3. pre-harvest/mwdl-jsonl-to-avro.py    — converts JSONL → Avro ready for the pipeline
  4. ingest.sh mwdl --skip-harvest        — runs mapping → enrichment → jsonl → S3

Each long-running step runs as a background nohup job on EC2; this script
tails the log every 60 s and blocks until the step succeeds or fails.

Usage:
    python3 mwdl/launch_mwdl.py                     # full run from step 1
    python3 mwdl/launch_mwdl.py --skip-to-harvest   # skip explorer, start at harvest
    python3 mwdl/launch_mwdl.py --skip-to-avro      # skip explorer + harvest
    python3 mwdl/launch_mwdl.py --skip-to-pipeline  # skip all pre-steps, just run ingest.sh

Prerequisites:
    - mwdl.harvest.apiKey set in i3.conf on EC2 (read automatically by the scripts)
    - AWS CLI authenticated locally (profile: dpla)
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation on the ingest instance
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import time

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
    for key in ("SLACK_BOT_TOKEN", "SLACK_TOKEN", "SLACK_CHANNEL"):
        if key in cfg:
            os.environ.setdefault(key, cfg[key])
    return cfg

_env = _load_dotenv()
INSTANCE_ID = _env.get("INGEST_INSTANCE_ID", "")


# ---------- Slack ----------

def slack_notify(msg: str) -> None:
    """Post a message to Slack. Silently skips if SLACK_BOT_TOKEN is not set."""
    import urllib.request as _req
    token   = os.environ.get("SLACK_BOT_TOKEN") or os.environ.get("SLACK_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "C02HEU2L3")
    if not token:
        return
    payload = json.dumps({"channel": channel, "text": msg}).encode()
    request = _req.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        _req.urlopen(request, timeout=10)
    except Exception:
        pass

MWDL_SCRIPTS = "/home/ec2-user/ingestion3/scripts/pre-harvest/mwdl"
INGEST_SCRIPT = "/home/ec2-user/ingestion3/scripts/ingest.sh"
HARVEST_DIR   = "/home/ec2-user/mwdl-harvest"

POLL_SECONDS  = 60   # log tail interval for long-running steps


# ---------- AWS / SSM helpers ----------

def aws(args):
    profile = [] if any(a.startswith("--profile") for a in args) else ["--profile", "dpla"]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120):
    """Run a short command on EC2 via SSM as ec2-user. Returns stdout."""
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
        time.sleep(3)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID,
               "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID,
               "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ssm_bg(shell_cmd, log_path):
    """Launch a long-running command as a background nohup job. Returns (pid, exit_file)."""
    exit_file = f"{log_path}.exitcode"
    wrapper = (
        f"bash -c {json.dumps(shell_cmd)} > {log_path} 2>&1; "
        f"echo $? > {exit_file}"
    )
    launch = f"nohup bash -c {json.dumps(wrapper)} </dev/null >/dev/null 2>&1 & echo $!"
    out = ssm_run(launch, timeout_seconds=60)
    pid = out.strip().split()[-1]
    return pid, exit_file


def wait_for_pid(pid, log_path, exit_file, timeout_seconds=None):
    """Poll until the process exits, tailing the log each cycle."""
    start = time.time()
    while True:
        time.sleep(POLL_SECONDS)
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

    # Read exit code sidecar
    exit_raw = ssm_run(
        f"cat {exit_file} 2>/dev/null || echo missing",
        timeout_seconds=30,
    ).strip()
    if exit_raw == "missing":
        raise RuntimeError(f"Exit code sidecar not found ({exit_file}) — process may have been killed.")
    try:
        exit_code = int(exit_raw)
    except ValueError:
        exit_code = 1
    if exit_code != 0:
        raise RuntimeError(f"Command failed (exit {exit_code}). See log: {log_path}")


# ---------- UI helpers ----------

def step(n, title):
    print()
    print("=" * 70)
    print(f"  STEP {n}: {title}")
    print("=" * 70)


# ---------- steps ----------

def run_prefix_explorer():
    step(1, "Build prefix query buckets (mwdl-prefix-explorer.py)")
    slack_notify(":arrow_forward: *MWDL pre-harvest started* — building title-prefix query buckets")
    log_path = f"{HARVEST_DIR}/mwdl-prefix-explorer.log"
    print(f"  Log: {log_path}")
    ssm_run(f"mkdir -p {HARVEST_DIR}", timeout_seconds=30)
    cmd = f"python3 {MWDL_SCRIPTS}/mwdl-prefix-explorer.py"
    pid, exit_file = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every {POLL_SECONDS}s (Ctrl+C stops tailing, job keeps running)")
    wait_for_pid(pid, log_path, exit_file, timeout_seconds=14400)  # 4h max
    print("  Prefix explorer complete.")


def run_prefix_harvest():
    step(2, "Harvest records by prefix (mwdl-harvest.py)")
    slack_notify(":arrow_forward: *MWDL harvest started* — paginating prefix buckets")
    log_path = f"{HARVEST_DIR}/mwdl-harvest.log"
    print(f"  Log: {log_path}")

    # Verify prefixes file exists
    count = ssm_run(
        f"[ -f {HARVEST_DIR}/mwdl-prefixes.json ] && "
        f"python3 -c \"import json; d=json.load(open('{HARVEST_DIR}/mwdl-prefixes.json')); print(len(d))\" "
        f"|| echo missing",
        timeout_seconds=30,
    ).strip()
    if count == "missing":
        sys.exit(f"  ERROR: {HARVEST_DIR}/mwdl-prefixes.json not found. Run without --skip-to-harvest first.")
    print(f"  Found {count} queryable prefix buckets.")

    cmd = f"python3 {MWDL_SCRIPTS}/mwdl-harvest.py"
    pid, exit_file = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every {POLL_SECONDS}s")
    wait_for_pid(pid, log_path, exit_file, timeout_seconds=18000)  # 5h max
    print("  Prefix harvest complete.")


def run_jsonl_to_avro():
    step(3, "Convert JSONL → Avro (mwdl-jsonl-to-avro.py)")
    slack_notify(":arrow_forward: *MWDL JSONL → Avro conversion started*")
    log_path = f"{HARVEST_DIR}/mwdl-avro.log"
    print(f"  Log: {log_path}")

    # Verify JSONL exists
    lines = ssm_run(
        f"[ -f {HARVEST_DIR}/mwdl-harvest.jsonl ] && wc -l < {HARVEST_DIR}/mwdl-harvest.jsonl || echo missing",
        timeout_seconds=30,
    ).strip()
    if lines == "missing":
        sys.exit(f"  ERROR: {HARVEST_DIR}/mwdl-harvest.jsonl not found. Run harvest first.")
    print(f"  JSONL has {lines} lines.")

    cmd = f"python3 {MWDL_SCRIPTS}/mwdl-jsonl-to-avro.py"
    pid, exit_file = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every {POLL_SECONDS}s")
    wait_for_pid(pid, log_path, exit_file, timeout_seconds=3600)  # 1h max
    print("  Avro conversion complete.")


def run_pipeline():
    step(4, "Run ingest pipeline (ingest.sh mwdl --skip-harvest)")
    log_path = "/home/ec2-user/data/mwdl-ingest.log"
    invocation = f"bash {INGEST_SCRIPT} mwdl --skip-harvest"
    inner = (
        'sudo -u ec2-user bash -lc "'
        f"nohup {invocation} > {log_path} 2>&1 </dev/null &"
        '"'
    )
    params = json.dumps({"commands": [inner]})
    result = subprocess.run(
        ["aws", "--profile", "dpla", "ssm", "send-command",
         "--instance-ids", INSTANCE_ID,
         "--document-name", "AWS-RunShellScript",
         "--timeout-seconds", "30",
         "--parameters", params,
         "--query", "Command.CommandId",
         "--output", "text"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"ssm send-command failed:\n{result.stderr.strip()}")
    cmdid = result.stdout.strip()
    print(f"  Launched: ingest.sh mwdl --skip-harvest")
    print(f"  SSM command id: {cmdid}")
    print(f"  Log on EC2:     {log_path}")
    print()
    print("  Watch #tech-alerts for milestone messages.")


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="Launch MWDL Primo VE ingest.")
    parser.add_argument(
        "--skip-to-harvest",
        action="store_true",
        help="Skip prefix explorer; use existing mwdl-prefixes.json.",
    )
    parser.add_argument(
        "--skip-to-avro",
        action="store_true",
        help="Skip explorer + harvest; use existing mwdl-harvest.jsonl.",
    )
    parser.add_argument(
        "--skip-to-pipeline",
        action="store_true",
        help="Skip all pre-steps; just run ingest.sh mwdl --skip-harvest.",
    )
    args = parser.parse_args()

    if not INSTANCE_ID:
        sys.exit("INGEST_INSTANCE_ID not set in .env")

    print(f"\nMWDL INGEST")
    print(f"Instance: {INSTANCE_ID}")

    if args.skip_to_pipeline:
        run_pipeline()
        return

    if args.skip_to_avro:
        run_jsonl_to_avro()
        run_pipeline()
        return

    if args.skip_to_harvest:
        run_prefix_harvest()
        run_jsonl_to_avro()
        run_pipeline()
        return

    # Full run
    run_prefix_explorer()
    run_prefix_harvest()
    run_jsonl_to_avro()
    run_pipeline()


if __name__ == "__main__":
    main()
