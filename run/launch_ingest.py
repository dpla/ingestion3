#!/usr/bin/env python3
"""Launch a DPLA hub ingest on the ingest EC2 via SSM.

Works for all hub types (localoai, api, file). For file-export hubs the
script fetches the harvest type from i3.conf, lists available S3 deliveries,
and prompts you to confirm the endpoint before launching.

Usage:
    python3 run/launch_ingest.py                      # prompts for hub
    python3 run/launch_ingest.py bpl                  # runs ingest.sh bpl
    python3 run/launch_ingest.py ohio                 # detects file hub, checks S3
    python3 run/launch_ingest.py ohio --bucket dpla-hub-ohio
    python3 run/launch_ingest.py bpl --resume-from mapping
"""
import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time

INSTANCE_ID = "i-0a0def8581efef783"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")
HUB_RE = re.compile(r"^[a-z0-9_-]+$")
VALID_RESUME_STEPS = ("mapping", "enrichment", "jsonl")

SCRIPTS_DIR = "/home/ec2-user/ingestion3/scripts"
CONF_PATH   = "/home/ec2-user/ingestion3-conf/i3.conf"


# ── SSM helpers ───────────────────────────────────────────────────────────────
def ssm_run(shell_cmd: str) -> str:
    """Run a short command on the EC2 box via SSM and return stdout."""
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})
    r = subprocess.run(
        ["aws", "ssm", "send-command",
         "--profile", AWS_PROFILE,
         "--instance-ids", INSTANCE_ID,
         "--document-name", "AWS-RunShellScript",
         "--timeout-seconds", "20",
         "--parameters", params,
         "--query", "Command.CommandId",
         "--output", "text"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return ""
    cmd_id = r.stdout.strip()
    for _ in range(8):
        time.sleep(3)
        status = subprocess.run(
            ["aws", "ssm", "get-command-invocation",
             "--command-id", cmd_id,
             "--instance-id", INSTANCE_ID,
             "--query", "Status", "--output", "text"],
            capture_output=True, text=True,
        ).stdout.strip()
        if status not in ("Pending", "InProgress", "Delayed"):
            break
    return subprocess.run(
        ["aws", "ssm", "get-command-invocation",
         "--command-id", cmd_id,
         "--instance-id", INSTANCE_ID,
         "--query", "StandardOutputContent", "--output", "text"],
        capture_output=True, text=True,
    ).stdout.strip()


def aws_s3_ls(s3_path: str) -> str:
    try:
        r = subprocess.run(
            ["aws", "s3", "ls", s3_path, "--profile", AWS_PROFILE],
            capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    return r.stdout if r.returncode == 0 else ""


# ── i3.conf helpers ───────────────────────────────────────────────────────────
def get_harvest_type(hub: str) -> str:
    out = ssm_run(f"grep '^{hub}\\.harvest\\.type' {CONF_PATH} 2>/dev/null || echo ''")
    m = re.search(r'=\s*"?([^"\s]+)"?', out)
    return m.group(1) if m else ""


def get_current_endpoint(hub: str) -> str:
    return ssm_run(
        f"grep '^{hub}\\.harvest\\.endpoint' {CONF_PATH} 2>/dev/null || echo '(not found)'"
    )


# ── S3 delivery helpers ───────────────────────────────────────────────────────
def list_deliveries(bucket: str) -> list[str]:
    out = aws_s3_ls(f"s3://{bucket}/")
    folders = []
    for line in out.splitlines():
        m = re.search(r"PRE\s+(\d{4}-?\d{2}-?\d{2})/", line)
        if m:
            folders.append(m.group(1))
    return sorted(folders, reverse=True)


# ── file-hub pre-flight ───────────────────────────────────────────────────────
def file_hub_preflight(hub: str, bucket: str) -> None:
    """Show S3 deliveries and current i3.conf endpoint; prompt to confirm."""
    print(f"\nChecking s3://{bucket}/ for deliveries …")
    deliveries = list_deliveries(bucket)
    if not deliveries:
        print(f"  No dated delivery folders found in s3://{bucket}/")
        print("  Check the bucket name or whether a delivery has arrived yet.")
        sys.exit(1)

    print(f"  Available deliveries:")
    for d in deliveries[:5]:
        print(f"    {d}")
    if len(deliveries) > 5:
        print(f"    … and {len(deliveries) - 5} older")

    print(f"\nCurrent i3.conf endpoint for {hub}:")
    print(f"  {get_current_endpoint(hub)}")

    print(f"""
⚠️  Before continuing, make sure i3.conf is pointing at the right delivery:

    {hub}.harvest.endpoint = "s3://{bucket}/{deliveries[0]}/"

If it needs updating:
    1. Edit ingestion3-conf/i3.conf locally
    2. git commit + push
    3. git pull on the EC2 box
""")
    try:
        ans = input("i3.conf is correct — launch ingest? [y/n]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        sys.exit("\nAborted.")
    if ans not in ("y", "yes"):
        sys.exit("Aborted.")


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Launch a DPLA hub ingest via SSM.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. bpl, ohio). Prompts if omitted.")
    parser.add_argument("--bucket", help="S3 bucket for file hubs (default: dpla-hub-<hub>).")
    parser.add_argument(
        "--resume-from",
        choices=VALID_RESUME_STEPS,
        help="Resume from a specific step instead of running the full pipeline.",
    )
    args = parser.parse_args()

    hub = (args.hub or input("Hub: ")).strip().lower()
    if not HUB_RE.match(hub):
        sys.exit(f"Invalid hub name: {hub!r}")

    # Check harvest type and run file-hub pre-flight if needed.
    print(f"Checking harvest type for {hub} …")
    harvest_type = get_harvest_type(hub)
    print(f"  harvest.type = {harvest_type or '(not found)'}")

    if harvest_type == "file":
        bucket = args.bucket or f"dpla-hub-{hub}"
        file_hub_preflight(hub, bucket)

    # Launch.
    extra = f" --resume-from {args.resume_from}" if args.resume_from else ""
    invocation = f"bash {SCRIPTS_DIR}/ingest.sh {hub}{extra}"
    inner = (
        'sudo -u ec2-user bash -lc "'
        f"nohup {invocation} > /home/ec2-user/data/{hub}-ingest.log 2>&1 </dev/null &"
        '"'
    )
    params = json.dumps({"commands": [inner]})

    result = subprocess.run(
        ["aws", "ssm", "send-command",
         "--profile", AWS_PROFILE,
         "--instance-ids", INSTANCE_ID,
         "--document-name", "AWS-RunShellScript",
         "--timeout-seconds", "30",
         "--parameters", params,
         "--query", "Command.CommandId",
         "--output", "text"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"aws ssm send-command failed:\n{result.stderr.strip()}")

    cmdid = result.stdout.strip()
    print(f"\nLaunched: {hub}{extra}")
    print(f"  SSM command id: {cmdid}")
    print(f"  Log on EC2:     /home/ec2-user/data/{hub}-ingest.log")
    print()
    print("Watch #tech-alerts for milestone messages.")


if __name__ == "__main__":
    main()