#!/usr/bin/env python3
"""Launch a DPLA hub ingest on the ingest EC2 via SSM.

Usage:
    python3 run/launch_ingest.py                      # prompts for hub
    python3 run/launch_ingest.py bpl                  # runs bpl with ingest.sh
    python3 run/launch_ingest.py community-webs       # runs community-webs-ingest.sh
    python3 run/launch_ingest.py bpl --resume-from mapping

Most hubs run through ingest.sh with the hub name as a positional argument.
A few hubs have dedicated launcher scripts (community-webs is one) — those
are handled automatically based on the HUB_SCRIPTS mapping below. Add a new
entry there if another hub needs a custom script.
"""
import argparse
import json
import os
import re
import subprocess
import sys

INSTANCE_ID = "i-0a0def8581efef783"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")
HUB_RE = re.compile(r"^[a-z0-9_-]+$")
VALID_RESUME_STEPS = ("mapping", "enrichment", "jsonl")

# Hubs that use a dedicated launcher script instead of the standard ingest.sh.
# Per-hub scripts are typically hardcoded to the hub they handle (no positional
# hub argument), so we omit the hub from the invocation. Add new entries here
# as more special-case hubs come up (e.g. nara, smithsonian).
HUB_SCRIPTS = {
    "community-webs": "harvest/community-webs-ingest.sh",
}
DEFAULT_SCRIPT = "ingest.sh"
SCRIPTS_DIR = "/home/ec2-user/ingestion3/scripts"


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch a DPLA hub ingest via SSM.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. bpl, wisconsin). Prompts if omitted.")
    parser.add_argument(
        "--resume-from",
        choices=VALID_RESUME_STEPS,
        help="Resume from a specific step instead of running the full pipeline.",
    )
    args = parser.parse_args()

    hub = (args.hub or input("Hub: ")).strip().lower()
    if not HUB_RE.match(hub):
        sys.exit(f"Invalid hub name: {hub!r} (use letters, digits, hyphens, underscores)")

    extra = f" --resume-from {args.resume_from}" if args.resume_from else ""

    # Pick the script. Hub-specific scripts (e.g. community-webs-ingest.sh) are
    # dedicated to a single hub and don't take a hub positional argument.
    if hub in HUB_SCRIPTS:
        script = HUB_SCRIPTS[hub]
        invocation = f"bash {SCRIPTS_DIR}/{script}{extra}"
        script_label = f"{script} (hub-specific)"
    else:
        invocation = f"bash {SCRIPTS_DIR}/{DEFAULT_SCRIPT} {hub}{extra}"
        script_label = f"{DEFAULT_SCRIPT} {hub}"

    # Build the remote command. The chosen script handles the full pipeline +
    # Slack notifications + safety checks + partner email. We background it
    # because ingests can outlast the SSM timeout.
    inner = (
        'sudo -u ec2-user bash -lc "'
        f"nohup {invocation} > /home/ec2-user/data/{hub}-ingest.log 2>&1 </dev/null &"
        '"'
    )
    params = json.dumps({"commands": [inner]})

    cmd = [
        "aws", "ssm", "send-command",
        "--profile", AWS_PROFILE,
        "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", "30",
        "--parameters", params,
        "--query", "Command.CommandId",
        "--output", "text",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"aws ssm send-command failed:\n{result.stderr.strip()}")

    cmdid = result.stdout.strip()
    label = f"{hub}{extra}".strip()
    print(f"Launched: {label}")
    print(f"  Script:         {script_label}")
    print(f"  SSM command id: {cmdid}")
    print(f"  Log on EC2:     /home/ec2-user/data/{hub}-ingest.log")
    print()
    print("Watch #tech-alerts for milestone messages.")


if __name__ == "__main__":
    main()