#!/usr/bin/env python3
"""Launch a DPLA hub ingest on the ingest EC2 via SSM.

For localoai and api hubs only. File-export hubs (CT, FL, Ohio, NYPL, etc.)
use launch_file_ingest.py instead, which handles S3 delivery detection and
i3.conf endpoint verification before launching.

Usage:
    python3 run/launch_ingest.py                      # prompts for hub
    python3 run/launch_ingest.py bpl                  # runs ingest.sh bpl
    python3 run/launch_ingest.py bpl --resume-from mapping
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

SCRIPTS_DIR = "/home/ec2-user/ingestion3/scripts"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch a localoai/api hub ingest via SSM."
    )
    parser.add_argument(
        "hub", nargs="?",
        help="Hub name (e.g. bpl, wisconsin). Prompts if omitted.",
    )
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
    invocation = f"bash {SCRIPTS_DIR}/ingest.sh {hub}{extra}"

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
    print(f"  SSM command id: {cmdid}")
    print(f"  Log on EC2:     /home/ec2-user/data/{hub}-ingest.log")
    print()
    print("Watch #tech-alerts for milestone messages.")


if __name__ == "__main__":
    main()