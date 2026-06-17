#!/usr/bin/env python3
"""Download all dplava GitHub repos as zip archives to /tmp/virginias-input/ on EC2.

VaFileHarvester expects ZIP files directly in the endpoint directory.
GitHub provides archive zips at:
  https://github.com/dplava/{repo}/archive/refs/heads/{branch}.zip

After downloading, updates i3.conf locally so the endpoint points at the
EC2 local path. You still need to git commit + push i3.conf and git pull
on EC2 before running the ingest.

Usage:
    python3 virginias_download.py
"""
import base64
import json
import os
import re
import subprocess
import sys
import time

DEST        = "/tmp/virginias-input"
INSTANCE_ID = "i-0a0def8581efef783"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")
REGION      = "us-east-1"

REPOS = [
    ("uva",      "master"),
    ("vt",       "master"),
    ("wvu",      "master"),
    ("wm",       "master"),
    ("gmu",      "master"),
    ("vcu",      "master"),
    ("vmfa",     "master"),
    ("vamve",    "main"),
    ("marshall", "main"),
    ("wlu",      "main"),
    ("hsc",      "main"),
]


def ssm_run(shell_cmd: str, poll_seconds: int = 600) -> tuple[str, str]:
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})

    r = subprocess.run(
        ["aws", "ssm", "send-command",
         "--profile", AWS_PROFILE, "--region", REGION,
         "--instance-ids", INSTANCE_ID,
         "--document-name", "AWS-RunShellScript",
         "--timeout-seconds", "30",
         "--parameters", params,
         "--query", "Command.CommandId",
         "--output", "text"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        sys.exit(f"send-command failed:\n{r.stderr.strip()}")

    cmd_id = r.stdout.strip()
    print(f"  SSM cmd: {cmd_id}")

    elapsed = 0
    interval = 10
    while elapsed < poll_seconds:
        time.sleep(interval)
        elapsed += interval
        status = subprocess.run(
            ["aws", "ssm", "get-command-invocation",
             "--profile", AWS_PROFILE, "--region", REGION,
             "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
             "--query", "Status", "--output", "text"],
            capture_output=True, text=True,
        ).stdout.strip()
        print(f"  [{elapsed}s] {status}")
        if status not in ("Pending", "InProgress", "Delayed"):
            break

    out = subprocess.run(
        ["aws", "ssm", "get-command-invocation",
         "--profile", AWS_PROFILE, "--region", REGION,
         "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
         "--query", "StandardOutputContent", "--output", "text"],
        capture_output=True, text=True,
    ).stdout.strip()

    err = subprocess.run(
        ["aws", "ssm", "get-command-invocation",
         "--profile", AWS_PROFILE, "--region", REGION,
         "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
         "--query", "StandardErrorContent", "--output", "text"],
        capture_output=True, text=True,
    ).stdout.strip()

    return out, err


def download_zips():
    curl_cmds = " && ".join(
        f"curl -L -o {DEST}/{repo}.zip https://github.com/dplava/{repo}/archive/refs/heads/{branch}.zip"
        f" && echo 'OK: {repo}'"
        for repo, branch in REPOS
    )
    shell_cmd = f"rm -rf {DEST} && mkdir -p {DEST} && {curl_cmds} && echo DONE && ls -lh {DEST}/"

    print(f"\nDownloading {len(REPOS)} dplava repo zips to EC2:{DEST} ...")
    out, err = ssm_run(shell_cmd, poll_seconds=600)
    if out:
        print(out)
    if err:
        print(f"stderr: {err}")


def update_i3conf():
    conf_path = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "..", "ingestion3-conf", "i3.conf")
    )
    if not os.path.exists(conf_path):
        print(f"  Could not find i3.conf — update manually:")
        print(f'  virginias.harvest.endpoint = "{DEST}"')
        return

    with open(conf_path) as f:
        content = f.read()

    new_content = re.sub(
        r'^(virginias\.harvest\.endpoint\s*=\s*).*$',
        rf'\g<1>"{DEST}"',
        content,
        flags=re.MULTILINE,
    )

    if new_content == content:
        print("  virginias.harvest.endpoint not found in i3.conf — update manually.")
        return

    with open(conf_path, "w") as f:
        f.write(new_content)
    print(f'  i3.conf updated: virginias.harvest.endpoint = "{DEST}"')


def main():
    download_zips()

    print("\nUpdating i3.conf locally ...")
    update_i3conf()

    print("""
Next steps:
  1. git commit + push ingestion3-conf/i3.conf
  2. python3 ec2_gitpull.py
  3. python3 launch_ingest.py virginias
""")


if __name__ == "__main__":
    main()
