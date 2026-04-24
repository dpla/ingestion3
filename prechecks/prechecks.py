#!/usr/bin/env python3
"""
DPLA ingestion pre-flight checks.

Runs the usual before-every-ingest checks against the EC2 box and prints
a clean summary. Exits 0 if everything is GOOD, 1 otherwise.

Usage:
    python3 prechecks.py

Requirements:
    - aws CLI installed and authenticated (aws sts get-caller-identity should work)
    - IAM permissions for ec2:DescribeInstances, ec2:StartInstances,
      ssm:SendCommand, ssm:GetCommandInvocation on the target instance
"""

import base64
import json
import subprocess
import sys
import time

# ---------- config ----------
INSTANCE_ID = "i-0a0def8581efef783"
REPO_PATH = "/home/ec2-user/ingestion3"
REMOTE_URL = "https://github.com/dpla/ingestion3.git"
REMOTE_BRANCH = "main"

# ---------- tiny output helpers ----------
def header(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)

def ok(msg):
    print(f"  [GOOD]    {msg}")

def warn(msg):
    print(f"  [CAUTION] {msg}")

def bad(msg):
    print(f"  [BAD]     {msg}")

def info(msg):
    print(f"  {msg}")

# ---------- AWS CLI wrappers ----------
def aws(args, capture=True):
    """Run an aws CLI command. Returns stdout text (stripped) on success."""
    result = subprocess.run(
        ["aws"] + args,
        capture_output=capture,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"aws {' '.join(args)} failed:\n{result.stderr.strip()}"
        )
    return result.stdout.strip()

def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    """
    Send a shell command to the EC2 instance via SSM (as ec2-user, login shell)
    and return its standard output. Raises RuntimeError on failure.
    """
    # Base64-encode the payload so the outer shell (running as root via SSM)
    # can't mangle any $VAR / $() / quotes before the inner ec2-user bash sees it.
    encoded = base64.b64encode(shell_cmd.encode("utf-8")).decode("ascii")
    wrapped = (
        f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    )
    params = json.dumps({"commands": [wrapped]})

    cmd_id = aws([
        "ssm", "send-command",
        "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", str(timeout_seconds),
        "--parameters", params,
        "--query", "Command.CommandId",
        "--output", "text",
    ])

    # Poll for completion.
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--query", "Status",
            "--output", "text",
        ])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM command {cmd_id} timed out after {timeout_seconds}s")

    output = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])
    err = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardErrorContent",
        "--output", "text",
    ])

    if status != "Success":
        raise RuntimeError(
            f"SSM command ended with status {status}.\n"
            f"STDOUT:\n{output}\nSTDERR:\n{err}"
        )
    return output

# ---------- the actual checks ----------
def check_instance_state():
    header("1. EC2 instance state")
    raw = aws([
        "ec2", "describe-instances",
        "--instance-ids", INSTANCE_ID,
        "--query", "Reservations[0].Instances[0].{State:State.Name,Type:InstanceType,IP:PublicIpAddress}",
        "--output", "json",
    ])
    data = json.loads(raw)
    state = data.get("State")
    info(f"Instance: {INSTANCE_ID}")
    info(f"Type:     {data.get('Type')}")
    info(f"IP:       {data.get('IP')}")
    info(f"State:    {state}")
    if state == "running":
        ok("Instance already running.")
        return True
    if state in ("stopped", "stopping"):
        info("Starting instance...")
        aws(["ec2", "start-instances", "--instance-ids", INSTANCE_ID])
        aws(["ec2", "wait", "instance-running", "--instance-ids", INSTANCE_ID])
        info("Waiting 30s for SSM agent to come up...")
        time.sleep(30)
        ok("Instance started.")
        return True
    bad(f"Instance in unexpected state '{state}'. Fix manually.")
    return False

def check_repo_sync(auto_reset=False):
    header("2. Repo is on the latest commit")
    cmd = (
        f"cd {REPO_PATH} && "
        f"git fetch {REMOTE_URL} {REMOTE_BRANCH} 2>/dev/null && "
        "echo '--- LOCAL ---' && git log --oneline -1 HEAD && "
        "echo '--- REMOTE ---' && git log --oneline -1 FETCH_HEAD && "
        "echo '--- DIFF ---' && git rev-list --left-right --count HEAD...FETCH_HEAD"
    )
    out = ssm_run(cmd)
    print(out.rstrip())

    # Parse the ahead/behind counts.
    ahead, behind = 0, 0
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            ahead, behind = int(parts[0]), int(parts[1])
            break

    if ahead == 0 and behind == 0:
        ok("Local is in sync with origin/main.")
        return True

    if behind > 0:
        info(f"Local is behind origin/main by {behind} commits.")
        if auto_reset:
            info("Auto-reset enabled: running git reset --hard FETCH_HEAD...")
            reset_cmd = f"cd {REPO_PATH} && git reset --hard FETCH_HEAD && git log --oneline -1"
            print(ssm_run(reset_cmd).rstrip())
            ok("Reset complete.")
            return True
        warn("Behind origin — consider running: git reset --hard FETCH_HEAD on the box.")
        return False

    # ahead > 0, behind == 0
    warn(f"Local is {ahead} commits ahead of origin/main (unpushed changes on the box).")
    info("Probably safe if your hub isn't affected by those commits. Flag it to your team.")
    return True  # non-blocking

def check_jar_freshness():
    header("3. Fat JAR freshness")
    cmd = f"""
cd {REPO_PATH}
JAR=$(ls -1 target/scala-*/ingestion3-assembly-*.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
  echo "JAR:    (not found)"
  echo "COMMIT: $(git log -1 --format=%cd --date=iso)"
  echo "VERDICT: NOT GOOD -- no JAR exists. Run sbt assembly before ingesting."
  exit 0
fi
JAR_EPOCH=$(stat -c %Y "$JAR")
COMMIT_EPOCH=$(git log -1 --format=%ct)
JAR_DATE=$(date -d @$JAR_EPOCH "+%Y-%m-%d %H:%M:%S")
COMMIT_DATE=$(date -d @$COMMIT_EPOCH "+%Y-%m-%d %H:%M:%S")
echo "JAR:    $JAR_DATE   ($JAR)"
echo "COMMIT: $COMMIT_DATE   ($(git log -1 --format='%h %s'))"
if [ $JAR_EPOCH -ge $COMMIT_EPOCH ]; then
  DIFF_HOURS=$(( (JAR_EPOCH - COMMIT_EPOCH) / 3600 ))
  echo "VERDICT: GOOD -- JAR is $DIFF_HOURS hours newer than the latest commit."
else
  DIFF_HOURS=$(( (COMMIT_EPOCH - JAR_EPOCH) / 3600 ))
  echo "VERDICT: NOT GOOD -- JAR is $DIFF_HOURS hours older than the latest commit. Run sbt assembly."
fi
""".strip()
    out = ssm_run(cmd)
    print(out.rstrip())
    if "VERDICT: GOOD" in out:
        ok("JAR is fresh.")
        return True
    bad("JAR is stale or missing — rebuild with `sbt assembly` on the box before ingesting.")
    return False

def check_target_ownership():
    header("4. target/ ownership")
    cmd = f"""
cd {REPO_PATH}
ROOT_COUNT=$(find target -user root 2>/dev/null | wc -l)
EC2_COUNT=$(find target -user ec2-user 2>/dev/null | wc -l)
echo "Files owned by root:     $ROOT_COUNT"
echo "Files owned by ec2-user: $EC2_COUNT"
if [ $ROOT_COUNT -gt 0 ]; then
  echo "VERDICT: NOT GOOD -- run chown -R ec2-user:ec2-user {REPO_PATH}/target/"
else
  echo "VERDICT: GOOD -- target/ ownership is clean."
fi
""".strip()
    out = ssm_run(cmd)
    print(out.rstrip())
    if "VERDICT: GOOD" in out:
        ok("target/ ownership is clean.")
        return True
    bad(f"Fix ownership: chown -R ec2-user:ec2-user {REPO_PATH}/target/")
    return False

def check_disk_space():
    header("5. Disk space")
    cmd = """
df -h /home/ec2-user /tmp
echo ---
AVAIL_GB=$(df --output=avail -BG /home/ec2-user | tail -1 | tr -d "G ")
if [ $AVAIL_GB -lt 20 ]; then
  echo "VERDICT: NOT GOOD -- only ${AVAIL_GB}GB free on /home/ec2-user. Clean up before ingesting."
elif [ $AVAIL_GB -lt 50 ]; then
  echo "VERDICT: CAUTION -- ${AVAIL_GB}GB free. Should be enough for a mid-size hub, but watch it."
else
  echo "VERDICT: GOOD -- ${AVAIL_GB}GB free on /home/ec2-user."
fi
""".strip()
    out = ssm_run(cmd)
    print(out.rstrip())
    if "VERDICT: GOOD" in out:
        ok("Plenty of disk.")
        return True
    if "VERDICT: CAUTION" in out:
        warn("Disk is getting tight — proceed, but keep an eye on it.")
        return True
    bad("Not enough disk — clean up /home/ec2-user/data/ before ingesting.")
    return False

# ---------- main ----------
def main():
    print("\nDPLA INGESTION PRE-FLIGHT CHECKS")
    print(f"Instance: {INSTANCE_ID}\n")

    results = []
    try:
        results.append(("instance",  check_instance_state()))
        results.append(("repo",      check_repo_sync(auto_reset=False)))
        results.append(("jar",       check_jar_freshness()))
        results.append(("ownership", check_target_ownership()))
        results.append(("disk",      check_disk_space()))
    except RuntimeError as e:
        print()
        bad(f"Check aborted: {e}")
        sys.exit(2)

    header("SUMMARY")
    for name, passed in results:
        (ok if passed else bad)(name)

    if all(passed for _, passed in results):
        print("\nAll checks passed. Safe to kick off the ingest.\n")
        sys.exit(0)
    else:
        print("\nOne or more checks failed. Fix the issues above before ingesting.\n")
        sys.exit(1)

if __name__ == "__main__":
    main()