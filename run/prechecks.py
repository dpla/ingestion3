#!/usr/bin/env python3
"""
DPLA ingestion pre-flight checks.

Runs the usual before-every-ingest checks against the EC2 box and prints
a clean summary. Exits 0 if everything is GOOD, 1 otherwise.

Usage:
    python3 prechecks.py                            # full checks — prompts for hub
    python3 prechecks.py --hub njde                 # full checks for a specific hub
    python3 prechecks.py --hub njde --endpoint-only # skip box-state checks; just verify the endpoint
    python3 prechecks.py --hub njde --skip-endpoint # opposite: skip the endpoint check
    python3 prechecks.py --no-start                 # don't auto-start the EC2 if it's stopped

The endpoint is ALWAYS read from i3.conf for the given hub — there is no
manual endpoint override. If you need to test a different URL, edit i3.conf
or point I3_CONF at a different file.

With no --hub (and an empty prompt), the endpoint check is skipped and you
just get the base box-state checks (instance, repo, JAR, ownership, disk).

Requirements:
    - aws CLI installed and authenticated (aws sts get-caller-identity should work)
    - IAM permissions for ec2:DescribeInstances, ec2:StartInstances,
      ssm:SendCommand, ssm:GetCommandInvocation on the target instance
    - curl in PATH (for the endpoint check)
"""

import argparse
import base64
import json
import os
import re
import shlex
import subprocess
import sys
import time

# ---------- config ----------
INSTANCE_ID = "i-0a0def8581efef783"

# Two repos that have to be in sync on the box for ingests to work correctly:
#   - ingestion3      → Scala/Spark pipeline code; lives on `main`.
#   - ingestion3-conf → HOCON hub config (i3.conf); lives on `master`.
# Both are checked because either being stale silently breaks ingests in
# different ways (stale code → wrong behavior; stale conf → harvester reads
# wrong endpoint, may NPE, may use deleted directories, etc.).
INGEST_REPO = {
    "path":   "/home/ec2-user/ingestion3",
    "branch": "main",
    "label":  "ingestion3 (code)",
}
CONF_REPO = {
    "path":   "/home/ec2-user/ingestion3-conf",
    "branch": "master",
    "label":  "ingestion3-conf (i3.conf)",
}

# Kept for backwards compatibility with any external callers — points at the
# code repo, since that's what the original constants targeted.
REPO_PATH = INGEST_REPO["path"]
REMOTE_URL = "https://github.com/dpla/ingestion3.git"
REMOTE_BRANCH = INGEST_REPO["branch"]

# Where the HOCON hub config lives on the local machine.
# Defaults to Zoe's checkout at ~/Documents/Repos/ingestion3-conf/i3.conf,
# but the I3_CONF env var wins if set — matches the convention used in the
# ingestion3 .env file (see §7 of the onboarding doc).
CONF_PATH = os.environ.get("I3_CONF") or os.path.expanduser(
    "~/Documents/Repos/ingestion3-conf/i3.conf"
)

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
def check_instance_state(auto_start=True):
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
        if not auto_start:
            warn("Instance is stopped and --no-start was set. Skipping startup.")
            return False
        info("Starting instance...")
        aws(["ec2", "start-instances", "--instance-ids", INSTANCE_ID])
        aws(["ec2", "wait", "instance-running", "--instance-ids", INSTANCE_ID])
        info("Waiting 30s for SSM agent to come up...")
        time.sleep(30)
        ok("Instance started.")
        return True
    bad(f"Instance in unexpected state '{state}'. Fix manually.")
    return False

def check_repo_sync(repo, header_num="2", auto_reset=False, interactive=True):
    """Check if a repo on the box is in sync with origin/<branch>.

    `repo` is a dict with keys: path, branch, label.
    Uses the box's existing `origin` remote (already authenticated for both
    code and conf repos as of the deploy-key fix), so no remote URL needs to
    be passed.

    Behavior when the box is behind origin:
      - auto_reset=True   → pull immediately without prompting
      - interactive=True  → prompt the user "Pull N commits? [y/N]"
      - both False        → warn and return False (operator must reset manually)
    """
    header(f"{header_num}. Repo sync: {repo['label']}")
    cmd = (
        f"cd {repo['path']} && "
        f"git fetch origin {repo['branch']} 2>/dev/null && "
        "echo '--- LOCAL ---' && git log --oneline -1 HEAD && "
        f"echo '--- REMOTE ---' && git log --oneline -1 origin/{repo['branch']} && "
        f"echo '--- DIFF ---' && git rev-list --left-right --count HEAD...origin/{repo['branch']}"
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

    branch = repo["branch"]
    if ahead == 0 and behind == 0:
        ok(f"{repo['label']}: in sync with origin/{branch}.")
        return True

    if behind > 0:
        info(f"{repo['label']}: behind origin/{branch} by {behind} commits.")

        do_pull = auto_reset
        if not do_pull and interactive:
            try:
                answer = input(
                    f"  Pull {behind} commit(s) from origin/{branch} into "
                    f"{repo['path']} on the box? [y/N] "
                ).strip().lower()
                do_pull = answer in ("y", "yes")
            except EOFError:
                do_pull = False

        if do_pull:
            info(f"Pulling: git reset --hard origin/{branch} on the box...")
            reset_cmd = (
                f"cd {repo['path']} && "
                f"git reset --hard origin/{branch} && "
                "git log --oneline -1"
            )
            print(ssm_run(reset_cmd).rstrip())
            ok(f"{repo['label']}: reset complete.")
            return True

        warn(f"{repo['label']}: behind origin — left as-is. Run git reset --hard origin/{branch} on the box when ready.")
        return False

    # ahead > 0, behind == 0
    warn(f"{repo['label']}: {ahead} commits ahead of origin/{branch} (unpushed changes on the box).")
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

def lookup_hub_in_conf(hub, conf_path=CONF_PATH):
    """
    Parse i3.conf (HOCON) and return (endpoint, harvest_type) for the hub.
    Returns (None, None) if not found. harvest_type may be 'oai', 'api', 'pss',
    'resourceSync', 'file', etc. — or None if not detected.

    This is a regex-based HOCON reader, not a full parser. It handles the two
    common shapes in the DPLA conf:
        illinois {
          harvest {
            type = "oai"
            endpoint = "https://..."
          }
        }
    and the flattened dotted-key form:
        illinois.harvest.type = "oai"
        illinois.harvest.endpoint = "https://..."
    """
    if not os.path.exists(conf_path):
        return None, None

    with open(conf_path, "r", encoding="utf-8") as f:
        text = f.read()

    # Strip comments (# ... or // ...) to simplify matching.
    text = re.sub(r"(?m)^\s*(#|//).*$", "", text)
    text = re.sub(r"(?<!:)//[^\n]*", "", text)  # trailing //-style comments

    endpoint = None
    harvest_type = None

    # --- shape 1: a block `hub { ... }`
    # Find `hub {` and then grab everything up to the matching brace.
    block_match = re.search(
        rf"(?ms)^\s*{re.escape(hub)}\s*(?:=|:)?\s*\{{(.*)",
        text,
    )
    if block_match:
        # Walk forward counting braces to find the matching close.
        depth = 1
        start = block_match.start(1)
        i = start
        while i < len(text) and depth > 0:
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
            i += 1
        block = text[start:i - 1] if depth == 0 else text[start:]

        ep_match = re.search(r"""endpoint\s*[=:]\s*["']([^"']+)["']""", block)
        if ep_match:
            endpoint = ep_match.group(1)
        type_match = re.search(r"""type\s*[=:]\s*["']?([A-Za-z]+)["']?""", block)
        if type_match:
            harvest_type = type_match.group(1).lower()

    # --- shape 2: dotted keys `hub.harvest.endpoint = "..."`
    if endpoint is None:
        ep_match = re.search(
            rf"""{re.escape(hub)}\.harvest\.endpoint\s*[=:]\s*["']([^"']+)["']""",
            text,
        )
        if ep_match:
            endpoint = ep_match.group(1)
    if harvest_type is None:
        type_match = re.search(
            rf"""{re.escape(hub)}\.harvest\.type\s*[=:]\s*["']?([A-Za-z]+)["']?""",
            text,
        )
        if type_match:
            harvest_type = type_match.group(1).lower()

    return endpoint, harvest_type


def _check_local_path_endpoint(path):
    """For file-type harvests with a local path on EC2: verify the path
    exists, is a directory (or file), and is non-empty. Catches the
    'CommunityWebsHarvester NPE' class of bug where the conf endpoint
    points at a stale or missing directory."""
    info(f"Endpoint: {path}")
    info("Type:     file-based (local path on EC2)")

    # Catch dev paths leaking into production conf — saves an SSM round-trip
    # and gives a much clearer error than "path does not exist on the box".
    # Real cases we've hit: /Users/scott/... (someone's Mac path committed),
    # /var/folders/... (Mac temp dir from local testing).
    dev_path_patterns = [
        (r"^/Users/",       "macOS user dir — almost certainly a developer's local path"),
        (r"^/var/folders/", "macOS temp dir — leftover from someone's local testing run"),
        (r"^/home/(?!ec2-user(/|$))", "Linux home dir for a non-ec2-user account"),
        (r"^[A-Za-z]:[\\/]", "Windows path — definitely not the EC2 box"),
        (r"^~",             "tilde-prefixed path that wasn't expanded by the conf reader"),
    ]
    for pattern, reason in dev_path_patterns:
        if re.match(pattern, path):
            bad("Endpoint looks like a developer's local path, not an EC2 path.")
            info(f"Reason: {reason}")
            info("Likely cause: conf was committed from someone's local dev environment")
            info("and never updated for production.")
            info("Fix: update <hub>.harvest.endpoint in i3.conf to the actual location")
            info("on the box (typically /home/ec2-user/data/<hub>/originalRecords/<DATE>/),")
            info("then push the conf change and pull on the box.")
            return False

    quoted = shlex.quote(path)
    cmd = f"""
if [ ! -e {quoted} ]; then
  echo "VERDICT: NOT GOOD -- path does not exist on the box."
  echo "Likely fix: re-run preprocessing for this hub (per onboarding doc §14)"
  echo "and update <hub>.harvest.endpoint in i3.conf to the new dated directory."
  exit 0
fi
if [ -d {quoted} ]; then
  echo "Path exists as a directory."
  echo "Top of listing:"
  ls -la {quoted} 2>&1 | head -10
  COUNT=$(find {quoted} -maxdepth 3 -type f 2>/dev/null | wc -l)
  echo "Files (up to 3 levels deep): $COUNT"
  if [ "$COUNT" -eq 0 ]; then
    echo "VERDICT: NOT GOOD -- directory is empty."
    echo "The harvester will trip on dir.listFiles() and throw NPE."
    echo "Fix: place the expected file (zip / xml / db) inside this directory."
  else
    echo "VERDICT: GOOD -- directory exists and contains $COUNT files."
  fi
elif [ -f {quoted} ]; then
  echo "Path exists as a regular file."
  ls -la {quoted} 2>&1
  echo "VERDICT: GOOD -- file exists."
else
  echo "VERDICT: NOT GOOD -- path exists but is neither file nor directory."
fi
""".strip()
    try:
        out = ssm_run(cmd)
    except RuntimeError as e:
        bad(f"SSM check failed: {e}")
        return False
    print(out.rstrip())
    if "VERDICT: GOOD" in out:
        ok("File endpoint is valid.")
        return True
    bad("File endpoint missing or empty — fix before running the ingest.")
    return False


def _check_s3_endpoint(s3_path):
    """For file-type harvests with an s3:// endpoint (e.g. dpla-hub-* buckets):
    verify the path exists and has at least one object."""
    info(f"Endpoint: {s3_path}")
    info("Type:     file-based (S3)")
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", s3_path],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        bad("aws s3 ls timed out after 30s.")
        return False
    if result.returncode != 0:
        bad(f"aws s3 ls failed: {result.stderr.strip() or 'exit ' + str(result.returncode)}")
        return False
    listing = result.stdout.strip()
    if not listing:
        bad(f"S3 path is empty: {s3_path}")
        info("Fix: ensure the hub has uploaded a fresh export to this prefix,")
        info("or update the endpoint in i3.conf to the correct dated subfolder.")
        return False
    info("--- first 5 entries ---")
    for line in listing.splitlines()[:5]:
        info(line)
    ok("S3 endpoint exists and has objects.")
    return True


def check_endpoint(endpoint, is_api=False):
    header("6. Endpoint reachability")
    if not endpoint:
        warn("No endpoint to check (pass --hub or enter one at the prompt).")
        return True  # non-blocking

    # Dispatch based on the endpoint scheme. File-type harvests get a real
    # existence/contents check on the box (or in S3); HTTP/S endpoints get
    # the curl-based reachability check below.
    if endpoint.startswith("s3://"):
        return _check_s3_endpoint(endpoint)
    if not re.match(r"^https?://", endpoint):
        return _check_local_path_endpoint(endpoint)

    timeout = 60 if is_api else 15
    test_url = endpoint if is_api else f"{endpoint}?verb=Identify"
    kind = "API" if is_api else "OAI"

    info(f"URL:     {test_url}")
    info(f"Timeout: {timeout}s ({kind})")

    try:
        result = subprocess.run(
            ["curl", "-sS", "--max-time", str(timeout), test_url],
            capture_output=True, text=True, timeout=timeout + 10,
        )
    except subprocess.TimeoutExpired:
        bad(f"curl timed out after {timeout}s — endpoint likely down.")
        return False
    except FileNotFoundError:
        bad("curl not found in PATH. Install curl or skip this check.")
        return False

    if result.returncode != 0:
        err = result.stderr.strip() or f"curl exit code {result.returncode}"
        bad(f"curl failed: {err}")
        return False

    body = result.stdout
    if not body.strip():
        bad("Empty response — endpoint reachable but returned nothing.")
        return False

    info("--- first 5 lines of response ---")
    for line in body.splitlines()[:5]:
        info(line[:200])
    info("---")

    if is_api:
        ok("Endpoint returned a non-empty response.")
        return True

    # OAI-specific sanity check
    lower = body.lower()
    if "<oai-pmh" in lower or "<identify" in lower:
        ok("OAI endpoint responded with valid-looking XML.")
        return True
    if "<error" in lower:
        bad("OAI endpoint returned an <error> element. Review output above.")
        return False
    warn("OAI endpoint responded, but no <OAI-PMH>/<Identify> tags found.")
    return False


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="DPLA ingestion pre-flight checks")
    parser.add_argument(
        "--hub",
        help="Hub name, e.g. 'njde'. Used to look up the endpoint from i3.conf. Optional.",
    )
    parser.add_argument(
        "--api", action="store_true",
        help="Force API-style check (60s timeout, no ?verb=Identify). Auto-detected from conf when --hub is used.",
    )
    parser.add_argument(
        "--endpoint-only", action="store_true",
        help="Skip the box-state checks (instance, repo, JAR, ownership, disk) and only run the endpoint check.",
    )
    parser.add_argument(
        "--skip-endpoint", action="store_true",
        help="Run all the box-state checks but skip the endpoint check.",
    )
    parser.add_argument(
        "--no-start", action="store_true",
        help="Don't auto-start the EC2 instance if it's stopped (useful with --endpoint-only for HTTP endpoints).",
    )
    parser.add_argument(
        "--auto-pull", action="store_true",
        help="Pull both repos automatically if they're behind origin (no prompt).",
    )
    parser.add_argument(
        "--no-pull", action="store_true",
        help="Never pull, even if behind. Just warn — useful for non-interactive runs.",
    )
    args = parser.parse_args()
    if args.endpoint_only and args.skip_endpoint:
        sys.exit("--endpoint-only and --skip-endpoint are mutually exclusive.")
    if args.auto_pull and args.no_pull:
        sys.exit("--auto-pull and --no-pull are mutually exclusive.")

    # If no --hub was passed, prompt for one. Empty input = skip endpoint check.
    hub = args.hub
    if hub is None:
        try:
            entered = input("Hub to check (leave blank to skip endpoint check): ").strip().lower()
        except EOFError:
            entered = ""
        if entered:
            if not re.match(r"^[a-z0-9_-]+$", entered):
                sys.exit(f"Invalid hub name: {entered!r} (use letters, digits, hyphens, underscores)")
            hub = entered

    print("\nDPLA INGESTION PRE-FLIGHT CHECKS")
    print(f"Instance: {INSTANCE_ID}")
    print(f"Conf:     {CONF_PATH}")
    if hub:
        print(f"Hub:      {hub}")

    # Resolve endpoint + mode strictly from i3.conf.
    endpoint = None
    is_api = args.api
    # Harvest types that should be tested as OAI-PMH (with ?verb=Identify
    # and validated by looking for <OAI-PMH>/<Identify> in the response).
    OAI_TYPES = ("oai", "localoai")
    if hub:
        looked_up_endpoint, harvest_type = lookup_hub_in_conf(hub)
        if looked_up_endpoint:
            endpoint = looked_up_endpoint
            # Auto-detect API vs OAI unless user forced it with --api.
            # Anything that's not in OAI_TYPES and not file-based gets API treatment.
            if not is_api and harvest_type and harvest_type not in OAI_TYPES:
                is_api = True
            print(f"Endpoint: {endpoint}  (from conf, type={harvest_type or 'unknown'})")
        else:
            print(f"Endpoint: (not found in {CONF_PATH} for hub '{hub}')")
    else:
        print("Endpoint: (none — endpoint check will be skipped)")
    print()

    # Decide which checks to run based on the flags.
    auto_start = not args.no_start
    run_box_checks = not args.endpoint_only
    run_endpoint_check = not args.skip_endpoint
    # File-type endpoints need SSM (the box must be up); HTTP/S endpoints don't.
    needs_box_for_endpoint = (
        run_endpoint_check
        and endpoint
        and not endpoint.startswith("s3://")
        and not re.match(r"^https?://", endpoint)
    )

    results = []
    try:
        # Always run the instance check if we need the box for ANY check
        # (file endpoint or any of the box-state checks).
        if run_box_checks or needs_box_for_endpoint:
            results.append(("instance", check_instance_state(auto_start=auto_start)))
        if run_box_checks:
            interactive_pull = not args.no_pull
            results.append(("ingestion3 repo",      check_repo_sync(INGEST_REPO, header_num="2a", auto_reset=args.auto_pull, interactive=interactive_pull)))
            results.append(("ingestion3-conf repo", check_repo_sync(CONF_REPO,   header_num="2b", auto_reset=args.auto_pull, interactive=interactive_pull)))
            results.append(("jar",                  check_jar_freshness()))
            results.append(("ownership",            check_target_ownership()))
            results.append(("disk",                 check_disk_space()))
        if run_endpoint_check:
            results.append(("endpoint", check_endpoint(endpoint, is_api=is_api)))
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