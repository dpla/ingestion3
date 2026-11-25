#!/usr/bin/env python3
"""Launch a DPLA hub ingest on the ingest EC2 via SSM.

Works for all hub types (localoai, api, file). For file-export hubs the
script fetches the harvest type from i3.conf, lists available S3 deliveries,
and shows which one the harvest will read (the newest) before launching.

Usage (from the repo root):
    python3 ingest_pipeline_python/3_launch_ingest.py                   # prompts for hub
    python3 ingest_pipeline_python/3_launch_ingest.py bpl               # runs ingest.sh bpl
    python3 ingest_pipeline_python/3_launch_ingest.py ohio              # detects file hub, checks S3
    python3 ingest_pipeline_python/3_launch_ingest.py ohio --bucket dpla-hub-ohio
    python3 ingest_pipeline_python/3_launch_ingest.py bpl --resume-from mapping
    python3 ingest_pipeline_python/3_launch_ingest.py bpl --skip-email  # no partner summary email
    python3 ingest_pipeline_python/3_launch_ingest.py bpl --no-prune    # don't offer to delete old runs

Before launching (interactive runs only), it shows a dry run of
scripts/prune-hub-data.sh for the hub and asks whether to delete old runs from
the box. On yes, ingest.sh gets --prune and does it at the start of the run:
the newest 2 complete runs per step are kept, older ones are deleted once
they're in S3 (harvest/mapping/enrichment are synced first if they aren't).
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

from helper_scripts.hub_conf import lookup_hub_in_conf

def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
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
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
))
# Instance ID: real env var first (CI), .env fallback (local operator).
INSTANCE_ID = os.environ.get("INGEST_INSTANCE_ID") or _env.get("INGEST_INSTANCE_ID", "")
# AWS profile: env var → .env → "dpla" when .env exists → None (CI role creds).
AWS_PROFILE = (
    os.environ.get("AWS_PROFILE")
    or _env.get("AWS_PROFILE")
    or ("dpla" if _env_file_exists else None)
)
HUB_RE = re.compile(r"^[a-z0-9_-]+$")
VALID_RESUME_STEPS = ("mapping", "enrichment", "jsonl")

SCRIPTS_DIR = "/home/ec2-user/ingestion3/scripts"
CONF_PATH   = "/home/ec2-user/ingestion3-conf/i3.conf"


# ── SSM helpers ───────────────────────────────────────────────────────────────
def _profile_args() -> list[str]:
    """Return ['--profile', AWS_PROFILE] if a profile is configured, else []."""
    return ["--profile", AWS_PROFILE] if AWS_PROFILE else []


def ssm_run(shell_cmd: str, timeout_seconds: int = 20) -> str:
    """Run a short command on the EC2 box via SSM and return stdout."""
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})
    r = subprocess.run(
        ["aws", "ssm", "send-command"]
        + _profile_args()
        + ["--instance-ids", INSTANCE_ID,
           "--document-name", "AWS-RunShellScript",
           "--timeout-seconds", str(timeout_seconds),
           "--parameters", params,
           "--query", "Command.CommandId",
           "--output", "text"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"  (SSM send-command failed: {r.stderr.strip()})", file=sys.stderr, flush=True)
        return ""
    cmd_id = r.stdout.strip()
    status = ""
    for _ in range(timeout_seconds // 3 + 2):
        time.sleep(3)
        status = subprocess.run(
            ["aws", "ssm", "get-command-invocation"]
            + _profile_args()
            + ["--command-id", cmd_id,
               "--instance-id", INSTANCE_ID,
               "--query", "Status", "--output", "text"],
            capture_output=True, text=True,
        ).stdout.strip()
        if status not in ("Pending", "InProgress", "Delayed"):
            break
    out = subprocess.run(
        ["aws", "ssm", "get-command-invocation"]
        + _profile_args()
        + ["--command-id", cmd_id,
           "--instance-id", INSTANCE_ID,
           "--query", "[StandardOutputContent, StandardErrorContent]", "--output", "json"],
        capture_output=True, text=True,
    )
    try:
        stdout, stderr = json.loads(out.stdout)
    except (ValueError, TypeError):
        stdout, stderr = "", out.stderr
    if status != "Success":
        print(f"  (SSM command {cmd_id} status '{status or 'unknown'}'"
              f"{': ' + stderr.strip()[-500:] if stderr and stderr.strip() else ''})",
              file=sys.stderr, flush=True)
    return (stdout or "").strip()


CONFIRM_ATTEMPTS = 6  # each waits up to ~36s for EC2 to answer
CONFIRM_SPACING = 30  # min seconds between the starts of unanswered attempts


def confirm_ingest_running(hub: str, log_path: str) -> None:
    """Keep asking EC2 whether ingest.sh is running until it answers yes or no.

    An empty reply means the check itself didn't finish (SSM slow, box busy),
    not that the ingest failed, so ask again. Exit if it isn't running, or if
    EC2 never answers: a launch we can't confirm must not look like a success.
    """
    check_cmd = (
        f"if pgrep -u ec2-user -f 'scripts/ingest.sh {hub}( |$)' >/dev/null; "
        f"then echo INGEST_RUNNING; "
        f"else echo INGEST_NOT_RUNNING; tail -20 {log_path} 2>&1; fi"
    )
    for attempt in range(1, CONFIRM_ATTEMPTS + 1):
        print(f"  Checking ingest.sh is running (attempt {attempt}/{CONFIRM_ATTEMPTS}) …", flush=True)
        started = time.monotonic()
        check = ssm_run(check_cmd, timeout_seconds=30)
        if "INGEST_NOT_RUNNING" in check:
            sys.exit(f"ingest.sh is not running for {hub} after launch. Log tail ({log_path}):\n{check}")
        if "INGEST_RUNNING" in check:
            print("  Confirmed: ingest.sh is running.")
            return
        if attempt < CONFIRM_ATTEMPTS:
            print("  No answer from EC2 yet; asking again.", flush=True)
            # A send-command error returns at once; don't burn every attempt in seconds.
            time.sleep(max(0.0, CONFIRM_SPACING - (time.monotonic() - started)))
    sys.exit(
        f"Couldn't confirm ingest.sh is running for {hub}: EC2 didn't answer {CONFIRM_ATTEMPTS} checks.\n"
        f"It may still be running. Check {log_path} on EC2 before relaunching, "
        f"or you may start a second run."
    )


def aws_s3_ls(s3_path: str) -> str:
    try:
        r = subprocess.run(
            ["aws", "s3", "ls", s3_path] + _profile_args(),
            capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    return r.stdout if r.returncode == 0 else ""


# ── i3.conf helpers ───────────────────────────────────────────────────────────
def get_harvest_type(hub: str) -> str:
    """Return harvest.type for hub, reading from EC2 conf via SSM.

    Falls back to the local i3.conf (via helper_scripts/hub_conf.py) if
    the SSM grep returns empty — e.g. because the box's conf repo is stale or
    the SSM call fails quietly.
    """
    try:
        out = ssm_run(f"grep '^{hub}\\.harvest\\.type' {CONF_PATH} 2>/dev/null || echo ''")
        m = re.search(r'=\s*"?([^"\s]+)"?', out)
        if m:
            return m.group(1)
    except Exception:
        pass

    # SSM returned nothing — fall back to local conf.
    _, harvest_type = lookup_hub_in_conf(hub)
    if harvest_type:
        print(f"  (harvest.type read from local conf — EC2 conf may be stale)")
    return harvest_type or ""


# ── S3 delivery helpers ───────────────────────────────────────────────────────
def _sortable_date(entry: str) -> str:
    """Return a YYYYMMDD string for sorting delivery entry names newest-first.

    Handles three formats found in hub buckets:
      YYYY-MM-DD  (ISO, with dashes)   → strip dashes
      YYYYMMDD    (8 digits, year first) → keep as-is
      MMDDYYYY    (8 digits, month first, e.g. Georgia) → reorder to YYYYMMDD
    """
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", entry)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    # Require a plausible 4-digit year (19xx or 20xx) in either the YYYYMMDD
    # or MMDDYYYY position to avoid treating arbitrary 8-digit numbers as dates.
    m = re.search(r"((?:19|20)\d{6}|\d{4}(?:19|20)\d{2})", entry)
    if m:
        s = m.group(1)
        # If the first 4 digits are a plausible year, it's YYYYMMDD; else MMDDYYYY.
        if 1900 <= int(s[:4]) <= 2099:
            return s
        return s[4:] + s[:4]  # MMDDYYYY → YYYYMMDD
    return entry


def list_deliveries(bucket: str) -> list[str]:
    """Return all top-level entries (folders and files) in the bucket, sorted newest-first.

    Handles both subdirectory deliveries (PRE entries) and flat file deliveries
    (e.g. s3://dpla-hub-fl/SSDN-2026-05-24.jsonl dropped directly in the bucket).
    """
    out = aws_s3_ls(f"s3://{bucket}/")
    dated = []    # entries that contain a YYYY-MM-DD date — sorted newest-first

    for line in out.splitlines():
        # Subdirectory: "                           PRE some-folder/"
        m = re.search(r"PRE\s+(.+?)/\s*$", line)
        if m:
            entry = m.group(1) + "/"
        else:
            # File: "2026-05-24 12:34:56      123456 SSDN-2026-05-24.jsonl"
            m = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d+\s+(\S+)\s*$", line)
            if not m:
                continue
            entry = m.group(1)

        # Only keep entries that look like a date delivery — either ISO format
        # (YYYY-MM-DD) or a plain 8-digit date string (MMDDYYYY / YYYYMMDD).
        # Non-dated entries like "archive/" are skipped.
        if re.search(r"\d{4}-\d{2}-\d{2}|(?:19|20)\d{6}|\d{4}(?:19|20)\d{2}", entry):
            dated.append(entry)

    return sorted(dated, key=_sortable_date, reverse=True)


# ── file-hub pre-flight ───────────────────────────────────────────────────────
def file_hub_preflight(hub: str, bucket: str, non_interactive: bool = False) -> None:
    """Show S3 deliveries and the one the harvest will read; prompt to confirm.

    Exits if the bucket has no dated delivery (the harvest would fail).
    In non-interactive mode the confirmation prompt is skipped.
    """
    print(f"\nChecking s3://{bucket}/ for deliveries …")
    deliveries = list_deliveries(bucket)
    if not deliveries:
        print(f"  No delivery folders found in s3://{bucket}/")
        print("  Check the bucket name or whether a delivery has arrived yet.")
        sys.exit(1)

    print(f"  Available deliveries:")
    for d in deliveries[:5]:
        print(f"    {d}")
    if len(deliveries) > 5:
        print(f"    … and {len(deliveries) - 5} older")

    # The harvest reads the newest dated delivery in the bucket itself
    # (resolve_file_delivery in scripts/common.sh); i3.conf only names the bucket.
    latest = f"s3://{bucket}/{deliveries[0]}"
    print(f"""
The harvest will read the newest delivery:
    {latest}
No i3.conf edit needed. If that delivery is from an earlier month and was
already published, the harvest fails rather than re-ingesting it.
""")
    if non_interactive:
        print("Non-interactive: skipping confirmation prompt.")
        return
    try:
        ans = input("Launch ingest? [y/n]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        sys.exit("\nAborted.")
    if ans not in ("y", "yes"):
        sys.exit("Aborted.")


# ── main ──────────────────────────────────────────────────────────────────────
def prune_preview(hub: str) -> bool:
    """Show what prune-hub-data.sh would delete for this hub and ask.

    Returns True if the operator says yes, so the launch adds --prune and
    ingest.sh does the sync-and-delete at the start of the run (a backlog sync
    can take far longer than an SSM call). Interactive launches only.
    """
    print(f"\nChecking old {hub} runs on the box (dry run, nothing is deleted) …")
    out = ssm_run(
        f"bash {SCRIPTS_DIR}/prune-hub-data.sh {shlex.quote(hub)} --dry-run 2>&1",
        timeout_seconds=120,
    )
    if not out:
        print("  Warning: couldn't run the prune dry run — launching without a prune.")
        return False
    print("\n".join(f"  {line}" for line in out.splitlines()))
    if "SKIP:" in out:
        print("  Not pruning while a step for this hub is running.")
        return False
    if "WOULD DELETE" not in out and "WOULD SYNC + DELETE" not in out:
        if "No such file" in out:
            print("  Warning: prune-hub-data.sh isn't on the box yet (run preflight) — launching without a prune.")
        else:
            print("  Nothing to prune.")
        return False
    try:
        ans = input(
            "\n  Delete the runs above at the start of the ingest "
            "(syncing to S3 first where marked)? [y/N] "
        ).strip().lower()
    except EOFError:
        ans = ""
    return ans in ("y", "yes")


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch a DPLA hub ingest via SSM.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. bpl, ohio). Prompts if omitted.")
    parser.add_argument("--bucket", help="S3 bucket for file hubs (default: dpla-hub-<hub>).")
    parser.add_argument(
        "--resume-from",
        choices=VALID_RESUME_STEPS,
        help="Resume from a specific step instead of running the full pipeline.",
    )
    parser.add_argument(
        "--harvest-only",
        action="store_true",
        help="Only run the harvest step (skip mapping/enrichment/jsonl/S3 sync).",
    )
    parser.add_argument(
        "--mapping-only",
        action="store_true",
        help="Only run the mapping step (skip harvest, enrichment, jsonl, S3 sync).",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="Don't send the partner summary email at the end (passes --skip-email to "
             "ingest.sh; the EC2 checkout must include it).",
    )
    parser.add_argument(
        "--no-prune", action="store_true",
        help="Don't check for or delete old runs of this hub on the box before launching.",
    )
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="Non-interactive mode for CI/GitHub Actions: skip all prompts, "
             "fail fast on missing hub or ambiguous harvest type.",
    )
    args = parser.parse_args()

    # Hub: required in non-interactive; prompt in interactive.
    if args.non_interactive and not args.hub:
        sys.exit("[CI] --hub is required in non-interactive mode.")
    hub = (args.hub or input("Hub: ")).strip().lower()
    if not HUB_RE.match(hub):
        sys.exit(f"Invalid hub name: {hub!r}")

    # Check harvest type and run file-hub pre-flight if needed.
    print(f"Checking harvest type for {hub} …")
    harvest_type = get_harvest_type(hub)
    print(f"  harvest.type = {harvest_type or '(not found)'}")

    if not harvest_type:
        if args.non_interactive:
            sys.exit(
                f"[CI] Could not determine harvest.type for '{hub}'. "
                "Check i3.conf and retry."
            )
        try:
            ans = input(
                f"  Could not determine harvest.type for '{hub}'. "
                "If this is a file-export hub, Ctrl-C now and check i3.conf. "
                "Continue anyway? [y/N] "
            ).strip().lower()
        except EOFError:
            ans = ""
        if ans not in ("y", "yes"):
            sys.exit("Aborted.")

    if harvest_type == "file":
        conf_endpoint, _ = lookup_hub_in_conf(hub)
        endpoint_is_local = bool(conf_endpoint) and not conf_endpoint.startswith("s3://")
        if endpoint_is_local:
            print(f"  Endpoint is a local path ({conf_endpoint}) — skipping S3 preflight.")
        else:
            if args.bucket:
                bucket = args.bucket
            else:
                s3_match = re.match(r"s3://([^/]+)", conf_endpoint or "")
                bucket = s3_match.group(1) if s3_match else f"dpla-hub-{hub}"
            file_hub_preflight(hub, bucket, non_interactive=args.non_interactive)

    # Launch.
    flags = [args.harvest_only, args.mapping_only, bool(args.resume_from)]
    if sum(flags) > 1:
        sys.exit("--harvest-only, --mapping-only, and --resume-from are mutually exclusive.")
    if args.harvest_only:
        extra = " --harvest-only"
    elif args.mapping_only:
        extra = " --mapping-only"
    elif args.resume_from:
        extra = f" --resume-from {args.resume_from}"
    else:
        extra = ""
    if args.skip_email:
        extra += " --skip-email"
    # Old-run prune: interactive only, since it deletes from the box and needs
    # a yes. CI launches (--non-interactive) never prune.
    if not args.non_interactive and not args.no_prune and prune_preview(hub):
        extra += " --prune"
    gha_actor = os.environ.get("GHA_ACTOR", "")
    # `env` is required: nohup execs its first argument, so a bare VAR=val
    # prefix is treated as the command name and the launch silently fails.
    env_prefix = f"env GHA_ACTOR={shlex.quote(gha_actor)} " if gha_actor else ""
    invocation = f"{env_prefix}bash {SCRIPTS_DIR}/ingest.sh {hub}{extra}"
    inner = (
        'sudo -u ec2-user bash -lc "'
        f"nohup {invocation} > /home/ec2-user/data/{hub}-ingest.log 2>&1 </dev/null &"
        '"'
    )
    params = json.dumps({"commands": [inner]})

    result = subprocess.run(
        ["aws", "ssm", "send-command"]
        + _profile_args()
        + ["--instance-ids", INSTANCE_ID,
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

    # Poll briefly to confirm EC2 received and started the command.
    # The nohup job backgrounds immediately so the invocation reaches Success fast.
    print(f"  Confirming delivery (SSM command: {cmdid}) …", flush=True)
    for _ in range(10):
        time.sleep(3)
        r = subprocess.run(
            ["aws", "ssm", "get-command-invocation"]
            + _profile_args()
            + ["--command-id", cmdid,
               "--instance-id", INSTANCE_ID,
               "--query", "[Status, StandardOutputContent, StandardErrorContent]",
               "--output", "text"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            continue
        parts = r.stdout.strip().split("\t", 2)
        status = parts[0] if parts else ""
        if status in ("Success", "Failed", "TimedOut", "Cancelled"):
            if status != "Success":
                sys.exit(f"SSM launch command ended with status '{status}':\n{parts[2] if len(parts) > 2 else ''}")
            break
    else:
        print("  Warning: could not confirm SSM delivery within 30s — ingest may still have launched.")

    # SSM Success only means nohup was backgrounded, not that ingest.sh is
    # running — a bad invocation dies instantly and the launch still "passes".
    # Confirm the process exists; on failure show the log so CI goes red.
    log_path = f"/home/ec2-user/data/{hub}-ingest.log"
    time.sleep(5)
    confirm_ingest_running(hub, log_path)

    print(f"\nLaunched: {hub}{extra}")
    print(f"  Log on EC2:     /home/ec2-user/data/{hub}-ingest.log")
    print()
    print("Watch #tech-alerts for milestone messages.")


if __name__ == "__main__":
    main()