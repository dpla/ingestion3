#!/usr/bin/env python3
"""Publish a hub ingest that the >5% safety check blocked, after you've reviewed the drop.

When ingest.sh sees the new JSON-L count drop more than 5% against the newest
snapshot in S3, it stops: the new snapshot stays on EC2, unsynced, and the hub
isn't emailed. If the drop is expected (the hub removed records, a validation
rule now rejects some, the previous snapshot was a test harvest, ...), this
script finishes the job:

  1. Shows the blocked snapshot's counts next to the previous S3 snapshot,
     plus the matching harvest and mapping counts and the hub contacts
  2. Asks, then syncs ONLY that snapshot to s3://dpla-master-dataset/
     (not the whole jsonl/ folder, which can hold other unsynced runs)
  3. Confirms it's now the newest snapshot in S3 (the one the indexer uses)
  4. Asks, then sends the partner summary email
  5. Posts what was done to Slack

Usage (from ingest_pipeline_python/):
    python3 helper_scripts/skip_safety_check.py <hub>
    python3 helper_scripts/skip_safety_check.py <hub> --snapshot 20261109_161818-dartmouth-MAP3_1.IndexRecord.jsonl

By default it uses the newest JSON-L snapshot on EC2. Community Webs has this
built in (special_cases/community-webs/launch_cw.py --send --force); NARA has
its own pipeline, so neither is handled here.
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
def _repo_env_path():
    return os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
    )


def _load_dotenv():
    cfg = {}
    env_file = _repo_env_path()
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
INSTANCE_ID = os.environ.get("INGEST_INSTANCE_ID") or _env.get("INGEST_INSTANCE_ID", "")
AWS_PROFILE = (
    os.environ.get("AWS_PROFILE")
    or _env.get("AWS_PROFILE")
    or ("dpla" if os.path.exists(_repo_env_path()) else None)
)
REGION = "us-east-1"
REPO = "/home/ec2-user/ingestion3"
DATA = "/home/ec2-user/data"
HUB_RE = re.compile(r"^[a-z0-9_-]+$")
SNAP_RE = re.compile(r"^\d{8}_\d{6}-[A-Za-z0-9_-]+-MAP3_1\.IndexRecord\.jsonl$")
NOT_HANDLED = {
    "community-webs": "use special_cases/community-webs/launch_cw.py --send --force",
    "nara": "NARA has its own pipeline (special_cases/nara/)",
}


# ---------- AWS / SSM ----------
def aws(args):
    profile = ["--profile", AWS_PROFILE] if AWS_PROFILE else []
    r = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{r.stderr.strip()}")
    return r.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    """Run shell_cmd on the ingest EC2 as ec2-user (login shell); return stdout."""
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    cmd_id = aws([
        "ssm", "send-command", "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", str(timeout_seconds),
        "--parameters", json.dumps({"commands": [wrapped]}),
        "--region", REGION, "--query", "Command.CommandId", "--output", "text",
    ])
    deadline = time.time() + timeout_seconds + 60
    while True:
        time.sleep(poll_seconds)
        try:
            status = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
                          "--instance-id", INSTANCE_ID, "--region", REGION,
                          "--query", "Status", "--output", "text"])
        except RuntimeError:
            status = "Pending"  # invocation not registered yet
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM command {cmd_id} still running after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--region", REGION,
               "--query", "StandardOutputContent", "--output", "text"])
    if status != "Success":
        err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
                   "--instance-id", INSTANCE_ID, "--region", REGION,
                   "--query", "StandardErrorContent", "--output", "text"])
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ask(msg):
    try:
        return input(f"\n  {msg} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


# ---------- gather ----------
GATHER = r'''
cd {repo} && source scripts/common.sh >/dev/null 2>&1
HUB={hub}
SNAP={snap}
JDIR={data}/$HUB/jsonl
newest_local=$(ls -1d "$JDIR"/*/ 2>/dev/null | xargs -n1 basename 2>/dev/null | grep -E '^[0-9]{{8}}_[0-9]{{6}}-' | sort | tail -1)
[ -z "$SNAP" ] && SNAP=$newest_local
echo "SNAP=$SNAP"
echo "NEWEST_LOCAL=$newest_local"
[ -n "$SNAP" ] || exit 0
echo "SUCCESS=$([ -f "$JDIR/$SNAP/_SUCCESS" ] && echo yes || echo no)"
echo "COUNT=$(read_manifest_count "$JDIR/$SNAP/_MANIFEST")"
TS=${{SNAP%%-*}}
latest_before() {{ ls -1d {data}/$HUB/$1/*/ 2>/dev/null | xargs -n1 basename 2>/dev/null | grep -E '^[0-9]{{8}}_[0-9]{{6}}-' | sort | awk -v ts="$TS" '$0 < ts' | tail -1; }}
MAP=$(latest_before mapping); HARV=$(latest_before harvest)
echo "MAP_DIR={data}/$HUB/mapping/$MAP"
echo "MAP=$MAP"
echo "HARVEST=$HARV"
echo "HARVEST_COUNT=$(read_manifest_count {data}/$HUB/harvest/$HARV/_MANIFEST)"
grep -E '^(Attempted|Successful|Failed)' {data}/$HUB/mapping/$MAP/_SUMMARY 2>/dev/null | sed 's/\.\.*/: /; s/^/MAPSUM=/'
PREFIX=$(resolve_s3_prefix "$HUB")
echo "S3_PREFIX=$PREFIX"
S3_SNAP=$(aws s3 ls "s3://dpla-master-dataset/$PREFIX/jsonl/" 2>/dev/null | awk '{{print $NF}}' | sed 's|/||g' | grep -E '^[0-9]{{8}}_[0-9]{{6}}-' | sort | tail -1)
echo "S3_SNAP=$S3_SNAP"
[ -n "$S3_SNAP" ] && echo "S3_COUNT=$(aws s3 cp "s3://dpla-master-dataset/$PREFIX/jsonl/$S3_SNAP/_MANIFEST" - 2>/dev/null | read_manifest_count)"
echo "IN_S3=$(aws s3 ls "s3://dpla-master-dataset/$PREFIX/jsonl/$SNAP/_SUCCESS" >/dev/null 2>&1 && echo yes || echo no)"
echo "EMAIL=$(get_hub_email "$HUB")"
LOG={data}/$HUB-ingest.log
if grep -q "$SNAP" "$LOG" 2>/dev/null; then
  grep -q "Uniqueness check FAILED" "$LOG" && echo "BLOCKED_BY=uniqueness"
  grep -q "Safety check FAILED" "$LOG" && echo "BLOCKED_BY=safety"
fi
exit 0
'''


def gather(hub, snapshot):
    out = ssm_run(GATHER.format(repo=REPO, data=DATA, hub=shlex.quote(hub),
                                snap=shlex.quote(snapshot or "")), timeout_seconds=120)
    info = {"MAPSUM": [], "BLOCKED_BY": []}
    for line in out.splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k in ("MAPSUM", "BLOCKED_BY"):
            info[k].append(v.strip())
        else:
            info[k] = v.strip()
    return info


def pct(new, prev):
    return f"{(new - prev) / prev * 100:+.1f}%" if prev else "n/a"


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Publish an ingest the >5% safety check blocked, after review.")
    parser.add_argument("hub", help="Hub short name (as used with ingest.sh)")
    parser.add_argument("--snapshot", help="JSON-L snapshot dir on EC2 (default: newest)")
    args = parser.parse_args()

    hub = args.hub.strip().lower()
    if not HUB_RE.match(hub):
        sys.exit(f"Invalid hub name: {hub!r}")
    if hub in NOT_HANDLED:
        sys.exit(f"{hub} isn't handled here: {NOT_HANDLED[hub]}.")
    if args.snapshot and not SNAP_RE.match(args.snapshot):
        sys.exit(f"Not a JSON-L snapshot name: {args.snapshot!r}")
    if not INSTANCE_ID:
        sys.exit("INGEST_INSTANCE_ID isn't set (env or the repo's .env).")

    print(f"\nChecking {hub} on EC2 …")
    info = gather(hub, args.snapshot)
    snap = info.get("SNAP", "")
    if not snap:
        sys.exit(f"No JSON-L snapshots found on EC2 under {DATA}/{hub}/jsonl/.")

    new = int(info.get("COUNT") or 0)
    prev = int(info.get("S3_COUNT") or 0)
    s3_snap = info.get("S3_SNAP", "")

    print(f"\nSAFETY CHECK OVERRIDE — {hub}")
    print(f"  Snapshot on EC2:   {snap}  ({new:,} records)")
    print(f"  Newest in S3:      {s3_snap or '(none)'}  ({prev:,} records)")
    print(f"  Change:            {new - prev:+,}  ({pct(new, prev)})")
    print(f"  Harvest:           {info.get('HARVEST') or '(not found)'}  "
          f"({int(info.get('HARVEST_COUNT') or 0):,} records)")
    print(f"  Mapping:           {info.get('MAP') or '(not found)'}")
    for line in info["MAPSUM"]:
        print(f"                     {line}")
    print(f"  Hub contacts:      {info.get('EMAIL') or '(none in i3.conf)'}")

    # Refuse anything that isn't a complete, blocked, newer snapshot
    if "uniqueness" in info["BLOCKED_BY"]:
        sys.exit("\nThis run was blocked by the ID uniqueness check, not the record-count check "
                 "(likely a harvester pagination bug). Fix and re-ingest; don't publish it.")
    if info.get("SUCCESS") != "yes":
        sys.exit(f"\n{snap} has no _SUCCESS marker; the JSON-L export didn't finish.")
    if new == 0:
        sys.exit(f"\n{snap} has 0 records.")
    if info.get("IN_S3") == "yes":
        sys.exit(f"\n{snap} is already in S3; nothing to publish. "
                 "To email the hub, use scripts/communication/send-ingest-email.sh.")
    if s3_snap and snap <= s3_snap:
        sys.exit(f"\n{snap} is older than the newest S3 snapshot ({s3_snap}); the indexer "
                 "wouldn't use it, so it isn't synced.")
    if info.get("NEWEST_LOCAL") and info["NEWEST_LOCAL"] != snap:
        print(f"\n  Note: a newer snapshot exists on EC2 ({info['NEWEST_LOCAL']}).")
    if "safety" not in info["BLOCKED_BY"]:
        print(f"\n  Note: the ingest log doesn't show the safety check blocking {snap}. "
              "Check that this is the run you mean.")

    if not ask(f"Sync {snap} to s3://dpla-master-dataset/{info.get('S3_PREFIX', hub)}/jsonl/?"):
        sys.exit("Nothing synced.")

    print("\n  Syncing …")
    try:
        out = ssm_run(
            f"cd {REPO} && source .env && bash scripts/s3-sync.sh {shlex.quote(hub)} "
            f"jsonl/{shlex.quote(snap)} 2>&1 | tail -3; exit ${{PIPESTATUS[0]}}",
            timeout_seconds=3600, poll_seconds=15,
        )
    except RuntimeError as e:
        sys.exit(f"\nS3 sync failed; nothing emailed.\n{e}")
    print("    " + out.replace("\n", "\n    "))

    after = gather(hub, snap)
    if after.get("IN_S3") != "yes" or after.get("S3_SNAP") != snap:
        sys.exit(f"\nSync finished, but S3's newest snapshot is {after.get('S3_SNAP')!r}, "
                 f"not {snap}. Check s3://dpla-master-dataset/{after.get('S3_PREFIX', hub)}/jsonl/.")
    print(f"  ✓ {snap} is now the newest snapshot in S3.")

    emailed = False
    if info.get("EMAIL") and info.get("MAP"):
        if ask(f"Email the summary to {info['EMAIL']}?"):
            try:
                out = ssm_run(
                    f"cd {REPO} && source .env && bash scripts/communication/send-ingest-email.sh "
                    f"--yes {shlex.quote(hub)} {shlex.quote(info['MAP_DIR'])} 2>&1 | tail -5; "
                    f"exit ${{PIPESTATUS[0]}}",
                    timeout_seconds=300,
                )
                print("    " + out.replace("\n", "\n    "))
                emailed = True
            except RuntimeError as e:
                print(f"  Email failed (the S3 sync stands). Retry with send-ingest-email.sh.\n{e}")
    else:
        print("\n  No contacts or mapping summary found; skipping the email.")

    note = "partner email sent" if emailed else "no partner email"
    msg = (f":white_check_mark: *{hub} safety check overridden* — synced to S3 after review\\n"
           f"New: {new:,} | Prev: {prev:,} | Change: {pct(new, prev)}\\n"
           f"Snapshot: `{snap}` ({note})")
    try:
        ssm_run(f"cd {REPO} && source scripts/common.sh && slack_notify {shlex.quote(msg)}",
                timeout_seconds=60)
    except RuntimeError as e:
        print(f"  Warning: Slack post failed ({e}).")

    print(f"\nDone: {hub} {snap} is in S3 ({note}). It goes live at the next index rebuild.")


if __name__ == "__main__":
    main()
