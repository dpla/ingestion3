#!/usr/bin/env python3
"""
DPLA Ingest Onboarding

Verifies that everything is in place to start working on the DPLA ingest pipeline.
On first run, prompts for your local repo paths and saves them to dpla_config.env
(next to this script) so you never have to edit the other scripts directly.

  1. Create / update dpla_config.env
  2. Python version
  3. Required CLI tools (aws, curl)
  4. AWS credentials — default profile + NARA profile
  5. EC2 instance reachability via SSM
  6. Local repo paths (ingestion3, ingestion3-conf / i3.conf)
  7. DPLA API key (~/.dpla-secrets.env)
  8. Summary

Usage:
    python3 onboarding.py           # first run — prompts for config
    python3 onboarding.py --update  # re-prompt to change saved paths
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys

# ─────────────────────────────────────────────
# Shared config constants (same for all users)
# ─────────────────────────────────────────────
REGION       = "us-east-1"
NARA_PROFILE = "nara"
SECRETS_FILE = os.path.expanduser("~/.dpla-secrets.env")

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".env"))

# Defaults shown in prompts
DEFAULT_INGESTION3      = os.path.expanduser("~/Documents/Repos/ingestion3")
DEFAULT_INGESTION3_CONF = os.path.expanduser("~/Documents/Repos/ingestion3-conf")
DEFAULT_BATCH_REPO      = os.path.expanduser("~/Documents/Repos/batch-process-dpla-index")


# ---------- color ----------
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
DIM    = "\033[2m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
USE_COLOR = sys.stdout.isatty()


def c(color, text):
    return f"{color}{text}{RESET}" if USE_COLOR else text


def ok(msg):    print(f"  {c(GREEN,  '✓')} {msg}")
def warn(msg):  print(f"  {c(YELLOW, '⚠')} {msg}")
def fail(msg):  print(f"  {c(RED,    '✗')} {msg}")


def section(title):
    print()
    print(c(BOLD, title))
    print(c(DIM, "  " + "─" * 50))


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


# ---------- config file ----------

def load_config():
    """Load dpla_config.env into a dict."""
    config = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    config[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    return config


def save_config(config):
    """
    Update INGESTION3_REPO and INGESTION3_CONF_REPO in .env in-place.
    Preserves all other lines (Slack keys, JAVA_HOME, comments, etc.).
    """
    keys_to_write = set(config.keys())

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            lines = f.readlines()
    else:
        lines = []

    updated = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in keys_to_write:
                new_lines.append(f"{k}={config[k]}\n")
                updated.add(k)
                continue
        new_lines.append(line)

    # Append any keys that weren't already in the file
    for k in sorted(keys_to_write - updated):
        new_lines.append(f"{k}={config[k]}\n")

    with open(CONFIG_FILE, "w") as f:
        f.writelines(new_lines)


def prompt_config(existing):
    """Interactively prompt for user-specific paths. Returns updated config dict."""
    section("Config Setup")
    print(c(DIM, f"  Saving to: {CONFIG_FILE}"))
    print()

    def ask(label, key, default):
        current = existing.get(key, default)
        try:
            val = input(f"  {label}\n  [{current}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit("\nAborted.")
        return val if val else current

    config = dict(existing)
    config["INGESTION3_REPO"]      = ask("Path to ingestion3 repo:",                "INGESTION3_REPO",      DEFAULT_INGESTION3)
    config["INGESTION3_CONF_REPO"] = ask("Path to ingestion3-conf repo:",           "INGESTION3_CONF_REPO", DEFAULT_INGESTION3_CONF)
    config["BATCH_REPO_DIR"]       = ask("Path to batch-process-dpla-index repo:",  "BATCH_REPO_DIR",       DEFAULT_BATCH_REPO)
    return config


# ---------- checks ----------

def check_python():
    section("Python")
    v = sys.version_info
    ver = f"{v.major}.{v.minor}.{v.micro}"
    if v < (3, 8):
        fail(f"Python {ver} — need 3.8+")
        return False
    ok(f"Python {ver}")
    return True


def check_cli_tools():
    section("CLI Tools")
    all_ok = True
    for tool in ["aws", "curl"]:
        path = shutil.which(tool)
        if path:
            ok(f"{tool:<6} found at {c(DIM, path)}")
        else:
            fail(f"{tool:<6} not found — install it and re-run")
            all_ok = False
    return all_ok


def check_aws_default():
    section("AWS — default profile")
    result = run(["aws", "sts", "get-caller-identity", "--region", REGION, "--output", "json"])
    if result.returncode != 0:
        err = result.stderr.strip().splitlines()[0] if result.stderr.strip() else "(no error output)"
        fail(f"aws sts get-caller-identity failed: {err}")
        warn("Run `aws configure` or set AWS_PROFILE / AWS_ACCESS_KEY_ID")
        return False
    try:
        identity = json.loads(result.stdout)
        ok(f"Account:  {identity.get('Account', '?')}")
    except json.JSONDecodeError:
        ok("Credentials valid")
    return True


def check_aws_nara():
    section(f"AWS — {NARA_PROFILE} profile")
    result = run(["aws", "sts", "get-caller-identity",
                  "--profile", NARA_PROFILE, "--region", REGION, "--output", "json"])
    if result.returncode != 0:
        fail(f"Profile '{NARA_PROFILE}' not configured or expired")
        warn(f"Add [{NARA_PROFILE}] to ~/.aws/credentials")
        warn("Keys are in 1Password: 'ingest@dp.la NARA keys'")
        return False
    try:
        identity = json.loads(result.stdout)
        ok(f"Account:  {identity.get('Account', '?')}")
    except json.JSONDecodeError:
        ok(f"Profile '{NARA_PROFILE}' valid")
    return True



def check_repos(config):
    section("Local Repos")
    all_ok = True
    changed = False

    for label, key, default in [
        ("ingestion3",               "INGESTION3_REPO",      DEFAULT_INGESTION3),
        ("ingestion3-conf",          "INGESTION3_CONF_REPO", DEFAULT_INGESTION3_CONF),
        ("batch-process-dpla-index", "BATCH_REPO_DIR",       DEFAULT_BATCH_REPO),
    ]:
        path = config.get(key, "")
        while not path or not os.path.isdir(path):
            if path:
                fail(f"{label:<20} not found at {c(DIM, path)}")
            else:
                fail(f"{label:<20} not set")
            try:
                val = input(f"  Enter path to {label} [{default}]: ").strip()
            except (EOFError, KeyboardInterrupt):
                sys.exit("\nAborted.")
            path = os.path.expanduser(val) if val else default
            config[key] = path
            changed = True

        is_git = os.path.isdir(os.path.join(path, ".git"))
        git_note = "" if is_git else f"  {c(YELLOW, '(no .git — not a git repo?)')}"
        ok(f"{label:<20} {c(DIM, path)}{git_note}")

    if changed:
        save_config(config)
        ok(f"Paths saved to {c(DIM, CONFIG_FILE)}")

    conf_repo = config.get("INGESTION3_CONF_REPO", "")
    conf_path = os.path.join(conf_repo, "i3.conf") if conf_repo else ""
    if conf_path and os.path.isfile(conf_path):
        ok(f"{'i3.conf':<20} {c(DIM, conf_path)}")
    else:
        fail(f"{'i3.conf':<20} not found at {c(DIM, conf_path)}")
        all_ok = False

    return all_ok


def check_api_key():
    section("DPLA API Key")
    if not os.path.exists(SECRETS_FILE):
        warn(f"{SECRETS_FILE} not found")
        warn("Create it with:  echo 'DPLA_API_KEY=your_key_here' > ~/.dpla-secrets.env")
        return False
    with open(SECRETS_FILE) as f:
        content = f.read()
    m = re.search(r"^DPLA_API_KEY\s*=\s*(.+)", content, re.MULTILINE)
    if not m:
        fail(f"DPLA_API_KEY not set in {SECRETS_FILE}")
        return False
    key = m.group(1).strip().strip('"').strip("'")
    if not key:
        fail("DPLA_API_KEY is empty")
        return False
    ok(f"DPLA_API_KEY set  ({c(DIM, SECRETS_FILE)})")
    return True


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="DPLA ingest onboarding check")
    parser.add_argument("--update", action="store_true",
                        help="Re-prompt for repo paths even if dpla_config.env already exists")
    args = parser.parse_args()

    print()
    print(c(DIM, "=" * 70))
    print("  DPLA Ingest — Onboarding")
    print(c(DIM, "=" * 70))

    # ── Config setup ─────────────────────────────────────────────────────────
    existing = load_config()
    needs_setup = args.update or not os.path.exists(CONFIG_FILE)

    if needs_setup:
        config = prompt_config(existing)
        save_config(config)
        print()
        ok(f"Config saved to {c(DIM, CONFIG_FILE)}")
    else:
        config = existing
        section("Config")
        ok(f"Loaded {c(DIM, CONFIG_FILE)}")
        print(c(DIM, "  Run with --update to change repo paths"))

    # ── Checks ───────────────────────────────────────────────────────────────
    results = {}
    results["python"]      = check_python()
    results["cli_tools"]   = check_cli_tools()
    results["aws_default"] = check_aws_default()
    results["aws_nara"]    = check_aws_nara()
    results["repos"]       = check_repos(config)
    results["api_key"]     = check_api_key()

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print(c(DIM, "=" * 70))
    passed = [k for k, v in results.items() if v]
    failed = [k for k, v in results.items() if not v]

    if not failed:
        print(f"  SUMMARY: {c(GREEN, 'All checks passed — you are good to go!')}")
    else:
        print(f"  SUMMARY: {c(YELLOW, f'{len(passed)}/{len(results)} checks passed')}")
        print()
        for k in failed:
            print(f"    {c(RED, '✗')} {k.replace('_', ' ')}")
        print()
        print(c(DIM, "  Fix the items above and re-run python3 onboarding.py"))

    print(c(DIM, "=" * 70))
    print()


if __name__ == "__main__":
    main()
