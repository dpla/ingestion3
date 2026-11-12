#!/usr/bin/env python3
"""Take DPLA items down from dp.la and from the S3 master dataset.

Every takedown needs two deletions: from the hub's newest JSON-L in
s3://dpla-master-dataset/ (so the items don't come back at the next index
rebuild) and from the live Elasticsearch index (so they leave dp.la now).
This script does both, with the checks from the dpla-takedown runbook:

  1. Looks up every ID in the DPLA API (title, institution, hub, isShownAt)
  2. Checks each item's source: ContentDM items through the ContentDM API,
     others by HTTP status. Items still live at ContentDM are always left out;
     items it can't confirm are left out unless you pass --include-unverified
  3. Shows the manifest and asks
  4. Removes the IDs from the hub's newest S3 JSON-L (scripts/delete/delete-from-jsonl.py)
  5. Counts the IDs in the live index (must match exactly), asks, deletes them
     in batches of 100, and checks each batch's result
  6. Confirms through the API that each ID is gone and that the total and hub
     counts dropped by exactly that many

There's no exclusion list: if the hub still has the items, they come back at its
next ingest. Ask the hub to remove them at the source.

Usage (from ingest_pipeline_python/):
    python3 helper_scripts/takedown.py <id> [<id> ...]
    python3 helper_scripts/takedown.py -f ids.txt                 # one ID per line
    python3 helper_scripts/takedown.py -f ids.txt --dry-run       # stop before deleting anything
    python3 helper_scripts/takedown.py -f ids.txt --include-unverified
    python3 helper_scripts/takedown.py -f ids.txt --s3-hub orbis-cascade   # if the S3 folder differs from the API's hub name

All IDs in one run must belong to one hub. DPLA_API_KEY comes from the
environment, the repo's .env, or ~/.dpla-secrets.env.
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


# ---------- config ----------
REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
ENV_FILE = os.path.join(REPO_ROOT, ".env")
SECRETS_FILE = os.path.expanduser("~/.dpla-secrets.env")


def _read_env(path):
    cfg = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("export "):
                    line = line[len("export "):]
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    return cfg


_env = _read_env(ENV_FILE)
if _env.get("AWS_SHARED_CREDENTIALS_FILE"):
    os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", _env["AWS_SHARED_CREDENTIALS_FILE"])
AWS_PROFILE = (
    os.environ.get("AWS_PROFILE") or _env.get("AWS_PROFILE")
    or ("dpla" if os.path.exists(ENV_FILE) else None)
)
API_KEY = (os.environ.get("DPLA_API_KEY") or _env.get("DPLA_API_KEY")
           or _read_env(SECRETS_FILE).get("DPLA_API_KEY", ""))
REGION = "us-east-1"
ES_INSTANCE = "i-00bbdfe0a6ff6cf78"  # search-prod1
API = "https://api.dp.la/v2/items"
BUCKET = "dpla-master-dataset"
ID_RE = re.compile(r"^[0-9a-f]{32}$")
INDEX_RE = re.compile(r"^dpla-all-\d{8}-\d{6}$")
BATCH = 100
CDM_RE = re.compile(r"^(https?)://([^/]+)/(?:cdm/ref/collection|digital/collection)/([^/]+)/id/(\d+)")


# ---------- helpers ----------
def ask(msg):
    try:
        return input(f"\n  {msg} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def aws(args):
    profile = ["--profile", AWS_PROFILE] if AWS_PROFILE else []
    r = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{r.stderr.strip()}")
    return r.stdout.strip()


def es(shell_cmd, timeout_seconds=300):
    """Run shell_cmd on search-prod1 via SSM; $IP is set to the node's private IP."""
    script = "IP=$(hostname -I | cut -d' ' -f1)\n" + shell_cmd
    encoded = base64.b64encode(script.encode()).decode("ascii")
    cmd_id = aws([
        "ssm", "send-command", "--instance-ids", ES_INSTANCE,
        "--document-name", "AWS-RunShellScript", "--timeout-seconds", str(timeout_seconds),
        "--parameters", json.dumps({"commands": [f"echo {encoded} | base64 -d | bash"]}),
        "--region", REGION, "--query", "Command.CommandId", "--output", "text",
    ])
    deadline = time.time() + timeout_seconds + 60
    while True:
        time.sleep(4)
        try:
            status = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
                          "--instance-id", ES_INSTANCE, "--region", REGION,
                          "--query", "Status", "--output", "text"])
        except RuntimeError:
            status = "Pending"
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM command {cmd_id} still running after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", ES_INSTANCE, "--region", REGION,
               "--query", "StandardOutputContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}: {out[-500:]}")
    return out


def es_query(index, action, ids):
    """POST a terms-on-id query to <index>/<action> and return the parsed response.
    Raises RuntimeError on an Elasticsearch error or an unreadable response."""
    query = base64.b64encode(json.dumps({"query": {"terms": {"id": ids}}}).encode()).decode()
    # A file per request, so overlapping runs can't read each other's query
    out = es(f"Q=$(mktemp /tmp/dpla-takedown-q.XXXXXX) && echo {query} | base64 -d > \"$Q\" && "
             f"curl -s -XPOST \"http://$IP:9200/{index}/{action}\" "
             f"-H 'Content-Type: application/json' -d @\"$Q\"; rm -f \"$Q\"")
    try:
        res = json.loads(out)
    except ValueError:
        raise RuntimeError(f"unreadable Elasticsearch response: {out[:300]}")
    if not isinstance(res, dict) or "error" in res:
        raise RuntimeError(f"Elasticsearch error: {json.dumps(res)[:300]}")
    return res


def live_index():
    """The single dpla-all-* index dpla_alias points to (exits if it's anything else)."""
    indices = list(json.loads(es("curl -s http://$IP:9200/_alias/dpla_alias")))
    if len(indices) != 1 or not INDEX_RE.match(indices[0]):
        sys.exit(f"dpla_alias points to {indices}; expected exactly one dpla-all-YYYYMMDD-HHMMSS index.")
    return indices[0]


def api_get(url):
    """GET a DPLA API URL; returns (http_status, parsed_json_or_None).
    Network errors and transient HTTP errors (429, 5xx) are retried; after that
    the last status is returned, or (None, None) if the API couldn't be reached."""
    sep = "&" if "?" in url else "?"
    result = (None, None)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(f"{url}{sep}api_key={API_KEY}", timeout=30) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            try:
                body = json.load(e)
            except ValueError:
                body = None
            if e.code != 429 and e.code < 500:
                return e.code, body
            result = (e.code, body)
        except (urllib.error.URLError, OSError, ValueError):
            result = (None, None)
        if attempt < 2:
            time.sleep(5)
    return result


def api_count(params=None):
    q = urllib.parse.urlencode({**(params or {}), "page_size": 0})
    status, body = api_get(f"{API}?{q}")
    if status != 200 or body is None:
        raise RuntimeError(f"API count failed (HTTP {status})")
    return body["count"]


def first(v):
    if isinstance(v, list):
        v = v[0] if v else ""
    if isinstance(v, dict):
        v = v.get("name") or v.get("@id") or ""
    return str(v)


def http_status(url):
    req = urllib.request.Request(url, headers={"User-Agent": "dpla-takedown-check"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


def source_status(is_shown_at):
    """Return (status, note): status is 'removed', 'live' or 'unverified'."""
    if not is_shown_at:
        return "unverified", "no isShownAt"
    m = CDM_RE.match(is_shown_at)
    if m:
        # ContentDM pages always return 200; its REST API tells the truth
        scheme, host, alias, item = m.groups()
        coll = http_status(f"{scheme}://{host}/digital/api/collections/{alias}")
        if coll == 404:
            return "removed", "ContentDM collection gone (404)"
        it = http_status(f"{scheme}://{host}/digital/api/collections/{alias}/items/{item}")
        if it == 404:
            return "removed", "ContentDM item gone (404)"
        if it == 200:
            return "live", "ContentDM item still published (200)"
        return "unverified", f"ContentDM API returned {it}"
    try:
        host = (urllib.parse.urlparse(is_shown_at).hostname or "").lower()
    except ValueError:
        return "unverified", "isShownAt isn't a valid URL"
    if host in ("ark.cdlib.org", "calisphere.org") or host.endswith(".calisphere.org"):
        return "unverified", "Calisphere blocks scripted checks; check in a browser"
    code = http_status(is_shown_at)
    if code in (404, 410):
        return "removed", f"source returns {code}"
    return "unverified", f"source returns {code or 'no response'}"


def gone_from_api(dpla_id):
    """True only when the API confirms the ID is gone; unreachable counts as not confirmed."""
    status, body = api_get(f"{API}/{dpla_id}")
    return status == 404 or (status == 200 and body is not None and body.get("count") == 0)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Take DPLA items down from dp.la and S3.")
    parser.add_argument("ids", nargs="*", help="DPLA IDs (32 hex characters)")
    parser.add_argument("-f", "--file", help="File with one DPLA ID per line")
    parser.add_argument("--s3-hub", help="S3 folder under dpla-master-dataset/ (default: the API's hub name)")
    parser.add_argument("--include-unverified", action="store_true",
                        help="Also take down items whose source removal couldn't be confirmed "
                             "(only after checking them yourself)")
    parser.add_argument("--dry-run", action="store_true", help="Stop before deleting anything")
    args = parser.parse_args()

    ids = list(args.ids)
    if args.file:
        with open(os.path.expanduser(args.file)) as f:
            ids += [line.strip() for line in f if line.strip() and not line.startswith("#")]
    ids = list(dict.fromkeys(i.lower() for i in ids))
    bad = [i for i in ids if not ID_RE.match(i)]
    if bad:
        sys.exit(f"Not DPLA IDs: {', '.join(bad[:5])}")
    if not ids:
        sys.exit("No IDs given.")
    if not API_KEY:
        sys.exit("DPLA_API_KEY isn't set (env, the repo's .env, or ~/.dpla-secrets.env).")

    # 1. Look up each ID
    print(f"\nLooking up {len(ids)} ID(s) in the DPLA API …")
    try:
        total_before = api_count()
    except RuntimeError as e:
        sys.exit(f"Couldn't read the DPLA API total ({e}). Nothing was changed; try again.")
    items, missing = [], []
    for i in ids:
        status, body = api_get(f"{API}/{i}")
        docs = (body or {}).get("docs") or [] if status == 200 else []
        # Only a 404, or a 200 with no record, means the ID really isn't there
        if status == 404 or (status == 200 and not docs):
            missing.append(i)
            continue
        if status != 200:
            sys.exit(f"Couldn't look up {i} in the DPLA API ({'no response' if status is None else f'HTTP {status}'}). "
                     "Nothing was changed; try again.")
        d = docs[0]
        prov = d.get("provider") or {}
        items.append({
            "id": i,
            "hub": str(prov.get("@id", "")).rstrip("/").split("/")[-1],
            "hub_name": prov.get("name", ""),
            "institution": first(d.get("dataProvider")),
            "title": first((d.get("sourceResource") or {}).get("title")),
            "is_shown_at": first(d.get("isShownAt")),
        })
    if missing:
        print(f"  Not in the API (already gone or wrong ID), skipped: {len(missing)}")
        for i in missing[:10]:
            print(f"    {i}")
    if not items:
        sys.exit("Nothing to take down.")
    hubs = sorted({it["hub"] for it in items})
    if len(hubs) > 1:
        sys.exit(f"These IDs span hubs ({', '.join(hubs)}). Run one hub at a time.")
    hub, hub_name = items[0]["hub"], items[0]["hub_name"]

    s3_hub = args.s3_hub or hub
    try:
        aws(["s3", "ls", f"s3://{BUCKET}/{s3_hub}/jsonl/"])
    except RuntimeError:
        sys.exit(f"No s3://{BUCKET}/{s3_hub}/jsonl/. If the S3 folder has another "
                 f"name, pass --s3-hub (see: aws s3 ls s3://dpla-master-dataset/ --profile dpla).")

    # 2. Source checks
    print(f"Checking {len(items)} source link(s) …")
    for it in items:
        it["source"], it["note"] = source_status(it["is_shown_at"])
    take = [it for it in items if it["source"] == "removed"
            or (it["source"] == "unverified" and args.include_unverified)]
    skipped = [it for it in items if it not in take]

    # 3. Manifest
    try:
        hub_before = api_count({"provider.name": f'"{hub_name}"'})
    except RuntimeError as e:
        sys.exit(f"Couldn't read the {hub_name} count from the API ({e}). Nothing was changed; try again.")
    print(f"\nTAKEDOWN MANIFEST — {hub_name} ({hub}; S3 folder {s3_hub})")
    print(f"  {'ID':32}  {'Source':10}  Title / institution")
    for it in take:
        print(f"  {it['id']}  {it['source']:10}  {it['title'][:50]} / {it['institution'][:30]}")
    if skipped:
        print("\n  Left out (source not confirmed removed):")
        for it in skipped:
            print(f"  {it['id']}  {it['source']:10}  {it['note']}  {it['is_shown_at']}")
        if any(it["source"] == "unverified" for it in skipped):
            print("  After checking those by hand, rerun with --include-unverified to include them.")
    if not take:
        sys.exit("\nNothing to take down.")
    n = len(take)
    print(f"\n  {n} item(s) will be removed from s3://dpla-master-dataset/{s3_hub}/jsonl/ "
          f"(newest snapshot) and from the live index.")
    print("  They come back at the hub's next ingest unless the hub removes them at the source.")

    # Find the live index and check every ID is in it before touching anything
    try:
        index = live_index()
    except (RuntimeError, ValueError) as e:
        sys.exit(f"Couldn't read dpla_alias from Elasticsearch ({e}). Nothing was changed.")
    batches = [[it["id"] for it in take[k:k + BATCH]] for k in range(0, n, BATCH)]
    try:
        found = sum(es_query(index, "_count", b)["count"] for b in batches)
    except (RuntimeError, KeyError) as e:
        sys.exit(f"Couldn't count the IDs in {index} ({e}). Nothing was changed.")
    print(f"\n  Live index {index}: {found} of {n} found")
    if found != n:
        sys.exit("  Counts don't match; stopping before any deletion. Investigate the missing IDs.")

    if args.dry_run:
        sys.exit("\nDry run: nothing deleted.")
    if not ask(f"Take down these {n} item(s)?"):
        sys.exit("Aborted. Nothing deleted.")

    # 4. S3 first, so a failure leaves the live site untouched
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("\n".join(it["id"] for it in take) + "\n")
        idfile = f.name
    print("\nRemoving from S3 …")
    cmd = [sys.executable, os.path.join(REPO_ROOT, "scripts", "delete", "delete-from-jsonl.py"),
           "--hub", s3_hub, "--bucket", BUCKET, "-y", "-f", idfile]
    if AWS_PROFILE:
        cmd += ["--profile", AWS_PROFILE]
    # A DRY_RUN inherited from the shell would make the S3 step a silent preview
    child_env = {k: v for k, v in os.environ.items() if k not in ("DRY_RUN", "S3_BUCKET")}
    r = subprocess.run(cmd, capture_output=True, text=True, env=child_env)
    os.unlink(idfile)
    print("    " + r.stdout.strip().replace("\n", "\n    "))
    if r.returncode != 0:
        sys.exit(f"S3 delete failed; the live index was not touched.\n{r.stderr.strip()}")
    m = re.search(r"Total records removed:\s*(\d+)", r.stdout)
    if not m or "[DRY RUN]" in r.stdout:
        sys.exit("Couldn't confirm the S3 removal (no 'Total records removed' in its output). "
                 "The live index was not touched.")
    removed = int(m.group(1))
    # Fewer can be legitimate (an ID missing from the newest export but still live)
    if removed != n and not ask(f"S3 removed {removed}, expected {n}. Continue to the live index anyway?"):
        sys.exit("Stopped after S3. The live index was not touched.")

    # 5. Live index, batch by batch, re-counting each batch right before deleting it
    print(f"\nRemoving from {index} …")
    deleted = 0
    for b in batches:
        try:
            # An alias swap mid-run would leave these deletes on an index that's no longer live
            if live_index() != index:
                sys.exit(f"dpla_alias no longer points to {index}; stopped after deleting {deleted}. "
                         "Rerun the takedown against the new index.")
            if es_query(index, "_count", b)["count"] != len(b):
                sys.exit(f"Batch count changed since the check; stopped after deleting {deleted}.")
            res = es_query(index, "_delete_by_query?conflicts=abort&refresh=true", b)
        except (RuntimeError, KeyError, ValueError) as e:
            sys.exit(f"Elasticsearch failed mid-takedown ({e}). Stopped after deleting {deleted} of {n} "
                     f"from {index}; S3 already has all {n} removed. Check which IDs remain before rerunning.")
        if res.get("deleted") != len(b) or res.get("failures") or res.get("version_conflicts"):
            sys.exit(f"Batch didn't delete cleanly (deleted {res.get('deleted')} of {len(b)}; "
                     f"failures {res.get('failures')}; conflicts {res.get('version_conflicts')}). "
                     f"Stopped after {deleted + (res.get('deleted') or 0)} deleted.")
        deleted += len(b)
        print(f"  ✓ {deleted}/{n}")

    # 6. Verify
    print("\nVerifying through the API …")
    time.sleep(30)
    still = [it["id"] for it in take if not gone_from_api(it["id"])]
    if still:
        print(f"  {len(still)} still showing; waiting 2 minutes for the API cache …")
        time.sleep(120)
        still = [i for i in still if not gone_from_api(i)]
    try:
        total_after = api_count()
        hub_after = api_count({"provider.name": f'"{hub_name}"'})
    except RuntimeError as e:
        print(f"  ✗ Couldn't read the API counts ({e}).")
        total_after, hub_after = total_before, hub_before  # forces the unverified verdict below
    t_delta, h_delta = total_before - total_after, hub_before - hub_after
    print(f"  Total: {total_before:,} → {total_after:,}  (−{t_delta})  {'✓' if t_delta == n else '✗'}")
    print(f"  {hub_name}: {hub_before:,} → {hub_after:,}  (−{h_delta})  {'✓' if h_delta == n else '✗'}")
    if still:
        print(f"  ✗ Still in the API: {', '.join(still)}")
    if t_delta > n:
        print("  ✗ MORE items left dp.la than were taken down. Investigate now.")

    reminder = "Reminder: ask the hub to remove them at the source, or they come back at its next ingest."
    if still or t_delta != n or h_delta != n:
        print(f"\nTAKEDOWN INCOMPLETE OR UNVERIFIED: {deleted} deleted from {index}, but the checks "
              "above don't all pass. Investigate before telling anyone it's done.")
        print(reminder)
        sys.exit(1)
    print(f"\nTAKEDOWN DONE: {n} item(s) removed from S3 ({s3_hub}) and {index}.")
    print(reminder)


if __name__ == "__main__":
    main()
