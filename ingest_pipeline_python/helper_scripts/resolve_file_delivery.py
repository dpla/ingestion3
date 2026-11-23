#!/usr/bin/env python3
"""
Pick the delivery a file hub's harvest should read, at harvest time.

For a hub with harvest.type = "file" and an s3:// endpoint in i3.conf, the
endpoint only has to name the hub's bucket (e.g. s3://dpla-hub-fl/). This
script finds the newest dated delivery in that bucket and writes a copy of
i3.conf whose endpoint points at it, for the harvest to use. i3.conf itself is
not changed. An endpoint that still names a dated delivery works too: only its
bucket is used.

Rules:
  - The newest delivery is from the current month: harvest it (reruns in the
    same month are fine).
  - It's from an earlier month: harvest it only if it hasn't been published
    already. "Published" means it's in the hub's newest JSONL snapshot in
    s3://<master>/<prefix>/jsonl/, the one the indexer reads (sparkindexer's
    MasterDataset takes the last snapshot in sort order). ingest.sh writes the
    delivery a snapshot was built from into it as _DELIVERY
    (write_delivery_marker in scripts/common.sh). Snapshots from before that,
    or from runs that don't write it (the orchestrator), have no _DELIVERY;
    for those, a snapshot made on or after the delivery's date counts as
    having published it.
  - No endpoint, the bucket can't be listed, or it has no dated delivery:
    nothing to harvest, so fail.

Harvests are not checked: ingest.sh only syncs JSONL to S3, so harvests stay
on the box and may be cleaned up.

FILE_DELIVERY=s3://bucket/path in the environment skips the search and the
already-published check, and harvests that delivery.

Called by resolve_file_delivery in scripts/common.sh (ingest.sh, harvest.sh).

Output (stdout, KEY=value lines for the shell):
  RESULT=skip        endpoint is not s3:// (local path): use i3.conf as-is
  RESULT=ok          DELIVERY, DELIVERY_DATE, CURRENT_MONTH=yes|no, CONF
  RESULT=published   DELIVERY, DELIVERY_DATE, LAST_SNAPSHOT
  RESULT=none        REASON
Human-readable detail goes to stderr.

Exit status: 0 for skip/ok, 3 for published, 2 for none, 1 for other errors
(bad arguments, unreadable i3.conf, or published snapshots can't be listed).
"""

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import file_deliveries as fd  # noqa: E402

# Only a profile the caller set. common.sh exports AWS_PROFILE from .env when
# it's set there and unsets it otherwise, so the EC2 instance role is used on
# the box (a named profile there makes every aws call fail).
fd.AWS_PROFILE = os.environ.get("AWS_PROFILE") or None

EXIT_OK, EXIT_USAGE, EXIT_NONE, EXIT_PUBLISHED = 0, 1, 2, 3


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def emit(**kv):
    # One line per key: the shell reads these with sed, so values (e.g. aws
    # error text in REASON) must not span lines.
    for k, v in kv.items():
        v = " ".join(str(v).split())
        print(f"{k}={v}", flush=True)


def conf_endpoint(conf_text, hub):
    """Return the hub's harvest.endpoint from i3.conf, or None."""
    text = re.sub(r"(?m)^\s*(#|//).*$", "", conf_text)
    m = re.search(
        rf"""^\s*{re.escape(hub)}\.harvest\.endpoint\s*[=:]\s*["']([^"']*)["']""",
        text, re.MULTILINE | re.IGNORECASE,
    )
    return m.group(1).strip() if m else None


def _s3(args):
    return subprocess.run(
        ["aws", "s3", *args, "--region", fd.REGION] + fd._profile_args(),
        capture_output=True, text=True,
    )


def last_published(master_bucket, prefix):
    """Return (snapshot name, recorded delivery or None) for the hub's newest
    JSONL snapshot in S3, the one the indexer reads, or (None, None) if the
    hub has none.

    Raises RuntimeError if the listing fails for another reason (e.g. access
    denied): without it we can't tell whether the delivery was published."""
    r = _s3(["ls", f"s3://{master_bucket}/{prefix}/jsonl/"])
    if r.returncode != 0:
        if r.stderr.strip():
            raise RuntimeError(
                f"can't list s3://{master_bucket}/{prefix}/jsonl/: {r.stderr.strip()}")
        # An empty or missing prefix exits non-zero with no error text: nothing published yet.
        return None, None
    names = sorted(m.group(1) for m in re.finditer(r"PRE\s+(\d{8}_\d{6}-[^/]+)/", r.stdout))
    if not names:
        return None, None
    newest = names[-1]
    key = f"s3://{master_bucket}/{prefix}/jsonl/{newest}/_DELIVERY"
    m = _s3(["cp", key, "-"])
    if m.returncode != 0:
        # Only a missing _DELIVERY means "not recorded". Anything else (access
        # denied, credentials, network) leaves us unable to tell, so fail.
        if not re.search(r"\(404\)|Not Found|does not exist|NoSuchKey", m.stderr or ""):
            raise RuntimeError(f"can't read {key}: {(m.stderr or '').strip() or 'exit ' + str(m.returncode)}")
        return newest, None
    recorded = m.stdout.strip().split()[0] if m.stdout.strip() else None
    return newest, recorded


def already_published(delivery_uri, delivery_date, snapshot_name, recorded):
    """True if the newest JSONL snapshot was built from this delivery."""
    if recorded:
        return recorded.rstrip("/") == delivery_uri.rstrip("/")
    # No _DELIVERY (older snapshot, or a run that doesn't write it): go by dates.
    return snapshot_name[:8] >= delivery_date


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--hub", required=True)
    p.add_argument("--conf", required=True, help="i3.conf to read")
    p.add_argument("--out-conf", required=True,
                   help="Where to write the i3.conf copy for the harvest")
    p.add_argument("--master-bucket", default="dpla-master-dataset")
    p.add_argument("--master-prefix", help="Hub's folder in the master bucket (default: --hub)")
    p.add_argument("--now", help="YYYYMM to treat as the current month (tests)")
    a = p.parse_args(argv)

    hub = a.hub.lower()
    prefix = a.master_prefix or hub
    this_month = a.now or datetime.now().strftime("%Y%m")

    try:
        conf_text = Path(a.conf).read_text()
    except OSError as e:
        log(f"Can't read i3.conf at {a.conf}: {e}")
        return EXIT_USAGE

    endpoint = conf_endpoint(conf_text, hub)
    if not endpoint:
        reason = f"{hub}.harvest.endpoint is not set in i3.conf"
        log(reason)
        emit(RESULT="none", REASON=reason)
        return EXIT_NONE
    if not endpoint.startswith("s3://"):
        emit(RESULT="skip")
        return EXIT_OK

    forced = os.environ.get("FILE_DELIVERY", "").strip()
    if forced:
        if not forced.startswith("s3://"):
            log(f"FILE_DELIVERY must be an s3:// path, got: {forced}")
            return EXIT_USAGE
        delivery = forced
        delivery_date = fd._sortable_date(forced.rstrip("/").split("/")[-1])
        log(f"FILE_DELIVERY set: harvesting {delivery} (no search, no already-published check)")
    else:
        bucket = re.match(r"s3://([^/]+)", endpoint).group(1)
        try:
            entry, delivery_date = fd.newest_delivery(bucket)
        except RuntimeError as e:
            reason = f"can't list s3://{bucket}/: {e}"
            log(reason)
            emit(RESULT="none", REASON=reason)
            return EXIT_NONE
        if not entry:
            reason = f"no dated delivery in s3://{bucket}/"
            log(reason)
            emit(RESULT="none", REASON=reason)
            return EXIT_NONE
        delivery = f"s3://{bucket}/{entry}"
        log(f"Newest delivery: {delivery} ({delivery_date})")

        if delivery_date[:6] != this_month:
            try:
                snapshot, recorded = last_published(a.master_bucket, prefix)
            except RuntimeError as e:
                log(f"Can't check whether {delivery} was already published: {e}")
                return EXIT_USAGE
            if snapshot and already_published(delivery, delivery_date, snapshot, recorded):
                how = "its _DELIVERY" if recorded else "date (no _DELIVERY recorded)"
                log(f"Already published in {snapshot} (by {how}); no new delivery since.")
                emit(RESULT="published", DELIVERY=delivery,
                     DELIVERY_DATE=delivery_date, LAST_SNAPSHOT=snapshot)
                return EXIT_PUBLISHED

    Path(a.out_conf).write_text(fd.update_endpoint_in_conf(conf_text, hub, delivery))
    emit(RESULT="ok", DELIVERY=delivery, DELIVERY_DATE=delivery_date,
         CURRENT_MONTH="yes" if delivery_date[:6] == this_month else "no",
         CONF=a.out_conf)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
