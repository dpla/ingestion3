#!/usr/bin/env python3
"""Tests for ingest_pipeline_python/helper_scripts/resolve_file_delivery.py.

Stubs the S3 listing and manifest reads, so it needs no AWS access.
Run: python3 scripts/tests/test-resolve-file-delivery.py
"""

import contextlib
import importlib.util
import io
import os
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
HELPERS = HERE.parent.parent / "ingest_pipeline_python" / "helper_scripts"
sys.path.insert(0, str(HELPERS))

spec = importlib.util.spec_from_file_location(
    "resolve_file_delivery", HELPERS / "resolve_file_delivery.py")
rfd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rfd)
fd = rfd.fd

NOW = "202611"


def conf(endpoint, hub="ohio"):
    return (f'{hub}.provider = "Ohio"\n'
            f'{hub}.harvest.type = "file"\n'
            + (f'{hub}.harvest.endpoint = "{endpoint}"\n' if endpoint is not None else ""))


class FakeS3:
    """deliveries: (dirs, files) for the hub bucket, or an Exception to raise.
    harvests: {harvest dir name: manifest text or None (no _MANIFEST)}."""

    def __init__(self, deliveries=((), ()), harvests=None, ls_error=""):
        self.deliveries = deliveries
        self.harvests = harvests or {}
        self.ls_error = ls_error

    def list_s3_root(self, bucket):
        if isinstance(self.deliveries, Exception):
            raise self.deliveries
        dirs, files = self.deliveries
        return list(dirs), list(files)

    def s3(self, args):
        ok = types.SimpleNamespace
        if args[0] == "ls":
            if self.ls_error:
                return ok(returncode=1, stdout="", stderr=self.ls_error)
            if not self.harvests:
                return ok(returncode=1, stdout="", stderr="")
            out = "".join(f"                           PRE {n}/\n" for n in self.harvests)
            return ok(returncode=0, stdout=out, stderr="")
        if args[0] == "cp":
            name = args[1].split("/harvest/")[1].split("/")[0]
            text = self.harvests.get(name)
            if text is None:
                return ok(returncode=1, stdout="", stderr="not found")
            return ok(returncode=0, stdout=text, stderr="")
        raise AssertionError(args)


class ResolveFileDeliveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.conf_path = os.path.join(self.tmp, "i3.conf")
        self.out_conf = os.path.join(self.tmp, "out.conf")

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_resolver(self, endpoint, s3, env=None, hub="ohio"):
        Path(self.conf_path).write_text(conf(endpoint, hub))
        out, err = io.StringIO(), io.StringIO()
        environ = {k: v for k, v in os.environ.items() if k != "FILE_DELIVERY"}
        environ.update(env or {})
        with mock.patch.object(fd, "list_s3_root", s3.list_s3_root), \
                mock.patch.object(rfd, "_s3", s3.s3), \
                mock.patch.dict(os.environ, environ, clear=True), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = rfd.main(["--hub", hub, "--conf", self.conf_path,
                             "--out-conf", self.out_conf, "--now", NOW])
        kv = dict(line.split("=", 1) for line in out.getvalue().splitlines() if "=" in line)
        return code, kv

    def out_endpoint(self):
        text = Path(self.out_conf).read_text()
        return [l for l in text.splitlines() if ".harvest.endpoint" in l][0]

    # ── current month ────────────────────────────────────────────────────────
    def test_current_month_delivery_is_harvested(self):
        s3 = FakeS3((["2026-11-02/", "2026-09-16/"], []))
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual(code, 0)
        self.assertEqual(kv["RESULT"], "ok")
        self.assertEqual(kv["DELIVERY"], "s3://dpla-hub-ohio/2026-11-02/")
        self.assertEqual(kv["CURRENT_MONTH"], "yes")
        self.assertIn('"s3://dpla-hub-ohio/2026-11-02/"', self.out_endpoint())

    def test_current_month_rerun_is_allowed_even_if_harvested(self):
        s3 = FakeS3((["2026-11-02/"], []), {
            "20261105_101010-ohio-OriginalRecord.avro":
                "Endpoint: s3://dpla-hub-ohio/2026-11-02/\nRecord count: 5"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"]), (0, "ok"))

    def test_dated_legacy_endpoint_only_uses_its_bucket(self):
        s3 = FakeS3((["2026-11-02/", "2026-09-16/"], []))
        code, kv = self.run_resolver("s3://dpla-hub-ohio/2026-09-16/", s3)
        self.assertEqual(kv["DELIVERY"], "s3://dpla-hub-ohio/2026-11-02/")

    # ── earlier month ────────────────────────────────────────────────────────
    def test_old_delivery_already_harvested_per_manifest_fails(self):
        s3 = FakeS3((["2026-09-16/"], []), {
            "20260920_101010-ohio-OriginalRecord.avro":
                "Endpoint: s3://dpla-hub-ohio/2026-09-16/\nRecord count: 5"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual(code, rfd.EXIT_HARVESTED)
        self.assertEqual(kv["RESULT"], "harvested")
        self.assertEqual(kv["LAST_HARVEST"], "20260920_101010-ohio-OriginalRecord.avro")
        self.assertFalse(os.path.exists(self.out_conf))

    def test_old_delivery_not_yet_harvested_per_manifest_continues(self):
        s3 = FakeS3((["2026-10-23/"], []), {
            "20260920_101010-ohio-OriginalRecord.avro":
                "Endpoint: s3://dpla-hub-ohio/2026-09-16/\nRecord count: 5"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"], kv["CURRENT_MONTH"]), (0, "ok", "no"))

    def test_no_endpoint_in_manifest_harvest_after_delivery_counts_as_harvested(self):
        s3 = FakeS3((["2026-09-16/"], []), {
            "20260920_101010-ohio-OriginalRecord.avro": "Record count: 5"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"]), (rfd.EXIT_HARVESTED, "harvested"))

    def test_no_endpoint_in_manifest_harvest_before_delivery_continues(self):
        s3 = FakeS3((["2026-10-23/"], []), {
            "20260920_101010-ohio-OriginalRecord.avro": "Record count: 5"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"]), (0, "ok"))

    def test_old_delivery_with_no_harvest_in_s3_continues(self):
        s3 = FakeS3((["2026-10-23/"], []))
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"]), (0, "ok"))

    def test_newest_harvest_without_manifest_is_skipped(self):
        s3 = FakeS3((["2026-09-16/"], []), {
            "20261001_000000-ohio-OriginalRecord.avro": None,
            "20260920_101010-ohio-OriginalRecord.avro":
                "Endpoint: s3://dpla-hub-ohio/2026-09-16/"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual(kv["LAST_HARVEST"], "20260920_101010-ohio-OriginalRecord.avro")

    def test_master_listing_error_fails_instead_of_assuming_not_harvested(self):
        s3 = FakeS3((["2026-09-16/"], []), ls_error="An error occurred (AccessDenied)")
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual(code, rfd.EXIT_USAGE)
        self.assertNotIn("RESULT", kv)
        self.assertFalse(os.path.exists(self.out_conf))

    def test_force_delivery_skips_the_check(self):
        s3 = FakeS3((["2026-09-16/"], []), {
            "20260920_101010-ohio-OriginalRecord.avro":
                "Endpoint: s3://dpla-hub-ohio/2026-09-16/"})
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3,
                                     env={"FILE_DELIVERY": "s3://dpla-hub-ohio/2026-09-16/"})
        self.assertEqual((code, kv["RESULT"]), (0, "ok"))
        self.assertIn('"s3://dpla-hub-ohio/2026-09-16/"', self.out_endpoint())

    # ── nothing to harvest ───────────────────────────────────────────────────
    def test_empty_bucket_fails(self):
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", FakeS3(([], [])))
        self.assertEqual((code, kv["RESULT"]), (rfd.EXIT_NONE, "none"))
        self.assertIn("no dated delivery", kv["REASON"])

    def test_unlistable_bucket_fails_with_one_line_reason(self):
        s3 = FakeS3(RuntimeError("aws s3 ls failed:\nAccessDenied\nmore"))
        code, kv = self.run_resolver("s3://dpla-hub-ohio/", s3)
        self.assertEqual((code, kv["RESULT"]), (rfd.EXIT_NONE, "none"))
        self.assertIn("AccessDenied", kv["REASON"])

    def test_missing_endpoint_fails(self):
        code, kv = self.run_resolver(None, FakeS3())
        self.assertEqual((code, kv["RESULT"]), (rfd.EXIT_NONE, "none"))
        self.assertIn("harvest.endpoint is not set", kv["REASON"])

    def test_local_endpoint_is_skipped(self):
        code, kv = self.run_resolver("/mnt/data/ct/originalRecords/", FakeS3(), hub="ct")
        self.assertEqual((code, kv["RESULT"]), (0, "skip"))
        self.assertFalse(os.path.exists(self.out_conf))

    # ── delivery selection ───────────────────────────────────────────────────
    def test_newest_compares_folders_and_files_by_name_date(self):
        s3 = FakeS3((["20240529/"], ["SSDN-2026-11-03.jsonl", "SSDN-2026-05-24.jsonl"]))
        with mock.patch.object(fd, "list_s3_root", s3.list_s3_root):
            self.assertEqual(fd.newest_delivery("b"), ("SSDN-2026-11-03.jsonl", "20261103"))

    def test_mmddyyyy_folder_names(self):
        s3 = FakeS3((["09212026/", "11012026/"], []))
        with mock.patch.object(fd, "list_s3_root", s3.list_s3_root):
            self.assertEqual(fd.newest_delivery("b"), ("11012026/", "20261101"))


if __name__ == "__main__":
    unittest.main()
