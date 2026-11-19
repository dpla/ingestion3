#!/usr/bin/env python3
"""Tests for scripts/delete/delete-from-jsonl.py (issue #851).

Uses a fake S3 client, so it needs no AWS access and no boto3.
Run: python3 scripts/tests/test-delete-from-jsonl.py
"""

import contextlib
import gzip
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "delete", "delete-from-jsonl.py")

# The script exits at import if boto3 is missing; give it a stand-in.
sys.modules.setdefault("boto3", types.ModuleType("boto3"))
spec = importlib.util.spec_from_file_location("delete_from_jsonl", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class FakeS3:
    """download_file copies from a local 'bucket' dir; uploads are recorded."""

    def __init__(self, root):
        self.root = root
        self.uploads = []

    def download_file(self, bucket, key, dest):
        shutil.copy(os.path.join(self.root, key), dest)

    def upload_file(self, src, bucket, key):
        with open(src, "rb") as f:
            self.uploads.append((key, f.read()))

    def get_paginator(self, name):
        root = self.root

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(os.listdir(root))]}

        return P()


class FakeS3Export(FakeS3):
    """Serves the files in root as one hub's newest JSONL export, for main()."""

    PREFIX = "hub/jsonl/20260101_000000-hub-MAP3_1.IndexRecord.jsonl/"

    def list_objects_v2(self, Bucket, Prefix, Delimiter):
        return {"CommonPrefixes": [{"Prefix": self.PREFIX}]}

    def download_file(self, bucket, key, dest):
        shutil.copy(os.path.join(self.root, key.split("/")[-1]), dest)

    def get_paginator(self, name):
        root, prefix = self.root, self.PREFIX

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": prefix + k} for k in sorted(os.listdir(root))]}

        return P()


def rec(i):
    return json.dumps({"_source": {"id": i}})


class DeleteFromJsonlTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.s3 = FakeS3(self.root)

    def tearDown(self):
        shutil.rmtree(self.root)

    def write(self, name, lines, gz=False):
        path = os.path.join(self.root, name)
        opener = gzip.open if gz else open
        with opener(path, "wt", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return name

    def test_blank_lines_are_not_counted_as_removed(self):
        key = self.write("part-0.jsonl", [rec("a"), "", rec("b"), "", ""])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"zzz"})
        self.assertEqual(r["removed"], 0)
        self.assertEqual(r["before"], 2)
        self.assertEqual(self.s3.uploads, [], "no match must not re-upload the file")

    def test_matched_ids_reported(self):
        key = self.write("part-0.jsonl.gz", [rec("a"), rec("b"), rec("c")], gz=True)
        r = mod.filter_jsonl_file(self.s3, "b", key, {"a", "c", "x"})
        self.assertEqual(r["removed"], 2)
        self.assertEqual(sorted(r["matched"]), ["a", "c"])

    def test_dry_run_scans_but_does_not_upload(self):
        key = self.write("part-0.jsonl", [rec("a"), rec("b")])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"a"}, dry_run=True)
        self.assertEqual(r["removed"], 1)
        self.assertEqual(r["matched"], ["a"])
        self.assertFalse(r["uploaded"])
        self.assertEqual(self.s3.uploads, [])

    def test_real_run_uploads_filtered_file(self):
        key = self.write("part-0.jsonl", [rec("a"), rec("b")])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"a"})
        self.assertTrue(r["uploaded"])
        self.assertEqual(self.s3.uploads[0][1].decode().strip(), rec("b"))

    def test_malformed_lines_kept_and_counted(self):
        key = self.write("part-0.jsonl", [rec("a"), "{not json", rec("b")])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"a"})
        self.assertEqual(r["parse_errors"], 1)
        self.assertEqual(r["removed"], 1)
        kept = self.s3.uploads[0][1].decode().splitlines()
        self.assertEqual(kept, ["{not json", rec("b")])

    def test_clean_file_has_no_parse_errors(self):
        key = self.write("part-0.jsonl", [rec("a"), "", rec("b")])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"zzz"})
        self.assertEqual(r["parse_errors"], 0)

    def test_whitespace_only_lines_are_skipped(self):
        key = self.write("part-0.jsonl", [rec("a"), "   ", "\t", rec("b")])
        r = mod.filter_jsonl_file(self.s3, "b", key, {"zzz"})
        self.assertEqual(r["before"], 2)
        self.assertEqual(r["parse_errors"], 0)

    def run_main(self, argv):
        """Run main() against FakeS3Export; return (exit code, stdout, fake s3)."""
        s3 = FakeS3Export(self.root)
        session = types.SimpleNamespace(client=lambda name: s3)
        out = io.StringIO()
        code = 0
        with mock.patch.object(mod.boto3, "Session", lambda profile_name=None: session, create=True), \
                mock.patch.object(sys, "argv", ["delete-from-jsonl.py"] + argv), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            try:
                mod.main()
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), s3

    def test_main_dry_run_malformed_line_marks_zero_incomplete_and_exits_1(self):
        self.write("part-0.jsonl", [rec("a"), "{not json", rec("b")])
        code, out, s3 = self.run_main(["--hub", "hub", "--dry-run", "a", "gone"])
        self.assertEqual(code, 1)
        self.assertIn("  a: 1", out)
        self.assertIn("  gone: 0", out)
        self.assertIn("counts are incomplete", out)
        self.assertIn("Unparseable lines: 1", out)
        self.assertEqual(s3.uploads, [])

    def test_main_dry_run_clean_scan_with_no_match_exits_0(self):
        self.write("part-0.jsonl", [rec("a"), "", rec("b")])
        code, out, s3 = self.run_main(["--hub", "hub", "--dry-run", "gone"])
        self.assertIn(code, (0, None))
        self.assertIn("  gone: 0", out)
        self.assertNotIn("incomplete", out)
        self.assertEqual(s3.uploads, [])

    def test_lists_json_files_too(self):
        for n in ("a.gz", "b.jsonl", "c.json", "_SUCCESS", "_MANIFEST"):
            self.write(n, [rec("x")])
        files = mod.list_jsonl_files(self.s3, "b", "")
        self.assertEqual(sorted(files), ["a.gz", "b.jsonl", "c.json"])


if __name__ == "__main__":
    unittest.main()
