"""Tests for Community Webs JSONL schema validation.

Validates DB transformation output against expected harvest schema.
"""

import json
import tempfile
from pathlib import Path

import pytest

# Import validation logic from script (script is standalone; run as subprocess for integration)
SCRIPT_DIR = Path(__file__).resolve().parent.parent
VALIDATE_SCRIPT = SCRIPT_DIR / "community-webs-validate-jsonl.py"


def run_validate(jsonl_path: Path) -> tuple[int, str]:
    """Run validation script; return (exit_code, stderr+stdout)."""
    import subprocess

    result = subprocess.run(
        ["python3", str(VALIDATE_SCRIPT), str(jsonl_path)],
        capture_output=True,
        text=True,
    )
    return result.returncode, (result.stderr + result.stdout).strip()


class TestCommunityWebsValidation:
    """Test community-webs-validate-jsonl.py schema validation."""

    def test_valid_record_passes(self, tmp_path):
        """Record with required id passes validation."""
        jsonl = tmp_path / "test.jsonl"
        record = {
            "id": "archive-it::seed:2225815",
            "title": "Test Title",
            "data_provider": "Test Provider",
        }
        jsonl.write_text(json.dumps(record) + "\n")
        code, out = run_validate(jsonl)
        assert code == 0, out
        assert "Validated 1 records" in out

    def test_expected_schema_sample_passes(self, tmp_path):
        """Sample from src/test/resources/community-webs.json passes."""
        jsonl = tmp_path / "test.jsonl"
        record = {
            "collection": "Athens, Georgia Area COVID-19 Response",
            "coverage": "Athens, Georgia",
            "creator": "Athens Access to Justice - Western Circuit",
            "data_provider": "Athens Regional Library System",
            "date": "2020-05-21",
            "description": "Athens Access to Justice's site listing COVID-19 resources...",
            "id": "archive-it::seed:2225815",
            "image_mimetype": "image/png",
            "language": "English",
            "preview": "https://wayback.archive-it.org/13711/*/http://athensaccesstojustice.org/covid-19/",
            "relation": "",
            "rights_free_text": "https://rightsstatements.org/en/",
            "rights_statement": None,
            "subject": "Unemployment;Housing;Economic Assistance;COVID-19 Pandemic, 2020-",
            "title": "COVID-19 Resources – Athens Access to Justice COVID-19 Resources",
            "type": "Archived Website",
        }
        jsonl.write_text(json.dumps(record) + "\n")
        code, out = run_validate(jsonl)
        assert code == 0, out
        assert "Validated 1 records" in out

    def test_missing_id_fails(self, tmp_path):
        """Record without id fails validation."""
        jsonl = tmp_path / "test.jsonl"
        record = {"title": "Test", "data_provider": "Provider"}
        jsonl.write_text(json.dumps(record) + "\n")
        code, out = run_validate(jsonl)
        assert code == 1, out
        assert "missing required field" in out
        assert "id" in out

    def test_empty_id_fails(self, tmp_path):
        """Record with empty id fails validation."""
        jsonl = tmp_path / "test.jsonl"
        record = {"id": "", "title": "Test"}
        jsonl.write_text(json.dumps(record) + "\n")
        code, out = run_validate(jsonl)
        assert code == 1, out
        assert "required field" in out.lower()

    def test_null_id_fails(self, tmp_path):
        """Record with null id fails validation."""
        jsonl = tmp_path / "test.jsonl"
        record = {"id": None, "title": "Test"}
        jsonl.write_text(json.dumps(record) + "\n")
        code, out = run_validate(jsonl)
        assert code == 1, out
        assert "required field" in out.lower()

    def test_deleted_record_skipped(self, tmp_path):
        """Records with status=deleted are skipped (harvester behavior)."""
        jsonl = tmp_path / "test.jsonl"
        lines = [
            json.dumps({"id": "valid-1", "title": "Valid"}),
            json.dumps({"id": "deleted-1", "title": "Deleted", "status": "deleted"}),
            json.dumps({"id": "valid-2", "title": "Valid 2"}),
        ]
        jsonl.write_text("\n".join(lines) + "\n")
        code, out = run_validate(jsonl)
        assert code == 0, out
        assert "Validated 2 records" in out
        assert "1 deleted" in out or "deleted" in out.lower()

    def test_invalid_json_fails(self, tmp_path):
        """Invalid JSON line fails validation."""
        jsonl = tmp_path / "test.jsonl"
        jsonl.write_text('{"id": "ok"}\n{ invalid json }\n')
        code, out = run_validate(jsonl)
        assert code == 1, out
        assert "Invalid JSON" in out or "invalid" in out.lower()

    def test_empty_file_passes(self, tmp_path):
        """Empty file passes (no records to validate)."""
        jsonl = tmp_path / "test.jsonl"
        jsonl.write_text("")
        code, out = run_validate(jsonl)
        assert code == 0, out
