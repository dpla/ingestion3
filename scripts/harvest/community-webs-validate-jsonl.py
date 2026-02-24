#!/usr/bin/env python3
"""
Validate Community Webs JSONL output against expected harvest schema.

Ensures each record has required fields and expected structure before passing
to the harvest step. Catches DB export issues early.

Usage:
    python -m scripts.community-webs-validate-jsonl /path/to/community-webs.jsonl
    ./scripts/harvest/community-webs-validate-jsonl.py /path/to/file.jsonl

Exit: 0 if valid, 1 if invalid.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


# Required by harvester: CommunityWebsHarvester throws if id is missing
REQUIRED_FIELDS = {"id"}


def validate_record(record: dict, line_num: int) -> list[str]:
    """Validate a single record. Returns list of error messages."""
    errors = []

    if not isinstance(record, dict):
        errors.append(f"Line {line_num}: record must be a JSON object, got {type(record).__name__}")
        return errors

    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"Line {line_num}: missing required field '{field}'")
        elif record[field] is None or record[field] == "":
            errors.append(f"Line {line_num}: required field '{field}' is empty")

    return errors


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: community-webs-validate-jsonl.py <path-to.jsonl>", file=sys.stderr)
        return 1

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Error: File not found: {path}", file=sys.stderr)
        return 1

    all_errors: list[str] = []
    line_num = 0
    valid_count = 0
    deleted_count = 0

    with open(path, encoding="utf-8") as f:
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                all_errors.append(f"Line {line_num}: Invalid JSON: {e}")
                continue

            # Harvester filters "status":"deleted" - we allow but count
            if record.get("status") == "deleted":
                deleted_count += 1
                continue

            errors = validate_record(record, line_num)
            if errors:
                all_errors.extend(errors)
            else:
                valid_count += 1

    if all_errors:
        for err in all_errors[:20]:  # Limit output
            print(err, file=sys.stderr)
        if len(all_errors) > 20:
            print(f"... and {len(all_errors) - 20} more errors", file=sys.stderr)
        return 1

    print(f"Validated {valid_count} records" + (f" ({deleted_count} deleted skipped)" if deleted_count else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
