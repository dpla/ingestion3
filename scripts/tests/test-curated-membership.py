#!/usr/bin/env python3
"""Parser tests for curated_membership.py. Run by test-scripts.sh."""

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "curated_membership.py"
spec = importlib.util.spec_from_file_location("curated_membership", SCRIPT)
cm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cm)

ID_A = "a" * 32
ID_B = "b" * 32
ID_C = "c" * 32
ID_D = "d" * 32


def attachment(element_name, text):
    return {"item": {"element_texts": [{"element": {"name": element_name}, "text": text}]}}


def page_of(*attachments):
    return {"pages": [{"page_blocks": [{"attachments": list(attachments)}]}]}


def test_exhibition_parser():
    data = page_of(
        attachment("Has Version", ID_A),         # exact case
        attachment("HAS VERSION", ID_B),         # upper case
        attachment(" has version ", ID_C),       # case + whitespace
        attachment("Title", ID_D),               # other element: ignored
        attachment("Has Version", ID_A.upper()), # uppercase ID: recovered
        attachment("Has Version", f"{ID_B}?"),   # stray junk: recovered
        attachment("Has Version", "e" * 31),     # truncated: dropped
    )
    ids = cm.exhibition_item_ids(data, "test")
    assert ids == {ID_A, ID_B, ID_C}, ids


def test_null_tolerance():
    data = {"pages": [{"page_blocks": None}, {"page_blocks": [{"attachments": None}]}]}
    assert cm.exhibition_item_ids(data, "test") == set()


def main():
    test_exhibition_parser()
    test_null_tolerance()
    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
