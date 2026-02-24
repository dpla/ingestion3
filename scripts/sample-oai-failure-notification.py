#!/usr/bin/env python3
"""Print sample Slack and email messages for an OAI harvest failure.

Usage:
    ./venv/bin/python scripts/sample-oai-failure-notification.py

This script builds a realistic OAI failure scenario (Indiana hub, bad XML in a
specific set) and prints the Slack and email messages that would be sent by the
notification system. Use it to verify message formatting without actually sending.
"""

SLACK_USER_ID = "U01234ABCD"  # placeholder for @dominic


def sample_slack_message():
    """Build the Slack message text for the sample failure."""
    return (
        ":x: *Harvest Failure: indiana*\n"
        f"<@{SLACK_USER_ID}> OAI harvest failure needs attention\n"
        "\n"
        "*OAI debug context*\n"
        "• Set: `problematic_set`\n"
        "• Resumption token: `abc123tokenXYZ`\n"
        "• Cursor: 500 / 1200\n"
        "• URL: https://dpla.library.in.gov/oai?verb=ListRecords"
        "&resumptionToken=abc123tokenXYZ\n"
        "• First ID: `oai:dpla.library.in.gov:PALNI_herb-22274`\n"
        "• Last ID: `oai:dpla.library.in.gov:PALNI_herb-22324`\n"
        "\n"
        "*Error*: SAXParseException: Invalid character in entity reference\n"
        "\n"
        "Review: `data/escalations/failures-TEST_oai_20260216.md`"
    )


def sample_email_body():
    """Build the email body for the sample failure."""
    return (
        "Subject: [DPLA Ingest] Harvest failure: indiana\n"
        "\n"
        "DPLA OAI Harvest Failure\n"
        "\n"
        "Hub: indiana\n"
        "Stage: harvest\n"
        "Run ID: TEST_oai_20260216\n"
        "\n"
        "Error: OAI harvest error at stage page_parse\n"
        "\n"
        "OAI debug context:\n"
        "- Set: problematic_set\n"
        "- Resumption token: abc123tokenXYZ\n"
        "- Cursor: 500 / 1200\n"
        "- URL: https://dpla.library.in.gov/oai?verb=ListRecords"
        "&resumptionToken=abc123tokenXYZ\n"
        "- First ID: oai:dpla.library.in.gov:PALNI_herb-22274\n"
        "- Last ID: oai:dpla.library.in.gov:PALNI_herb-22324\n"
        "\n"
        "Full error: SAXParseException: Invalid character in entity reference\n"
        "\n"
        "---\n"
        "If you have questions, contact tech@dp.la."
    )


def main():
    print("=" * 60)
    print("  SAMPLE SLACK MESSAGE")
    print("=" * 60)
    print(sample_slack_message())
    print()
    print("=" * 60)
    print("  SAMPLE EMAIL MESSAGE")
    print("=" * 60)
    print(sample_email_body())
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
