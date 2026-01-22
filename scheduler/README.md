# DPLA Hub Ingestion Scheduler

This directory contains scripts and configuration for automating monthly hub ingestion scheduling based on the DPLA Hub Re-ingest Schedule.

## Overview

The scheduler system:
1. **Collects** hub metadata (contacts, schedules, feed info) from Atlassian Confluence dashboard pages
2. **Generates** monthly ingest command scripts based on schedule.json
3. **Creates** templated email notifications for hubs

## Files

- `schedule.json` - Hub configuration with contacts, schedules, and metadata
- `schedule.sh` - Generates monthly ingest commands
- `email-template.txt` - Email template with placeholders
- `email-template.sh` - Generates personalized emails from template
- `collect-hub-data.py` - Python script to collect hub data from Confluence pages
- `parse-schedules.py` - Updates schedule information in hub data
- `create-summary.py` - Creates human-readable summary of collected data

## Usage

### Generating Monthly Ingest Commands

To generate ingest commands for the current month:

```bash
./scheduler/schedule.sh
```

This will:
- Identify hubs scheduled for the current month
- Generate a script with all ingest commands: `ingest-commands-YYYY-Mon.sh`
- Display a summary of hubs to be ingested

Example output:
```
Monthly Ingest Schedule for Jan 2026
==================================================================

Found 9 hub(s) scheduled for Jan:
  - Harvard Library (harvard)
  - National Archives and Records Administration (nara)
  ...

Generated ingest commands: scheduler/ingest-commands-2026-Jan.sh
```

To execute the generated commands:
```bash
./scheduler/ingest-commands-2026-Jan.sh
```

### Generating Email Notifications

To generate email notifications for hubs scheduled this month:

```bash
./scheduler/email-template.sh
```

This creates personalized email files in `scheduler/emails/` directory, one per hub.

### Updating Hub Data

To refresh hub data from Confluence pages:

```bash
# Collect data from dashboard pages
python3 scheduler/collect-hub-data.py

# Update with schedule information
python3 scheduler/parse-schedules.py

# Create summary for review
python3 scheduler/create-summary.py
```

The summary will be written to `scheduler/hub-data-summary.txt` for review.

## Schedule.json Format

The `schedule.json` file contains hub configuration:

```json
{
  "hubs": [
    {
      "name": "Hub Name",
      "provider_code": "provider-name",
      "dashboard_url": "https://...",
      "contacts": ["email1@example.com", "email2@example.com"],
      "schedule": {
        "frequency": "quarterly",
        "months": [1, 4, 7, 10],
        "week": "last",
        "notes": "Wikimedia Partner"
      },
      "feed_type": "",
      "feed_url": "",
      "metadata_format": "",
      "notes": ""
    }
  ]
}
```

### Schedule Frequencies

- `quarterly` - 4 times per year
- `bi-monthly` - 6 times per year (every 2 months)
- `bi-annually` - 2 times per year
- `monthly` - 12 times per year
- `as-needed` - On demand

### Months

Months are specified as numbers (1-12):
- 1 = January
- 2 = February
- ...
- 12 = December

### Week Specification

- `last` - Run during the last week of the month
- `first` - Run during the first week of the month
- Empty string = No specific week requirement

## Email Template

The email template (`email-template.txt`) supports the following placeholders:

- `{{MONTH}}` - Current month name (e.g., "January")
- `{{YEAR}}` - Current year (e.g., "2026")
- `{{HUB_NAME}}` - Hub name
- `{{FREQUENCY}}` - Schedule frequency
- `{{MONTHS}}` - Comma-separated list of scheduled months
- `{{WEEK}}` - Week specification (if applicable)
- `{{NOTES}}` - Special notes
- `{{DASHBOARD_URL}}` - Link to hub dashboard

Conditional blocks:
- `{{#WEEK}}...{{/WEEK}}` - Only included if week is specified
- `{{#NOTES}}...{{/NOTES}}` - Only included if notes exist

## Dependencies

- `jq` - JSON processor (required for schedule.sh and email-template.sh)
  - Install: `brew install jq` (macOS) or `apt-get install jq` (Linux)
- `python3` - For data collection scripts
- `curl` - For fetching web pages

## Workflow

### Monthly Workflow

1. **Generate ingest commands** (beginning of month):
   ```bash
   ./scheduler/schedule.sh
   ```

2. **Generate email notifications**:
   ```bash
   ./scheduler/email-template.sh
   ```

3. **Review and send emails** (optional):
   - Review generated emails in `scheduler/emails/`
   - Send using your email client or service

4. **Execute ingests**:
   ```bash
   ./scheduler/ingest-commands-YYYY-Mon.sh
   ```

### Updating Hub Information

When hub information changes (new contacts, schedule updates):

1. **Re-collect data**:
   ```bash
   python3 scheduler/collect-hub-data.py
   python3 scheduler/parse-schedules.py
   ```

2. **Review summary**:
   ```bash
   python3 scheduler/create-summary.py
   cat scheduler/hub-data-summary.txt
   ```

3. **Manually update schedule.json** if needed (e.g., for schedule changes not captured automatically)

4. **Verify**:
   ```bash
   ./scheduler/schedule.sh  # Should show updated information
   ```

## Notes

- The data collection scripts extract emails and basic metadata from Confluence pages
- Schedule information is manually maintained in `parse-schedules.py` based on the main schedule page
- Some hubs may have incomplete data (e.g., missing feed URLs) - these should be filled in manually
- The system assumes `ingest.sh` exists in the parent directory and accepts a provider code as argument

## Troubleshooting

### jq not found
Install jq: `brew install jq` (macOS) or `apt-get install jq` (Linux)

### No hubs found for current month
This is normal if no hubs are scheduled. Check `schedule.json` to verify schedules.

### Email template not generating correctly
Check that `email-template.txt` exists and contains valid placeholders. The script uses simple sed substitutions.

### Schedule information missing
Run `python3 scheduler/parse-schedules.py` to update schedule information, or manually edit `schedule.json`.
