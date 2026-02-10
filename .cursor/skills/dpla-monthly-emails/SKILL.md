# DPLA Monthly Hub Scheduling Emails Skill

## Purpose
Generate and send monthly scheduling notification emails to DPLA hubs informing them that their metadata ingest will be performed during the last week of the month.

## When to Use
Activate this skill when the user says:
- "Generate monthly emails"
- "Create hub notification emails for [month]"
- "Send scheduling emails to hubs"
- "Prepare February hub notifications"
- "Draft ingest notification emails"

## Overview
At the beginning of each month, DPLA sends notification emails to hubs scheduled for ingest that month. These emails inform hub contacts that:
1. Their data will be harvested during the **last week of the month**
2. Any feed issues should be reported in advance
3. They can monitor progress on their dashboard

## Key Files

| Resource | Path |
|----------|------|
| Schedule JSON | `/Users/scott/dpla/code/ingestion3/scheduler/schedule.json` |
| Email Template | `/Users/scott/dpla/code/ingestion3/scheduler/email-template.txt` |
| Email Script | `/Users/scott/dpla/code/ingestion3/scheduler/email-template.sh` |
| Output Dir | `/Users/scott/dpla/code/ingestion3/scheduler/emails/` |
| i3.conf | `/Users/scott/dpla/code/ingestion3-conf/i3.conf` |

## Workflow

### Step 1: Identify Scheduled Hubs
First, determine which hubs are scheduled for the target month.

```bash
# Using the existing script
cd /Users/scott/dpla/code/ingestion3/scheduler
./schedule.sh
```

Or query the schedule.json directly:
```bash
# Find February (month 2) hubs
jq '.hubs[] | select(.schedule.months[] == 2) | {name, provider_code, contacts, frequency: .schedule.frequency}' schedule.json
```

### Step 2: Generate Email Files
Use the existing email generation script:

```bash
cd /Users/scott/dpla/code/ingestion3/scheduler
./email-template.sh
```

This creates files in `scheduler/emails/` with format: `{provider_code}-{year}-{month}.txt`

### Step 3: Review and Customize
Review generated emails in `scheduler/emails/`. Customize as needed:
- Add specific notes about known issues
- Include any special instructions
- Update dashboard URLs if changed

### Step 4: Send Emails
Emails can be sent via:
1. Copy/paste into email client
2. Command line (if configured)
3. Through DPLA's email system

## Email Template Format

The standard template includes:

```
Subject: DPLA Monthly Ingest Notification - {{MONTH}} {{YEAR}}

Dear {{HUB_NAME}} Team,

This is a scheduled notification that DPLA will be performing a metadata ingest 
for {{HUB_NAME}} during the last week of {{MONTH}} {{YEAR}}.

Schedule Details:
- Frequency: {{FREQUENCY}}
- Scheduled Months: {{MONTHS}}
- Ingest Window: Last week of {{MONTH}}

The ingest process will include:
1. Harvesting metadata from your feed
2. Mapping to DPLA metadata model
3. Enrichment with additional data
4. Publishing to dp.la

{{#NOTES}}
Special Notes: {{NOTES}}
{{/NOTES}}

If you have any questions or need to report feed issues, please contact us 
before the ingest window begins.

Dashboard: {{DASHBOARD_URL}}

Best regards,
DPLA Content Team
```

## Hub Schedule Reference

### Quarterly Hubs (4x/year)
| Hub | Months | Notes |
|-----|--------|-------|
| David Rumsey | Dec, Mar, Jun, Sep | |
| Internet Archive | Nov, Feb, May, Aug | |
| NARA | Jan, Apr, Jul, Oct | Wikimedia Partner |
| Smithsonian | Nov, Feb, May, Aug | File harvest, needs preprocessing |

### Monthly Hubs (12x/year)
| Hub | Notes |
|-----|-------|
| Harvard | OAI harvest |

### Bi-Monthly Hubs (6x/year)
| Hub | Months |
|-----|--------|
| Various | Every other month |

## Generating Custom Emails

### For a Specific Month
```bash
# Set the month (1-12) before running
export TARGET_MONTH=2  # February
./email-template.sh
```

### For a Specific Hub
Draft a custom email:

```
Subject: DPLA Ingest Notification - [Hub Name] - [Month] [Year]

Dear [Hub Name] Team,

This is a reminder that DPLA will be harvesting your metadata during the 
last week of [Month] [Year].

IMPORTANT: Please ensure your OAI feed is accessible and up-to-date before 
[Date of last week].

If you have any updates, new collections, or known issues with your feed, 
please let us know by replying to this email.

Your Dashboard: [Dashboard URL]

Schedule: [Frequency] - [List of scheduled months]

Best regards,
DPLA Content Team
```

## Contact Information Sources

Contacts are stored in `schedule.json` under each hub's `contacts` array:

```json
{
  "name": "Hub Name",
  "provider_code": "hub-code",
  "contacts": [
    "primary@example.com",
    "secondary@example.com"
  ]
}
```

To get all contacts for a month:
```bash
jq -r '.hubs[] | select(.schedule.months[] == 2) | "\(.name): \(.contacts | join(", "))"' schedule.json
```

## Special Considerations

### File-Based Hubs
For hubs with file-based harvests (Florida, Smithsonian, Vermont):
- May need to request updated data export
- Include note about expected data delivery timing

### Wikimedia Partners
Hubs marked as "Wikimedia Partner" in notes:
- May have special requirements
- Include Wikimedia-specific language if needed

### New/Changed Feeds
If a hub has recently changed their feed:
- Include note about testing/verification
- May want to schedule a pre-ingest check

## Example Agent Interaction

**User**: "Generate February hub notification emails"

**Agent Actions**:
1. Read `schedule.json` to find February hubs
2. List the hubs: florida, getty, gpo, harvard, ia, mwdl, nypl, oklahoma, scdl, smithsonian, texas, vt
3. Run `./email-template.sh` or generate emails directly
4. Show the user the generated emails
5. Offer to customize any specific emails
6. Provide instructions for sending

**User**: "Draft a special email for Smithsonian about their preprocessing requirements"

**Agent Actions**:
1. Create custom email noting:
   - File-based harvest requiring data export
   - Request for updated export by specific date
   - Note about XML preprocessing requirements
2. Show draft for approval
3. Save to `scheduler/emails/smithsonian-2026-Feb.txt`

## Quick Commands

```bash
# Generate all emails for current month
cd /Users/scott/dpla/code/ingestion3/scheduler && ./email-template.sh

# List scheduled hubs for February
jq -r '.hubs[] | select(.schedule.months[] == 2) | .name' schedule.json

# Get contacts for a specific hub
jq -r '.hubs[] | select(.provider_code == "harvard") | .contacts[]' schedule.json

# Preview what ingests are scheduled
cd /Users/scott/dpla/code/ingestion3/scheduler && ./schedule.sh
```
