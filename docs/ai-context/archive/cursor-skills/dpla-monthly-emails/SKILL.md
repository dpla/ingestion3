---
name: dpla-monthly-emails
description: Generate/preview/draft/send the monthly pre-scheduling summary email to hub contacts scheduled for a month (from i3.conf schedule.months). Use when user asks to send the scheduling email, monthly scheduling email, notify hubs for a month, or pre-scheduling email.
---

# DPLA Monthly Hub Scheduling Emails Skill

## Purpose
Generate and send the **monthly pre-scheduling email** to DPLA hub contacts: one summary email listing all hubs scheduled for that month, the last calendar week date range, and asking for data readiness or skip requests.

## When to Use
Activate this skill when the user says:
- "Send scheduling email"
- "Send monthly scheduling email"
- "Notify hubs for February"
- "Pre-scheduling email for this month"
- "Generate monthly emails" / "Prepare February hub notifications"

## Overview
At the beginning of each month, DPLA sends **one summary email** to all contacts of hubs scheduled for that month (from **i3.conf**). The email:
1. States that ingests will run during the **last calendar week** of the month (with concrete date range)
2. Lists all hubs included that month
3. Asks contacts to have data ready or to tell DPLA if they need to be skipped or have issues
4. CC: **ingest@dp.la**, **dominic@dp.la** on every send

**Source of truth:** i3.conf. Hub inclusion is determined by `<hub>.schedule.months` (e.g. `digitalnc.schedule.months = [2, 5, 8, 11]`). Empty `schedule.months = []` means the hub is never scheduled (on-hold). Contacts come from `<hub>.email`.

**Environment:** Run `source .env` from repo root when using config/paths. See [AGENTS.md](AGENTS.md) § Environment and build.

## Key Files

| Resource | Path |
|----------|------|
| Scheduling email script | `scheduler/orchestrator/scheduling_emails.py` |
| Draft output dir | `scheduler/emails/` |
| i3.conf | `$I3_CONF` (e.g. `~/dpla/code/ingestion3-conf/i3.conf`) |

## Workflow: Always Preview Before Send

1. **Show preview first:** Run with `--dry-run` so the user sees:
   - **Date range** (e.g. "February 23 – February 28, 2026")
   - **Hubs included** (with "(no email in i3.conf)" noted where applicable)
   - Full To/CC list and email body
2. **After the user has seen and accepted the preview**, offer:
   - `--draft` — write draft to `scheduler/emails/scheduling-YYYY-MM.txt` for manual review/send
   - `--send` — send via AWS SES (CC ingest@dp.la, dominic@dp.la)

**Agent must always show the preview (date range + hubs list) before sending.**

## Commands

```bash
# Preview for current month (date range, hubs, body)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --dry-run

# Preview for a specific month (e.g. February = 2)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --dry-run

# Write draft file (after user approves preview)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --draft

# Send via SES (after user approves preview)
./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --send
```

## Example Agent Interaction

**User:** "Send scheduling email for February"

**Agent Actions:**
1. Run: `./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --dry-run`
2. Show the user the **Preview** output: date range (e.g. February 23 – February 28, 2026), list of hubs, To/CC, and body
3. Ask: "If this looks correct, should I write a draft to `scheduler/emails/` or send the email now?"
4. If user says send: run with `--send`. If draft: run with `--draft`

## Optional: Per-Hub Drafts (schedule.json)

The repo also has **schedule.json**-based tools for per-hub email drafts (different workflow):
- `scheduler/schedule.json` — alternate schedule/contacts source
- `scheduler/email-template.sh` + `scheduler/email-template.txt` — generate one file per hub in `scheduler/emails/`

The **canonical** monthly pre-scheduling process is the i3.conf-based summary email above. Use the schedule.json workflow only if the user explicitly asks for per-hub drafts or Confluence-derived data.

## Quick Reference

| Task | Command |
|------|---------|
| Preview (current month) | `./venv/bin/python -m scheduler.orchestrator.scheduling_emails --dry-run` |
| Preview (February) | `./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --dry-run` |
| Write draft | `./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --draft` |
| Send via SES | `./venv/bin/python -m scheduler.orchestrator.scheduling_emails --month=2 --send` |

Documented in [scripts/SCRIPTS.md](scripts/SCRIPTS.md) under "Scheduling emails (monthly pre-scheduling notification)".
