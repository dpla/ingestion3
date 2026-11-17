---
name: pre-ingest-email
description: >
  Workflow for generating and sending DPLA monthly pre-ingest emails to hub
  partners via AWS SES. Use this skill whenever someone asks to: send the
  monthly ingest email, prepare the DPLA pre-ingest notice, generate the hub
  email for a given month, preview the pre-ingest email, or do anything
  involving the pre-ingest email pipeline. Trigger even if the user just says
  something like "time to send the ingest email" or "it's email day" — context
  from the project makes the intent clear.
---

# DPLA Pre-Ingest Email Skill

This skill walks through the full pipeline: gathering hub data from `i3.conf`,
generating a structured `.txt` draft, previewing the rendered HTML, and sending
via AWS SES. Everything lives in `pre_ingest_monthly_email/` in the ingestion3 repo.

---

## File layout

```
pre_ingest_monthly_email/
├── 1_get_info.py        — generates the .txt draft from i3.conf
├── 2_send_email.py      — renders HTML + sends via SES (or dry-runs)
├── txt_emails/          — generated .txt files land here
├── images/              — downloaded cover images land here
└── standard_images/
    └── image.png        — DPLA logo (embedded in every email)
```

`i3.conf` lives separately at `~/Documents/Repos/ingestion3-conf/i3.conf`
(or wherever `INGESTION3_CONF_REPO` points in the `.env` file).

---

## Workflow — step by step

### 1. Confirm the details with the user

Before running anything, check you have:

| Info | How to get it |
|------|--------------|
| **Month & year** | Ask if not mentioned — default to current month |
| **Cover image URL** | Optional — a representative image from one of the hub collections. If the user has one, grab it; if not, skip it. |
| **Custom intro text** | Optional — the default intro is fine for most months. |
| **Custom subject line** | Optional — default is `DPLA Ingests for <Month> <Year>`. |

If all you know is "send the May email", that's enough to proceed with defaults.

### 2. Generate the .txt draft

Run `1_get_info.py` from the ingestion3 repo root:

```bash
./venv/bin/python pre_ingest_monthly_email/1_get_info.py --month <M> --year <YYYY>
```

With optional flags:
```bash
./venv/bin/python pre_ingest_monthly_email/1_get_info.py \
  --month 5 --year 2026 \
  --image-url "https://example.com/cover.jpg" \
  --image-caption "Harvest Time, Illinois Digital Heritage Hub" \
  --subject "DPLA Ingests for May 2026"   # only if overriding
```

`get_info.py` will:
- Read `i3.conf` to find hubs scheduled for that month
- Calculate the last full work week of the month (the ingest week)
- Download the cover image (if a URL was given) to `images/`
- Write a `.txt` file to `txt_emails/pre-ingest-<YYYY>-<MM>_<timestamp>.txt`

Tell the user the output filename and the ingest week it calculated, so they can confirm it looks right before proceeding.

### 3. Preview the email (dry-run)

Show the user what the email will look like before sending:

```bash
./venv/bin/python pre_ingest_monthly_email/2_send_email.py --dry-run
```

This prints the raw HTML to stdout. You don't need to show all of it —
just confirm the key parts look right: subject line, hub list, ingest week date,
cover image (if any). If the user spots anything wrong, re-run `1_get_info.py`
with corrected flags rather than editing the `.txt` file by hand.

### 4. Send the email

Once the user confirms everything looks good:

**Test send first**, to the user's own address only (no BCC). `--test` uses `DPLA_USER_EMAIL` from the repo's `.env`, which `ingest_pipeline_python/helper_scripts/onboarding.py` sets; if it's missing, run that (or use `--to <address>`):

```bash
./venv/bin/python pre_ingest_monthly_email/2_send_email.py --test
```

Tell the user to check their inbox and confirm it looks right.

**Production send** — `TEST_MODE = False` in `2_send_email.py` (its current setting), so a plain run is the real send:

```bash
./venv/bin/python pre_ingest_monthly_email/2_send_email.py
```

- Sends `To: ingest@dp.la`, `BCC:` all hub contact emails from `i3.conf`
- Only run it when Zoe explicitly says "send for real" or "production send"

Always confirm with the user before the production send.

---

## Useful flags reference

### 1_get_info.py

| Flag | Default | Notes |
|------|---------|-------|
| `--month` | current month | 1–12 |
| `--year` | current year | |
| `--subject` | `DPLA Ingests for <Month> <Year>` | |
| `--intro` | standard harvest intro | multi-sentence string |
| `--image-url` | _(none)_ | downloads to `images/` |
| `--image-caption` | _(none)_ | credit line shown under cover image |
| `--dry-run` | off | prints to stdout, no file written |
| `--i3-conf` | from `.env` or `~/Documents/Repos/ingestion3-conf/i3.conf` | |

### 2_send_email.py

| Flag | Default | Notes |
|------|---------|-------|
| `--dry-run` | off | renders HTML to stdout, no send |
| `--file` | latest `.txt` in `txt_emails/` | specify a filename to use a different one |
| `--aws-profile` | `dpla` | AWS credentials profile |
| `--i3-conf` | from `.env` | used for BCC list in production mode |
| `--test` | off | send only to `DPLA_USER_EMAIL` from `.env`, no BCC |
| `--to` | _(none)_ | send only to this address, no BCC |

---

## Troubleshooting

**`AccessDenied` on send:**
The `[user]` IAM user needs `ses:SendRawEmail` on
`arn:aws:ses:us-east-1:283408157088:identity/dp.la`.
This is an IAM permission — ask Dominic to add it if it's missing.

**Logo not found:**
`standard_images/image.png` must exist. It's the DPLA logo. If it's missing,
the email still sends but without the logo image.

**Wrong ingest week:**
`1_get_info.py` calculates the last full Monday–Friday of the month. If the
auto-detected week looks wrong, pass a corrected `--intro` with the right date
or let the user know they may need to adjust.

**`i3.conf` not found:**
Set `INGESTION3_CONF_REPO` in the `.env` file at the root of the ingestion3
repo, or pass `--i3-conf /path/to/i3.conf` directly.

**No hubs listed:**
The hub may be on hold (`schedule.status = "on-hold"` in `i3.conf`) or not
scheduled for that month. Check `i3.conf` directly if a hub seems missing.
