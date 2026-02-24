# How AGENTS.md works: a human-readable guide

This document explains what AGENTS.md is, how it is used, and how an agent (or a person following the same process) moves through it to run ingests and handle notifications consistently.

---

## What is AGENTS.md?

**AGENTS.md** is a single, agent-facing document at the root of the ingestion3 repository. Its job is to:

- Tell an agent **how to run an ingest** (which runbooks and scripts to use, and in what order).
- Define **when and how to notify** (Slack #tech-alerts and/or email tech@dp.la) for errors and status.
- Point to **other docs** (runbooks, SCRIPTS.md, i3.conf, debug skill) so the agent does not have to guess where to look.

It is written for AI agents and for humans who want to behave like the agent: follow one place for procedure and notification policy.

---

## How does it get utilized?

**By an AI agent:** When you ask the agent to “run the ingest for maryland” or “debug the nara failure,” the agent is expected to open or reference AGENTS.md (via project rules, or by finding it when searching for “how to run ingest”). From there it:

1. Uses the “Running an ingest” section to choose the right runbook and scripts.
2. Uses the “Notifications and errors” and “Post stage failures to #tech-alerts” sections to know that every failure must be reported to #tech-alerts or tech@dp.la.
3. Uses the checklist and tables to avoid skipping verification or notification.

**By a human:** A developer or operator can read AGENTS.md the same way: as the single procedure for running ingests and for ensuring errors and status are posted to #tech-alerts or emailed to tech@dp.la. It reduces reliance on tribal knowledge and keeps manual runs aligned with what the agent would do.

**By automation:** The orchestrator (`scheduler/orchestrator`) already implements much of the notification behavior (Slack alerts, escalation files). AGENTS.md documents that behavior and adds the rule that the Slack webhook should target #tech-alerts and that, if Slack is unavailable, tech@dp.la must be notified. So automation and agents share the same “contract.”

---

## How the agent goes through its process

Below is a step-by-step flow that matches how an agent (or a person following AGENTS.md) is supposed to work.

### 1. Request received

The user says something like:
- “Run the ingest for maryland.”
- “Run the NARA delta ingest for January 2026.”
- “The smithsonian harvest failed; debug it and post to the team.”

The agent’s first move is to treat this as an ingest-related task and open **AGENTS.md**.

### 2. Identify hub and harvest type

From AGENTS.md, the agent:

- Identifies the **hub** (e.g. maryland, nara, smithsonian).
- Looks up **harvest type** in i3.conf (`$I3_CONF`, default `~/dpla/code/ingestion3-conf/i3.conf`) under `<hub>.harvest.type` (e.g. `localoai`, `api`, `file`, `nara.file.delta`).

That determines which runbook applies and whether this is a standard OAI/API run, a file-based run, or a special workflow (NARA, Smithsonian).

### 3. Choose runbook and follow it

Using the harvest-type → runbook mapping in AGENTS.md:

- **localoai** → runbooks/05-standard-oai-ingests.md  
- **api** → runbooks/06-standard-api-ingests.md  
- **file** → runbooks/04-file-based-imports.md (or 03 Smithsonian / 02 NARA when that hub)  
- **nara.file.delta** → runbooks/02-nara.md  

The agent opens that runbook and follows its steps (e.g. “run ingest.sh,” “run nara-ingest.sh,” “run fix-si.sh then harvest then remap”). If the runbooks directory does not exist yet, the agent falls back to scripts/SCRIPTS.md and the harvest-type logic still applies.

### 4. Run scripts and verify

The agent runs the scripts indicated in the runbook (e.g. `./scripts/ingest.sh maryland`, `./scripts/harvest/nara-ingest.sh --month=202601`). It then verifies outputs (e.g. _SUCCESS, record counts) and runs `./scripts/s3-sync.sh <hub>` when the runbook says so.

### 5. Apply notification policy

Throughout and after the run, the agent applies the rules from AGENTS.md:

- **Errors (must notify):** Feed unreachable, harvest failure, mapping/remap failure, S3 sync failure, anomaly halt, or any non-zero/failed step. Each of these must be reported to **#tech-alerts** (Slack) or **tech@dp.la** (email) if Slack is not available.
- **Stage failures:** The “Post stage failures to #tech-alerts” subsection says that for harvest, mapping, sync, or anomaly failures, the agent must ensure a message goes to #tech-alerts (or email tech@dp.la) with hub name, stage, and a short error or path to the report.
- **Status (should notify):** Run started, run completed, anomaly warnings. When using the orchestrator, these are sent automatically to Slack; when running scripts only, the agent may need to post or email status if that’s the team’s practice.

So the agent does not “run and forget”: it checks for failures and, for every failure, satisfies the “#tech-alerts or tech@dp.la” rule.

### 6. Use the checklist

AGENTS.md ends with a short checklist. The agent uses it to confirm:

- **Before:** Hub and harvest type confirmed; correct runbook opened; if using the orchestrator, Slack webhook (or email fallback) is in place.
- **After:** If any hub failed, a failure summary was posted to #tech-alerts or emailed to tech@dp.la, including stage and report path if available.

This closes the loop: run → verify → notify on failure → document where notifications went.

---

## Summary

- **AGENTS.md** = one place for “how to run ingests” and “how to notify (Slack #tech-alerts / tech@dp.la).”
- **Utilization** = agents and humans follow it for procedure and notifications; automation (orchestrator) is aligned with it and targets #tech-alerts.
- **Agent process** = (1) open AGENTS.md, (2) identify hub and harvest type, (3) follow the right runbook, (4) run scripts and verify, (5) apply notification policy for every error and stage failure, (6) use the checklist so nothing is skipped.

For the full, agent-facing text, see [AGENTS.md](../AGENTS.md) in the repository root.
