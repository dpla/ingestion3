# DPLA Pipeline Unification -- Hub Readiness and Scheduling Layer

**Audience:** Engineers, architects, leadership at receiving organization

**Reading time:** 25 minutes

**Status:** Beta proposal -- ready for review and implementation scoping

**Context:** This document addresses the operational costs of the monthly hub communication cycle -- specifically, the cost of not knowing whether a hub is ready before starting its ingest. It proposes a lightweight, hub-friendly solution that respects the partnership while significantly reducing engineering overhead.

---

## Contents

- [The Cost of the Current Communication Model](#the-cost-of-the-current-communication-model)
- [How the Current Process Works (Before This Proposal)](#how-the-current-process-works-before-this-proposal)
- [The Core Idea: Every Hub Gets a "Ready" Button](#the-core-idea-every-hub-gets-a-ready-button)
- [Design Principles](#design-principles)
- [i3.conf Configuration](#i3conf-configuration)
  - [Property Reference](#property-reference)
- [The Monthly Flow](#the-monthly-flow)
  - [Step 1: Scheduling Email (All Hubs)](#step-1-scheduling-email-all-hubs)
  - [Step 2: Hub Clicks "Ready"](#step-2-hub-clicks-ready)
  - [Step 3: Pipeline Runs](#step-3-pipeline-runs)
  - [Step 4: Mapping Summary Email (All Hubs)](#step-4-mapping-summary-email-all-hubs--existing-process-enhanced)
  - [Implementation: Enhancing the Existing Emailer](#implementation-enhancing-the-existing-emailer)
- [S3 Deposit Monitoring (Additional Signal for File-Based Hubs)](#s3-deposit-monitoring-additional-signal-for-file-based-hubs)
- [Tennessee Set List: A Special Case](#tennessee-set-list-a-special-case)
- [Post-Ingest Revocations: Reducing Cost When They Still Happen](#post-ingest-revocations-reducing-cost-when-they-still-happen)
  - [Automated Hub-Level S3 Purge](#automated-hub-level-s3-purge)
  - [Automated Quality Gates (Pre-Sync)](#automated-quality-gates-pre-sync)
  - [The Revocation Cost Argument](#the-revocation-cost-argument)
  - [Hubs That Revoke Frequently](#hubs-that-revoke-frequently)
- [Technical Investigation: Hub-Partitioned Indexing](#technical-investigation-hub-partitioned-indexing)
  - [Approach A: Spanning Alias Over Per-Hub Indices](#approach-a-spanning-alias-over-per-hub-indices)
  - [Approach B: Delete-by-Query](#approach-b-delete-by-query)
  - [The CDL Use Case](#the-cdl-use-case)
  - [Why This Is Deferred](#why-this-is-deferred)
- [Unified Architecture](#unified-architecture)
  - [Orchestrator Integration](#orchestrator-integration)
- [Beta Proposal: Implementation Plan](#beta-proposal-implementation-plan)
  - [What to Build](#what-to-build)
  - [Suggested Build Order](#suggested-build-order)
  - [Cost](#cost)
  - [What's Deferred to Greenfield](#whats-deferred-to-greenfield)
- [Hub Onboarding](#hub-onboarding)
- [Risks and Mitigations](#risks-and-mitigations)
- [Summary](#summary)

---

## The Cost of the Current Communication Model

Every month, DPLA runs ingests for ~60 partner hubs. Before each ingest can start, DPLA needs to know that the hub's data is ready. Today, there is no structured way to get that answer.

DPLA sends a batch scheduling email telling hubs that ingests will run during a given week. Some hubs reply with delays or flags. Most say nothing -- because they're ready and don't think to respond, or because the email was missed. For file-based hubs that deposit data in S3 buckets, someone at DPLA must manually check whether new data has arrived. Tennessee sends a monthly email with a list of OAI sets that must be manually read and applied to the harvest configuration.

The most expensive version of this problem is a **post-ingest revocation**: a hub says they're ready, DPLA ingests their data, and then the hub discovers a problem -- a mapping error, unexpected record counts, data they didn't intend to include. They ask DPLA to delete the data and redo the ingest. Sometimes this happens after the search index has already been rebuilt with the bad data, making the fix even more costly.

Each full revocation cycle takes 5--8 hours of engineering time:

| Action | Approximate Time |
|--------|-----------------|
| Delete local data and re-harvest | 30 minutes |
| Delete from S3, restore previous | 45 minutes |
| Re-index all hubs and re-flip the alias | 3--4 hours (plus EMR cost) |
| Debug the original data problem | 1--3 hours |
| Coordinate with the hub by email | 30--60 minutes |

At a 5% revocation rate across 60 hubs, that is roughly 15--24 hours of engineering time per month spent on corrections that could have been avoided with a cleaner readiness signal. And this is before accounting for the routine overhead: the daily bucket-checking, the email threads trying to determine whether a hub is ready, the judgment calls made when silence is ambiguous.

The current process is not a failure of the partnership -- it is a natural result of managing 60 relationships with email as the only structured channel. This proposal gives hubs a simple, direct way to communicate readiness, and gives DPLA an automated way to receive it.

---

## How the Current Process Works (Before This Proposal)

For reference:

1. **Scheduling email goes out** (e.g., `scheduler/emails/scheduling-2026-02.txt`) telling hubs that ingests will run during the last week of the month.
2. **Hubs reply by email** if they need to skip, delay, or flag issues ("our feed isn't ready, can you wait a week?").
3. **File-based hubs deposit data in S3 buckets** (`dpla-hub-fl`, `dpla-hub-si`, etc.), and someone at DPLA manually checks whether new data has arrived.
4. **Tennessee sends a monthly email** with the specific OAI set list available for harvest, which must be manually read and applied to the harvest configuration.
5. **Post-ingest revocations** occur when hubs confirm readiness, get ingested, and then discover a problem -- sometimes after the search index has already been rebuilt.

There is no structured way to capture hub readiness status, no automated monitoring of S3 deposits, and no way to distinguish "hub didn't reply because they're ready" from "hub didn't reply because the email went to spam." Problems are discovered at harvest time -- the most expensive moment to learn that a feed was not ready.

---

## The Core Idea: Every Hub Gets a "Ready" Button

Today, DPLA sends a scheduling email and then guesses when hubs are ready. Hubs email back corrections ("not yet," "skip us," "wait a week"). This proposal flips that model:

**Every hub receives a monthly scheduling email with a "Ready -- start my ingest" button.** The pipeline does not start for a hub until they click it (or the ingest window closes and they're auto-skipped).

This is the universal default for all hubs. It standardizes the process:

- Same email format for every hub.
- Same one-click interaction for every hub.
- No ambiguity about whether a hub is ready.
- The hub is in control of timing. They click when their data is ready.
- No more "we harvested too early" or "wait, we weren't ready yet."

The **only optional flag** is whether the hub also wants to **review before publishing** -- a post-ingest confirmation step where they see a summary of the harvest and approve or reject before the data syncs to S3 and enters the search index. Some hubs will always want this. Most won't need it.

```
            Every hub, every month
            ────────────────────────

  Scheduling email with "Ready" button
  (sent at start of ingest window)
                    │
                    ▼
  Hub clicks "Ready — start my ingest"
  (or "Skip this month")
                    │
                    ▼
  Pipeline runs: harvest → map → enrich → JSONL
                    │
                    ▼
  Mapping summary email sent (all hubs — existing process)
  Includes: _SUMMARY, error logs ZIP, record counts
                    │
                    ▼
            ┌───────┴───────┐
            │               │
     review = false    review = true
            │               │
     Summary email is    Summary email adds
     informational       Approve/Reject links
     (same as today)     at the top
            │               │
     Auto-sync to S3   Hub approves → sync
                        Hub rejects → hold
                        No response → auto-approve
                                      after 48h
```

---

## Design Principles

The proposal is designed around four constraints: it must work for all 60 hubs, it must require minimal change for hubs, it must fit inside the existing orchestrator, and it must address the revocation cost problem at the source.

1. **Universal process.** Every hub gets the same "Ready" button email. No special cases for how readiness is communicated. The process is the same whether you're NARA or a small state hub.

2. **One optional flag.** The only per-hub configuration choice is whether they want post-ingest review before publishing. Everything else is standard.

3. **Software handles what software can handle.** For file-based hubs, S3 deposit monitoring provides an additional signal -- if data appears in the bucket, the hub is implicitly ready even if they forgot to click. For OAI/API hubs, the "Ready" click is the only signal needed.

4. **Hub-configurable in i3.conf.** Preferences are stored alongside existing hub configuration, making them visible, versionable, and consumable by the orchestrator.

---

## i3.conf Configuration

Two new properties per hub. That's it.

```ini
# Review before publish: false (default) or true
florida.readiness.review = "false"

# Review before publish: true, with 48h auto-approve window
mwdl.readiness.review = "true"
mwdl.readiness.review_window_hours = 48

# Review before publish: true, no auto-approve (explicit approval required)
problematic_hub.readiness.review = "true"
problematic_hub.readiness.review_window_hours = 0

# Tennessee: has a set list submission step
tn.readiness.review = "true"
tn.readiness.set_list_source = "form"
```

### Property Reference

| Property | Values | Default | Description |
|----------|--------|---------|-------------|
| `readiness.review` | `true`, `false` | `false` | Whether this hub reviews a post-ingest summary before data is published |
| `readiness.review_window_hours` | integer | `48` | Hours to wait before auto-approving (0 = no auto-approve, hub must explicitly approve) |
| `readiness.set_list_source` | `form`, `static` | — | For hubs like TN that provide a per-month set list |

That's the entire configuration surface. Every other aspect of the process is standardized.

---

## The Monthly Flow

### Step 1: Scheduling Email (All Hubs)

At the start of the ingest window, every scheduled hub's contacts receive an email:

```
Dear Florida Hub Contacts,

DPLA metadata ingests are scheduled for the last week of February 2026.

Your hub: florida (Sunshine State Digital Network)

When your data is ready for harvest, click the link below:

  ▶ Ready — start my ingest:
    https://dpla-hub.example.com/go/florida/2026-02?token=abc123

If you need to skip this month:
    https://dpla-hub.example.com/skip/florida/2026-02?token=abc123

Current status: ⏳ Waiting for your confirmation
View status:    https://dpla-hub.example.com/status/florida/2026-02

If we don't hear from you by February 28, your hub will be
automatically skipped for this month.

Best regards,
DPLA Content Team
```

This replaces the current batch scheduling email with per-hub emails that have actionable links.

### Step 2: Hub Clicks "Ready"

```
Hub clicks "Ready — start my ingest"
  │
  ▼
API Gateway → Lambda
  ├─ Validate token (per-hub, per-month, time-scoped)
  ├─ Update readiness store: florida = "ready"
  ├─ Return confirmation page:
  │    "Thanks! Your ingest will begin shortly.
  │     Changed your mind? Click here to pause."
  ├─ Notify Slack #tech-alerts:
  │    "✅ Florida clicked Ready for February. Ingest queued."
  └─ Orchestrator picks up the hub on its next poll
```

The hub is in control. They click when their data is ready. The pipeline responds.

### Step 3: Pipeline Runs

Standard pipeline: harvest → map → enrich → JSONL. No changes to the pipeline itself.

### Step 4: Mapping Summary Email (All Hubs -- Existing Process, Enhanced)

Every hub already receives a mapping summary email after ingest. This is sent via `send-ingest-email.sh`, which invokes the Scala `Emailer` class. The email includes the `_SUMMARY` file (record counts, mapping success rate, error breakdown) and a ZIP attachment of error/warning logs. It's sent from `tech@dp.la` and CC'd to `tech@dp.la`. Hubs expect this email -- it's a standard part of the monthly cycle.

**The only change:** For hubs with `review = true`, the summary email adds approve/reject links at the top. For hubs with `review = false`, the email is unchanged from today.

#### review = false (most hubs, same as today)

The mapping summary email goes out exactly as it does now. Data auto-syncs to S3. The email is informational:

```
Subject: DPLA Ingest Summary for Sunshine State Digital Network - February 2026

This is an automated email summarizing the DPLA ingest. Please see
attached ZIP file for record level information about errors and warnings.

If you have questions please contact us at tech@dp.la

    Attempted     Successful    Failed
    12,847        12,691        156

    [detailed _SUMMARY content...]

Bleep bloop.

-----------------  END  -----------------

Attachment: log_file.zip
```

No behavior change for the hub. No new buttons. Same email they've always received.

#### review = true (opt-in hubs)

The mapping summary email includes the same content, but with approve/reject links prepended:

```
Subject: DPLA Ingest Summary for Mountain West Digital Library - February 2026

┌─────────────────────────────────────────────────────┐
│  Your February ingest is complete and ready for      │
│  review before publishing.                           │
│                                                      │
│  ✅ Approve — publish this data:                     │
│     https://dpla-hub.example.com/review/mwdl/...     │
│                                                      │
│  ❌ Reject — there's a problem, hold off:            │
│     https://dpla-hub.example.com/review/mwdl/...     │
│                                                      │
│  This data will auto-publish in 48 hours if we       │
│  don't hear from you.                                │
└─────────────────────────────────────────────────────┘

This is an automated email summarizing the DPLA ingest. Please see
attached ZIP file for record level information about errors and warnings.

If you have questions please contact us at tech@dp.la

    Attempted     Successful    Failed
    45,231        44,892        339

    [detailed _SUMMARY content...]

Bleep bloop.

-----------------  END  -----------------

Attachment: log_file.zip
```

**What "Approve" does:** Triggers S3 sync. Hub data enters the publish pipeline.
**What "Reject" does:** Holds the data, notifies DPLA staff via Slack, asks the hub what's wrong. The hub can coordinate a fix and re-trigger when ready.

For hubs with `review_window_hours = 0` (explicit approval required), the auto-publish line changes to: "This data will NOT publish until you approve it."

#### Implementation: Enhancing the Existing Emailer

The existing `Emailer.scala` sends the summary via SES. The enhancement is minimal:

1. Before calling `Emailer`, the orchestrator checks the hub's `readiness.review` setting.
2. If `review = true`, the orchestrator generates approve/reject URLs (token-authenticated) and passes them to the Emailer.
3. The Emailer prepends the review block to the HTML body (the `prefix` variable in `Emailer.scala` already provides a template for prepended content).
4. If `review = false`, the Emailer sends exactly as it does today.

The `_SUMMARY` content, the ZIP attachment, the CC to `tech@dp.la` -- all unchanged. The only addition is the review block at the top for opted-in hubs.

---

## S3 Deposit Monitoring (Additional Signal for File-Based Hubs)

For the 10 hubs that deposit data in S3 buckets, S3 monitoring provides an **additional readiness signal** alongside the "Ready" button. If the orchestrator detects new data in a hub's source bucket, it treats that as an implicit "ready" even if the hub hasn't clicked the button yet.

This is not a separate mode -- it's layered on top of the universal "Ready" button flow. File-based hubs still get the same scheduling email, but the orchestrator also watches their bucket:

```
Orchestrator pre-flight (daily during ingest window):
  │
  ├─ For each file-based hub in S3_SOURCE_BUCKETS:
  │    └─ aws s3 ls s3://{bucket}/ → compare to last ingest timestamp
  │
  ├─ New data detected AND hub hasn't clicked "Ready" yet?
  │   └─ Auto-mark as "ready" (S3 deposit is implicit confirmation)
  │   └─ Notify Slack: "Florida: new data detected in dpla-hub-fl.
  │                     Auto-starting ingest (hub hasn't clicked Ready yet)."
  │
  └─ No new data detected AND hub clicked "Ready"?
      └─ Flag for staff: "Ohio clicked Ready but no new data in bucket.
                          Proceed with existing data? [Yes] [Hold]"
```

**What this eliminates:** The "did they upload yet?" checking cycle. The software watches the bucket. When data appears, the ingest starts. When it doesn't, the hub is auto-skipped at the end of the window.

```python
def check_s3_deposits(config, month):
    """Check S3 source buckets for file-based hubs."""
    for hub_name, bucket in config.S3_SOURCE_BUCKETS.items():
        if hub_name not in config.get_scheduled_hubs(month):
            continue
        latest = get_latest_object_timestamp(bucket, config.aws_profile)
        last_ingest = get_last_ingest_timestamp(hub_name)
        if latest and latest > last_ingest:
            yield hub_name, "deposited", latest
        else:
            yield hub_name, "no_new_data", latest
```

---

## Tennessee Set List: A Special Case

Tennessee sends a monthly email listing the specific OAI sets available for harvest. Their scheduling email includes an additional element:

```
Dear Tennessee Hub Contacts,

Your hub: tn (Digital Library of Tennessee)

When your set list is ready for this month's harvest:

  ▶ Submit set list and start ingest:
    https://dpla-hub.example.com/sets/tn/2026-02?token=abc123

  This will open a simple form where you can paste your set list.
  We'll preview the diff from last month and start the harvest
  once you confirm.

  ⏭️ Skip this month:
    https://dpla-hub.example.com/skip/tn/2026-02?token=abc123
```

The "Submit set list" link opens a simple form where the TN contact pastes their set list. The system parses the set names, shows a diff from last month, and on confirmation updates the harvest config and queues the ingest. This replaces the current manual process of reading TN's email and hand-editing the configuration.

---

## Post-Ingest Revocations: Reducing Cost When They Still Happen

Even with the "Ready" button and optional review gate, some revocations will still occur. The goal is to reduce the cost of each one, both through prevention (quality gates before sync) and through automation (a single command to purge and flag for re-indexing).

### Automated Hub-Level S3 Purge

When a revocation happens post-sync, a single command handles cleanup:

```python
def revoke_hub(hub_name, month, config):
    """Purge a hub's data from staged S3 and flag for re-indexing."""
    s3_prefix = config.get_s3_prefix(hub_name)
    bucket = config.get_s3_dest_bucket()
    latest_jsonl = find_latest_s3_directory(bucket, f"{s3_prefix}/jsonl/")
    if latest_jsonl:
        delete_s3_directory(bucket, latest_jsonl, config.aws_profile)
    previous_jsonl = find_previous_s3_directory(bucket, f"{s3_prefix}/jsonl/")
    update_readiness_status(hub_name, month, "revoked",
                           notes=f"Data purged. Previous: {previous_jsonl}")
    post_slack(f"⚠️ {hub_name} revoked for {month}. "
               f"S3 data purged. Re-indexing required.")
```

### Automated Quality Gates (Pre-Sync)

Catch common problems automatically before they reach S3:

1. **Record count anomaly:** Flag if count differs >10% from previous harvest.
2. **Schema drift detection:** Flag if mapped records have new null fields or unexpected values.
3. **Sample-based diff:** Compare 100 random records to their previous versions. Flag if >20% have significant structural changes.
4. **Broken link spot-check:** For a sample of `isShownAt` URLs, verify they resolve.

These gates run between ingest and sync. A gate failure holds the data and notifies both the hub contact and DPLA staff -- regardless of whether the hub opted into review. Quality gates are not optional; they protect both sides.

### The Revocation Cost Argument

Every revocation has a real cost:

| Action | Approximate Engineer Time |
|--------|--------------------------|
| Delete local data and re-harvest | 30 minutes |
| Delete from S3, restore previous | 45 minutes |
| Re-index all hubs + re-flip alias | 3-4 hours (+ EMR cost) |
| Debug "why does the data look wrong" | 1-3 hours |
| Email back-and-forth coordinating | 30-60 minutes |
| **Typical full revocation cycle** | **5-8 hours** |

These costs were documented in the opening section. They bear repeating here in the context of quality gates: the gates are designed to catch the most common revocation trigger -- data quality issues -- before the data reaches S3. Once data is in S3 and the index has been rebuilt, each revocation costs 5--8 hours. Before sync, a quality gate that holds the data costs minutes. The "Ready" button prevents most timing-related revocations. The quality gates prevent most data-quality revocations. The optional review gate gives hubs the remaining control.

### Hubs That Revoke Frequently

For hubs with a pattern of revocations, the outreach is direct and collaborative:

> We've noticed that [hub name] has needed [N] data corrections over the past year. We'd recommend turning on the "review before publish" option for your hub -- it gives your team a chance to catch issues before the data goes live, and saves both of us the overhead of corrections after the fact.

If a hub declines review and continues to revoke frequently, the concrete cost data (5--8 hours per revocation, documented) provides a basis for a firmer conversation about expectations. The collaborative offer of the tool should come first.

---

## Technical Investigation: Hub-Partitioned Indexing

> **Status:** Flagged for separate technical investigation. Not part of this beta proposal.

The most expensive revocation scenario is post-index: a hub needs to be removed from the search index after the alias has already been flipped. This section documents the problem and two approaches for addressing it -- but defers the decision to a separate technical spike because the implications are significant and require benchmarking at production scale.

Today, removing a hub from the search index after the alias has been flipped requires re-indexing the entire ~40M record dataset -- because sparkindexer builds a single monolithic index. Two approaches deserve investigation.

Two approaches deserve investigation:

### Approach A: Spanning Alias Over Per-Hub Indices

Instead of one `dpla-all-{timestamp}` index, create one index per hub (`dpla-florida-{timestamp}`, `dpla-georgia-{timestamp}`, etc.) and point the `dpla_alias` at all of them. Removing a hub becomes a metadata operation: remove that hub's index from the alias. Re-adding it means re-indexing only that hub, not all 60.

### Approach B: Delete-by-Query

Keep the monolithic index but use Elasticsearch's `_delete_by_query` API to surgically remove records by `provider.name`. Cheaper than a full re-index, though it leaves the index in a slightly degraded state until segment merges reclaim space.

### The CDL Use Case

This investigation is particularly relevant for the California Digital Library, which sends frequent delete requests for **specific individual records** -- not whole-hub revocations but item-level deletions. Today this requires both:
1. Deleting the specific record from the hub's JSONL in S3 (so it doesn't reappear on re-index).
2. Deleting the specific record from the Elasticsearch index.

A hub-partitioned index model would make operation (2) simpler (smaller index to query against), and both approaches would benefit from having a documented, automated "delete item from hub" operation rather than ad-hoc manual cleanup.

### Why This Is Deferred

Hub-partitioned indexing has significant implications for:
- Query performance (spanning alias across 60+ indices vs. one large index)
- Index management complexity (60+ index lifecycle vs. 1)
- sparkindexer rewrite scope (fundamental change to how indexing works)
- Elasticsearch cluster sizing and shard allocation

This needs benchmarking with production-scale data before committing to either approach. It belongs in the greenfield roadmap or as a standalone spike, not in the beta proposal.

---

## Unified Architecture

The diagram below shows how the readiness layer fits into the existing orchestrator. The layer is deliberately thin: a status store (DynamoDB or a local JSON file), a small API for the one-click links, and a pre-flight check in the orchestrator that consults the store before starting each hub.

```
                    ┌──────────────────────────────────────────────────────┐
                    │                   i3.conf                            │
                    │  Per-hub: readiness.review, review_window_hours     │
                    │  (that's the only per-hub config needed)            │
                    └──────────────────────────┬───────────────────────────┘
                                               │
                    ┌──────────────────────────▼───────────────────────────┐
                    │              Hub Readiness Layer                      │
                    │                                                       │
  Scheduling email ─┤  Status Store (DynamoDB or local JSON)               │
  with "Ready" btn  │                                                       │
                    │  ┌─────────────────────────────────────┐             │
  Hub clicks       ─┤  │ { hub: "florida",                   │             │
  "Ready" or       │  │   month: "2026-02",                 │             │
  "Skip"           │  │   status: "ready",                  │             │
                    │  │   source: "link_click",             │             │
  S3 deposit       ─┤  │   updated_at: "2026-02-23T14:30Z",  │             │
  detected          │  │   confirmed_by: "contact@fsu.edu" } │             │
  (file-based hubs) │  └─────────────────────────────────────┘             │
                    │                                                       │
  Post-ingest      ─┤  Status API                                          │
  approve/reject    │  POST /go/{hub}/{month}                              │
                    │  POST /skip/{hub}/{month}                             │
  Quality gate     ─┤  POST /review/{hub}/{month}/approve                  │
  results           │  POST /review/{hub}/{month}/reject                   │
                    │  GET  /status/{hub}/{month}                           │
                    │  GET  /readiness/{month}  (all hubs)                 │
                    │                                                       │
                    └──────────────────────────┬───────────────────────────┘
                                               │
                    ┌──────────────────────────▼───────────────────────────┐
                    │                   Orchestrator                        │
                    │                                                       │
                    │  for hub in scheduled_hubs(month):                   │
                    │    status = readiness_store.get(hub)                 │
                    │                                                       │
                    │    if status == "ready" or "deposited":             │
                    │      → run pipeline                                 │
                    │      → run quality gates                            │
                    │      → if hub.review == false: auto-sync            │
                    │      → if hub.review == true:  send review email,   │
                    │                                 wait for approval   │
                    │    elif status == "skip":                           │
                    │      → log, move on                                 │
                    │    elif no response by window close:                │
                    │      → auto-skip, notify staff                     │
                    │                                                       │
                    └─────────────────────────────────────────────────────┘
```

### Orchestrator Integration

```python
def get_ready_hubs(config, month):
    """Get hubs that are both scheduled and ready to ingest."""
    scheduled = config.get_scheduled_hubs(month)
    readiness = fetch_readiness_status(month)
    s3_deposits = check_s3_deposits(config, month)

    ready, waiting, skipped = [], [], []
    for hub_name in scheduled:
        status = readiness.get(hub_name, {}).get("status")
        s3 = s3_deposits.get(hub_name, {}).get("status")

        if status == "ready" or s3 == "deposited":
            ready.append(hub_name)
        elif status == "skip":
            skipped.append((hub_name, readiness[hub_name].get("notes", "")))
        else:
            waiting.append(hub_name)

    return ready, waiting, skipped
```

---

## Beta Proposal: Implementation Plan

The implementation is designed to be incremental. Phase A works entirely within the existing codebase with no new AWS infrastructure. Phase B adds the hub-facing API and email enhancements. Phase C wires everything into the orchestrator.

### What to Build

| # | Component | Effort | Dependencies | Delivers |
|---|-----------|--------|-------------|----------|
| 1 | **i3.conf readiness properties** | 2 hours | None | Add `readiness.review` and `readiness.review_window_hours` parsing to `config.py`. Default: `review=false`. |
| 2 | **S3 deposit polling** | 4 hours | None | Pre-flight check for file-based hubs. Eliminates manual bucket checking. |
| 3 | **Local readiness file** | 2 hours | None | `hub-readiness-{month}.json` for manual status tracking. Bridge until the API is built. |
| 4 | **Hub status API** | 2-3 days | None | API Gateway + Lambda + DynamoDB. Handles `/go`, `/skip`, `/review` endpoints. Returns confirmation pages. |
| 5 | **Per-hub scheduling emails** | 4 hours | #4 | One email per hub with "Ready" and "Skip" links. Token-authenticated. Replaces current batch email. |
| 6 | **Enhance mapping summary email** | 4 hours | #1, #4 | For `review=true` hubs, prepend approve/reject block to existing `Emailer.scala` output. Existing email content unchanged. |
| 7 | **Orchestrator readiness integration** | 1 day | #1, #2, #3 or #4 | `get_ready_hubs()` function. Orchestrator waits for "ready" signal before starting each hub. Holds sync for review hubs until approved. |
| 8 | **Quality gates** | 2-3 days | #7 | Record count anomaly, schema drift, sample diff. Run between pipeline and sync. |
| 9 | **Hub-level S3 purge script** | 4 hours | None | `revoke_hub` command for post-sync cleanup. |
| 10 | **Slack notifications** | 4 hours | #4 | Hub clicks "Ready" → Slack. Hub clicks "Skip" → Slack. Quality gate failures → Slack. Review approve/reject → Slack. |

### Suggested Build Order

```
Phase A: Foundation (Week 1)
  ├─ #1  i3.conf readiness properties
  ├─ #2  S3 deposit polling
  ├─ #3  Local readiness file (interim bridge)
  └─ #9  Hub-level S3 purge script

Phase B: Hub Status API + Emails (Week 2-3)
  ├─ #4  API Gateway + Lambda + DynamoDB
  ├─ #5  Per-hub scheduling emails with "Ready" button
  ├─ #6  Enhance existing mapping summary email (add review block)
  └─ #10 Slack notifications

Phase C: Orchestrator Integration (Week 3-4)
  ├─ #7  Orchestrator readiness integration
  └─ #8  Quality gates
```

Phase A requires no new AWS infrastructure -- only Python additions to the existing orchestrator. Phase B introduces the hub-facing API and modifies the existing scheduling and summary emails; the summary email enhancement is a small change to `Emailer.scala` that conditionally prepends a review block. The existing `_SUMMARY` content, ZIP attachment, and CC to `tech@dp.la` are untouched. Phase C wires everything together: the orchestrator consults the readiness layer before starting each hub and holds S3 sync for review-enabled hubs until approval arrives.

### Cost

| Component | Monthly Cost | Notes |
|-----------|-------------|-------|
| DynamoDB (readiness store) | ~$1 | Minimal read/write volume |
| API Gateway + Lambda (status API) | ~$1 | <1000 requests/month |
| SES (per-hub scheduling emails) | ~$1 | Already in use for ingest emails |
| **Total** | **~$3/month** | |

### What's Deferred to Greenfield

| Component | Why Deferred |
|-----------|-------------|
| SES email parsing with LLM classification | Nice-to-have for catching old-habit email replies. One-click links are the primary channel. |
| Hub dashboard (web UI) | Nice-to-have but not essential when one-click links cover the core workflow. |
| S3 event-driven triggers (replace polling) | Polling is sufficient at this scale. Event-driven is a greenfield optimization. |
| Hub-partitioned indexing | Separate technical investigation (see above). |
| Tennessee set list web form | Can use the simple form in the status API. Deferred only if API is deferred. |
| Detailed report web view | The `/report/{hub}/{month}` link in the review email could show an interactive view of the mapping results. For now, the `_SUMMARY` content and ZIP attachment in the email itself are sufficient. |

---

## Hub Onboarding

The rollout email to hubs is designed to be brief, clear, and non-threatening. The only decision a hub needs to make is whether they want the optional review step.

> Hi [Hub Contact],
>
> We're improving our ingest scheduling to give you more control over when your data gets harvested.
>
> **What's changing:** Starting [month], you'll receive a monthly email with a "Ready -- start my ingest" button. Click it when your data is ready, and we'll start your harvest. If you need to skip a month, there's a "Skip" button too. If we don't hear from you by the end of the ingest window, we'll automatically skip that month.
>
> **Optional: review before publishing.** If you'd like to see a summary of your harvest and approve it before the data goes live on dp.la, let us know and we'll turn that on for your hub. You'll get an email after each harvest with record counts and a one-click approve/reject.
>
> **No action needed right now.** Your first "Ready" button email will arrive at the start of the [month] ingest window.
>
> Questions? Reply to this email or contact tech@dp.la.

No action is required from the hub before the first "Ready" button email arrives.

---

## Risks and Mitigations

The risks below are the most likely failure modes. Each has a straightforward mitigation.

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hub clicks "Skip" by accident | Hub skipped erroneously | Confirmation page with undo link. Staff can override. |
| Hub never clicks "Ready" (forgot, email lost) | Hub not ingested | Slack alert to staff 3 days before window closes: "Florida hasn't responded." Reminder email to hub at same time. |
| One-click link tokens leaked | Unauthorized status changes | Tokens are per-hub, per-month, time-scoped. Low stakes (worst case: premature trigger or erroneous skip, both easily noticed). |
| S3 deposit detected but data is incomplete | Ingest starts on partial data | Completeness check: require minimum object count or marker file before marking "deposited." |
| Quality gates too sensitive | Constant false alarms, fatigue | Tune thresholds per hub via i3.conf overrides. Start conservative, relax as confidence grows. |
| Hub-trigger adds delay to cycle | Cycle takes longer if hubs are slow to respond | Most hubs will click within 24 hours. Window close + auto-skip prevents indefinite waiting. S3-watch auto-triggers file-based hubs. |

---

## Summary

This proposal standardizes the monthly ingest cycle around one interaction: **every hub gets a "Ready" button.** The hub clicks it when their data is ready, and the pipeline starts. File-based hubs additionally get automatic S3 monitoring as a second signal.

The only per-hub configuration is a single optional flag: **review before publishing.** For hubs that opt in, the existing mapping summary email -- which every hub already receives with record counts, error breakdowns, and log files -- gains approve/reject links at the top. The hub reviews the same data they've always reviewed, but now they can explicitly approve or reject before the data syncs to S3 and enters the search index. For hubs that don't opt in, the mapping summary email is unchanged from today, and data auto-syncs after quality gates pass.

All configuration lives in `i3.conf` (two properties per hub). The readiness layer is a thin status store (DynamoDB or local JSON) with a small API for the one-click links. The orchestrator consults the readiness layer before starting each hub.

**Implementation:** ~4 weeks, ~$3/month infrastructure. Phase A (foundation) is pure Python within the existing codebase. Phase B adds the hub-facing API. Phase C wires it into the orchestrator.

**Separate technical investigation:** Hub-partitioned indexing (spanning alias or delete-by-query) for surgical hub/record removal from the search index. Particularly relevant for CDL's frequent item-level delete requests. Deferred pending benchmarking.
