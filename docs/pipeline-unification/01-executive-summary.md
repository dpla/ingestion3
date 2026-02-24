# DPLA Pipeline Unification -- Executive Summary

**Audience:** Director, Community Manager, funders, leadership at receiving organization

**Reading time:** 15 minutes

---

## Contents

- [DPLA's Mission and What the Pipeline Enables](#dplas-mission-and-what-the-pipeline-enables)
- [What Does DPLA's Data Pipeline Do?](#what-does-dplas-data-pipeline-do)
- [What the Current Implementation Costs](#what-the-current-implementation-costs)
  - [Silent Failures](#silent-failures)
  - [Organizational Risk](#organizational-risk)
  - [Partner Coordination Overhead](#partner-coordination-overhead)
  - [The Cost Is Not the System](#the-cost-is-not-the-system)
- [What Is Being Proposed?](#what-is-being-proposed)
  - [Orchestration, Not Replacement](#orchestration-not-replacement)
  - [Strategic Principles](#strategic-principles)
  - [What Changes for Non-Technical Staff](#what-changes-for-non-technical-staff)
- [What External Audiences Should Know](#what-external-audiences-should-know)
  - [For Funders and Board Members](#for-funders-and-board-members)
  - [For Peer Organizations and Potential Receiving Organizations](#for-peer-organizations-and-potential-receiving-organizations)
- [Timeline and Phases](#timeline-and-phases)
  - [Phase 0: Integration Contracts](#phase-0-integration-contracts-week-1)
  - [Phase 1: Reliability Fixes](#phase-1-reliability-fixes-weeks-1--2)
  - [Phase 2: Monitoring and Observability](#phase-2-monitoring-and-observability-weeks-2--3)
  - [Phase 3: Controlled Indexer Automation](#phase-3-controlled-indexer-automation-weeks-3--4)
  - [Phase 4: End-to-End Coordination](#phase-4-end-to-end-coordination-week-4)
- [Hub Communication and Scheduling](#hub-communication-and-scheduling)
- [Risk Summary](#risk-summary)
- [Cost Implications](#cost-implications)
- [Documented Procedures and Coding Assistants](#documented-procedures-and-coding-assistants)
- [Residual Risk After Implementation](#residual-risk-after-implementation)
- [Success Criteria](#success-criteria)
- [Glossary](#glossary)

---

## DPLA's Mission and What the Pipeline Enables

DPLA -- the Digital Public Library of America -- makes the cultural heritage held by America's libraries, archives, and museums freely available to everyone. Through dp.la, anyone can discover and explore tens of millions of records -- photographs, manuscripts, maps, audio recordings, and much more -- contributed by approximately 60 partner institutions across the country. These partners range from large research universities and national repositories to state historical societies and specialized archives. DPLA does not own these items. It aggregates the metadata that describes them and makes that metadata searchable in one place.

The data pipeline is the machinery that makes this possible. It is the infrastructure that moves records from partner institutions into dp.la's search index each month, uploads eligible media to Wikimedia Commons for Wikipedia and other Wikimedia projects, and keeps the whole system current. This document describes what the pipeline is, what it costs to operate today, and what is being proposed to make it more reliable, more visible, and easier to hand off.

---

## What Does DPLA's Data Pipeline Do?

The pipeline has three stages, each handled by a separate software project:

**Stage 1 -- Ingestion.** Once a month, DPLA reaches out to partner hubs and pulls in their latest records. Some hubs provide data through a standard library protocol (OAI-PMH), some through APIs, and some through files placed on Amazon S3. The system transforms each record into a common format, checks it for quality, and stores the result. This takes about a week each month.

**Stage 2 -- Indexing.** After ingestion, DPLA takes all processed records and builds a search index -- the database that powers dp.la's search. This runs on a temporary cluster of 10 servers on Amazon Web Services (AWS), takes about seven hours, and is then shut down. When the new index is ready, a "switch" is flipped so the website starts serving the fresh data. This switch is the single most consequential moment in the monthly cycle: it determines what millions of users see on dp.la. Older indicies need to manually deleted to preserve disk space on the seach cluster. We maintain at least one historical index at all timess.

**Stage 3 -- Wikimedia uploads.** For records with openly-licensed media -- images, documents, maps -- DPLA downloads the media from the source institution and uploads it to Wikimedia Commons, making it available on Wikipedia and other Wikimedia projects. This is a continuous process that runs for weeks or months at a time on a dedicated server.

---

## What the Current Implementation Costs

Today, these three stages run independently with no coordination between them. Each one runs on a different computer, is written in a different programming language, is started manually by an engineer, and has no way to signal to the others that it has finished. The pipeline works -- it has run monthly for years -- but it carries real operational costs that grow more significant as the team and resource base remain small.

### Silent Failures

The pipeline's largest operational risk is not that individual components fail. It is that failures go undetected. When a stage breaks down and no one knows, the consequences compound over time and become much more expensive to unwind than if the failure had been caught immediately.

### Organizational Risk

Today, the complete picture of how to run the monthly cycle -- which hubs to process, in what order, what to do when something fails, how to flip the search index, how to manage the Wikimedia uploads -- is held primarily by one engineer. If that engineer is unavailable, the monthly cycle stops.

This is not a failure of documentation or process. It is the predictable result of a small team building and operating sophisticated infrastructure over time. The knowledge concentration is a structural risk. This document set is intended to directly address it: by writing down what that engineer knows, packaging the procedures so others can follow them, and proposing changes that make the pipeline more self-explanatory.

### Partner Coordination Overhead

Every month, DPLA sends scheduling emails to hub contacts, waits for replies, and makes judgment calls about which hubs are ready to be ingested. Because there is no structured way to confirm hub readiness, DPLA occasionally ingests data that was not ready -- and then must delete it and re-run the affected portion of the pipeline. Each of these post-ingest revocations costs 5--8 hours of engineering time. The coordination overhead of answering status questions from hub contacts ("when was our data last updated?") adds further steady drain on capacity.

In addition, a hub can request specific records to be removed from our search index. This is a two-step process, frist to delete the record from the live search index and to delete the same record from the data in S3 so it is not re-introduced when the index is rebuilt. These are manual steps for an engineer and it requires knowing to perform both steps to correctly handle a request. A final factor to consider is that these requests typically come as a list of URLs or a web query. Engineers need the DPLA IDs to perform these operations so in addition to the actual implementation engineers need to manually gather the IDs when not provided by a hub.

### The Cost Is Not the System

These costs are the natural accumulation of solving real problems incrementally, under resource constraints, over time. The pipeline was built to serve the mission, and it has. What is being proposed here is not a condemnation of the current implementation -- it is a set of targeted investments that reduce the operational burden and make the system more durable.

---

## What Is Being Proposed?

### Orchestration, Not Replacement

The three existing systems work. Replacing them would be expensive and risky. Instead, DPLA is proposing to:

1. **Formalize contracts** between the three systems -- write down the rules for how they communicate, so changes in one do not silently break another.
2. **Add safety gates** at every critical transition -- especially the search index switch, which is the single highest-risk moment in the monthly cycle.
3. **Install monitoring** so that failures are detected in minutes, not months.
4. **Harden** each system to handle failures more gracefully: crash recovery, session management, structured error logging.
5. **Coordinate** the three stages so they hand off work automatically, with human approval at high-stakes decision points.
6. **Document procedures** so routine operations can be run by a human or by a coding assistant following the same steps, reducing the dependency on any single engineer's knowledge.

### Strategic Principles

Three principles guide the sequencing of this work:

1. **Optimize handoff correctness before throughput.** Get the data flowing reliably between stages before trying to make it faster.
2. **Optimize observability before deeper automation.** See what's happening before automating decisions about what to do.
3. **Optimize restartability and rollback before autonomy.** Make every automated action reversible before removing human oversight.

This ordering reduces risk faster than broad feature expansion.

### What Changes for Non-Technical Staff

- **Community Manager:** You will receive a daily Slack message summarizing Wikimedia upload progress -- how many files were uploaded, which partners are active, whether any uploads are stalled. Today this information requires SSH access to a server. After the change, it arrives in Slack every morning.

- **Director:** You will have a single document suite (this one) that explains the entire pipeline. When the pipeline is handed off, the receiving organization reads these documents to understand what they are getting and how to operate it.

- **Everyone:** When something fails, the system will post an alert to Slack with a plain-English description of what went wrong and what to do about it. Today, failures can go undetected for weeks.

---

## What External Audiences Should Know

### Anmyone who will take ownership of this program

The data pipeline is DPLA's core infrastructure -- the system that moves partner collections into dp.la's search index and keeps them current. Without it running reliably each month, DPLA cannot deliver on its mission. Investment in this proposal yields:

- **Reduced engineering cost to operate.** The monthly cycle currently requires 15-25 hours of engineering attention. The proposed monitoring and coordination work reduces this significantly, freeing engineering capacity for higher-value work.
- **Faster failure detection.** Failures that today go undetected for weeks will be detected in minutes, reducing the compounding cost of unresolved problems.
- **Knowledge transfer that sticks.** The documentation suite and procedures in this proposal are designed to survive engineer turnover. The next team does not start from scratch.
- **Infrastructure proportionate to the mission.** DPLA's aggregation role requires a pipeline that partner institutions, funders, and the public can trust. This proposal makes that trust grounded in observable, documented reliability.

### For Peer Organizations and Potential Receiving Organizations

DPLA's pipeline aggregates records from ~60 partner institutions monthly, builds a search index serving millions of queries, and uploads media to Wikimedia Commons continuously. Operationally, this means:

- **Monthly engineering cycle:** ~15--25 hours of active engineering time to run ingestion, indexing, and Wikimedia coordination, currently on a single engineer's laptop and two cloud servers.
- **Documented handoff path:** This document set is the handoff package. The [Implementation Roadmap](04-implementation-roadmap.md) provides a phased plan; the [Technical Findings](05-technical-findings.md) are the work queue; the [Agent Skills and Automation](06-agent-skills-and-automation.md) are the operating manual.
- **AWS cost:** Approximately $150--250/month in current state (S3 storage, EMR indexing, EC2 for Wikimedia). Moving ingestion from a laptop to a cloud server adds ~$100--150/month.
- **Minimum viable team:** One engineer can operate the pipeline following the documented procedures. Two engineers are needed to make improvements while keeping operations running.

---

## Timeline and Phases

The work is organized into five phases, each delivering value independently. If a phase is delayed or cancelled, earlier phases remain intact. Each phase has explicit exit criteria -- completion is measured by verifiable outcomes, not by "feature shipped" status.

### Phase 0: Integration Contracts (Week 1)

Write down the rules for how the three systems communicate: what file formats they expect, what names they use for hubs, where data lives in S3. Today these rules exist only in code. Writing them down means anyone can verify that the systems are compatible.

**What you'll see:** A reference document. No system changes.

### Phase 1: Reliability Fixes (Weeks 1--2)

Fix the most dangerous failure patterns in each system:
- Make the ingestion system's progress tracking crash-safe.
- Make the indexer report failures instead of silently continuing.
- Give the Wikimedia uploader the ability to resume after a crash instead of starting over.

**What you'll see:** No visible change in normal operation. Fewer silent failures.

### Phase 2: Monitoring and Observability (Weeks 2--3)

Install monitoring on the Wikimedia server. Set up the daily Slack digest. Replace manual terminal sessions with service management that automatically restarts crashed processes. Add health checks across all systems.

**What you'll see:** Daily Slack messages with upload progress. Alerts when something stalls. This is the most visible deliverable for non-engineering staff.

### Phase 3: Controlled Indexer Automation (Weeks 3--4)

Automate the search index build and the production switch-over, with mandatory safety checks. The system validates the new index (correct record count, correct fields, cluster health) before switching. If validation fails, it stops and asks a human. If the switch goes wrong, it automatically switches back.

**What you'll see:** The monthly index build requires less manual work. Slack notifications for indexing progress. Explicit human approval required for the first several cycles.

### Phase 4: End-to-End Coordination (Week 4+)

Connect all three stages so that completing one automatically triggers the next. After ingestion finishes, indexing starts. After indexing finishes, Wikimedia uploads refresh their target lists. Human approval gates remain at high-stakes transitions.

**What you'll see:** The monthly cycle runs with minimal human intervention. A single command kicks off the entire process.

---

## Hub Communication and Scheduling

A significant source of operational friction is the monthly communication cycle with hub contacts. Today, DPLA sends a batch scheduling email and then waits for hubs to flag issues by reply. There is no structured way to know whether a hub is ready, and no automated monitoring of file-based hub deposits. Hubs sometimes confirm they're ready, get ingested, and then discover a problem -- requiring DPLA to delete data and re-run parts of the pipeline at significant engineering cost.

A beta proposal for a **Hub Readiness and Scheduling Layer** is documented in the [Hub Readiness and Scheduling Layer](08-hub-readiness-automation.md). The core idea:

- **Every hub receives a monthly email with a "Ready -- start my ingest" button.** The pipeline does not start for a hub until they click it, replacing the current model where DPLA guesses when hubs are ready.
- **File-based hubs get automatic S3 deposit monitoring** -- the orchestrator watches their S3 bucket and starts the ingest when new data appears, eliminating manual checking.
- **Hubs can optionally opt into "review before publish"** -- the existing mapping summary email gains approve/reject links so hubs can block publication if they spot a problem before data enters the search index.

This addresses three costs: the engineering time spent manually coordinating schedules, wasted compute from ingesting data that was not ready, and the 5--8 hours of rework per post-ingest revocation. The proposal is lightweight (approximately $3/month in infrastructure, approximately 4 weeks to implement) and designed to integrate into both the current system and a future rebuild.

---

## Risk Summary

| Risk | Severity | Addressed In |
|------|----------|--------------|
| Silent failures go undetected for weeks or months | Critical | Phases 1 and 2 |
| Search index switch points to incomplete data | Critical | Phase 3 (safety gates) |
| Cross-repo hub naming drift breaks data routing | Critical | Phase 0 (contracts) |
| Single engineer dependency | High | This documentation suite |
| Manual stage handoffs depend on operator memory | High | Phases 3 and 4 |
| Hub communication is unstructured; readiness is guessed | High | [Hub Readiness Layer](08-hub-readiness-automation.md) |
| Post-ingest revocations require multi-hour rework | High | [Hub Readiness Layer](08-hub-readiness-automation.md) |
| Wikimedia server dies with no rebuild procedure | Medium | Phase 2 (documented rebuild) |
| Cloud server costs run higher than expected | Medium | Phase 3 (monitoring, auto-shutdown) |
| Automation introduces new failure modes | Medium | Phased rollout with explicit gates |

### Risks Introduced by Unification Itself

Automating a fragile pipeline carries its own risks:

- **False confidence from automating weak assumptions.** If the automation encodes today's implicit rules, it can fail in the same ways -- just faster and with less human oversight.
- **Race conditions at stage boundaries.** Automated handoffs must handle the edge cases that humans handle intuitively (for example, "not all hubs are done yet").
- **Over-centralizing control logic.** If the coordination layer breaks, manual operation must remain possible as a fallback.

These are manageable when the rollout is gate-driven and every automation has a manual override -- which is why each phase has explicit exit criteria.

---

## Cost Implications

| Service | Current Monthly Cost | Human Cost Equivalent |
|---------|---------------------|----------------------|
| AWS EMR (indexing) | ~$20--60 | ~1 hour to launch, monitor, and verify |
| AWS EC2 (Wikimedia) | ~$30--50 | ~2--3 hours/month to check, restart, manage |
| AWS S3 (data storage) | ~$50--100 | Minimal; largely automated today |
| Slack notifications | Free | -- |
| Local machine (ingestion) | $0 (engineer's laptop) | ~6--8 hours/month to run the full cycle |

The proposed changes do not significantly increase AWS costs in Phases 0--2. Phase 3 adds minor Lambda and CloudWatch costs (under $10/month). Moving the ingestion orchestrator from a laptop to a cloud server -- recommended for handoff but not required for initial implementation -- adds approximately $100--150/month and removes the dependency on any individual engineer's machine.

---

## Documented Procedures and Coding Assistants

The pipeline includes **documented procedures** (called "skills") that a human or a **coding assistant** can follow. When engineering capacity is limited, these procedures allow the receiving organization to operate the pipeline without needing deep expertise in the codebase. An engineer -- or a coding assistant following the same steps -- can run the ingest for a given hub: look up configuration, choose the correct procedure, run the scripts in order, and report the result.

Today, the ingestion stage has these procedures. The indexer and Wikimedia systems do not yet; part of this plan is to create them so the full monthly cycle can be run from documented procedures. Well-written procedures serve as both readable documentation for humans and executable steps for a coding assistant. Details, in-repo locations, and links to external documentation are in [Agent Skills and Automation](06-agent-skills-and-automation.md).

---

## Residual Risk After Implementation

Even after all phases are complete, some risks cannot be eliminated -- only detected and contained:

- **Source partner endpoint instability.** DPLA depends on ~60 external institutions maintaining their data feeds. When a partner's server goes down or changes format, the pipeline detects the failure but cannot fix it.
- **Upstream metadata variance.** Partners change their metadata schemas over time. The pipeline's mapping layer handles known variations, but novel changes require human investigation.
- **Prolonged partial-degradation scenarios.** When a problem affects a subset of records for a hub, the decision to proceed with a partial ingest  vs. wait for all remediations is a  judgment that cannot be fully automated and requires communication with the affected hub.

The pipeline is designed with these constraints in mind. The focus is fast detection and clear escalation, not automatic resolution of problems that require human judgment.

---

## Success Criteria

The unification is successful when:

- The monthly ingest-to-index-to-refresh cycle runs with deterministic handoff logic.
- Every critical transition has an explicit pass/fail gate.
- Failure modes are visible quickly to operations staff and stakeholders.
- Manual fallback remains available at every high-impact decision point.
- The pipeline can be operated by someone who did not build it.

---

## Glossary

| Term | Definition |
|------|------------|
| **Alias flip** | Switching the production search to point at a newly-built index. Atomic -- it either succeeds completely or not at all. |
| **AWS (Amazon Web Services)** | Cloud computing platform. DPLA uses it for storage (S3), temporary compute clusters (EMR), and a dedicated server (EC2). |
| **Coding assistant** | A tool (e.g., in a code editor or terminal) that reads written procedures and carries out tasks by following the steps. Used for pipeline operation when engineering resources are constrained. |
| **Contract** | A formal agreement between two systems about the format, location, and meaning of shared data. Breaking a contract breaks the pipeline. |
| **EC2** | A virtual server rented from AWS. DPLA has one dedicated EC2 instance for Wikimedia uploads. |
| **Elasticsearch** | The search database that powers dp.la. Records are stored in an "index" and queried through an API. |
| **EMR (Elastic MapReduce)** | An AWS service that creates a temporary cluster of servers for large data processing jobs. DPLA uses it to build the search index. |
| **Gate** | A mandatory checkpoint where the system verifies preconditions before proceeding. If the check fails, the system stops and asks for human input. |
| **Hub** | A partner institution that contributes records to DPLA. Examples: HathiTrust, National Archives (NARA), Digital Library of Georgia. |
| **JSONL** | A file format where each line is a separate JSON record. The standard interchange format between ingestion and indexing. |
| **OAI-PMH** | A protocol used by libraries and archives to share their catalog records. Most DPLA hubs use OAI-PMH. |
| **Orchestrator** | A program that coordinates other programs -- starting them in order, watching for failures, and deciding what to do next. |
| **S3** | Amazon's cloud storage service. DPLA stores all processed records in S3 buckets (named storage containers). |
| **Safety gate** | See "Gate." Used interchangeably throughout this document suite. |
| **Skill** | A written procedure (e.g., a `SKILL.md` file) that describes how to perform a specific task. It can be followed by a human or by a coding assistant; it serves as both documentation and executable instructions. |
| **Slack webhook** | A URL that accepts messages and posts them to a Slack channel. Programs use webhooks to send notifications automatically. |
| **Wikimedia Commons** | A media repository used by Wikipedia and other Wikimedia projects. DPLA uploads openly-licensed images and documents there. |
