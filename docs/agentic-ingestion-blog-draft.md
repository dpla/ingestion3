# Making Metadata Ingestion More Accessible: AI-Assisted Workflows at DPLA

**Draft for review and refinement, February 2026**

---

## Introduction

DPLA's cultural heritage aggregation program ingests metadata from more than 50 hubs (libraries, archives, and museums) across the country. Each ingest can run for many hours, and some workflows involve multiple steps, specialized configurations, and careful handling of large datasets. For years, running and troubleshooting these processes required deep familiarity with the pipeline and its tools.

We've been exploring a different approach: pairing structured documentation with AI coding assistants so that team members can perform every step of the monthly ingestion process, from scheduling and running ingests to debugging failures, using plain language. The tools we've used include [Cursor](https://cursor.com) (an AI-powered IDE), [Claude Code](https://docs.anthropic.com/en/docs/claude-code/quickstart) and [OpenAI Codex](https://developers.openai.com/codex/cli) (terminal-based coding agents), and Anthropic's [Claude Opus](https://www.anthropic.com/claude/opus) as the underlying model. In this post, we share what we've learned and how it's changing the way we work.

---

## The Challenge

Metadata ingestion at scale is inherently complex. Different hubs provide data in different ways, some via APIs, others through file exports or feeds, and each workflow has its own requirements. Some involve preprocessing steps before harvest; others require merging updates into an existing dataset. When something goes wrong, diagnosing the issue traditionally meant knowing where to look in logs, which commands to re-run, and how to notify the right people.

While we've strived to document those processes in Confluence and GitHub, we wanted to make it easier for anyone to keep the pipeline moving without needing to memorize commands or dig through technical documentation.

---

## What We Built

We focused on three things: better documentation, clearer workflows, and smarter automation.

**Documentation that works for humans and AI.** We created a single guide that explains how to run ingests, when to escalate failures, and where to look for help. That guide is written so both team members and AI assistants can follow it. We use [Cursor Rules](https://docs.cursor.com/context/rules) and [Claude Skills](https://docs.anthropic.com/en/docs/claude-code/skills) to give each assistant persistent context about our project: environment setup, script conventions, and notification policies. These lightweight configuration files ensure that when someone asks an assistant to "run the ingest for Hub X" or "debug the failure," the assistant starts from the same instructions every time.

**Specialized workflows for common tasks.** We defined small, focused workflows (as Cursor Skills and Claude Skills) for the tasks we do most often: running an ingest for a specific hub, debugging a failure, verifying outputs, and notifying the team when something goes wrong. Each workflow encodes the steps that used to live in people's heads: which procedure to use, what to check, and how to report results. That consistency matters: the assistant follows the same checklist whether you're working in Cursor, running Claude Code from the terminal, or using Codex.

**Automation that keeps everyone informed.** Our pipeline orchestrator runs multiple hubs in parallel and sends updates as each stage completes. When something fails, it writes a clear summary and ensures the right people are notified. The documentation and workflows we added align with this: they describe when and how to escalate, so nothing slips through the cracks.

---

## What It Looks Like in Practice

Today, a team member can open Cursor, Claude Code, or Codex and say something like "harvest Indiana" or "run the ingest for Maryland." The assistant starts the process and sets up monitoring in the background. If the harvest runs for hours and then fails, because a particular data set returned an invalid response for example, the system detects the failure, scans the logs, and surfaces a clear diagnosis: what went wrong, where it failed, and suggested next steps. That might mean contacting the hub, excluding the problematic set and retrying, or continuing with a partial harvest and following up later. What used to be a manual debugging session (digging through logs, guessing at causes, deciding who to contact) becomes a guided workflow. Even non-developers can understand the diagnosis and take action.

**Anomaly detection that used to rely on human judgment.** We've also implemented automated anomaly detection with Slack notifications. In the past, spotting when an ingest had gone badly wrong (sudden spikes in record failures, harvest counts that were far too low, or other red flags) depended on someone's familiarity with what "looks about right." That context was valuable but hard to scale. We now run checks before syncing data and alert the team when something doesn't add up. The system compares current results to baselines and flags critical changes, so we catch problems before bad data reaches our users. Slack notifications keep everyone informed without requiring anyone to watch the pipeline manually.

We're not replacing the underlying pipeline. We're making it easier to use and easier to trust. The complexity is still there, but it's documented and encapsulated. That allows us to hand off more of this work to people who can describe what they need in plain language rather than remembering commands and configuration details.

---

## What It Cost, and What We Gained

This work took time. We had to review the existing documentation, find gaps, write new documentation and process instructions for our agents, define and keep workflows in sync across tools, and learn how to correct things when they go wrong. We also had to accept that the assistant isn't perfect; it can misread context or suggest the wrong fix. The workflows help reduce that risk, but humans still need to review critical steps. However, the agents and skills make that step infinitely easier.

The benefits have been real. We're spending less developer time on routine runs and retries. Colleagues who aren't developers can now run ingests and trigger debug workflows by describing what they need. Onboarding is easier: new team members can learn by asking the assistant to run something and explain the steps. And because everyone, human and assistant alike, follows the same procedures, we have fewer inconsistencies and less reliance on tribal knowledge.

---

## Looking Ahead

We see this as part of a broader shift: treating documentation as something that serves both humans and AI, and designing workflows so that more people can participate in running and maintaining our infrastructure. We're also exploring ways to surface status and progress on demand so stakeholders can get answers without running scripts, and we expect to keep refining these tools as we learn what works.

---

## For Cultural Heritage Technologists

If you're considering similar approaches, a few lessons stand out:

- **Documentation is infrastructure.** A single, well-maintained guide that both humans and AI use can become your source of truth. Keep it up to date.
- **Start small.** We began with a few workflows and expanded from there. You don't need to automate everything at once.
- **Encapsulate complexity.** Put the hard parts in scripts and docs. Let the assistant call them instead of reinventing them.
- **Plan for handoff.** The goal isn't just to speed up developers, it's to let more people run and debug workflows safely. Design with that in mind.
- **Accept short-term cost for long-term gain.** Writing and maintaining these workflows takes time, but it pays off in reduced load and better resilience.

---

*This draft was generated from DPLA's ingestion documentation and can be refined over time. For questions or feedback, we encourage you to get in touch with us.*
