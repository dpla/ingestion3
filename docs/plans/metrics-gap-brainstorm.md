# Metrics Gap: Brainstorm Notes

**Type:** Working notes / meeting prep
**Status:** Draft -- not for distribution
**Purpose:** Capture thoughts on the absence of pipeline metrics and the challenges this creates before raising it in conversation. Not a formal proposal.

---

## The Core Problem

DPLA's ingest pipeline has no KPIs, no SLOs, and no SLIs. We have SLAs with partners in the broad sense -- an expectation that ingests happen monthly, that data appears on dp.la -- but nothing that defines success in measurable terms, and nothing that makes our operational investment visible in a way that could inform budget or staffing decisions.

This is not a criticism of anyone. It is the predictable outcome of a team of one or two engineers building and operating a complex system under time pressure. When you are the only person running the pipeline, you carry the metrics in your head. You know roughly how long things take, roughly what breaks, roughly what each month costs in attention. That knowledge is real -- it just does not exist anywhere that an executive or a funder or a new team member can see.

The problem surfaces when you try to argue for changes. "This optimization would save engineering time" is hard to defend when you have no baseline for how much engineering time the current process costs. "This would reduce partner friction" is intuitive but impossible to quantify when partner coordination happens in email threads that no one is counting. "The AWS bill would go down" is sometimes true, but the AWS bill is not the whole picture and may not even be the relevant number.

---

## What We Don't Have

### No baseline time measurements

- How many hours does it actually take to run a monthly cycle, end to end?
- How many of those hours are active engineering work vs. waiting?
- How many hours per month are spent responding to status inquiries from hub contacts?
- How many hours per month are spent on post-ingest revocations?
- How many hours are lost to diagnosing silent failures that went undetected?

We have intuitions about all of these. We do not have numbers.

### No throughput or reliability metrics

- What percentage of scheduled hubs complete successfully on the first attempt?
- What is the average time from ingest start to data appearing on dp.la?
- How often does the pipeline require manual intervention?
- What is the mean time to detect a failure? Mean time to recover?

### No partner experience metrics

- How many status inquiry emails do we receive per month?
- What percentage of hubs revoke or request re-ingestion?
- How long does the average revocation cycle take to resolve?
- How many hubs skip months and why?

### No cost attribution

The AWS bill is visible but incomplete. It does not capture:
- Engineer hours (not tracked)
- Partner coordination time (not tracked)
- Time lost to context-switching when a pipeline alert interrupts other work (not tracked)
- Opportunity cost of work not done because the pipeline needed attention (definitely not tracked)

A proposed change might increase the AWS bill by $30/month and save 8 hours of engineering time. Whether that is a good trade depends on how you value engineering hours -- and that depends on whether engineering hours are even acknowledged as a cost. At a small nonprofit, staff time is often treated as "free" until it isn't.

---

## Why This Is Hard to Fix

### Tracking takes time too

With a staff of one or two, any time spent tracking metrics is time not spent on the pipeline. A formal time-tracking system would require consistent discipline from a very small team, and the overhead of maintaining it may exceed the value it produces in the short term. There is no good answer here, only tradeoffs.

### The baseline problem is circular

You cannot demonstrate the ROI of an improvement without a before-and-after measurement. But establishing the "before" measurement requires... engineering time. Which is the same resource you are trying to justify spending. This is the classic measurement bootstrapping problem for small teams.

### Intuitions are not the same as evidence

The high-friction points in the pipeline are real and well understood by the people who work on it. Post-ingest revocations are painful. Silent failures are expensive. Manual stage handoffs cost attention. These observations are credible and backed by experience -- but they are not data. An executive or a grantmaker will hear them as anecdotes, not evidence, and may weigh them accordingly.

### The audience problem

DPLA leadership understands the mission deeply. But some of the framing here -- SLOs, KPIs, mean time to recover -- comes from a software industry context where operations teams are large enough that these metrics pay for themselves. Translating this vocabulary into terms that resonate with a nonprofit leadership team requires careful framing. "Our pipeline has no SLOs" will land differently with an engineering manager than with a program director. The conversation needs to start with shared language before it can make the case for shared metrics.

---

## What We Could Establish Without Much Overhead

Even without formal instrumentation, some lightweight proxies are available right now:

- **Monthly cycle time:** Start time and end time of each monthly cycle are logged. This is already partially tracked in orchestrator logs. A rough number could be extracted.
- **Revocation count:** We know which hubs have requested revocations or re-ingestion. Counting these over 6--12 months is tractable.
- **S3 sync success rate:** The orchestrator logs hub-level success/failure. This data exists; it just is not summarized anywhere.
- **AWS cost per month:** Already visible in the billing console. Not the whole picture but a starting point.

The point is not to build a dashboard. It is to have some numbers -- even rough ones -- before going into a conversation about investment.

---

## The Argument Without Metrics

When presenting to people who do not track metrics in this way, the case can still be made with:

- **Stories, not statistics.** The Minnesota two-month silent failure is a better argument for monitoring than any MTTD number. One real example is worth more than a benchmark.
- **Comparison to the alternative.** "If this engineer is unavailable, the monthly cycle stops" is a concrete organizational risk statement that does not require a metric.
- **The incremental investment angle.** The proposal costs ~$3/month in new infrastructure and ~4 weeks of engineering time. That framing is accessible without needing ROI calculations.
- **Framing operational work as mission-critical.** dp.la does not work without this pipeline running monthly. The question is not whether to invest in it -- it is whether to invest proactively (before something breaks) or reactively (after it has). The reactive cost is always higher.

---

## What Would Need to Change Before Metrics Are Realistic

1. **Engineering time needs to be acknowledged as a cost.** This is a cultural and organizational question, not a technical one. If staff time is effectively free in the budget model, ROI arguments do not land.

2. **A lightweight logging habit.** Even a shared doc where the engineer notes "monthly cycle: started Feb 15, finished Feb 21, 3 hubs needed manual intervention, 1 revocation" would create a baseline over 6--12 months.

3. **A shared vocabulary.** Before talking about SLOs with non-engineering leadership, the conversation needs a simpler frame: "How reliable does the pipeline need to be?" and "How quickly do we need to know when it breaks?" are questions anyone can answer, and the answers imply SLOs without requiring the term.

---

## How to Raise This

This needs to happen verbally before it exists in writing. A written proposal on this topic could easily read as a complaint or an indictment -- "we have failed to measure our work" -- rather than as a constructive observation. The goal is to open a conversation, not deliver a verdict.

The conversation starters that seem most likely to land:

- "I want to make sure we can defend our infrastructure investment when we talk to funders. Right now, I'm not sure we can answer 'how much does running this pipeline cost, in total?' Do we want to fix that?"
- "When something goes wrong with the pipeline, it can take days to discover. I don't think that's acceptable for infrastructure this mission-critical. What would it take to get visibility into that?"
- "I could make a strong case for [specific improvement] if I had even a rough sense of how much time the current approach costs us per month. Is there appetite for tracking that?"

The point is to make the gap visible as a shared problem, not to assign responsibility for it.

---

## Questions I Don't Have Answers To

- Is there appetite at the leadership level for this kind of operational rigor? Or is the pipeline treated as "it works, don't touch it"?
- If we established some basic metrics, who would own them? Who would look at them?
- Are there grant opportunities specifically tied to infrastructure reliability or operational improvement? This seems like it could be fundable if framed correctly.
- What is the minimum useful measurement -- the one thing that, if we only tracked one thing, would tell us the most about pipeline health? (My guess: monthly cycle time end-to-end, or percentage of hubs completing successfully. But I don't know.)

---

*Last updated: February 2026. For personal reference only.*
