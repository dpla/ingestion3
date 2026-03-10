# Claude Code rules (ingest and scripts)

These rule files are **automatically loaded** by Claude Code when you run it in this repo. They mirror the workflows in `.cursor/skills/` so you get the same behavior in Cursor (skills) and in Claude Code (these rules).

**Suggested flow:** Warp (or any terminal) → `claude` (Claude Code CLI) in this repo → these rules are in context → run ingest pipelines, add scripts, sync S3, etc. using the same conventions as in Cursor.

**Environment:** Before any of: building the JAR (`sbt assembly`), running the orchestrator, or running pipeline scripts — run `source .env` from repo root so `JAVA_HOME` (Java 11+), `SLACK_WEBHOOK`, `DPLA_DATA`, `I3_CONF`, etc. are set. Full checklist: [AGENTS.md](../AGENTS.md) § Environment and build.

| Rule file | Use when |
|-----------|----------|
| script-workflow.md | Adding or modifying scripts; document in SCRIPTS.md, run tests |
| orchestrator.md | Running or monitoring the Python orchestrator (parallel ingest, status) |
| run-ingest.md | Running a single-hub ingest (harvest, remap, full pipeline) |
| verify-and-notify.md | Verifying runs and posting failures to #tech-alerts or tech@dp.la |
| s3-and-aws.md | Syncing to S3, checking JSONL sync; always `--profile dpla` |

Keep these in sync with `.cursor/skills/dpla-*` when you update workflows.
