# Claude Code rules (synced from docs/ai-context/)

These rule files are **generated** by `./scripts/ai-context/sync.sh` from `docs/ai-context/rules/`. Do not edit directly; edit the source and re-run sync.

**Environment:** Before any of: building the JAR (`sbt assembly`), running the orchestrator, or running pipeline scripts — run `source .env` from repo root so `JAVA_HOME` (Java 11+), `SLACK_WEBHOOK`, `DPLA_DATA`, `I3_CONF`, etc. are set. Full checklist: [AGENTS.md](../../AGENTS.md) § Environment and build.

| Rule file | Use when |
|-----------|----------|
| ingestion.md | Running a single-hub ingest (harvest, remap, full pipeline) |
| orchestrator.md | Running or monitoring the Python orchestrator (parallel ingest, status) |
| aws-tools.md | S3 sync, finding latest S3 data; always `--profile dpla` |
| notifications.md | Posting failures to #tech-alerts or emailing tech@dp.la |
| validation.md | Verifying pipeline output (_SUCCESS markers, escalation reports) |
| script-standards.md | Adding or modifying scripts; document in SCRIPTS.md, run tests |

**Source:** `docs/ai-context/` — edit there, then run `./scripts/ai-context/sync.sh`.
