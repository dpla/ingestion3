See @docs/ingestion/README_INGESTS.md for the generialized rules of running ingests and @docs/ingestion/README_NARA.md and @docs/ingestion/README_SMITHSONIAN.md for NARA and Smithsonian ingest documentations

# Project rules

1. **Environment:** Source `.env` before running any process that needs project env (including building the JAR: run `source .env` then `sbt assembly` from repo root so the correct Java 11+ JDK is used). From repo root: `source .env`. Full checklist: [AGENTS.md](AGENTS.md) § Environment and build.
2. **Python:** Use the virtualenv at `./venv/` (e.g. `./venv/bin/python` or `source ./venv/bin/activate`) for any new or existing Python scripts.
3. **AWS CLI:** Use `--profile dpla` for all AWS commands.
4. **Shell scripts:** Write POSIX-compliant bash; avoid macOS- or Linux-specific flags (e.g. `sed -i`, `readlink -f` — use helpers from `scripts/common.sh` where applicable).
5. **Build:** Before running ingests or skills that use the Scala pipeline, run `sbt assembly` from repo root so the fat JAR is current; scripts use the JAR when present.

# Ingest and script workflows (Claude Code)

All markdown files in **.claude/rules/** are loaded automatically by Claude Code when you run it in this repo. User-invocable **skills** in **.claude/skills/** provide on-demand commands. Rules and skills are generated from `docs/ai-context/` via `./scripts/ai-context/sync.sh`; edit there, then run sync.

- **Warp → Claude Code → .claude/rules/ + .claude/skills/**
  Run `claude` (or your Claude Code CLI) from the ingestion3 repo root. The rules in `.claude/rules/` (orchestrator, ingestion, script-standards, notifications, validation, aws-tools) are in context so you can say "run ingest for maryland", "run the orchestrator for wisconsin and p2p", "sync hub X to S3", "add a script for ...", etc. Skills in `.claude/skills/` provide invocable commands like "send email for nara". See:
  - [.claude/rules/README.md](.claude/rules/README.md) — Rule index
  - [.claude/skills/README.md](.claude/skills/README.md) — Available skills

# Environment Commands
- Source project env: `source .env` (from repo root; sets JAVA_HOME, SLACK_WEBHOOK, DPLA_DATA, I3_CONF, etc.)
- Build: run `source .env` then `sbt assembly` from repo root (required so sbt uses Java 11+)
- Python Source: `source ./venv/bin/activate`
- Build Command: `sbt assembly` (pipeline scripts use the assembly JAR when present)
- AWS Alias: Use `--profile dpla`
