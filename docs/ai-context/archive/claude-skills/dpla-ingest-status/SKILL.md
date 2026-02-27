---
name: dpla-ingest-status
description: Show all active and completed ingests in one consolidated view. Use when user asks status of the ingests, ingest status, which hubs are running, show active ingests, or ingest progress.
---

# dpla-ingest-status

Show ingest status for all hubs (or specific hubs) in one command. Reads `logs/status/*.status` files — no per-hub log tailing.

## When to Use

- "Status of the ingests"
- "Ingest status"
- "Which hubs are running"
- "Show active ingests"
- "Ingest progress"

## Primary Action

Run one command:

```bash
source .env
./scripts/status/ingest-status.sh
```

For live updates:

```bash
source .env
./scripts/status/ingest-status.sh --watch
```

## Rules

- **Do not** tail logs per hub, grep ingest.sh, or run monitor-remap.sh per hub.
- **Do not** assemble status tables manually from log output.
- If "No status files found": Status comes from `logs/status/*.status` and `logs/orchestrator_state.json`. Ensure ingests use harvest.sh, ingest.sh, or the orchestrator. For debugging failures, use the dpla-ingest-debug skill instead.

## Options

| Option | Purpose |
|--------|---------|
| (none) | Table view, all hubs |
| `--watch` | Auto-refresh every 30s |
| `--watch 10` | Auto-refresh every 10s |
| `-v` | Include stage history |
| `--json` | Raw JSON output |
| `--all` | Include stale entries (e.g. stuck >72h) |
| `hub1 hub2` | Show only specified hubs |

## Data sources

Status comes from `logs/orchestrator_state.json` and `logs/status/*.status`. Stale entries (>72h in same stage) are excluded by default; use `--all` to include.

## References

- [scripts/SCRIPTS.md](scripts/SCRIPTS.md) — ingest-status.sh usage
- [AGENTS.md](AGENTS.md) — agent guide
