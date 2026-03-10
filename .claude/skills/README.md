# Claude Code Skills (synced from docs/ai-context/)

These skills are **generated** by `./scripts/ai-context/sync.sh` from `docs/ai-context/skills/`. Do not edit directly; edit the source and re-run sync.

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| send-email | `send email <hub>` | Send ingest summary email to hub contacts on demand |
| dpla-hub-info | `hub info <hub>` | Show key i3.conf config for a hub (harvest type/endpoint, schedule, email, setlist) |
| dpla-staged-report | `staged report` | Report which hubs have new JSONL staged in S3 for a month (optionally post to Slack) |
| dpla-oai-harvest-watch | `watch oai harvest <hub>` | Watch an OAI harvest log and report set-by-set progress + ETA |
| dpla-monitor-ingest-remap | `monitor remap <hub>` | Monitor mapping/enrichment/jsonl progress via orchestrator status or `_SUCCESS` markers |
| dpla-ingest-status | `status of the ingests` | Show all active and completed ingests in one consolidated table |
| dpla-ingest-debug | `debug <hub>` | Debug and fix hub ingestion failures |
| dpla-community-webs-ingest | `harvest community-webs` | Run Community Webs ingest from SQLite DB |
| dpla-monthly-emails | `send scheduling email` | Generate/preview/send the monthly pre-scheduling email |

## Using Skills

Skills are automatically discovered when you run Claude Code in this repo. Invoke them naturally:

```
User: "send email for nara"
Claude: [invokes send-email skill with argument "nara"]
```

## Skill Structure

Each skill is defined in its own directory with a `SKILL.md` file:

```
.claude/skills/
  <skill-name>/
    SKILL.md         # Skill definition with frontmatter and instructions
```

## Integration with Project Rules

Skills complement the `.claude/rules/` files:
- **Rules** (`.claude/rules/*.md`): Always loaded context for workflows
- **Skills** (`.claude/skills/*/SKILL.md`): On-demand invocable commands

Both work together to provide comprehensive Claude Code support for DPLA ingests.

**Source:** `docs/ai-context/` — edit there, then run `./scripts/ai-context/sync.sh`.

## Related

- [.claude/rules/README.md](../rules/README.md) - Auto-loaded workflow rules
- [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) - Shell script reference
- [CLAUDE.md](../../CLAUDE.md) - Project-level Claude instructions
