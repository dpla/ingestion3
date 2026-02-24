# Claude Code Skills for DPLA Ingestion3

This directory contains user-invocable skills for Claude Code. Skills provide specialized workflows and commands that can be invoked by name.

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| send-email | `send email <hub>` | Send ingest summary email to hub contacts on demand |
| s3-latest | `what is the latest data for <hub>` | Show latest harvest, mapping, and JSONL in S3 for a hub |

## Using Skills

### In Claude Code

Skills are automatically discovered when you run Claude Code in this repo. You can invoke them naturally:

```
User: "send email for nara"
Claude: [invokes send-email skill with argument "nara"]
```

Or explicitly:

```
User: "/send-email nara"
```

### Skill Structure

Each skill is defined in its own directory with a `SKILL.md` file:

```
.claude/skills/
  <skill-name>/
    SKILL.md         # Skill definition with frontmatter and instructions
```

The `SKILL.md` format:
```markdown
---
name: skill-name
description: Brief description for skill discovery
---

# skill-name

[Detailed instructions for Claude on how to use the skill]
```

## Creating New Skills

1. Create a new directory: `.claude/skills/<skill-name>/`
2. Add `SKILL.md` with frontmatter and instructions
3. Document the skill in this README
4. Test by invoking in Claude Code

## Integration with Project Rules

Skills complement the `.claude/rules/` files:
- **Rules** (`.claude/rules/*.md`): Always loaded context for workflows
- **Skills** (`.claude/skills/*/SKILL.md`): On-demand invocable commands

Both work together to provide comprehensive Claude Code support for DPLA ingests.

## Related

- [.claude/rules/README.md](../rules/README.md) - Auto-loaded workflow rules
- [scripts/SCRIPTS.md](../../scripts/SCRIPTS.md) - Shell script reference
- [CLAUDE.md](../../CLAUDE.md) - Project-level Claude instructions
