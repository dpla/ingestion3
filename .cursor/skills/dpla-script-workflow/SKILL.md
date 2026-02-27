---
name: dpla-script-workflow
description: Follow the project workflow when adding or modifying shell or Python scripts. Use when the user asks to add a script, create a new script, modify a script, or write a script for a task. Ensures POSIX bash, common.sh, documentation in SCRIPTS.md, and tests are created or updated and run.
---

# DPLA Script Workflow

## Purpose
When adding or modifying scripts in this repo, follow the project's conventions so scripts are documented, portable, and tested.

## When to Use
- "Add a script for..."
- "New script to..."
- "Modify script X"
- "Create a script that..."
- "Script for [task]"

**Environment:** When running the test suite or any script that depends on project env, run `source .env` from repo root first.

## Project Rules (apply to all new/changed scripts)

1. **Python:** Use the virtualenv at `./venv/` (e.g. `./venv/bin/python` or `source ./venv/bin/activate`). Never assume system Python.
2. **AWS CLI:** Use `--profile dpla` for any AWS commands.
3. **Shell scripts:** Write POSIX-compliant bash. Avoid macOS- or Linux-specific flags (e.g. `sed -i`, `readlink -f`). Use helpers from `scripts/common.sh`: `sed_i`, `get_script_dir`, `get_common_dir`, `log_info`, `die`, etc.

## Workflow Checklist

### For new scripts
- [ ] Implement using `scripts/common.sh` if bash (source it, use its helpers).
- [ ] Add to **Quick Reference** table in [scripts/SCRIPTS.md](../../../scripts/SCRIPTS.md) (script name, purpose, usage).
- [ ] If non-trivial, add a **Script Details** subsection in SCRIPTS.md (purpose, usage, env vars).
- [ ] Add or extend tests in `scripts/tests/test-scripts.sh` (or under `scripts/tests/` as appropriate).
- [ ] Run `./scripts/tests/test-scripts.sh` (or `--quick` for syntax) before committing.

### For modified scripts
- [ ] Update SCRIPTS.md if behavior or usage changed (Quick Reference and/or Script Details).
- [ ] Update tests if behavior changed; add tests for new behavior.
- [ ] Run `./scripts/tests/test-scripts.sh` before committing.

## Key References

| Resource | Path |
|----------|------|
| Script docs | [scripts/SCRIPTS.md](../../../scripts/SCRIPTS.md) |
| Updating docs | SCRIPTS.md section "Updating This Documentation" |
| Shared helpers | scripts/common.sh |
| Test suite | scripts/tests/test-scripts.sh |

## Quick Commands

```bash
./scripts/tests/test-scripts.sh           # Full suite
./scripts/tests/test-scripts.sh --quick  # Syntax / lightweight
./scripts/tests/test-scripts.sh --verbose
```

## Example: Adding a new bash script

1. Create script under `scripts/` with shebang `#!/usr/bin/env bash`, `set -euo pipefail`, then source common.sh using the inline pattern (since `get_script_dir` is defined in common.sh): `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` and `source "$SCRIPT_DIR/common.sh"` (or `source "$SCRIPT_DIR/../common.sh"` for scripts in subdirs like `scripts/status/`).
2. After sourcing, use `sed_i`, `get_script_dir`, `log_info`, `die` instead of raw `sed -i` or `readlink -f`.
3. Add row to Quick Reference in SCRIPTS.md.
4. Add a test in `scripts/tests/test-scripts.sh` (e.g. syntax check, help output, or sourcing common.sh).
5. Run `./scripts/tests/test-scripts.sh` and fix any failures.
