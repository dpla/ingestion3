# i3 Hub Alias Cleanup Plan (Post-Switchover)

This document defines how to remove temporary hub-name alias compatibility
after the canonical hub keys are fully adopted:

- `tn` -> `tennessee`
- `hathi` -> `hathitrust`

The goal is to remove monkey-patching/compatibility shims safely, with clear
rollback points and low operational risk.

## Purpose

- Eliminate long-term alias code paths once canonical naming is stable.
- Reduce cognitive load by making hub names consistent from config through S3.
- Keep cleanup reversible and easy to validate.

## Preconditions (Do Not Start Cleanup Until All Are True)

1. External `i3.conf` uses canonical keys (`tennessee.*`, `hathitrust.*`) in active environments.
2. At least one full ingest cycle has completed successfully with canonical keys.
3. No operational workflows/scripts still rely on legacy names (`tn`, `hathi`).
4. Existing rollout gates pass consistently in CI/local:
   - `./venv/bin/python -m pytest scheduler/orchestrator/tests/test_config.py scheduler/orchestrator/tests/test_notifications.py scheduler/orchestrator/tests/test_scheduling_emails.py`
   - `./scripts/tests/test-hub-alias-mock.sh`
   - `./scripts/tests/test-scripts.sh --quick`

## Recommended Delivery Strategy

Use two PRs instead of one:

1. Strict-mode enforcement + observation.
2. Alias deletion + test/doc cleanup.

This minimizes blast radius and makes rollback straightforward.

## PR 1: Strict-Mode Enforcement (No Alias Deletion Yet)

### Objective

Prove no remaining runtime dependency on alias fallback while keeping code
reversible.

### Actions

1. Enable strict mode in non-prod/CI paths:
   - `I3_STRICT_HUB_NAMES=1`
2. Run all rollout gates with strict mode enabled.
3. Perform orchestrator dry-runs using canonical names:
   - `./venv/bin/python -m scheduler.orchestrator.main --dry-run --hub=tennessee,hathitrust`
4. Observe for 1-2 ingest cycles.

### Exit Criteria for PR 1

- No strict-mode regressions.
- No legacy hub-name usage detected in operational logs/docs/scripts.

## PR 2: Delete Alias Monkey-Patching

### Objective

Remove compatibility logic and legacy-name pathways from code and tests.

### Code Cleanup Scope

1. `scheduler/orchestrator/config.py`
   - Remove alias maps/constants:
     - `HUB_KEY_ALIASES`
     - `CANONICAL_TO_LEGACY_HUB_KEY`
   - Remove alias-toggle helper:
     - `hub_aliases_enabled()`
   - Simplify methods:
     - `resolve_hub_key()` -> exact key only
     - `resolve_s3_prefix()` -> pass-through canonical key
     - `get_s3_prefix()` -> pass-through or remove if redundant
   - Remove `I3_STRICT_HUB_NAMES` behavior if no longer needed.

2. `scripts/common.sh`
   - Remove alias case logic from `resolve_s3_prefix()` or remove helper entirely.

3. `scripts/s3-sync.sh` and `scripts/check-jsonl-sync.sh`
   - Remove remaining compatibility assumptions.
   - Keep canonical-only behavior.

4. `scheduler/orchestrator/backlog_emails.py` and `scheduler/orchestrator/staged_report.py`
   - Remove legacy translation paths.
   - Ensure display and filtering use canonical names consistently.

### Test Cleanup Scope

1. `scheduler/orchestrator/tests/test_config.py`
   - Remove legacy-key fixtures/assertions (`tn`, `hathi`) once out of support.
   - Keep canonical-key fixtures (`tennessee`, `hathitrust`) as source of truth.
2. `scripts/tests/test-hub-alias-mock.sh`
   - Replace dual-mode alias tests with canonical-only tests.
3. Add/retain regression checks that fail if legacy-key assumptions reappear.

### Docs Cleanup Scope

1. Update runbooks/help text to canonical names only.
2. Remove references to temporary alias fallback and strict-mode rollout steps.
3. Keep this file as historical record or archive after final stabilization.

## Rollback Plan

If any regression occurs during cleanup:

1. Revert PR 2 entirely (single revert preferred).
2. Re-enable strict-mode verification from PR 1 to localize the break.
3. Resume with targeted fixes and re-run gates.

If regression occurs before alias deletion (during PR 1):

1. Disable strict mode (`unset I3_STRICT_HUB_NAMES` or equivalent env config).
2. Continue operation using existing compatibility paths.
3. Fix remaining canonicalization gaps before retrying strict mode.

## Final Completion Criteria

Cleanup is done when all are true:

1. Canonical naming is the only supported path in config, scripts, orchestrator, and docs.
2. No alias constants/toggles/translation logic remain in production code.
3. Test suite is canonical-only (or explicitly marks legacy tests as removed/deprecated).
4. At least one successful ingest cycle after cleanup with no rollback required.
