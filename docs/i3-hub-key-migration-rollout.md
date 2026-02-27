# i3 Hub Key Migration Rollout (Deferred)

This document defines a conservative rollout/rollback for eventual `i3.conf`
hub-key normalization (`hathi -> hathitrust`, `tn -> tennessee`).

## Current policy

- Do **not** rename `i3.conf` keys yet.
- Keep alias compatibility enabled (`I3_STRICT_HUB_NAMES` unset/false).
- Use centralized resolver logic in orchestrator and scripts.

## Rollout gates (must pass before any config rename)

1. `./venv/bin/python -m pytest scheduler/orchestrator/tests/test_config.py scheduler/orchestrator/tests/test_notifications.py scheduler/orchestrator/tests/test_scheduling_emails.py`
2. `./venv/bin/python -m scheduler.orchestrator.main --dry-run --hub=hathi,tn`
3. `./venv/bin/python -m scheduler.orchestrator.main --dry-run --hub=hathitrust,tennessee`
4. `./scripts/tests/test-scripts.sh --quick`
5. `./scripts/tests/test-hub-alias-mock.sh`

## Deferred migration steps

When migration is approved:

1. Add duplicate entries in external `i3.conf` for canonical keys first.
2. Keep compatibility mode ON and run all rollout gates.
3. Cut over one alias pair at a time (`hathi` first, then `tn`).
4. Observe one full ingest cycle after each cutover.
5. Only after stability, consider strict mode in non-prod.

## Rollback

If any regression appears:

1. Re-enable compatibility mode by unsetting strict mode:
   - `unset I3_STRICT_HUB_NAMES`
2. Re-run affected command using legacy hub keys (`hathi`, `tn`).
3. Re-run rollout gates to confirm restored behavior.
4. Revert recent key changes in external `i3.conf` (if any were made).

This rollback is intentionally low-friction and avoids production S3 mutations.

## Related document

For post-cutover alias and monkey-patching removal, see:

- [i3-hub-alias-cleanup-plan.md](i3-hub-alias-cleanup-plan.md)
