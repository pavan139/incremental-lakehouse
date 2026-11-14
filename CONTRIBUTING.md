# Changing CDC or model maintenance

A publication change should be explainable as source identities consumed, winning rows changed, and old/new contributions propagated. Keep those three counts separate in a regression.

Use `tests/test_warehouse.py` for boundary cases and the seeded full-rebuild comparisons. The oracle may share schema validation, but it must not call the incremental delta-maintenance helpers. Include deletes, group transfers, no-op batches, and a failed publication when changing view maintenance.

Schema changes need an accepted migration example and a rejected one. Row-quality failures consume an offset and enter quarantine; control-plane failures abort the batch. Preserve that distinction and the retained tombstone rule.

Run `python -m unittest discover -s tests -v` and Ruff before `python maintenance/check_publish.py --require-lint --wheel`. For a new workload, keep the seed, source offsets, batch boundaries, input hash, quarantines, and independent rebuild results. [Recovery cases](design/recovery-cases.md) and [change-stream runs](design/change-stream-runs.md) show the current evidence.

Avoid adding an external connector without specifying where its acknowledgement sits relative to SQLite publication. Database atomicity alone cannot acknowledge an upstream stream.
