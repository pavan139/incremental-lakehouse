# Publication and recovery checks

The review covered schema normalization/evolution, source identities and offsets, conflict resolution, staging recovery, publication atomicity, incremental group maintenance, content lineage and independent rebuild validity.

## Concrete issues found and corrected during implementation

- Python dictionary equality treats `100` and `100.0` as equal. Using it for schema migration skipped the promised float representation. Canonical typed JSON comparison now detects and rewrites type widening; tests check source and filtered-view types.
- Advancing every inspected model's content version could invalidate lineage even when its output was unchanged. Content generations now advance only for actual row changes, with initial empty views at generation zero. Tests and every evaluation batch check dependency-generation consistency.
- Naive float delta subtraction loses small residuals: adding `1e12` and `0.1`, then deleting `1e12`, produces a drifted float total. Persisted rational accumulators now maintain exact input contributions across restart; a dedicated cancellation test requires the remaining total to equal `0.1`.
- Malformed UTF-8 keys/text, huge numeric values and nonnumeric aggregate measures now fail explicit contracts instead of leaking runtime/SQL type failures.
- Delete events now require a known historical schema, matching upsert control semantics.

## Meaningful behavior coverage

The suite passed **22/22 test methods**. It covers group transfers/refunds/deletes, repeated-key coalescing, tombstone resurrection prevention, exact replay/conflicting identities, source gaps, partial staging/restart, pre-commit rollback, additive migration/type widening, quarantine with offset advancement, staging tampering, WAL reader isolation, no-op lineage, DAG cycles/missing columns and exact float cancellation. The seeded end-to-end experiment runs at seeds 17, 23 and 101 inside tests and compares actual incremental snapshots to an independent full rebuild.

Commands executed from this repository:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
PYTHONPATH=src python -m incremental_lakehouse evaluate --config contracts/experiment.json --output fixtures/evaluation.json
PYTHONPATH=src python -m incremental_lakehouse demo --output fixtures/demo.json
```

Ruff checks and formatting passed. Packaging/standalone CI-equivalent validation is recorded separately in `reports/verification.json` when the verifier is executed; a report's source hash identifies the code it checked.

## Remaining limits

The supplied data and failures are synthetic. The reference shares schema contracts but independently rebuilds winners and aggregates. Exception injection and reopen tests do not certify power-loss resilience. SQLite single-writer publication has no distributed source/ack transaction. There is no external connector, retention compactor, distributed scheduler or production load benchmark. Schema changes that require dropping/narrowing data need an explicit migration project, rather than silent compatibility.
