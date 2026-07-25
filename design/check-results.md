# Warehouse check results

Use [reports/verification.json](../reports/verification.json) for the current source digest, test count, interpreter, and build outcomes. The check entry point is `maintenance/check_publish.py`, and behavior cases remain under `tests/`.

The report verifies the standalone package and CLI. [Recovery cases](recovery-cases.md) covers publication boundaries; [change-stream runs](change-stream-runs.md) records rebuild, cursor, lineage, and replay comparisons. These counts describe the specified local input history, not source delivery or downstream model quality.
