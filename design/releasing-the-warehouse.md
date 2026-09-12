# Releasing the warehouse code

Check that the distribution includes schema contracts, staging/recovery code, incremental models, and the independent rebuild oracle. The fixture reports should identify their source events and batch boundaries; local warehouse databases should not be packaged.

```sh
python -m pip install -e . -r requirements-dev.txt
python maintenance/check_publish.py --require-lint --wheel
python -m incremental_lakehouse evaluate --config contracts/experiment.json --output artifacts/release-run.json
```

Review quarantined rows and rejected publications along with successful batches. Changes to event identity, winner ordering, schema evolution, or content-generation semantics require updated fixtures and an explanation of compatibility with previously staged payloads.

The configured CI builds and tests a wheel and uploads `reports/verification.json`. Keep a local result separate from a remote Actions run. Publish source and bounded synthetic fixtures rather than a database containing retained offsets or private source payloads. [Change-stream runs](change-stream-runs.md) gives the recorded seed and hash used by the examples.
