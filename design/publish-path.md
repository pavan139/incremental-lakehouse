# From staged chunks to published views

```mermaid
flowchart LR
  E[Typed CDC envelopes] --> S[Durable chunk staging]
  C[Versioned schema contracts] --> P[Atomic publish transaction]
  S --> P
  P --> L[Offset ledger and conflict policy]
  L --> B[Upserts and retained tombstones]
  L --> Q[Quality quarantine]
  B --> D[Key deltas through model DAG]
  D --> A[Incremental grouped aggregates]
  A --> V[Content generations and lineage]
  P --> K[Offsets and publish checkpoint]
  O[Independent history rebuild] --> X[Snapshot comparison]
  V --> X
```

`contracts.py` owns typed fields, compatibility rules, CDC envelopes and declarative model definitions. `warehouse.py` owns staging and publication transactions. `oracle.py` independently scans history and rebuilds model results. `experiments.py` injects duplicates, late events, schema changes and failures, then compares committed snapshots and lineage.

The mutation boundary is SQLite publication. Staging is visible only through staging tables; analytical snapshots expose committed source/model rows, offset checkpoints and lineage together. Untrusted row values never construct SQL identifiers or executable code. Source/model names are bounded validated names and SQL values use parameters.
