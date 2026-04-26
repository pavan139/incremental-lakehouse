# CDC identities and incremental views

## Identities, clocks and conflict resolution

A CDC envelope carries `(source, partition, offset)` as its immutable ingestion identity, a logical `(table, key)`, operation, event-time integer and schema version. The ledger stores the full envelope hash. Cursor progress is distinct from winner version: a late but valid source event advances the cursor even if its event-time version cannot overwrite the current row. Deletes preserve their winner version as tombstones.

This project explicitly chooses event-time LWW with source/partition/offset tie breaking. A source with a different correction policy must replace that contract; silently treating offsets and event time interchangeably would be incorrect.

## Durable staging and atomic publish

Staging first saves an immutable bounded batch payload, content hash, status and staging cursor. Chunk transactions insert ordinal-addressed events and move the cursor together. Recovery reads the stored payload and resumes the next ordinal. A published batch replay returns its committed report. Changed content under an existing batch ID fails.

Publication acquires a SQLite write transaction and verifies every staged ordinal against the payload. It applies schema migrations, ledger checks, quarantines, source winners, incremental model deltas, content generations, lineage and offsets before one commit. The injected pre-commit crash rolls all those effects back. The staged batch remains retryable. Source gaps or conflicts similarly roll back any earlier work in that batch.

SQLite uses WAL and `synchronous=FULL`. A read transaction captures all snapshot queries together. The guarantees cover local SQLite effects only; external source acknowledgements are not part of this transaction. Failure injection tests simulate exceptions and process restart, not certified power-loss behavior.

## Schema and quality contracts

Fields support exact integer, finite float, bounded UTF-8 text and Boolean types. Booleans never satisfy integer contracts. The primary key is nonnullable text. Input unknown columns are rejected. Missing values use declared defaults or nullable nulls.

Schema evolution requires the next consecutive version. Existing columns cannot disappear, change to incompatible types or narrow nullability. New required fields need a typed default; integer-to-float widening and relaxed nullability are allowed. Existing live rows migrate in the same publication transaction. Canonical JSON comparison preserves type changes such as `100` to `100.0`, which Python numeric equality alone would hide.

An event first validates against its declared historical schema, then normalizes to the current schema. Row/quality failures are quarantined with their bounded original envelope and consumed offset. Incompatible control-plane schema changes abort publication instead of silently quarantining an entire table migration.

## Incremental analytical DAG

A topological sort validates source/model dependencies and catches missing dependencies/cycles. Initial model column/type checks reject unavailable columns, nonnumeric aggregate measures and ambiguous output-name collisions. Transformations are a closed declarative set: equality filters and grouped numeric sum/count.

For every source key, the engine retains `(pre_batch_row, final_row)`, coalescing repeated mutations and net-zero changes. A filter computes old/new membership per dirty key. An aggregate subtracts old input contributions and adds new contributions only for affected groups. Zero-count buckets disappear. Aggregate state persists an exact rational accumulator for accepted integer/binary-float contributions; deleting a large value cannot lose a small residual through floating subtraction. Only the displayed JSON total is rounded to float when nonintegral. This is exact binary-float accumulation, not an assertion that floats represent decimal money exactly. This handles amount changes, group transfers, filter entry/exit and deletes without double counting. No arbitrary model-supplied code executes.

Content versions advance only when visible rows change. Lineage records the actual dependency content generation inspected by each model. An unchanged filtered view does not falsely invalidate its aggregate descendants. Initial empty views have generation zero.

## Independent rebuild oracle

The oracle does not call incremental publication or delta-maintenance helpers. It deduplicates source identities, validates rows, selects the maximum valid winner per key, filters full winning tables and computes grouped sums by full scans. Shared contract validation defines the accepted schema; transition/aggregation algorithms are independent. Evaluation compares both source and analytical rows after each batch, checks offsets/lineage and replays published batches.

## Bounds and complexity

Batches contain at most 2,048 envelopes of at most 16KB, 32 schema changes and 32 model nodes. Movement through the DAG is bounded by configured nodes and dirty rows. A database publication rejects more than 100,000 consumed offsets or 50,000 source keys. World-independent retained history still grows until those bounds; no garbage collection is implemented.

Ordinary CDC processing is roughly `O(B log B + B log N + D log N)` with SQLite indexes, where B is batch size and D is propagated dirty keys/groups. Staging payload hashing is linear in payload bytes. Additive schema migration scans live source rows; the independent oracle intentionally scans complete history and tables. SQLite serializes writers. No benchmark claims distributed scalability or production throughput.
