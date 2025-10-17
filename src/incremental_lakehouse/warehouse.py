"""SQLite staged CDC with one atomic publish and key-level DAG maintenance."""

from dataclasses import asdict
from fractions import Fraction
import json
from pathlib import Path
import sqlite3

from .contracts import (
    ContractError,
    DEFAULT_MODELS,
    DEFAULT_SCHEMA,
    Event,
    Schema,
    canonical,
    digest,
    quality,
    topological_order,
)


class InjectedCrash(RuntimeError):
    pass


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class Warehouse:
    def __init__(self, path, schemas=(DEFAULT_SCHEMA,), models=DEFAULT_MODELS):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.schemas = tuple(schemas)
        if len({schema.table for schema in schemas}) != len(schemas) or any(schema.version != 1 for schema in schemas):
            raise ContractError("Initialize one version-1 schema per source; evolve through published batches")
        self.models = tuple(topological_order(tuple(models), {schema.table for schema in schemas}))
        catalog = {schema.table: {field.name: field.kind for field in schema.fields} for schema in schemas}
        for model in self.models:
            required = {model.column} | ({model.measure} if model.kind == "aggregate" else set())
            if not required <= catalog[model.dependency].keys():
                raise ContractError("Model references unavailable dependency columns")
            if model.kind == "aggregate" and (
                catalog[model.dependency][model.measure] not in {"int", "float"}
                or model.column in {"total_amount_cents", "order_count"}
            ):
                raise ContractError("Aggregate needs a numeric measure and noncolliding group column")
            catalog[model.name] = (
                catalog[model.dependency]
                if model.kind == "filter"
                else {
                    model.column: catalog[model.dependency][model.column],
                    "total_amount_cents": catalog[model.dependency][model.measure],
                    "order_count": "int",
                }
            )
        self.pipeline_hash = digest([asdict(model) for model in self.models])
        with self.connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS schemas(table_name TEXT,version INTEGER,body TEXT NOT NULL,PRIMARY KEY(table_name,version));
                CREATE TABLE IF NOT EXISTS table_versions(name TEXT PRIMARY KEY,generation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,cursor INTEGER NOT NULL,report TEXT);
                CREATE TABLE IF NOT EXISTS staged(batch_id TEXT,ordinal INTEGER,body TEXT NOT NULL,PRIMARY KEY(batch_id,ordinal));
                CREATE TABLE IF NOT EXISTS offsets(source TEXT,partition_id TEXT,offset INTEGER NOT NULL,PRIMARY KEY(source,partition_id));
                CREATE TABLE IF NOT EXISTS ledger(source TEXT,partition_id TEXT,offset INTEGER,event_hash TEXT NOT NULL,PRIMARY KEY(source,partition_id,offset));
                CREATE TABLE IF NOT EXISTS records(table_name TEXT,key TEXT,body TEXT,version TEXT NOT NULL,tombstone INTEGER NOT NULL,PRIMARY KEY(table_name,key));
                CREATE TABLE IF NOT EXISTS derived(model TEXT,key TEXT,body TEXT NOT NULL,PRIMARY KEY(model,key));
                CREATE TABLE IF NOT EXISTS aggregate_state(model TEXT,key TEXT,numerator TEXT NOT NULL,denominator TEXT NOT NULL,PRIMARY KEY(model,key));
                CREATE TABLE IF NOT EXISTS lineage(model TEXT PRIMARY KEY,dependency TEXT NOT NULL,input_generation INTEGER NOT NULL,generation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS quarantine(source TEXT,partition_id TEXT,offset INTEGER,reason TEXT NOT NULL,body TEXT NOT NULL,batch_id TEXT NOT NULL,PRIMARY KEY(source,partition_id,offset));
            """)
            existing = connection.execute("SELECT value FROM settings WHERE key='pipeline'").fetchone()
            if existing and existing[0] != self.pipeline_hash:
                raise ContractError("Persisted DAG differs from requested pipeline")
            connection.execute("INSERT OR IGNORE INTO settings VALUES('pipeline',?)", (self.pipeline_hash,))
            connection.execute("INSERT OR IGNORE INTO settings VALUES('generation','0')")
            for schema in schemas:
                body = canonical(asdict(schema))
                old = connection.execute(
                    "SELECT body FROM schemas WHERE table_name=? AND version=?", (schema.table, schema.version)
                ).fetchone()
                if old and old[0] != body:
                    raise ContractError("Initial schema identity changed")
                connection.execute("INSERT OR IGNORE INTO schemas VALUES(?,?,?)", (schema.table, schema.version, body))
                connection.execute("INSERT OR IGNORE INTO table_versions VALUES(?,0)", (schema.table,))
            for model in self.models:
                connection.execute("INSERT OR IGNORE INTO table_versions VALUES(?,0)", (model.name,))
                connection.execute("INSERT OR IGNORE INTO lineage VALUES(?,?,0,0)", (model.name, model.dependency))

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=5, factory=ClosingConnection)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def stage(self, batch_id, events, schema_updates=(), *, chunk_size=8, crash_after_chunks=None):
        if not isinstance(batch_id, str) or not 1 <= len(batch_id) <= 128:
            raise ContractError("Bounded batch id required")
        if (
            not isinstance(events, (list, tuple))
            or len(events) > 2048
            or not all(isinstance(event, Event) for event in events)
        ):
            raise ContractError("Batch requires at most 2048 validated events")
        if not events and not schema_updates:
            raise ContractError("Empty batch")
        if type(chunk_size) is not int or not 1 <= chunk_size <= 256:
            raise ContractError("Chunk size must be 1..256")
        if (
            not isinstance(schema_updates, (list, tuple))
            or len(schema_updates) > 32
            or not all(isinstance(value, Schema) for value in schema_updates)
        ):
            raise ContractError("Invalid schema update list")
        for event in events:
            Event(**asdict(event))
        payload = {
            "events": [asdict(event) for event in events],
            "schemas": [asdict(value) for value in schema_updates],
            "pipeline": self.pipeline_hash,
        }
        fingerprint = digest(payload)
        with self.connect() as connection:
            previous = connection.execute("SELECT payload_hash,status FROM batches WHERE id=?", (batch_id,)).fetchone()
            if previous and previous[0] != fingerprint:
                raise ContractError("Batch id reused with different content")
            if previous and previous[1] == "published":
                return {"batch_id": batch_id, "status": "published", "replayed": True}
            connection.execute(
                "INSERT OR IGNORE INTO batches VALUES(?,?,?,'staging',0,NULL)",
                (batch_id, fingerprint, canonical(payload)),
            )
        return self._finish_stage(batch_id, chunk_size, crash_after_chunks)

    def _finish_stage(self, batch_id, chunk_size, crash_after_chunks):
        chunks = 0
        while True:
            with self.connect() as connection:
                row = connection.execute(
                    "SELECT payload,payload_hash,cursor,status FROM batches WHERE id=?", (batch_id,)
                ).fetchone()
                if not row:
                    raise ContractError("Unknown batch")
                payload, fingerprint, cursor, status = json.loads(row[0]), row[1], row[2], row[3]
                if digest(payload) != fingerprint or payload["pipeline"] != self.pipeline_hash:
                    raise ContractError("Staged payload integrity mismatch")
                if status in {"ready", "published"}:
                    return {"batch_id": batch_id, "status": status, "staged": cursor}
                stop = min(len(payload["events"]), cursor + chunk_size)
                for index in range(cursor, stop):
                    connection.execute(
                        "INSERT INTO staged VALUES(?,?,?)", (batch_id, index, canonical(payload["events"][index]))
                    )
                status = "ready" if stop == len(payload["events"]) else "staging"
                connection.execute("UPDATE batches SET cursor=?,status=? WHERE id=?", (stop, status, batch_id))
            chunks += 1
            if crash_after_chunks is not None and chunks >= crash_after_chunks:
                raise InjectedCrash("Crash after committed staging chunk; publish remains invisible")

    def recover(self, batch_id):
        self._finish_stage(batch_id, 8, None)
        return self.publish(batch_id)

    @staticmethod
    def _current_schemas(connection):
        return {
            table: Schema.from_dict(json.loads(body))
            for table, _, body in connection.execute(
                "SELECT table_name,version,body FROM schemas s WHERE version=(SELECT max(version) FROM schemas x WHERE x.table_name=s.table_name)"
            )
        }

    def publish(self, batch_id, *, crash_before_commit=False):
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            batch = connection.execute(
                "SELECT payload,payload_hash,status,report FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
            if not batch or batch[2] not in {"ready", "published"}:
                raise ContractError("Batch is not completely staged")
            if batch[2] == "published":
                report = json.loads(batch[3])
                report["replayed"] = True
                return report
            payload = json.loads(batch[0])
            if digest(payload) != batch[1] or payload["pipeline"] != self.pipeline_hash:
                raise ContractError("Batch integrity mismatch")
            staged = [
                json.loads(body)
                for _, body in connection.execute(
                    "SELECT ordinal,body FROM staged WHERE batch_id=? ORDER BY ordinal", (batch_id,)
                )
            ]
            if staged != payload["events"]:
                raise ContractError("Staging is incomplete or corrupted")
            current = self._current_schemas(connection)
            changes = {}

            def change(table, key, old, new):
                table_changes = changes.setdefault(table, {})
                original = table_changes[key][0] if key in table_changes else old
                table_changes[key] = (original, new)

            for raw in payload["schemas"]:
                schema = Schema.from_dict(raw)
                if schema.table not in current:
                    raise ContractError("New source tables require a new explicit pipeline")
                schema.evolve_from(current[schema.table])
                for key, body in connection.execute(
                    "SELECT key,body FROM records WHERE table_name=? AND tombstone=0", (schema.table,)
                ).fetchall():
                    old = json.loads(body)
                    new = schema.normalize(old)
                    quality(new)
                    if canonical(old) != canonical(new):
                        change(schema.table, key, old, new)
                        connection.execute(
                            "UPDATE records SET body=? WHERE table_name=? AND key=?",
                            (canonical(new), schema.table, key),
                        )
                connection.execute(
                    "INSERT INTO schemas VALUES(?,?,?)", (schema.table, schema.version, canonical(asdict(schema)))
                )
                current[schema.table] = schema
            report = {
                "batch_id": batch_id,
                "input_events": len(staged),
                "new_offsets": 0,
                "duplicate_events": 0,
                "quarantined": 0,
                "stale_events": 0,
                "changed_source_keys": 0,
                "models_refreshed": [],
                "replayed": False,
            }
            for raw in sorted(staged, key=lambda row: (row["source"], row["partition"], row["offset"])):
                event = Event(**raw)
                identity = event.identity
                event_hash = digest(raw)
                seen = connection.execute(
                    "SELECT event_hash FROM ledger WHERE source=? AND partition_id=? AND offset=?", identity
                ).fetchone()
                if seen:
                    if seen[0] != event_hash:
                        raise ContractError("Conflicting content at a previously consumed source offset")
                    report["duplicate_events"] += 1
                    continue
                previous = connection.execute(
                    "SELECT offset FROM offsets WHERE source=? AND partition_id=?", identity[:2]
                ).fetchone()
                expected_offset = previous[0] + 1 if previous else 0
                if event.offset != expected_offset:
                    raise ContractError(f"Source offset gap: expected {expected_offset}, received {event.offset}")
                if event.table not in current:
                    raise ContractError("Source table is not in the pipeline")
                normalized = None
                try:
                    historical = connection.execute(
                        "SELECT body FROM schemas WHERE table_name=? AND version=?", (event.table, event.schema_version)
                    ).fetchone()
                    if not historical:
                        raise ContractError("Unknown event schema version")
                    if event.operation == "upsert":
                        normalized = Schema.from_dict(json.loads(historical[0])).normalize(event.row)
                        normalized = current[event.table].normalize(normalized)
                        if normalized["id"] != event.key:
                            raise ContractError("Record id differs from CDC key")
                        quality(normalized)
                except ContractError as error:
                    connection.execute(
                        "INSERT INTO quarantine VALUES(?,?,?,?,?,?)", (*identity, str(error), canonical(raw), batch_id)
                    )
                    report["quarantined"] += 1
                else:
                    previous_row = connection.execute(
                        "SELECT body,version,tombstone FROM records WHERE table_name=? AND key=?",
                        (event.table, event.key),
                    ).fetchone()
                    if previous_row and event.version <= tuple(json.loads(previous_row[1])):
                        report["stale_events"] += 1
                    else:
                        old = json.loads(previous_row[0]) if previous_row and not previous_row[2] else None
                        new = normalized if event.operation == "upsert" else None
                        change(event.table, event.key, old, new)
                        connection.execute(
                            "INSERT INTO records VALUES(?,?,?,?,?) ON CONFLICT(table_name,key) DO UPDATE SET body=excluded.body,version=excluded.version,tombstone=excluded.tombstone",
                            (
                                event.table,
                                event.key,
                                canonical(new) if new is not None else None,
                                canonical(event.version),
                                int(new is None),
                            ),
                        )
                connection.execute("INSERT INTO ledger VALUES(?,?,?,?)", (*identity, event_hash))
                connection.execute(
                    "INSERT INTO offsets VALUES(?,?,?) ON CONFLICT(source,partition_id) DO UPDATE SET offset=excluded.offset",
                    (*identity[:2], event.offset),
                )
                report["new_offsets"] += 1
            generation = int(connection.execute("SELECT value FROM settings WHERE key='generation'").fetchone()[0]) + 1
            report["generation"] = generation
            if (
                connection.execute("SELECT count(*) FROM ledger").fetchone()[0] > 100000
                or connection.execute("SELECT count(*) FROM records").fetchone()[0] > 50000
            ):
                raise ContractError("Experiment exceeds 100000 consumed offsets or 50000 source keys")
            changes = {
                table: {key: pair for key, pair in rows.items() if canonical(pair[0]) != canonical(pair[1])}
                for table, rows in changes.items()
            }
            report["changed_source_keys"] = sum(len(rows) for rows in changes.values())
            for table, rows in changes.items():
                if rows:
                    connection.execute(
                        "INSERT INTO table_versions VALUES(?,?) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation",
                        (table, generation),
                    )
            for model in self.models:
                upstream = changes.get(model.dependency, {})
                if not upstream:
                    continue
                downstream = {}
                if model.kind == "filter":
                    for key, (_, new_input) in upstream.items():
                        previous = connection.execute(
                            "SELECT body FROM derived WHERE model=? AND key=?", (model.name, key)
                        ).fetchone()
                        old = json.loads(previous[0]) if previous else None
                        new = new_input if new_input is not None and new_input[model.column] == model.value else None
                        if canonical(old) != canonical(new):
                            downstream[key] = (old, new)
                else:
                    touched = {}
                    for old, new in upstream.values():
                        for row, sign in ((old, -1), (new, 1)):
                            if row is None:
                                continue
                            key = canonical(row[model.column])
                            if key not in touched:
                                previous = connection.execute(
                                    "SELECT body FROM derived WHERE model=? AND key=?", (model.name, key)
                                ).fetchone()
                                prior = json.loads(previous[0]) if previous else None
                                accumulator = connection.execute(
                                    "SELECT numerator,denominator FROM aggregate_state WHERE model=? AND key=?",
                                    (model.name, key),
                                ).fetchone()
                                if prior and accumulator is None:
                                    raise ContractError("Aggregate state missing; explicit rebuild required")
                                touched[key] = [
                                    prior,
                                    dict(prior)
                                    if prior
                                    else {model.column: row[model.column], "total_amount_cents": 0, "order_count": 0},
                                    Fraction(int(accumulator[0]), int(accumulator[1])) if accumulator else Fraction(0),
                                ]
                            value = touched[key][1]
                            touched[key][2] += sign * Fraction(row[model.measure])
                            value["order_count"] += sign
                    for key, (old, new, total) in touched.items():
                        if new["order_count"] < 0:
                            raise ContractError("Aggregate contribution invariant failed")
                        if new["order_count"]:
                            new["total_amount_cents"] = total.numerator if total.denominator == 1 else float(total)
                            connection.execute(
                                "INSERT INTO aggregate_state VALUES(?,?,?,?) ON CONFLICT(model,key) DO UPDATE SET numerator=excluded.numerator,denominator=excluded.denominator",
                                (model.name, key, str(total.numerator), str(total.denominator)),
                            )
                        else:
                            if total:
                                raise ContractError("Empty aggregate retains a nonzero exact sum")
                            new = None
                            connection.execute("DELETE FROM aggregate_state WHERE model=? AND key=?", (model.name, key))
                        if canonical(old) != canonical(new):
                            downstream[key] = (old, new)
                for key, (_, new) in downstream.items():
                    if new is None:
                        connection.execute("DELETE FROM derived WHERE model=? AND key=?", (model.name, key))
                    else:
                        connection.execute(
                            "INSERT INTO derived VALUES(?,?,?) ON CONFLICT(model,key) DO UPDATE SET body=excluded.body",
                            (model.name, key, canonical(new)),
                        )
                changes[model.name] = downstream
                if downstream:
                    connection.execute(
                        "INSERT INTO table_versions VALUES(?,?) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation",
                        (model.name, generation),
                    )
                input_generation = connection.execute(
                    "SELECT generation FROM table_versions WHERE name=?", (model.dependency,)
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO lineage VALUES(?,?,?,?) ON CONFLICT(model) DO UPDATE SET input_generation=excluded.input_generation,generation=excluded.generation",
                    (model.name, model.dependency, input_generation, generation),
                )
                report["models_refreshed"].append(model.name)
            connection.execute("UPDATE settings SET value=? WHERE key='generation'", (str(generation),))
            connection.execute(
                "UPDATE batches SET status='published',report=? WHERE id=?", (canonical(report), batch_id)
            )
            if crash_before_commit:
                raise InjectedCrash("Publish interrupted before atomic commit")
            connection.commit()
            return report
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def snapshot(self):
        with self.connect() as connection:
            connection.execute("BEGIN")
            sources = {}
            for table, key, body in connection.execute(
                "SELECT table_name,key,body FROM records WHERE tombstone=0 ORDER BY table_name,key"
            ):
                sources.setdefault(table, {})[key] = json.loads(body)
            derived = {model.name: {} for model in self.models}
            for model, key, body in connection.execute("SELECT model,key,body FROM derived ORDER BY model,key"):
                derived[model][key] = json.loads(body)
            return {
                "sources": sources,
                "models": derived,
                "offsets": [
                    {"source": source, "partition": partition, "offset": offset}
                    for source, partition, offset in connection.execute(
                        "SELECT * FROM offsets ORDER BY source,partition_id"
                    )
                ],
                "quarantine_count": connection.execute("SELECT count(*) FROM quarantine").fetchone()[0],
                "generation": int(
                    connection.execute("SELECT value FROM settings WHERE key='generation'").fetchone()[0]
                ),
                "content_generations": dict(
                    connection.execute("SELECT name,generation FROM table_versions ORDER BY name")
                ),
                "lineage": [
                    {
                        "model": model,
                        "dependency": dependency,
                        "input_generation": input_generation,
                        "generation": generation,
                    }
                    for model, dependency, input_generation, generation in connection.execute(
                        "SELECT * FROM lineage ORDER BY model"
                    )
                ],
            }
