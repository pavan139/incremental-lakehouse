"""Seeded CDC experiments checked by an independent full rebuild."""

from dataclasses import asdict, replace
from pathlib import Path
import random
import tempfile

from .contracts import ContractError, DEFAULT_SCHEMA, Event, Field, Schema, digest
from .oracle import rebuild
from .warehouse import InjectedCrash, Warehouse


def evaluate(seed=17):
    if type(seed) is not int:
        raise ValueError("Seed must be an integer")
    rng = random.Random(seed)
    events, offsets = [], {f"p{i}": 0 for i in range(3)}
    for index in range(120):
        partition = f"p{index % 3}"
        key = f"order_{rng.randrange(12)}"
        operation = "delete" if index % 11 == 0 else "upsert"
        row = (
            {
                "id": key,
                "entity": f"entity_{rng.randrange(4)}",
                "amount_cents": -5 if index % 17 == 0 else rng.randrange(1, 300),
                "status": rng.choice(("paid", "pending", "refunded")),
            }
            if operation == "upsert"
            else None
        )
        version = 2 if index >= 30 and index % 5 == 0 and row is not None else 1
        if version == 2:
            row["region"] = rng.choice(("eu", "na"))
        events.append(
            Event(
                "orders_source",
                partition,
                offsets[partition],
                "orders",
                key,
                operation,
                max(0, index - 20) if index % 7 == 0 else index + 1,
                version,
                row,
            )
        )
        offsets[partition] += 1
    schema2 = Schema("orders", 2, DEFAULT_SCHEMA.fields + (Field("region", "text", default="unknown"),))
    cases, reports, history, schemas = [], [], [], [DEFAULT_SCHEMA]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "warehouse.sqlite"
        warehouse = Warehouse(path)
        for batch_index in range(12):
            batch_id = f"batch-{batch_index:02}"
            chunk = events[batch_index * 10 : (batch_index + 1) * 10]
            inputs = chunk + (history[-1:] if history else [])
            updates = (schema2,) if batch_index == 3 else ()
            before = warehouse.snapshot()
            if batch_index == 4:
                try:
                    warehouse.stage(batch_id, inputs, updates, chunk_size=3, crash_after_chunks=1)
                except InjectedCrash:
                    cases.append({"id": "partial-staging-invisible", "passed": warehouse.snapshot() == before})
                warehouse = Warehouse(path)
                report = warehouse.recover(batch_id)
            else:
                warehouse.stage(batch_id, inputs, updates)
                if batch_index == 7:
                    try:
                        warehouse.publish(batch_id, crash_before_commit=True)
                    except InjectedCrash:
                        cases.append({"id": "failed-publish-atomic", "passed": warehouse.snapshot() == before})
                report = warehouse.publish(batch_id)
            reports.append(report)
            history.extend(inputs)
            schemas.extend(updates)
            actual = warehouse.snapshot()
            expected = rebuild(history, schemas, warehouse.models)
            cases.append(
                {"id": f"{batch_id}-full-rebuild", "passed": all(actual[key] == expected[key] for key in expected)}
            )
            expected_offsets = {
                partition: max(event.offset for event in history if event.partition == partition)
                for partition in offsets
            }
            cases.append(
                {
                    "id": f"{batch_id}-offsets",
                    "passed": {row["partition"]: row["offset"] for row in actual["offsets"]} == expected_offsets,
                }
            )
            cases.append(
                {
                    "id": f"{batch_id}-lineage",
                    "passed": all(
                        row["input_generation"] == actual["content_generations"][row["dependency"]]
                        for row in actual["lineage"]
                    ),
                }
            )
            repeated = warehouse.publish(batch_id)
            cases.append(
                {"id": f"{batch_id}-idempotence", "passed": repeated["replayed"] and warehouse.snapshot() == actual}
            )
        bad = [
            ("conflicting-offset", [replace(events[1], event_time=9999)], ()),
            ("offset-gap", [Event("orders_source", "new_partition", 2, "orders", "gap", "delete", 200, 1)], ()),
            (
                "incompatible-schema",
                [],
                (
                    Schema(
                        "orders",
                        3,
                        tuple(
                            replace(field, kind="text") if field.name == "amount_cents" else field
                            for field in schema2.fields
                        ),
                    ),
                ),
            ),
        ]
        for batch_id, inputs, updates in bad:
            before = warehouse.snapshot()
            warehouse.stage(batch_id, inputs, updates)
            rejected = False
            try:
                warehouse.publish(batch_id)
            except ContractError:
                rejected = True
            cases.append({"id": batch_id, "passed": rejected and warehouse.snapshot() == before})
        final = warehouse.snapshot()
    return {
        "schema_version": 1,
        "experiment": "seeded_synthetic_CDC_vs_independent_full_rebuild",
        "seed": seed,
        "input_sha256": digest([asdict(event) for event in events]),
        "unique_source_events": len(events),
        "published_batches": len(reports),
        "checks": len(cases),
        "passed": sum(case["passed"] for case in cases),
        "metrics": {
            key: sum(report[key] for report in reports)
            for key in ("new_offsets", "duplicate_events", "quarantined", "stale_events", "changed_source_keys")
        },
        "final_snapshot": final,
        "cases": cases,
        "limitations": [
            "SQLite local single-writer publication; no distributed transactions.",
            "Event-time LWW with deterministic source ties is the declared conflict policy.",
            "Synthetic integer-valued orders; no production throughput or downstream model quality claim.",
        ],
    }
