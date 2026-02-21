from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from incremental_lakehouse.contracts import (
    ContractError,
    DEFAULT_SCHEMA,
    Event,
    Field,
    Model,
    Schema,
    canonical,
    topological_order,
)
from incremental_lakehouse.experiments import evaluate
from incremental_lakehouse.warehouse import InjectedCrash, Warehouse


def event(offset, key="o", amount=100, entity="a", status="paid", time=None, partition="p0", version=1):
    return Event(
        "source",
        partition,
        offset,
        "orders",
        key,
        "upsert",
        offset + 10 if time is None else time,
        version,
        {"id": key, "entity": entity, "amount_cents": amount, "status": status},
    )


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "warehouse.sqlite"
        self.store = Warehouse(self.path)

    def publish(self, batch, events, updates=()):
        self.store.stage(batch, events, updates)
        return self.store.publish(batch)

    def test_upsert_group_transfer_refund_and_delete_delta_aggregates(self):
        self.publish("first", [event(0), event(1, "p", amount=50)])
        self.assertEqual(self.store.snapshot()["models"]["entity_totals"][canonical("a")]["total_amount_cents"], 150)
        self.publish("second", [event(2, entity="b", amount=70), event(3, "p", amount=50, status="refunded")])
        snapshot = self.store.snapshot()
        self.assertNotIn(canonical("a"), snapshot["models"]["entity_totals"])
        self.assertEqual(snapshot["models"]["entity_totals"][canonical("b")]["order_count"], 1)
        self.assertEqual(snapshot["models"]["refund_totals"][canonical("a")]["total_amount_cents"], 50)
        self.publish("delete", [Event("source", "p0", 4, "orders", "o", "delete", 99, 1)])
        self.assertEqual(self.store.snapshot()["models"]["entity_totals"], {})

    def test_multiple_mutations_of_one_key_are_coalesced(self):
        self.publish("coalesced", [event(0), event(1, amount=20), event(2, entity="b", amount=30)])
        self.assertEqual(
            self.store.snapshot()["models"]["entity_totals"],
            {canonical("b"): {"entity": "b", "order_count": 1, "total_amount_cents": 30}},
        )

    def test_tombstone_prevents_old_backfill_resurrection(self):
        self.publish("initial", [event(0, time=20), Event("source", "p0", 1, "orders", "o", "delete", 30, 1)])
        report = self.publish("late", [event(2, time=25)])
        self.assertEqual(report["stale_events"], 1)
        self.assertEqual(self.store.snapshot()["sources"], {})
        self.publish("new", [event(3, time=31)])
        self.assertIn("o", self.store.snapshot()["sources"]["orders"])

    def test_duplicate_exact_event_and_batch_are_idempotent(self):
        original = event(0)
        report = self.publish("dup", [original, original])
        self.assertEqual(report["new_offsets"], 1)
        self.assertEqual(report["duplicate_events"], 1)
        before = self.store.snapshot()
        self.assertTrue(self.store.publish("dup")["replayed"])
        self.assertEqual(before, self.store.snapshot())

    def test_conflicting_offset_and_batch_identity_fail_without_side_effects(self):
        self.publish("first", [event(0)])
        before = self.store.snapshot()
        with self.assertRaises(ContractError):
            self.store.stage("first", [event(0, amount=999)])
        self.store.stage("conflict", [event(0, amount=999)])
        with self.assertRaisesRegex(ContractError, "Conflicting"):
            self.store.publish("conflict")
        self.assertEqual(before, self.store.snapshot())

    def test_gap_rolls_back_earlier_valid_events_in_same_batch(self):
        before = self.store.snapshot()
        self.store.stage("gap", [event(0), event(2)])
        with self.assertRaisesRegex(ContractError, "gap"):
            self.store.publish("gap")
        self.assertEqual(self.store.snapshot(), before)
        self.publish("valid", [event(0)])
        self.assertEqual(self.store.snapshot()["offsets"][0]["offset"], 0)

    def test_staging_restart_recovers_from_persisted_payload(self):
        before = self.store.snapshot()
        with self.assertRaises(InjectedCrash):
            self.store.stage("restart", [event(i, f"o{i}") for i in range(5)], chunk_size=2, crash_after_chunks=1)
        self.assertEqual(self.store.snapshot(), before)
        reopened = Warehouse(self.path)
        report = reopened.recover("restart")
        self.assertEqual(report["new_offsets"], 5)
        self.assertEqual(len(reopened.snapshot()["sources"]["orders"]), 5)

    def test_publish_failure_rolls_back_schema_models_ledger_and_offsets(self):
        schema = Schema("orders", 2, DEFAULT_SCHEMA.fields + (Field("region", "text", default="unknown"),))
        before = self.store.snapshot()
        self.store.stage("failure", [event(0)], (schema,))
        with self.assertRaises(InjectedCrash):
            self.store.publish("failure", crash_before_commit=True)
        self.assertEqual(before, self.store.snapshot())
        self.assertEqual(self.store.publish("failure")["new_offsets"], 1)

    def test_additive_schema_migrates_old_rows_and_old_version_events(self):
        self.publish("first", [event(0)])
        schema = Schema("orders", 2, DEFAULT_SCHEMA.fields + (Field("region", "text", default="unknown"),))
        self.publish("schema", [], (schema,))
        self.publish("old-client", [event(1, "p")])
        for row in self.store.snapshot()["sources"]["orders"].values():
            self.assertEqual(row["region"], "unknown")
        self.assertEqual(Warehouse(self.path).snapshot(), self.store.snapshot())

    def test_type_widening_preserves_declared_float_representation(self):
        self.publish("first", [event(0)])
        schema = Schema(
            "orders",
            2,
            tuple(
                replace(field, kind="float") if field.name == "amount_cents" else field
                for field in DEFAULT_SCHEMA.fields
            ),
        )
        self.publish("widen", [], (schema,))
        self.assertIs(type(self.store.snapshot()["sources"]["orders"]["o"]["amount_cents"]), float)
        self.assertIs(type(self.store.snapshot()["models"]["paid_orders"]["o"]["amount_cents"]), float)

    def test_float_delta_cancellation_uses_persisted_exact_accumulator(self):
        schema = Schema(
            "orders",
            2,
            tuple(
                replace(field, kind="float") if field.name == "amount_cents" else field
                for field in DEFAULT_SCHEMA.fields
            ),
        )
        self.publish("schema", [], (schema,))
        self.publish("values", [event(0, "large", amount=10**12, version=2), event(1, "small", amount=0.1, version=2)])
        self.store = Warehouse(self.path)
        self.publish("remove", [Event("source", "p0", 2, "orders", "large", "delete", 999, 2)])
        self.assertEqual(self.store.snapshot()["models"]["entity_totals"][canonical("a")]["total_amount_cents"], 0.1)

    def test_incompatible_schema_and_no_default_are_rejected(self):
        for schema in (
            Schema("orders", 2, DEFAULT_SCHEMA.fields[:-1]),
            Schema("orders", 2, DEFAULT_SCHEMA.fields + (Field("required", "text"),)),
            Schema(
                "orders",
                2,
                tuple(
                    replace(field, kind="text") if field.name == "amount_cents" else field
                    for field in DEFAULT_SCHEMA.fields
                ),
            ),
        ):
            with self.subTest(schema=schema), self.assertRaises(ContractError):
                schema.evolve_from(DEFAULT_SCHEMA)

    def test_bad_quality_type_key_and_unknown_schema_are_quarantined(self):
        wrong_key = replace(event(2), row={"id": "different", "entity": "a", "amount_cents": 2, "status": "paid"})
        report = self.publish("bad", [event(0, amount=-1), event(1, amount=True), wrong_key, event(3, version=99)])
        self.assertEqual(report["quarantined"], 4)
        self.assertEqual(report["new_offsets"], 4)
        self.assertEqual(self.store.snapshot()["sources"], {})
        self.assertEqual(self.store.snapshot()["offsets"][0]["offset"], 3)

    def test_invalid_delete_schema_is_quarantined(self):
        report = self.publish("unknown", [Event("source", "p0", 0, "orders", "o", "delete", 10, 999)])
        self.assertEqual(report["quarantined"], 1)

    def test_tampered_staging_cannot_publish(self):
        self.store.stage("tamper", [event(0)])
        with self.store.connect() as connection:
            connection.execute("UPDATE staged SET body='{}' WHERE batch_id='tamper'")
        with self.assertRaisesRegex(ContractError, "corrupted"):
            self.store.publish("tamper")
        self.assertEqual(self.store.snapshot()["generation"], 0)

    def test_wal_reader_sees_one_atomic_generation(self):
        self.publish("initial", [event(0)])
        with self.store.connect() as reader:
            reader.execute("BEGIN")
            self.assertEqual(reader.execute("SELECT count(*) FROM records").fetchone()[0], 1)
            self.publish("next", [event(1, "new")])
            self.assertEqual(reader.execute("SELECT count(*) FROM records").fetchone()[0], 1)
        self.assertEqual(len(self.store.snapshot()["sources"]["orders"]), 2)

    def test_lineage_points_to_published_dependency_generation(self):
        self.publish("lineage", [event(0)])
        snapshot = self.store.snapshot()
        self.assertTrue(snapshot["lineage"])
        self.assertTrue(
            all(row["input_generation"] <= row["generation"] <= snapshot["generation"] for row in snapshot["lineage"])
        )
        self.assertTrue(
            all(
                row["input_generation"] == snapshot["content_generations"][row["dependency"]]
                for row in snapshot["lineage"]
            )
        )

    def test_noop_filter_does_not_invalidate_downstream_content_lineage(self):
        self.publish("first", [event(0, status="pending")])
        self.publish("second", [event(1, amount=50, status="pending")])
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["content_generations"]["paid_orders"], 0)
        self.assertEqual(snapshot["content_generations"]["entity_totals"], 0)
        self.assertTrue(
            all(
                row["input_generation"] == snapshot["content_generations"][row["dependency"]]
                for row in snapshot["lineage"]
            )
        )

    def test_dag_cycles_missing_columns_and_changed_pipeline_are_rejected(self):
        with self.assertRaises(ContractError):
            topological_order((Model("a", "b", "filter", "status"), Model("b", "a", "filter", "status")), {"orders"})
        with self.assertRaises(ContractError):
            Warehouse(
                Path(self.temporary.name) / "bad.sqlite", models=(Model("bad", "orders", "aggregate", "missing"),)
            )
        with self.assertRaises(ContractError):
            Warehouse(
                Path(self.temporary.name) / "nonnumeric.sqlite",
                models=(Model("bad", "orders", "aggregate", "entity", measure="status"),),
            )
        with self.assertRaisesRegex(ContractError, "DAG"):
            Warehouse(self.path, models=())

    def test_source_ties_are_deterministic_across_partitions(self):
        a, z = event(0, amount=1, partition="a", time=10), event(0, amount=2, partition="z", time=10)
        self.publish("ties", [z, a])
        self.assertEqual(self.store.snapshot()["sources"]["orders"]["o"]["amount_cents"], 2)

    def test_bounded_typed_inputs_and_utf8_are_explicit(self):
        for value in (True, -1, 10**10):
            with self.assertRaises(ContractError):
                event(value)
        with self.assertRaises(ContractError):
            Field("text", "text").validate("\ud800")
        with self.assertRaises(ContractError):
            self.store.stage("empty", [])

    def test_seeded_experiment_matches_full_rebuild_across_seeds(self):
        for seed in (17, 23, 101):
            report = evaluate(seed)
            self.assertEqual(report["checks"], report["passed"])
            self.assertEqual(report["metrics"]["new_offsets"], 120)
            self.assertGreater(report["metrics"]["quarantined"], 0)


if __name__ == "__main__":
    unittest.main()
