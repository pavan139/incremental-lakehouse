"""Pure, bounded schema and CDC contracts; no dynamic transformation code."""

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import re


class ContractError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def name(value):
    return isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,47}", value) is not None


@dataclass(frozen=True)
class Field:
    name: str
    kind: str
    nullable: bool = False
    default: object = None

    def __post_init__(self):
        if not name(self.name) or self.kind not in {"int", "float", "text", "bool"} or type(self.nullable) is not bool:
            raise ContractError("Invalid typed field")
        if self.default is not None:
            self.validate(self.default)

    def validate(self, value):
        if value is None and self.nullable:
            return None
        try:
            bounded_text = isinstance(value, str) and len(value.encode("utf-8")) <= 1024
        except UnicodeError:
            bounded_text = False
        valid = (
            self.kind == "int"
            and type(value) is int
            and abs(value) <= 10**12
            or self.kind == "float"
            and type(value) in (int, float)
            and abs(value) <= 10**12
            and math.isfinite(value)
            or self.kind == "text"
            and bounded_text
            or self.kind == "bool"
            and type(value) is bool
        )
        if not valid:
            raise ContractError(f"Field {self.name} violates {self.kind}/nullability contract")
        return float(value) if self.kind == "float" else value


@dataclass(frozen=True)
class Schema:
    table: str
    version: int
    fields: tuple[Field, ...]

    def __post_init__(self):
        if not name(self.table) or type(self.version) is not int or not 1 <= self.version <= 1000:
            raise ContractError("Invalid table/version")
        if (
            not isinstance(self.fields, tuple)
            or not 1 <= len(self.fields) <= 32
            or not all(isinstance(f, Field) for f in self.fields)
        ):
            raise ContractError("Schema requires 1..32 typed fields")
        if len({f.name for f in self.fields}) != len(self.fields) or "id" not in {f.name for f in self.fields}:
            raise ContractError("Fields must be unique and contain id")
        key = next(f for f in self.fields if f.name == "id")
        if key.kind != "text" or key.nullable:
            raise ContractError("id must be a nonnullable text field")

    def normalize(self, row):
        if not isinstance(row, dict) or set(row) - {f.name for f in self.fields}:
            raise ContractError("Unknown columns or non-object row")
        result = {f.name: f.validate(row.get(f.name, f.default)) for f in self.fields}
        if not result["id"]:
            raise ContractError("Empty primary key")
        return result

    def evolve_from(self, previous):
        if self.table != previous.table or self.version != previous.version + 1:
            raise ContractError("Schema versions must advance by exactly one")
        older = {field.name: field for field in previous.fields}
        newer = {field.name: field for field in self.fields}
        if not older.keys() <= newer.keys():
            raise ContractError("Dropping columns is incompatible")
        for key, old in older.items():
            new = newer[key]
            if new.kind != old.kind and (old.kind, new.kind) != ("int", "float"):
                raise ContractError("Only int-to-float widening is compatible")
            if old.nullable and not new.nullable:
                raise ContractError("Narrowing nullability is incompatible")
        for key in newer.keys() - older.keys():
            field = newer[key]
            if not field.nullable and field.default is None:
                raise ContractError("Added nonnullable fields require a typed default")

    @classmethod
    def from_dict(cls, row):
        return cls(row["table"], row["version"], tuple(Field(**field) for field in row["fields"]))


@dataclass(frozen=True)
class Event:
    source: str
    partition: str
    offset: int
    table: str
    key: str
    operation: str
    event_time: int
    schema_version: int
    row: dict | None = None

    def __post_init__(self):
        if not all(name(value) for value in (self.source, self.partition, self.table)):
            raise ContractError("Invalid source/partition/table")
        try:
            valid_key = (
                isinstance(self.key, str) and 1 <= len(self.key.encode("utf-8")) <= 128 and "\x00" not in self.key
            )
        except UnicodeError:
            valid_key = False
        if not valid_key:
            raise ContractError("Bounded nonempty key required")
        if any(type(value) is not int for value in (self.offset, self.event_time, self.schema_version)):
            raise ContractError("Offsets, times and schema versions are exact integers")
        if not 0 <= self.offset <= 10**9 or not 0 <= self.event_time <= 10**12 or not 1 <= self.schema_version <= 1000:
            raise ContractError("Offset/time/version outside budget")
        if self.operation not in {"upsert", "delete"} or (self.operation == "delete" and self.row is not None):
            raise ContractError("Operation must be upsert or body-free delete")
        try:
            if len(canonical(asdict(self)).encode()) > 16384:
                raise ContractError("Event exceeds 16KB")
        except (ValueError, TypeError, UnicodeError, RecursionError) as error:
            raise ContractError("Event is not bounded finite JSON") from error

    @property
    def identity(self):
        return self.source, self.partition, self.offset

    @property
    def version(self):
        return self.event_time, self.source, self.partition, self.offset


@dataclass(frozen=True)
class Model:
    name: str
    dependency: str
    kind: str
    column: str
    value: object = None
    measure: str = "amount_cents"

    def __post_init__(self):
        if not all(
            name(value) for value in (self.name, self.dependency, self.column, self.measure)
        ) or self.kind not in {"filter", "aggregate"}:
            raise ContractError("Invalid declarative analytical model")
        canonical(self.value)


def topological_order(models, sources):
    if len(models) > 32 or len({m.name for m in models}) != len(models):
        raise ContractError("At most 32 uniquely named models")
    pending = {model.name: model for model in models}
    if set(pending) & set(sources):
        raise ContractError("Model/source names cannot collide")
    known, ordered = set(sources), []
    while pending:
        ready = sorted(key for key, model in pending.items() if model.dependency in known)
        if not ready:
            raise ContractError("Unknown dependency or cyclic model DAG")
        for key in ready:
            ordered.append(pending.pop(key))
            known.add(key)
    return ordered


DEFAULT_SCHEMA = Schema(
    "orders", 1, (Field("id", "text"), Field("entity", "text"), Field("amount_cents", "int"), Field("status", "text"))
)
DEFAULT_MODELS = (
    Model("paid_orders", "orders", "filter", "status", "paid"),
    Model("entity_totals", "paid_orders", "aggregate", "entity"),
    Model("refunded_orders", "orders", "filter", "status", "refunded"),
    Model("refund_totals", "refunded_orders", "aggregate", "entity"),
)


def quality(row):
    if "amount_cents" in row and (type(row["amount_cents"]) not in (int, float) or row["amount_cents"] < 0):
        raise ContractError("quality: negative amount")
    if "status" in row and row["status"] not in {"paid", "pending", "refunded"}:
        raise ContractError("quality: unsupported order status")
