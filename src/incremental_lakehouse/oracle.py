"""Full-rebuild reference: recompute winners and aggregate scans from event history."""

from dataclasses import asdict
from fractions import Fraction

from .contracts import ContractError, canonical, digest, quality


def rebuild(events, schema_history, models):
    versions = {(schema.table, schema.version): schema for schema in schema_history}
    latest = {}
    for schema in schema_history:
        if schema.table not in latest or schema.version > latest[schema.table].version:
            latest[schema.table] = schema
    identities, winners, rejected = {}, {}, 0
    for event in events:
        fingerprint = digest(asdict(event))
        if event.identity in identities:
            if identities[event.identity] != fingerprint:
                raise ContractError("Conflicting oracle source offset")
            continue
        identities[event.identity] = fingerprint
        try:
            historical = versions[(event.table, event.schema_version)]
            if event.operation == "upsert":
                row = latest[event.table].normalize(historical.normalize(event.row))
                if row["id"] != event.key:
                    raise ContractError("Key mismatch")
                quality(row)
            else:
                row = None
        except (ContractError, KeyError):
            rejected += 1
            continue
        key = (event.table, event.key)
        previous = winners.get(key)
        if previous is None or event.version > previous[0]:
            winners[key] = (event.version, row)
    sources = {}
    for (table, key), (_, row) in winners.items():
        if row is not None:
            sources.setdefault(table, {})[key] = row
    derived = {}
    for model in models:
        source = sources.get(model.dependency, derived.get(model.dependency, {}))
        if model.kind == "filter":
            derived[model.name] = {key: dict(row) for key, row in source.items() if row[model.column] == model.value}
        else:
            groups = {}
            for row in source.values():
                groups.setdefault(canonical(row[model.column]), []).append(row)
            derived[model.name] = {}
            for key, rows in groups.items():
                total = sum((Fraction(row[model.measure]) for row in rows), Fraction())
                derived[model.name][key] = {
                    model.column: rows[0][model.column],
                    "order_count": len(rows),
                    "total_amount_cents": total.numerator if total.denominator == 1 else float(total),
                }
    return {"sources": sources, "models": derived, "quarantine_count": rejected}
