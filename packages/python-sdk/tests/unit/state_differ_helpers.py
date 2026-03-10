"""
Shared test helpers for state differ tests.
"""

from datetime import UTC, datetime

from schemax.providers.base.operations import Operation


def _make_op(op_type: str, target: str, payload: dict) -> Operation:
    return Operation(
        id=f"op_{target}",
        ts=datetime.now(UTC).isoformat(),
        provider="unity",
        op=f"unity.{op_type}",
        target=target,
        payload=payload,
    )


def _base_state(catalogs=None):
    return {"catalogs": catalogs or []}


def _catalog(id, name, schemas=None, **kwargs):
    return {"id": id, "name": name, "schemas": schemas or [], **kwargs}


def _schema(
    id,
    name,
    tables=None,
    views=None,
    volumes=None,
    functions=None,
    materialized_views=None,
    **kwargs,
):
    d = {"id": id, "name": name, "tables": tables or []}
    if views is not None:
        d["views"] = views
    if volumes is not None:
        d["volumes"] = volumes
    if functions is not None:
        d["functions"] = functions
    if materialized_views is not None:
        d["materializedViews"] = materialized_views
    d.update(kwargs)
    return d


def _table(id, name, columns=None, fmt="delta", **kwargs):
    return {"id": id, "name": name, "format": fmt, "columns": columns or [], **kwargs}


def _col(id, name, type="STRING", nullable=True, **kwargs):
    return {"id": id, "name": name, "type": type, "nullable": nullable, **kwargs}


def _view(id, name, definition="SELECT 1", **kwargs):
    return {"id": id, "name": name, "definition": definition, **kwargs}


def _volume(id, name, volume_type="managed", **kwargs):
    return {"id": id, "name": name, "volumeType": volume_type, **kwargs}


def _function(id, name, body="RETURN 1", **kwargs):
    return {"id": id, "name": name, "language": "SQL", "returnType": "INT", "body": body, **kwargs}


def _mv(id, name, definition="SELECT 1", **kwargs):
    return {"id": id, "name": name, "definition": definition, **kwargs}


def _constraint(id, type, columns, **kwargs):
    return {"id": id, "type": type, "columns": columns, **kwargs}


def _grant(principal, privileges):
    return {"principal": principal, "privileges": privileges}


def _op_types(ops):
    return [o.op for o in ops]


def _ops_of_type(ops, op_type):
    return [o for o in ops if o.op == op_type]
