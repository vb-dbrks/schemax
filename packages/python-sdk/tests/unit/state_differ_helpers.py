"""
Shared test helpers for state differ tests.
"""

from datetime import UTC, datetime
from typing import Any

from schemax.providers.base.operations import Operation


def _make_op(op_type: str, target: str, payload: dict[str, Any]) -> Operation:
    return Operation(
        id=f"op_{target}",
        ts=datetime.now(UTC).isoformat(),
        provider="unity",
        op=f"unity.{op_type}",
        target=target,
        payload=payload,
    )


def _base_state(catalogs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"catalogs": catalogs or []}


def _catalog(
    id: str, name: str, schemas: list[dict[str, Any]] | None = None, **kwargs: Any
) -> dict[str, Any]:
    return {"id": id, "name": name, "schemas": schemas or [], **kwargs}


def _schema(
    id: str,
    name: str,
    tables: list[dict[str, Any]] | None = None,
    views: list[dict[str, Any]] | None = None,
    volumes: list[dict[str, Any]] | None = None,
    functions: list[dict[str, Any]] | None = None,
    materialized_views: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    d: dict[str, Any] = {"id": id, "name": name, "tables": tables or []}
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


def _table(
    id: str,
    name: str,
    columns: list[dict[str, Any]] | None = None,
    fmt: str = "delta",
    **kwargs: Any,
) -> dict[str, Any]:
    return {"id": id, "name": name, "format": fmt, "columns": columns or [], **kwargs}


def _col(
    id: str,
    name: str,
    type: str = "STRING",
    nullable: bool = True,
    **kwargs: Any,
) -> dict[str, Any]:
    return {"id": id, "name": name, "type": type, "nullable": nullable, **kwargs}


def _view(id: str, name: str, definition: str = "SELECT 1", **kwargs: Any) -> dict[str, Any]:
    return {"id": id, "name": name, "definition": definition, **kwargs}


def _volume(id: str, name: str, volume_type: str = "managed", **kwargs: Any) -> dict[str, Any]:
    return {"id": id, "name": name, "volumeType": volume_type, **kwargs}


def _function(id: str, name: str, body: str = "RETURN 1", **kwargs: Any) -> dict[str, Any]:
    return {"id": id, "name": name, "language": "SQL", "returnType": "INT", "body": body, **kwargs}


def _mv(id: str, name: str, definition: str = "SELECT 1", **kwargs: Any) -> dict[str, Any]:
    return {"id": id, "name": name, "definition": definition, **kwargs}


def _constraint(id: str, type: str, columns: list[str], **kwargs: Any) -> dict[str, Any]:
    return {"id": id, "type": type, "columns": columns, **kwargs}


def _grant(principal: str, privileges: list[str]) -> dict[str, Any]:
    return {"principal": principal, "privileges": privileges}


def _op_types(ops: list[Operation]) -> list[str]:
    return [o.op for o in ops]


def _ops_of_type(ops: list[Operation], op_type: str) -> list[Operation]:
    return [o for o in ops if o.op == op_type]
