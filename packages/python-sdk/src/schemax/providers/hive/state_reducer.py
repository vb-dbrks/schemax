"""State reducer for Hive provider MVP."""

from copy import deepcopy
from typing import Any

from schemax.providers.base.operations import Operation

HiveState = dict[str, Any]


def apply_operation(state: HiveState, operation: Operation) -> HiveState:
    """Apply a single Hive operation to state immutably."""
    new_state = deepcopy(state)
    databases = new_state.setdefault("databases", [])
    op_type = operation.op.replace("hive.", "")

    if op_type == "add_database":
        databases.append(
            {
                "id": operation.target,
                "name": operation.payload.get("name"),
                "tables": [],
                "comment": operation.payload.get("comment"),
            }
        )
        return new_state

    if op_type == "drop_database":
        new_state["databases"] = [db for db in databases if db.get("id") != operation.target]
        return new_state

    if op_type == "add_table":
        database_id = str(operation.payload.get("database", ""))
        for database in databases:
            if database.get("id") != database_id:
                continue
            database.setdefault("tables", []).append(
                {
                    "id": operation.target,
                    "name": operation.payload.get("name"),
                    "columns": operation.payload.get("columns", []),
                    "comment": operation.payload.get("comment"),
                }
            )
            return new_state
        return new_state

    if op_type == "drop_table":
        for database in databases:
            tables = database.setdefault("tables", [])
            database["tables"] = [table for table in tables if table.get("id") != operation.target]

    return new_state


def apply_operations(state: HiveState, operations: list[Operation]) -> HiveState:
    """Apply multiple operations in sequence."""
    current = deepcopy(state)
    for operation in operations:
        current = apply_operation(current, operation)
    return current
