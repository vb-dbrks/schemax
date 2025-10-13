"""
State reducer for applying operations to schema state.

This module contains the logic for applying all 31 operation types to the schema state,
maintaining immutability through deep copying.
"""

from copy import deepcopy
from typing import List, Optional

from .models import (
    State,
    Catalog,
    Schema,
    Table,
    Column,
    Constraint,
    RowFilter,
    ColumnMask,
    Op,
)


def apply_ops_to_state(state: State, ops: List[Op]) -> State:
    """
    Apply a list of operations to the state immutably.

    Args:
        state: The current state
        ops: List of operations to apply

    Returns:
        New state with operations applied
    """
    new_state = deepcopy(state)

    for op in ops:
        new_state = _apply_single_op(new_state, op)

    return new_state


def _apply_single_op(state: State, op: Op) -> State:
    """Apply a single operation to the state"""

    # Catalog operations
    if op.op == "add_catalog":
        return _add_catalog(state, op)
    elif op.op == "rename_catalog":
        return _rename_catalog(state, op)
    elif op.op == "drop_catalog":
        return _drop_catalog(state, op)

    # Schema operations
    elif op.op == "add_schema":
        return _add_schema(state, op)
    elif op.op == "rename_schema":
        return _rename_schema(state, op)
    elif op.op == "drop_schema":
        return _drop_schema(state, op)

    # Table operations
    elif op.op == "add_table":
        return _add_table(state, op)
    elif op.op == "rename_table":
        return _rename_table(state, op)
    elif op.op == "drop_table":
        return _drop_table(state, op)
    elif op.op == "set_table_comment":
        return _set_table_comment(state, op)
    elif op.op == "set_table_property":
        return _set_table_property(state, op)
    elif op.op == "unset_table_property":
        return _unset_table_property(state, op)

    # Column operations
    elif op.op == "add_column":
        return _add_column(state, op)
    elif op.op == "rename_column":
        return _rename_column(state, op)
    elif op.op == "drop_column":
        return _drop_column(state, op)
    elif op.op == "reorder_columns":
        return _reorder_columns(state, op)
    elif op.op == "change_column_type":
        return _change_column_type(state, op)
    elif op.op == "set_nullable":
        return _set_nullable(state, op)
    elif op.op == "set_column_comment":
        return _set_column_comment(state, op)

    # Column tag operations
    elif op.op == "set_column_tag":
        return _set_column_tag(state, op)
    elif op.op == "unset_column_tag":
        return _unset_column_tag(state, op)

    # Constraint operations
    elif op.op == "add_constraint":
        return _add_constraint(state, op)
    elif op.op == "drop_constraint":
        return _drop_constraint(state, op)

    # Row filter operations
    elif op.op == "add_row_filter":
        return _add_row_filter(state, op)
    elif op.op == "update_row_filter":
        return _update_row_filter(state, op)
    elif op.op == "remove_row_filter":
        return _remove_row_filter(state, op)

    # Column mask operations
    elif op.op == "add_column_mask":
        return _add_column_mask(state, op)
    elif op.op == "update_column_mask":
        return _update_column_mask(state, op)
    elif op.op == "remove_column_mask":
        return _remove_column_mask(state, op)

    return state


# Helper function to find a table by ID
def _find_table(state: State, table_id: str) -> Optional[Table]:
    """Find a table by ID across all catalogs and schemas"""
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            for table in schema.tables:
                if table.id == table_id:
                    return table
    return None


# Catalog operations
def _add_catalog(state: State, op: Op) -> State:
    catalog = Catalog(
        id=op.payload["catalogId"],
        name=op.payload["name"],
        schemas=[],
    )
    state.catalogs.append(catalog)
    return state


def _rename_catalog(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        if catalog.id == op.target:
            catalog.name = op.payload["newName"]
            break
    return state


def _drop_catalog(state: State, op: Op) -> State:
    state.catalogs = [c for c in state.catalogs if c.id != op.target]
    return state


# Schema operations
def _add_schema(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        if catalog.id == op.payload["catalogId"]:
            schema = Schema(
                id=op.payload["schemaId"],
                name=op.payload["name"],
                tables=[],
            )
            catalog.schemas.append(schema)
            break
    return state


def _rename_schema(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            if schema.id == op.target:
                schema.name = op.payload["newName"]
                return state
    return state


def _drop_schema(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        catalog.schemas = [s for s in catalog.schemas if s.id != op.target]
    return state


# Table operations
def _add_table(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            if schema.id == op.payload["schemaId"]:
                table = Table(
                    id=op.payload["tableId"],
                    name=op.payload["name"],
                    format=op.payload["format"],
                    columns=[],
                    properties={},
                    constraints=[],
                    grants=[],
                )
                schema.tables.append(table)
                return state
    return state


def _rename_table(state: State, op: Op) -> State:
    table = _find_table(state, op.target)
    if table:
        table.name = op.payload["newName"]
    return state


def _drop_table(state: State, op: Op) -> State:
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            schema.tables = [t for t in schema.tables if t.id != op.target]
    return state


def _set_table_comment(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        table.comment = op.payload["comment"]
    return state


def _set_table_property(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        table.properties[op.payload["key"]] = op.payload["value"]
    return state


def _unset_table_property(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table and op.payload["key"] in table.properties:
        del table.properties[op.payload["key"]]
    return state


# Column operations
def _add_column(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        column = Column(
            id=op.payload["colId"],
            name=op.payload["name"],
            type=op.payload["type"],
            nullable=op.payload["nullable"],
            comment=op.payload.get("comment"),
        )
        table.columns.append(column)
    return state


def _rename_column(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target:
                column.name = op.payload["newName"]
                break
    return state


def _drop_column(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        table.columns = [c for c in table.columns if c.id != op.target]
    return state


def _reorder_columns(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        order = op.payload["order"]
        # Sort columns by their position in the order list
        table.columns.sort(key=lambda c: order.index(c.id) if c.id in order else len(order))
    return state


def _change_column_type(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target:
                column.type = op.payload["newType"]
                break
    return state


def _set_nullable(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target:
                column.nullable = op.payload["nullable"]
                break
    return state


def _set_column_comment(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target:
                column.comment = op.payload["comment"]
                break
    return state


# Column tag operations
def _set_column_tag(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target:
                if column.tags is None:
                    column.tags = {}
                column.tags[op.payload["tagName"]] = op.payload["tagValue"]
                break
    return state


def _unset_column_tag(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        for column in table.columns:
            if column.id == op.target and column.tags:
                if op.payload["tagName"] in column.tags:
                    del column.tags[op.payload["tagName"]]
                break
    return state


# Constraint operations
def _add_constraint(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        constraint = Constraint(
            id=op.payload["constraintId"],
            type=op.payload["type"],
            name=op.payload.get("name"),
            columns=op.payload["columns"],
            timeseries=op.payload.get("timeseries"),
            parent_table=op.payload.get("parentTable"),
            parent_columns=op.payload.get("parentColumns"),
            match_full=op.payload.get("matchFull"),
            on_update=op.payload.get("onUpdate"),
            on_delete=op.payload.get("onDelete"),
            expression=op.payload.get("expression"),
            not_enforced=op.payload.get("notEnforced"),
            deferrable=op.payload.get("deferrable"),
            initially_deferred=op.payload.get("initiallyDeferred"),
            rely=op.payload.get("rely"),
        )
        table.constraints.append(constraint)
    return state


def _drop_constraint(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        table.constraints = [c for c in table.constraints if c.id != op.target]
    return state


# Row filter operations
def _add_row_filter(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        if table.row_filters is None:
            table.row_filters = []
        filter_obj = RowFilter(
            id=op.payload["filterId"],
            name=op.payload["name"],
            enabled=op.payload.get("enabled", True),
            udf_expression=op.payload["udfExpression"],
            description=op.payload.get("description"),
        )
        table.row_filters.append(filter_obj)
    return state


def _update_row_filter(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table and table.row_filters:
        for filter_obj in table.row_filters:
            if filter_obj.id == op.target:
                if "name" in op.payload:
                    filter_obj.name = op.payload["name"]
                if "enabled" in op.payload:
                    filter_obj.enabled = op.payload["enabled"]
                if "udfExpression" in op.payload:
                    filter_obj.udf_expression = op.payload["udfExpression"]
                if "description" in op.payload:
                    filter_obj.description = op.payload["description"]
                break
    return state


def _remove_row_filter(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table and table.row_filters:
        table.row_filters = [f for f in table.row_filters if f.id != op.target]
    return state


# Column mask operations
def _add_column_mask(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table:
        if table.column_masks is None:
            table.column_masks = []
        mask = ColumnMask(
            id=op.payload["maskId"],
            column_id=op.payload["columnId"],
            name=op.payload["name"],
            enabled=op.payload.get("enabled", True),
            mask_function=op.payload["maskFunction"],
            description=op.payload.get("description"),
        )
        table.column_masks.append(mask)

        # Link mask to column
        for column in table.columns:
            if column.id == op.payload["columnId"]:
                column.mask_id = op.payload["maskId"]
                break
    return state


def _update_column_mask(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table and table.column_masks:
        for mask in table.column_masks:
            if mask.id == op.target:
                if "name" in op.payload:
                    mask.name = op.payload["name"]
                if "enabled" in op.payload:
                    mask.enabled = op.payload["enabled"]
                if "maskFunction" in op.payload:
                    mask.mask_function = op.payload["maskFunction"]
                if "description" in op.payload:
                    mask.description = op.payload["description"]
                break
    return state


def _remove_column_mask(state: State, op: Op) -> State:
    table = _find_table(state, op.payload["tableId"])
    if table and table.column_masks:
        # Find the mask to get column ID
        mask = next((m for m in table.column_masks if m.id == op.target), None)
        if mask:
            # Unlink mask from column
            for column in table.columns:
                if column.id == mask.column_id:
                    column.mask_id = None
                    break
            # Remove mask
            table.column_masks = [m for m in table.column_masks if m.id != op.target]
    return state
