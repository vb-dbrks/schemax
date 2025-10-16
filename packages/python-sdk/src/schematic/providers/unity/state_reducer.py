"""
Unity Catalog State Reducer

Applies operations to Unity Catalog state immutably.
Migrated from TypeScript state-reducer.ts
"""

from copy import deepcopy
from typing import List, Optional

from ..base.operations import Operation
from .models import (
    UnityCatalog,
    UnityColumn,
    UnityColumnMask,
    UnityConstraint,
    UnityRowFilter,
    UnitySchema,
    UnityState,
    UnityTable,
)


def apply_operation(state: UnityState, op: Operation) -> UnityState:
    """
    Apply a single operation to Unity Catalog state

    Args:
        state: Current state
        op: Operation to apply

    Returns:
        New state with operation applied (immutable)
    """
    # Deep clone state for immutability
    new_state = deepcopy(state)

    # Strip provider prefix from operation type
    op_type = op.op.replace("unity.", "")

    # Catalog operations
    if op_type == "add_catalog":
        catalog = UnityCatalog(
            id=op.payload["catalogId"],
            name=op.payload["name"],
            schemas=[],
        )
        new_state.catalogs.append(catalog)

    elif op_type == "rename_catalog":
        for catalog in new_state.catalogs:
            if catalog.id == op.target:
                catalog.name = op.payload["newName"]
                break

    elif op_type == "drop_catalog":
        new_state.catalogs = [c for c in new_state.catalogs if c.id != op.target]

    # Schema operations
    elif op_type == "add_schema":
        for catalog in new_state.catalogs:
            if catalog.id == op.payload["catalogId"]:
                schema = UnitySchema(
                    id=op.payload["schemaId"],
                    name=op.payload["name"],
                    tables=[],
                )
                catalog.schemas.append(schema)
                break

    elif op_type == "rename_schema":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op.target:
                    schema.name = op.payload["newName"]
                    return new_state

    elif op_type == "drop_schema":
        for catalog in new_state.catalogs:
            catalog.schemas = [s for s in catalog.schemas if s.id != op.target]

    # Table operations
    elif op_type == "add_table":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op.payload["schemaId"]:
                    table = UnityTable(
                        id=op.payload["tableId"],
                        name=op.payload["name"],
                        format=op.payload["format"],
                        columns=[],
                        properties={},
                        constraints=[],
                        grants=[],
                    )
                    schema.tables.append(table)
                    return new_state

    elif op_type == "rename_table":
        table = _find_table(new_state, op.target)
        if table:
            table.name = op.payload["newName"]

    elif op_type == "drop_table":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                schema.tables = [t for t in schema.tables if t.id != op.target]

    elif op_type == "set_table_comment":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            table.comment = op.payload["comment"]

    elif op_type == "set_table_property":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            table.properties[op.payload["key"]] = op.payload["value"]

    elif op_type == "unset_table_property":
        table = _find_table(new_state, op.payload["tableId"])
        if table and op.payload["key"] in table.properties:
            del table.properties[op.payload["key"]]

    # Column operations
    elif op_type == "add_column":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            column = UnityColumn(
                id=op.payload["colId"],
                name=op.payload["name"],
                type=op.payload["type"],
                nullable=op.payload["nullable"],
                comment=op.payload.get("comment"),
            )
            table.columns.append(column)

    elif op_type == "rename_column":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target:
                    column.name = op.payload["newName"]
                    break

    elif op_type == "drop_column":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            table.columns = [c for c in table.columns if c.id != op.target]

    elif op_type == "reorder_columns":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            order = op.payload["order"]
            # Sort columns by their position in the order list
            table.columns.sort(key=lambda c: order.index(c.id) if c.id in order else len(order))

    elif op_type == "change_column_type":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target:
                    column.type = op.payload["newType"]
                    break

    elif op_type == "set_nullable":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target:
                    column.nullable = op.payload["nullable"]
                    break

    elif op_type == "set_column_comment":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target:
                    column.comment = op.payload["comment"]
                    break

    # Column tag operations
    elif op_type == "set_column_tag":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target:
                    if column.tags is None:
                        column.tags = {}
                    column.tags[op.payload["tagName"]] = op.payload["tagValue"]
                    break

    elif op_type == "unset_column_tag":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            for column in table.columns:
                if column.id == op.target and column.tags:
                    if op.payload["tagName"] in column.tags:
                        del column.tags[op.payload["tagName"]]
                    break

    # Constraint operations
    elif op_type == "add_constraint":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            constraint = UnityConstraint(
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

    elif op_type == "drop_constraint":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            table.constraints = [c for c in table.constraints if c.id != op.target]

    # Row filter operations
    elif op_type == "add_row_filter":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            if table.row_filters is None:
                table.row_filters = []
            filter_obj = UnityRowFilter(
                id=op.payload["filterId"],
                name=op.payload["name"],
                enabled=op.payload.get("enabled", True),
                udf_expression=op.payload["udfExpression"],
                description=op.payload.get("description"),
            )
            table.row_filters.append(filter_obj)

    elif op_type == "update_row_filter":
        table = _find_table(new_state, op.payload["tableId"])
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

    elif op_type == "remove_row_filter":
        table = _find_table(new_state, op.payload["tableId"])
        if table and table.row_filters:
            table.row_filters = [f for f in table.row_filters if f.id != op.target]

    # Column mask operations
    elif op_type == "add_column_mask":
        table = _find_table(new_state, op.payload["tableId"])
        if table:
            if table.column_masks is None:
                table.column_masks = []
            mask = UnityColumnMask(
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

    elif op_type == "update_column_mask":
        table = _find_table(new_state, op.payload["tableId"])
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

    elif op_type == "remove_column_mask":
        table = _find_table(new_state, op.payload["tableId"])
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

    return new_state


def apply_operations(state: UnityState, ops: List[Operation]) -> UnityState:
    """
    Apply multiple operations to state

    Args:
        state: Current state
        ops: List of operations to apply

    Returns:
        New state with all operations applied
    """
    current_state = state
    for op in ops:
        current_state = apply_operation(current_state, op)
    return current_state


def _find_table(state: UnityState, table_id: str) -> Optional[UnityTable]:
    """
    Find a table by ID across all catalogs and schemas

    Args:
        state: Unity state
        table_id: Table ID to find

    Returns:
        Table or None if not found
    """
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            for table in schema.tables:
                if table.id == table_id:
                    return table
    return None
