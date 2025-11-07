"""
Unity Catalog State Reducer

Applies operations to Unity Catalog state immutably.
Migrated from TypeScript state-reducer.ts
"""

from copy import deepcopy

from schematic.providers.base.operations import Operation
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
            managed_location_name=op.payload.get("managedLocationName"),
            schemas=[],
        )
        new_state.catalogs.append(catalog)

    elif op_type == "rename_catalog":
        for catalog in new_state.catalogs:
            if catalog.id == op.target:
                catalog.name = op.payload["newName"]
                break

    elif op_type == "update_catalog":
        for catalog in new_state.catalogs:
            if catalog.id == op.target:
                if "managedLocationName" in op.payload:
                    catalog.managed_location_name = op.payload.get("managedLocationName")
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
                    managed_location_name=op.payload.get("managedLocationName"),
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

    elif op_type == "update_schema":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op.target:
                    if "managedLocationName" in op.payload:
                        schema.managed_location_name = op.payload.get("managedLocationName")
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
                        external=op.payload.get("external"),
                        external_location_name=op.payload.get("externalLocationName"),
                        path=op.payload.get("path"),
                        partition_columns=op.payload.get("partitionColumns"),
                        cluster_columns=op.payload.get("clusterColumns"),
                        comment=op.payload.get("comment"),
                        columns=[],
                        properties={},
                        tags={},
                        constraints=[],
                        grants=[],
                        column_mapping=None,
                        row_filters=[],
                        column_masks=[],
                    )
                    schema.tables.append(table)
                    return new_state

    elif op_type == "rename_table":
        table_opt = _find_table(new_state, op.target)
        if table_opt:
            table_opt.name = op.payload["newName"]

    elif op_type == "drop_table":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                schema.tables = [t for t in schema.tables if t.id != op.target]

    elif op_type == "set_table_comment":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            table_opt.comment = op.payload["comment"]

    elif op_type == "set_table_property":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            table_opt.properties[op.payload["key"]] = op.payload["value"]

    elif op_type == "unset_table_property":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and op.payload["key"] in table_opt.properties:
            del table_opt.properties[op.payload["key"]]

    elif op_type == "set_table_tag":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            table_opt.tags[op.payload["tagName"]] = op.payload["tagValue"]

    elif op_type == "unset_table_tag":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and op.payload["tagName"] in table_opt.tags:
            del table_opt.tags[op.payload["tagName"]]

    # Column operations
    elif op_type == "add_column":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            column = UnityColumn(
                id=op.payload["colId"],
                name=op.payload["name"],
                type=op.payload["type"],
                nullable=op.payload["nullable"],
                comment=op.payload.get("comment"),
                tags={},
                mask_id=None,
            )
            table_opt.columns.append(column)

    elif op_type == "rename_column":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target:
                    column.name = op.payload["newName"]
                    break

    elif op_type == "drop_column":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            table_opt.columns = [c for c in table_opt.columns if c.id != op.target]

    elif op_type == "reorder_columns":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            order = op.payload["order"]
            # Sort columns by their position in the order list
            table_opt.columns.sort(key=lambda c: order.index(c.id) if c.id in order else len(order))

    elif op_type == "change_column_type":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target:
                    column.type = op.payload["newType"]
                    break

    elif op_type == "set_nullable":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target:
                    column.nullable = op.payload["nullable"]
                    break

    elif op_type == "set_column_comment":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target:
                    column.comment = op.payload["comment"]
                    break

    # Column tag operations
    elif op_type == "set_column_tag":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target:
                    if column.tags is None:
                        column.tags = {}
                    column.tags[op.payload["tagName"]] = op.payload["tagValue"]
                    break

    elif op_type == "unset_column_tag":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op.target and column.tags:
                    if op.payload["tagName"] in column.tags:
                        del column.tags[op.payload["tagName"]]
                    break

    # Constraint operations
    elif op_type == "add_constraint":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
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
            table_opt.constraints.append(constraint)

    elif op_type == "drop_constraint":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            table_opt.constraints = [c for c in table_opt.constraints if c.id != op.target]

    # Row filter operations
    elif op_type == "add_row_filter":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            if table_opt.row_filters is None:
                table_opt.row_filters = []
            filter_obj = UnityRowFilter(
                id=op.payload["filterId"],
                name=op.payload["name"],
                enabled=op.payload.get("enabled", True),
                udf_expression=op.payload["udfExpression"],
                description=op.payload.get("description"),
            )
            table_opt.row_filters.append(filter_obj)

    elif op_type == "update_row_filter":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and table_opt.row_filters:
            for filter_obj in table_opt.row_filters:
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
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and table_opt.row_filters:
            table_opt.row_filters = [f for f in table_opt.row_filters if f.id != op.target]

    # Column mask operations
    elif op_type == "add_column_mask":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt:
            if table_opt.column_masks is None:
                table_opt.column_masks = []
            mask = UnityColumnMask(
                id=op.payload["maskId"],
                column_id=op.payload["columnId"],
                name=op.payload["name"],
                enabled=op.payload.get("enabled", True),
                mask_function=op.payload["maskFunction"],
                description=op.payload.get("description"),
            )
            table_opt.column_masks.append(mask)

            # Link mask to column
            for column in table_opt.columns:
                if column.id == op.payload["columnId"]:
                    column.mask_id = op.payload["maskId"]
                    break

    elif op_type == "update_column_mask":
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and table_opt.column_masks:
            for mask in table_opt.column_masks:
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
        table_opt = _find_table(new_state, op.payload["tableId"])
        if table_opt and table_opt.column_masks:
            # Find the mask to get column ID
            mask_opt = next((m for m in table_opt.column_masks if m.id == op.target), None)
            if mask_opt:
                # Unlink mask from column
                for column in table_opt.columns:
                    if column.id == mask_opt.column_id:
                        column.mask_id = None
                        break
                # Remove mask
                table_opt.column_masks = [m for m in table_opt.column_masks if m.id != op.target]

    return new_state


def apply_operations(state: UnityState, ops: list[Operation]) -> UnityState:
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


def _find_table(state: UnityState, table_id: str) -> UnityTable | None:
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
