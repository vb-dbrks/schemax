"""
Unity Catalog State Reducer

Applies operations to Unity Catalog state immutably.
Migrated from TypeScript state-reducer.ts
"""

from copy import deepcopy
from typing import Any

from schemax.providers.base.operations import Operation

from .models import (
    UnityCatalog,
    UnityColumn,
    UnityColumnMask,
    UnityConstraint,
    UnityGrant,
    UnityRowFilter,
    UnitySchema,
    UnityState,
    UnityTable,
    UnityView,
)


def apply_operation(state: UnityState, op: Operation) -> UnityState:
    """
    Apply a single operation to Unity Catalog state

    Args:
        state: Current state
        op: Operation to apply (can be Operation object or dict)

    Returns:
        New state with operation applied (immutable)
    """
    # Deep clone state for immutability
    new_state = deepcopy(state)

    # Convert Operation object to dict if necessary
    op_dict: dict[str, Any]
    if not isinstance(op, dict):
        op_dict = op.model_dump()
    else:
        op_dict = op

    # Strip provider prefix from operation type
    op_type = op_dict.get("op", "").replace("unity.", "")

    # Catalog operations
    if op_type == "add_catalog":
        payload = op_dict.get("payload", {})
        catalog = UnityCatalog(
            id=payload["catalogId"],
            name=payload["name"],
            managed_location_name=payload.get("managedLocationName"),
            comment=payload.get("comment"),
            tags=payload.get("tags", {}),
            schemas=[],
            grants=[],
        )
        new_state.catalogs.append(catalog)

    elif op_type == "rename_catalog":
        target = op_dict.get("target")
        payload = op_dict.get("payload", {})
        for catalog in new_state.catalogs:
            if catalog.id == target:
                catalog.name = payload["newName"]
                break

    elif op_type == "update_catalog":
        target = op_dict.get("target")
        payload = op_dict.get("payload", {})
        for catalog in new_state.catalogs:
            if catalog.id == target:
                if "managedLocationName" in payload:
                    catalog.managed_location_name = payload.get("managedLocationName")
                if "comment" in payload:
                    catalog.comment = payload.get("comment")
                if "tags" in payload:
                    catalog.tags = payload.get("tags", {})
                break

    elif op_type == "drop_catalog":
        target = op_dict.get("target")
        new_state.catalogs = [c for c in new_state.catalogs if c.id != target]

    # Schema operations
    elif op_type == "add_schema":
        for catalog in new_state.catalogs:
            if catalog.id == op_dict["payload"]["catalogId"]:
                schema = UnitySchema(
                    id=op_dict["payload"]["schemaId"],
                    name=op_dict["payload"]["name"],
                    managed_location_name=op_dict["payload"].get("managedLocationName"),
                    comment=op_dict["payload"].get("comment"),
                    tags=op_dict["payload"].get("tags", {}),
                    tables=[],
                    views=[],
                    grants=[],
                )
                catalog.schemas.append(schema)
                break

    elif op_type == "rename_schema":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op_dict["target"]:
                    schema.name = op_dict["payload"]["newName"]
                    return new_state

    elif op_type == "update_schema":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op_dict["target"]:
                    if "managedLocationName" in op_dict["payload"]:
                        schema.managed_location_name = op_dict["payload"].get("managedLocationName")
                    if "comment" in op_dict["payload"]:
                        schema.comment = op_dict["payload"].get("comment")
                    if "tags" in op_dict["payload"]:
                        schema.tags = op_dict["payload"].get("tags", {})
                    return new_state

    elif op_type == "drop_schema":
        for catalog in new_state.catalogs:
            catalog.schemas = [s for s in catalog.schemas if s.id != op_dict["target"]]

    # Table operations
    elif op_type == "add_table":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op_dict["payload"]["schemaId"]:
                    table = UnityTable(
                        id=op_dict["payload"]["tableId"],
                        name=op_dict["payload"]["name"],
                        format=op_dict["payload"]["format"],
                        external=op_dict["payload"].get("external"),
                        external_location_name=op_dict["payload"].get("externalLocationName"),
                        path=op_dict["payload"].get("path"),
                        partition_columns=op_dict["payload"].get("partitionColumns"),
                        cluster_columns=op_dict["payload"].get("clusterColumns"),
                        comment=op_dict["payload"].get("comment"),
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
        table_opt = _find_table(new_state, op_dict["target"])
        if table_opt:
            table_opt.name = op_dict["payload"]["newName"]

    elif op_type == "drop_table":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                schema.tables = [t for t in schema.tables if t.id != op_dict["target"]]

    elif op_type == "set_table_comment":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            table_opt.comment = op_dict["payload"]["comment"]

    elif op_type == "set_table_property":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            table_opt.properties[op_dict["payload"]["key"]] = op_dict["payload"]["value"]

    elif op_type == "unset_table_property":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and op_dict["payload"]["key"] in table_opt.properties:
            del table_opt.properties[op_dict["payload"]["key"]]

    elif op_type == "set_table_tag":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            table_opt.tags[op_dict["payload"]["tagName"]] = op_dict["payload"]["tagValue"]

    elif op_type == "unset_table_tag":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and op_dict["payload"]["tagName"] in table_opt.tags:
            del table_opt.tags[op_dict["payload"]["tagName"]]

    # View operations
    elif op_type == "add_view":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                if schema.id == op_dict["payload"]["schemaId"]:
                    view = UnityView(
                        id=op_dict["payload"]["viewId"],
                        name=op_dict["payload"]["name"],
                        definition=op_dict["payload"]["definition"],
                        comment=op_dict["payload"].get("comment"),
                        dependencies=op_dict["payload"].get("dependencies"),
                        extracted_dependencies=op_dict["payload"].get("extractedDependencies"),
                        tags={},
                        properties={},
                        grants=[],
                    )
                    schema.views.append(view)
                    return new_state

    elif op_type == "rename_view":
        view_opt = _find_view(new_state, op_dict["target"])
        if view_opt:
            view_opt.name = op_dict["payload"]["newName"]

    elif op_type == "drop_view":
        for catalog in new_state.catalogs:
            for schema in catalog.schemas:
                schema.views = [v for v in schema.views if v.id != op_dict["target"]]

    elif op_type == "update_view":
        view_opt = _find_view(new_state, op_dict["target"])
        if view_opt:
            if "definition" in op_dict["payload"]:
                view_opt.definition = op_dict["payload"]["definition"]
            if "dependencies" in op_dict["payload"]:
                view_opt.dependencies = op_dict["payload"].get("dependencies")
            if "extractedDependencies" in op_dict["payload"]:
                view_opt.extracted_dependencies = op_dict["payload"].get("extractedDependencies")

    elif op_type == "set_view_comment":
        view_opt = _find_view(new_state, op_dict["payload"]["viewId"])
        if view_opt:
            view_opt.comment = op_dict["payload"]["comment"]

    elif op_type == "set_view_property":
        view_opt = _find_view(new_state, op_dict["payload"]["viewId"])
        if view_opt:
            view_opt.properties[op_dict["payload"]["key"]] = op_dict["payload"]["value"]

    elif op_type == "unset_view_property":
        view_opt = _find_view(new_state, op_dict["payload"]["viewId"])
        if view_opt and op_dict["payload"]["key"] in view_opt.properties:
            del view_opt.properties[op_dict["payload"]["key"]]

    # Column operations
    elif op_type == "add_column":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            column = UnityColumn(
                id=op_dict["payload"]["colId"],
                name=op_dict["payload"]["name"],
                type=op_dict["payload"]["type"],
                nullable=op_dict["payload"]["nullable"],
                comment=op_dict["payload"].get("comment"),
                tags={},
                mask_id=None,
            )
            table_opt.columns.append(column)

    elif op_type == "rename_column":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"]:
                    column.name = op_dict["payload"]["newName"]
                    break

    elif op_type == "drop_column":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            table_opt.columns = [c for c in table_opt.columns if c.id != op_dict["target"]]

    elif op_type == "reorder_columns":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            order = op_dict["payload"]["order"]
            # Sort columns by their position in the order list
            table_opt.columns.sort(key=lambda c: order.index(c.id) if c.id in order else len(order))

    elif op_type == "change_column_type":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"]:
                    column.type = op_dict["payload"]["newType"]
                    break

    elif op_type == "set_nullable":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"]:
                    column.nullable = op_dict["payload"]["nullable"]
                    break

    elif op_type == "set_column_comment":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"]:
                    column.comment = op_dict["payload"]["comment"]
                    break

    # Column tag operations
    elif op_type == "set_column_tag":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"]:
                    if column.tags is None:
                        column.tags = {}
                    column.tags[op_dict["payload"]["tagName"]] = op_dict["payload"]["tagValue"]
                    break

    elif op_type == "unset_column_tag":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            for column in table_opt.columns:
                if column.id == op_dict["target"] and column.tags:
                    if op_dict["payload"]["tagName"] in column.tags:
                        del column.tags[op_dict["payload"]["tagName"]]
                    break

    # Constraint operations
    elif op_type == "add_constraint":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            constraint = UnityConstraint(
                id=op_dict["payload"]["constraintId"],
                type=op_dict["payload"]["type"],
                name=op_dict["payload"].get("name"),
                columns=op_dict["payload"]["columns"],
                timeseries=op_dict["payload"].get("timeseries"),
                parent_table=op_dict["payload"].get("parentTable"),
                parent_columns=op_dict["payload"].get("parentColumns"),
                expression=op_dict["payload"].get("expression"),
            )
            # Insert at specific position if provided (for updates), otherwise append
            insert_at = op_dict["payload"].get("insertAt")
            if insert_at is not None and insert_at >= 0:
                table_opt.constraints.insert(insert_at, constraint)
            else:
                table_opt.constraints.append(constraint)

    elif op_type == "drop_constraint":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            table_opt.constraints = [c for c in table_opt.constraints if c.id != op_dict["target"]]

    # Row filter operations
    elif op_type == "add_row_filter":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            if table_opt.row_filters is None:
                table_opt.row_filters = []
            filter_obj = UnityRowFilter(
                id=op_dict["payload"]["filterId"],
                name=op_dict["payload"]["name"],
                enabled=op_dict["payload"].get("enabled", True),
                udf_expression=op_dict["payload"]["udfExpression"],
                description=op_dict["payload"].get("description"),
            )
            table_opt.row_filters.append(filter_obj)

    elif op_type == "update_row_filter":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and table_opt.row_filters:
            for filter_obj in table_opt.row_filters:
                if filter_obj.id == op_dict["target"]:
                    if "name" in op_dict["payload"]:
                        filter_obj.name = op_dict["payload"]["name"]
                    if "enabled" in op_dict["payload"]:
                        filter_obj.enabled = op_dict["payload"]["enabled"]
                    if "udfExpression" in op_dict["payload"]:
                        filter_obj.udf_expression = op_dict["payload"]["udfExpression"]
                    if "description" in op_dict["payload"]:
                        filter_obj.description = op_dict["payload"]["description"]
                    break

    elif op_type == "remove_row_filter":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and table_opt.row_filters:
            table_opt.row_filters = [f for f in table_opt.row_filters if f.id != op_dict["target"]]

    # Column mask operations
    elif op_type == "add_column_mask":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt:
            if table_opt.column_masks is None:
                table_opt.column_masks = []
            mask = UnityColumnMask(
                id=op_dict["payload"]["maskId"],
                column_id=op_dict["payload"]["columnId"],
                name=op_dict["payload"]["name"],
                enabled=op_dict["payload"].get("enabled", True),
                mask_function=op_dict["payload"]["maskFunction"],
                description=op_dict["payload"].get("description"),
            )
            table_opt.column_masks.append(mask)

            # Link mask to column
            for column in table_opt.columns:
                if column.id == op_dict["payload"]["columnId"]:
                    column.mask_id = op_dict["payload"]["maskId"]
                    break

    elif op_type == "update_column_mask":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and table_opt.column_masks:
            for mask in table_opt.column_masks:
                if mask.id == op_dict["target"]:
                    if "name" in op_dict["payload"]:
                        mask.name = op_dict["payload"]["name"]
                    if "enabled" in op_dict["payload"]:
                        mask.enabled = op_dict["payload"]["enabled"]
                    if "maskFunction" in op_dict["payload"]:
                        mask.mask_function = op_dict["payload"]["maskFunction"]
                    if "description" in op_dict["payload"]:
                        mask.description = op_dict["payload"]["description"]
                    break

    elif op_type == "remove_column_mask":
        table_opt = _find_table(new_state, op_dict["payload"]["tableId"])
        if table_opt and table_opt.column_masks:
            # Find the mask to get column ID
            mask_opt = next((m for m in table_opt.column_masks if m.id == op_dict["target"]), None)
            if mask_opt:
                # Unlink mask from column
                for column in table_opt.columns:
                    if column.id == mask_opt.column_id:
                        column.mask_id = None
                        break
                # Remove mask
                table_opt.column_masks = [
                    m for m in table_opt.column_masks if m.id != op_dict["target"]
                ]

    # Grant operations
    elif op_type == "add_grant":
        payload = op_dict.get("payload", {})
        target_type = payload.get("targetType")
        target_id = payload.get("targetId")
        principal = payload.get("principal")
        privileges = payload.get("privileges") or []
        if not target_type or not target_id or principal is None:
            return new_state
        obj = _find_grant_target(new_state, target_type, target_id)
        if obj is not None:
            grant = UnityGrant(principal=principal, privileges=list(privileges))
            # Replace existing grant for same principal or append
            existing = [g for g in obj.grants if g.principal != principal]
            existing.append(grant)
            obj.grants = existing

    elif op_type == "revoke_grant":
        payload = op_dict.get("payload", {})
        target_type = payload.get("targetType")
        target_id = payload.get("targetId")
        principal = payload.get("principal")
        privileges_to_remove = payload.get("privileges")  # Optional; if None, revoke all
        if not target_type or not target_id or principal is None:
            return new_state
        obj = _find_grant_target(new_state, target_type, target_id)
        if obj is not None:
            if privileges_to_remove is None or len(privileges_to_remove) == 0:
                obj.grants = [g for g in obj.grants if g.principal != principal]
            else:
                set_remove = set(privileges_to_remove)
                new_grants = []
                for g in obj.grants:
                    if g.principal != principal:
                        new_grants.append(g)
                    else:
                        remaining = [p for p in g.privileges if p not in set_remove]
                        if remaining:
                            new_grants.append(UnityGrant(principal=g.principal, privileges=remaining))
                obj.grants = new_grants

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


def _find_view(state: UnityState, view_id: str) -> UnityView | None:
    """
    Find a view by ID across all catalogs and schemas

    Args:
        state: Unity state
        view_id: View ID to find

    Returns:
        View or None if not found
    """
    for catalog in state.catalogs:
        for schema in catalog.schemas:
            for view in schema.views:
                if view.id == view_id:
                    return view
    return None


def _find_grant_target(
    state: UnityState, target_type: str, target_id: str
) -> UnityCatalog | UnitySchema | UnityTable | UnityView | None:
    """
    Find a securable object by type and ID for grant operations.

    Returns:
        The catalog, schema, table, or view, or None if not found.
    """
    if target_type == "catalog":
        for catalog in state.catalogs:
            if catalog.id == target_id:
                return catalog
        return None
    if target_type == "schema":
        for catalog in state.catalogs:
            for schema in catalog.schemas:
                if schema.id == target_id:
                    return schema
        return None
    if target_type == "table":
        return _find_table(state, target_id)
    if target_type == "view":
        return _find_view(state, target_id)
    return None
