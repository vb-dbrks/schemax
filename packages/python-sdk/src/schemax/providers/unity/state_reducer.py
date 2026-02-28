"""
Unity Catalog state reducer.

Applies operations to Unity Catalog state immutably.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from copy import deepcopy
from typing import Any

from schemax.providers.base.operations import Operation

from .models import (
    UnityCatalog,
    UnityColumn,
    UnityColumnMask,
    UnityConstraint,
    UnityFunction,
    UnityFunctionParameter,
    UnityGrant,
    UnityMaterializedView,
    UnityRowFilter,
    UnitySchema,
    UnityState,
    UnityTable,
    UnityView,
    UnityVolume,
)

OperationDict = dict[str, Any]
OperationHandler = Callable[[UnityState, OperationDict], None]
GrantTarget = (
    UnityCatalog
    | UnitySchema
    | UnityTable
    | UnityView
    | UnityVolume
    | UnityFunction
    | UnityMaterializedView
)


def apply_operation(state: UnityState, operation: Operation) -> UnityState:
    """Apply a single operation to Unity Catalog state."""
    new_state = deepcopy(state)
    _apply_operation_mutating(new_state, operation)
    return new_state


def apply_operations(state: UnityState, operations: list[Operation]) -> UnityState:
    """Apply multiple operations to Unity Catalog state."""
    if not operations:
        return state
    current_state = deepcopy(state)
    for operation in operations:
        _apply_operation_mutating(current_state, operation)
    return current_state


def _normalize_operation(operation: Operation | OperationDict) -> OperationDict:
    if isinstance(operation, dict):
        return operation
    return operation.model_dump()


def _payload(operation_dict: OperationDict) -> OperationDict:
    payload = operation_dict.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _apply_operation_mutating(state: UnityState, operation: Operation | OperationDict) -> None:
    """Apply one operation in-place to an existing mutable state object."""
    operation_dict = _normalize_operation(operation)
    operation_type = str(operation_dict.get("op", "")).replace("unity.", "")
    handler = _HANDLERS.get(operation_type)
    if handler:
        handler(state, operation_dict)


def _iter_schemas(state: UnityState) -> Iterator[UnitySchema]:
    for catalog in state.catalogs:
        yield from catalog.schemas


def _find_catalog(state: UnityState, catalog_id: str) -> UnityCatalog | None:
    for catalog in state.catalogs:
        if catalog.id == catalog_id:
            return catalog
    return None


def _find_schema(state: UnityState, schema_id: str) -> UnitySchema | None:
    for schema in _iter_schemas(state):
        if schema.id == schema_id:
            return schema
    return None


def _find_table(state: UnityState, table_id: str) -> UnityTable | None:
    for schema in _iter_schemas(state):
        for table in schema.tables:
            if table.id == table_id:
                return table
    return None


def _find_view(state: UnityState, view_id: str) -> UnityView | None:
    for schema in _iter_schemas(state):
        for view in schema.views:
            if view.id == view_id:
                return view
    return None


def _find_volume(state: UnityState, volume_id: str) -> UnityVolume | None:
    for schema in _iter_schemas(state):
        for volume in schema.volumes:
            if volume.id == volume_id:
                return volume
    return None


def _find_function(state: UnityState, function_id: str) -> UnityFunction | None:
    for schema in _iter_schemas(state):
        for function in schema.functions:
            if function.id == function_id:
                return function
    return None


def _find_materialized_view(
    state: UnityState, materialized_view_id: str
) -> UnityMaterializedView | None:
    for schema in _iter_schemas(state):
        for materialized_view in schema.materialized_views:
            if materialized_view.id == materialized_view_id:
                return materialized_view
    return None


def _find_grant_target(state: UnityState, target_type: str, target_id: str) -> GrantTarget | None:
    finders: dict[str, Callable[[UnityState, str], GrantTarget | None]] = {
        "catalog": _find_catalog,
        "schema": _find_schema,
        "table": _find_table,
        "view": _find_view,
        "volume": _find_volume,
        "function": _find_function,
        "materialized_view": _find_materialized_view,
    }
    finder = finders.get(target_type)
    if finder is None:
        return None
    return finder(state, target_id)


def _add_catalog(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    state.catalogs.append(
        UnityCatalog(
            id=payload["catalogId"],
            name=payload["name"],
            managed_location_name=payload.get("managedLocationName"),
            comment=payload.get("comment"),
            tags=payload.get("tags", {}),
            schemas=[],
            grants=[],
        )
    )


def _rename_catalog(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    payload = _payload(operation_dict)
    for catalog in state.catalogs:
        if catalog.id == target_id:
            catalog.name = payload["newName"]
            return


def _update_catalog(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    payload = _payload(operation_dict)
    for catalog in state.catalogs:
        if catalog.id != target_id:
            continue
        if "managedLocationName" in payload:
            catalog.managed_location_name = payload.get("managedLocationName")
        if "comment" in payload:
            catalog.comment = payload.get("comment")
        if "tags" in payload:
            catalog.tags = payload.get("tags", {})
        return


def _drop_catalog(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    state.catalogs = [catalog for catalog in state.catalogs if catalog.id != target_id]


def _add_schema(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    catalog = _find_catalog(state, payload["catalogId"])
    if not catalog:
        return
    catalog.schemas.append(
        UnitySchema(
            id=payload["schemaId"],
            name=payload["name"],
            managed_location_name=payload.get("managedLocationName"),
            comment=payload.get("comment"),
            tags=payload.get("tags", {}),
            tables=[],
            views=[],
            volumes=[],
            functions=[],
            materialized_views=[],
            grants=[],
        )
    )


def _rename_schema(state: UnityState, operation_dict: OperationDict) -> None:
    schema = _find_schema(state, str(operation_dict.get("target", "")))
    if schema:
        schema.name = _payload(operation_dict)["newName"]


def _update_schema(state: UnityState, operation_dict: OperationDict) -> None:
    schema = _find_schema(state, str(operation_dict.get("target", "")))
    if not schema:
        return
    payload = _payload(operation_dict)
    if "managedLocationName" in payload:
        schema.managed_location_name = payload.get("managedLocationName")
    if "comment" in payload:
        schema.comment = payload.get("comment")
    if "tags" in payload:
        schema.tags = payload.get("tags", {})


def _drop_schema(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for catalog in state.catalogs:
        catalog.schemas = [schema for schema in catalog.schemas if schema.id != target_id]


def _add_table(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    schema = _find_schema(state, payload["schemaId"])
    if not schema:
        return
    schema.tables.append(
        UnityTable(
            id=payload["tableId"],
            name=payload["name"],
            format=payload["format"],
            external=payload.get("external"),
            external_location_name=payload.get("externalLocationName"),
            path=payload.get("path"),
            partition_columns=payload.get("partitionColumns"),
            cluster_columns=payload.get("clusterColumns"),
            comment=payload.get("comment"),
            columns=[],
            properties={},
            tags={},
            constraints=[],
            grants=[],
            column_mapping=None,
            row_filters=[],
            column_masks=[],
        )
    )


def _rename_table(state: UnityState, operation_dict: OperationDict) -> None:
    table = _find_table(state, str(operation_dict.get("target", "")))
    if table:
        table.name = _payload(operation_dict)["newName"]


def _drop_table(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for schema in _iter_schemas(state):
        schema.tables = [table for table in schema.tables if table.id != target_id]


def _set_table_comment(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table:
        table.comment = payload["comment"]


def _set_table_property(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table:
        table.properties[payload["key"]] = payload["value"]


def _unset_table_property(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table and payload["key"] in table.properties:
        del table.properties[payload["key"]]


def _set_table_tag(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    target_object_id = payload["tableId"]
    table = _find_table(state, target_object_id)
    if table:
        table.tags[payload["tagName"]] = payload["tagValue"]
        return
    view = _find_view(state, target_object_id)
    if view:
        view.tags[payload["tagName"]] = payload["tagValue"]


def _unset_table_tag(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    target_object_id = payload["tableId"]
    table = _find_table(state, target_object_id)
    if table and payload["tagName"] in table.tags:
        del table.tags[payload["tagName"]]
        return
    view = _find_view(state, target_object_id)
    if view and payload["tagName"] in view.tags:
        del view.tags[payload["tagName"]]


def _add_view(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    schema = _find_schema(state, payload["schemaId"])
    if not schema:
        return
    schema.views.append(
        UnityView(
            id=payload["viewId"],
            name=payload["name"],
            definition=payload["definition"],
            comment=payload.get("comment"),
            dependencies=payload.get("dependencies"),
            extracted_dependencies=payload.get("extractedDependencies"),
            tags={},
            properties={},
            grants=[],
        )
    )


def _rename_view(state: UnityState, operation_dict: OperationDict) -> None:
    view = _find_view(state, str(operation_dict.get("target", "")))
    if view:
        view.name = _payload(operation_dict)["newName"]


def _drop_view(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for schema in _iter_schemas(state):
        schema.views = [view for view in schema.views if view.id != target_id]


def _update_view(state: UnityState, operation_dict: OperationDict) -> None:
    view = _find_view(state, str(operation_dict.get("target", "")))
    if not view:
        return
    payload = _payload(operation_dict)
    if "definition" in payload:
        view.definition = payload["definition"]
    if "dependencies" in payload:
        view.dependencies = payload.get("dependencies")
    if "extractedDependencies" in payload:
        view.extracted_dependencies = payload.get("extractedDependencies")


def _set_view_comment(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    view = _find_view(state, payload["viewId"])
    if view:
        view.comment = payload["comment"]


def _set_view_property(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    view = _find_view(state, payload["viewId"])
    if view:
        view.properties[payload["key"]] = payload["value"]


def _unset_view_property(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    view = _find_view(state, payload["viewId"])
    if view and payload["key"] in view.properties:
        del view.properties[payload["key"]]


def _add_volume(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    schema = _find_schema(state, payload["schemaId"])
    if not schema:
        return
    schema.volumes.append(
        UnityVolume(
            id=payload["volumeId"],
            name=payload["name"],
            volume_type=payload.get("volumeType", "managed"),
            comment=payload.get("comment"),
            location=payload.get("location"),
            grants=[],
        )
    )


def _rename_volume(state: UnityState, operation_dict: OperationDict) -> None:
    volume = _find_volume(state, str(operation_dict.get("target", "")))
    if volume:
        volume.name = _payload(operation_dict)["newName"]


def _update_volume(state: UnityState, operation_dict: OperationDict) -> None:
    volume = _find_volume(state, str(operation_dict.get("target", "")))
    if not volume:
        return
    payload = _payload(operation_dict)
    if "comment" in payload:
        volume.comment = payload.get("comment")
    if "location" in payload:
        volume.location = payload.get("location")


def _drop_volume(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for schema in _iter_schemas(state):
        schema.volumes = [volume for volume in schema.volumes if volume.id != target_id]


def _parse_function_parameters(raw_parameters: Any) -> list[UnityFunctionParameter]:
    parameters = raw_parameters or []
    if not isinstance(parameters, list):
        return []
    return [
        UnityFunctionParameter(
            name=parameter.get("name", ""),
            data_type=parameter.get("dataType", "STRING"),
            default_expression=parameter.get("defaultExpression"),
            comment=parameter.get("comment"),
        )
        for parameter in parameters
        if isinstance(parameter, dict)
    ]


def _add_function(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    schema = _find_schema(state, payload["schemaId"])
    if not schema:
        return
    schema.functions.append(
        UnityFunction(
            id=payload["functionId"],
            name=payload["name"],
            language=payload.get("language", "SQL"),
            return_type=payload.get("returnType"),
            returns_table=payload.get("returnsTable"),
            body=payload["body"],
            comment=payload.get("comment"),
            parameters=_parse_function_parameters(payload.get("parameters")),
            grants=[],
        )
    )


def _rename_function(state: UnityState, operation_dict: OperationDict) -> None:
    function = _find_function(state, str(operation_dict.get("target", "")))
    if function:
        function.name = _payload(operation_dict)["newName"]


def _update_function(state: UnityState, operation_dict: OperationDict) -> None:
    function = _find_function(state, str(operation_dict.get("target", "")))
    if not function:
        return
    payload = _payload(operation_dict)
    if "body" in payload:
        function.body = payload["body"]
    if "returnType" in payload:
        function.return_type = payload.get("returnType")
    if "parameters" in payload:
        function.parameters = _parse_function_parameters(payload.get("parameters"))
    if "comment" in payload:
        function.comment = payload.get("comment")


def _drop_function(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for schema in _iter_schemas(state):
        schema.functions = [function for function in schema.functions if function.id != target_id]


def _set_function_comment(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    function = _find_function(state, payload["functionId"])
    if function:
        function.comment = payload["comment"]


def _add_materialized_view(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    schema = _find_schema(state, payload["schemaId"])
    if not schema:
        return
    schema.materialized_views.append(
        UnityMaterializedView(
            id=payload["materializedViewId"],
            name=payload["name"],
            definition=payload["definition"],
            comment=payload.get("comment"),
            refresh_schedule=payload.get("refreshSchedule"),
            partition_columns=payload.get("partitionColumns"),
            cluster_columns=payload.get("clusterColumns"),
            properties=payload.get("properties", {}),
            dependencies=payload.get("dependencies"),
            extracted_dependencies=payload.get("extractedDependencies"),
            grants=[],
        )
    )


def _rename_materialized_view(state: UnityState, operation_dict: OperationDict) -> None:
    materialized_view = _find_materialized_view(state, str(operation_dict.get("target", "")))
    if materialized_view:
        materialized_view.name = _payload(operation_dict)["newName"]


def _update_materialized_view(state: UnityState, operation_dict: OperationDict) -> None:
    materialized_view = _find_materialized_view(state, str(operation_dict.get("target", "")))
    if not materialized_view:
        return
    payload = _payload(operation_dict)
    if "definition" in payload:
        materialized_view.definition = payload["definition"]
    if "refreshSchedule" in payload:
        materialized_view.refresh_schedule = payload.get("refreshSchedule")
    if "extractedDependencies" in payload:
        materialized_view.extracted_dependencies = payload.get("extractedDependencies")


def _drop_materialized_view(state: UnityState, operation_dict: OperationDict) -> None:
    target_id = operation_dict.get("target")
    for schema in _iter_schemas(state):
        schema.materialized_views = [
            materialized_view
            for materialized_view in schema.materialized_views
            if materialized_view.id != target_id
        ]


def _set_materialized_view_comment(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    materialized_view = _find_materialized_view(state, payload["materializedViewId"])
    if materialized_view:
        materialized_view.comment = payload["comment"]


def _add_column(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    table.columns.append(
        UnityColumn(
            id=payload["colId"],
            name=payload["name"],
            type=payload["type"],
            nullable=payload["nullable"],
            comment=payload.get("comment"),
            tags={},
            mask_id=None,
        )
    )


def _rename_column(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id == target_id:
            column.name = payload["newName"]
            return


def _drop_column(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table:
        target_id = operation_dict.get("target")
        table.columns = [column for column in table.columns if column.id != target_id]


def _reorder_columns(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    order = payload["order"]
    table.columns.sort(
        key=lambda column: order.index(column.id) if column.id in order else len(order)
    )


def _change_column_type(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id == target_id:
            column.type = payload["newType"]
            return


def _set_nullable(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id == target_id:
            column.nullable = payload["nullable"]
            return


def _set_column_comment(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id == target_id:
            column.comment = payload["comment"]
            return


def _set_column_tag(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id != target_id:
            continue
        if column.tags is None:
            column.tags = {}
        column.tags[payload["tagName"]] = payload["tagValue"]
        return


def _unset_column_tag(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    target_id = operation_dict.get("target")
    for column in table.columns:
        if column.id == target_id and column.tags and payload["tagName"] in column.tags:
            del column.tags[payload["tagName"]]
            return


def _add_constraint(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    constraint = UnityConstraint(
        id=payload["constraintId"],
        type=payload["type"],
        name=payload.get("name"),
        columns=payload["columns"],
        timeseries=payload.get("timeseries"),
        parent_table=payload.get("parentTable"),
        parent_columns=payload.get("parentColumns"),
        expression=payload.get("expression"),
    )
    insert_at = payload.get("insertAt")
    if insert_at is not None and insert_at >= 0:
        table.constraints.insert(insert_at, constraint)
        return
    table.constraints.append(constraint)


def _drop_constraint(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table:
        target_id = operation_dict.get("target")
        table.constraints = [
            constraint for constraint in table.constraints if constraint.id != target_id
        ]


def _add_row_filter(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    if table.row_filters is None:
        table.row_filters = []
    table.row_filters.append(
        UnityRowFilter(
            id=payload["filterId"],
            name=payload["name"],
            enabled=payload.get("enabled", True),
            udf_expression=payload["udfExpression"],
            description=payload.get("description"),
        )
    )


def _update_row_filter(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table or not table.row_filters:
        return
    target_id = operation_dict.get("target")
    for row_filter in table.row_filters:
        if row_filter.id != target_id:
            continue
        if "name" in payload:
            row_filter.name = payload["name"]
        if "enabled" in payload:
            row_filter.enabled = payload["enabled"]
        if "udfExpression" in payload:
            row_filter.udf_expression = payload["udfExpression"]
        if "description" in payload:
            row_filter.description = payload["description"]
        return


def _remove_row_filter(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if table and table.row_filters:
        target_id = operation_dict.get("target")
        table.row_filters = [
            row_filter for row_filter in table.row_filters if row_filter.id != target_id
        ]


def _add_column_mask(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table:
        return
    if table.column_masks is None:
        table.column_masks = []
    table.column_masks.append(
        UnityColumnMask(
            id=payload["maskId"],
            column_id=payload["columnId"],
            name=payload["name"],
            enabled=payload.get("enabled", True),
            mask_function=payload["maskFunction"],
            description=payload.get("description"),
        )
    )
    for column in table.columns:
        if column.id == payload["columnId"]:
            column.mask_id = payload["maskId"]
            return


def _update_column_mask(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table or not table.column_masks:
        return
    target_id = operation_dict.get("target")
    for column_mask in table.column_masks:
        if column_mask.id != target_id:
            continue
        if "name" in payload:
            column_mask.name = payload["name"]
        if "enabled" in payload:
            column_mask.enabled = payload["enabled"]
        if "maskFunction" in payload:
            column_mask.mask_function = payload["maskFunction"]
        if "description" in payload:
            column_mask.description = payload["description"]
        return


def _remove_column_mask(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    table = _find_table(state, payload["tableId"])
    if not table or not table.column_masks:
        return
    target_id = operation_dict.get("target")
    mask = next((entry for entry in table.column_masks if entry.id == target_id), None)
    if not mask:
        return
    for column in table.columns:
        if column.id == mask.column_id:
            column.mask_id = None
            break
    table.column_masks = [entry for entry in table.column_masks if entry.id != target_id]


def _add_grant(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    target_type = payload.get("targetType")
    target_id = payload.get("targetId")
    principal = payload.get("principal")
    privileges = payload.get("privileges") or []
    if not target_type or not target_id or principal is None:
        return
    grant_target = _find_grant_target(state, target_type, target_id)
    if grant_target is None:
        return
    existing_grants = [grant for grant in grant_target.grants if grant.principal != principal]
    existing_grants.append(UnityGrant(principal=principal, privileges=list(privileges)))
    grant_target.grants = existing_grants


def _revoke_grant(state: UnityState, operation_dict: OperationDict) -> None:
    payload = _payload(operation_dict)
    target_type = payload.get("targetType")
    target_id = payload.get("targetId")
    principal = payload.get("principal")
    privileges_to_remove = payload.get("privileges")
    if not target_type or not target_id or principal is None:
        return
    grant_target = _find_grant_target(state, target_type, target_id)
    if grant_target is None:
        return
    if not privileges_to_remove:
        grant_target.grants = [
            grant for grant in grant_target.grants if grant.principal != principal
        ]
        return

    privileges_to_remove_set = set(privileges_to_remove)
    updated_grants: list[UnityGrant] = []
    for grant in grant_target.grants:
        if grant.principal != principal:
            updated_grants.append(grant)
            continue
        remaining_privileges = [
            privilege for privilege in grant.privileges if privilege not in privileges_to_remove_set
        ]
        if remaining_privileges:
            updated_grants.append(
                UnityGrant(principal=grant.principal, privileges=remaining_privileges)
            )
    grant_target.grants = updated_grants


_HANDLERS: dict[str, OperationHandler] = {
    "add_catalog": _add_catalog,
    "rename_catalog": _rename_catalog,
    "update_catalog": _update_catalog,
    "drop_catalog": _drop_catalog,
    "add_schema": _add_schema,
    "rename_schema": _rename_schema,
    "update_schema": _update_schema,
    "drop_schema": _drop_schema,
    "add_table": _add_table,
    "rename_table": _rename_table,
    "drop_table": _drop_table,
    "set_table_comment": _set_table_comment,
    "set_table_property": _set_table_property,
    "unset_table_property": _unset_table_property,
    "set_table_tag": _set_table_tag,
    "unset_table_tag": _unset_table_tag,
    "add_view": _add_view,
    "rename_view": _rename_view,
    "drop_view": _drop_view,
    "update_view": _update_view,
    "set_view_comment": _set_view_comment,
    "set_view_property": _set_view_property,
    "unset_view_property": _unset_view_property,
    "add_volume": _add_volume,
    "rename_volume": _rename_volume,
    "update_volume": _update_volume,
    "drop_volume": _drop_volume,
    "add_function": _add_function,
    "rename_function": _rename_function,
    "update_function": _update_function,
    "drop_function": _drop_function,
    "set_function_comment": _set_function_comment,
    "add_materialized_view": _add_materialized_view,
    "rename_materialized_view": _rename_materialized_view,
    "update_materialized_view": _update_materialized_view,
    "drop_materialized_view": _drop_materialized_view,
    "set_materialized_view_comment": _set_materialized_view_comment,
    "add_column": _add_column,
    "rename_column": _rename_column,
    "drop_column": _drop_column,
    "reorder_columns": _reorder_columns,
    "change_column_type": _change_column_type,
    "set_nullable": _set_nullable,
    "set_column_comment": _set_column_comment,
    "set_column_tag": _set_column_tag,
    "unset_column_tag": _unset_column_tag,
    "add_constraint": _add_constraint,
    "drop_constraint": _drop_constraint,
    "add_row_filter": _add_row_filter,
    "update_row_filter": _update_row_filter,
    "remove_row_filter": _remove_row_filter,
    "add_column_mask": _add_column_mask,
    "update_column_mask": _update_column_mask,
    "remove_column_mask": _remove_column_mask,
    "add_grant": _add_grant,
    "revoke_grant": _revoke_grant,
}
