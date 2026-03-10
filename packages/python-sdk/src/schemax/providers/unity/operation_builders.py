"""
Operation Builders — Pure data factory functions for Unity Catalog operations.

Each function constructs an Operation representing a single Unity Catalog mutation.
Stateless: no class state, no comparison logic.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from schemax.providers.base.operations import Operation


def _op_id() -> str:
    return f"op_diff_{uuid4().hex[:8]}"


def _ts() -> str:
    return datetime.now(UTC).isoformat()


# ─── Catalog Operations ──────────────────────────────────────────────────────


def create_add_catalog_op(catalog: dict[str, Any]) -> Operation:
    payload: dict[str, Any] = {"catalogId": catalog["id"], "name": catalog["name"]}
    managed_loc = catalog.get("managedLocationName") or catalog.get("managed_location_name")
    if managed_loc is not None:
        payload["managedLocationName"] = managed_loc
    comment = catalog.get("comment")
    if comment is not None:
        payload["comment"] = comment
    tags = catalog.get("tags")
    if tags and isinstance(tags, dict) and len(tags) > 0:
        payload["tags"] = dict(tags)
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_catalog",
        target=catalog["id"],
        payload=payload,
    )


def create_rename_catalog_op(catalog_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_catalog",
        target=catalog_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_drop_catalog_op(catalog: dict[str, Any]) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_catalog",
        target=catalog["id"],
        payload={"name": catalog.get("name", "")},
    )


def create_update_catalog_op(catalog_id: str, payload: dict[str, Any]) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_catalog",
        target=catalog_id,
        payload=payload,
    )


# ─── Schema Operations ───────────────────────────────────────────────────────


def create_add_schema_op(schema: dict[str, Any], catalog_id: str) -> Operation:
    payload: dict[str, Any] = {
        "schemaId": schema["id"],
        "name": schema["name"],
        "catalogId": catalog_id,
    }
    managed_loc = schema.get("managedLocationName") or schema.get("managed_location_name")
    if managed_loc is not None:
        payload["managedLocationName"] = managed_loc
    comment = schema.get("comment")
    if comment is not None:
        payload["comment"] = comment
    tags = schema.get("tags")
    if tags and isinstance(tags, dict) and len(tags) > 0:
        payload["tags"] = dict(tags)
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_schema",
        target=schema["id"],
        payload=payload,
    )


def create_rename_schema_op(schema_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_schema",
        target=schema_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_drop_schema_op(schema: dict[str, Any]) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_schema",
        target=schema["id"],
        payload={},
    )


def create_update_schema_op(schema_id: str, payload: dict[str, Any]) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_schema",
        target=schema_id,
        payload=payload,
    )


# ─── Table Operations ────────────────────────────────────────────────────────


def create_add_table_op(table: dict[str, Any], schema_id: str) -> Operation:
    payload = {
        "tableId": table["id"],
        "name": table["name"],
        "schemaId": schema_id,
        "format": table.get("format", "delta"),
    }
    if "comment" in table and table["comment"]:
        payload["comment"] = table["comment"]
    if "external" in table:
        payload["external"] = table["external"]
    if "externalLocationName" in table:
        payload["externalLocationName"] = table["externalLocationName"]
    if "path" in table:
        payload["path"] = table["path"]
    if "partitionColumns" in table:
        payload["partitionColumns"] = table["partitionColumns"]
    if "clusterColumns" in table:
        payload["clusterColumns"] = table["clusterColumns"]
    properties = table.get("properties")
    if isinstance(properties, dict) and properties:
        payload["properties"] = dict(properties)
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_table",
        target=table["id"],
        payload=payload,
    )


def create_rename_table_op(table_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_table",
        target=table_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_drop_table_op(table: dict[str, Any], catalog_id: str, schema_id: str) -> Operation:
    payload: dict[str, Any] = {}
    if "name" in table:
        payload["name"] = table["name"]
    payload["catalogId"] = catalog_id
    payload["schemaId"] = schema_id
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_table",
        target=table["id"],
        payload=payload,
    )


def create_set_table_comment_op(table_id: str, comment: str | None) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_table_comment",
        target=table_id,
        payload={"tableId": table_id, "comment": comment or ""},
    )


def create_set_table_property_op(table_id: str, key: str, value: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_table_property",
        target=table_id,
        payload={"tableId": table_id, "key": key, "value": value},
    )


def create_unset_table_property_op(table_id: str, key: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.unset_table_property",
        target=table_id,
        payload={"tableId": table_id, "key": key},
    )


def create_set_table_tag_op(table_id: str, tag_name: str, tag_value: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_table_tag",
        target=table_id,
        payload={"tableId": table_id, "tagName": tag_name, "tagValue": tag_value},
    )


def create_unset_table_tag_op(table_id: str, tag_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.unset_table_tag",
        target=table_id,
        payload={"tableId": table_id, "tagName": tag_name},
    )


# ─── View Operations ─────────────────────────────────────────────────────────


def create_add_view_op(view: dict[str, Any], schema_id: str) -> Operation:
    payload = {
        "viewId": view["id"],
        "name": view["name"],
        "schemaId": schema_id,
        "definition": view.get("definition", ""),
    }
    if "comment" in view and view["comment"]:
        payload["comment"] = view["comment"]
    dependencies = view.get("dependencies")
    if isinstance(dependencies, list):
        payload["dependencies"] = dependencies
    extracted_dependencies = view.get("extractedDependencies")
    if isinstance(extracted_dependencies, dict):
        payload["extractedDependencies"] = extracted_dependencies
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_view",
        target=view["id"],
        payload=payload,
    )


def create_rename_view_op(view_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_view",
        target=view_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_update_view_op(view_id: str, view: dict[str, Any]) -> Operation:
    payload = {"definition": view.get("definition", "")}
    extracted_dependencies = view.get("extractedDependencies")
    if isinstance(extracted_dependencies, dict):
        payload["extractedDependencies"] = extracted_dependencies
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_view",
        target=view_id,
        payload=payload,
    )


def create_set_view_comment_op(view_id: str, comment: str | None) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_view_comment",
        target=view_id,
        payload={"viewId": view_id, "comment": comment},
    )


def create_drop_view_op(view: dict[str, Any], catalog_id: str, schema_id: str) -> Operation:
    payload: dict[str, Any] = {}
    if "name" in view:
        payload["name"] = view["name"]
    payload["catalogId"] = catalog_id
    payload["schemaId"] = schema_id
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_view",
        target=view["id"],
        payload=payload,
    )


def create_set_view_property_op(view_id: str, key: str, value: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_view_property",
        target=view_id,
        payload={"viewId": view_id, "key": key, "value": value},
    )


def create_unset_view_property_op(view_id: str, key: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.unset_view_property",
        target=view_id,
        payload={"viewId": view_id, "key": key},
    )


def create_set_view_tag_op(view_id: str, tag_name: str, tag_value: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_view_tag",
        target=view_id,
        payload={"viewId": view_id, "tagName": tag_name, "tagValue": tag_value},
    )


def create_unset_view_tag_op(view_id: str, tag_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.unset_view_tag",
        target=view_id,
        payload={"viewId": view_id, "tagName": tag_name},
    )


# ─── Volume Operations ───────────────────────────────────────────────────────


def create_add_volume_op(volume: dict[str, Any], schema_id: str) -> Operation:
    vol_id = volume.get("id", "")
    payload: dict[str, Any] = {
        "volumeId": vol_id,
        "name": volume.get("name", ""),
        "schemaId": schema_id,
        "volumeType": volume.get("volumeType", volume.get("volume_type", "managed")),
    }
    if volume.get("comment") is not None:
        payload["comment"] = volume["comment"]
    if volume.get("location") is not None:
        payload["location"] = volume["location"]
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_volume",
        target=vol_id,
        payload=payload,
    )


def create_rename_volume_op(volume_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_volume",
        target=volume_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_update_volume_op(volume_id: str, volume: dict[str, Any]) -> Operation:
    payload: dict[str, Any] = {}
    if "comment" in volume:
        payload["comment"] = volume.get("comment")
    if "location" in volume:
        payload["location"] = volume.get("location")
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_volume",
        target=volume_id,
        payload=payload,
    )


def create_drop_volume_op(volume: dict[str, Any], catalog_id: str, schema_id: str) -> Operation:
    payload: dict[str, Any] = {
        "name": volume.get("name", ""),
        "catalogId": catalog_id,
        "schemaId": schema_id,
    }
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_volume",
        target=volume["id"],
        payload=payload,
    )


# ─── Function Operations ─────────────────────────────────────────────────────


def create_add_function_op(func: dict[str, Any], schema_id: str) -> Operation:
    func_id = func.get("id", "")
    payload: dict[str, Any] = {
        "functionId": func_id,
        "name": func.get("name", ""),
        "schemaId": schema_id,
        "language": func.get("language", "SQL"),
        "returnType": func.get("returnType", func.get("return_type")),
        "body": func.get("body", ""),
    }
    if func.get("comment") is not None:
        payload["comment"] = func["comment"]
    if func.get("parameters") is not None:
        payload["parameters"] = func["parameters"]
    if func.get("returnsTable") is not None:
        payload["returnsTable"] = func["returnsTable"]
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_function",
        target=func_id,
        payload=payload,
    )


def create_rename_function_op(function_id: str, old_name: str, new_name: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_function",
        target=function_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_update_function_op(function_id: str, func: dict[str, Any]) -> Operation:
    payload: dict[str, Any] = {
        "body": func.get("body"),
        "returnType": func.get("returnType", func.get("return_type")),
        "parameters": func.get("parameters"),
        "comment": func.get("comment"),
    }
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_function",
        target=function_id,
        payload=payload,
    )


def create_set_function_comment_op(function_id: str, comment: str | None) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_function_comment",
        target=function_id,
        payload={"functionId": function_id, "comment": comment},
    )


def create_drop_function_op(func: dict[str, Any], catalog_id: str, schema_id: str) -> Operation:
    payload: dict[str, Any] = {
        "name": func.get("name", ""),
        "catalogId": catalog_id,
        "schemaId": schema_id,
    }
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_function",
        target=func["id"],
        payload=payload,
    )


# ─── Materialized View Operations ────────────────────────────────────────────


def create_add_materialized_view_op(materialized_view: dict[str, Any], schema_id: str) -> Operation:
    materialized_view_id = materialized_view.get("id", "")
    payload: dict[str, Any] = {
        "materializedViewId": materialized_view_id,
        "name": materialized_view.get("name", ""),
        "schemaId": schema_id,
        "definition": materialized_view.get("definition", "SELECT 1"),
    }
    if materialized_view.get("comment") is not None:
        payload["comment"] = materialized_view["comment"]
    if materialized_view.get("refreshSchedule") is not None:
        payload["refreshSchedule"] = materialized_view["refreshSchedule"]
    if materialized_view.get("refresh_schedule") is not None:
        payload["refreshSchedule"] = materialized_view["refresh_schedule"]
    if materialized_view.get("dependencies") is not None:
        payload["dependencies"] = materialized_view["dependencies"]
    if materialized_view.get("extractedDependencies") is not None:
        payload["extractedDependencies"] = materialized_view["extractedDependencies"]
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_materialized_view",
        target=materialized_view_id,
        payload=payload,
    )


def create_rename_materialized_view_op(
    materialized_view_id: str, old_name: str, new_name: str
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_materialized_view",
        target=materialized_view_id,
        payload={"oldName": old_name, "newName": new_name},
    )


def create_update_materialized_view_op(
    materialized_view_id: str, materialized_view: dict[str, Any]
) -> Operation:
    payload: dict[str, Any] = {}
    definition = materialized_view.get("definition")
    if definition is not None:
        payload["definition"] = definition
    refresh_schedule = materialized_view.get(
        "refreshSchedule", materialized_view.get("refresh_schedule")
    )
    if refresh_schedule is not None:
        payload["refreshSchedule"] = refresh_schedule
    extracted_dependencies = materialized_view.get("extractedDependencies")
    if isinstance(extracted_dependencies, dict):
        payload["extractedDependencies"] = extracted_dependencies
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.update_materialized_view",
        target=materialized_view_id,
        payload=payload,
    )


def create_set_materialized_view_comment_op(
    materialized_view_id: str, comment: str | None
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_materialized_view_comment",
        target=materialized_view_id,
        payload={"materializedViewId": materialized_view_id, "comment": comment},
    )


def create_drop_materialized_view_op(
    materialized_view: dict[str, Any], catalog_id: str, schema_id: str
) -> Operation:
    payload: dict[str, Any] = {
        "name": materialized_view.get("name", ""),
        "catalogId": catalog_id,
        "schemaId": schema_id,
    }
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_materialized_view",
        target=materialized_view["id"],
        payload=payload,
    )


# ─── Column Operations ───────────────────────────────────────────────────────


def create_add_column_op(column: dict[str, Any], table_id: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_column",
        target=column["id"],
        payload={
            "tableId": table_id,
            "colId": column["id"],
            "name": column["name"],
            "type": column["type"],
            "nullable": column.get("nullable", True),
            "comment": column.get("comment"),
        },
    )


def create_rename_column_op(
    column_id: str, table_id: str, old_name: str, new_name: str
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.rename_column",
        target=column_id,
        payload={"tableId": table_id, "oldName": old_name, "newName": new_name},
    )


def create_change_column_type_op(column_id: str, table_id: str, new_type: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.change_column_type",
        target=column_id,
        payload={"tableId": table_id, "newType": new_type},
    )


def create_set_nullable_op(column_id: str, table_id: str, nullable: bool) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_nullable",
        target=column_id,
        payload={"tableId": table_id, "nullable": nullable},
    )


def create_set_column_comment_op(column_id: str, table_id: str, comment: str | None) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_column_comment",
        target=column_id,
        payload={"tableId": table_id, "comment": comment or ""},
    )


def create_drop_column_op(column: dict[str, Any], table_id: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_column",
        target=column["id"],
        payload={"tableId": table_id, "name": column["name"]},
    )


def create_reorder_columns_op(
    table_id: str, new_order: list[str], previous_order: list[str]
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.reorder_columns",
        target=table_id,
        payload={
            "tableId": table_id,
            "order": new_order,
            "previousOrder": previous_order,
        },
    )


# ─── Column Tag Operations ───────────────────────────────────────────────────


def create_set_column_tag_op(
    column_id: str, table_id: str, column_name: str, tag_name: str, tag_value: str
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.set_column_tag",
        target=column_id,
        payload={
            "tableId": table_id,
            "name": column_name,
            "tagName": tag_name,
            "tagValue": tag_value,
        },
    )


def create_unset_column_tag_op(
    column_id: str, table_id: str, column_name: str, tag_name: str
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.unset_column_tag",
        target=column_id,
        payload={
            "tableId": table_id,
            "name": column_name,
            "tagName": tag_name,
        },
    )


# ─── Constraint Operations ───────────────────────────────────────────────────


def create_add_constraint_op(constraint: dict[str, Any], table_id: str) -> Operation:
    payload: dict[str, Any] = {
        "tableId": table_id,
        "constraintId": constraint["id"],
        "type": constraint["type"],
        "columns": constraint["columns"],
    }
    if "name" in constraint and constraint["name"]:
        payload["name"] = constraint["name"]
    if "timeseries" in constraint:
        payload["timeseries"] = constraint["timeseries"]
    if "parentTable" in constraint:
        payload["parentTable"] = constraint["parentTable"]
    if "parentColumns" in constraint:
        payload["parentColumns"] = constraint["parentColumns"]
    if "expression" in constraint:
        payload["expression"] = constraint["expression"]
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_constraint",
        target=constraint["id"],
        payload=payload,
    )


def create_drop_constraint_op(constraint: dict[str, Any], table_id: str) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.drop_constraint",
        target=constraint["id"],
        payload={
            "tableId": table_id,
            "name": constraint.get("name"),
        },
    )


# ─── Grant Operations ────────────────────────────────────────────────────────


def create_add_grant_op(
    target_type: str,
    target_id: str,
    principal: str,
    privileges: list[str],
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.add_grant",
        target=target_id,
        payload={
            "targetType": target_type,
            "targetId": target_id,
            "principal": principal,
            "privileges": privileges,
        },
    )


def create_revoke_grant_op(
    target_type: str,
    target_id: str,
    principal: str,
    privileges: list[str] | None,
) -> Operation:
    return Operation(
        id=_op_id(),
        ts=_ts(),
        provider="unity",
        op="unity.revoke_grant",
        target=target_id,
        payload={
            "targetType": target_type,
            "targetId": target_id,
            "principal": principal,
            "privileges": privileges,
        },
    )
