"""
Bulk Operations — Recursive traversal helpers for adding all objects in new containers.

Used when a catalog, schema, or table is newly created and all nested children
need corresponding add operations.
"""

from typing import Any

from schemax.providers.base.operations import Operation

from .grant_differ import diff_grants
from .operation_builders import (
    create_add_column_op,
    create_add_constraint_op,
    create_add_function_op,
    create_add_materialized_view_op,
    create_add_schema_op,
    create_add_table_op,
    create_add_view_op,
    create_add_volume_op,
    create_set_column_tag_op,
    create_set_table_tag_op,
    create_set_view_tag_op,
)


def add_all_schemas_in_catalog(catalog_id: str, catalog: dict[str, Any]) -> list[Operation]:
    """Add all schemas in a newly created catalog"""
    ops: list[Operation] = []

    for schema in catalog.get("schemas", []):
        sch_id = schema["id"]
        ops.append(create_add_schema_op(schema, catalog_id))
        ops.extend(add_all_tables_in_schema(sch_id, schema))
        ops.extend(add_all_views_in_schema(sch_id, schema))
        ops.extend(add_all_volumes_in_schema(sch_id, schema))
        ops.extend(add_all_functions_in_schema(sch_id, schema))
        ops.extend(add_all_materialized_views_in_schema(sch_id, schema))
        ops.extend(diff_grants("schema", sch_id, [], schema.get("grants", [])))

    return ops


def add_all_tables_in_schema(schema_id: str, schema: dict[str, Any]) -> list[Operation]:
    """Add all tables in a newly created schema"""
    ops: list[Operation] = []

    for table in schema.get("tables", []):
        tbl_id = table["id"]
        ops.append(create_add_table_op(table, schema_id))
        ops.extend(add_all_columns_in_table(tbl_id, table))
        ops.extend(add_all_tags_for_table(tbl_id, table))
        ops.extend(add_all_constraints_for_table(tbl_id, table))
        ops.extend(diff_grants("table", tbl_id, [], table.get("grants", [])))

    return ops


def add_all_views_in_schema(schema_id: str, schema: dict[str, Any]) -> list[Operation]:
    """Add all views in a newly created schema"""
    ops: list[Operation] = []

    for view in schema.get("views", []):
        view_id = view["id"]
        ops.append(create_add_view_op(view, schema_id))
        ops.extend(add_all_tags_for_view(view_id, view))
        ops.extend(diff_grants("view", view_id, [], view.get("grants", [])))

    return ops


def add_all_volumes_in_schema(schema_id: str, schema: dict[str, Any]) -> list[Operation]:
    """Add all volumes in a newly created schema"""
    ops: list[Operation] = []
    for vol in schema.get("volumes", []):
        vol_id = vol["id"]
        ops.append(create_add_volume_op(vol, schema_id))
        ops.extend(diff_grants("volume", vol_id, [], vol.get("grants", [])))
    return ops


def add_all_functions_in_schema(schema_id: str, schema: dict[str, Any]) -> list[Operation]:
    """Add all functions in a newly created schema"""
    ops: list[Operation] = []
    for func in schema.get("functions", []):
        func_id = func["id"]
        ops.append(create_add_function_op(func, schema_id))
        ops.extend(diff_grants("function", func_id, [], func.get("grants", [])))
    return ops


def add_all_materialized_views_in_schema(schema_id: str, schema: dict[str, Any]) -> list[Operation]:
    """Add all materialized views in a newly created schema"""
    ops: list[Operation] = []
    mvs = schema.get("materialized_views", schema.get("materializedViews", []))
    for materialized_view in mvs:
        materialized_view_id = materialized_view["id"]
        ops.append(create_add_materialized_view_op(materialized_view, schema_id))
        ops.extend(
            diff_grants(
                "materialized_view",
                materialized_view_id,
                [],
                materialized_view.get("grants", []),
            )
        )
    return ops


def add_all_columns_in_table(table_id: str, table: dict[str, Any]) -> list[Operation]:
    """Add all columns in a newly created table"""
    ops: list[Operation] = []

    for column in table.get("columns", []):
        if not all(key in column for key in ("id", "name", "type")):
            continue
        ops.append(create_add_column_op(column, table_id))
        ops.extend(create_column_tag_ops(column, table_id))

    return ops


def create_column_tag_ops(column: dict[str, Any], table_id: str) -> list[Operation]:
    """Create tag operations for one column."""
    column_tags = column.get("tags")
    column_name = column.get("name")
    if not column_tags or not isinstance(column_tags, dict) or not column_name:
        return []
    ops: list[Operation] = []
    for tag_name, tag_value in column_tags.items():
        ops.append(
            create_set_column_tag_op(column["id"], table_id, column_name, tag_name, str(tag_value))
        )
    return ops


def add_all_tags_for_view(view_id: str, view: dict[str, Any]) -> list[Operation]:
    """Add all tags for a newly created view"""
    ops: list[Operation] = []

    for tag_name, tag_value in view.get("tags", {}).items():
        ops.append(create_set_view_tag_op(view_id, tag_name, str(tag_value)))

    return ops


def add_all_tags_for_table(table_id: str, table: dict[str, Any]) -> list[Operation]:
    """Add all tags for a newly created table"""
    ops: list[Operation] = []

    for tag_name, tag_value in table.get("tags", {}).items():
        ops.append(create_set_table_tag_op(table_id, tag_name, str(tag_value)))

    return ops


def add_all_constraints_for_table(table_id: str, table: dict[str, Any]) -> list[Operation]:
    """Add all constraints for a newly created table"""
    ops: list[Operation] = []

    for constraint in table.get("constraints", []):
        if not all(key in constraint for key in ("id", "type", "columns")):
            continue
        ops.append(create_add_constraint_op(constraint, table_id))

    return ops
