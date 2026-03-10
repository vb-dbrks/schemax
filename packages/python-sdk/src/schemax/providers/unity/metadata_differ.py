"""
Metadata Differ — Comparison logic for metadata, properties, tags, constraints, and columns.

Handles catalog/schema metadata diffs, table/view property diffs, table/column tag diffs,
constraint comparison, and deep column field comparison.
"""

from typing import Any, cast

from schemax.providers.base.operations import Operation

from .operation_builders import (
    create_add_constraint_op,
    create_change_column_type_op,
    create_drop_constraint_op,
    create_rename_column_op,
    create_set_column_comment_op,
    create_set_column_tag_op,
    create_set_nullable_op,
    create_set_table_property_op,
    create_set_table_tag_op,
    create_set_view_property_op,
    create_set_view_tag_op,
    create_unset_column_tag_op,
    create_unset_table_property_op,
    create_unset_table_tag_op,
    create_unset_view_property_op,
    create_unset_view_tag_op,
    create_update_catalog_op,
    create_update_schema_op,
)

# ─── Catalog / Schema Metadata ───────────────────────────────────────────────


def diff_catalog_metadata(
    catalog_id: str, old_cat: dict[str, Any], new_cat: dict[str, Any]
) -> list[Operation]:
    """Compare catalog metadata (comment, tags, managedLocationName) and emit update_catalog ops."""
    payload: dict[str, Any] = {}

    old_comment = old_cat.get("comment")
    new_comment = new_cat.get("comment")
    if old_comment != new_comment:
        payload["comment"] = new_comment or ""

    old_tags = old_cat.get("tags") or {}
    new_tags = new_cat.get("tags") or {}
    if old_tags != new_tags:
        payload["tags"] = dict(new_tags)

    old_loc = old_cat.get("managedLocationName") or old_cat.get("managed_location_name")
    new_loc = new_cat.get("managedLocationName") or new_cat.get("managed_location_name")
    if old_loc != new_loc:
        payload["managedLocationName"] = new_loc

    if not payload:
        return []

    return [create_update_catalog_op(catalog_id, payload)]


def diff_schema_metadata(
    schema_id: str, old_sch: dict[str, Any], new_sch: dict[str, Any]
) -> list[Operation]:
    """Compare schema metadata (comment, tags, managedLocationName) and emit update_schema ops."""
    payload: dict[str, Any] = {}

    old_comment = old_sch.get("comment")
    new_comment = new_sch.get("comment")
    if old_comment != new_comment:
        payload["comment"] = new_comment or ""

    old_tags = old_sch.get("tags") or {}
    new_tags = new_sch.get("tags") or {}
    if old_tags != new_tags:
        payload["tags"] = dict(new_tags)

    old_loc = old_sch.get("managedLocationName") or old_sch.get("managed_location_name")
    new_loc = new_sch.get("managedLocationName") or new_sch.get("managed_location_name")
    if old_loc != new_loc:
        payload["managedLocationName"] = new_loc

    if not payload:
        return []

    return [create_update_schema_op(schema_id, payload)]


# ─── Table Properties & Tags ─────────────────────────────────────────────────


def diff_table_properties(
    table_id: str, old_props: dict[str, Any], new_props: dict[str, Any]
) -> list[Operation]:
    """Compare table properties (TBLPROPERTIES)"""
    ops: list[Operation] = []

    old_props = old_props or {}
    new_props = new_props or {}

    for key, value in new_props.items():
        if key not in old_props or old_props[key] != value:
            ops.append(create_set_table_property_op(table_id, key, value))

    for key in old_props:
        if key not in new_props:
            ops.append(create_unset_table_property_op(table_id, key))

    return ops


def diff_table_tags(
    table_id: str, old_tags: dict[str, Any], new_tags: dict[str, Any]
) -> list[Operation]:
    """Compare table tags (Unity Catalog governance tags)"""
    ops: list[Operation] = []

    old_tags = old_tags or {}
    new_tags = new_tags or {}

    for tag_name, tag_value in new_tags.items():
        if tag_name not in old_tags or old_tags[tag_name] != tag_value:
            ops.append(create_set_table_tag_op(table_id, tag_name, str(tag_value)))

    for tag_name in old_tags:
        if tag_name not in new_tags:
            ops.append(create_unset_table_tag_op(table_id, tag_name))

    return ops


# ─── View Properties ─────────────────────────────────────────────────────────


def diff_view_properties(
    view_id: str, old_props: dict[str, Any], new_props: dict[str, Any]
) -> list[Operation]:
    """Compare view properties (TBLPROPERTIES)"""
    ops: list[Operation] = []

    old_props = old_props or {}
    new_props = new_props or {}

    for key, value in new_props.items():
        if key not in old_props or old_props[key] != value:
            ops.append(create_set_view_property_op(view_id, key, value))

    for key in old_props:
        if key not in new_props:
            ops.append(create_unset_view_property_op(view_id, key))

    return ops


# ─── View Tags ────────────────────────────────────────────────────────────────


def diff_view_tags(
    view_id: str, old_tags: dict[str, Any], new_tags: dict[str, Any]
) -> list[Operation]:
    """Compare view tags (Unity Catalog governance tags)"""
    ops: list[Operation] = []

    old_tags = old_tags or {}
    new_tags = new_tags or {}

    for tag_name, tag_value in new_tags.items():
        if tag_name not in old_tags or old_tags[tag_name] != tag_value:
            ops.append(create_set_view_tag_op(view_id, tag_name, str(tag_value)))

    for tag_name in old_tags:
        if tag_name not in new_tags:
            ops.append(create_unset_view_tag_op(view_id, tag_name))

    return ops


# ─── Column Tags ─────────────────────────────────────────────────────────────


def diff_column_tags(
    column_id: str,
    table_id: str,
    column_name: str,
    old_tags: dict[str, Any],
    new_tags: dict[str, Any],
) -> list[Operation]:
    """Compare column tags"""
    ops: list[Operation] = []

    old_tags = old_tags or {}
    new_tags = new_tags or {}

    for tag_name, tag_value in new_tags.items():
        if tag_name not in old_tags or old_tags[tag_name] != tag_value:
            ops.append(
                create_set_column_tag_op(column_id, table_id, column_name, tag_name, str(tag_value))
            )

    for tag_name in old_tags:
        if tag_name not in new_tags:
            ops.append(create_unset_column_tag_op(column_id, table_id, column_name, tag_name))

    return ops


# ─── Constraints ─────────────────────────────────────────────────────────────


def diff_constraints(
    table_id: str,
    old_constraints: list[dict[str, Any]],
    new_constraints: list[dict[str, Any]],
) -> list[Operation]:
    """Compare table constraints (PRIMARY KEY, FOREIGN KEY, CHECK)"""
    ops: list[Operation] = []

    old_constraints = old_constraints or []
    new_constraints = new_constraints or []

    old_constraint_map = {c["id"]: c for c in old_constraints}
    new_constraint_map = {c["id"]: c for c in new_constraints}

    # Added constraints
    for constraint_id, constraint in new_constraint_map.items():
        if constraint_id not in old_constraint_map:
            ops.append(create_add_constraint_op(constraint, table_id))

    # Removed constraints
    for constraint_id, constraint in old_constraint_map.items():
        if constraint_id not in new_constraint_map:
            ops.append(create_drop_constraint_op(constraint, table_id))

    # Modified constraints (Unity Catalog doesn't support ALTER CONSTRAINT,
    # so we must DROP and re-ADD to modify)
    for constraint_id, new_constraint in new_constraint_map.items():
        if constraint_id in old_constraint_map:
            old_constraint = old_constraint_map[constraint_id]
            if _constraint_has_changed(old_constraint, new_constraint):
                ops.append(create_drop_constraint_op(old_constraint, table_id))
                ops.append(create_add_constraint_op(new_constraint, table_id))

    return ops


def _constraint_has_changed(old_constraint: dict[str, Any], new_constraint: dict[str, Any]) -> bool:
    """Check if constraint definition has changed (requires DROP + ADD)"""
    if old_constraint.get("type") != new_constraint.get("type"):
        return True
    if old_constraint.get("name") != new_constraint.get("name"):
        return True
    if old_constraint.get("columns", []) != new_constraint.get("columns", []):
        return True

    constraint_type = old_constraint.get("type")
    if constraint_type == "primary_key":
        return old_constraint.get("timeseries") != new_constraint.get("timeseries")
    if constraint_type == "foreign_key":
        if old_constraint.get("parentTable") != new_constraint.get("parentTable"):
            return True
        old_parent_cols = cast(list[Any], old_constraint.get("parentColumns", []))
        new_parent_cols = cast(list[Any], new_constraint.get("parentColumns", []))
        return old_parent_cols != new_parent_cols
    if constraint_type == "check":
        return old_constraint.get("expression") != new_constraint.get("expression")
    return False


# ─── Existing Column Diff ────────────────────────────────────────────────────


def diff_existing_column(
    col_id: str,
    table_id: str,
    old_col: dict[str, Any],
    new_col: dict[str, Any],
    detect_rename_fn: Any,
) -> list[Operation]:
    """Compare an existing column and return update operations.

    Args:
        col_id: unique identifier of the column
        table_id: parent table identifier
        old_col: previous column state dict
        new_col: current column state dict
        detect_rename_fn: callable(old_id, new_id, old_name, new_name, op_type) -> bool
    """
    ops: list[Operation] = []
    old_name = old_col.get("name")
    new_name = new_col.get("name")
    if (
        old_name
        and new_name
        and old_name != new_name
        and detect_rename_fn(col_id, col_id, old_name, new_name, "rename_column")
    ):
        ops.append(create_rename_column_op(col_id, table_id, old_name, new_name))
    if old_col.get("type") != new_col.get("type") and "type" in new_col:
        ops.append(create_change_column_type_op(col_id, table_id, new_col["type"]))
    if old_col.get("nullable") != new_col.get("nullable"):
        ops.append(create_set_nullable_op(col_id, table_id, new_col["nullable"]))
    if old_col.get("comment") != new_col.get("comment"):
        ops.append(create_set_column_comment_op(col_id, table_id, new_col.get("comment")))
    if "name" in new_col:
        ops.extend(
            diff_column_tags(
                col_id,
                table_id,
                new_col["name"],
                old_col.get("tags", {}),
                new_col.get("tags", {}),
            )
        )
    return ops
