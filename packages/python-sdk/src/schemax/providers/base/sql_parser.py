"""
SQL Parser for Dependency Extraction

Uses SQLGlot to parse SQL queries and extract dependencies (tables, views, etc.)
for dependency-aware SQL generation.
"""

from typing import Any

import sqlglot
from sqlglot import expressions as exp


def extract_table_references(sql: str, dialect: str = "databricks") -> dict[str, list[str]]:
    """
    Extract all table/view references from a SQL statement.

    Args:
        sql: SQL query string (typically a VIEW definition)
        dialect: SQL dialect (default: databricks)

    Returns:
        Dictionary with categorized references:
        {
            "tables": ["catalog.schema.table1", "catalog.schema.table2"],
            "views": [],  # Cannot distinguish from tables without metadata
            "all": ["catalog.schema.table1", "catalog.schema.table2"]
        }

    Note:
        SQLGlot cannot distinguish between tables and views without additional metadata.
        The caller should resolve this based on the current state.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        # If parsing fails, return empty result
        return {"tables": [], "views": [], "all": []}

    # Extract all table references
    all_refs: list[str] = []
    for table in parsed.find_all(exp.Table):
        # Build fully-qualified name
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        parts.append(table.name)

        table_ref = ".".join(parts)
        all_refs.append(table_ref)

    return {
        "tables": all_refs.copy(),  # Assume all are tables (caller resolves)
        "views": [],  # Caller will populate based on state
        "all": all_refs,
    }


def extract_column_references(sql: str, dialect: str = "databricks") -> list[str]:
    """
    Extract all column references from a SQL statement.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: databricks)

    Returns:
        List of column references (may include table.column format)
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return []

    columns: list[str] = []
    for column in parsed.find_all(exp.Column):
        # Get column name (may be qualified with table)
        column_ref = column.sql(dialect=dialect)
        columns.append(column_ref)

    return columns


def validate_sql_syntax(sql: str, dialect: str = "databricks") -> tuple[bool, str]:
    """
    Validate SQL syntax using SQLGlot.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: databricks)

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        if parsed is None:
            return False, "SQLGlot returned None (invalid SQL)"
        return True, ""
    except Exception as e:
        return False, f"SQL parsing error: {str(e)}"


def resolve_table_or_view(
    table_ref: str, current_state: dict[str, Any], default_catalog: str | None = None
) -> tuple[str, str]:
    """
    Resolve a table reference to determine if it's a table or view, and get its ID.

    Args:
        table_ref: Table reference (e.g., "catalog.schema.table" or "schema.table")
        current_state: Current state dict with catalogs
        default_catalog: Default catalog if not specified in reference

    Returns:
        Tuple of (object_type, object_id) where object_type is "table" or "view"

    Note:
        This function searches the current state to determine if a reference
        points to a table or view. If not found, it assumes "table".
    """
    parsed = _parse_table_reference(table_ref, default_catalog)
    if parsed is None:
        return "table", ""  # Invalid reference
    catalog_name, schema_name, object_name = parsed

    # Search in current state
    catalogs = current_state.get("catalogs", [])

    for catalog in catalogs:
        if catalog.get("name") != catalog_name:
            continue

        for schema in catalog.get("schemas", []):
            if schema_name and schema.get("name") != schema_name:
                continue
            resolved = _resolve_from_schema(schema, object_name)
            if resolved is not None:
                return resolved

    # Not found - assume table (external or not yet created)
    return "table", ""


def _parse_table_reference(
    table_ref: str, default_catalog: str | None
) -> tuple[str, str | None, str] | None:
    """Parse object reference into (catalog_name, schema_name, object_name)."""
    parts = table_ref.split(".")
    if len(parts) == 3:
        catalog_name, schema_name, object_name = parts
        return catalog_name, schema_name, object_name
    if len(parts) == 2:
        schema_name, object_name = parts
        return default_catalog or "__implicit__", schema_name, object_name
    if len(parts) == 1:
        object_name = parts[0]
        return default_catalog or "__implicit__", None, object_name
    return None


def _resolve_from_schema(schema: dict[str, Any], object_name: str) -> tuple[str, str] | None:
    """Resolve object from one schema by name."""
    for table in schema.get("tables", []):
        if table.get("name") == object_name:
            return "table", table.get("id", "")
    for view in schema.get("views", []):
        if view.get("name") == object_name:
            return "view", view.get("id", "")
    return None


def extract_dependencies_from_view(
    view_definition: str,
    current_state: dict[str, Any],
    default_catalog: str | None = None,
    dialect: str = "databricks",
) -> dict[str, list[str]]:
    """
    Extract dependencies from a view definition, categorized by type.

    This is the main entry point for view dependency extraction.

    Args:
        view_definition: SQL query for the view (SELECT statement)
        current_state: Current state dict with catalogs, schemas, tables, views
        default_catalog: Default catalog if not specified in references
        dialect: SQL dialect (default: databricks)

    Returns:
        Dictionary with categorized dependencies:
        {
            "tables": ["table_id_1", "table_id_2"],
            "views": ["view_id_1"],
            "unresolved": ["catalog.schema.unknown"]
        }
    """
    # Extract raw table references
    raw_refs = extract_table_references(view_definition, dialect)

    if "error" in raw_refs:
        # Parsing failed - return empty dependencies
        return {"tables": [], "views": [], "unresolved": raw_refs["all"]}

    # Resolve each reference to table or view
    tables: list[str] = []
    views: list[str] = []
    unresolved: list[str] = []

    for ref in raw_refs["all"]:
        obj_type, obj_id = resolve_table_or_view(ref, current_state, default_catalog)

        if not obj_id:
            # Could not resolve - might be external or not yet created
            unresolved.append(ref)
        elif obj_type == "table":
            tables.append(obj_id)
        elif obj_type == "view":
            views.append(obj_id)

    return {"tables": tables, "views": views, "unresolved": unresolved}
