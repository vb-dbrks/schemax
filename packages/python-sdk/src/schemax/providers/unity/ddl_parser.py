"""
Unity Catalog DDL parser and state builder.

Parses Databricks DDL statements (CREATE CATALOG/SCHEMA/TABLE/VIEW, COMMENT ON, etc.)
into a structured form and builds UnityState from them. Used for import-from-SQL file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import expressions as exp

from schemax.core.sql_utils import split_sql_statements
from schemax.providers.base.models import ProviderState
from schemax.providers.base.sql_parser import extract_table_references

from .models import (
    UnityCatalog,
    UnityColumn,
    UnitySchema,
    UnityState,
    UnityTable,
    UnityView,
)

# --- Parsed DDL result types (SRP: parser produces these; builder consumes) ---


@dataclass(frozen=True)
class CreateCatalog:
    """Parsed CREATE CATALOG."""

    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CreateSchema:
    """Parsed CREATE SCHEMA (catalog.schema or schema)."""

    catalog: str
    name: str
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnDef:
    """Parsed column definition from CREATE TABLE."""

    name: str
    type: str
    nullable: bool = True
    comment: str | None = None


@dataclass(frozen=True)
class CreateTable:
    """Parsed CREATE TABLE."""

    catalog: str
    schema_name: str
    name: str
    columns: list[ColumnDef]
    format: str = "delta"  # delta | iceberg
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CreateView:
    """Parsed CREATE VIEW."""

    catalog: str
    schema_name: str
    name: str
    definition: str
    comment: str | None = None


@dataclass(frozen=True)
class CommentOn:
    """Parsed COMMENT ON catalog/schema/table."""

    object_type: str  # catalog | schema | table | view
    catalog: str
    schema_name: str | None
    name: str
    comment: str


# --- ALTER result types (applied in statement order in the SQL file) ---


@dataclass(frozen=True)
class AlterTableAddColumn:
    """ALTER TABLE ... ADD COLUMN."""

    catalog: str
    schema_name: str
    table_name: str
    column: ColumnDef


@dataclass(frozen=True)
class AlterTableDropColumn:
    """ALTER TABLE ... DROP COLUMN."""

    catalog: str
    schema_name: str
    table_name: str
    column_name: str


@dataclass(frozen=True)
class AlterTableRenameColumn:
    """ALTER TABLE ... RENAME COLUMN old TO new."""

    catalog: str
    schema_name: str
    table_name: str
    old_name: str
    new_name: str


@dataclass(frozen=True)
class AlterTableAlterColumn:
    """ALTER TABLE ... ALTER COLUMN ... SET NOT NULL / SET DATA TYPE."""

    catalog: str
    schema_name: str
    table_name: str
    column_name: str
    nullable: bool | None = None
    new_type: str | None = None


@dataclass(frozen=True)
class AlterTableRenameTo:
    """ALTER TABLE ... RENAME TO new_name."""

    catalog: str
    schema_name: str
    table_name: str
    new_name: str


@dataclass(frozen=True)
class AlterTableSetTblproperties:
    """ALTER TABLE ... SET TBLPROPERTIES."""

    catalog: str
    schema_name: str
    table_name: str
    properties: dict[str, str]


@dataclass(frozen=True)
class AlterCatalogSetTags:
    """ALTER CATALOG ... SET TAGS."""

    name: str
    tags: dict[str, str]


@dataclass(frozen=True)
class AlterSchemaSetTags:
    """ALTER SCHEMA ... SET TAGS."""

    catalog: str
    schema_name: str
    tags: dict[str, str]


@dataclass(frozen=True)
class AlterTableSetTags:
    """ALTER TABLE ... SET TAGS."""

    catalog: str
    schema_name: str
    table_name: str
    tags: dict[str, str]


@dataclass(frozen=True)
class Unsupported:
    """Statement parsed but not supported for state build."""

    reason: str
    index: int


@dataclass(frozen=True)
class ParseError:
    """Statement failed to parse."""

    index: int
    message: str


@dataclass(frozen=True)
class MultiResult:
    """Multiple results from one statement (e.g. ALTER TABLE with several actions). Applied in order."""

    results: tuple[DDLStatementResult, ...]


AlterResult = (
    AlterTableAddColumn
    | AlterTableDropColumn
    | AlterTableRenameColumn
    | AlterTableAlterColumn
    | AlterTableRenameTo
    | AlterTableSetTblproperties
    | AlterCatalogSetTags
    | AlterSchemaSetTags
    | AlterTableSetTags
)

DDLStatementResult = (
    CreateCatalog
    | CreateSchema
    | CreateTable
    | CreateView
    | CommentOn
    | AlterTableAddColumn
    | AlterTableDropColumn
    | AlterTableRenameColumn
    | AlterTableAlterColumn
    | AlterTableRenameTo
    | AlterTableSetTblproperties
    | AlterCatalogSetTags
    | AlterSchemaSetTags
    | AlterTableSetTags
    | MultiResult
    | Unsupported
    | ParseError
)


def _get_name(expr: exp.Expression | None) -> str:
    """Get identifier name from sqlglot expression."""
    if expr is None:
        return ""
    if isinstance(expr, exp.Identifier):
        return expr.name or ""
    if hasattr(expr, "name"):
        return getattr(expr, "name", "") or ""
    if hasattr(expr, "this"):
        return _get_name(getattr(expr, "this", None))
    return ""


def _get_comment_from_create(create_expr: exp.Create) -> str | None:
    """Extract COMMENT from CREATE statement if present."""
    # Some dialects use expression property; Databricks may use COMMENT '...'
    for prop in create_expr.find_all(exp.SchemaCommentProperty):
        if prop.this:
            return prop.this.this if hasattr(prop.this, "this") else str(prop.this)
    return None


def _strip_quoted_name(value: str) -> str:
    return value.rstrip(";").strip("`\"'")


def _parse_command_statement(sql: str, index: int) -> DDLStatementResult:
    sql_upper = sql.upper()
    if "CREATE CATALOG" in sql_upper:
        match = re.match(
            r"CREATE\s+CATALOG\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        catalog_name = _strip_quoted_name(match.group(1)) if match else "unknown"
        comment_match = re.search(r"COMMENT\s+['\"]([^'\"]*)['\"]", sql, re.IGNORECASE)
        comment = comment_match.group(1) if comment_match else None
        return CreateCatalog(name=catalog_name, comment=comment)

    if "CREATE SCHEMA" in sql_upper:
        match = re.match(
            r"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+(?:\.[^\s]+)?)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        full_name = _strip_quoted_name(match.group(1)) if match else "unknown"
        parts = full_name.split(".", 1)
        catalog = parts[0] if len(parts) == 2 else "__implicit__"
        schema_name = parts[1] if len(parts) == 2 else full_name
        comment_match = re.search(r"COMMENT\s+['\"]([^'\"]*)['\"]", sql, re.IGNORECASE)
        comment = comment_match.group(1) if comment_match else None
        return CreateSchema(catalog=catalog, name=schema_name, comment=comment)

    if "ALTER TABLE" in sql_upper and "DROP COLUMN" in sql_upper:
        match = re.match(
            r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?([^\s,;]+)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return Unsupported(reason="command", index=index)
        full_table = _strip_quoted_name(match.group(1))
        column_name = _strip_quoted_name(match.group(2))
        parts = full_table.split(".")
        if len(parts) == 3:
            catalog, schema_name, table_name = parts
        elif len(parts) == 2:
            catalog, schema_name, table_name = "__implicit__", parts[0], parts[1]
        else:
            catalog, schema_name, table_name = "__implicit__", "", full_table
        return AlterTableDropColumn(
            catalog=catalog,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
        )
    return Unsupported(reason="command", index=index)


def _parse_comment_statement(parsed: exp.Comment, index: int) -> DDLStatementResult:
    target = parsed.this
    comment = ""
    if hasattr(parsed, "expression") and parsed.expression:
        comment = parsed.expression.sql(dialect="databricks").strip("'\"")
    if isinstance(target, exp.Table):
        catalog = target.catalog or "__implicit__"
        schema_name = target.db or ""
        name = _get_name(target.this) or target.name or ""
        return CommentOn(
            object_type="table",
            catalog=str(catalog),
            schema_name=schema_name or None,
            name=name,
            comment=comment,
        )
    return Unsupported(reason=f"comment_on_shape:{type(target).__name__}", index=index)


def _parse_column_def(column_expr: exp.ColumnDef) -> ColumnDef:
    column_name = _get_name(column_expr.this) or (column_expr.this.name if column_expr.this else "")
    column_type = "STRING"
    if column_expr.kind:
        column_type = (
            column_expr.kind.sql(dialect="databricks")
            if hasattr(column_expr.kind, "sql")
            else str(column_expr.kind)
        )
    nullable = not (column_expr.args.get("not_null") or getattr(column_expr, "not_null", False))
    comment = None
    if hasattr(column_expr, "comment") and column_expr.comment:
        comment = (
            column_expr.comment.this
            if hasattr(column_expr.comment, "this")
            else str(column_expr.comment)
        )
    return ColumnDef(name=column_name, type=column_type, nullable=nullable, comment=comment)


def _parse_create_catalog(create_expr: exp.Create, index: int) -> DDLStatementResult:
    this_expr = create_expr.this
    if not isinstance(this_expr, (exp.Identifier, exp.Table)):
        return Unsupported(reason="create_catalog_shape", index=index)
    name = this_expr.name or _get_name(this_expr) or ""
    if not name and hasattr(this_expr, "this"):
        name = _get_name(this_expr.this)
    return CreateCatalog(name=name or "unknown", comment=_get_comment_from_create(create_expr))


def _parse_create_schema(create_expr: exp.Create, index: int) -> DDLStatementResult:
    this_expr = create_expr.this
    if isinstance(this_expr, exp.Table):
        catalog = this_expr.catalog or "__implicit__"
        if isinstance(catalog, exp.Identifier):
            catalog = catalog.name or "__implicit__"
        schema_name = this_expr.db or _get_name(this_expr.this) or this_expr.name or ""
        return CreateSchema(
            catalog=str(catalog),
            name=schema_name or "unknown",
            comment=_get_comment_from_create(create_expr),
        )
    if isinstance(this_expr, exp.Schema):
        catalog = getattr(this_expr, "catalog", None) or "__implicit__"
        if isinstance(catalog, exp.Identifier):
            catalog = catalog.name or "__implicit__"
        schema_name = _get_name(this_expr.this) or this_expr.name or ""
        return CreateSchema(
            catalog=str(catalog),
            name=schema_name or "unknown",
            comment=_get_comment_from_create(create_expr),
        )
    if isinstance(this_expr, exp.Identifier):
        return CreateSchema(catalog="__implicit__", name=this_expr.name or "unknown", comment=None)
    return Unsupported(reason="create_schema_shape", index=index)


def _parse_create_table(create_expr: exp.Create, sql: str, index: int) -> DDLStatementResult:
    schema_expr = create_expr.this
    if not isinstance(schema_expr, exp.Schema):
        return Unsupported(reason="create_table_no_schema", index=index)
    table_ref = schema_expr.this
    catalog = "__implicit__"
    schema_name = ""
    table_name = ""
    if isinstance(table_ref, exp.Table):
        catalog = table_ref.catalog or "__implicit__"
        schema_name = table_ref.db or ""
        table_name = _get_name(table_ref.this) or table_ref.name or ""
    elif hasattr(schema_expr, "name"):
        table_name = schema_expr.name or ""

    columns = [
        _parse_column_def(expr)
        for expr in schema_expr.expressions or []
        if isinstance(expr, exp.ColumnDef)
    ]
    format_value = "delta"
    for file_format_prop in create_expr.find_all(exp.FileFormatProperty):
        if file_format_prop.this:
            format_value = _get_name(file_format_prop.this) or "delta"
            break
    if format_value == "delta":
        format_match = re.search(r"\bUSING\s+(DELTA|ICEBERG)\b", sql, re.IGNORECASE)
        if format_match:
            format_value = format_match.group(1).lower()
    return CreateTable(
        catalog=str(catalog),
        schema_name=str(schema_name),
        name=table_name or "unknown",
        columns=columns,
        format=format_value.lower(),
        comment=_get_comment_from_create(create_expr),
    )


def _parse_create_view(create_expr: exp.Create, index: int) -> DDLStatementResult:
    schema_expr = create_expr.this
    if not isinstance(schema_expr, exp.Schema):
        return Unsupported(reason="create_view_no_schema", index=index)
    view_ref = schema_expr.this
    catalog = "__implicit__"
    schema_name = ""
    view_name = ""
    if isinstance(view_ref, exp.Table):
        catalog = view_ref.catalog or "__implicit__"
        schema_name = view_ref.db or ""
        view_name = _get_name(view_ref.this) or view_ref.name or ""
    definition = ""
    if schema_expr.expressions:
        definition = schema_expr.expressions[0].sql(dialect="databricks")
    return CreateView(
        catalog=str(catalog),
        schema_name=str(schema_name),
        name=view_name or "unknown",
        definition=definition,
    )


def _parse_create_statement(create_expr: exp.Create, sql: str, index: int) -> DDLStatementResult:
    kind = (getattr(create_expr, "kind", None) or "").upper()
    this_expr = create_expr.this
    if kind == "CATALOG" or (kind == "" and this_expr and "catalog" in sql.lower()[:30]):
        return _parse_create_catalog(create_expr, index)
    if kind == "SCHEMA" or (kind == "" and this_expr and "schema" in sql.lower()[:30]):
        return _parse_create_schema(create_expr, index)
    if kind == "TABLE":
        return _parse_create_table(create_expr, sql, index)
    if kind == "VIEW":
        return _parse_create_view(create_expr, index)
    return Unsupported(reason=f"create_kind:{kind}", index=index)


def _extract_table_coordinates(target: exp.Table) -> tuple[str, str, str]:
    catalog = target.catalog or "__implicit__"
    if isinstance(catalog, exp.Identifier):
        catalog_name = catalog.name or "__implicit__"
    else:
        catalog_name = str(catalog)
    schema_name = target.db or ""
    if isinstance(schema_name, exp.Identifier):
        resolved_schema_name = schema_name.name or ""
    else:
        resolved_schema_name = str(schema_name)
    table_name = _get_name(target.this) or target.name or ""
    return catalog_name, resolved_schema_name, table_name


def _parse_tblproperties_action(action: exp.Expression) -> dict[str, str]:
    if not isinstance(action, exp.AlterSet) or not action.expressions:
        return {}
    for expression in action.expressions:
        if not isinstance(expression, exp.Properties) or not expression.expressions:
            continue
        properties: dict[str, str] = {}
        for prop in expression.expressions:
            if not isinstance(prop, exp.Property):
                continue
            key_obj = prop.this
            key = (
                str(key_obj.this).strip("'\"")
                if hasattr(key_obj, "this")
                else str(key_obj).strip("'\"")
            )
            value_obj = prop.args.get("value")
            if value_obj is None:
                properties[key] = ""
                continue
            properties[key] = (
                value_obj.sql(dialect="databricks").strip("'\"")
                if hasattr(value_obj, "sql")
                else str(value_obj)
            )
        return properties
    return {}


def _parse_alter_add_column_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    if isinstance(action, exp.ColumnDef):
        return AlterTableAddColumn(
            catalog=catalog,
            schema_name=schema_name,
            table_name=table_name,
            column=_parse_column_def(action),
        )
    return None


def _parse_alter_rename_column_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    if type(action).__name__ == "RenameColumn":
        old_name = _get_name(action.this) if action.this else ""
        to_expr = getattr(action, "to", None) or action.args.get("to")
        new_name = _get_name(to_expr) if to_expr else ""
        if old_name and new_name:
            return AlterTableRenameColumn(
                catalog=catalog,
                schema_name=schema_name,
                table_name=table_name,
                old_name=old_name,
                new_name=new_name,
            )
        return None
    return None


def _parse_alter_column_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    if type(action).__name__ == "AlterColumn":
        column_name = _get_name(action.this) if action.this else ""
        if not column_name:
            return None
        allow_null = None
        if "allow_null" in (action.args or {}):
            allow_null = bool(action.args.get("allow_null"))
        new_type = None
        if hasattr(action, "dtype") and action.dtype:
            new_type = (
                action.dtype.sql(dialect="databricks")
                if hasattr(action.dtype, "sql")
                else str(action.dtype)
            )
        return AlterTableAlterColumn(
            catalog=catalog,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=allow_null,
            new_type=new_type,
        )
    return None


def _parse_alter_rename_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    if type(action).__name__ == "AlterRename":
        new_table = action.this
        if isinstance(new_table, exp.Table):
            new_name = _get_name(new_table.this) or new_table.name or ""
            if new_name:
                return AlterTableRenameTo(
                    catalog=catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    new_name=new_name,
                )
        return None
    return None


def _parse_alter_tblproperties_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    properties = _parse_tblproperties_action(action)
    if properties:
        return AlterTableSetTblproperties(
            catalog=catalog,
            schema_name=schema_name,
            table_name=table_name,
            properties=properties,
        )
    return None


def _parse_alter_action(
    action: exp.Expression, catalog: str, schema_name: str, table_name: str
) -> AlterResult | None:
    for parser in (
        _parse_alter_add_column_action,
        _parse_alter_rename_column_action,
        _parse_alter_column_action,
        _parse_alter_rename_action,
        _parse_alter_tblproperties_action,
    ):
        result = parser(action, catalog, schema_name, table_name)
        if result is not None:
            return result
    return None


def _parse_alter_statement(parsed: exp.Alter, index: int) -> DDLStatementResult:
    if not isinstance(parsed.this, exp.Table):
        return Unsupported(reason="alter_other", index=index)
    catalog, schema_name, table_name = _extract_table_coordinates(parsed.this)
    results: list[AlterResult] = []
    for action in list(parsed.args.get("actions") or []):
        parsed_action = _parse_alter_action(action, catalog, schema_name, table_name)
        if parsed_action is not None:
            results.append(parsed_action)
    if not results:
        return Unsupported(reason="alter_no_actions", index=index)
    if len(results) == 1:
        return results[0]
    return MultiResult(results=tuple(results))


def parse_ddl_statement(sql: str, index: int = 0) -> DDLStatementResult:
    """Parse a single SQL statement and return a classified DDL result."""
    sql = sql.strip()
    if not sql:
        return Unsupported(reason="empty", index=index)

    try:
        parsed = sqlglot.parse_one(sql, dialect="databricks")
    except Exception as err:
        return ParseError(index=index, message=str(err))
    if parsed is None:
        return ParseError(index=index, message="Parser returned None")

    if isinstance(parsed, exp.Command):
        return _parse_command_statement(sql, index)
    if isinstance(parsed, exp.Comment):
        return _parse_comment_statement(parsed, index)
    if isinstance(parsed, exp.Create):
        return _parse_create_statement(parsed, sql, index)
    if isinstance(parsed, exp.Alter):
        return _parse_alter_statement(parsed, index)
    if "ALTER" in sql.upper()[:10]:
        return Unsupported(reason="alter_other", index=index)
    return Unsupported(reason="unknown", index=index)


# --- State builder (consumes DDL results, produces UnityState) ---


class UnityDDLStateBuilder:
    """Build UnityState from a sequence of parsed DDL statement results."""

    def __init__(self) -> None:
        self._catalogs: dict[str, UnityCatalog] = {}
        self._schemas: dict[str, UnitySchema] = {}
        self._tables: dict[str, UnityTable] = {}
        self._views: dict[str, UnityView] = {}
        self._catalog_schema_positions: dict[str, dict[str, int]] = {}
        self._schema_table_positions: dict[str, dict[str, int]] = {}
        self._schema_view_positions: dict[str, dict[str, int]] = {}
        self._report: dict[str, Any] = {
            "created": {"catalogs": 0, "schemas": 0, "tables": 0, "views": 0},
            "skipped": 0,
            "parse_errors": [],
        }

    def add(self, result: DDLStatementResult) -> None:
        """Feed one parsed DDL result; update internal state or report."""
        if isinstance(result, MultiResult):
            for statement_result in result.results:
                self.add(statement_result)
            return
        if isinstance(result, ParseError):
            self._report["parse_errors"].append({"index": result.index, "message": result.message})
            return
        if isinstance(result, Unsupported):
            self._report["skipped"] += 1
            return

        self._dispatch_result(result)

    def _dispatch_result(self, result: DDLStatementResult) -> None:
        handlers: tuple[tuple[type[Any], Any], ...] = (
            (CreateCatalog, self._handle_create_catalog),
            (CreateSchema, self._handle_create_schema),
            (CreateTable, self._handle_create_table),
            (CreateView, self._handle_create_view),
            (CommentOn, self._handle_comment_on),
            (AlterTableAddColumn, self._handle_alter_table_add_column),
            (AlterTableDropColumn, self._handle_alter_table_drop_column),
            (AlterTableRenameColumn, self._handle_alter_table_rename_column),
            (AlterTableAlterColumn, self._handle_alter_table_alter_column),
            (AlterTableRenameTo, self._handle_alter_table_rename_to),
            (AlterTableSetTblproperties, self._handle_alter_table_set_tblproperties),
            (AlterCatalogSetTags, self._handle_alter_catalog_set_tags),
            (AlterSchemaSetTags, self._handle_alter_schema_set_tags),
            (AlterTableSetTags, self._handle_alter_table_set_tags),
        )
        for result_type, handler in handlers:
            if isinstance(result, result_type):
                handler(result)
                return

    def _handle_create_catalog(self, result: CreateCatalog) -> None:
        catalog_id = f"catalog_{result.name}"
        if catalog_id not in self._catalogs:
            self._catalogs[catalog_id] = UnityCatalog(
                id=catalog_id,
                name=result.name,
                comment=result.comment,
                tags=result.tags,
                schemas=[],
            )
            self._catalog_schema_positions[catalog_id] = {}
            self._report["created"]["catalogs"] += 1
            return
        existing = self._catalogs[catalog_id]
        if result.comment is None and not result.tags:
            return
        self._catalogs[catalog_id] = existing.model_copy(
            update={
                "comment": result.comment if result.comment is not None else existing.comment,
                "tags": result.tags if result.tags else existing.tags,
            }
        )

    def _handle_create_schema(self, result: CreateSchema) -> None:
        catalog_id = f"catalog_{result.catalog}"
        if catalog_id not in self._catalogs:
            self._catalogs[catalog_id] = UnityCatalog(
                id=catalog_id, name=result.catalog, schemas=[]
            )
            self._catalog_schema_positions[catalog_id] = {}
        schema_id = f"schema_{result.catalog}_{result.name}"
        if schema_id in self._schemas:
            return
        new_schema = UnitySchema(
            id=schema_id,
            name=result.name,
            comment=result.comment,
            tags=result.tags,
            tables=[],
            views=[],
        )
        self._schemas[schema_id] = new_schema
        self._schema_table_positions[schema_id] = {}
        self._schema_view_positions[schema_id] = {}
        catalog_obj = self._catalogs[catalog_id]
        catalog_obj.schemas.append(new_schema)
        self._catalog_schema_positions[catalog_id][schema_id] = len(catalog_obj.schemas) - 1
        self._report["created"]["schemas"] += 1

    def _handle_create_table(self, result: CreateTable) -> None:
        self._ensure_catalog_schema(result.catalog, result.schema_name)
        schema_id = f"schema_{result.catalog}_{result.schema_name}"
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.name}"
        if table_id in self._tables:
            return
        columns = [
            UnityColumn(
                id=f"col_{table_id}_{column_def.name}",
                name=column_def.name,
                type=column_def.type,
                nullable=column_def.nullable,
                comment=column_def.comment,
            )
            for column_def in result.columns
        ]
        new_table = UnityTable(
            id=table_id,
            name=result.name,
            format=result.format,
            columns=columns,
            comment=result.comment,
            tags=result.tags,
        )
        self._tables[table_id] = new_table
        schema_obj = self._schemas[schema_id]
        schema_obj.tables.append(new_table)
        self._schema_table_positions[schema_id][table_id] = len(schema_obj.tables) - 1
        self._report["created"]["tables"] += 1

    def _handle_create_view(self, result: CreateView) -> None:
        self._ensure_catalog_schema(result.catalog, result.schema_name)
        schema_id = f"schema_{result.catalog}_{result.schema_name}"
        view_id = f"view_{result.catalog}_{result.schema_name}_{result.name}"
        if view_id in self._views:
            return
        extracted_dependencies: dict[str, list[str]] | None = None
        if result.definition:
            raw_refs = extract_table_references(result.definition, dialect="databricks")
            extracted_dependencies = {
                "tables": raw_refs.get("tables", []) or [],
                "views": raw_refs.get("views", []) or [],
            }
        new_view = UnityView(
            id=view_id,
            name=result.name,
            definition=result.definition,
            comment=result.comment,
            extracted_dependencies=extracted_dependencies,
        )
        self._views[view_id] = new_view
        schema_obj = self._schemas[schema_id]
        schema_obj.views.append(new_view)
        self._schema_view_positions[schema_id][view_id] = len(schema_obj.views) - 1
        self._report["created"]["views"] += 1

    def _handle_comment_on(self, result: CommentOn) -> None:
        if result.object_type == "catalog":
            catalog_id = f"catalog_{result.name}"
            if catalog_id in self._catalogs:
                self._catalogs[catalog_id] = self._catalogs[catalog_id].model_copy(
                    update={"comment": result.comment}
                )
            return
        if result.object_type not in {"schema", "table", "view"} or not result.schema_name:
            return
        schema_id = f"schema_{result.catalog}_{result.schema_name}"
        if (
            schema_id in self._schemas
            and result.object_type == "schema"
            and result.name == self._schemas[schema_id].name
        ):
            self._schemas[schema_id] = self._schemas[schema_id].model_copy(
                update={"comment": result.comment}
            )
            self._update_catalog_schemas_list(
                result.catalog, result.schema_name, self._schemas[schema_id]
            )
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.name}"
        if table_id in self._tables:
            self._tables[table_id] = self._tables[table_id].model_copy(
                update={"comment": result.comment}
            )
            self._update_schema_tables_list(
                result.catalog, result.schema_name, self._tables[table_id]
            )
        view_id = f"view_{result.catalog}_{result.schema_name}_{result.name}"
        if view_id in self._views:
            self._views[view_id] = self._views[view_id].model_copy(
                update={"comment": result.comment}
            )
            self._update_schema_views_list(result.catalog, result.schema_name, self._views[view_id])

    def _handle_alter_table_add_column(self, result: AlterTableAddColumn) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        table_obj = self._tables[table_id]
        column_def = result.column
        column_id = f"col_{table_id}_{column_def.name}"
        if any(col.id == column_id or col.name == column_def.name for col in table_obj.columns):
            return
        new_column = UnityColumn(
            id=column_id,
            name=column_def.name,
            type=column_def.type,
            nullable=column_def.nullable,
            comment=column_def.comment,
        )
        updated_table = table_obj.model_copy(update={"columns": [*table_obj.columns, new_column]})
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _handle_alter_table_drop_column(self, result: AlterTableDropColumn) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        table_obj = self._tables[table_id]
        updated_table = table_obj.model_copy(
            update={"columns": [col for col in table_obj.columns if col.name != result.column_name]}
        )
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _handle_alter_table_rename_column(self, result: AlterTableRenameColumn) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        table_obj = self._tables[table_id]
        updated_columns = []
        for col in table_obj.columns:
            if col.name == result.old_name:
                updated_columns.append(
                    col.model_copy(
                        update={"name": result.new_name, "id": f"col_{table_id}_{result.new_name}"}
                    )
                )
            else:
                updated_columns.append(col)
        updated_table = table_obj.model_copy(update={"columns": updated_columns})
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _handle_alter_table_alter_column(self, result: AlterTableAlterColumn) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        updates: dict[str, Any] = {}
        if result.nullable is not None:
            updates["nullable"] = result.nullable
        if result.new_type is not None:
            updates["type"] = result.new_type
        table_obj = self._tables[table_id]
        updated_columns = [
            col.model_copy(update=updates) if col.name == result.column_name else col
            for col in table_obj.columns
        ]
        updated_table = table_obj.model_copy(update={"columns": updated_columns})
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _handle_alter_table_rename_to(self, result: AlterTableRenameTo) -> None:
        table_id_old = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id_old not in self._tables:
            return
        table_obj = self._tables.pop(table_id_old)
        table_id_new = f"table_{result.catalog}_{result.schema_name}_{result.new_name}"
        updated_columns = [
            col.model_copy(update={"id": f"col_{table_id_new}_{col.name}"})
            for col in table_obj.columns
        ]
        updated_table = table_obj.model_copy(
            update={"id": table_id_new, "name": result.new_name, "columns": updated_columns}
        )
        self._tables[table_id_new] = updated_table
        schema_id = f"schema_{result.catalog}_{result.schema_name}"
        if schema_id not in self._schemas:
            return
        self._update_schema_tables_list(
            result.catalog,
            result.schema_name,
            updated_table,
            previous_table_id=table_id_old,
        )

    def _handle_alter_table_set_tblproperties(self, result: AlterTableSetTblproperties) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        table_obj = self._tables[table_id]
        updated_properties = {**(table_obj.properties or {}), **result.properties}
        updated_table = table_obj.model_copy(update={"properties": updated_properties})
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _handle_alter_catalog_set_tags(self, result: AlterCatalogSetTags) -> None:
        catalog_id = f"catalog_{result.name}"
        if catalog_id not in self._catalogs:
            return
        catalog_obj = self._catalogs[catalog_id]
        updated_tags = {**(getattr(catalog_obj, "tags", None) or {}), **result.tags}
        self._catalogs[catalog_id] = catalog_obj.model_copy(update={"tags": updated_tags})

    def _handle_alter_schema_set_tags(self, result: AlterSchemaSetTags) -> None:
        schema_id = f"schema_{result.catalog}_{result.schema_name}"
        if schema_id not in self._schemas:
            return
        schema_obj = self._schemas[schema_id]
        updated_tags = {**(getattr(schema_obj, "tags", None) or {}), **result.tags}
        updated_schema = schema_obj.model_copy(update={"tags": updated_tags})
        self._schemas[schema_id] = updated_schema
        self._update_catalog_schemas_list(result.catalog, result.schema_name, updated_schema)

    def _handle_alter_table_set_tags(self, result: AlterTableSetTags) -> None:
        table_id = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
        if table_id not in self._tables:
            return
        table_obj = self._tables[table_id]
        updated_tags = {**(getattr(table_obj, "tags", None) or {}), **result.tags}
        updated_table = table_obj.model_copy(update={"tags": updated_tags})
        self._tables[table_id] = updated_table
        self._update_schema_tables_list(result.catalog, result.schema_name, updated_table)

    def _ensure_catalog_schema(self, catalog: str, schema_name: str) -> None:
        cat_id = f"catalog_{catalog}"
        if cat_id not in self._catalogs:
            self._catalogs[cat_id] = UnityCatalog(id=cat_id, name=catalog, schemas=[])
            self._catalog_schema_positions[cat_id] = {}
        sid = f"schema_{catalog}_{schema_name}"
        if sid not in self._schemas:
            self._schemas[sid] = UnitySchema(id=sid, name=schema_name, tables=[], views=[])
            self._schema_table_positions[sid] = {}
            self._schema_view_positions[sid] = {}
            catalog_obj = self._catalogs[cat_id]
            catalog_obj.schemas.append(self._schemas[sid])
            self._catalog_schema_positions[cat_id][sid] = len(catalog_obj.schemas) - 1

    def _update_catalog_schemas_list(
        self, catalog: str, schema_name: str, updated_schema: UnitySchema
    ) -> None:
        """Replace schema in catalog's schemas list using cached indexes."""
        cat_id = f"catalog_{catalog}"
        sid = f"schema_{catalog}_{schema_name}"
        if cat_id not in self._catalogs:
            return
        schema_positions = self._catalog_schema_positions.setdefault(cat_id, {})
        schema_index = schema_positions.get(sid)
        catalog_obj = self._catalogs[cat_id]
        if schema_index is None:
            catalog_obj.schemas.append(updated_schema)
            schema_positions[sid] = len(catalog_obj.schemas) - 1
            return
        if 0 <= schema_index < len(catalog_obj.schemas):
            catalog_obj.schemas[schema_index] = updated_schema

    def _update_schema_tables_list(
        self,
        catalog: str,
        schema_name: str,
        updated_table: UnityTable,
        previous_table_id: str | None = None,
    ) -> None:
        """Replace or append table in schema list using cached indexes."""
        sid = f"schema_{catalog}_{schema_name}"
        if sid not in self._schemas:
            return
        schema = self._schemas[sid]
        table_positions = self._schema_table_positions.setdefault(sid, {})
        lookup_id = previous_table_id or updated_table.id
        table_index = table_positions.get(lookup_id)
        if table_index is None:
            schema.tables.append(updated_table)
            table_positions[updated_table.id] = len(schema.tables) - 1
        else:
            schema.tables[table_index] = updated_table
            if previous_table_id and previous_table_id != updated_table.id:
                table_positions.pop(previous_table_id, None)
            table_positions[updated_table.id] = table_index
        self._update_catalog_schemas_list(catalog, schema_name, self._schemas[sid])

    def _update_schema_views_list(
        self, catalog: str, schema_name: str, updated_view: UnityView
    ) -> None:
        """Replace or append view in schema list using cached indexes."""
        sid = f"schema_{catalog}_{schema_name}"
        if sid not in self._schemas:
            return
        schema = self._schemas[sid]
        view_positions = self._schema_view_positions.setdefault(sid, {})
        view_index = view_positions.get(updated_view.id)
        if view_index is None:
            schema.views.append(updated_view)
            view_positions[updated_view.id] = len(schema.views) - 1
        else:
            schema.views[view_index] = updated_view
        self._update_catalog_schemas_list(catalog, schema_name, self._schemas[sid])

    def build(self) -> UnityState:
        """Return the built UnityState (catalogs list)."""
        return UnityState(catalogs=list(self._catalogs.values()))

    def get_report(self) -> dict[str, Any]:
        """Return import report (counts, skipped, parse_errors)."""
        return dict(self._report)


# --- Provider integration: state_from_ddl entry ---

# Max statements to process in one run (v1 limit)
DDL_STATEMENT_LIMIT = 50_000


def state_from_ddl(
    sql_path: Path | None = None,
    sql_statements: list[str] | None = None,
    dialect: str = "databricks",
) -> tuple[ProviderState, dict[str, Any]]:
    """Build UnityState from a SQL file or list of statements.

    Args:
        sql_path: Path to .sql file (read and split).
        sql_statements: Alternatively, pre-split list of statements.
        dialect: Reserved for future use. Parsing and serialization currently
            use the Databricks dialect only; this argument is ignored.

    Returns:
        (state_dict, report). state_dict is UnityState as dict for ProviderState.
        report has created counts, skipped, parse_errors.

    Raises:
        ValueError: If neither sql_path nor sql_statements provided.
        ValueError: If statement count exceeds DDL_STATEMENT_LIMIT.
    """
    del dialect
    if sql_path is not None:
        text = sql_path.read_text()
        statements = split_sql_statements(text)
    elif sql_statements is not None:
        statements = sql_statements
    else:
        raise ValueError("Provide sql_path or sql_statements")

    if len(statements) > DDL_STATEMENT_LIMIT:
        raise ValueError(
            f"SQL script has {len(statements)} statements; maximum is {DDL_STATEMENT_LIMIT}. "
            "Split the file or process in chunks."
        )

    builder = UnityDDLStateBuilder()
    for i, stmt in enumerate(statements):
        result = parse_ddl_statement(stmt, index=i)
        builder.add(result)

    state = builder.build()
    report = builder.get_report()
    # ProviderState is dict with catalogs; UnityState.model_dump(by_alias=True) for JSON compat
    state_dict: ProviderState = {"catalogs": [c.model_dump(by_alias=True) for c in state.catalogs]}
    return state_dict, report
