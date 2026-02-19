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


def parse_ddl_statement(sql: str, index: int = 0) -> DDLStatementResult:
    """Parse a single SQL statement and return a classified DDL result.

    Args:
        sql: One DDL statement (e.g. CREATE CATALOG x, CREATE TABLE ...).
        index: Statement index (for error reporting).

    Returns:
        CreateCatalog, CreateSchema, CreateTable, CreateView, CommentOn,
        Unsupported, or ParseError.
    """
    sql = sql.strip()
    if not sql:
        return Unsupported(reason="empty", index=index)

    try:
        parsed = sqlglot.parse_one(sql, dialect="databricks")
    except Exception as e:
        return ParseError(index=index, message=str(e))

    if parsed is None:
        return ParseError(index=index, message="Parser returned None")

    # CREATE CATALOG / CREATE SCHEMA (sqlglot may parse as Command when MANAGED LOCATION etc. present)
    if isinstance(parsed, exp.Command):
        if "CREATE CATALOG" in sql.upper():
            m = re.match(
                r"CREATE\s+CATALOG\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)",
                sql,
                re.IGNORECASE | re.DOTALL,
            )
            name = m.group(1).strip("`\"'") if m else "unknown"
            comment = None
            cm = re.search(r"COMMENT\s+['\"]([^'\"]*)['\"]", sql, re.IGNORECASE)
            if cm:
                comment = cm.group(1)
            return CreateCatalog(name=name, comment=comment)
        if "CREATE SCHEMA" in sql.upper():
            m = re.match(
                r"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+(?:\.[^\s]+)?)",
                sql,
                re.IGNORECASE | re.DOTALL,
            )
            full_name = m.group(1).strip("`\"'") if m else "unknown"
            parts = full_name.split(".", 1)
            if len(parts) == 2:
                catalog, schema_name = parts[0], parts[1]
            else:
                catalog, schema_name = "__implicit__", full_name
            return CreateSchema(catalog=catalog, name=schema_name, comment=None)
        if "ALTER TABLE" in sql.upper() and "DROP COLUMN" in sql.upper():
            m = re.match(
                r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?([^\s,;]+)",
                sql,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                full_table = m.group(1).strip("`\"'")
                col_name = m.group(2).strip("`\"'")
                parts = full_table.split(".")
                if len(parts) == 3:
                    catalog, schema_name, table_name = parts[0], parts[1], parts[2]
                elif len(parts) == 2:
                    catalog, schema_name, table_name = "__implicit__", parts[0], parts[1]
                else:
                    catalog, schema_name, table_name = "__implicit__", "", full_table
                return AlterTableDropColumn(
                    catalog=catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col_name,
                )
        return Unsupported(reason="command", index=index)

    # COMMENT ON ...
    if isinstance(parsed, exp.Comment):
        # Comment expression: this might be the target (Table or Identifier), expression is the string
        target = parsed.this
        comment_str = ""
        if hasattr(parsed, "expression") and parsed.expression:
            comment_str = parsed.expression.sql(dialect="databricks").strip("'\"")
        if isinstance(target, exp.Table):
            catalog = target.catalog or "__implicit__"
            schema_name = target.db or ""
            name = _get_name(target.this) or target.name or ""
            obj_type = "table"  # could be view; treat as table for simplicity
            return CommentOn(
                object_type=obj_type,
                catalog=catalog,
                schema_name=schema_name or None,
                name=name,
                comment=comment_str,
            )
        # COMMENT ON CATALOG x IS '...' might be different shape
        return Unsupported(reason=f"comment_on_shape:{type(target).__name__}", index=index)

    # CREATE ...
    if isinstance(parsed, exp.Create):
        kind = (getattr(parsed, "kind", None) or "").upper()
        this = parsed.this

        # CREATE CATALOG name [COMMENT '...']
        if kind == "CATALOG" or (kind == "" and this and "catalog" in sql.lower()[:30]):
            if isinstance(this, (exp.Identifier, exp.Table)):
                name = this.name or _get_name(this) or (this.this.name if hasattr(this, "this") else "")
                if not name and hasattr(this, "this"):
                    name = _get_name(this.this)
                comment = _get_comment_from_create(parsed)
                return CreateCatalog(name=name or "unknown", comment=comment)
            return Unsupported(reason="create_catalog_shape", index=index)

        # CREATE SCHEMA [catalog.]schema
        if kind == "SCHEMA" or (kind == "" and this and "schema" in sql.lower()[:30]):
            if isinstance(this, exp.Table):
                # cat.schema_name -> catalog=cat, db=schema_name, name may be empty
                catalog = this.catalog or "__implicit__"
                if isinstance(catalog, exp.Identifier):
                    catalog = catalog.name or "__implicit__"
                schema_name = this.db or _get_name(this.this) or this.name or ""
                comment = _get_comment_from_create(parsed)
                return CreateSchema(catalog=str(catalog), name=schema_name or "unknown", comment=comment)
            if isinstance(this, exp.Schema):
                catalog = getattr(this, "catalog", None) or "__implicit__"
                if isinstance(catalog, exp.Identifier):
                    catalog = catalog.name or "__implicit__"
                name = _get_name(this.this) or this.name or ""
                comment = _get_comment_from_create(parsed)
                return CreateSchema(catalog=str(catalog), name=name or "unknown", comment=comment)
            if isinstance(this, exp.Identifier):
                return CreateSchema(catalog="__implicit__", name=this.name or "unknown", comment=None)
            return Unsupported(reason="create_schema_shape", index=index)

        # CREATE TABLE [catalog.]schema.table (cols) USING DELTA|ICEBERG
        if kind == "TABLE":
            schema_expr = this
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

            columns: list[ColumnDef] = []
            for expr in schema_expr.expressions or []:
                if isinstance(expr, exp.ColumnDef):
                    col_name = _get_name(expr.this) or expr.this.name if expr.this else ""
                    col_type = "STRING"
                    if expr.kind:
                        col_type = expr.kind.sql(dialect="databricks") if hasattr(expr.kind, "sql") else str(expr.kind)
                    nullable = not (expr.args.get("not_null") or getattr(expr, "not_null", False))
                    col_comment = None
                    if hasattr(expr, "comment") and expr.comment:
                        col_comment = expr.comment.this if hasattr(expr.comment, "this") else str(expr.comment)
                    columns.append(
                        ColumnDef(name=col_name, type=col_type, nullable=nullable, comment=col_comment)
                    )

            format_val = "delta"
            for prop in parsed.find_all(exp.FileFormatProperty):
                if prop.this:
                    format_val = _get_name(prop.this) or "delta"
                    break
            if format_val == "delta":
                m = re.search(r"\bUSING\s+(DELTA|ICEBERG)\b", sql, re.IGNORECASE)
                if m:
                    format_val = m.group(1).lower()
            comment = _get_comment_from_create(parsed)
            return CreateTable(
                catalog=str(catalog),
                schema_name=str(schema_name),
                name=table_name or "unknown",
                columns=columns,
                format=format_val.lower(),
                comment=comment,
            )

        # CREATE VIEW [catalog.]schema.view AS SELECT ...
        if kind == "VIEW":
            schema_expr = this
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
                definition = schema_expr.expressions[0].sql(dialect="databricks") if schema_expr.expressions else ""
            return CreateView(
                catalog=str(catalog),
                schema_name=str(schema_name),
                name=view_name or "unknown",
                definition=definition,
            )

        return Unsupported(reason=f"create_kind:{kind}", index=index)

    # ALTER TABLE / CATALOG / SCHEMA - parse and return Alter* or MultiResult
    if isinstance(parsed, exp.Alter):
        target = parsed.this
        actions = list(parsed.args.get("actions") or [])
        if isinstance(target, exp.Table):
            catalog = target.catalog or "__implicit__"
            if isinstance(catalog, exp.Identifier):
                catalog = catalog.name or "__implicit__"
            else:
                catalog = str(catalog)
            schema_name = target.db or ""
            if isinstance(schema_name, exp.Identifier):
                schema_name = schema_name.name or ""
            else:
                schema_name = str(schema_name)
            table_name = _get_name(target.this) or target.name or ""
            results: list[AlterResult | Unsupported] = []
            for act in actions:
                if isinstance(act, exp.ColumnDef):
                    col_name = _get_name(act.this) or (act.this.name if act.this else "")
                    col_type = "STRING"
                    if act.kind:
                        col_type = act.kind.sql(dialect="databricks") if hasattr(act.kind, "sql") else str(act.kind)
                    nullable = not (act.args.get("not_null") or getattr(act, "not_null", False))
                    col_comment = None
                    if hasattr(act, "comment") and act.comment:
                        col_comment = act.comment.this if hasattr(act.comment, "this") else str(act.comment)
                    results.append(
                        AlterTableAddColumn(
                            catalog=catalog,
                            schema_name=schema_name,
                            table_name=table_name,
                            column=ColumnDef(name=col_name, type=col_type, nullable=nullable, comment=col_comment),
                        )
                    )
                elif type(act).__name__ == "RenameColumn":
                    old_name = _get_name(act.this) if act.this else ""
                    to_expr = getattr(act, "to", None) or act.args.get("to")
                    new_name = _get_name(to_expr) if to_expr else ""
                    if old_name and new_name:
                        results.append(
                            AlterTableRenameColumn(
                                catalog=catalog,
                                schema_name=schema_name,
                                table_name=table_name,
                                old_name=old_name,
                                new_name=new_name,
                            )
                        )
                elif type(act).__name__ == "AlterColumn":
                    col_name = _get_name(act.this) if act.this else ""
                    nullable = None
                    if "allow_null" in (act.args or {}):
                        nullable = bool(act.args.get("allow_null"))
                    new_type = None
                    if hasattr(act, "dtype") and act.dtype:
                        new_type = act.dtype.sql(dialect="databricks") if hasattr(act.dtype, "sql") else str(act.dtype)
                    if col_name:
                        results.append(
                            AlterTableAlterColumn(
                                catalog=catalog,
                                schema_name=schema_name,
                                table_name=table_name,
                                column_name=col_name,
                                nullable=nullable,
                                new_type=new_type,
                            )
                        )
                elif isinstance(act, exp.AlterSet) and act.expressions:
                    for ex in act.expressions:
                        if isinstance(ex, exp.Properties) and ex.expressions:
                            props: dict[str, str] = {}
                            for prop in ex.expressions:
                                if isinstance(prop, exp.Property):
                                    k = prop.this
                                    if hasattr(k, "this"):
                                        k = str(k.this).strip("'\"")
                                    else:
                                        k = str(k).strip("'\"")
                                    v = prop.args.get("value")
                                    if v is not None:
                                        v = v.sql(dialect="databricks").strip("'\"") if hasattr(v, "sql") else str(v)
                                    else:
                                        v = ""
                                    props[k] = v
                            if props:
                                results.append(
                                    AlterTableSetTblproperties(
                                        catalog=catalog,
                                        schema_name=schema_name,
                                        table_name=table_name,
                                        properties=props,
                                    )
                                )
                            break
                elif type(act).__name__ == "AlterRename":
                    # RENAME TO new_name: act.this is the new Table
                    new_table = act.this
                    if isinstance(new_table, exp.Table):
                        new_name = _get_name(new_table.this) or new_table.name or ""
                        if new_name:
                            results.append(
                                AlterTableRenameTo(
                                    catalog=catalog,
                                    schema_name=schema_name,
                                    table_name=table_name,
                                    new_name=new_name,
                                )
                            )
            if not results:
                return Unsupported(reason="alter_no_actions", index=index)
            if len(results) == 1 and not isinstance(results[0], Unsupported):
                return results[0]
            return MultiResult(results=tuple(results))

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
        self._report: dict[str, Any] = {
            "created": {"catalogs": 0, "schemas": 0, "tables": 0, "views": 0},
            "skipped": 0,
            "parse_errors": [],
        }

    def add(self, result: DDLStatementResult) -> None:
        """Feed one parsed DDL result; update internal state or report."""
        if isinstance(result, MultiResult):
            for r in result.results:
                self.add(r)
            return
        if isinstance(result, ParseError):
            self._report["parse_errors"].append({"index": result.index, "message": result.message})
            return
        if isinstance(result, Unsupported):
            self._report["skipped"] += 1
            return

        if isinstance(result, CreateCatalog):
            cid = f"catalog_{result.name}"
            if cid not in self._catalogs:
                self._catalogs[cid] = UnityCatalog(
                    id=cid,
                    name=result.name,
                    comment=result.comment,
                    tags=result.tags,
                    schemas=[],
                )
                self._report["created"]["catalogs"] += 1
            return

        if isinstance(result, CreateSchema):
            cat_id = f"catalog_{result.catalog}"
            if cat_id not in self._catalogs:
                self._catalogs[cat_id] = UnityCatalog(
                    id=cat_id,
                    name=result.catalog,
                    schemas=[],
                )
            sid = f"schema_{result.catalog}_{result.name}"
            if sid not in self._schemas:
                self._schemas[sid] = UnitySchema(
                    id=sid,
                    name=result.name,
                    comment=result.comment,
                    tags=result.tags,
                    tables=[],
                    views=[],
                )
                self._catalogs[cat_id].schemas.append(self._schemas[sid])
                self._report["created"]["schemas"] += 1
            return

        if isinstance(result, CreateTable):
            self._ensure_catalog_schema(result.catalog, result.schema_name)
            cat_id = f"catalog_{result.catalog}"
            sid = f"schema_{result.catalog}_{result.schema_name}"
            tid = f"table_{result.catalog}_{result.schema_name}_{result.name}"
            if tid not in self._tables:
                cols = [
                    UnityColumn(
                        id=f"col_{tid}_{c.name}",
                        name=c.name,
                        type=c.type,
                        nullable=c.nullable,
                        comment=c.comment,
                    )
                    for c in result.columns
                ]
                self._tables[tid] = UnityTable(
                    id=tid,
                    name=result.name,
                    format=result.format,
                    columns=cols,
                    comment=result.comment,
                    tags=result.tags,
                )
                self._schemas[sid].tables.append(self._tables[tid])
                self._report["created"]["tables"] += 1
            return

        if isinstance(result, CreateView):
            self._ensure_catalog_schema(result.catalog, result.schema_name)
            sid = f"schema_{result.catalog}_{result.schema_name}"
            vid = f"view_{result.catalog}_{result.schema_name}_{result.name}"
            if vid not in self._views:
                self._views[vid] = UnityView(
                    id=vid,
                    name=result.name,
                    definition=result.definition,
                    comment=result.comment,
                )
                self._schemas[sid].views.append(self._views[vid])
                self._report["created"]["views"] += 1
            return

        if isinstance(result, CommentOn):
            # Best-effort: apply comment to existing object if we find it
            if result.object_type == "catalog":
                cid = f"catalog_{result.name}"
                if cid in self._catalogs:
                    self._catalogs[cid].comment = result.comment
            elif result.object_type in ("schema", "table", "view") and result.schema_name:
                sid = f"schema_{result.catalog}_{result.schema_name}"
                if sid in self._schemas and result.object_type == "schema" and result.name == self._schemas[sid].name:
                    self._schemas[sid].comment = result.comment
                tid = f"table_{result.catalog}_{result.schema_name}_{result.name}"
                if tid in self._tables:
                    self._tables[tid].comment = result.comment
                vid = f"view_{result.catalog}_{result.schema_name}_{result.name}"
                if vid in self._views:
                    self._views[vid].comment = result.comment
            return

        if isinstance(result, AlterTableAddColumn):
            tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid in self._tables:
                t = self._tables[tid]
                c = result.column
                col_id = f"col_{tid}_{c.name}"
                if not any(col.id == col_id or col.name == c.name for col in t.columns):
                    t.columns.append(
                        UnityColumn(
                            id=col_id,
                            name=c.name,
                            type=c.type,
                            nullable=c.nullable,
                            comment=c.comment,
                        )
                    )
            return

        if isinstance(result, AlterTableDropColumn):
            tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid in self._tables:
                self._tables[tid].columns = [
                    col for col in self._tables[tid].columns if col.name != result.column_name
                ]
            return

        if isinstance(result, AlterTableRenameColumn):
            tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid in self._tables:
                for col in self._tables[tid].columns:
                    if col.name == result.old_name:
                        col.name = result.new_name
                        col.id = f"col_{tid}_{result.new_name}"
                        break
            return

        if isinstance(result, AlterTableAlterColumn):
            tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid in self._tables:
                for col in self._tables[tid].columns:
                    if col.name == result.column_name:
                        if result.nullable is not None:
                            col.nullable = result.nullable
                        if result.new_type is not None:
                            col.type = result.new_type
                        break
            return

        if isinstance(result, AlterTableRenameTo):
            tid_old = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid_old in self._tables:
                t = self._tables.pop(tid_old)
                sid = f"schema_{result.catalog}_{result.schema_name}"
                if sid in self._schemas:
                    self._schemas[sid].tables = [x for x in self._schemas[sid].tables if x.id != tid_old]
                tid_new = f"table_{result.catalog}_{result.schema_name}_{result.new_name}"
                t.id = tid_new
                t.name = result.new_name
                for col in t.columns:
                    col.id = f"col_{tid_new}_{col.name}"
                self._tables[tid_new] = t
                if sid in self._schemas:
                    self._schemas[sid].tables.append(t)
            return

        if isinstance(result, AlterTableSetTblproperties):
            tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
            if tid in self._tables:
                t = self._tables[tid]
                if not hasattr(t, "properties") or t.properties is None:
                    t.properties = {}
                t.properties.update(result.properties)
            return

        if isinstance(result, (AlterCatalogSetTags, AlterSchemaSetTags, AlterTableSetTags)):
            # Tags on catalog/schema/table: best-effort update if we have the object
            if isinstance(result, AlterCatalogSetTags):
                cid = f"catalog_{result.name}"
                if cid in self._catalogs:
                    self._catalogs[cid].tags = getattr(self._catalogs[cid], "tags", {}) or {}
                    self._catalogs[cid].tags.update(result.tags)
            elif isinstance(result, AlterSchemaSetTags):
                sid = f"schema_{result.catalog}_{result.schema_name}"
                if sid in self._schemas:
                    self._schemas[sid].tags = getattr(self._schemas[sid], "tags", {}) or {}
                    self._schemas[sid].tags.update(result.tags)
            else:
                tid = f"table_{result.catalog}_{result.schema_name}_{result.table_name}"
                if tid in self._tables:
                    self._tables[tid].tags = getattr(self._tables[tid], "tags", {}) or {}
                    self._tables[tid].tags.update(result.tags)
            return

    def _ensure_catalog_schema(self, catalog: str, schema_name: str) -> None:
        cat_id = f"catalog_{catalog}"
        if cat_id not in self._catalogs:
            self._catalogs[cat_id] = UnityCatalog(id=cat_id, name=catalog, schemas=[])
        sid = f"schema_{catalog}_{schema_name}"
        if sid not in self._schemas:
            self._schemas[sid] = UnitySchema(id=sid, name=schema_name, tables=[], views=[])
            self._catalogs[cat_id].schemas.append(self._schemas[sid])

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
        dialect: SQL dialect (only databricks supported in v1).

    Returns:
        (state_dict, report). state_dict is UnityState as dict for ProviderState.
        report has created counts, skipped, parse_errors.

    Raises:
        ValueError: If neither sql_path nor sql_statements provided.
        ValueError: If statement count exceeds DDL_STATEMENT_LIMIT.
    """
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
