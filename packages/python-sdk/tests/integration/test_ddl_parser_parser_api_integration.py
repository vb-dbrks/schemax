"""Integration tests for parser API behavior without mocks."""

from __future__ import annotations

import pytest

from schemax.providers.unity.ddl_parser import (
    AlterCatalogSetTags,
    AlterSchemaSetTags,
    AlterTableDropColumn,
    AlterTableSetTags,
    CommentOn,
    ParseError,
    Unsupported,
    parse_ddl_statement,
    state_from_ddl,
)


@pytest.mark.integration
def test_parse_ddl_statement_command_and_fallback_shapes() -> None:
    result = parse_ddl_statement("COMMENT ON CATALOG cat_a IS 'catalog comment'")
    assert isinstance(result, CommentOn)
    assert result.object_type == "catalog"
    assert result.name == "cat_a"

    result = parse_ddl_statement("COMMENT ON SCHEMA cat_a.core IS 'schema comment'")
    assert isinstance(result, CommentOn)
    assert result.object_type == "schema"
    assert result.catalog == "cat_a"
    assert result.schema_name == "core"

    result = parse_ddl_statement("COMMENT ON VIEW cat_a.core.v1 IS 'view comment'")
    assert isinstance(result, CommentOn)
    assert result.object_type == "view"
    assert result.name == "v1"

    result = parse_ddl_statement("ALTER CATALOG cat_a SET TAGS ('owner' = 'platform')")
    assert isinstance(result, AlterCatalogSetTags)
    assert result.tags["owner"] == "platform"

    result = parse_ddl_statement("ALTER SCHEMA cat_a.core SET TAGS ('tier' = 'core')")
    assert isinstance(result, AlterSchemaSetTags)
    assert result.tags["tier"] == "core"

    result = parse_ddl_statement("ALTER TABLE cat_a.core.t1 SET TAGS ('domain' = 'finance')")
    assert isinstance(result, AlterTableSetTags)
    assert result.tags["domain"] == "finance"

    result = parse_ddl_statement("ALTER TABLE cat_a.core.t1 DROP COLUMN IF EXISTS col_a")
    assert isinstance(result, AlterTableDropColumn)
    assert result.column_name == "col_a"


@pytest.mark.integration
def test_parse_ddl_statement_error_and_unsupported_contracts() -> None:
    result = parse_ddl_statement("", index=7)
    assert isinstance(result, Unsupported)
    assert result.reason == "empty"
    assert result.index == 7

    result = parse_ddl_statement("SELECT 1", index=8)
    assert isinstance(result, Unsupported)
    assert result.index == 8

    result = parse_ddl_statement("CREATE TABLE broken (", index=9)
    assert isinstance(result, ParseError)
    assert result.index == 9

    result = parse_ddl_statement("ALTER CATALOG cat_a SET TAGS (", index=10)
    assert isinstance(result, Unsupported)
    assert result.reason == "command"
    assert result.index == 10

    result = parse_ddl_statement("ALTER TABLE cat_a.core.t1 DROP COLUMN", index=11)
    assert isinstance(result, Unsupported)
    assert result.reason == "command"
    assert result.index == 11

    result = parse_ddl_statement("CREATE FUNCTION cat_a.core.fn AS $$ SELECT 1 $$", index=12)
    assert isinstance(result, Unsupported)
    assert result.reason.startswith("create_kind:")


@pytest.mark.integration
def test_state_from_ddl_report_and_state_mutation_contract() -> None:
    state_dict, report = state_from_ddl(
        sql_statements=[
            "CREATE CATALOG cat_i",
            "CREATE SCHEMA cat_i.core",
            "CREATE TABLE cat_i.core.orders (id BIGINT) USING DELTA",
            "ALTER TABLE cat_i.core.orders ADD COLUMN amount INT, ADD COLUMN note STRING",
            "ALTER TABLE cat_i.core.orders SET TAGS ('domain' = 'sales')",
            "ALTER TABLE cat_i.core.orders DROP COLUMN note",
            "ALTER TABLE cat_i.core.orders RENAME TO cat_i.core.orders_v2",
            "CREATE VIEW cat_i.core.v_orders AS SELECT id, amount FROM cat_i.core.orders_v2",
            "COMMENT ON TABLE cat_i.core.orders_v2 IS 'orders table'",
            "COMMENT ON VIEW cat_i.core.v_orders IS 'orders view'",
            "SELECT 1",
            "CREATE TABLE broken (",
        ]
    )

    assert report["created"]["catalogs"] >= 1
    assert report["created"]["schemas"] >= 1
    assert report["created"]["tables"] >= 1
    assert report["created"]["views"] >= 1
    assert report["skipped"] >= 1
    assert len(report["parse_errors"]) >= 1

    catalog = next(catalog for catalog in state_dict["catalogs"] if catalog["name"] == "cat_i")
    schema = next(schema for schema in catalog["schemas"] if schema["name"] == "core")
    table = next(table for table in schema["tables"] if table["name"] == "orders_v2")
    assert table.get("comment") == "orders table"
    assert (table.get("tags") or {}).get("domain") == "sales"
    assert not any(column["name"] == "note" for column in table.get("columns", []))

    view = next(view for view in schema["views"] if view["name"] == "v_orders")
    assert view.get("comment") == "orders view"
