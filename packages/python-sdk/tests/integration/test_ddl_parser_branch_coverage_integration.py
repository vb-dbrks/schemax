"""Branch-focused integration tests for Unity DDL parser public APIs (no mocks)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlglot import expressions as exp

from schemax.providers.unity import ddl_parser as parser_module
from schemax.providers.unity.ddl_parser import (
    DDL_STATEMENT_LIMIT,
    AlterCatalogSetTags,
    AlterSchemaSetTags,
    AlterTableAddColumn,
    AlterTableAlterColumn,
    AlterTableDropColumn,
    AlterTableRenameColumn,
    AlterTableRenameTo,
    AlterTableSetTags,
    AlterTableSetTblproperties,
    ColumnDef,
    CommentOn,
    CreateCatalog,
    CreateSchema,
    CreateTable,
    CreateView,
    ParseError,
    UnityDDLStateBuilder,
    Unsupported,
    parse_ddl_statement,
    state_from_ddl,
)


@pytest.mark.integration
def test_parse_ddl_statement_command_and_alter_shapes() -> None:
    schema_comment = parse_ddl_statement("COMMENT ON SCHEMA only_schema IS 's'")
    assert isinstance(schema_comment, Unsupported)
    assert schema_comment.reason.startswith("comment_on_shape")

    view_comment = parse_ddl_statement("COMMENT ON VIEW s.v IS 'v'")
    assert isinstance(view_comment, CommentOn)
    assert view_comment.name == "v"

    table_comment = parse_ddl_statement("COMMENT ON TABLE t IS 't'")
    assert isinstance(table_comment, CommentOn)
    assert table_comment.name == "t"

    malformed_schema_tags = parse_ddl_statement("ALTER SCHEMA malformed SET TAGS")
    assert isinstance(malformed_schema_tags, Unsupported)

    two_part_drop = parse_ddl_statement("ALTER TABLE s.t DROP COLUMN c1")
    assert isinstance(two_part_drop, AlterTableDropColumn)
    assert two_part_drop.catalog == "__implicit__"
    assert two_part_drop.schema_name == "s"

    one_part_drop = parse_ddl_statement("ALTER TABLE t DROP COLUMN c1")
    assert isinstance(one_part_drop, AlterTableDropColumn)
    assert one_part_drop.schema_name == ""
    assert one_part_drop.table_name == "t"

    alter_catalog_tags = parse_ddl_statement("ALTER CATALOG c1 SET TAGS ('owner' = 'platform')")
    assert isinstance(alter_catalog_tags, AlterCatalogSetTags)
    assert alter_catalog_tags.tags["owner"] == "platform"

    alter_schema_tags = parse_ddl_statement("ALTER SCHEMA c1.s1 SET TAGS ('tier' = 'silver')")
    assert isinstance(alter_schema_tags, AlterSchemaSetTags)
    assert alter_schema_tags.tags["tier"] == "silver"

    alter_table_tags = parse_ddl_statement("ALTER TABLE c1.s1.t1 SET TAGS ('domain' = 'sales')")
    assert isinstance(alter_table_tags, AlterTableSetTags)
    assert alter_table_tags.tags["domain"] == "sales"

    unknown_alter = parse_ddl_statement("ALTER MAGIC unknown")
    assert isinstance(unknown_alter, Unsupported)
    assert unknown_alter.reason == "command"


@pytest.mark.integration
def test_parse_ddl_statement_error_and_create_kind_paths() -> None:
    empty = parse_ddl_statement("", index=7)
    assert isinstance(empty, Unsupported)
    assert empty.reason == "empty"
    assert empty.index == 7

    invalid = parse_ddl_statement("CREATE TABLE broken (", index=9)
    assert isinstance(invalid, ParseError)
    assert invalid.index == 9

    unsupported_create = parse_ddl_statement("CREATE FUNCTION c1.s1.fn AS $$ SELECT 1 $$", index=10)
    assert isinstance(unsupported_create, Unsupported)
    assert unsupported_create.reason.startswith("create_kind:")


class _NoNameHasThis:
    def __init__(self, value: object) -> None:
        self.this = value


@pytest.mark.integration
def test_private_helper_edges_via_dynamic_lookup() -> None:
    get_name = getattr(parser_module, "_get_name")
    parse_command_statement = getattr(parser_module, "_parse_command_statement")
    parse_create_catalog = getattr(parser_module, "_parse_create_catalog")
    parse_create_schema = getattr(parser_module, "_parse_create_schema")
    parse_create_table = getattr(parser_module, "_parse_create_table")
    parse_create_view = getattr(parser_module, "_parse_create_view")
    parse_tblproperties_action = getattr(parser_module, "_parse_tblproperties_action")
    parse_alter_action = getattr(parser_module, "_parse_alter_action")
    parse_alter_statement = getattr(parser_module, "_parse_alter_statement")

    assert get_name(None) == ""
    assert get_name(_NoNameHasThis(exp.Identifier(this="z", quoted=False))) == "z"

    malformed_catalog = parse_command_statement("ALTER CATALOG broken SET TAGS", 0)
    assert isinstance(malformed_catalog, Unsupported)

    assert isinstance(parse_create_catalog(exp.Create(this=exp.Boolean(this=True)), 0), Unsupported)
    assert isinstance(parse_create_schema(exp.Create(this=exp.Boolean(this=True)), 0), Unsupported)
    assert isinstance(
        parse_create_table(
            exp.Create(this=exp.Table(this=exp.Identifier(this="t"))), "CREATE TABLE t", 0
        ),
        Unsupported,
    )
    assert isinstance(parse_create_view(exp.Create(this=exp.Boolean(this=True)), 0), Unsupported)

    assert not parse_tblproperties_action(exp.AlterSet(expressions=[]))
    assert parse_tblproperties_action(
        exp.AlterSet(
            expressions=[
                exp.Properties(expressions=[exp.Property(this=exp.Literal.string("k"), value=None)])
            ]
        )
    ) == {"k": ""}

    assert parse_alter_action(exp.CurrentDate(), "c", "s", "t") is None
    assert isinstance(parse_alter_statement(exp.Alter(this=exp.Boolean(this=True)), 0), Unsupported)


@pytest.mark.integration
def test_state_builder_add_contracts_cover_duplicate_and_missing_handlers() -> None:
    builder = UnityDDLStateBuilder()

    builder.add(CreateCatalog(name="c1"))
    builder.add(CreateCatalog(name="c1"))  # duplicate no-op
    builder.add(CreateSchema(catalog="c1", name="s1"))
    builder.add(CreateSchema(catalog="c1", name="s1"))  # duplicate no-op
    builder.add(CreateTable(catalog="c1", schema_name="s1", name="t1", columns=[]))
    builder.add(
        CreateTable(catalog="c1", schema_name="s1", name="t1", columns=[])
    )  # duplicate no-op
    builder.add(CreateView(catalog="c1", schema_name="s1", name="v1", definition="SELECT 1"))
    builder.add(CreateView(catalog="c1", schema_name="s1", name="v1", definition="SELECT 1"))

    builder.add(
        CommentOn(
            object_type="unknown", catalog="c1", schema_name=None, name="x", comment="ignored"
        )
    )

    builder.add(
        AlterTableAddColumn(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            column=ColumnDef(name="c", type="INT"),
        )
    )
    builder.add(
        AlterTableDropColumn(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            column_name="c",
        )
    )
    builder.add(
        AlterTableRenameColumn(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            old_name="old",
            new_name="new",
        )
    )
    builder.add(
        AlterTableAlterColumn(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            column_name="c",
            nullable=False,
        )
    )
    builder.add(
        AlterTableRenameTo(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            new_name="new_t",
        )
    )
    builder.add(
        AlterTableSetTblproperties(
            catalog="c1",
            schema_name="s1",
            table_name="missing",
            properties={"k": "v"},
        )
    )
    builder.add(AlterCatalogSetTags(name="missing_cat", tags={"k": "v"}))
    builder.add(AlterSchemaSetTags(catalog="c1", schema_name="missing", tags={"k": "v"}))
    builder.add(
        AlterTableSetTags(catalog="c1", schema_name="s1", table_name="missing", tags={"k": "v"})
    )

    state = builder.build().model_dump(by_alias=True)
    report = builder.get_report()
    assert any(catalog["name"] == "c1" for catalog in state["catalogs"])
    assert report["created"]["catalogs"] >= 1
    assert report["created"]["schemas"] >= 1
    assert report["created"]["tables"] >= 1
    assert report["created"]["views"] >= 1


@pytest.mark.integration
def test_state_from_ddl_error_paths_and_sql_path_branch(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sql_path or sql_statements"):
        state_from_ddl()

    with pytest.raises(ValueError, match="maximum is"):
        state_from_ddl(sql_statements=["CREATE CATALOG c"] * (DDL_STATEMENT_LIMIT + 1))

    sql_file = tmp_path / "one.sql"
    sql_file.write_text("CREATE CATALOG c_file;", encoding="utf-8")
    state, report = state_from_ddl(sql_path=sql_file)
    assert any(catalog["name"] == "c_file" for catalog in state["catalogs"])
    assert report["created"]["catalogs"] >= 1
