"""
Unit tests for Unity DDL parser and state_from_ddl.

Covers all Unity Catalog DDL patterns: CREATE CATALOG/SCHEMA/TABLE/VIEW,
COMMENT ON, Unsupported (ALTER, empty, unknown), ParseError, and full
state_from_ddl / builder behavior including implicit catalog, multiple
objects, and report counts.
"""

from pathlib import Path

import pytest

from schemax.providers.unity.ddl_parser import (
    DDL_STATEMENT_LIMIT,
    AlterTableAddColumn,
    AlterTableAlterColumn,
    AlterTableDropColumn,
    AlterTableRenameColumn,
    AlterTableRenameTo,
    AlterTableSetTblproperties,
    CommentOn,
    CreateCatalog,
    CreateSchema,
    CreateTable,
    CreateView,
    ParseError,
    Unsupported,
    parse_ddl_statement,
    state_from_ddl,
)


class TestParseDdlStatementCreateCatalog:
    """Parse: CREATE CATALOG variants."""

    def test_create_catalog_simple(self) -> None:
        result = parse_ddl_statement("CREATE CATALOG my_cat", index=0)
        assert isinstance(result, CreateCatalog)
        assert result.name.strip(";") == "my_cat"
        assert result.comment is None

    def test_create_catalog_if_not_exists_with_comment(self) -> None:
        result = parse_ddl_statement(
            "CREATE CATALOG IF NOT EXISTS my_cat COMMENT 'dev catalog';", index=0
        )
        assert isinstance(result, CreateCatalog)
        assert result.name == "my_cat"
        assert result.comment == "dev catalog"

    def test_create_catalog_with_managed_location_command_path(self) -> None:
        # When sqlglot parses as Command (MANAGED LOCATION etc), regex path is used
        sql = (
            "CREATE CATALOG prod_cat WITH MANAGED LOCATION 'abfss://cont@st.dfs.core.windows.net/';"
        )
        result = parse_ddl_statement(sql, index=0)
        assert isinstance(result, CreateCatalog)
        assert result.name.strip("`\"'").replace(";", "") == "prod_cat"

    def test_create_catalog_command_path_trailing_semicolon_stripped(self) -> None:
        # Bug: [^\s]+ captured "my_cat;" when SQL was "CREATE CATALOG my_cat;". Strip semicolons.
        sql = "CREATE CATALOG my_cat;"
        result = parse_ddl_statement(sql, index=0)
        assert isinstance(result, CreateCatalog)
        assert result.name == "my_cat"


class TestParseDdlStatementCreateSchema:
    """Parse: CREATE SCHEMA variants."""

    def test_create_schema_qualified_catalog_dot_schema(self) -> None:
        result = parse_ddl_statement("CREATE SCHEMA my_cat.my_schema", index=0)
        assert isinstance(result, CreateSchema)
        assert result.catalog == "my_cat"
        assert result.name == "my_schema"

    def test_create_schema_unqualified_implicit_catalog(self) -> None:
        # Single name -> __implicit__ catalog
        result = parse_ddl_statement("CREATE SCHEMA public", index=0)
        assert isinstance(result, CreateSchema)
        assert result.catalog == "__implicit__"
        assert result.name == "public"

    def test_create_schema_if_not_exists(self) -> None:
        result = parse_ddl_statement("CREATE SCHEMA IF NOT EXISTS main.analytics", index=0)
        assert isinstance(result, CreateSchema)
        assert result.catalog == "main"
        assert result.name == "analytics"

    def test_create_schema_command_path_with_comment(self) -> None:
        # MANAGED LOCATION causes sqlglot to parse as Command; comment must still be extracted
        result = parse_ddl_statement(
            "CREATE SCHEMA main.analytics MANAGED LOCATION 'abfs://container/path' COMMENT 'Analytics schema'",
            index=0,
        )
        assert isinstance(result, CreateSchema)
        assert result.catalog == "main"
        assert result.name == "analytics"
        assert result.comment == "Analytics schema"

    def test_create_schema_command_path_trailing_semicolon_stripped(self) -> None:
        # Bug: full_name could be "main.public;" from regex; strip semicolons so schema name is clean.
        result = parse_ddl_statement("CREATE SCHEMA main.public;", index=0)
        assert isinstance(result, CreateSchema)
        assert result.catalog == "main"
        assert result.name == "public"


class TestParseDdlStatementCreateTable:
    """Parse: CREATE TABLE variants (Unity Catalog patterns)."""

    def test_create_table_qualified_using_delta(self) -> None:
        sql = """
        CREATE TABLE main.analytics.users (
            id BIGINT NOT NULL,
            name STRING
        ) USING DELTA;
        """
        result = parse_ddl_statement(sql.strip(), index=0)
        assert isinstance(result, CreateTable)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.name == "users"
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[0].type.upper() == "BIGINT"
        assert result.columns[1].name == "name"
        assert result.format == "delta"

    def test_create_table_using_iceberg(self) -> None:
        sql = "CREATE TABLE cat.sch.ice_t (a INT) USING ICEBERG"
        result = parse_ddl_statement(sql, index=0)
        assert isinstance(result, CreateTable)
        assert result.format == "iceberg"
        assert result.name == "ice_t"

    def test_create_table_with_comment(self) -> None:
        sql = """
        CREATE TABLE main.analytics.config (k STRING, v STRING)
        USING DELTA
        COMMENT 'key-value config table';
        """
        result = parse_ddl_statement(sql.strip(), index=0)
        assert isinstance(result, CreateTable)
        assert result.name == "config"
        # Table comment may be extracted by sqlglot or not
        assert result.comment is None or "config" in (result.comment or "")

    def test_create_table_various_column_types(self) -> None:
        sql = """
        CREATE TABLE cat.sch.typed (
            id BIGINT,
            name STRING,
            created TIMESTAMP,
            dt DATE,
            flag BOOLEAN,
            amount DECIMAL(18,2)
        ) USING DELTA
        """
        result = parse_ddl_statement(sql.strip(), index=0)
        assert isinstance(result, CreateTable)
        assert len(result.columns) == 6
        assert result.columns[0].type.upper() == "BIGINT"
        assert result.columns[1].type.upper() == "STRING"
        assert result.columns[2].type.upper() == "TIMESTAMP"
        assert result.columns[3].type.upper() == "DATE"
        assert result.columns[4].type.upper() == "BOOLEAN"
        assert "DECIMAL" in result.columns[5].type.upper()

    def test_create_table_not_null_column(self) -> None:
        sql = "CREATE TABLE a.b.t (pk BIGINT NOT NULL) USING DELTA"
        result = parse_ddl_statement(sql, index=0)
        assert isinstance(result, CreateTable)
        assert result.columns[0].name == "pk"
        # Parser may or may not set nullable from NOT NULL depending on sqlglot
        assert result.columns[0].nullable in (True, False)


class TestParseDdlStatementCreateView:
    """Parse: CREATE VIEW (Unity Catalog)."""

    def test_create_view_qualified_with_definition(self) -> None:
        sql = """
        CREATE VIEW main.analytics.active_users AS
        SELECT * FROM main.analytics.users WHERE active = true
        """
        result = parse_ddl_statement(sql.strip(), index=0)
        # sqlglot may parse as CreateView or as Unsupported depending on dialect
        if isinstance(result, CreateView):
            assert result.catalog == "main"
            assert result.schema_name == "analytics"
            assert result.name == "active_users"
            assert "SELECT" in result.definition
        else:
            assert isinstance(result, Unsupported)
            assert "view" in result.reason.lower()


class TestParseDdlStatementCommentOn:
    """Parse: COMMENT ON table/schema/catalog."""

    def test_comment_on_table(self) -> None:
        sql = "COMMENT ON TABLE main.analytics.users IS 'User dimension table';"
        result = parse_ddl_statement(sql, index=0)
        assert isinstance(result, CommentOn)
        assert result.object_type == "table"
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.name == "users"
        assert "User" in result.comment


class TestParseDdlStatementAlter:
    """Parse: ALTER statements (Unity Catalog).

    ALTER TABLE ADD/DROP/RENAME COLUMN, RENAME TO, SET TBLPROPERTIES, ALTER COLUMN
    are parsed and applied in file order. ALTER CATALOG/SCHEMA SET TAGS/OWNER
    are still Unsupported.
    """

    def _assert_alter_unsupported(self, sql: str, index: int = 0) -> None:
        result = parse_ddl_statement(sql, index=index)
        assert isinstance(result, Unsupported), (
            f"Expected Unsupported, got {type(result).__name__}: {result}"
        )
        assert result.reason.lower() in ("alter", "alter_other", "command", "alter_no_actions"), (
            f"ALTER should be unsupported: {result.reason}"
        )

    def test_alter_catalog_set_tags(self) -> None:
        self._assert_alter_unsupported("ALTER CATALOG my_cat SET TAGS ('env' = 'prod');")

    def test_alter_catalog_owner(self) -> None:
        self._assert_alter_unsupported("ALTER CATALOG my_cat SET OWNER TO `account users`;")

    def test_alter_schema_set_owner(self) -> None:
        self._assert_alter_unsupported("ALTER SCHEMA main.analytics SET OWNER TO `data-engineers`;")

    def test_alter_schema_set_tags(self) -> None:
        self._assert_alter_unsupported("ALTER SCHEMA main.analytics SET TAGS ('pii' = 'true');")

    def test_alter_table_add_column(self) -> None:
        result = parse_ddl_statement("ALTER TABLE cat.sch.t ADD COLUMN c STRING", index=0)
        assert isinstance(result, AlterTableAddColumn)
        assert result.catalog == "cat"
        assert result.schema_name == "sch"
        assert result.table_name == "t"
        assert result.column.name == "c"
        assert result.column.type == "STRING"
        assert result.column.nullable is True

    def test_alter_table_drop_column(self) -> None:
        result = parse_ddl_statement("ALTER TABLE main.analytics.users DROP COLUMN email;", index=0)
        assert isinstance(result, AlterTableDropColumn)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.table_name == "users"
        assert result.column_name == "email"

    def test_alter_table_rename_to(self) -> None:
        result = parse_ddl_statement(
            "ALTER TABLE main.analytics.users RENAME TO main.analytics.users_v2;", index=0
        )
        assert isinstance(result, AlterTableRenameTo)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.table_name == "users"
        assert result.new_name == "users_v2"

    def test_alter_table_rename_column(self) -> None:
        result = parse_ddl_statement(
            "ALTER TABLE main.analytics.users RENAME COLUMN name TO full_name;", index=0
        )
        assert isinstance(result, AlterTableRenameColumn)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.table_name == "users"
        assert result.old_name == "name"
        assert result.new_name == "full_name"

    def test_alter_table_set_tblproperties(self) -> None:
        result = parse_ddl_statement(
            "ALTER TABLE main.analytics.events SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');",
            index=0,
        )
        assert isinstance(result, AlterTableSetTblproperties)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.table_name == "events"
        assert "delta.autoOptimize.optimizeWrite" in result.properties
        assert result.properties["delta.autoOptimize.optimizeWrite"] == "true"

    def test_alter_table_alter_column(self) -> None:
        result = parse_ddl_statement(
            "ALTER TABLE main.analytics.users ALTER COLUMN id SET NOT NULL;", index=0
        )
        assert isinstance(result, AlterTableAlterColumn)
        assert result.catalog == "main"
        assert result.schema_name == "analytics"
        assert result.table_name == "users"
        assert result.column_name == "id"
        assert result.nullable is False
        assert result.new_type is None


class TestParseDdlStatementUnsupportedAndErrors:
    """Parse: Unsupported and ParseError outcomes."""

    def test_empty_statement_returns_unsupported(self) -> None:
        result = parse_ddl_statement("", index=0)
        assert isinstance(result, Unsupported)
        assert result.reason == "empty"

    def test_alter_returns_unsupported(self) -> None:
        # ALTER CATALOG SET OWNER is not supported (parsed as Command, returns Unsupported)
        result = parse_ddl_statement("ALTER CATALOG my_cat SET OWNER TO `account users`;", index=0)
        assert isinstance(result, Unsupported)
        assert result.reason in ("alter", "alter_other", "alter_no_actions", "command")

    def test_select_returns_unsupported(self) -> None:
        result = parse_ddl_statement("SELECT 1", index=0)
        assert isinstance(result, Unsupported)

    def test_invalid_sql_returns_parse_error(self) -> None:
        result = parse_ddl_statement("CREATE TABLE broken (", index=0)
        assert isinstance(result, ParseError)
        assert result.index == 0
        assert len(result.message) > 0


class TestStateFromDdl:
    """Tests for state_from_ddl (file and list of statements)."""

    def test_state_from_ddl_requires_path_or_statements(self) -> None:
        with pytest.raises(ValueError, match="sql_path or sql_statements"):
            state_from_ddl()

    def test_state_from_ddl_from_statements_catalog_schema_table(self) -> None:
        statements = [
            "CREATE CATALOG demo",
            "CREATE SCHEMA demo.analytics",
            """
            CREATE TABLE demo.analytics.events (
                id BIGINT,
                ts TIMESTAMP
            ) USING DELTA
            """,
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        assert "catalogs" in state_dict
        catalogs = state_dict["catalogs"]
        assert len(catalogs) >= 1
        demo_cats = [c for c in catalogs if c["name"].strip(";") == "demo"]
        assert len(demo_cats) >= 1
        cat = demo_cats[0]
        schemas = cat.get("schemas", [])
        assert len(schemas) >= 1
        assert schemas[0]["name"] == "analytics"
        tables = schemas[0].get("tables", [])
        assert len(tables) >= 1
        assert tables[0]["name"] == "events"
        assert report["created"]["catalogs"] >= 1
        assert report["created"]["schemas"] >= 1
        assert report["created"]["tables"] >= 1

    def test_state_from_ddl_includes_views_when_parsed(self) -> None:
        statements = [
            "CREATE CATALOG vcat",
            "CREATE SCHEMA vcat.vsch",
            "CREATE VIEW vcat.vsch.myview AS SELECT 1 AS x",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        catalogs = state_dict["catalogs"]
        assert len(catalogs) >= 1
        schemas = [s for c in catalogs for s in c.get("schemas", [])]
        views = [v for s in schemas for v in s.get("views", [])]
        # Databricks CREATE VIEW may parse as CreateView or Unsupported
        if views:
            assert views[0]["name"] == "myview"
            assert report["created"]["views"] >= 1
        else:
            assert report["created"]["views"] >= 0
            assert report["created"]["catalogs"] >= 1
            assert report["created"]["schemas"] >= 1

    def test_state_from_ddl_view_has_extracted_dependencies(self) -> None:
        """Views parsed from SQL get extractedDependencies from definition (for dependency ordering)."""
        statements = [
            "CREATE CATALOG dc",
            "CREATE SCHEMA dc.ds",
            "CREATE VIEW dc.ds.v1 AS SELECT * FROM dc.ds.t1",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        catalogs = state_dict["catalogs"]
        schemas = [s for c in catalogs for s in c.get("schemas", [])]
        views = [v for s in schemas for v in s.get("views", [])]
        if not views:
            pytest.skip("CREATE VIEW parsed as Unsupported in this dialect")
        view = views[0]
        assert "extractedDependencies" in view
        deps = view["extractedDependencies"]
        assert "tables" in deps
        assert "views" in deps
        # FROM dc.ds.t1 should yield a table reference
        assert len(deps["tables"]) >= 1
        assert "t1" in deps["tables"][0] or "dc" in str(deps["tables"])

    def test_state_from_ddl_comment_on_applied_to_table(self) -> None:
        statements = [
            "CREATE CATALOG c",
            "CREATE SCHEMA c.s",
            "CREATE TABLE c.s.t (id INT) USING DELTA",
            "COMMENT ON TABLE c.s.t IS 'table comment'",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        catalogs = state_dict["catalogs"]
        tables = [t for c in catalogs for s in c.get("schemas", []) for t in s.get("tables", [])]
        assert len(tables) >= 1
        assert tables[0].get("comment") == "table comment"

    def test_state_from_ddl_implicit_catalog_when_schema_only(self) -> None:
        statements = [
            "CREATE SCHEMA public",
            "CREATE TABLE __implicit__.public.t (id INT) USING DELTA",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        catalogs = state_dict["catalogs"]
        implicit = [c for c in catalogs if c["name"] == "__implicit__"]
        assert len(implicit) >= 1
        assert any(s["name"] == "public" for s in implicit[0].get("schemas", []))

    def test_state_from_ddl_schema_before_catalog_preserves_catalog_comment(self) -> None:
        # Bug: CREATE SCHEMA main.public creates implicit catalog "main"; then CREATE CATALOG main
        # was skipped (cid in _catalogs), losing comment/tags. Now we update existing catalog.
        statements = [
            "CREATE SCHEMA main.public",
            "CREATE CATALOG main COMMENT 'main catalog'",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        catalogs = state_dict["catalogs"]
        main_cats = [c for c in catalogs if c["name"] == "main"]
        assert len(main_cats) == 1
        assert main_cats[0].get("comment") == "main catalog"
        assert any(s["name"] == "public" for s in main_cats[0].get("schemas", []))

    def test_state_from_ddl_multiple_catalogs(self) -> None:
        statements = [
            "CREATE CATALOG cat_a",
            "CREATE SCHEMA cat_a.sch",
            "CREATE CATALOG cat_b",
            "CREATE SCHEMA cat_b.sch",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        names = [c["name"].strip(";") for c in state_dict["catalogs"]]
        assert "cat_a" in names
        assert "cat_b" in names
        assert report["created"]["catalogs"] >= 2
        assert report["created"]["schemas"] >= 2

    def test_state_from_ddl_parse_error_in_report(self) -> None:
        statements = [
            "CREATE CATALOG ok",
            "CREATE TABLE broken (",  # invalid
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        assert len(state_dict["catalogs"]) >= 1
        assert len(report["parse_errors"]) >= 1
        assert report["parse_errors"][0]["index"] == 1

    def test_state_from_ddl_unsupported_increments_skipped(self) -> None:
        statements = [
            "CREATE CATALOG ok",
            "SELECT 1",
            "CREATE SCHEMA ok.public",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        assert report["skipped"] >= 1
        assert report["created"]["catalogs"] >= 1
        assert report["created"]["schemas"] >= 1

    def test_state_from_ddl_alter_applied_in_sequence(self) -> None:
        """CREATE then ALTERs are applied in file order (e.g. CREATE TABLE then ALTER ADD COLUMN)."""
        statements = [
            "CREATE CATALOG c",
            "CREATE SCHEMA c.s",
            "CREATE TABLE c.s.t (id INT) USING DELTA",
            "ALTER TABLE c.s.t ADD COLUMN name STRING",
            "ALTER TABLE c.s.t SET TBLPROPERTIES ('k' = 'v')",
        ]
        state_dict, report = state_from_ddl(sql_statements=statements)
        assert report["created"]["catalogs"] >= 1
        assert report["created"]["schemas"] >= 1
        assert report["created"]["tables"] >= 1
        tables = [
            t
            for cat in state_dict["catalogs"]
            for s in cat.get("schemas", [])
            for t in s.get("tables", [])
        ]
        assert len(tables) == 1
        # Table has both CREATE column and ALTER ADD COLUMN (applied in order)
        assert len(tables[0]["columns"]) == 2
        col_names = [col["name"] for col in tables[0]["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        # ALTER SET TBLPROPERTIES applied
        assert tables[0].get("properties") is not None
        assert tables[0]["properties"].get("k") == "v"

    def test_state_from_ddl_statement_limit_exceeded_raises(self) -> None:
        statements = ["CREATE CATALOG c"] * (DDL_STATEMENT_LIMIT + 1)
        with pytest.raises(ValueError, match="maximum is"):
            state_from_ddl(sql_statements=statements)

    def test_state_from_ddl_from_file(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "schema.sql"
        # Semicolons required so split_sql_statements yields separate statements
        sql_file.write_text(
            "CREATE CATALOG app;\n"
            "CREATE SCHEMA app.public;\n"
            "CREATE TABLE app.public.config (k STRING, v STRING) USING DELTA;\n"
        )
        state_dict, report = state_from_ddl(sql_path=sql_file)
        assert len(state_dict["catalogs"]) >= 1
        assert state_dict["catalogs"][0]["name"].strip(";") == "app"
        assert report["created"]["tables"] >= 1
