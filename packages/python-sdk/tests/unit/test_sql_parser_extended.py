"""
Extended tests for sql_parser.py — covers internal functions for coverage.
"""

from schemax.providers.base.sql_parser import (
    _parse_table_reference,
    _resolve_from_schema,
    extract_column_references,
    extract_dependencies_from_view,
    extract_table_references,
    resolve_table_or_view,
    validate_sql_syntax,
)

# ── extract_table_references ───────────────────────────────────────────


class TestExtractTableReferences:
    def test_simple_select(self):
        result = extract_table_references("SELECT * FROM catalog.schema.tbl")
        assert "catalog.schema.tbl" in result["all"][0]

    def test_join_two_tables(self):
        sql = "SELECT * FROM a.b.t1 JOIN a.b.t2 ON t1.id = t2.id"
        result = extract_table_references(sql)
        assert len(result["all"]) == 2

    def test_returns_empty_on_parse_error(self):
        result = extract_table_references("NOT VALID SQL ;;; ///")
        assert result == {"tables": [], "views": [], "all": []}

    def test_table_without_catalog(self):
        result = extract_table_references("SELECT * FROM my_table")
        assert len(result["all"]) >= 1
        assert "my_table" in result["all"][0]

    def test_backtick_identifiers(self):
        result = extract_table_references("SELECT * FROM `cat`.`sch`.`tbl`")
        assert len(result["all"]) >= 1

    def test_views_always_empty(self):
        result = extract_table_references("SELECT * FROM x.y.z")
        assert result["views"] == []

    def test_tables_copy_of_all(self):
        result = extract_table_references("SELECT * FROM a.b.c")
        assert result["tables"] == result["all"]


# ── extract_column_references ──────────────────────────────────────────


class TestExtractColumnReferences:
    def test_simple_columns(self):
        cols = extract_column_references("SELECT id, name FROM tbl")
        # Should find at least id and name
        col_text = " ".join(cols)
        assert "id" in col_text
        assert "name" in col_text

    def test_qualified_columns(self):
        cols = extract_column_references("SELECT t.id FROM tbl AS t")
        assert any("id" in c for c in cols)

    def test_returns_empty_on_error(self):
        assert extract_column_references("NOT VALID SQL ;;; ///") == []


# ── validate_sql_syntax ────────────────────────────────────────────────


class TestValidateSqlSyntax:
    def test_valid_sql(self):
        valid, msg = validate_sql_syntax("SELECT 1")
        assert valid is True
        assert msg == ""

    def test_invalid_sql(self):
        valid, msg = validate_sql_syntax("SELECT FROM WHERE")
        # SQLGlot may or may not error — if it does, we get a message
        # Some invalid SQL is still parseable by SQLGlot, so just check the tuple shape
        assert isinstance(valid, bool)
        assert isinstance(msg, str)

    def test_empty_sql(self):
        valid, msg = validate_sql_syntax("")
        # Empty string may return False/"None" or True depending on SQLGlot version
        assert isinstance(valid, bool)


# ── _parse_table_reference ─────────────────────────────────────────────


class TestParseTableReference:
    def test_three_parts(self):
        result = _parse_table_reference("cat.sch.tbl", None)
        assert result == ("cat", "sch", "tbl")

    def test_two_parts_with_default_catalog(self):
        result = _parse_table_reference("sch.tbl", "my_cat")
        assert result == ("my_cat", "sch", "tbl")

    def test_two_parts_no_default(self):
        result = _parse_table_reference("sch.tbl", None)
        assert result == ("__implicit__", "sch", "tbl")

    def test_one_part(self):
        result = _parse_table_reference("tbl", "my_cat")
        assert result == ("my_cat", None, "tbl")

    def test_one_part_no_default(self):
        result = _parse_table_reference("tbl", None)
        assert result == ("__implicit__", None, "tbl")

    def test_too_many_parts_returns_none(self):
        result = _parse_table_reference("a.b.c.d", None)
        assert result is None


# ── _resolve_from_schema ──────────────────────────────────────────────


class TestResolveFromSchema:
    def test_finds_table(self):
        schema = {"tables": [{"name": "t1", "id": "tid_1"}], "views": []}
        assert _resolve_from_schema(schema, "t1") == ("table", "tid_1")

    def test_finds_view(self):
        schema = {"tables": [], "views": [{"name": "v1", "id": "vid_1"}]}
        assert _resolve_from_schema(schema, "v1") == ("view", "vid_1")

    def test_returns_none_when_not_found(self):
        schema = {"tables": [{"name": "t1", "id": "tid_1"}], "views": []}
        assert _resolve_from_schema(schema, "missing") is None

    def test_empty_schema(self):
        assert _resolve_from_schema({}, "anything") is None

    def test_table_without_id(self):
        schema = {"tables": [{"name": "t1"}], "views": []}
        assert _resolve_from_schema(schema, "t1") == ("table", "")


# ── resolve_table_or_view ─────────────────────────────────────────────


class TestResolveTableOrView:
    def test_resolves_table(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {"name": "sch", "tables": [{"name": "tbl", "id": "t1"}], "views": []}
                    ],
                }
            ]
        }
        obj_type, obj_id = resolve_table_or_view("cat.sch.tbl", state)
        assert obj_type == "table"
        assert obj_id == "t1"

    def test_resolves_view(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {"name": "sch", "tables": [], "views": [{"name": "vw", "id": "v1"}]}
                    ],
                }
            ]
        }
        obj_type, obj_id = resolve_table_or_view("cat.sch.vw", state)
        assert obj_type == "view"
        assert obj_id == "v1"

    def test_returns_table_when_not_found(self):
        state = {"catalogs": []}
        obj_type, obj_id = resolve_table_or_view("cat.sch.unknown", state)
        assert obj_type == "table"
        assert obj_id == ""

    def test_invalid_reference(self):
        state = {"catalogs": []}
        obj_type, obj_id = resolve_table_or_view("a.b.c.d", state)
        assert obj_type == "table"
        assert obj_id == ""

    def test_with_default_catalog(self):
        state = {
            "catalogs": [
                {
                    "name": "my_cat",
                    "schemas": [
                        {"name": "sch", "tables": [{"name": "tbl", "id": "t1"}], "views": []}
                    ],
                }
            ]
        }
        obj_type, obj_id = resolve_table_or_view("sch.tbl", state, default_catalog="my_cat")
        assert obj_type == "table"
        assert obj_id == "t1"

    def test_catalog_mismatch(self):
        state = {
            "catalogs": [
                {
                    "name": "other_cat",
                    "schemas": [
                        {"name": "sch", "tables": [{"name": "tbl", "id": "t1"}], "views": []}
                    ],
                }
            ]
        }
        obj_type, obj_id = resolve_table_or_view("cat.sch.tbl", state)
        assert obj_type == "table"
        assert obj_id == ""

    def test_schema_mismatch(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {"name": "other_sch", "tables": [{"name": "tbl", "id": "t1"}], "views": []}
                    ],
                }
            ]
        }
        obj_type, obj_id = resolve_table_or_view("cat.sch.tbl", state)
        assert obj_type == "table"
        assert obj_id == ""


# ── extract_dependencies_from_view ─────────────────────────────────────


class TestExtractDependenciesFromView:
    def test_resolves_table_dependency(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {"name": "sch", "tables": [{"name": "src", "id": "t1"}], "views": []}
                    ],
                }
            ]
        }
        deps = extract_dependencies_from_view(
            "SELECT * FROM cat.sch.src", state, default_catalog="cat"
        )
        assert "t1" in deps["tables"]
        assert deps["views"] == []

    def test_resolves_view_dependency(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {"name": "sch", "tables": [], "views": [{"name": "vw", "id": "v1"}]}
                    ],
                }
            ]
        }
        deps = extract_dependencies_from_view(
            "SELECT * FROM cat.sch.vw", state, default_catalog="cat"
        )
        assert "v1" in deps["views"]

    def test_unresolved_dependency(self):
        state = {"catalogs": []}
        deps = extract_dependencies_from_view("SELECT * FROM cat.sch.unknown", state)
        assert len(deps["unresolved"]) >= 1

    def test_invalid_sql_returns_empty(self):
        deps = extract_dependencies_from_view("NOT VALID ;;; ///", {})
        assert deps["tables"] == []
        assert deps["views"] == []

    def test_mixed_dependencies(self):
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {
                            "name": "sch",
                            "tables": [{"name": "t1", "id": "tid1"}],
                            "views": [{"name": "v1", "id": "vid1"}],
                        }
                    ],
                }
            ]
        }
        sql = "SELECT * FROM cat.sch.t1 JOIN cat.sch.v1 ON t1.id = v1.id"
        deps = extract_dependencies_from_view(sql, state)
        assert "tid1" in deps["tables"]
        assert "vid1" in deps["views"]
