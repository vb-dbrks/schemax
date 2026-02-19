"""
Unit tests for schemax.core.sql_utils (statement splitting).
"""

from schemax.core.sql_utils import split_sql_statements


class TestSplitSqlStatements:
    """Tests for split_sql_statements."""

    def test_empty_string_returns_empty_list(self) -> None:
        assert split_sql_statements("") == []
        assert split_sql_statements("   \n\n  ") == []

    def test_single_statement_no_semicolon(self) -> None:
        sql = "CREATE CATALOG my_catalog"
        assert split_sql_statements(sql) == [sql]

    def test_single_statement_with_semicolon(self) -> None:
        sql = "CREATE CATALOG my_catalog;"
        assert split_sql_statements(sql) == ["CREATE CATALOG my_catalog"]

    def test_multiple_statements(self) -> None:
        sql = "CREATE CATALOG a;\nCREATE SCHEMA a.s1;\nCREATE TABLE a.s1.t1 (id INT);"
        result = split_sql_statements(sql)
        assert len(result) == 3
        assert "CREATE CATALOG a" in result[0]
        assert "CREATE SCHEMA a.s1" in result[1]
        assert "CREATE TABLE a.s1.t1" in result[2]

    def test_semicolon_inside_single_quotes_not_split(self) -> None:
        sql = "CREATE TABLE t (c STRING COMMENT 'use; semicolon');"
        result = split_sql_statements(sql)
        assert len(result) == 1
        assert "use; semicolon" in result[0]

    def test_semicolon_inside_double_quotes_not_split(self) -> None:
        sql = 'SELECT "col;name" FROM t;'
        result = split_sql_statements(sql)
        assert len(result) == 1
        assert "col;name" in result[0]

    def test_comment_only_lines_skipped(self) -> None:
        sql = "-- comment\nCREATE CATALOG c;\n-- another\nCREATE SCHEMA c.s;"
        result = split_sql_statements(sql)
        assert len(result) == 2
        assert result[0].strip().startswith("CREATE CATALOG")
        assert result[1].strip().startswith("CREATE SCHEMA")

    def test_blank_lines_skipped_between_statements(self) -> None:
        sql = "CREATE CATALOG c;\n\n\nCREATE SCHEMA c.s;"
        result = split_sql_statements(sql)
        assert len(result) == 2

    def test_trailing_statement_without_semicolon(self) -> None:
        sql = "CREATE CATALOG c;\nCREATE SCHEMA c.s"
        result = split_sql_statements(sql)
        assert len(result) == 2
        assert result[1] == "CREATE SCHEMA c.s"
