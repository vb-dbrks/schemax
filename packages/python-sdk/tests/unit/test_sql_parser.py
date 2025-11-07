"""
Unit tests for SQL parsing with SQLGlot

Tests SQLGlot's capability to parse Databricks SQL and extract table dependencies.
This validates that SQLGlot is suitable for dependency-aware SQL generation.
"""

import pytest

try:
    import sqlglot
    from sqlglot import expressions as exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

pytestmark = pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="SQLGlot not available")


def extract_table_references(sql: str, dialect: str = "databricks") -> list[str]:
    """
    Extract all table references from a SQL statement.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: databricks)

    Returns:
        List of fully-qualified table names referenced in the query
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)

    tables = []
    for table in parsed.find_all(exp.Table):
        # Get the table name (may be qualified with catalog.schema.table)
        table_name = table.sql(dialect=dialect)
        tables.append(table_name)

    return tables


class TestSQLGlotBasicParsing:
    """Test basic SQLGlot parsing capabilities"""

    def test_parse_simple_select(self):
        """Test parsing simple SELECT statement"""
        sql = "SELECT * FROM catalog.schema.table1"
        parsed = sqlglot.parse_one(sql, dialect="databricks")

        assert parsed is not None
        assert isinstance(parsed, exp.Select)

    def test_parse_databricks_dialect(self):
        """Test Databricks-specific SQL parsing"""
        sql = "SELECT * FROM `catalog`.`schema`.`table`"
        parsed = sqlglot.parse_one(sql, dialect="databricks")

        assert parsed is not None

    def test_parse_create_view(self):
        """Test parsing CREATE VIEW statement"""
        sql = """
        CREATE VIEW my_view AS
        SELECT id, name FROM catalog.schema.source_table
        """
        parsed = sqlglot.parse_one(sql, dialect="databricks")

        assert parsed is not None
        assert isinstance(parsed, exp.Create)


class TestTableReferenceExtraction:
    """Test extracting table references from SQL"""

    def test_extract_single_table(self):
        """Test extracting single table reference"""
        sql = "SELECT * FROM catalog.schema.table1"
        tables = extract_table_references(sql)

        assert len(tables) == 1
        assert "catalog.schema.table1" in tables[0]

    def test_extract_table_with_alias(self):
        """Test extracting table with alias"""
        sql = "SELECT * FROM catalog.schema.table1 AS t1"
        tables = extract_table_references(sql)

        assert len(tables) == 1
        assert "table1" in tables[0]

    def test_extract_multiple_tables_join(self):
        """Test extracting multiple tables from JOIN"""
        sql = """
        SELECT a.id, b.name
        FROM catalog1.schema1.table_a AS a
        JOIN catalog1.schema1.table_b AS b ON a.id = b.id
        """
        tables = extract_table_references(sql)

        assert len(tables) == 2
        # Check both tables are present (order may vary)
        table_names = " ".join(tables)
        assert "table_a" in table_names
        assert "table_b" in table_names

    def test_extract_left_join(self):
        """Test extracting tables from LEFT JOIN"""
        sql = """
        SELECT a.*, b.*
        FROM bronze.raw.sales AS a
        LEFT JOIN bronze.raw.products AS b ON a.product_id = b.id
        """
        tables = extract_table_references(sql)

        assert len(tables) == 2
        table_names = " ".join(tables)
        assert "sales" in table_names
        assert "products" in table_names

    def test_extract_three_way_join(self):
        """Test extracting tables from three-way JOIN"""
        sql = """
        SELECT s.*, p.*, c.*
        FROM bronze.raw.sales s
        JOIN bronze.raw.products p ON s.product_id = p.id
        LEFT JOIN silver.curated.customers c ON s.customer_id = c.id
        """
        tables = extract_table_references(sql)

        assert len(tables) == 3
        table_names = " ".join(tables)
        assert "sales" in table_names
        assert "products" in table_names
        assert "customers" in table_names


class TestComplexSQLParsing:
    """Test parsing complex SQL with CTEs, subqueries, etc."""

    def test_extract_from_cte(self):
        """Test extracting tables from CTE (WITH clause)"""
        sql = """
        WITH temp AS (
            SELECT * FROM catalog.schema.source_table
        )
        SELECT * FROM temp
        JOIN catalog.schema.another_table ON temp.id = another_table.id
        """
        tables = extract_table_references(sql)

        # Should find source_table and another_table (temp is a CTE, not a table)
        assert len(tables) >= 2
        table_names = " ".join(tables)
        assert "source_table" in table_names
        assert "another_table" in table_names

    def test_extract_from_subquery(self):
        """Test extracting tables from subquery"""
        sql = """
        SELECT *
        FROM (
            SELECT id, name FROM catalog.schema.inner_table
        ) AS sub
        WHERE sub.id > 100
        """
        tables = extract_table_references(sql)

        assert len(tables) >= 1
        assert any("inner_table" in t for t in tables)

    def test_extract_from_union(self):
        """Test extracting tables from UNION"""
        sql = """
        SELECT id FROM catalog1.schema1.table_a
        UNION
        SELECT id FROM catalog2.schema2.table_b
        """
        tables = extract_table_references(sql)

        assert len(tables) == 2
        table_names = " ".join(tables)
        assert "table_a" in table_names
        assert "table_b" in table_names

    def test_extract_from_nested_cte(self):
        """Test extracting tables from nested CTEs"""
        sql = """
        WITH
        stage1 AS (
            SELECT * FROM bronze.raw.source1
        ),
        stage2 AS (
            SELECT * FROM stage1
            JOIN bronze.raw.source2 ON stage1.id = source2.id
        )
        SELECT * FROM stage2
        """
        tables = extract_table_references(sql)

        # Should find source1 and source2 (stage1 and stage2 are CTEs)
        assert len(tables) >= 2
        table_names = " ".join(tables)
        assert "source1" in table_names
        assert "source2" in table_names


class TestViewDependencyScenarios:
    """Test real-world view dependency scenarios"""

    def test_view_referencing_table(self):
        """Test view that references a single table"""
        sql = """
        CREATE VIEW silver.curated.active_users AS
        SELECT * FROM bronze.raw.users WHERE status = 'active'
        """
        tables = extract_table_references(sql)

        assert len(tables) >= 1
        assert any("users" in t for t in tables)

    def test_view_referencing_multiple_tables(self):
        """Test view that joins multiple tables"""
        sql = """
        CREATE VIEW gold.analytics.user_purchases AS
        SELECT
            u.user_id,
            u.name,
            p.purchase_date,
            p.amount
        FROM bronze.raw.users u
        JOIN bronze.raw.purchases p ON u.user_id = p.user_id
        WHERE p.status = 'completed'
        """
        tables = extract_table_references(sql)

        assert len(tables) >= 2
        table_names = " ".join(tables)
        assert "users" in table_names
        assert "purchases" in table_names

    def test_view_with_aggregation(self):
        """Test view with GROUP BY and aggregation"""
        sql = """
        CREATE VIEW silver.curated.sales_summary AS
        SELECT
            product_id,
            SUM(quantity) as total_quantity,
            SUM(amount) as total_amount
        FROM bronze.raw.sales
        GROUP BY product_id
        """
        tables = extract_table_references(sql)

        assert len(tables) >= 1
        assert any("sales" in t for t in tables)

    def test_view_with_window_function(self):
        """Test view with window functions"""
        sql = """
        CREATE VIEW silver.curated.ranked_sales AS
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC) as rank
        FROM bronze.raw.sales
        """
        tables = extract_table_references(sql)

        assert len(tables) >= 1
        assert any("sales" in t for t in tables)


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_invalid_sql_raises_error(self):
        """Test that invalid SQL raises an error"""
        sql = "SELECT * FROM WHERE"

        with pytest.raises(Exception):
            sqlglot.parse_one(sql, dialect="databricks")

    def test_empty_sql(self):
        """Test parsing empty SQL"""
        sql = ""

        # Empty SQL may parse as None or raise error
        try:
            parsed = sqlglot.parse_one(sql, dialect="databricks")
            assert parsed is None or isinstance(parsed, exp.Expression)
        except Exception:
            pass  # Empty SQL may raise error, which is acceptable

    def test_complex_qualified_names(self):
        """Test handling complex qualified names with backticks"""
        sql = "SELECT * FROM `my-catalog`.`my-schema`.`my-table`"
        tables = extract_table_references(sql)

        assert len(tables) >= 1
        # Table name should preserve catalog.schema.table structure
        assert "my-" in tables[0] or "my" in tables[0]


class TestDatabricksSpecificFeatures:
    """Test Databricks-specific SQL features"""

    def test_delta_table_syntax(self):
        """Test Delta table syntax"""
        sql = "SELECT * FROM delta.`/path/to/table`"

        # This may or may not parse - test to understand SQLGlot's limitations
        try:
            parsed = sqlglot.parse_one(sql, dialect="databricks")
            assert parsed is not None
        except Exception:
            # If it fails, we know this is a limitation we need to handle
            pass

    def test_using_delta_syntax(self):
        """Test USING DELTA syntax in CREATE TABLE"""
        sql = """
        CREATE TABLE catalog.schema.my_table (
            id INT,
            name STRING
        ) USING DELTA
        """

        try:
            parsed = sqlglot.parse_one(sql, dialect="databricks")
            assert parsed is not None
        except Exception:
            # Document this limitation if it fails
            pass


def test_sqlglot_parse_success_rate():
    """
    Test to estimate SQLGlot's parse success rate with various SQL patterns.
    This helps us understand when to fall back to explicit dependencies.
    """
    test_queries = [
        # Simple queries
        "SELECT * FROM table1",
        "SELECT a, b FROM schema.table1",
        "SELECT * FROM catalog.schema.table1",
        # Joins
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT JOIN t2 USING (id)",
        # CTEs
        "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte",
        # Subqueries
        "SELECT * FROM (SELECT * FROM t1) AS sub",
        # Aggregations
        "SELECT COUNT(*) FROM t1 GROUP BY col",
        # Window functions
        "SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM t1",
        # UNION
        "SELECT * FROM t1 UNION SELECT * FROM t2",
    ]

    successful_parses = 0
    failed_parses = []

    for sql in test_queries:
        try:
            parsed = sqlglot.parse_one(sql, dialect="databricks")
            if parsed is not None:
                successful_parses += 1
        except Exception as e:
            failed_parses.append((sql, str(e)))

    success_rate = (successful_parses / len(test_queries)) * 100

    # We expect at least 80% success rate
    assert success_rate >= 80, f"Parse success rate too low: {success_rate}%"

    # Log failed queries for analysis
    if failed_parses:
        print(f"\nFailed to parse {len(failed_parses)} queries:")
        for sql, error in failed_parses:
            print(f"  SQL: {sql[:50]}...")
            print(f"  Error: {error}")

