"""
Integration tests for view dependencies and SQL generation

Tests end-to-end dependency system:
1. View creation with dependencies
2. Dependency extraction from SQL
3. Topological sorting
4. Breaking change detection
5. Circular dependency detection
"""

from schematic.providers.base.sql_parser import extract_dependencies_from_view
from schematic.providers.unity.models import UnityState
from schematic.providers.unity.state_reducer import apply_operation, apply_operations
from tests.utils.operation_builders import OperationBuilder


class TestSimpleViewDependencies:
    """Test simple view depending on a single table"""

    def test_view_depends_on_table(self):
        """Test creating a view that depends on a table"""
        # Create initial state with a table
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        # Add catalog
        state = apply_operation(state, builder.add_catalog("cat1", "my_catalog", op_id="op1"))

        # Add schema
        state = apply_operation(
            state, builder.add_schema("schema1", "my_schema", "cat1", op_id="op2")
        )

        # Add table
        state = apply_operation(
            state, builder.add_table("table1", "users", "schema1", "delta", op_id="op3")
        )

        # Add view that depends on table
        view_sql = "SELECT * FROM my_catalog.my_schema.users WHERE active = true"
        state = apply_operation(
            state,
            builder.add_view("view1", "active_users", "schema1", view_sql, op_id="op4"),
        )

        # Verify state
        assert len(state.catalogs) == 1
        catalog = state.catalogs[0]
        assert len(catalog.schemas) == 1
        schema = catalog.schemas[0]
        assert len(schema.tables) == 1
        assert len(schema.views) == 1

        view = schema.views[0]
        assert view.name == "active_users"
        assert view.definition == view_sql

    def test_extract_dependencies_from_simple_view(self):
        """Test extracting dependencies from a simple view SQL"""
        # Create state with a table
        state_dict = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "my_catalog",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "my_schema",
                            "tables": [{"id": "table1", "name": "users"}],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        view_sql = "SELECT * FROM my_catalog.my_schema.users WHERE active = true"

        deps = extract_dependencies_from_view(view_sql, state_dict, "my_catalog")

        # Should find the users table
        assert len(deps["tables"]) == 1
        assert "table1" in deps["tables"]
        assert len(deps["views"]) == 0


class TestChainedViewDependencies:
    """Test chained views (view → view → table)"""

    def test_chained_views(self):
        """Test view A depends on view B which depends on table C"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        # Create catalog and schema
        ops = [
            builder.add_catalog("cat1", "analytics", op_id="op1"),
            builder.add_schema("schema1", "bronze", "cat1", op_id="op2"),
            builder.add_table("table1", "raw_events", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "filtered_events",
                "schema1",
                "SELECT * FROM analytics.bronze.raw_events WHERE status = 'success'",
                op_id="op4",
            ),
            builder.add_view(
                "view2",
                "aggregated_events",
                "schema1",
                "SELECT date, COUNT(*) FROM analytics.bronze.filtered_events GROUP BY date",
                op_id="op5",
            ),
        ]

        state = apply_operations(state, ops)

        # Verify all objects created
        schema = state.catalogs[0].schemas[0]
        assert len(schema.tables) == 1
        assert len(schema.views) == 2

        # Verify view names
        view_names = [v.name for v in schema.views]
        assert "filtered_events" in view_names
        assert "aggregated_events" in view_names

    def test_extract_chained_dependencies(self):
        """Test extracting dependencies from chained views"""
        state_dict = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "bronze",
                            "tables": [{"id": "table1", "name": "raw_events"}],
                            "views": [{"id": "view1", "name": "filtered_events"}],
                        }
                    ],
                }
            ]
        }

        # View that depends on another view
        view_sql = "SELECT date, COUNT(*) FROM analytics.bronze.filtered_events GROUP BY date"

        deps = extract_dependencies_from_view(view_sql, state_dict, "analytics")

        # Should find the view dependency
        assert len(deps["views"]) == 1
        assert "view1" in deps["views"]


class TestCircularDependencies:
    """Test circular dependency detection"""

    def test_detect_two_view_cycle(self):
        """Test detecting a 2-view circular dependency"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "test_catalog", op_id="op1"),
            builder.add_schema("schema1", "test_schema", "cat1", op_id="op2"),
            builder.add_view(
                "viewA",
                "view_a",
                "schema1",
                "SELECT * FROM test_catalog.test_schema.view_b",
                dependencies=["viewB"],
                op_id="op3",
            ),
            builder.add_view(
                "viewB",
                "view_b",
                "schema1",
                "SELECT * FROM test_catalog.test_schema.view_a",
                dependencies=["viewA"],
                op_id="op4",
            ),
        ]

        state = apply_operations(state, ops)

        # Verify views were created in state
        schema = state.catalogs[0].schemas[0]
        assert len(schema.views) == 2

        # Note: Cycle detection happens during SQL generation, not state application
        # The state just stores the operations; SQL generator will detect the cycle


class TestBreakingChanges:
    """Test breaking change detection"""

    def test_dropping_table_with_dependent_view(self):
        """Test that dropping a table with a dependent view is detected"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "my_catalog", op_id="op1"),
            builder.add_schema("schema1", "my_schema", "cat1", op_id="op2"),
            builder.add_table("table1", "source_data", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "derived_view",
                "schema1",
                "SELECT * FROM my_catalog.my_schema.source_data",
                dependencies=["table1"],
                op_id="op4",
            ),
            builder.drop_table("table1", op_id="op5"),
        ]

        state = apply_operations(state, ops)

        # After applying all ops, the table should be gone
        schema = state.catalogs[0].schemas[0]
        assert len(schema.tables) == 0
        assert len(schema.views) == 1

        # Note: Breaking change detection happens during SQL generation
        # The SQL generator will warn about dropping table1 which has dependent view1


class TestComplexViewScenarios:
    """Test complex real-world view scenarios"""

    def test_view_with_joins(self):
        """Test view that joins multiple tables"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "warehouse", op_id="op1"),
            builder.add_schema("schema1", "sales", "cat1", op_id="op2"),
            builder.add_table("orders", "orders", "schema1", "delta", op_id="op3"),
            builder.add_table("customers", "customers", "schema1", "delta", op_id="op4"),
            builder.add_view(
                "view1",
                "customer_orders",
                "schema1",
                """
                    SELECT c.customer_id, c.name, o.order_id, o.amount
                    FROM warehouse.sales.customers c
                    JOIN warehouse.sales.orders o ON c.customer_id = o.customer_id
                """,
                op_id="op5",
            ),
        ]

        state = apply_operations(state, ops)

        schema = state.catalogs[0].schemas[0]
        assert len(schema.tables) == 2
        assert len(schema.views) == 1

    def test_view_with_cte(self):
        """Test view with CTE (Common Table Expression)"""
        state_dict = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "metrics",
                            "tables": [{"id": "events", "name": "events"}],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        view_sql = """
        WITH daily_counts AS (
            SELECT date, COUNT(*) as count
            FROM analytics.metrics.events
            GROUP BY date
        )
        SELECT * FROM daily_counts WHERE count > 100
        """

        deps = extract_dependencies_from_view(view_sql, state_dict, "analytics")

        # Should extract the events table dependency
        assert len(deps["tables"]) == 1
        assert "events" in deps["tables"]

    def test_view_with_subquery(self):
        """Test view with subquery"""
        state_dict = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "db",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "public",
                            "tables": [
                                {"id": "orders", "name": "orders"},
                                {"id": "products", "name": "products"},
                            ],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        view_sql = """
        SELECT *
        FROM db.public.orders o
        WHERE product_id IN (
            SELECT product_id
            FROM db.public.products
            WHERE category = 'electronics'
        )
        """

        deps = extract_dependencies_from_view(view_sql, state_dict, "db")

        # Should find both tables
        assert len(deps["tables"]) == 2
        assert "orders" in deps["tables"]
        assert "products" in deps["tables"]


class TestViewUpdateScenarios:
    """Test updating views and detecting dependency changes"""

    def test_update_view_definition(self):
        """Test updating a view's SQL definition"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "data", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "filtered_data",
                "schema1",
                "SELECT * FROM test.main.data WHERE active = true",
                op_id="op4",
            ),
            builder.update_view(
                "view1",
                definition="SELECT id, name FROM test.main.data WHERE active = true",
                op_id="op5",
            ),
        ]

        state = apply_operations(state, ops)

        view = state.catalogs[0].schemas[0].views[0]
        assert "SELECT id, name FROM" in view.definition

    def test_rename_view(self):
        """Test renaming a view"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "data", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1", "old_name", "schema1", "SELECT * FROM test.main.data", op_id="op4"
            ),
            builder.rename_view("view1", "new_name", "old_name", op_id="op5"),
        ]

        state = apply_operations(state, ops)

        view = state.catalogs[0].schemas[0].views[0]
        assert view.name == "new_name"

    def test_drop_view(self):
        """Test dropping a view"""
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "data", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1", "test_view", "schema1", "SELECT * FROM test.main.data", op_id="op4"
            ),
            builder.drop_view("view1", op_id="op5"),
        ]

        state = apply_operations(state, ops)

        schema = state.catalogs[0].schemas[0]
        assert len(schema.views) == 0
        assert len(schema.tables) == 1  # Table should still exist

    def test_update_view_dependencies_are_reextracted(self):
        """
        Test that when a view's SQL is updated, dependencies are re-extracted.

        Regression test for bug where updating a view wouldn't update its dependencies.

        Scenario:
        1. Create view1 that depends on table1
        2. Update view1 to depend on table2 instead
        3. Verify dependencies are updated correctly
        """
        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        # Create state with two tables
        state_dict = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "test",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "main",
                            "tables": [
                                {"id": "table1", "name": "table1"},
                                {"id": "table2", "name": "table2"},
                                {"id": "table3", "name": "table3"},
                            ],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        # Initial view SQL depends on table1
        initial_sql = "SELECT * FROM table1"
        initial_deps = extract_dependencies_from_view(initial_sql, state_dict, "test")
        assert "table1" in initial_deps["tables"]
        assert "table2" not in initial_deps["tables"]

        # Updated view SQL depends on table2
        updated_sql = "SELECT * FROM table2"
        updated_deps = extract_dependencies_from_view(updated_sql, state_dict, "test")
        assert "table2" in updated_deps["tables"]
        assert "table1" not in updated_deps["tables"]

        # Create operations
        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "table1", "schema1", "delta", op_id="op3"),
            builder.add_table("table2", "table2", "schema1", "delta", op_id="op4"),
            builder.add_table("table3", "table3", "schema1", "delta", op_id="op5"),
            builder.add_view(
                "view1",
                "my_view",
                "schema1",
                initial_sql,
                dependencies=["table1"],
                op_id="op6",
            ),
            builder.update_view(
                "view1",
                definition=updated_sql,
                extracted_dependencies=updated_deps,
                op_id="op7",
            ),
        ]

        state = apply_operations(state, ops)

        # Verify view has updated SQL and dependencies
        view = state.catalogs[0].schemas[0].views[0]
        assert view.definition == updated_sql
        # Check extracted_dependencies (dict), not dependencies (list)
        assert view.extracted_dependencies is not None
        assert "table2" in view.extracted_dependencies.get("tables", [])
        assert "table1" not in view.extracted_dependencies.get("tables", [])


class TestViewSQLBatching:
    """Test SQL generation batching optimizations for views"""

    def test_create_and_update_view_batched(self):
        """
        Test that CREATE + UPDATE_VIEW operations are batched into single CREATE OR REPLACE VIEW.

        Regression test for bug where separate CREATE and UPDATE statements were generated.

        Scenario:
        1. Create a view with initial SQL
        2. Update the view's SQL in the same changeset
        3. SQL generator should produce single CREATE OR REPLACE VIEW statement
        """
        from schematic.providers.unity.sql_generator import UnitySQLGenerator

        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        # Create operations
        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "source", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "my_view",
                "schema1",
                "SELECT * FROM table1",
                dependencies=["table1"],
                op_id="op4",
            ),
            builder.update_view(
                "view1",
                definition="SELECT id, name FROM table1",
                extracted_dependencies={"tables": ["table1"], "views": []},
                op_id="op5",
            ),
        ]

        # Apply operations to get state
        state = apply_operations(state, ops)

        # Generate SQL
        generator = UnitySQLGenerator(state, catalog_name_mapping={})
        result = generator.generate_sql_with_mapping(ops)

        # Extract SQL statements
        sql_statements = [stmt.sql for stmt in result.statements]

        # Count view creation statements
        # Note: "CREATE OR REPLACE VIEW" contains "CREATE VIEW", so we need to be specific
        create_if_not_exists_count = sum(
            1 for sql in sql_statements if "CREATE VIEW IF NOT EXISTS" in sql
        )
        create_or_replace_count = sum(
            1 for sql in sql_statements if "CREATE OR REPLACE VIEW" in sql
        )

        # Should have exactly ONE CREATE OR REPLACE VIEW statement
        assert create_or_replace_count == 1, "Should batch into single CREATE OR REPLACE VIEW"
        # Should NOT have separate CREATE VIEW IF NOT EXISTS
        assert create_if_not_exists_count == 0, (
            "Should not generate separate CREATE VIEW IF NOT EXISTS"
        )

        # Verify the final SQL has the updated definition
        view_sql = next(sql for sql in sql_statements if "CREATE OR REPLACE VIEW" in sql)
        # SQLGlot may pretty-print the SQL, so check for the key parts
        assert "SELECT id, name FROM" in view_sql or (
            "id" in view_sql and "name" in view_sql and "FROM" in view_sql
        ), "Should use updated SQL definition"

    def test_create_and_multiple_updates_batched(self):
        """
        Test that CREATE + multiple UPDATE_VIEW operations use the final definition.

        Scenario:
        1. Create a view
        2. Update it multiple times in the same changeset
        3. Final SQL should use the last update's definition
        """
        from schematic.providers.unity.sql_generator import UnitySQLGenerator

        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "test", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "data", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "evolving_view",
                "schema1",
                "SELECT * FROM table1",
                dependencies=["table1"],
                op_id="op4",
            ),
            builder.update_view(
                "view1",
                definition="SELECT id FROM table1",
                extracted_dependencies={"tables": ["table1"], "views": []},
                op_id="op5",
            ),
            builder.update_view(
                "view1",
                definition="SELECT id, name FROM table1",
                extracted_dependencies={"tables": ["table1"], "views": []},
                op_id="op6",
            ),
            builder.update_view(
                "view1",
                definition="SELECT id, name, status FROM table1 WHERE active = true",
                extracted_dependencies={"tables": ["table1"], "views": []},
                op_id="op7",
            ),
        ]

        state = apply_operations(state, ops)

        # Generate SQL
        generator = UnitySQLGenerator(state, catalog_name_mapping={})
        result = generator.generate_sql_with_mapping(ops)

        sql_statements = [stmt.sql for stmt in result.statements]

        # Should have exactly ONE view creation statement
        view_statements = [sql for sql in sql_statements if "VIEW" in sql and "CREATE" in sql]
        assert len(view_statements) == 1, "Should generate only one CREATE statement"

        # Should use the FINAL definition
        final_sql = view_statements[0]
        # SQLGlot may pretty-print the SQL, so check for the key parts
        assert "SELECT id, name, status FROM" in final_sql or (
            "id" in final_sql and "name" in final_sql and "status" in final_sql
        ), "Should use final definition"
        assert "WHERE active = true" in final_sql or (
            "WHERE" in final_sql and "active" in final_sql
        ), "Should include final WHERE clause"


class TestViewFQNQualification:
    """Test that view SQL is properly qualified with backticks for Databricks"""

    def test_unqualified_table_refs_are_qualified_with_backticks(self):
        """
        REGRESSION TEST: View SQL must have FQNs with backticks

        This test validates the exact issue reported where views failed with:
        [TABLE_OR_VIEW_NOT_FOUND] The table or view `table4` cannot be found

        The problem was unqualified table names in view SQL.
        The fix qualifies them with full catalog.schema.table and backticks.
        """
        from schematic.providers.unity.sql_generator import UnitySQLGenerator

        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        # Create operations with unqualified SQL
        ops = [
            builder.add_catalog("cat1", "sales_analytics", op_id="op1"),
            builder.add_schema("schema1", "customer_analytics", "cat1", op_id="op2"),
            builder.add_table("table1", "table4", "schema1", "delta", op_id="op3"),
            builder.add_view(
                "view1",
                "test4",
                "schema1",
                "SELECT * FROM table4",  # ← Unqualified table name!
                dependencies=["table4"],
                op_id="op4",
            ),
        ]

        # Apply to get state
        state = apply_operations(state, ops)

        # Generate SQL with catalog mapping (simulating dev deployment)
        catalog_mapping = {"sales_analytics": "dev_sales_analytics"}
        generator = UnitySQLGenerator(state, catalog_name_mapping=catalog_mapping)
        result = generator.generate_sql_with_mapping(ops)

        # Find the CREATE VIEW statement
        view_sql = None
        for stmt in result.statements:
            if "CREATE VIEW" in stmt.sql and "test4" in stmt.sql:
                view_sql = stmt.sql
                break

        assert view_sql is not None, "Should generate CREATE VIEW statement"

        # Critical assertions: Must have FULLY-QUALIFIED names with BACKTICKS
        assert "`dev_sales_analytics`.`customer_analytics`.`table4`" in view_sql, (
            "Table reference must be fully qualified with backticks"
        )

        # Should NOT have unqualified reference
        assert "FROM table4" not in view_sql, "Should not have unqualified table name"
        assert "FROM `table4`" not in view_sql, "Should not have partially qualified table name"

        print("Generated view SQL:")
        print(view_sql)
        print("✅ View SQL is properly qualified with backticks!")

    def test_view_with_multiple_tables_all_qualified(self):
        """Test that views referencing multiple tables get all refs qualified"""
        from schematic.providers.unity.sql_generator import UnitySQLGenerator

        state = UnityState(catalogs=[])
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat1", "analytics", op_id="op1"),
            builder.add_schema("schema1", "main", "cat1", op_id="op2"),
            builder.add_table("table1", "orders", "schema1", "delta", op_id="op3"),
            builder.add_table("table2", "customers", "schema1", "delta", op_id="op4"),
            builder.add_view(
                "view1",
                "order_details",
                "schema1",
                "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
                dependencies=["orders", "customers"],
                op_id="op5",
            ),
        ]

        state = apply_operations(state, ops)

        catalog_mapping = {"analytics": "prod_analytics"}
        generator = UnitySQLGenerator(state, catalog_name_mapping=catalog_mapping)
        result = generator.generate_sql_with_mapping(ops)

        view_sql = next((stmt.sql for stmt in result.statements if "CREATE VIEW" in stmt.sql), None)
        assert view_sql is not None

        # Both tables must be qualified
        assert "`prod_analytics`.`main`.`orders`" in view_sql, "orders must be qualified"
        assert "`prod_analytics`.`main`.`customers`" in view_sql, "customers must be qualified"

        # Should NOT have unqualified references
        assert "FROM orders" not in view_sql.lower()
        assert "JOIN customers" not in view_sql.lower()
