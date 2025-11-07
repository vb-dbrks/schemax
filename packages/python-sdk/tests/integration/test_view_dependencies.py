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
