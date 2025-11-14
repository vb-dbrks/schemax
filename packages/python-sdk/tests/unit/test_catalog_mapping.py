"""
Unit tests for catalog name mapping (logical → physical)
"""

import pytest

from schematic.commands.sql import SQLGenerationError, build_catalog_mapping
from schematic.providers.unity.sql_generator import UnitySQLGenerator


class TestCatalogMapping:
    """Test logical → physical catalog name mapping"""

    def test_build_catalog_mapping_implicit_catalog(self):
        """Should map __implicit__ to environment's physical catalog"""
        state = {"catalogs": [{"id": "cat_implicit", "name": "__implicit__", "schemas": []}]}
        env_config = {"topLevelName": "dev_my_analytics"}

        mapping = build_catalog_mapping(state, env_config)

        assert mapping == {"__implicit__": "dev_my_analytics"}

    def test_build_catalog_mapping_single_explicit_catalog(self):
        """Should map single explicit catalog to environment catalog"""
        state = {"catalogs": [{"id": "cat_123", "name": "sales_analytics", "schemas": []}]}
        env_config = {"topLevelName": "dev_sales"}

        mapping = build_catalog_mapping(state, env_config)

        assert mapping == {"sales_analytics": "dev_sales"}

    def test_build_catalog_mapping_no_catalogs(self):
        """Should return empty mapping if no catalogs"""
        state = {"catalogs": []}
        env_config = {"topLevelName": "dev_my_analytics"}

        mapping = build_catalog_mapping(state, env_config)

        assert mapping == {}

    def test_build_catalog_mapping_multi_catalog_error(self):
        """Should raise error for multi-catalog projects (not yet supported)"""
        state = {
            "catalogs": [
                {"id": "cat_1", "name": "sales", "schemas": []},
                {"id": "cat_2", "name": "analytics", "schemas": []},
            ]
        }
        env_config = {"topLevelName": "dev_primary"}

        with pytest.raises(
            SQLGenerationError, match="Multi-catalog projects are not yet supported"
        ):
            build_catalog_mapping(state, env_config)


class TestSQLGeneratorWithMapping:
    """Test SQL generator with catalog name mapping"""

    def test_sql_generator_applies_catalog_mapping(self):
        """Should use physical catalog name in generated SQL"""
        state = {
            "catalogs": [
                {
                    "id": "cat_implicit",
                    "name": "__implicit__",
                    "schemas": [{"id": "schema_1", "name": "customer_360", "tables": []}],
                }
            ]
        }

        # Map __implicit__ → dev_analytics
        catalog_mapping = {"__implicit__": "dev_analytics"}

        generator = UnitySQLGenerator(state, catalog_mapping)

        # Generate SQL for schema
        from schematic.providers.base.operations import Operation

        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_schema",
            target="schema_1",
            payload={"schemaId": "schema_1", "name": "customer_360", "catalogId": "cat_implicit"},
        )

        result = generator.generate_sql_for_operation(op)

        # Should use dev_analytics, not __implicit__
        assert "dev_analytics" in result.sql
        assert "__implicit__" not in result.sql
        assert "customer_360" in result.sql

    def test_sql_generator_without_mapping(self):
        """Should use logical names when no mapping provided"""
        state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "sales_analytics",
                    "schemas": [{"id": "schema_1", "name": "customer_360", "tables": []}],
                }
            ]
        }

        # No mapping
        generator = UnitySQLGenerator(state, {})

        from schematic.providers.base.operations import Operation

        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_schema",
            target="schema_1",
            payload={"schemaId": "schema_1", "name": "customer_360", "catalogId": "cat_1"},
        )

        result = generator.generate_sql_for_operation(op)

        # Should use logical name
        assert "sales_analytics" in result.sql
        assert "customer_360" in result.sql

    def test_sql_generator_table_with_mapping(self):
        """Should apply mapping to fully-qualified table names"""
        state = {
            "catalogs": [
                {
                    "id": "cat_implicit",
                    "name": "__implicit__",
                    "schemas": [
                        {
                            "id": "schema_1",
                            "name": "customer_360",
                            "tables": [
                                {
                                    "id": "table_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "INT",
                                            "nullable": False,
                                        }
                                    ],
                                    "properties": {},
                                    "constraints": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        catalog_mapping = {"__implicit__": "prod_analytics"}

        generator = UnitySQLGenerator(state, catalog_mapping)

        from schematic.providers.base.operations import Operation

        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_table",
            target="table_1",
            payload={
                "tableId": "table_1",
                "name": "customers",
                "schemaId": "schema_1",
                "format": "delta",
            },
        )

        result = generator.generate_sql_for_operation(op)

        # Should use prod_analytics in FQN
        assert "prod_analytics" in result.sql
        assert "customer_360" in result.sql
        assert "customers" in result.sql
        assert "__implicit__" not in result.sql


class TestEnvironmentSpecificSQL:
    """Test generating different SQL for different environments"""

    def test_same_state_different_environments(self):
        """Should generate different SQL for dev vs prod using same state"""
        state = {
            "catalogs": [
                {
                    "id": "cat_implicit",
                    "name": "__implicit__",
                    "schemas": [{"id": "schema_1", "name": "sales", "tables": []}],
                }
            ]
        }

        from schematic.providers.base.operations import Operation

        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_schema",
            target="schema_1",
            payload={"schemaId": "schema_1", "name": "sales", "catalogId": "cat_implicit"},
        )

        # Dev environment
        dev_mapping = {"__implicit__": "dev_my_project"}
        dev_generator = UnitySQLGenerator(state, dev_mapping)
        dev_sql = dev_generator.generate_sql_for_operation(op).sql

        # Prod environment
        prod_mapping = {"__implicit__": "prod_my_project"}
        prod_generator = UnitySQLGenerator(state, prod_mapping)
        prod_sql = prod_generator.generate_sql_for_operation(op).sql

        # Different catalogs, same schema name
        assert "dev_my_project" in dev_sql
        assert "prod_my_project" in prod_sql
        assert "sales" in dev_sql
        assert "sales" in prod_sql
        assert dev_sql != prod_sql

    def test_view_catalog_name_mapping(self):
        """Test that views get their catalog names mapped to physical names"""
        state = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "sales_analytics",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "product_360",
                            "tables": [
                                {"id": "table1", "name": "products", "columns": []},
                            ],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        from schematic.providers.base.operations import Operation

        # Create a view
        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_view",
            target="view1",
            payload={
                "viewId": "view1",
                "name": "view1",
                "schemaId": "schema1",
                "definition": "SELECT * FROM products",
                "extractedDependencies": {"tables": ["products"], "views": []},
            },
        )

        # Dev environment - should map sales_analytics → dev_sales_analytics
        dev_mapping = {"sales_analytics": "dev_sales_analytics"}
        dev_generator = UnitySQLGenerator(state, dev_mapping)
        dev_sql = dev_generator.generate_sql_for_operation(op).sql

        # Prod environment - should map sales_analytics → prod_sales_analytics
        prod_mapping = {"sales_analytics": "prod_sales_analytics"}
        prod_generator = UnitySQLGenerator(state, prod_mapping)
        prod_sql = prod_generator.generate_sql_for_operation(op).sql

        # Verify physical catalog names are used in CREATE VIEW statements
        assert "CREATE VIEW IF NOT EXISTS `dev_sales_analytics`.`product_360`.`view1`" in dev_sql
        assert "CREATE VIEW IF NOT EXISTS `prod_sales_analytics`.`product_360`.`view1`" in prod_sql

        # Verify table references inside view SQL are also qualified correctly
        assert "`dev_sales_analytics`.`product_360`.`products`" in dev_sql
        assert "`prod_sales_analytics`.`product_360`.`products`" in prod_sql

        # Verify logical names are NOT present (CRITICAL: catches if mapping is broken)
        assert "sales_analytics" not in dev_sql.replace("dev_sales_analytics", "").replace(
            "prod_sales_analytics", ""
        )

        # Verify they differ
        assert dev_sql != prod_sql

    def test_multi_level_view_dependencies_with_catalog_mapping(self):
        """Test view dependencies across multiple levels with catalog name mapping"""
        state = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "reporting",
                            "tables": [
                                {"id": "tbl1", "name": "base_table", "columns": []},
                            ],
                            "views": [],
                        }
                    ],
                }
            ]
        }

        from schematic.providers.base.operations import Operation

        # Create 3 views with dependencies: view3 → view2 → view1 → base_table
        ops = [
            Operation(
                id="op_1",
                ts="2025-01-01T00:00:01Z",
                provider="unity",
                op="unity.add_view",
                target="view1",
                payload={
                    "viewId": "view1",
                    "name": "view1",
                    "schemaId": "schema1",
                    "definition": "SELECT * FROM base_table WHERE active = true",
                    "extractedDependencies": {"tables": ["base_table"], "views": []},
                },
            ),
            Operation(
                id="op_2",
                ts="2025-01-01T00:00:02Z",
                provider="unity",
                op="unity.add_view",
                target="view2",
                payload={
                    "viewId": "view2",
                    "name": "view2",
                    "schemaId": "schema1",
                    "definition": "SELECT id, name FROM view1 WHERE created_at > CURRENT_DATE - INTERVAL 30 DAYS",
                    "extractedDependencies": {"tables": [], "views": ["view1"]},
                },
            ),
            Operation(
                id="op_3",
                ts="2025-01-01T00:00:03Z",
                provider="unity",
                op="unity.add_view",
                target="view3",
                payload={
                    "viewId": "view3",
                    "name": "view3",
                    "schemaId": "schema1",
                    "definition": "SELECT name, COUNT(*) as count FROM view2 GROUP BY name",
                    "extractedDependencies": {"tables": [], "views": ["view2"]},
                },
            ),
        ]

        # Generate SQL for staging environment
        staging_mapping = {"analytics": "staging_analytics"}
        staging_generator = UnitySQLGenerator(state, staging_mapping)
        staging_sql = staging_generator.generate_sql(ops)

        # Verify all views use staging catalog
        assert "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view1`" in staging_sql
        assert "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view2`" in staging_sql
        assert "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view3`" in staging_sql

        # Verify table reference in view1 is qualified
        assert "`staging_analytics`.`reporting`.`base_table`" in staging_sql

        # Verify view references are qualified
        assert "`staging_analytics`.`reporting`.`view1`" in staging_sql
        assert "`staging_analytics`.`reporting`.`view2`" in staging_sql

        # CRITICAL: Verify logical catalog name is NOT present anywhere
        assert "analytics" not in staging_sql.replace("staging_analytics", ""), (
            "Logical catalog name found! Catalog mapping is broken!"
        )

        # Verify views are in correct dependency order (view1 before view2 before view3)
        view1_idx = staging_sql.index(
            "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view1`"
        )
        view2_idx = staging_sql.index(
            "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view2`"
        )
        view3_idx = staging_sql.index(
            "CREATE VIEW IF NOT EXISTS `staging_analytics`.`reporting`.`view3`"
        )
        assert view1_idx < view2_idx < view3_idx, "Views not in correct dependency order!"

    def test_complex_view_with_catalog_mapping(self):
        """Test complex view (CTEs, JOINs, subqueries) with catalog name mapping"""
        state = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "sales_db",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "sales",
                            "tables": [
                                {"id": "tbl1", "name": "customers", "columns": []},
                                {"id": "tbl2", "name": "orders", "columns": []},
                                {"id": "tbl3", "name": "products", "columns": []},
                                {"id": "tbl4", "name": "order_items", "columns": []},
                            ],
                            "views": [
                                {
                                    "id": "view1",
                                    "name": "customer_summary",
                                    "definition": "SELECT * FROM customers",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        from schematic.providers.base.operations import Operation

        # Complex view with CTEs, JOINs, subqueries, window functions, UNION
        complex_sql = """
        WITH monthly_sales AS (
            SELECT
                c.customer_id,
                c.name,
                SUM(oi.quantity * oi.price) as total_sales,
                COUNT(DISTINCT o.order_id) as order_count,
                ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date DESC) as rn
            FROM customers c
            INNER JOIN orders o ON c.customer_id = o.customer_id
            INNER JOIN order_items oi ON o.order_id = oi.order_id
            INNER JOIN products p ON oi.product_id = p.product_id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY c.customer_id, c.name, o.order_date
        ),
        top_customers AS (
            SELECT * FROM monthly_sales WHERE rn = 1
        )
        SELECT
            tc.customer_id,
            tc.name,
            tc.total_sales,
            tc.order_count,
            cs.lifetime_value
        FROM top_customers tc
        LEFT JOIN customer_summary cs ON tc.customer_id = cs.customer_id

        UNION ALL

        SELECT
            c.customer_id,
            c.name,
            0 as total_sales,
            0 as order_count,
            0 as lifetime_value
        FROM customers c
        WHERE c.customer_id NOT IN (SELECT customer_id FROM orders)
        """

        op = Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_view",
            target="view2",
            payload={
                "viewId": "view2",
                "name": "customer_360_view",
                "schemaId": "schema1",
                "definition": complex_sql,
                "extractedDependencies": {
                    "tables": ["customers", "orders", "order_items", "products"],
                    "views": ["customer_summary"],
                },
            },
        )

        # Test with production environment
        prod_mapping = {"sales_db": "prod_sales_db"}
        prod_generator = UnitySQLGenerator(state, prod_mapping)
        prod_sql = prod_generator.generate_sql_for_operation(op).sql

        # Verify view name uses physical catalog
        assert "CREATE VIEW IF NOT EXISTS `prod_sales_db`.`sales`.`customer_360_view`" in prod_sql

        # Verify all table references are qualified with physical catalog
        assert "`prod_sales_db`.`sales`.`customers`" in prod_sql
        assert "`prod_sales_db`.`sales`.`orders`" in prod_sql
        assert "`prod_sales_db`.`sales`.`order_items`" in prod_sql
        assert "`prod_sales_db`.`sales`.`products`" in prod_sql

        # Verify view reference is qualified
        assert "`prod_sales_db`.`sales`.`customer_summary`" in prod_sql

        # CRITICAL: Verify logical catalog name is NOT present
        # This catches if catalog mapping is broken for ANY reference
        clean_sql = prod_sql.replace("prod_sales_db", "")
        assert "sales_db" not in clean_sql, (
            "Logical catalog 'sales_db' found! Catalog mapping failed!"
        )

        # Verify complex SQL features are preserved
        assert "WITH monthly_sales AS" in prod_sql  # CTE
        assert "INNER JOIN" in prod_sql  # JOINs
        assert "LEFT JOIN" in prod_sql  # More JOINs
        assert "ROW_NUMBER() OVER" in prod_sql  # Window function
        assert "UNION ALL" in prod_sql  # UNION
        assert " IN (" in prod_sql  # Subquery (SQLGlot reformats NOT IN to NOT ... IN)
        assert "COUNT(DISTINCT" in prod_sql  # Aggregation

    def test_view_operations_all_use_catalog_mapping(self):
        """Test that ALL view operations (not just CREATE) use catalog mapping"""
        state = {
            "catalogs": [
                {
                    "id": "cat1",
                    "name": "data_warehouse",
                    "schemas": [
                        {
                            "id": "schema1",
                            "name": "analytics",
                            "tables": [{"id": "tbl1", "name": "events", "columns": []}],
                            "views": [
                                {
                                    "id": "view1",
                                    "name": "event_summary",
                                    "definition": "SELECT * FROM events",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        from schematic.providers.base.operations import Operation

        # Test different view operations
        test_cases = [
            # UPDATE VIEW
            (
                "unity.update_view",
                Operation(
                    id="op_1",
                    ts="2025-01-01T00:00:00Z",
                    provider="unity",
                    op="unity.update_view",
                    target="view1",
                    payload={
                        "definition": "SELECT * FROM events WHERE timestamp > CURRENT_DATE",
                        "extractedDependencies": {"tables": ["events"], "views": []},
                    },
                ),
                "CREATE OR REPLACE VIEW `uat_data_warehouse`.`analytics`.`event_summary`",
            ),
            # RENAME VIEW
            (
                "unity.rename_view",
                Operation(
                    id="op_2",
                    ts="2025-01-01T00:00:00Z",
                    provider="unity",
                    op="unity.rename_view",
                    target="view1",
                    payload={"newName": "event_summary_v2"},
                ),
                "ALTER VIEW `uat_data_warehouse`.`analytics`.`event_summary` RENAME TO `uat_data_warehouse`.`analytics`.`event_summary_v2`",
            ),
            # DROP VIEW
            (
                "unity.drop_view",
                Operation(
                    id="op_3",
                    ts="2025-01-01T00:00:00Z",
                    provider="unity",
                    op="unity.drop_view",
                    target="view1",
                    payload={},
                ),
                "DROP VIEW IF EXISTS `uat_data_warehouse`.`analytics`.`event_summary`",
            ),
            # SET VIEW COMMENT
            (
                "unity.set_view_comment",
                Operation(
                    id="op_4",
                    ts="2025-01-01T00:00:00Z",
                    provider="unity",
                    op="unity.set_view_comment",
                    target="view1",
                    payload={"viewId": "view1", "comment": "Event summary view"},
                ),
                "COMMENT ON VIEW `uat_data_warehouse`.`analytics`.`event_summary`",
            ),
        ]

        uat_mapping = {"data_warehouse": "uat_data_warehouse"}
        uat_generator = UnitySQLGenerator(state, uat_mapping)

        for op_name, operation, expected_sql_fragment in test_cases:
            result = uat_generator.generate_sql_for_operation(operation)
            sql = result.sql

            # Verify physical catalog is used
            assert expected_sql_fragment in sql, f"{op_name}: Expected SQL fragment not found"

            # CRITICAL: Verify logical catalog is NOT present
            clean_sql = sql.replace("uat_data_warehouse", "")
            assert "data_warehouse" not in clean_sql, (
                f"{op_name}: Logical catalog 'data_warehouse' found! Catalog mapping broken!"
            )
