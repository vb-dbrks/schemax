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
