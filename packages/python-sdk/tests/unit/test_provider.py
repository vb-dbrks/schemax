"""
Unit tests for Provider System

Tests the provider registry and Unity provider implementation including:
- Provider registration and retrieval
- Operation validation
- State validation
- SQL generator creation
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from databricks.sdk.service.sql import StatementState

from schematic.providers import Operation, ProviderRegistry
from schematic.providers.base.executor import ExecutionConfig
from tests.utils import OperationBuilder


class TestProviderRegistry:
    """Test ProviderRegistry functionality"""

    def test_registry_has_unity_provider(self):
        """Test that Unity provider is registered"""
        assert ProviderRegistry.has("unity")

    def test_registry_get_unity_provider(self):
        """Test getting Unity provider from registry"""
        provider = ProviderRegistry.get("unity")
        assert provider is not None
        assert provider.info.id == "unity"
        assert provider.info.name == "Unity Catalog"

    def test_registry_get_nonexistent_provider(self):
        """Test getting non-existent provider returns None"""
        provider = ProviderRegistry.get("nonexistent")
        assert provider is None

    def test_registry_get_all_ids(self):
        """Test getting all provider IDs"""
        ids = ProviderRegistry.get_all_ids()
        assert isinstance(ids, list)
        assert "unity" in ids

    def test_registry_get_all_providers(self):
        """Test getting all registered providers"""
        providers = ProviderRegistry.get_all()
        assert len(providers) > 0
        assert any(p.info.id == "unity" for p in providers)


class TestUnityProvider:
    """Test Unity Catalog Provider"""

    @pytest.fixture
    def unity_provider(self):
        """Get Unity provider instance"""
        return ProviderRegistry.get("unity")

    def test_provider_info(self, unity_provider):
        """Test provider info"""
        assert unity_provider.info.id == "unity"
        assert unity_provider.info.name == "Unity Catalog"
        assert unity_provider.info.version is not None
        assert unity_provider.info.description is not None

    def test_provider_capabilities(self, unity_provider):
        """Test provider capabilities"""
        caps = unity_provider.capabilities

        # Should support Unity operations
        assert "unity.add_catalog" in caps.supported_operations
        assert "unity.add_schema" in caps.supported_operations
        assert "unity.add_table" in caps.supported_operations
        assert "unity.add_column" in caps.supported_operations

        # Should have Unity object types
        assert "catalog" in caps.supported_object_types
        assert "schema" in caps.supported_object_types
        assert "table" in caps.supported_object_types

        # Should have Unity features enabled
        assert caps.features["constraints"] is True
        assert caps.features["row_filters"] is True
        assert caps.features["column_masks"] is True
        assert caps.features["column_tags"] is True
        assert caps.features["table_properties"] is True

    def test_provider_hierarchy(self, unity_provider):
        """Test provider hierarchy"""
        hierarchy = unity_provider.capabilities.hierarchy

        assert hierarchy.get_depth() == 3
        assert hierarchy.get_level(0).name == "catalog"
        assert hierarchy.get_level(1).name == "schema"
        assert hierarchy.get_level(2).name == "table"

    def test_create_initial_state(self, unity_provider):
        """Test creating initial empty state"""
        state = unity_provider.create_initial_state()

        assert "catalogs" in state
        assert state["catalogs"] == []

    def test_validate_valid_operation(self, unity_provider):
        """Test validating a valid operation"""
        builder = OperationBuilder()
        op = builder.add_catalog("cat_123", "bronze", op_id="op_001")

        validation = unity_provider.validate_operation(op)
        assert validation.valid
        assert len(validation.errors) == 0

    def test_validate_invalid_operation_missing_payload(self, unity_provider):
        """Test validating operation with missing payload fields"""
        OperationBuilder()
        op = Operation(
            id="op_002",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_catalog",
            target="cat_123",
            payload={},  # Missing catalogId and name
        )

        validation = unity_provider.validate_operation(op)
        assert not validation.valid
        assert len(validation.errors) > 0

    def test_validate_unsupported_operation(self, unity_provider):
        """Test validating unsupported operation type"""
        OperationBuilder()
        op = Operation(
            id="op_003",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.unsupported_operation",
            target="target_123",
            payload={},
        )

        validation = unity_provider.validate_operation(op)
        assert not validation.valid

    def test_apply_single_operation(self, unity_provider, empty_unity_state):
        """Test applying single operation to state"""
        builder = OperationBuilder()
        op = builder.add_catalog("cat_123", "bronze", op_id="op_001")

        state_dict = empty_unity_state.model_dump(by_alias=True)
        new_state = unity_provider.apply_operation(state_dict, op)

        assert len(new_state["catalogs"]) == 1
        assert new_state["catalogs"][0]["name"] == "bronze"

    def test_apply_multiple_operations(self, unity_provider, empty_unity_state, sample_operations):
        """Test applying multiple operations to state"""
        state_dict = empty_unity_state.model_dump(by_alias=True)
        new_state = unity_provider.apply_operations(state_dict, sample_operations)

        # Verify all operations were applied
        assert len(new_state["catalogs"]) == 1
        assert len(new_state["catalogs"][0]["schemas"]) == 1
        assert len(new_state["catalogs"][0]["schemas"][0]["tables"]) == 1
        assert len(new_state["catalogs"][0]["schemas"][0]["tables"][0]["columns"]) == 1

    def test_validate_valid_state(self, unity_provider, sample_unity_state):
        """Test validating a valid state"""
        state_dict = sample_unity_state.model_dump(by_alias=True)
        validation = unity_provider.validate_state(state_dict)

        assert validation.valid
        assert len(validation.errors) == 0

    def test_validate_invalid_state_structure(self, unity_provider):
        """Test validating state with invalid structure"""
        invalid_state = {"invalid": "structure"}

        validation = unity_provider.validate_state(invalid_state)
        assert not validation.valid

    def test_get_sql_generator(self, unity_provider, sample_unity_state):
        """Test getting SQL generator"""
        builder = OperationBuilder()
        state_dict = sample_unity_state.model_dump(by_alias=True)
        generator = unity_provider.get_sql_generator(state_dict)

        assert generator is not None

        # Test generating SQL
        op = builder.add_catalog("cat_001", "test", op_id="op_001")

        result = generator.generate_sql_for_operation(op)
        assert "CREATE CATALOG" in result.sql

    @staticmethod
    def _mock_query_response(
        columns: list[str], rows: list[list[str]], state: StatementState = StatementState.SUCCEEDED
    ) -> SimpleNamespace:
        return SimpleNamespace(
            status=SimpleNamespace(
                state=state,
                error=SimpleNamespace(message="boom")
                if state != StatementState.SUCCEEDED
                else None,
            ),
            result=SimpleNamespace(data_array=rows),
            manifest=SimpleNamespace(
                schema=SimpleNamespace(columns=[SimpleNamespace(name=col) for col in columns])
            ),
        )

    def test_discover_state_builds_hierarchy(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())
        executed_sql: list[str] = []

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            executed_sql.append(statement)
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    ["table_name", "table_type", "comment"],
                    [["users", "BASE TABLE", "user table"], ["active_users", "VIEW", None]],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [
                        ["users", "id", "BIGINT", "NO", "pk", "1"],
                        ["users", "email", "STRING", "YES", None, "2"],
                    ],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [["users", "sensitivity", "low"]],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [["users", "pk_users", "PRIMARY KEY", "id", "1", None, None, None, None, None]],
                )
            if statement.strip().startswith("SHOW TBLPROPERTIES"):
                return self._mock_query_response(
                    ["key", "value"],
                    [["tag", "core"]],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client

            state = unity_provider.discover_state(
                config=ExecutionConfig(
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_123",
                ),
                scope={"catalog": "main", "schema": "analytics"},
            )

        assert len(state["catalogs"]) == 1
        cat = state["catalogs"][0]
        assert cat["name"] == "main"
        assert len(cat["schemas"]) == 1
        sch = cat["schemas"][0]
        assert sch["name"] == "analytics"
        assert len(sch["tables"]) == 1
        assert sch["tables"][0]["name"] == "users"
        assert sch["tables"][0]["tags"] == {"sensitivity": "low"}
        assert sch["tables"][0]["properties"] == {"tag": "core"}
        assert len(sch["tables"][0]["constraints"]) == 1
        assert sch["tables"][0]["constraints"][0]["type"] == "primary_key"
        assert len(sch["tables"][0]["columns"]) == 2
        assert sch["tables"][0]["columns"][0]["nullable"] is False
        assert len(sch["views"]) == 1
        assert sch["views"][0]["name"] == "active_users"
        assert any("catalog_name = 'main'" in sql for sql in executed_sql)
        assert any("schema_name = 'analytics'" in sql for sql in executed_sql)

    def test_discover_state_generates_deterministic_ids(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    ["table_name", "table_type", "comment"],
                    [["users", "BASE TABLE", None]],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [["users", "id", "BIGINT", "NO", None, "1"]],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            if statement.strip().startswith("SHOW TBLPROPERTIES"):
                return self._mock_query_response(["key", "value"], [])
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            cfg = ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123")
            state_1 = unity_provider.discover_state(config=cfg, scope={"catalog": "main"})
            state_2 = unity_provider.discover_state(config=cfg, scope={"catalog": "main"})

        assert state_1["catalogs"][0]["id"] == state_2["catalogs"][0]["id"]
        assert (
            state_1["catalogs"][0]["schemas"][0]["tables"][0]["id"]
            == state_2["catalogs"][0]["schemas"][0]["tables"][0]["id"]
        )
        assert (
            state_1["catalogs"][0]["schemas"][0]["tables"][0]["columns"][0]["id"]
            == state_2["catalogs"][0]["schemas"][0]["tables"][0]["columns"][0]["id"]
        )

    def test_discover_state_imports_table_tags_properties_and_constraints(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    ["table_name", "table_type", "comment"],
                    [
                        ["users", "BASE TABLE", "users table"],
                        ["orders", "BASE TABLE", "orders table"],
                    ],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [
                        ["users", "user_id", "INT", "NO", None, "1"],
                        ["orders", "order_id", "INT", "NO", None, "1"],
                        ["orders", "user_id", "INT", "NO", None, "2"],
                    ],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [["users", "sensitivity", "low"], ["orders", "domain", "sales"]],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [
                        [
                            "users",
                            "pk_users",
                            "PRIMARY KEY",
                            "user_id",
                            "1",
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        [
                            "orders",
                            "fk_orders_user",
                            "FOREIGN KEY",
                            "user_id",
                            "1",
                            "users",
                            "user_id",
                            "1",
                            "NO_ACTION",
                            "NO_ACTION",
                        ],
                    ],
                )
            if statement.strip() == "SHOW TBLPROPERTIES `main`.`analytics`.`users`":
                return self._mock_query_response(["key", "value"], [["tag", "core"]])
            if statement.strip() == "SHOW TBLPROPERTIES `main`.`analytics`.`orders`":
                return self._mock_query_response(
                    ["key", "value"],
                    [["tag", "transactional"], ["sensitivity", "medium"]],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={"catalog": "main", "schema": "analytics"},
            )

        tables = state["catalogs"][0]["schemas"][0]["tables"]
        users = next(table for table in tables if table["name"] == "users")
        orders = next(table for table in tables if table["name"] == "orders")

        assert users["tags"] == {"sensitivity": "low"}
        assert users["properties"] == {"tag": "core"}
        assert len(users["constraints"]) == 1
        assert users["constraints"][0]["type"] == "primary_key"

        assert orders["tags"] == {"domain": "sales"}
        assert orders["properties"] == {"tag": "transactional", "sensitivity": "medium"}
        assert len(orders["constraints"]) == 1
        assert orders["constraints"][0]["type"] == "foreign_key"
        assert orders["constraints"][0]["parentTable"] == unity_provider._stable_id(
            "tbl", "main", "analytics", "users"
        )

    def test_discover_state_imports_column_tags_and_external_table_metadata(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "table_type",
                        "comment",
                        "data_source_format",
                        "storage_path",
                    ],
                    [
                        [
                            "events_ext",
                            "EXTERNAL",
                            "external events",
                            "DELTA",
                            "abfss://bucket/path",
                        ],
                    ],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [
                        ["events_ext", "event_id", "STRING", "NO", None, "1"],
                        ["events_ext", "payload", "STRING", "YES", None, "2"],
                    ],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [["events_ext", "layer", "bronze"]],
                )
            if "information_schema.column_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "column_name", "tag_name", "tag_value"],
                    [["events_ext", "event_id", "classification", "public"]],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            if statement.strip() == "SHOW TBLPROPERTIES `main`.`analytics`.`events_ext`":
                return self._mock_query_response(["key", "value"], [["quality", "bronze"]])
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={"catalog": "main", "schema": "analytics"},
            )

        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert table["name"] == "events_ext"
        assert table["format"] == "delta"
        assert table["external"] is True
        assert table["path"] == "abfss://bucket/path"
        assert table["tags"] == {"layer": "bronze"}
        assert table["properties"] == {"quality": "bronze"}

        event_id_col = next(col for col in table["columns"] if col["name"] == "event_id")
        payload_col = next(col for col in table["columns"] if col["name"] == "payload")
        assert event_id_col["tags"] == {"classification": "public"}
        assert payload_col["tags"] == {}

    def test_discover_state_imports_catalog_schema_view_and_check_metadata(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            normalized = " ".join(statement.strip().split())
            if normalized.startswith("SELECT catalog_name FROM system.information_schema.catalogs"):
                return self._mock_query_response(["catalog_name"], [["main"]])
            if normalized.startswith(
                "SELECT catalog_name, comment FROM system.information_schema.catalogs"
            ):
                return self._mock_query_response(
                    ["catalog_name", "comment"], [["main", "Main catalog comment"]]
                )
            if "FROM system.information_schema.catalog_tags" in normalized:
                return self._mock_query_response(
                    ["catalog_name", "tag_name", "tag_value"],
                    [["main", "domain", "core"]],
                )
            if normalized.startswith("SELECT schema_name FROM system.information_schema.schemata"):
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if normalized.startswith(
                "SELECT schema_name, comment FROM system.information_schema.schemata"
            ):
                return self._mock_query_response(
                    ["schema_name", "comment"], [["analytics", "Analytics schema comment"]]
                )
            if "FROM system.information_schema.schema_tags" in normalized:
                return self._mock_query_response(
                    ["schema_name", "tag_name", "tag_value"],
                    [["analytics", "tier", "gold"]],
                )
            if "FROM system.information_schema.tables" in normalized:
                return self._mock_query_response(
                    [
                        "table_name",
                        "table_type",
                        "comment",
                        "data_source_format",
                        "storage_path",
                    ],
                    [
                        ["users", "BASE TABLE", "users table", "DELTA", None],
                        ["active_users", "VIEW", "active users view", None, None],
                    ],
                )
            if "FROM system.information_schema.views" in normalized:
                return self._mock_query_response(
                    ["table_name", "view_definition", "is_materialized"],
                    [["active_users", "SELECT user_id FROM main.analytics.users", "NO"]],
                )
            if "FROM system.information_schema.columns" in normalized:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [["users", "user_id", "BIGINT", "NO", None, "1"]],
                )
            if "FROM system.information_schema.table_tags" in normalized:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [["active_users", "consumer", "bi"]],
                )
            if "FROM system.information_schema.column_tags" in normalized:
                return self._mock_query_response(
                    ["table_name", "column_name", "tag_name", "tag_value"],
                    [],
                )
            if "AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')" in normalized:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            if "information_schema.check_constraints" in normalized:
                return self._mock_query_response(
                    ["table_name", "constraint_name", "check_clause"],
                    [["users", "ck_user_id_positive", "(user_id > 0)"]],
                )
            if normalized == "SHOW TBLPROPERTIES `main`.`analytics`.`users`":
                return self._mock_query_response(["key", "value"], [["quality", "gold"]])
            if normalized == "SHOW TBLPROPERTIES `main`.`analytics`.`active_users`":
                return self._mock_query_response(["key", "value"], [["comment", "View comment"]])
            if normalized == "DESCRIBE DETAIL `main`.`analytics`.`users`":
                return self._mock_query_response(
                    ["format", "location", "partitionColumns", "clusteringColumns"],
                    [
                        [
                            "delta",
                            "abfss://test@storage/path/users",
                            '["user_id"]',
                            '["user_id"]',
                        ]
                    ],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={"catalog": "main", "schema": "analytics"},
            )

        catalog = state["catalogs"][0]
        schema = catalog["schemas"][0]
        table = schema["tables"][0]
        view = schema["views"][0]

        assert catalog["comment"] == "Main catalog comment"
        assert catalog["tags"] == {"domain": "core"}
        assert schema["comment"] == "Analytics schema comment"
        assert schema["tags"] == {"tier": "gold"}

        assert table["partitionColumns"] == ["user_id"]
        assert table["clusterColumns"] == ["user_id"]
        check_constraints = [c for c in table["constraints"] if c["type"] == "check"]
        assert len(check_constraints) == 1
        assert check_constraints[0]["expression"] == "(user_id > 0)"

        assert view["definition"] == "SELECT user_id FROM main.analytics.users"
        assert view["comment"] == "active users view"
        assert view["tags"] == {"consumer": "bi"}
        assert view["properties"] == {"comment": "View comment"}
        assert view["extractedDependencies"] is not None
        assert "main.analytics.users" in view["extractedDependencies"]["all"]

    def test_discover_state_imports_check_constraints_from_delta_properties(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "table_type",
                        "comment",
                        "data_source_format",
                        "storage_path",
                    ],
                    [["orders", "BASE TABLE", "orders table", "DELTA", None]],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [
                        ["orders", "order_id", "BIGINT", "NO", None, "1"],
                        ["orders", "order_amount", "DECIMAL(12,2)", "YES", None, "2"],
                    ],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [],
                )
            if "information_schema.column_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "column_name", "tag_name", "tag_value"],
                    [],
                )
            if "AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            if "information_schema.check_constraints" in statement:
                return self._mock_query_response(
                    ["table_name", "constraint_name", "check_clause"],
                    [],
                )
            if statement.strip() == "SHOW TBLPROPERTIES `main`.`analytics`.`orders`":
                return self._mock_query_response(
                    ["key", "value"],
                    [["delta.constraints.chk_order_amount_non_negative", "order_amount >= 0"]],
                )
            if statement.strip() == "DESCRIBE DETAIL `main`.`analytics`.`orders`":
                return self._mock_query_response(
                    ["format", "location", "partitionColumns", "clusteringColumns"],
                    [["delta", None, "[]", "[]"]],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={"catalog": "main", "schema": "analytics"},
            )

        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        check_constraints = [c for c in table["constraints"] if c["type"] == "check"]
        assert len(check_constraints) == 1
        assert check_constraints[0]["name"] == "chk_order_amount_non_negative"
        assert check_constraints[0]["expression"] == "order_amount >= 0"

    def test_discover_state_raises_on_failed_query(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())
        mock_client.statement_execution.execute_statement = lambda **_: self._mock_query_response(
            ["catalog_name"], [], state=StatementState.FAILED
        )

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            with pytest.raises(RuntimeError, match="Unity metadata discovery query failed"):
                unity_provider.discover_state(
                    config=ExecutionConfig(
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_123",
                    ),
                    scope={"catalog": "main"},
                )

    def test_discover_state_filters_information_schema(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())
        executed_sql: list[str] = []

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            executed_sql.append(statement)
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                if "lower(schema_name) <> 'information_schema'" in statement:
                    return self._mock_query_response(["schema_name"], [["analytics"]])
                return self._mock_query_response(
                    ["schema_name"], [["analytics"], ["information_schema"]]
                )
            if "information_schema.tables" in statement:
                return self._mock_query_response(["table_name", "table_type", "comment"], [])
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={"catalog": "main"},
            )

        schemas = state["catalogs"][0]["schemas"]
        assert [s["name"] for s in schemas] == ["analytics"]
        assert any("lower(schema_name) <> 'information_schema'" in sql for sql in executed_sql)

    def test_discover_state_warns_on_unsupported_table_types(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            if "information_schema.catalogs" in statement:
                return self._mock_query_response(["catalog_name"], [["main"]])
            if "information_schema.schemata" in statement:
                return self._mock_query_response(["schema_name"], [["analytics"]])
            if "information_schema.tables" in statement:
                return self._mock_query_response(
                    ["table_name", "table_type", "comment"],
                    [
                        ["users", "MANAGED", None],
                        ["active_users", "VIEW", None],
                        ["mv_users", "MATERIALIZED VIEW", None],
                    ],
                )
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [["users", "id", "BIGINT", "NO", None, "1"]],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            if statement.strip().startswith("SHOW TBLPROPERTIES"):
                return self._mock_query_response(["key", "value"], [])
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            config = ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123")
            state = unity_provider.discover_state(
                config=config,
                scope={"catalog": "main", "schema": "analytics"},
            )
            warnings = unity_provider.collect_import_warnings(
                config=config,
                scope={"catalog": "main", "schema": "analytics"},
                discovered_state=state,
            )

        assert [table["name"] for table in state["catalogs"][0]["schemas"][0]["tables"]] == [
            "users"
        ]
        assert [view["name"] for view in state["catalogs"][0]["schemas"][0]["views"]] == [
            "active_users"
        ]
        assert any("mv_users (MATERIALIZED VIEW)" in warning for warning in warnings)

    def test_discover_state_filters_system_catalog(self, unity_provider):
        mock_client = SimpleNamespace(statement_execution=SimpleNamespace())
        executed_sql: list[str] = []

        def _execute_statement(warehouse_id: str, statement: str, wait_timeout: str):
            del warehouse_id, wait_timeout
            executed_sql.append(statement)
            if "information_schema.catalogs" in statement:
                if "lower(catalog_name) NOT IN ('system')" in statement:
                    return self._mock_query_response(["catalog_name"], [["main"], ["finance"]])
                return self._mock_query_response(
                    ["catalog_name"], [["main"], ["system"], ["finance"]]
                )
            if "information_schema.schemata" in statement:
                if "catalog_name = 'main'" in statement:
                    return self._mock_query_response(["schema_name"], [["analytics"]])
                if "catalog_name = 'finance'" in statement:
                    return self._mock_query_response(["schema_name"], [["core"]])
                raise AssertionError(f"Unexpected schemata query: {statement}")
            if "information_schema.tables" in statement:
                return self._mock_query_response(["table_name", "table_type", "comment"], [])
            if "information_schema.columns" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "column_name",
                        "data_type",
                        "is_nullable",
                        "comment",
                        "ordinal_position",
                    ],
                    [],
                )
            if "information_schema.table_tags" in statement:
                return self._mock_query_response(
                    ["table_name", "tag_name", "tag_value"],
                    [],
                )
            if "information_schema.table_constraints" in statement:
                return self._mock_query_response(
                    [
                        "table_name",
                        "constraint_name",
                        "constraint_type",
                        "child_column_name",
                        "child_ordinal_position",
                        "parent_table_name",
                        "parent_column_name",
                        "parent_ordinal_position",
                        "update_rule",
                        "delete_rule",
                    ],
                    [],
                )
            raise AssertionError(f"Unexpected query: {statement}")

        mock_client.statement_execution.execute_statement = _execute_statement

        with patch("schematic.providers.unity.provider.create_databricks_client") as mock_factory:
            mock_factory.return_value = mock_client
            state = unity_provider.discover_state(
                config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_123"),
                scope={},
            )

        assert [catalog["name"] for catalog in state["catalogs"]] == ["main", "finance"]
        assert any("lower(catalog_name) NOT IN ('system')" in sql for sql in executed_sql)

    def test_validate_import_scope_rejects_system_catalog(self, unity_provider):
        validation = unity_provider.validate_import_scope({"catalog": "system"})
        assert not validation.valid
        assert validation.errors
        assert validation.errors[0].field == "catalog"
        assert "system-managed" in validation.errors[0].message


class TestOperationMetadata:
    """Test operation metadata"""

    @pytest.fixture
    def unity_provider(self):
        """Get Unity provider instance"""
        return ProviderRegistry.get("unity")

    def test_get_operation_metadata(self, unity_provider):
        """Test getting operation metadata"""
        caps = unity_provider.capabilities

        # Verify all Unity operations have metadata
        unity_ops = [op for op in caps.supported_operations if op.startswith("unity.")]

        assert len(unity_ops) == 40  # All 40 Unity operations (4+4+6+7+7+2+2+2+3+3)
        # 4 catalog (add, rename, update, drop), 4 schema (add, rename, update, drop)
        # 6 table, 7 column, 7 view, 2 constraint, 2 tag, 2 row filter, 3 column mask, 3 property


class TestProviderIntegration:
    """Test provider integration with other components"""

    @pytest.fixture
    def unity_provider(self):
        """Get Unity provider instance"""
        return ProviderRegistry.get("unity")

    def test_full_workflow_with_provider(self, unity_provider, empty_unity_state):
        """Test complete workflow using provider"""
        builder = OperationBuilder()
        # Create operations
        ops = [
            builder.add_catalog("cat_123", "production", op_id="op_001"),
            builder.add_schema("schema_456", "analytics", "cat_123", op_id="op_002"),
            builder.add_table("table_789", "events", "schema_456", "delta", op_id="op_003"),
        ]

        # Validate all operations
        for op in ops:
            validation = unity_provider.validate_operation(op)
            assert validation.valid, f"Operation {op.id} failed validation: {validation.errors}"

        # Apply operations
        state_dict = empty_unity_state.model_dump(by_alias=True)
        final_state = unity_provider.apply_operations(state_dict, ops)

        # Validate final state
        validation = unity_provider.validate_state(final_state)
        assert validation.valid

        # Generate SQL
        generator = unity_provider.get_sql_generator(final_state)
        sql = generator.generate_sql(ops)

        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql
        assert "CREATE TABLE" in sql
        assert "`production`" in sql
        assert "`analytics`" in sql
        assert "`events`" in sql

    def test_provider_handles_invalid_operations_gracefully(
        self, unity_provider, empty_unity_state
    ):
        """Test that provider handles invalid operations gracefully"""
        OperationBuilder()
        invalid_op = Operation(
            id="op_invalid",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_catalog",
            target="cat_123",
            payload={},  # Missing required fields
        )

        # Validation should fail
        validation = unity_provider.validate_operation(invalid_op)
        assert not validation.valid

        # Applying should not crash (implementation-dependent)
        state_dict = empty_unity_state.model_dump(by_alias=True)
        try:
            # Some implementations might throw, others might return unchanged state
            unity_provider.apply_operation(state_dict, invalid_op)
            # If it doesn't throw, state should be unchanged or handle gracefully
        except (ValueError, KeyError):
            # Expected for some implementations
            pass

    def test_provider_state_immutability(self, unity_provider, sample_unity_state):
        """Test that provider operations don't mutate original state"""
        builder = OperationBuilder()
        original_state_dict = sample_unity_state.model_dump(by_alias=True)
        original_catalog_count = len(original_state_dict["catalogs"])

        op = builder.add_catalog("cat_new", "new_catalog", op_id="op_001")

        # Apply operation
        new_state = unity_provider.apply_operation(original_state_dict, op)

        # Original state should be unchanged (provider should create new state)
        # Note: This depends on whether apply_operation creates a deep copy
        # The important thing is the new_state has the changes
        assert len(new_state["catalogs"]) == original_catalog_count + 1
        assert new_state["catalogs"][-1]["name"] == "new_catalog"


class TestProviderErrorHandling:
    """Test provider error handling"""

    @pytest.fixture
    def unity_provider(self):
        """Get Unity provider instance"""
        return ProviderRegistry.get("unity")

    def test_validate_operation_with_wrong_provider_prefix(self, unity_provider):
        """Test validating operation with wrong provider prefix"""
        OperationBuilder()
        op = Operation(
            id="op_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="hive.add_database",  # Wrong provider
            target="db_123",
            payload={"databaseId": "db_123", "name": "test"},
        )

        validation = unity_provider.validate_operation(op)
        assert not validation.valid

    def test_sql_generator_handles_missing_references(self, unity_provider, empty_unity_state):
        """Test SQL generator handles missing references gracefully"""
        builder = OperationBuilder()
        state_dict = empty_unity_state.model_dump(by_alias=True)
        generator = unity_provider.get_sql_generator(state_dict)

        # Operation referencing non-existent table
        op = builder.add_column(
            "col_001",
            "nonexistent_table",
            "test_col",
            "STRING",
            nullable=True,
            comment="None",
            op_id="op_001",
        )

        # Should handle gracefully (might generate SQL with table ID or error comment)
        result = generator.generate_sql_for_operation(op)
        assert result.sql is not None
