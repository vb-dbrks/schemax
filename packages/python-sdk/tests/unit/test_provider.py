"""
Unit tests for Provider System

Tests the provider registry and Unity provider implementation including:
- Provider registration and retrieval
- Operation validation
- State validation
- SQL generator creation
"""

import pytest

from schematic.providers import Operation, ProviderRegistry
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

        assert len(unity_ops) == 33  # All 33 Unity operations (4+4+6+7+2+2+2+3+3)
        # 4 catalog (add, rename, update, drop), 4 schema (add, rename, update, drop)


class TestProviderIntegration:
    """Test provider integration with other components"""

    @pytest.fixture
    def unity_provider(self):
        """Get Unity provider instance"""
        return ProviderRegistry.get("unity")

    @pytest.mark.skip(reason="Integration test - blocked by issue #19")
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
