"""
Unit tests for deployment scope filter (managed categories and existing objects).
"""

import pytest

from schemax.providers import ProviderRegistry
from schemax.providers.base.operations import Operation
from schemax.providers.base.scope_filter import filter_operations_by_managed_scope


@pytest.fixture
def unity_provider():
    """Unity provider for metadata lookup."""
    return ProviderRegistry.get("unity")


def test_filter_no_managed_categories_returns_all_ops(unity_provider):
    """When managedCategories is missing, all ops pass."""
    ops = [
        Operation(
            id="op1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"catalogId": "cat_1", "name": "main"},
        ),
        Operation(
            id="op2",
            ts="2025-01-01T00:00:01Z",
            provider="unity",
            op="unity.add_grant",
            target="cat_1",
            payload={"targetType": "catalog", "targetId": "cat_1", "principal": "u", "privileges": ["USE CATALOG"]},
        ),
    ]
    env_config = {}
    result = filter_operations_by_managed_scope(ops, env_config, unity_provider)
    assert len(result) == 2
    assert result[0].op == "unity.add_catalog"
    assert result[1].op == "unity.add_grant"


def test_filter_governance_only_keeps_only_governance_ops(unity_provider):
    """When managedCategories is ['governance'], only governance ops pass."""
    ops = [
        Operation(
            id="op1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"catalogId": "cat_1", "name": "main"},
        ),
        Operation(
            id="op2",
            ts="2025-01-01T00:00:01Z",
            provider="unity",
            op="unity.add_grant",
            target="cat_1",
            payload={"targetType": "catalog", "targetId": "cat_1", "principal": "u", "privileges": ["USE CATALOG"]},
        ),
        Operation(
            id="op3",
            ts="2025-01-01T00:00:02Z",
            provider="unity",
            op="unity.set_table_comment",
            target="tbl_1",
            payload={"tableId": "tbl_1", "comment": "x"},
        ),
    ]
    env_config = {"managedCategories": ["governance"]}
    result = filter_operations_by_managed_scope(ops, env_config, unity_provider)
    assert len(result) == 2
    assert result[0].op == "unity.add_grant"
    assert result[1].op == "unity.set_table_comment"


def test_filter_existing_objects_catalog_drops_add_catalog(unity_provider):
    """When existingObjects.catalog contains a name, add_catalog for that name is dropped."""
    ops = [
        Operation(
            id="op1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_analytics",
            payload={"catalogId": "cat_analytics", "name": "analytics"},
        ),
        Operation(
            id="op2",
            ts="2025-01-01T00:00:01Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_main",
            payload={"catalogId": "cat_main", "name": "main"},
        ),
    ]
    env_config = {"existingObjects": {"catalog": ["analytics"]}}
    result = filter_operations_by_managed_scope(ops, env_config, unity_provider)
    assert len(result) == 1
    assert result[0].payload["name"] == "main"


def test_filter_empty_managed_categories_treated_as_all(unity_provider):
    """When managedCategories is [], treat as missing (all ops pass)."""
    ops = [
        Operation(
            id="op1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"catalogId": "cat_1", "name": "main"},
        ),
    ]
    env_config = {"managedCategories": []}
    result = filter_operations_by_managed_scope(ops, env_config, unity_provider)
    assert len(result) == 1
