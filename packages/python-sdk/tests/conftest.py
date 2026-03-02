from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree

import pytest

from schemax.core.storage import (
    append_ops,
    default_project_skeleton_tail,
    ensure_project_file,
)
from schemax.providers.unity.models import (
    UnityState,
)
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder
from tests.utils.fixture_data import make_rich_sample_operations

# SQLGlot for SQL validation
try:
    import sqlglot

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


@pytest.fixture
def temp_workspace(tmp_path):
    """Create a temporary workspace directory"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return workspace


@pytest.fixture
def resource_workspace(tmp_path):
    """Factory fixture that copies a checked-in resource project to a temp workspace."""

    def _load(project_name: str) -> Path:
        resources_root = Path(__file__).parent / "resources" / "projects"
        source = resources_root / project_name
        if not source.exists():
            raise FileNotFoundError(f"Test resource project not found: {source}")
        destination = tmp_path / project_name
        copytree(source, destination)
        return destination

    return _load


@pytest.fixture
def schemax_demo_workspace(resource_workspace):
    """Workspace loaded from tests/resources/projects/schemax_demo."""
    return resource_workspace("schemax_demo")


@pytest.fixture
def unity_full_workspace(resource_workspace):
    """Workspace loaded from tests/resources/projects/unity_full (rich fixture snapshot)."""
    return resource_workspace("unity_full")


@pytest.fixture
def schemax_dir(temp_workspace):
    """Create .schemax directory"""
    schemax = temp_workspace / ".schemax"
    schemax.mkdir()
    return schemax


@pytest.fixture
def sample_project_v4():
    """Sample v4 project structure"""
    return {
        "version": 4,
        "name": "test_project",
        "provider": {
            "type": "unity",
            "version": "1.0.0",
            "environments": {
                "dev": {
                    "catalog": "dev_test_project",
                    "description": "Development environment",
                    "allowDrift": True,
                    "requireSnapshot": False,
                    "autoCreateCatalog": True,
                    "autoCreateSchemaxSchema": True,
                },
                "test": {
                    "catalog": "test_test_project",
                    "description": "Test environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "autoCreateCatalog": True,
                    "autoCreateSchemaxSchema": True,
                },
                "prod": {
                    "catalog": "prod_test_project",
                    "description": "Production environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "requireApproval": False,
                    "autoCreateCatalog": False,
                    "autoCreateSchemaxSchema": True,
                },
            },
        },
        **default_project_skeleton_tail(),
    }


@pytest.fixture
def sample_changelog():
    """Sample empty changelog"""
    return {
        "version": 1,
        "sinceSnapshot": None,
        "ops": [],
        "lastModified": datetime.now(UTC).isoformat(),
    }


@pytest.fixture
def sample_catalog_op():
    """Sample add_catalog operation"""
    builder = OperationBuilder()
    return builder.catalog.add_catalog("cat_123", "bronze", op_id="op_001")


@pytest.fixture
def sample_operations():
    """Rich sample operations: all data types, tables (Delta/Iceberg/partitioned), views, functions, volumes, MVs."""
    return make_rich_sample_operations()


@pytest.fixture
def sample_unity_state(empty_unity_state, sample_operations):
    """Unity Catalog state built by applying sample_operations to empty state."""
    return apply_operations(empty_unity_state, sample_operations)


@pytest.fixture
def empty_unity_state():
    """Empty Unity Catalog state"""
    return UnityState(catalogs=[])


@pytest.fixture
def initialized_workspace(temp_workspace):
    """Workspace with initialized .schemax project (v4)"""
    ensure_project_file(temp_workspace, provider_id="unity")
    return temp_workspace


@pytest.fixture
def workspace_with_operations(initialized_workspace, sample_operations):
    """Workspace with operations in changelog"""
    append_ops(initialized_workspace, sample_operations)
    return initialized_workspace


# SQL Validation Helpers
def validate_sql(sql: str, dialect: str = "databricks") -> tuple[bool, str]:
    """
    Validate SQL syntax using SQLGlot.

    Args:
        sql: SQL string to validate
        dialect: SQL dialect (default: databricks)

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not SQLGLOT_AVAILABLE:
        return True, "SQLGlot not available, skipping validation"

    try:
        # Parse the SQL
        parsed = sqlglot.parse_one(sql, dialect=dialect)

        if parsed is None:
            return False, "SQLGlot returned None (invalid SQL)"

        return True, "SQL is valid"
    except Exception as e:
        return False, f"SQLGlot parsing error: {str(e)}"


def assert_valid_sql(sql: str, dialect: str = "databricks") -> None:
    """
    Assert that SQL is syntactically valid.

    Raises AssertionError if SQL is invalid.
    """
    is_valid, error_msg = validate_sql(sql, dialect)
    assert is_valid, f"Invalid SQL:\n{sql}\n\nError: {error_msg}"


@pytest.fixture
def sql_validator():
    """Fixture that provides SQL validation function"""
    return validate_sql


@pytest.fixture
def assert_sql():
    """Fixture that provides SQL assertion function"""
    return assert_valid_sql
