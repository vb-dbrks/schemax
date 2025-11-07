from datetime import UTC, datetime

import pytest

from schematic.providers.unity.models import (
    UnityCatalog,
    UnityColumn,
    UnitySchema,
    UnityState,
    UnityTable,
)
from tests.utils import OperationBuilder

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
def schematic_dir(temp_workspace):
    """Create .schematic directory"""
    schematic = temp_workspace / ".schematic"
    schematic.mkdir()
    return schematic


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
                    "autoCreateSchematicSchema": True,
                },
                "test": {
                    "catalog": "test_test_project",
                    "description": "Test environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "autoCreateCatalog": True,
                    "autoCreateSchematicSchema": True,
                },
                "prod": {
                    "catalog": "prod_test_project",
                    "description": "Production environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "requireApproval": False,
                    "autoCreateCatalog": False,
                    "autoCreateSchematicSchema": True,
                },
            },
        },
        "snapshots": [],
        "deployments": [],
        "settings": {
            "autoIncrementVersion": True,
            "versionPrefix": "v",
            "catalogMode": "single",
        },
        "latestSnapshot": None,
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
    return builder.add_catalog("cat_123", "bronze", op_id="op_001")


@pytest.fixture
def sample_operations():
    """Sample operations for testing workflows"""
    builder = OperationBuilder()
    return [
        builder.add_catalog("cat_123", "bronze", op_id="op_001"),
        builder.add_schema("schema_456", "raw", "cat_123", op_id="op_002"),
        builder.add_table("table_789", "users", "schema_456", "delta", op_id="op_003"),
        builder.add_column(
            "col_001",
            "table_789",
            "user_id",
            "BIGINT",
            nullable=False,
            comment="User ID",
            op_id="op_004",
        ),
    ]


@pytest.fixture
def sample_unity_state():
    """Sample Unity Catalog state with data"""
    return UnityState(
        catalogs=[
            UnityCatalog(
                id="cat_123",
                provider="unity",
                name="bronze",
                schemas=[
                    UnitySchema(
                        id="schema_456",
                        provider="unity",
                        name="raw",
                        tables=[
                            UnityTable(
                                id="table_789",
                                provider="unity",
                                name="users",
                                format="delta",
                                columns=[
                                    UnityColumn(
                                        id="col_001",
                                        provider="unity",
                                        name="user_id",
                                        type="BIGINT",
                                        nullable=False,
                                        comment="User ID",
                                    ),
                                    UnityColumn(
                                        id="col_002",
                                        provider="unity",
                                        name="email",
                                        type="STRING",
                                        nullable=True,
                                        comment="Email address",
                                    ),
                                ],
                                properties={},
                                constraints=[],
                                grants=[],
                            )
                        ],
                    )
                ],
            )
        ]
    )


@pytest.fixture
def empty_unity_state():
    """Empty Unity Catalog state"""
    return UnityState(catalogs=[])


@pytest.fixture
def initialized_workspace(temp_workspace):
    """Workspace with initialized .schematic project (v4)"""
    from schematic.core.storage import ensure_project_file

    ensure_project_file(temp_workspace, provider_id="unity")
    return temp_workspace


@pytest.fixture
def workspace_with_operations(initialized_workspace, sample_operations):
    """Workspace with operations in changelog"""
    from schematic.core.storage import append_ops

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
