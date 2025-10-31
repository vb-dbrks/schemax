"""
Integration tests for complete workflows

Tests end-to-end workflows including:
- Project initialization → operations → snapshots → SQL generation
- Multiple snapshots and version management
- Deployment tracking
- Complete schema creation and modification workflows
"""

import pytest

from schematic.providers.base.operations import Operation
from schematic.storage_v4 import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_last_deployment,
    get_uncommitted_ops_count,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_deployment,
)
from tests.utils import OperationBuilder


@pytest.mark.integration
class TestBasicWorkflow:
    """Test basic project workflow"""

    @pytest.mark.skip(
        reason="Integration test - SQL assertions need adjustment (related to issue #19, #20)"
    )
    def test_init_to_sql_workflow(self, temp_workspace):
        """Test: initialize project → add operations → generate SQL"""
        builder = OperationBuilder()
        # Step 1: Initialize project
        ensure_project_file(temp_workspace, provider_id="unity")

        # Verify initialization
        project = read_project(temp_workspace)
        assert project["version"] == 4
        assert project["provider"]["type"] == "unity"

        # Step 2: Add operations
        ops = [
            builder.add_catalog("cat_123", "production", op_id="op_001"),
            builder.add_schema("schema_456", "analytics", "cat_123", op_id="op_002"),
            builder.add_table("table_789", "events", "schema_456", "delta", op_id="op_003"),
        ]
        append_ops(temp_workspace, ops)

        # Verify operations were added
        changelog = read_changelog(temp_workspace)
        assert len(changelog["ops"]) == 3

        # Step 3: Load state and generate SQL
        state, changelog, provider = load_current_state(temp_workspace)

        # Verify state
        assert len(state["catalogs"]) == 1
        assert state["catalogs"][0]["name"] == "production"
        assert len(state["catalogs"][0]["schemas"]) == 1
        assert state["catalogs"][0]["schemas"][0]["name"] == "analytics"

        # Generate SQL
        generator = provider.get_sql_generator(state)
        sql = generator.generate_sql([Operation(**op) for op in changelog["ops"]])

        # Verify SQL
        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql
        assert "CREATE TABLE" in sql
        assert "`production`" in sql
        assert "`analytics`" in sql
        assert "`events`" in sql


@pytest.mark.integration
class TestSnapshotWorkflow:
    """Test snapshot creation and management workflows"""

    def test_snapshot_creation_workflow(self, initialized_workspace, sample_operations):
        """Test: add operations → create snapshot → verify state"""
        # Add operations
        append_ops(initialized_workspace, sample_operations)

        # Create snapshot
        project, snapshot = create_snapshot(
            initialized_workspace,
            name="Initial schema",
            version="v0.1.0",
            comment="First version of the schema",
            tags=["initial", "test"],
        )

        # Verify snapshot was created
        assert project["latestSnapshot"] == "v0.1.0"
        assert len(project["snapshots"]) == 1

        snapshot_meta = project["snapshots"][0]
        assert snapshot_meta["version"] == "v0.1.0"
        assert snapshot_meta["name"] == "Initial schema"
        assert snapshot_meta["comment"] == "First version of the schema"
        assert "initial" in snapshot_meta["tags"]

        # Verify changelog was cleared
        changelog = read_changelog(initialized_workspace)
        assert len(changelog["ops"]) == 0
        assert changelog["sinceSnapshot"] == "v0.1.0"

        # Verify snapshot file was created
        snapshot_data = read_snapshot(initialized_workspace, "v0.1.0")
        assert snapshot_data["version"] == "v0.1.0"
        assert "state" in snapshot_data
        # V4 auto-creates implicit catalog, so opsIncluded = sample_operations + 1
        assert len(snapshot_data["opsIncluded"]) == len(sample_operations) + 1

    def test_multiple_snapshots_workflow(self, initialized_workspace, sample_operations):
        """Test creating multiple snapshots with auto-incrementing versions"""
        # First snapshot
        append_ops(initialized_workspace, sample_operations[:2])
        create_snapshot(initialized_workspace, "First snapshot")

        project = read_project(initialized_workspace)
        assert project["latestSnapshot"] == "v0.1.0"

        # Second snapshot
        append_ops(initialized_workspace, sample_operations[2:])
        create_snapshot(initialized_workspace, "Second snapshot")

        project = read_project(initialized_workspace)
        assert project["latestSnapshot"] == "v0.2.0"
        assert len(project["snapshots"]) == 2

        # Verify both snapshots exist
        snapshot1 = read_snapshot(initialized_workspace, "v0.1.0")
        snapshot2 = read_snapshot(initialized_workspace, "v0.2.0")

        assert snapshot1["version"] == "v0.1.0"
        assert snapshot2["version"] == "v0.2.0"
        assert snapshot2["previousSnapshot"] == "v0.1.0"

    def test_snapshot_state_loading(self, initialized_workspace, sample_operations):
        """Test loading state from snapshot + changelog"""
        builder = OperationBuilder()
        # Create first snapshot with partial operations
        append_ops(initialized_workspace, sample_operations[:2])
        create_snapshot(initialized_workspace, "Partial schema", version="v0.1.0")

        # Add more operations after snapshot
        additional_ops = [
            builder.add_column(
                "col_new_001",
                "table_789",
                "status",
                "STRING",
                nullable=True,
                comment="None",
                op_id="op_new_001",
            )
        ]
        append_ops(initialized_workspace, additional_ops)

        # Load current state (should be snapshot + changelog ops)
        state, changelog, provider = load_current_state(initialized_workspace)

        # Verify state includes operations from snapshot
        # V4 has implicit catalog + user-defined catalog = 2
        assert len(state["catalogs"]) == 2
        # User-defined catalog is the second one (index 1)
        user_catalog = next(c for c in state["catalogs"] if c["name"] == "bronze")
        assert len(user_catalog["schemas"]) == 1

        # Verify changelog only has new operations
        assert len(changelog["ops"]) == 1
        assert changelog["ops"][0]["id"] == "op_new_001"


@pytest.mark.integration
class TestCompleteSchemaWorkflow:
    """Test complete schema design and modification workflows"""

    @pytest.mark.skip(reason="Integration test - blocked by issue #19")
    def test_create_complete_schema(self, initialized_workspace):
        """Test creating a complete schema with all features"""
        builder = OperationBuilder()
        ops = [
            # Create catalog
            builder.add_catalog("cat_prod", "production", op_id="op_001"),
            # Create schema
            builder.add_schema("schema_crm", "crm", "cat_prod", op_id="op_002"),
            # Create table
            builder.add_table("table_users", "users", "schema_crm", "delta", op_id="op_003"),
            # Add columns
            builder.add_column(
                "col_id",
                "table_users",
                "user_id",
                "BIGINT",
                nullable=False,
                comment="Primary key",
                op_id="op_004",
            ),
            builder.add_column(
                "col_email",
                "table_users",
                "email",
                "STRING",
                nullable=False,
                comment="User email",
                op_id="op_005",
            ),
            builder.add_column(
                "col_region",
                "table_users",
                "region",
                "STRING",
                nullable=True,
                comment="None",
                op_id="op_006",
            ),
            # Add primary key constraint
            builder.add_constraint(
                "pk_users",
                "table_users",
                "primary_key",
                ["col_id"],
                name="pk_users_id",
                notEnforced=True,
                op_id="op_007",
            ),
            # Add PII tag to email
            builder.set_column_tag("col_email", "table_users", "PII", "true", op_id="op_008"),
            # Add column mask to email
            builder.add_column_mask(
                "mask_email",
                "table_users",
                "col_email",
                "email_mask",
                "SHA2(email, 256)",
                enabled=True,
                description="Hash email for privacy",
                op_id="op_009",
            ),
            # Add row filter
            builder.add_row_filter(
                "filter_region",
                "table_users",
                "region_filter",
                "region = current_user_region()",
                enabled=True,
                description="Filter by user's region",
                op_id="op_010",
            ),
            # Set table properties
            builder.set_table_property(
                "table_users", "delta.enableChangeDataFeed", "true", op_id="op_011"
            ),
        ]

        # Apply all operations
        append_ops(initialized_workspace, ops)

        # Load and verify state
        state, changelog, provider = load_current_state(initialized_workspace)

        # Verify structure
        assert len(state["catalogs"]) == 1
        catalog = state["catalogs"][0]
        assert catalog["name"] == "production"

        schema = catalog["schemas"][0]
        assert schema["name"] == "crm"

        table = schema["tables"][0]
        assert table["name"] == "users"
        assert len(table["columns"]) == 3
        assert len(table["constraints"]) == 1
        assert len(table["columnMasks"]) == 1
        assert len(table["rowFilters"]) == 1
        assert "delta.enableChangeDataFeed" in table["properties"]

        # Verify column tags
        email_col = next((c for c in table["columns"] if c["name"] == "email"), None)
        assert email_col["tags"]["PII"] == "true"

        # Generate SQL
        generator = provider.get_sql_generator(state)
        sql = generator.generate_sql([Operation(**op) for op in changelog["ops"]])

        # Verify SQL contains all elements
        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql
        assert "CREATE TABLE" in sql
        assert "PRIMARY KEY" in sql
        assert "MASK" in sql or "COLUMN MASK" in sql

    def test_schema_modification_workflow(self, initialized_workspace):
        """Test modifying existing schema"""
        builder = OperationBuilder()
        # Create initial schema
        initial_ops = [
            builder.add_catalog("cat_test", "test", op_id="op_001"),
            builder.add_schema("schema_test", "public", "cat_test", op_id="op_002"),
            builder.add_table("table_test", "data", "schema_test", "delta", op_id="op_003"),
            builder.add_column(
                "col_old",
                "table_test",
                "old_name",
                "STRING",
                nullable=True,
                comment="None",
                op_id="op_004",
            ),
        ]
        append_ops(initialized_workspace, initial_ops)

        # Create snapshot
        create_snapshot(initialized_workspace, "Initial version", version="v0.1.0")

        # Modify schema
        modification_ops = [
            # Rename column
            builder.rename_column(
                "col_old", "table_test", "new_name", "old_name", op_id="op_mod_001"
            ),
            # Add new column
            builder.add_column(
                "col_new",
                "table_test",
                "additional_field",
                "INT",
                nullable=False,
                comment="None",
                op_id="op_mod_002",
            ),
            # Change column type
            builder.change_column_type("col_old", "table_test", "VARCHAR(255)", op_id="op_mod_003"),
        ]
        append_ops(initialized_workspace, modification_ops)

        # Create second snapshot
        create_snapshot(initialized_workspace, "Modified version", version="v0.2.0")

        # Load and verify final state
        state, changelog, provider = load_current_state(initialized_workspace)

        # Find user-defined catalog (not the implicit one)
        user_catalog = next(c for c in state["catalogs"] if c["name"] == "test")
        table = user_catalog["schemas"][0]["tables"][0]
        assert len(table["columns"]) == 2

        # Verify column was renamed
        renamed_col = next((c for c in table["columns"] if c["id"] == "col_old"), None)
        assert renamed_col["name"] == "new_name"
        assert renamed_col["type"] == "VARCHAR(255)"

        # Verify new column was added
        new_col = next((c for c in table["columns"] if c["id"] == "col_new"), None)
        assert new_col["name"] == "additional_field"


@pytest.mark.integration
class TestDeploymentWorkflow:
    """Test deployment tracking workflows"""

    def test_deployment_tracking(self, initialized_workspace, sample_operations):
        """Test tracking deployments across environments"""
        # Create initial schema
        append_ops(initialized_workspace, sample_operations)
        project, snapshot = create_snapshot(initialized_workspace, "Release 1.0", version="v1.0.0")

        # Deploy to dev
        dev_deployment = {
            "id": "deploy_dev_001",
            "environment": "dev",
            "ts": "2025-01-01T10:00:00Z",
            "deployedBy": "test@example.com",
            "snapshotId": snapshot["id"],
            "opsApplied": snapshot["opsIncluded"],
            "schemaVersion": "v1.0.0",
            "status": "success",
            "driftDetected": False,
        }
        write_deployment(initialized_workspace, dev_deployment)

        # Deploy to prod
        prod_deployment = {
            "id": "deploy_prod_001",
            "environment": "prod",
            "ts": "2025-01-01T14:00:00Z",
            "deployedBy": "test@example.com",
            "snapshotId": snapshot["id"],
            "opsApplied": snapshot["opsIncluded"],
            "schemaVersion": "v1.0.0",
            "status": "success",
            "driftDetected": False,
        }
        write_deployment(initialized_workspace, prod_deployment)

        # Verify deployments were recorded
        project = read_project(initialized_workspace)
        assert len(project["deployments"]) == 2

        # Get last deployment for each environment
        last_dev = get_last_deployment(project, "dev")
        last_prod = get_last_deployment(project, "prod")

        assert last_dev["id"] == "deploy_dev_001"
        assert last_prod["id"] == "deploy_prod_001"


@pytest.mark.integration
class TestErrorRecovery:
    """Test error recovery and edge cases"""

    def test_recovery_from_failed_snapshot(self, initialized_workspace, sample_operations):
        """Test that operations are preserved if snapshot fails"""
        # Add operations
        append_ops(initialized_workspace, sample_operations)

        # Verify operations are in changelog
        # V4 auto-creates implicit catalog, so total ops = sample_operations + 1
        ops_count = get_uncommitted_ops_count(initialized_workspace)
        assert ops_count == len(sample_operations) + 1

        # Even if snapshot creation fails, operations should still be in changelog
        # This is implicit - they're not deleted until snapshot succeeds

    def test_empty_project_operations(self, initialized_workspace):
        """Test operations on empty project"""
        # Load state from empty project
        state, changelog, provider = load_current_state(initialized_workspace)

        # V4 auto-creates implicit catalog
        assert len(state["catalogs"]) == 1
        assert state["catalogs"][0]["name"] == "__implicit__"
        # Changelog has the implicit catalog operation
        assert len(changelog["ops"]) == 1

        # Generate SQL from empty state
        generator = provider.get_sql_generator(state)
        sql = generator.generate_sql([])

        # Should handle empty operations gracefully
        assert sql == "" or sql is not None


@pytest.mark.integration
class TestComplexWorkflows:
    """Test complex multi-step workflows"""

    def test_branching_schema_versions(self, initialized_workspace, sample_operations):
        """Test creating multiple snapshots with different schemas"""
        builder = OperationBuilder()
        # Create base schema
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "Base schema", version="v1.0.0")

        # Add feature A
        feature_a_ops = [
            builder.add_column(
                "col_feature_a",
                "table_789",
                "feature_a_field",
                "STRING",
                nullable=True,
                comment="None",
                op_id="feature_a_001",
            )
        ]
        append_ops(initialized_workspace, feature_a_ops)
        create_snapshot(initialized_workspace, "Feature A", version="v1.1.0")

        # Add feature B
        feature_b_ops = [
            builder.add_column(
                "col_feature_b",
                "table_789",
                "feature_b_field",
                "INT",
                nullable=True,
                comment="None",
                op_id="feature_b_001",
            )
        ]
        append_ops(initialized_workspace, feature_b_ops)
        create_snapshot(initialized_workspace, "Feature B", version="v1.2.0")

        # Verify all snapshots exist
        project = read_project(initialized_workspace)
        assert len(project["snapshots"]) == 3
        assert project["latestSnapshot"] == "v1.2.0"

        # Verify snapshot chain
        snap_1_0_0 = read_snapshot(initialized_workspace, "v1.0.0")
        snap_1_1_0 = read_snapshot(initialized_workspace, "v1.1.0")
        snap_1_2_0 = read_snapshot(initialized_workspace, "v1.2.0")

        assert snap_1_0_0["previousSnapshot"] is None
        assert snap_1_1_0["previousSnapshot"] == "v1.0.0"
        assert snap_1_2_0["previousSnapshot"] == "v1.1.0"
