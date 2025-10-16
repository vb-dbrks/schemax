"""
Unit tests for storage_v3.py module

Tests all storage layer functionality including:
- Project initialization
- File I/O operations
- Snapshot creation
- State loading
- Migration from v2 to v3
"""

import json
from datetime import datetime

import pytest

from schematic.providers.base.operations import Operation
from schematic.storage_v3 import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_changelog_file_path,
    get_last_deployment,
    get_project_file_path,
    get_schematic_dir,
    get_snapshot_file_path,
    get_snapshots_dir,
    get_uncommitted_ops_count,
    load_current_state,
    migrate_v2_to_v3,
    read_changelog,
    read_project,
    read_snapshot,
    write_changelog,
    write_deployment,
    write_project,
    write_snapshot,
)
from tests.utils import OperationBuilder


class TestPathHelpers:
    """Test path helper functions"""

    def test_get_schematic_dir(self, temp_workspace):
        """Test getting .schematic directory path"""
        schematic_dir = get_schematic_dir(temp_workspace)
        assert schematic_dir == temp_workspace / ".schematic"

    def test_get_project_file_path(self, temp_workspace):
        """Test getting project file path"""
        project_path = get_project_file_path(temp_workspace)
        assert project_path == temp_workspace / ".schematic" / "project.json"

    def test_get_changelog_file_path(self, temp_workspace):
        """Test getting changelog file path"""
        changelog_path = get_changelog_file_path(temp_workspace)
        assert changelog_path == temp_workspace / ".schematic" / "changelog.json"

    def test_get_snapshots_dir(self, temp_workspace):
        """Test getting snapshots directory path"""
        snapshots_dir = get_snapshots_dir(temp_workspace)
        assert snapshots_dir == temp_workspace / ".schematic" / "snapshots"

    def test_get_snapshot_file_path(self, temp_workspace):
        """Test getting snapshot file path"""
        snapshot_path = get_snapshot_file_path(temp_workspace, "v0.1.0")
        assert snapshot_path == temp_workspace / ".schematic" / "snapshots" / "v0.1.0.json"


class TestProjectInitialization:
    """Test project initialization"""

    def test_ensure_project_file_creates_new_project(self, temp_workspace):
        """Test creating a new project initializes all files"""
        ensure_project_file(temp_workspace, provider_id="unity")

        # Verify .schematic directory exists
        assert get_schematic_dir(temp_workspace).exists()
        assert get_snapshots_dir(temp_workspace).exists()

        # Verify project file exists
        project_file = get_project_file_path(temp_workspace)
        assert project_file.exists()

        # Verify project structure
        project = read_project(temp_workspace)
        assert project["version"] == 3
        assert project["name"] == temp_workspace.name
        assert project["provider"]["type"] == "unity"
        assert project["latestSnapshot"] is None
        assert project["snapshots"] == []
        assert project["deployments"] == []
        assert "settings" in project

        # Verify changelog file exists
        changelog_file = get_changelog_file_path(temp_workspace)
        assert changelog_file.exists()

        # Verify changelog structure
        changelog = read_changelog(temp_workspace)
        assert changelog["version"] == 1
        assert changelog["sinceSnapshot"] is None
        assert changelog["ops"] == []

    def test_ensure_project_file_idempotent(self, temp_workspace):
        """Test that calling ensure_project_file twice doesn't fail"""
        ensure_project_file(temp_workspace, provider_id="unity")
        ensure_project_file(temp_workspace, provider_id="unity")

        # Should not raise error and project should still be valid
        project = read_project(temp_workspace)
        assert project["version"] == 3

    def test_ensure_project_file_invalid_provider(self, temp_workspace):
        """Test that invalid provider raises ValueError"""
        with pytest.raises(ValueError, match="Provider 'invalid' not found"):
            ensure_project_file(temp_workspace, provider_id="invalid")


class TestFileOperations:
    """Test file read/write operations"""

    def test_read_write_project(self, temp_workspace, sample_project_v3):
        """Test reading and writing project file"""
        # Create .schematic directory
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)

        # Write project
        write_project(temp_workspace, sample_project_v3)

        # Read project
        project = read_project(temp_workspace)
        assert project["version"] == 3
        assert project["name"] == "test_project"
        assert project["provider"]["type"] == "unity"

    def test_read_write_changelog(self, temp_workspace, sample_changelog):
        """Test reading and writing changelog file"""
        # Create .schematic directory
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)

        # Write changelog
        write_changelog(temp_workspace, sample_changelog)

        # Read changelog
        changelog = read_changelog(temp_workspace)
        assert changelog["version"] == 1
        assert changelog["ops"] == []
        assert "lastModified" in changelog

    def test_read_changelog_creates_if_missing(self, temp_workspace):
        """Test that reading non-existent changelog creates it"""
        # Create .schematic directory
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)

        # Read changelog (should create it)
        changelog = read_changelog(temp_workspace)
        assert changelog["version"] == 1
        assert changelog["ops"] == []
        assert get_changelog_file_path(temp_workspace).exists()

    def test_read_write_snapshot(self, temp_workspace, sample_unity_state):
        """Test reading and writing snapshot file"""
        # Create .schematic directory
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)
        get_snapshots_dir(temp_workspace).mkdir(parents=True, exist_ok=True)

        # Create snapshot
        snapshot = {
            "id": "snap_001",
            "version": "v0.1.0",
            "name": "Initial snapshot",
            "ts": datetime.utcnow().isoformat() + "Z",
            "state": sample_unity_state.model_dump(by_alias=True),
            "opsIncluded": ["op_001", "op_002"],
            "previousSnapshot": None,
            "hash": "abc123",
            "tags": ["initial"],
            "comment": "First snapshot",
        }

        # Write snapshot
        write_snapshot(temp_workspace, snapshot)

        # Read snapshot
        read_snapshot_data = read_snapshot(temp_workspace, "v0.1.0")
        assert read_snapshot_data["id"] == "snap_001"
        assert read_snapshot_data["version"] == "v0.1.0"
        assert read_snapshot_data["name"] == "Initial snapshot"


class TestOperationManagement:
    """Test operation management"""

    def test_append_ops_to_changelog(self, initialized_workspace, sample_catalog_op):
        """Test appending operations to changelog"""
        # Append operation
        append_ops(initialized_workspace, [sample_catalog_op])

        # Verify operation was added
        changelog = read_changelog(initialized_workspace)
        assert len(changelog["ops"]) == 1
        assert changelog["ops"][0]["id"] == "op_001"
        assert changelog["ops"][0]["op"] == "unity.add_catalog"

    def test_append_multiple_ops(self, initialized_workspace, sample_operations):
        """Test appending multiple operations"""
        append_ops(initialized_workspace, sample_operations)

        changelog = read_changelog(initialized_workspace)
        assert len(changelog["ops"]) == len(sample_operations)

    def test_append_ops_validates_operations(self, initialized_workspace):
        """Test that append_ops validates operations"""
        OperationBuilder()
        # Create an invalid operation (missing required payload fields)
        invalid_op = Operation(
            id="op_invalid",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_catalog",
            target="cat_999",
            payload={},  # Missing catalogId and name
        )

        # Should raise ValueError for validation failure
        with pytest.raises(ValueError, match="Invalid operation"):
            append_ops(initialized_workspace, [invalid_op])

    def test_get_uncommitted_ops_count(self, workspace_with_operations):
        """Test getting count of uncommitted operations"""
        count = get_uncommitted_ops_count(workspace_with_operations)
        assert count == 4  # sample_operations has 4 ops


class TestStateLoading:
    """Test state loading functionality"""

    def test_load_current_state_empty(self, initialized_workspace):
        """Test loading current state with no snapshots or operations"""
        state, changelog, provider = load_current_state(initialized_workspace)

        # State should be empty
        assert state["catalogs"] == []

        # Changelog should be empty
        assert len(changelog["ops"]) == 0

        # Provider should be Unity
        assert provider.info.id == "unity"

    def test_load_current_state_with_operations(self, workspace_with_operations):
        """Test loading current state with operations applied"""
        state, changelog, provider = load_current_state(workspace_with_operations)

        # State should have data from applied operations
        assert len(state["catalogs"]) == 1
        assert state["catalogs"][0]["name"] == "bronze"
        assert len(state["catalogs"][0]["schemas"]) == 1
        assert state["catalogs"][0]["schemas"][0]["name"] == "raw"
        assert len(state["catalogs"][0]["schemas"][0]["tables"]) == 1

        # Changelog should have all operations
        assert len(changelog["ops"]) == 4

    def test_load_current_state_with_snapshot(self, initialized_workspace, sample_operations):
        """Test loading current state from snapshot"""
        # Add operations and create snapshot
        append_ops(initialized_workspace, sample_operations[:2])  # Add 2 operations
        create_snapshot(initialized_workspace, "First snapshot", version="v0.1.0")

        # Add more operations after snapshot
        append_ops(initialized_workspace, sample_operations[2:])  # Add remaining operations

        # Load current state
        state, changelog, provider = load_current_state(initialized_workspace)

        # State should have all operations applied (snapshot + changelog)
        assert len(state["catalogs"]) == 1
        assert len(state["catalogs"][0]["schemas"]) == 1
        assert len(state["catalogs"][0]["schemas"][0]["tables"]) == 1

        # Changelog should only have operations since snapshot
        assert len(changelog["ops"]) == 2


class TestSnapshotCreation:
    """Test snapshot creation"""

    def test_create_snapshot_basic(self, workspace_with_operations):
        """Test creating a basic snapshot"""
        project, snapshot = create_snapshot(
            workspace_with_operations, name="Test snapshot", version="v0.1.0"
        )

        # Verify snapshot was created
        assert project["latestSnapshot"] == "v0.1.0"
        assert len(project["snapshots"]) == 1
        assert project["snapshots"][0]["version"] == "v0.1.0"
        assert project["snapshots"][0]["name"] == "Test snapshot"

        # Verify snapshot file exists
        snapshot_file = get_snapshot_file_path(workspace_with_operations, "v0.1.0")
        assert snapshot_file.exists()

        # Verify changelog was cleared
        changelog = read_changelog(workspace_with_operations)
        assert len(changelog["ops"]) == 0
        assert changelog["sinceSnapshot"] == "v0.1.0"

    def test_create_snapshot_with_comment_and_tags(self, workspace_with_operations):
        """Test creating snapshot with comment and tags"""
        project, snapshot = create_snapshot(
            workspace_with_operations,
            name="Tagged snapshot",
            version="v0.1.0",
            comment="This is a test snapshot",
            tags=["test", "initial"],
        )

        assert snapshot["comment"] == "This is a test snapshot"
        assert snapshot["tags"] == ["test", "initial"]

    def test_create_snapshot_auto_version(self, workspace_with_operations):
        """Test creating snapshot with auto-generated version"""
        project, snapshot = create_snapshot(workspace_with_operations, name="Auto version")

        # Should auto-generate v0.1.0 for first snapshot
        assert project["latestSnapshot"] == "v0.1.0"

    def test_create_snapshot_sequential_versions(
        self, workspace_with_operations, sample_operations
    ):
        """Test creating multiple snapshots with auto-incrementing versions"""
        # Create first snapshot
        create_snapshot(workspace_with_operations, name="First")
        project = read_project(workspace_with_operations)
        assert project["latestSnapshot"] == "v0.1.0"

        # Add more operations and create second snapshot
        append_ops(workspace_with_operations, sample_operations)
        create_snapshot(workspace_with_operations, name="Second")
        project = read_project(workspace_with_operations)
        assert project["latestSnapshot"] == "v0.2.0"


class TestMigration:
    """Test v2 to v3 migration"""

    def test_migrate_v2_to_v3(self, temp_workspace):
        """Test migrating v2 project to v3"""
        # Create v2 project
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)
        v2_project = {
            "version": 2,
            "name": "old_project",
            "environments": ["dev", "prod"],
            "snapshots": [],
            "deployments": [],
            "settings": {
                "autoIncrementVersion": True,
                "versionPrefix": "v",
            },
            "latestSnapshot": None,
        }

        # Write v2 project
        with open(get_project_file_path(temp_workspace), "w") as f:
            json.dump(v2_project, f)

        # Create v2 changelog with non-prefixed operations
        v2_changelog = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": [
                {
                    "id": "op_001",
                    "ts": "2025-01-01T00:00:00Z",
                    "op": "add_catalog",  # No provider prefix
                    "target": "cat_123",
                    "payload": {"catalogId": "cat_123", "name": "bronze"},
                }
            ],
            "lastModified": datetime.utcnow().isoformat() + "Z",
        }

        with open(get_changelog_file_path(temp_workspace), "w") as f:
            json.dump(v2_changelog, f)

        # Migrate
        migrate_v2_to_v3(temp_workspace, v2_project, provider_id="unity")

        # Verify migration
        project = read_project(temp_workspace)
        assert project["version"] == 3
        assert project["provider"]["type"] == "unity"
        assert project["name"] == "old_project"

        # Verify operations were prefixed
        changelog = read_changelog(temp_workspace)
        assert changelog["ops"][0]["op"] == "unity.add_catalog"

    def test_read_project_auto_migrates(self, temp_workspace):
        """Test that read_project auto-migrates v2 projects"""
        # Create v2 project
        get_schematic_dir(temp_workspace).mkdir(parents=True, exist_ok=True)
        v2_project = {
            "version": 2,
            "name": "auto_migrate",
            "environments": ["dev"],
            "snapshots": [],
            "deployments": [],
            "settings": {},
            "latestSnapshot": None,
        }

        with open(get_project_file_path(temp_workspace), "w") as f:
            json.dump(v2_project, f)

        # Create empty changelog
        with open(get_changelog_file_path(temp_workspace), "w") as f:
            json.dump(
                {
                    "version": 1,
                    "sinceSnapshot": None,
                    "ops": [],
                    "lastModified": datetime.utcnow().isoformat() + "Z",
                },
                f,
            )

        # Read project (should auto-migrate)
        project = read_project(temp_workspace)
        assert project["version"] == 3
        assert project["provider"]["type"] == "unity"


class TestDeploymentTracking:
    """Test deployment tracking functionality"""

    def test_write_deployment(self, initialized_workspace):
        """Test writing deployment record"""
        deployment = {
            "id": "deploy_001",
            "environment": "dev",
            "ts": datetime.utcnow().isoformat() + "Z",
            "deployedBy": "user@example.com",
            "snapshotId": "snap_001",
            "opsApplied": ["op_001", "op_002"],
            "schemaVersion": "v0.1.0",
            "status": "success",
            "driftDetected": False,
        }

        write_deployment(initialized_workspace, deployment)

        # Verify deployment was added
        project = read_project(initialized_workspace)
        assert len(project["deployments"]) == 1
        assert project["deployments"][0]["id"] == "deploy_001"

    def test_get_last_deployment(self, initialized_workspace):
        """Test getting last deployment for environment"""
        # Add multiple deployments
        deployments = [
            {
                "id": "deploy_001",
                "environment": "dev",
                "ts": "2025-01-01T00:00:00Z",
                "status": "success",
            },
            {
                "id": "deploy_002",
                "environment": "prod",
                "ts": "2025-01-01T01:00:00Z",
                "status": "success",
            },
            {
                "id": "deploy_003",
                "environment": "dev",
                "ts": "2025-01-01T02:00:00Z",
                "status": "success",
            },
        ]

        for deployment in deployments:
            write_deployment(initialized_workspace, deployment)

        project = read_project(initialized_workspace)

        # Get last deployment for dev
        last_dev = get_last_deployment(project, "dev")
        assert last_dev["id"] == "deploy_003"  # Most recent

        # Get last deployment for prod
        last_prod = get_last_deployment(project, "prod")
        assert last_prod["id"] == "deploy_002"

        # Get last deployment for non-existent environment
        last_test = get_last_deployment(project, "test")
        assert last_test is None
