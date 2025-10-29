"""
Unit tests for storage_v4 module (v4 project schema with multi-environment support)
"""

import json
from datetime import datetime

import pytest

from schematic import storage_v4


class TestPathHelpers:
    """Test path helper functions"""

    def test_get_schematic_dir(self, tmp_path):
        expected = tmp_path / ".schematic"
        assert storage_v4.get_schematic_dir(tmp_path) == expected

    def test_get_project_file_path(self, tmp_path):
        expected = tmp_path / ".schematic" / "project.json"
        assert storage_v4.get_project_file_path(tmp_path) == expected

    def test_get_changelog_file_path(self, tmp_path):
        expected = tmp_path / ".schematic" / "changelog.json"
        assert storage_v4.get_changelog_file_path(tmp_path) == expected

    def test_get_snapshots_dir(self, tmp_path):
        expected = tmp_path / ".schematic" / "snapshots"
        assert storage_v4.get_snapshots_dir(tmp_path) == expected

    def test_get_snapshot_file_path(self, tmp_path):
        expected = tmp_path / ".schematic" / "snapshots" / "v1.0.0.json"
        assert storage_v4.get_snapshot_file_path(tmp_path, "v1.0.0") == expected


class TestProjectInitialization:
    """Test project initialization with v4 schema"""

    def test_ensure_project_file_creates_new_v4_project(self, tmp_path):
        """Should create new v4 project with default environments"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        project_file = tmp_path / ".schematic" / "project.json"
        assert project_file.exists()

        with open(project_file) as f:
            project = json.load(f)

        # Check v4 schema
        assert project["version"] == 4
        assert "provider" in project
        assert "environments" in project["provider"]

        # Check default environments
        envs = project["provider"]["environments"]
        assert "dev" in envs
        assert "test" in envs
        assert "prod" in envs

        # Check environment structure
        assert envs["dev"]["catalog"].startswith("dev_")
        assert envs["dev"]["allowDrift"] is True
        assert envs["dev"]["requireSnapshot"] is False
        assert envs["dev"]["autoCreateCatalog"] is True

        # Check catalogMode setting
        assert project["settings"]["catalogMode"] == "single"

    def test_ensure_project_file_creates_implicit_catalog(self, tmp_path):
        """Should create implicit catalog operation in changelog"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        changelog_file = tmp_path / ".schematic" / "changelog.json"
        assert changelog_file.exists()

        with open(changelog_file) as f:
            changelog = json.load(f)

        # Check implicit catalog operation
        assert len(changelog["ops"]) == 1
        op = changelog["ops"][0]
        assert op["op"] == "unity.add_catalog"
        assert op["payload"]["name"] == "__implicit__"
        assert op["payload"]["catalogId"] == "cat_implicit"

    def test_ensure_project_file_idempotent(self, tmp_path):
        """Should not recreate project if it already exists"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        project_file = tmp_path / ".schematic" / "project.json"
        mtime_before = project_file.stat().st_mtime

        # Call again
        storage_v4.ensure_project_file(tmp_path, "unity")

        mtime_after = project_file.stat().st_mtime
        assert mtime_before == mtime_after

    def test_ensure_project_file_invalid_provider(self, tmp_path):
        """Should raise error for invalid provider"""
        with pytest.raises(ValueError, match="Provider.*not found"):
            storage_v4.ensure_project_file(tmp_path, "invalid_provider")


class TestEnvironmentConfig:
    """Test environment configuration helpers"""

    def test_get_environment_config(self, tmp_path):
        """Should retrieve environment configuration"""
        storage_v4.ensure_project_file(tmp_path, "unity")
        project = storage_v4.read_project(tmp_path)

        dev_config = storage_v4.get_environment_config(project, "dev")

        assert "catalog" in dev_config
        assert dev_config["catalog"].startswith("dev_")
        assert dev_config["allowDrift"] is True
        assert dev_config["requireSnapshot"] is False

    def test_get_environment_config_not_found(self, tmp_path):
        """Should raise error for non-existent environment"""
        storage_v4.ensure_project_file(tmp_path, "unity")
        project = storage_v4.read_project(tmp_path)

        with pytest.raises(ValueError, match="Environment 'invalid' not found"):
            storage_v4.get_environment_config(project, "invalid")


class TestFileOperations:
    """Test file read/write operations"""

    def test_read_write_project(self, tmp_path):
        """Should read and write project file"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        # Read project
        project = storage_v4.read_project(tmp_path)
        assert project["version"] == 4
        assert "provider" in project

        # Modify and write
        project["name"] = "test_modified"
        storage_v4.write_project(tmp_path, project)

        # Read again
        project_reloaded = storage_v4.read_project(tmp_path)
        assert project_reloaded["name"] == "test_modified"

    def test_read_write_changelog(self, tmp_path):
        """Should read and write changelog file"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        # Read changelog
        changelog = storage_v4.read_changelog(tmp_path)
        assert changelog["version"] == 1
        assert "ops" in changelog

        # Modify and write
        changelog["ops"].append(
            {
                "id": "op_test",
                "ts": datetime.now().isoformat() + "Z",
                "provider": "unity",
                "op": "unity.add_schema",
                "target": "schema_test",
                "payload": {
                    "schemaId": "schema_test",
                    "name": "test_schema",
                    "catalogId": "cat_implicit",
                },
            }
        )
        storage_v4.write_changelog(tmp_path, changelog)

        # Read again
        changelog_reloaded = storage_v4.read_changelog(tmp_path)
        assert len(changelog_reloaded["ops"]) == 2  # 1 implicit catalog + 1 schema


class TestStateLoading:
    """Test state loading with v4 schema"""

    def test_load_current_state_empty(self, tmp_path):
        """Should load empty state with implicit catalog"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        state, changelog, provider = storage_v4.load_current_state(tmp_path)

        # Should have implicit catalog
        assert len(state["catalogs"]) == 1
        assert state["catalogs"][0]["name"] == "__implicit__"
        assert len(state["catalogs"][0]["schemas"]) == 0

    def test_load_current_state_with_operations(self, tmp_path):
        """Should apply operations to build current state"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        # Add schema operation
        from schematic.providers.base.operations import Operation

        ops = [
            Operation(
                id="op_schema",
                ts=datetime.now().isoformat() + "Z",
                provider="unity",
                op="unity.add_schema",
                target="schema_1",
                payload={
                    "schemaId": "schema_1",
                    "name": "test_schema",
                    "catalogId": "cat_implicit",
                },
            )
        ]
        storage_v4.append_ops(tmp_path, ops)

        state, changelog, provider = storage_v4.load_current_state(tmp_path)

        # Should have implicit catalog with schema
        assert len(state["catalogs"]) == 1
        assert len(state["catalogs"][0]["schemas"]) == 1
        assert state["catalogs"][0]["schemas"][0]["name"] == "test_schema"


class TestSnapshotCreation:
    """Test snapshot creation with v4 schema"""

    def test_create_snapshot_basic(self, tmp_path):
        """Should create snapshot with v4 project"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        # Add some operations
        from schematic.providers.base.operations import Operation

        ops = [
            Operation(
                id="op_schema",
                ts=datetime.now().isoformat() + "Z",
                provider="unity",
                op="unity.add_schema",
                target="schema_1",
                payload={
                    "schemaId": "schema_1",
                    "name": "test_schema",
                    "catalogId": "cat_implicit",
                },
            )
        ]
        storage_v4.append_ops(tmp_path, ops)

        # Create snapshot (returns project, snapshot tuple)
        project, snapshot = storage_v4.create_snapshot(
            tmp_path, name="Test Snapshot", comment="Initial version", tags=["test"]
        )

        # Check snapshot created (version starts with "v")
        snapshot_version = project["latestSnapshot"]
        assert snapshot_version.startswith("v")

        snapshot_file = tmp_path / ".schematic" / "snapshots" / f"{snapshot_version}.json"
        assert snapshot_file.exists()

        # Read and check snapshot content from file
        saved_snapshot = storage_v4.read_snapshot(tmp_path, snapshot_version)
        assert saved_snapshot["name"] == "Test Snapshot"
        assert saved_snapshot["comment"] == "Initial version"
        assert "test" in saved_snapshot["tags"]
        assert len(saved_snapshot["state"]["catalogs"]) == 1
        assert len(saved_snapshot["state"]["catalogs"][0]["schemas"]) == 1

        # Check changelog cleared
        changelog = storage_v4.read_changelog(tmp_path)
        assert len(changelog["ops"]) == 0
        assert changelog["sinceSnapshot"] == snapshot_version


class TestDeploymentTracking:
    """Test deployment tracking in v4 project"""

    def test_write_deployment(self, tmp_path):
        """Should write deployment record to project"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        deployment_record = {
            "id": "deployment_1",
            "environment": "dev",
            "version": "v1.0.0",
            "ts": datetime.now().isoformat() + "Z",
            "status": "success",
        }

        storage_v4.write_deployment(tmp_path, deployment_record)

        project = storage_v4.read_project(tmp_path)
        assert len(project["deployments"]) == 1
        assert project["deployments"][0]["environment"] == "dev"

    def test_get_last_deployment(self, tmp_path):
        """Should get last deployment for environment"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        # Write two deployments
        storage_v4.write_deployment(
            tmp_path,
            {
                "id": "deployment_1",
                "environment": "dev",
                "version": "v1.0.0",
                "ts": "2025-01-01T00:00:00Z",
                "status": "success",
            },
        )
        storage_v4.write_deployment(
            tmp_path,
            {
                "id": "deployment_2",
                "environment": "dev",
                "version": "v1.1.0",
                "ts": "2025-01-02T00:00:00Z",
                "status": "success",
            },
        )

        project = storage_v4.read_project(tmp_path)
        last_deployment = storage_v4.get_last_deployment(project, "dev")

        assert last_deployment is not None
        assert last_deployment["id"] == "deployment_2"
        assert last_deployment["version"] == "v1.1.0"

    def test_get_last_deployment_none(self, tmp_path):
        """Should return None if no deployments"""
        storage_v4.ensure_project_file(tmp_path, "unity")

        project = storage_v4.read_project(tmp_path)
        last_deployment = storage_v4.get_last_deployment(project, "dev")
        assert last_deployment is None
