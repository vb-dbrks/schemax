"""
Unit tests for v5 multi-target project schema and v4 → v5 migration.
"""

import json

import pytest

from schemax.core import storage
from schemax.providers.base.operations import Operation, create_operation


class TestV4ToV5Migration:
    """Test transparent v4 → v5 auto-migration."""

    def _write_v4_project(self, tmp_path, project_data=None):
        """Write a v4 project file directly for migration testing."""
        storage.ensure_schemax_dir(tmp_path)
        if project_data is None:
            project_data = {
                "version": 4,
                "name": "test_workspace",
                "provider": {
                    "type": "unity",
                    "version": "1.0.0",
                    "environments": {
                        "dev": {
                            "topLevelName": "dev_test",
                            "allowDrift": True,
                            "requireSnapshot": False,
                            "autoCreateTopLevel": True,
                            "autoCreateSchemaxSchema": True,
                            "catalogMappings": {"analytics": "dev_analytics"},
                        },
                        "prod": {
                            "topLevelName": "prod_test",
                            "allowDrift": False,
                            "requireSnapshot": True,
                            "autoCreateTopLevel": False,
                            "autoCreateSchemaxSchema": True,
                            "catalogMappings": {"analytics": "prod_analytics"},
                        },
                    },
                },
                **storage.default_project_skeleton_tail(),
            }
        project_path = storage.get_project_file_path(tmp_path)
        with open(project_path, "w", encoding="utf-8") as f:
            json.dump(project_data, f, indent=2)
        # Also write an empty changelog
        changelog_path = storage.get_changelog_file_path(tmp_path)
        changelog = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": [],
            "lastModified": "2024-01-01T00:00:00Z",
        }
        with open(changelog_path, "w", encoding="utf-8") as f:
            json.dump(changelog, f, indent=2)

    def test_read_project_auto_migrates_v4_to_v5(self, tmp_path):
        """read_project should transparently migrate v4 → v5."""
        self._write_v4_project(tmp_path)
        project = storage.read_project(tmp_path)

        assert project["version"] == 5
        assert "targets" in project
        assert "defaultTarget" in project
        assert project["defaultTarget"] == "default"
        assert "provider" not in project

        # Target should contain the old provider config
        target = project["targets"]["default"]
        assert target["type"] == "unity"
        assert target["version"] == "1.0.0"
        assert "dev" in target["environments"]
        assert "prod" in target["environments"]

    def test_v4_migration_persists_to_disk(self, tmp_path):
        """After migration, the file on disk should be v5."""
        self._write_v4_project(tmp_path)
        storage.read_project(tmp_path)

        # Read raw file
        project_path = storage.get_project_file_path(tmp_path)
        with open(project_path, encoding="utf-8") as f:
            raw = json.load(f)

        assert raw["version"] == 5
        assert "targets" in raw
        assert "provider" not in raw

    def test_v4_migration_preserves_environment_config(self, tmp_path):
        """Environment configs should be intact after migration."""
        self._write_v4_project(tmp_path)
        project = storage.read_project(tmp_path)

        dev_config = storage.get_environment_config(project, "dev")
        assert dev_config["topLevelName"] == "dev_test"
        assert dev_config["allowDrift"] is True
        assert dev_config["catalogMappings"] == {"analytics": "dev_analytics"}

        prod_config = storage.get_environment_config(project, "prod")
        assert prod_config["topLevelName"] == "prod_test"
        assert prod_config["requireSnapshot"] is True

    def test_v4_migration_preserves_catalog_mappings(self, tmp_path):
        """Catalog mappings should survive migration."""
        self._write_v4_project(tmp_path)
        project = storage.read_project(tmp_path)
        target = storage.get_target_config(project)
        dev_env = target["environments"]["dev"]
        assert dev_env["catalogMappings"] == {"analytics": "dev_analytics"}

    def test_v5_project_reads_without_migration(self, tmp_path):
        """A native v5 project should load directly without changes."""
        storage.ensure_project_file(tmp_path, "unity")
        project = storage.read_project(tmp_path)
        assert project["version"] == 5
        assert "targets" in project

    def test_ensure_project_file_creates_v5(self, tmp_path):
        """New projects should be v5."""
        storage.ensure_project_file(tmp_path, "unity")

        project_path = storage.get_project_file_path(tmp_path)
        with open(project_path, encoding="utf-8") as f:
            raw = json.load(f)

        assert raw["version"] == 5
        assert "targets" in raw
        assert raw["defaultTarget"] == "default"
        assert "default" in raw["targets"]
        assert raw["targets"]["default"]["type"] == "unity"

    def test_ensure_project_file_auto_migrates_existing_v4(self, tmp_path):
        """ensure_project_file should auto-migrate existing v4 projects."""
        self._write_v4_project(tmp_path)
        storage.ensure_project_file(tmp_path, "unity")

        project = storage.read_project(tmp_path)
        assert project["version"] == 5

    def test_unsupported_version_raises(self, tmp_path):
        """Should raise ValueError for unsupported versions."""
        storage.ensure_schemax_dir(tmp_path)
        project_path = storage.get_project_file_path(tmp_path)
        with open(project_path, "w", encoding="utf-8") as f:
            json.dump({"version": 3, "name": "old"}, f)

        with pytest.raises(ValueError, match="not supported"):
            storage.read_project(tmp_path)


class TestV5TargetConfig:
    """Test target configuration helpers for v5 projects."""

    def test_get_target_config_default(self, tmp_path):
        """get_target_config should return default target when name is None."""
        storage.ensure_project_file(tmp_path, "unity")
        project = storage.read_project(tmp_path)

        target = storage.get_target_config(project)
        assert target["type"] == "unity"
        assert "environments" in target

    def test_get_target_config_by_name(self, tmp_path):
        """get_target_config should return named target."""
        storage.ensure_project_file(tmp_path, "unity")
        project = storage.read_project(tmp_path)

        # Add a second target
        project["targets"]["analytics"] = {
            "type": "unity",
            "version": "1.0.0",
            "environments": {
                "dev": {
                    "topLevelName": "dev_analytics",
                    "allowDrift": True,
                    "requireSnapshot": False,
                    "autoCreateTopLevel": True,
                    "autoCreateSchemaxSchema": True,
                    "catalogMappings": {},
                },
            },
        }
        storage.write_project(tmp_path, project)
        project = storage.read_project(tmp_path)

        target = storage.get_target_config(project, "analytics")
        assert target["environments"]["dev"]["topLevelName"] == "dev_analytics"

    def test_get_target_config_not_found(self, tmp_path):
        """get_target_config should raise ValueError for missing target."""
        storage.ensure_project_file(tmp_path, "unity")
        project = storage.read_project(tmp_path)

        with pytest.raises(ValueError, match="Target 'nonexistent' not found"):
            storage.get_target_config(project, "nonexistent")

    def test_get_environment_config_with_target(self, tmp_path):
        """get_environment_config should accept scope parameter."""
        storage.ensure_project_file(tmp_path, "unity")
        project = storage.read_project(tmp_path)

        # Default target
        dev_config = storage.get_environment_config(project, "dev")
        assert "topLevelName" in dev_config

        # Explicit target name
        dev_config2 = storage.get_environment_config(project, "dev", scope="default")
        assert dev_config2["topLevelName"] == dev_config["topLevelName"]


class TestV5OperationScope:
    """Test scope field on Operation."""

    def test_operation_scope_default_none(self):
        """Operation scope should default to None."""
        op = Operation(
            id="op_1",
            ts="2024-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"name": "test"},
        )
        assert op.scope is None

    def test_operation_scope_set(self):
        """Operation scope should be settable."""
        op = Operation(
            id="op_1",
            ts="2024-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"name": "test"},
            scope="analytics",
        )
        assert op.scope == "analytics"

    def test_create_operation_with_scope(self):
        """create_operation should accept scope."""
        op = create_operation(
            provider="unity",
            op_type="add_catalog",
            target="cat_1",
            payload={"name": "test"},
            scope="raw",
        )
        assert op.scope == "raw"

    def test_create_operation_without_scope(self):
        """create_operation without scope should default to None."""
        op = create_operation(
            provider="unity",
            op_type="add_catalog",
            target="cat_1",
            payload={"name": "test"},
        )
        assert op.scope is None

    def test_operation_serialization_includes_scope(self):
        """scope should survive serialization round-trip."""
        op = Operation(
            id="op_1",
            ts="2024-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"name": "test"},
            scope="analytics",
        )
        dumped = op.model_dump(by_alias=True)
        assert dumped["scope"] == "analytics"

        restored = Operation(**dumped)
        assert restored.scope == "analytics"

    def test_operation_serialization_null_scope(self):
        """Null scope should round-trip cleanly."""
        op = Operation(
            id="op_1",
            ts="2024-01-01T00:00:00Z",
            provider="unity",
            op="unity.add_catalog",
            target="cat_1",
            payload={"name": "test"},
        )
        dumped = op.model_dump(by_alias=True)
        assert dumped["scope"] is None

        restored = Operation(**dumped)
        assert restored.scope is None


class TestV5LoadCurrentState:
    """Test load_current_state with v5 target support."""

    def test_load_current_state_default_target(self, tmp_path):
        """load_current_state should work with default target."""
        storage.ensure_project_file(tmp_path, "unity")
        state, changelog, provider, _ = storage.load_current_state(tmp_path)
        assert provider is not None
        assert provider.info.id == "unity"

    def test_load_current_state_explicit_scope(self, tmp_path):
        """load_current_state should accept scope."""
        storage.ensure_project_file(tmp_path, "unity")
        state, changelog, provider, _ = storage.load_current_state(tmp_path, scope="default")
        assert provider.info.id == "unity"

    def test_load_current_state_invalid_scope(self, tmp_path):
        """load_current_state should raise for invalid scope."""
        storage.ensure_project_file(tmp_path, "unity")
        with pytest.raises(ValueError, match="Target 'nonexistent' not found"):
            storage.load_current_state(tmp_path, scope="nonexistent")
