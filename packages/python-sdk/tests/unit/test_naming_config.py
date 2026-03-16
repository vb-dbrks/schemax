"""Unit tests for schemax.commands.naming_config."""

from pathlib import Path

import pytest

from schemax.commands import naming_config as naming_config_cmd
from schemax.core.naming import NamingRule, NamingStandardsConfig


def _minimal_project(workspace: Path) -> None:
    """Write a minimal v5 project.json so naming_config can read/write."""
    import json

    project_path = workspace / ".schemax" / "project.json"
    project_path.parent.mkdir(parents=True, exist_ok=True)
    project = {
        "version": 5,
        "name": "test",
        "targets": {
            "default": {
                "type": "unity",
                "version": "1.0.0",
                "environments": {
                    "dev": {
                        "topLevelName": "dev_catalog",
                        "allowDrift": False,
                        "requireSnapshot": False,
                        "autoCreateTopLevel": True,
                        "autoCreateSchemaxSchema": True,
                    }
                },
            }
        },
        "settings": {},
    }
    project_path.write_text(json.dumps(project, indent=2), encoding="utf-8")


@pytest.fixture
def workspace_with_project(tmp_path: Path) -> Path:
    """Workspace with minimal project.json."""
    _minimal_project(tmp_path)
    return tmp_path


def test_get_naming_config_defaults(workspace_with_project: Path) -> None:
    """get_naming_config returns defaults when no namingStandards."""
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.apply_to_renames is False
    assert config.strict_mode is False
    assert config.catalog is None


def test_set_naming_config_persists(workspace_with_project: Path) -> None:
    """set_naming_config writes and get_naming_config reads back."""
    config = NamingStandardsConfig(
        apply_to_renames=True,
        strict_mode=True,
        catalog=NamingRule(pattern="^[a-z]+$", enabled=True, description="cat"),
    )
    naming_config_cmd.set_naming_config(workspace_with_project, config)
    read_back = naming_config_cmd.get_naming_config(workspace_with_project)
    assert read_back.apply_to_renames is True
    assert read_back.strict_mode is True
    assert read_back.catalog is not None
    assert read_back.catalog.pattern == "^[a-z]+$"


def test_apply_naming_config_from_dict(workspace_with_project: Path) -> None:
    """apply_naming_config applies dict and persists."""
    naming_config_cmd.apply_naming_config(
        workspace_with_project,
        {
            "applyToRenames": True,
            "strictMode": False,
            "catalog": {"pattern": "^[a-z]+$", "enabled": True},
        },
    )
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.apply_to_renames is True
    assert config.catalog is not None
    assert config.catalog.pattern == "^[a-z]+$"


def test_set_strict(workspace_with_project: Path) -> None:
    """set_strict updates only strict_mode."""
    naming_config_cmd.set_strict(workspace_with_project, True)
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.strict_mode is True
    naming_config_cmd.set_strict(workspace_with_project, False)
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.strict_mode is False


def test_set_enforce_on_renames(workspace_with_project: Path) -> None:
    """set_enforce_on_renames updates only apply_to_renames."""
    naming_config_cmd.set_enforce_on_renames(workspace_with_project, True)
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.apply_to_renames is True


def test_set_rule_adds_and_updates(workspace_with_project: Path) -> None:
    """set_rule adds rule then updates it."""
    naming_config_cmd.set_rule(
        workspace_with_project,
        "table",
        "^[a-z][a-z0-9_]*$",
        description="snake_case",
        enabled=True,
    )
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.table is not None
    assert config.table.pattern == "^[a-z][a-z0-9_]*$"
    assert config.table.description == "snake_case"

    naming_config_cmd.set_rule(
        workspace_with_project,
        "table",
        "^[A-Z][a-zA-Z0-9]*$",
        description="PascalCase",
    )
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.table is not None
    assert config.table.pattern == "^[A-Z][a-zA-Z0-9]*$"
    assert config.table.description == "PascalCase"


def test_set_rule_invalid_regex_raises(workspace_with_project: Path) -> None:
    """set_rule raises ValueError for invalid regex."""
    with pytest.raises(ValueError, match="Invalid regex"):
        naming_config_cmd.set_rule(workspace_with_project, "table", "[invalid", enabled=True)


def test_set_rule_empty_pattern_raises(workspace_with_project: Path) -> None:
    """set_rule raises ValueError for empty or whitespace-only pattern."""
    with pytest.raises(ValueError, match="Pattern cannot be empty"):
        naming_config_cmd.set_rule(workspace_with_project, "catalog", "", enabled=True)
    with pytest.raises(ValueError, match="Pattern cannot be empty"):
        naming_config_cmd.set_rule(workspace_with_project, "schema", "   ", enabled=True)


def test_set_rule_invalid_type_raises(workspace_with_project: Path) -> None:
    """set_rule raises ValueError for unknown object type."""
    with pytest.raises(ValueError, match="Unknown object type"):
        naming_config_cmd.set_rule(workspace_with_project, "invalid_type", "^[a-z]+$")


def test_remove_rule(workspace_with_project: Path) -> None:
    """remove_rule removes the rule for the type."""
    naming_config_cmd.set_rule(workspace_with_project, "schema", "^[a-z]+$")
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.schema is not None

    naming_config_cmd.remove_rule(workspace_with_project, "schema")
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.schema is None


def test_load_template(workspace_with_project: Path) -> None:
    """load_template applies preset and persists."""
    naming_config_cmd.load_template(workspace_with_project, "databricks")
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.catalog is not None
    assert config.catalog.pattern == "^[a-z][a-z0-9_]*$"
    assert config.table is not None

    naming_config_cmd.load_template(workspace_with_project, "warehouse")
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.table is not None
    assert "dim_" in config.table.pattern or "fact_" in config.table.pattern


def test_load_template_invalid_preset_raises(workspace_with_project: Path) -> None:
    """load_template raises ValueError for unknown preset."""
    with pytest.raises(ValueError, match="Unknown preset"):
        naming_config_cmd.load_template(workspace_with_project, "unknown_preset")


def test_apply_naming_config_from_json(workspace_with_project: Path) -> None:
    """apply_naming_config_from_json parses JSON and applies."""
    import json

    payload = {
        "applyToRenames": True,
        "strictMode": True,
        "catalog": {"pattern": "^[a-z]+$", "enabled": True, "description": "cat"},
    }
    naming_config_cmd.apply_naming_config_from_json(workspace_with_project, json.dumps(payload))
    config = naming_config_cmd.get_naming_config(workspace_with_project)
    assert config.apply_to_renames is True
    assert config.strict_mode is True
    assert config.catalog is not None
    assert config.catalog.pattern == "^[a-z]+$"


def test_apply_naming_config_empty_pattern_raises(workspace_with_project: Path) -> None:
    """apply_naming_config raises ValueError when any rule has empty pattern."""
    with pytest.raises(ValueError, match="empty pattern"):
        naming_config_cmd.apply_naming_config(
            workspace_with_project,
            {
                "applyToRenames": False,
                "strictMode": False,
                "catalog": {"pattern": "", "enabled": True},
            },
        )
    with pytest.raises(ValueError, match="empty pattern"):
        naming_config_cmd.apply_naming_config(
            workspace_with_project,
            {
                "applyToRenames": False,
                "strictMode": False,
                "table": {"pattern": "  ", "enabled": True},
            },
        )


def test_get_naming_config_missing_project_raises(tmp_path: Path) -> None:
    """get_naming_config raises FileNotFoundError when project.json missing."""
    (tmp_path / ".schemax").mkdir(exist_ok=True)
    with pytest.raises(FileNotFoundError):
        naming_config_cmd.get_naming_config(tmp_path)
