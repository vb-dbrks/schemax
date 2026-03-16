"""
Naming config command – get/set naming standards in project.json.

Used by the `schemax naming` CLI and by the VS Code extension (via `naming apply`).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from schemax.core.naming import NamingRule, NamingStandardsConfig
from schemax.core.workspace_repository import WorkspaceRepository

VALID_OBJECT_TYPES = ("catalog", "schema", "table", "view", "column")


# Presets aligned with extension TEMPLATES (Databricks Best Practices, etc.)
def _rule(pattern: str, description: str) -> NamingRule:
    return NamingRule(pattern=pattern, enabled=True, description=description)


PRESETS: dict[str, NamingStandardsConfig] = {
    "databricks": NamingStandardsConfig(
        apply_to_renames=False,
        strict_mode=False,
        catalog=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        schema=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        table=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        view=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        column=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
    ),
    "warehouse": NamingStandardsConfig(
        apply_to_renames=False,
        strict_mode=False,
        catalog=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        schema=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        table=_rule("^(dim_|fact_|stg_|int_)[a-z0-9_]+$", "Prefixed table names"),
        view=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
        column=_rule("^[a-z][a-z0-9_]*$", "Lowercase snake_case"),
    ),
}


def get_naming_config(
    workspace: Path,
    workspace_repo: WorkspaceRepository | None = None,
) -> NamingStandardsConfig:
    """Load naming standards from project.json. Uses defaults if missing."""
    repo = workspace_repo or WorkspaceRepository()
    project = repo.read_project(workspace=workspace)
    settings: dict[str, Any] = project.get("settings", {})
    naming_raw: dict[str, Any] = settings.get("namingStandards", {})
    return NamingStandardsConfig.from_dict(naming_raw)


def set_naming_config(
    workspace: Path,
    config: NamingStandardsConfig,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Write naming standards to project.json. Preserves other settings keys."""
    repo = workspace_repo or WorkspaceRepository()
    project = repo.read_project(workspace=workspace)
    if "settings" not in project or not isinstance(project["settings"], dict):
        project["settings"] = {}
    project["settings"]["namingStandards"] = config.to_dict()
    repo.write_project(workspace=workspace, project=project)


def apply_naming_config(
    workspace: Path,
    config_dict: dict[str, Any],
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Apply full naming config from a dict (e.g. from UI or --json)."""
    config = NamingStandardsConfig.from_dict(config_dict)
    _validate_no_empty_patterns(config)
    set_naming_config(workspace, config, workspace_repo=workspace_repo)


def set_strict(
    workspace: Path,
    value: bool,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Set strict mode on or off."""
    config = get_naming_config(workspace, workspace_repo)
    updated = NamingStandardsConfig(
        apply_to_renames=config.apply_to_renames,
        strict_mode=value,
        catalog=config.catalog,
        schema=config.schema,
        table=config.table,
        view=config.view,
        column=config.column,
    )
    set_naming_config(workspace, updated, workspace_repo)


def set_enforce_on_renames(
    workspace: Path,
    value: bool,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Set enforce on renames (applyToRenames) on or off."""
    config = get_naming_config(workspace, workspace_repo)
    updated = NamingStandardsConfig(
        apply_to_renames=value,
        strict_mode=config.strict_mode,
        catalog=config.catalog,
        schema=config.schema,
        table=config.table,
        view=config.view,
        column=config.column,
    )
    set_naming_config(workspace, updated, workspace_repo)


def _validate_no_empty_patterns(config: NamingStandardsConfig) -> None:
    """Raise ValueError if any rule has an empty or whitespace-only pattern."""
    for key in VALID_OBJECT_TYPES:
        rule = config.get_rule(key)
        if rule is not None and (not rule.pattern or not rule.pattern.strip()):
            raise ValueError(f"Naming rule for '{key}' has an empty pattern. Pattern is required.")


def set_rule(
    workspace: Path,
    object_type: str,
    pattern: str,
    description: str = "",
    enabled: bool = True,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Add or update the naming rule for object_type. Validates regex."""
    if object_type not in VALID_OBJECT_TYPES:
        raise ValueError(
            f"Unknown object type '{object_type}'. Must be one of: {', '.join(VALID_OBJECT_TYPES)}."
        )
    if not pattern or not pattern.strip():
        raise ValueError("Pattern cannot be empty.")
    try:
        re.compile(pattern)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern: {e}") from e

    rule = NamingRule(pattern=pattern, enabled=enabled, description=description)
    config = get_naming_config(workspace, workspace_repo)
    updated = NamingStandardsConfig(
        apply_to_renames=config.apply_to_renames,
        strict_mode=config.strict_mode,
        catalog=rule if object_type == "catalog" else config.catalog,
        schema=rule if object_type == "schema" else config.schema,
        table=rule if object_type == "table" else config.table,
        view=rule if object_type == "view" else config.view,
        column=rule if object_type == "column" else config.column,
    )
    set_naming_config(workspace, updated, workspace_repo)


def remove_rule(
    workspace: Path,
    object_type: str,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Remove the naming rule for object_type."""
    if object_type not in VALID_OBJECT_TYPES:
        raise ValueError(
            f"Unknown object type '{object_type}'. Must be one of: {', '.join(VALID_OBJECT_TYPES)}."
        )
    config = get_naming_config(workspace, workspace_repo)
    updated = NamingStandardsConfig(
        apply_to_renames=config.apply_to_renames,
        strict_mode=config.strict_mode,
        catalog=None if object_type == "catalog" else config.catalog,
        schema=None if object_type == "schema" else config.schema,
        table=None if object_type == "table" else config.table,
        view=None if object_type == "view" else config.view,
        column=None if object_type == "column" else config.column,
    )
    set_naming_config(workspace, updated, workspace_repo)


def load_template(
    workspace: Path,
    preset_id: str,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Apply a preset (databricks, warehouse, camelcase, pascalcase)."""
    if preset_id not in PRESETS:
        raise ValueError(
            f"Unknown preset '{preset_id}'. Must be one of: {', '.join(sorted(PRESETS))}."
        )
    set_naming_config(workspace, PRESETS[preset_id], workspace_repo=workspace_repo)


def apply_naming_config_from_json(
    workspace: Path,
    json_str: str,
    workspace_repo: WorkspaceRepository | None = None,
) -> None:
    """Apply full naming config from JSON string (e.g. stdin or --json)."""
    data = json.loads(json_str)
    if not isinstance(data, dict):
        raise ValueError("JSON must be an object (naming config dict).")
    apply_naming_config(workspace, data, workspace_repo=workspace_repo)
