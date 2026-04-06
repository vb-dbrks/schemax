"""
Validate-Name Command

Validates a single object name against the naming standards configured in
the project's ``settings.namingStandards`` section.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from schemax.core.naming import (
    NamingStandardsConfig,
    VALID_OBJECT_TYPES,
    suggest_name,
    validate_name,
)
from schemax.core.workspace_repository import WorkspaceRepository


def validate_name_command(
    name: str,
    object_type: str,
    workspace: Path,
    workspace_repo: WorkspaceRepository | None = None,
) -> dict[str, Any]:
    """Validate *name* against the project's naming standard for *object_type*.

    Args:
        name: The object name to validate.
        object_type: One of ``catalog``, ``schema``, ``table``, ``view``, ``column``.
        workspace: Path to the ``.schemax/`` project directory.
        workspace_repo: Optional repository override (for tests).

    Returns:
        A data dict suitable for embedding in a ``CommandEnvelope``.  Shape::

            {
                "valid": bool,
                "name": str,
                "objectType": str,
                "error": str | None,
                "suggestion": str | None,
                "pattern": str | None,
                "description": str | None,
            }
    """
    repo = workspace_repo or WorkspaceRepository()

    base: dict[str, Any] = {
        "valid": True,
        "name": name,
        "objectType": object_type,
        "error": None,
        "suggestion": None,
        "pattern": None,
        "description": None,
    }

    if object_type not in VALID_OBJECT_TYPES:
        base["valid"] = False
        base["error"] = (
            f"Unknown object type '{object_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_OBJECT_TYPES))}."
        )
        return base

    project = repo.read_project(workspace=workspace)
    settings: dict[str, Any] = project.get("settings", {})
    naming_raw: dict[str, Any] = settings.get("namingStandards", {})

    config = NamingStandardsConfig.from_dict(naming_raw)
    rule = config.get_rule(object_type)

    if rule is None or not rule.enabled:
        return base  # no standard configured → always valid

    base["pattern"] = rule.pattern
    base["description"] = rule.description or None

    valid, error = validate_name(name, rule)
    if valid:
        return base

    suggestion = suggest_name(name, rule.pattern)
    base["valid"] = False
    base["error"] = error
    base["suggestion"] = suggestion if suggestion != name else None
    return base
