"""
Naming Standards

Business logic for enforcing naming conventions on database objects.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class NamingRule:
    """A naming rule for a specific object type."""

    pattern: str
    """Regex pattern, e.g. ``^[a-z][a-z0-9_]*$``."""
    enabled: bool
    description: str = ""
    examples_valid: list[str] = field(default_factory=list)
    examples_invalid: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NamingRule":
        """Deserialize from a project.json dict."""
        return cls(
            pattern=d["pattern"],
            enabled=bool(d.get("enabled", True)),
            description=d.get("description", ""),
            examples_valid=list(d.get("examples", {}).get("valid", [])),
            examples_invalid=list(d.get("examples", {}).get("invalid", [])),
        )


@dataclass(slots=True, frozen=True)
class NamingStandardsConfig:
    """Project-level naming standards configuration."""

    apply_to_renames: bool = False
    strict_mode: bool = False
    catalog: NamingRule | None = None
    schema: NamingRule | None = None
    table: NamingRule | None = None
    view: NamingRule | None = None
    column: NamingRule | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NamingStandardsConfig":
        """Deserialize from a project.json settings.namingStandards dict."""

        def _rule(key: str) -> NamingRule | None:
            raw = d.get(key)
            if raw is None or not isinstance(raw, dict):
                return None
            return NamingRule.from_dict(raw)

        return cls(
            apply_to_renames=bool(d.get("applyToRenames", False)),
            strict_mode=bool(d.get("strictMode", False)),
            catalog=_rule("catalog"),
            schema=_rule("schema"),
            table=_rule("table"),
            view=_rule("view"),
            column=_rule("column"),
        )

    def get_rule(self, object_type: str) -> NamingRule | None:
        """Return the rule for *object_type*, or None if unconfigured."""
        return getattr(self, object_type, None)


# ---------------------------------------------------------------------------
# Core validation / suggestion helpers
# ---------------------------------------------------------------------------


def validate_name(name: str, rule: NamingRule) -> tuple[bool, str | None]:
    """Check *name* against *rule*.

    Returns:
        (True, None) when valid.
        (False, error_message) when the rule is enabled and the name does not match.
        (True, None) when the rule is disabled (acts as pass-through).
    """
    if not rule.enabled:
        return True, None

    if re.fullmatch(rule.pattern, name):
        return True, None

    label = rule.description or rule.pattern
    return False, f"Name does not match naming standard ({label}: {rule.pattern})"


def suggest_name(name: str, pattern: str) -> str:
    """Return a sanitised version of *name* that is more likely to match *pattern*.

    Strategy:
    - Lowercase when the pattern contains no uppercase ASCII range ``[A-Z]``
      and requires a lowercase start (``^[a-z``).
    - Replace hyphens, dots, and spaces with underscores.
    - Strip any characters that are *not* alphanumeric or underscore.
    - Collapse consecutive underscores.
    - Strip leading/trailing underscores.
    """
    result = name

    # Lowercase if the pattern targets snake_case / lowercase identifiers
    needs_lower = bool(re.search(r"\^?\[a-z", pattern)) and "[A-Z]" not in pattern
    if needs_lower:
        result = result.lower()

    # Normalise word separators to underscores
    result = re.sub(r"[-.\s]+", "_", result)

    # Strip characters that are definitely not word chars or underscores
    result = re.sub(r"[^\w]", "", result)

    # Collapse runs of underscores and strip edge underscores
    result = re.sub(r"_+", "_", result).strip("_")

    return result


# ---------------------------------------------------------------------------
# Bulk validation against full project state
# ---------------------------------------------------------------------------

_OBJECT_TYPES_TO_CHECK = ("catalog", "schema", "table", "view", "column")


def validate_naming_standards(
    state: Any,
    config: NamingStandardsConfig,
) -> list[str]:
    """Iterate all objects in *state* and return a list of naming violation strings.

    *state* is expected to have the same shape as the Python SDK state dict
    (``state["catalogs"]`` → list of catalogs each with ``schemas`` → ...).

    Returns an empty list when there are no violations or no rules are configured.
    """
    violations: list[str] = []

    if not isinstance(state, dict):
        return violations

    catalogs = state.get("catalogs", [])
    if not isinstance(catalogs, list):
        return violations

    for catalog in catalogs:
        catalog_name: str = catalog.get("name", "")
        rule = config.catalog
        if rule and catalog_name:
            valid, error = validate_name(catalog_name, rule)
            if not valid and error:
                violations.append(f"catalog '{catalog_name}': {error}")

        for schema in catalog.get("schemas", []):
            schema_name: str = schema.get("name", "")
            schema_rule = config.schema
            if schema_rule and schema_name:
                valid, error = validate_name(schema_name, schema_rule)
                if not valid and error:
                    violations.append(
                        f"schema '{catalog_name}.{schema_name}': {error}"
                    )

            for table in schema.get("tables", []):
                table_name: str = table.get("name", "")
                table_type: str = table.get("tableType", table.get("type", "table"))
                is_view = table_type in ("view", "VIEW", "materialized_view", "MATERIALIZED_VIEW")
                obj_rule = config.view if is_view else config.table
                if obj_rule and table_name:
                    valid, error = validate_name(table_name, obj_rule)
                    if not valid and error:
                        label = "view" if is_view else "table"
                        violations.append(
                            f"{label} '{catalog_name}.{schema_name}.{table_name}': {error}"
                        )

                for column in table.get("columns", []):
                    column_name: str = column.get("name", "")
                    col_rule = config.column
                    if col_rule and column_name:
                        valid, error = validate_name(column_name, col_rule)
                        if not valid and error:
                            violations.append(
                                f"column '{catalog_name}.{schema_name}.{table_name}.{column_name}': {error}"
                            )

    return violations
