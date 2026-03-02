"""
Naming standards validation for Unity Catalog.

Validates catalog object names (schema, table, view, column, volume, function,
materialized_view) against per-catalog naming rules. Mirrors logic from
packages/vscode-extension/src/webview/utils/namingStandards.ts.
"""

import re
from dataclasses import dataclass
from typing import Any

from schemax.providers.unity.models import (
    UnityCatalog,
    UnitySchema,
    UnityState,
)


@dataclass
class NamingViolation:
    """A single naming convention violation."""

    catalog_name: str
    object_type: str
    object_name: str
    rule_pattern: str
    message: str


def _get_applicable_rule(
    object_type: str,
    rules: list[dict[str, Any]],
    table_type: str | None = None,
) -> dict[str, Any] | None:
    """Find the first applicable rule for the given object type.

    Prefers exact tableType match for tables, then falls back to tableType 'any'.
    """
    enabled = [r for r in rules if r.get("enabled", True) is not False]
    if object_type == "table" and table_type and table_type != "any":
        exact = next(
            (
                r
                for r in enabled
                if r.get("objectType") == "table" and r.get("tableType") == table_type
            ),
            None,
        )
        if exact:
            return exact
    fallback = next(
        (
            r
            for r in enabled
            if r.get("objectType") == object_type
            and (object_type != "table" or not r.get("tableType") or r.get("tableType") == "any")
        ),
        None,
    )
    return fallback


def _validate_name_against_rule(name: str, rule: dict[str, Any]) -> tuple[bool, str | None]:
    """Validate a name against a single rule's regex pattern.

    Returns (valid, error_message). If valid, error_message is None.
    Empty or missing pattern means no constraint (valid).
    """
    if rule.get("enabled") is False:
        return True, None
    pattern = (rule.get("pattern") or "").strip()
    if not pattern:
        return True, None
    try:
        if re.search(pattern, name):
            return True, None
        return False, "Name does not match the naming pattern."
    except re.error:
        return False, "Invalid regex in naming rule."


def _validate_catalog_objects(
    catalog: UnityCatalog,
) -> list[NamingViolation]:
    """Validate all objects in a single catalog against its naming_standards rules."""
    violations: list[NamingViolation] = []
    ns = catalog.naming_standards or {}
    strict = ns.get("strictMode", False)
    rules_raw = ns.get("rules")
    if not strict or not rules_raw:
        return violations
    rules = list(rules_raw) if isinstance(rules_raw, list) else []

    def check(name: str, object_type: str, table_type: str | None = None) -> None:
        rule = _get_applicable_rule(object_type, rules, table_type)
        if not rule:
            return
        valid, msg = _validate_name_against_rule(name, rule)
        if not valid and msg:
            pattern = (rule.get("pattern") or "").strip()
            violations.append(
                NamingViolation(
                    catalog_name=catalog.name,
                    object_type=object_type,
                    object_name=name,
                    rule_pattern=pattern,
                    message=msg,
                )
            )

    for schema in catalog.schemas or []:
        check(schema.name, "schema")
        for table in schema.tables or []:
            check(table.name, "table")
            for col in table.columns or []:
                check(col.name, "column")
        for view in schema.views or []:
            check(view.name, "view")
        for vol in schema.volumes or []:
            check(vol.name, "volume")
        for func in schema.functions or []:
            check(func.name, "function")
        for mv in schema.materialized_views or []:
            check(mv.name, "materialized_view")

    return violations


def collect_naming_violations(state: UnityState) -> list[NamingViolation]:
    """Collect all naming convention violations for catalogs with strict mode enabled.

    Only catalogs that have naming_standards.strictMode True and rules are checked.
    Returns a list of violations (catalog name, object type, object name, rule pattern, message).
    """
    violations: list[NamingViolation] = []
    for catalog in state.catalogs or []:
        violations.extend(_validate_catalog_objects(catalog))
    return violations
