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
    schema_name: str | None = None  # For table, view, column, volume, function, materialized_view
    table_name: str | None = None  # For column only (catalog.schema.table.column)
    strict_mode: bool = True  # True when catalog has strictMode on → error + block; False → warning only


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


def format_qualified_name(v: NamingViolation) -> str:
    """Format a violation as a fully qualified name: catalog.schema.table or catalog.schema.table.column."""
    if v.object_type == "schema":
        return f"{v.catalog_name}.{v.object_name}"
    if v.table_name is not None:
        return f"{v.catalog_name}.{v.schema_name or ''}.{v.table_name}.{v.object_name}"
    if v.schema_name is not None:
        return f"{v.catalog_name}.{v.schema_name}.{v.object_name}"
    return f"{v.catalog_name}.{v.object_name}"


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
    if not rules_raw:
        return violations
    rules = list(rules_raw) if isinstance(rules_raw, list) else []

    def check(
        name: str,
        object_type: str,
        table_type: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
    ) -> None:
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
                    schema_name=schema_name,
                    table_name=table_name,
                    strict_mode=strict,
                )
            )

    for schema in catalog.schemas or []:
        check(schema.name, "schema")
        for table in schema.tables or []:
            check(table.name, "table", schema_name=schema.name)
            for col in table.columns or []:
                check(col.name, "column", schema_name=schema.name, table_name=table.name)
        for view in schema.views or []:
            check(view.name, "view", schema_name=schema.name)
        for vol in schema.volumes or []:
            check(vol.name, "volume", schema_name=schema.name)
        for func in schema.functions or []:
            check(func.name, "function", schema_name=schema.name)
        for mv in schema.materialized_views or []:
            check(mv.name, "materialized_view", schema_name=schema.name)

    return violations


def collect_naming_violations(state: UnityState) -> list[NamingViolation]:
    """Collect naming convention violations for all catalogs that have naming rules.

    Each violation is tagged with strict_mode (True if the catalog has strictMode on).
    Callers should: block only when any violation has strict_mode True; report strict as
    errors, non-strict as warnings.
    """
    violations: list[NamingViolation] = []
    for catalog in state.catalogs or []:
        violations.extend(_validate_catalog_objects(catalog))
    return violations
