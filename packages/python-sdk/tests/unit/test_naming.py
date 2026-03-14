"""
Unit tests for schemax.core.naming

Tests validate_name(), suggest_name(), NamingStandardsConfig.from_dict(),
and validate_naming_standards().
"""

import pytest

from schemax.commands.validate import get_naming_validation_errors_and_warnings
from schemax.core.naming import (
    NamingRule,
    NamingStandardsConfig,
    suggest_name,
    validate_name,
    validate_naming_standards,
)


# ---------------------------------------------------------------------------
# NamingRule.from_dict
# ---------------------------------------------------------------------------


class TestNamingRuleFromDict:
    def test_minimal(self) -> None:
        rule = NamingRule.from_dict({"pattern": "^[a-z]+$", "enabled": True})
        assert rule.pattern == "^[a-z]+$"
        assert rule.enabled is True
        assert rule.description == ""
        assert rule.examples_valid == []
        assert rule.examples_invalid == []

    def test_full(self) -> None:
        rule = NamingRule.from_dict(
            {
                "pattern": "^[a-z]+$",
                "enabled": False,
                "description": "lowercase only",
                "examples": {
                    "valid": ["foo", "bar"],
                    "invalid": ["Foo", "BAR"],
                },
            }
        )
        assert rule.enabled is False
        assert rule.description == "lowercase only"
        assert rule.examples_valid == ["foo", "bar"]
        assert rule.examples_invalid == ["Foo", "BAR"]

    def test_enabled_defaults_to_true(self) -> None:
        rule = NamingRule.from_dict({"pattern": "^[a-z]+$"})
        assert rule.enabled is True


# ---------------------------------------------------------------------------
# NamingStandardsConfig.from_dict
# ---------------------------------------------------------------------------


class TestNamingStandardsConfigFromDict:
    def test_empty_dict(self) -> None:
        config = NamingStandardsConfig.from_dict({})
        assert config.apply_to_renames is False
        assert config.catalog is None
        assert config.schema is None
        assert config.table is None
        assert config.view is None
        assert config.column is None

    def test_apply_to_renames(self) -> None:
        config = NamingStandardsConfig.from_dict({"applyToRenames": True})
        assert config.apply_to_renames is True

    def test_strict_mode(self) -> None:
        config = NamingStandardsConfig.from_dict({})
        assert config.strict_mode is False
        config = NamingStandardsConfig.from_dict({"strictMode": True})
        assert config.strict_mode is True

    def test_with_table_rule(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {
                "table": {
                    "pattern": "^[a-z][a-z0-9_]*$",
                    "enabled": True,
                    "description": "snake_case",
                }
            }
        )
        assert config.table is not None
        assert config.table.pattern == "^[a-z][a-z0-9_]*$"
        assert config.catalog is None

    def test_all_object_types(self) -> None:
        raw = {
            "catalog": {"pattern": "^[a-z]+$", "enabled": True},
            "schema": {"pattern": "^[a-z]+$", "enabled": True},
            "table": {"pattern": "^[a-z]+$", "enabled": True},
            "view": {"pattern": "^[a-z]+$", "enabled": True},
            "column": {"pattern": "^[a-z]+$", "enabled": True},
        }
        config = NamingStandardsConfig.from_dict(raw)
        for attr in ("catalog", "schema", "table", "view", "column"):
            assert getattr(config, attr) is not None

    def test_get_rule(self) -> None:
        raw = {"table": {"pattern": "^[a-z]+$", "enabled": True}}
        config = NamingStandardsConfig.from_dict(raw)
        assert config.get_rule("table") is not None
        assert config.get_rule("catalog") is None
        assert config.get_rule("nonexistent") is None


# ---------------------------------------------------------------------------
# validate_name
# ---------------------------------------------------------------------------


SNAKE_CASE_RULE = NamingRule(pattern="^[a-z][a-z0-9_]*$", enabled=True, description="snake_case")
DISABLED_RULE = NamingRule(pattern="^[a-z][a-z0-9_]*$", enabled=False)


class TestValidateName:
    def test_valid_name(self) -> None:
        valid, err = validate_name("my_table", SNAKE_CASE_RULE)
        assert valid is True
        assert err is None

    def test_invalid_name_uppercase(self) -> None:
        valid, err = validate_name("MyTable", SNAKE_CASE_RULE)
        assert valid is False
        assert err is not None
        assert "snake_case" in err

    def test_invalid_name_starts_with_number(self) -> None:
        valid, err = validate_name("1table", SNAKE_CASE_RULE)
        assert valid is False

    def test_disabled_rule_always_passes(self) -> None:
        valid, err = validate_name("MyTable", DISABLED_RULE)
        assert valid is True
        assert err is None

    def test_error_includes_pattern(self) -> None:
        valid, err = validate_name("Bad-Name", SNAKE_CASE_RULE)
        assert valid is False
        assert "^[a-z][a-z0-9_]*$" in (err or "")

    def test_error_includes_description_if_set(self) -> None:
        valid, err = validate_name("BadName", SNAKE_CASE_RULE)
        assert err is not None
        assert "snake_case" in err

    def test_error_uses_pattern_when_no_description(self) -> None:
        rule = NamingRule(pattern="^[a-z]+$", enabled=True)
        valid, err = validate_name("Foo", rule)
        assert err is not None
        assert "^[a-z]+$" in err


# ---------------------------------------------------------------------------
# suggest_name
# ---------------------------------------------------------------------------


class TestSuggestName:
    def test_lowercase_for_snake_case_pattern(self) -> None:
        # No separator in 'MyTable', so lowercasing produces 'mytable'
        result = suggest_name("MyTable", "^[a-z][a-z0-9_]*$")
        assert result == "mytable"

    def test_hyphen_to_underscore(self) -> None:
        result = suggest_name("my-table", "^[a-z][a-z0-9_]*$")
        assert result == "my_table"

    def test_dot_to_underscore(self) -> None:
        result = suggest_name("my.table", "^[a-z][a-z0-9_]*$")
        assert result == "my_table"

    def test_space_to_underscore(self) -> None:
        result = suggest_name("my table", "^[a-z][a-z0-9_]*$")
        assert result == "my_table"

    def test_strips_special_chars(self) -> None:
        result = suggest_name("my!table#name", "^[a-z][a-z0-9_]*$")
        assert result == "mytablename"

    def test_collapses_underscores(self) -> None:
        result = suggest_name("my__table", "^[a-z][a-z0-9_]*$")
        assert result == "my_table"

    def test_no_lowercasing_for_pascal_pattern(self) -> None:
        result = suggest_name("my-table", "^[A-Z][a-zA-Z0-9]*$")
        # Should replace hyphen with underscore and strip invalid, but NOT lowercase
        assert "my" in result.lower()

    def test_already_valid_unchanged(self) -> None:
        result = suggest_name("my_table", "^[a-z][a-z0-9_]*$")
        assert result == "my_table"

    def test_mixed_separators(self) -> None:
        result = suggest_name("My.Table-Name", "^[a-z][a-z0-9_]*$")
        assert result == "my_table_name"


# ---------------------------------------------------------------------------
# validate_naming_standards
# ---------------------------------------------------------------------------


class TestValidateNamingStandards:
    def _make_state(self) -> dict:
        return {
            "catalogs": [
                {
                    "name": "MyProdCatalog",
                    "schemas": [
                        {
                            "name": "SalesSchema",
                            "tables": [
                                {
                                    "name": "MyTable",
                                    "tableType": "table",
                                    "columns": [
                                        {"name": "ColumnOne", "type": "STRING"},
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    def test_no_rules_returns_empty(self) -> None:
        config = NamingStandardsConfig.from_dict({})
        violations = validate_naming_standards(self._make_state(), config)
        assert violations == []

    def test_catalog_violation(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {"catalog": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True}}
        )
        violations = validate_naming_standards(self._make_state(), config)
        assert any("MyProdCatalog" in v for v in violations)

    def test_table_violation(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {"table": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True}}
        )
        violations = validate_naming_standards(self._make_state(), config)
        assert any("MyTable" in v for v in violations)

    def test_column_violation(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {"column": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True}}
        )
        violations = validate_naming_standards(self._make_state(), config)
        assert any("ColumnOne" in v for v in violations)

    def test_no_violation_when_names_match(self) -> None:
        state = {
            "catalogs": [
                {
                    "name": "my_catalog",
                    "schemas": [
                        {
                            "name": "my_schema",
                            "tables": [
                                {
                                    "name": "my_table",
                                    "columns": [{"name": "my_col", "type": "STRING"}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        config = NamingStandardsConfig.from_dict(
            {
                "catalog": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True},
                "schema": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True},
                "table": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True},
                "column": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True},
            }
        )
        violations = validate_naming_standards(state, config)
        assert violations == []

    def test_non_dict_state_returns_empty(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {"catalog": {"pattern": "^[a-z]+$", "enabled": True}}
        )
        assert validate_naming_standards(None, config) == []  # type: ignore[arg-type]
        assert validate_naming_standards("bad", config) == []  # type: ignore[arg-type]

    def test_disabled_rule_skipped(self) -> None:
        config = NamingStandardsConfig.from_dict(
            {"catalog": {"pattern": "^[a-z]+$", "enabled": False}}
        )
        violations = validate_naming_standards(self._make_state(), config)
        # Disabled rule should not generate violations
        assert not any("MyProdCatalog" in v for v in violations)

    def test_view_uses_view_rule(self) -> None:
        state = {
            "catalogs": [
                {
                    "name": "cat",
                    "schemas": [
                        {
                            "name": "sch",
                            "tables": [
                                {"name": "MyView", "tableType": "view", "columns": []},
                            ],
                        }
                    ],
                }
            ]
        }
        config = NamingStandardsConfig.from_dict(
            {"view": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True}}
        )
        violations = validate_naming_standards(state, config)
        assert any("MyView" in v for v in violations)


# ---------------------------------------------------------------------------
# get_naming_validation_errors_and_warnings (strict mode)
# ---------------------------------------------------------------------------


class TestGetNamingValidationErrorsAndWarnings:
    def _make_project_and_state(self, strict_mode: bool = False) -> tuple[dict, dict]:
        project = {
            "settings": {
                "namingStandards": {
                    "strictMode": strict_mode,
                    "catalog": {"pattern": "^[a-z][a-z0-9_]*$", "enabled": True},
                }
            }
        }
        state = {
            "catalogs": [
                {"name": "MyCatalog", "schemas": [{"name": "my_schema", "tables": []}]}
            ]
        }
        return project, state

    def test_strict_mode_violations_in_errors(self) -> None:
        project, state = self._make_project_and_state(strict_mode=True)
        errors, warnings = get_naming_validation_errors_and_warnings(project, state)
        assert len(errors) > 0
        assert any("MyCatalog" in e for e in errors)
        assert len(warnings) == 0

    def test_non_strict_violations_in_warnings(self) -> None:
        project, state = self._make_project_and_state(strict_mode=False)
        errors, warnings = get_naming_validation_errors_and_warnings(project, state)
        assert len(errors) == 0
        assert len(warnings) > 0
        assert any("MyCatalog" in w for w in warnings)

    def test_no_naming_config_returns_empty(self) -> None:
        project = {"settings": {}}
        state = {"catalogs": [{"name": "Anything", "schemas": []}]}
        errors, warnings = get_naming_validation_errors_and_warnings(project, state)
        assert errors == []
        assert warnings == []
