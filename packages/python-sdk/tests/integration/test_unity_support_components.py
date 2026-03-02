"""Integration-style tests for Unity support modules with low coverage."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from schemax.providers.base.executor import ExecutionConfig, ExecutionResult, StatementResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel
from schemax.providers.unity import privileges
from schemax.providers.unity.auth import (
    _format_auth_error,
    check_profile_exists,
    get_current_user,
    get_workspace_url,
)
from schemax.providers.unity.safety_validator import SafetyValidator


@dataclass
class _FakeUser:
    user_name: str | None = None
    display_name: str | None = None


class _FakeCurrentUserApi:
    def __init__(self, user: _FakeUser | None = None, raises: Exception | None = None) -> None:
        self._user = user
        self._raises = raises

    def me(self) -> _FakeUser:
        if self._raises is not None:
            raise self._raises
        assert self._user is not None
        return self._user


class _FakeClient:
    def __init__(
        self,
        user: _FakeUser | None = None,
        user_error: Exception | None = None,
        host: str | None = None,
        no_config: bool = False,
    ) -> None:
        self.current_user = _FakeCurrentUserApi(user=user, raises=user_error)
        if no_config:
            return
        self.config = type("_Cfg", (), {"host": host})()


class _FakeExecutor:
    """Simple SQLExecutor-compatible fake for safety validator tests."""

    def __init__(
        self, counts: dict[str, int] | None = None, raises: set[str] | None = None
    ) -> None:
        self._counts = counts or {}
        self._raises = raises or set()

    def execute_statements(self, statements: list[str], config: ExecutionConfig) -> ExecutionResult:
        _ = config
        sql = statements[0]
        for marker in self._raises:
            if marker in sql:
                raise RuntimeError(f"forced failure for {marker}")
        cnt = 0
        for marker, value in self._counts.items():
            if marker in sql:
                cnt = value
                break
        result = StatementResult(
            statement_id="stmt_1",
            sql=sql,
            status="success",
            execution_time_ms=1,
            result_data=[{"cnt": cnt}],
        )
        return ExecutionResult(
            deployment_id="dep_1",
            total_statements=1,
            successful_statements=1,
            status="success",
            statement_results=[result],
        )


def _cfg() -> ExecutionConfig:
    return ExecutionConfig(
        target_env="dev",
        profile="test",
        warehouse_id="wh",
        dry_run=True,
        no_interaction=True,
    )


@pytest.mark.integration
def test_privilege_constants_are_non_empty_and_include_core_actions() -> None:
    assert "USE CATALOG" in privileges.CATALOG_PRIVILEGES
    assert "CREATE TABLE" in privileges.SCHEMA_PRIVILEGES
    assert "SELECT" in privileges.TABLE_VIEW_PRIVILEGES
    assert "READ VOLUME" in privileges.VOLUME_PRIVILEGES
    assert "EXECUTE" in privileges.FUNCTION_PRIVILEGES
    assert "REFRESH" in privileges.MATERIALIZED_VIEW_PRIVILEGES


@pytest.mark.integration
def test_check_profile_exists_uses_home_databrickscfg(tmp_path: Path, monkeypatch: Any) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".databrickscfg").write_text("[fieldeng]\nhost=https://x\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))

    assert check_profile_exists("fieldeng") is True
    assert check_profile_exists("missing") is False


@pytest.mark.integration
def test_auth_helpers_return_expected_user_and_url() -> None:
    client = _FakeClient(user=_FakeUser(user_name="user@example.com"), host="https://dbc")
    assert get_current_user(client) == "user@example.com"
    assert get_workspace_url(client) == "https://dbc"


@pytest.mark.integration
def test_auth_helpers_gracefully_handle_failures() -> None:
    client = _FakeClient(user=None, user_error=RuntimeError("auth failed"), no_config=True)
    assert get_current_user(client) == "unknown"
    assert get_workspace_url(client) is None


@pytest.mark.integration
def test_format_auth_error_profile_missing_branch(tmp_path: Path, monkeypatch: Any) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    message = _format_auth_error(RuntimeError("boom"), profile="does-not-exist")
    assert "Profile: does-not-exist" in message
    assert "not found in ~/.databrickscfg" in message


@pytest.mark.integration
def test_format_auth_error_default_without_env_or_profile(tmp_path: Path, monkeypatch: Any) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    message = _format_auth_error(RuntimeError("boom"), profile=None)
    assert "No authentication configured" in message
    assert "Environment variables" in message


@pytest.mark.integration
def test_safety_validator_drop_table_classifies_safe_risky_destructive() -> None:
    op = Operation(
        id="op_1",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_table",
        target="cat.schema.tbl",
        payload={},
    )

    safe_validator = SafetyValidator(_FakeExecutor(counts={"COUNT(*)": 0}), _cfg())
    safe = safe_validator.validate(op, {"cat": "phys_cat"})
    assert safe.level == SafetyLevel.SAFE

    risky_validator = SafetyValidator(_FakeExecutor(counts={"COUNT(*)": 10}), _cfg())
    risky = risky_validator.validate(op, {"cat": "phys_cat"})
    assert risky.level == SafetyLevel.RISKY

    destructive_validator = SafetyValidator(_FakeExecutor(counts={"COUNT(*)": 5000}), _cfg())
    destructive = destructive_validator.validate(op, {"cat": "phys_cat"})
    assert destructive.level == SafetyLevel.DESTRUCTIVE


@pytest.mark.integration
def test_safety_validator_drop_column_with_invalid_reference_is_safe() -> None:
    op = Operation(
        id="op_2",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_column",
        target="ignored",
        payload={"tableId": "bad-format", "name": "col_a"},
    )
    validator = SafetyValidator(_FakeExecutor(), _cfg())
    report = validator.validate(op, {"cat": "phys_cat"})
    assert report.level == SafetyLevel.SAFE
    assert "Invalid table or column reference" in report.reason


@pytest.mark.integration
def test_safety_validator_unknown_operation_defaults_to_safe() -> None:
    op = Operation(
        id="op_3",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.unknown_operation",
        target="x",
        payload={},
    )
    validator = SafetyValidator(_FakeExecutor(), _cfg())
    report = validator.validate(op)
    assert report.level == SafetyLevel.SAFE
    assert "non-destructive operation" in report.reason
