"""Unit tests for Unity auth and SQL executor behaviors."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.sql import StatementState

from schemax.providers.base.executor import ExecutionConfig, StatementResult
from schemax.providers.unity.auth import (
    AuthenticationError,
    _format_auth_error,
    check_profile_exists,
    create_databricks_client,
    get_current_user,
    get_workspace_url,
    validate_auth,
)
from schemax.providers.unity.executor import ExecutionError, UnitySQLExecutor


def _mk_resp(
    *,
    state: StatementState,
    statement_id: str = "stmt_1",
    data_array: list[list] | None = None,
    columns: list[str] | None = None,
    error_message: str | None = None,
) -> SimpleNamespace:
    status = SimpleNamespace(state=state, error=None)
    if error_message is not None:
        status.error = SimpleNamespace(message=error_message)
    result = None if data_array is None else SimpleNamespace(data_array=data_array)
    manifest = None
    if columns is not None:
        manifest = SimpleNamespace(
            schema=SimpleNamespace(columns=[SimpleNamespace(name=name) for name in columns])
        )
    return SimpleNamespace(
        statement_id=statement_id,
        status=status,
        result=result,
        manifest=manifest,
    )


def _mk_config(timeout_seconds: int = 2) -> ExecutionConfig:
    return ExecutionConfig(
        target_env="dev",
        profile="DEFAULT",
        warehouse_id="wh_1",
        timeout_seconds=timeout_seconds,
    )


@pytest.fixture
def mock_client() -> SimpleNamespace:
    return SimpleNamespace(statement_execution=Mock(), current_user=Mock(), config=Mock())


@pytest.fixture
def executor(mock_client: SimpleNamespace) -> UnitySQLExecutor:
    return UnitySQLExecutor(client=mock_client)


def test_validate_auth_success(mock_client: SimpleNamespace) -> None:
    mock_client.current_user.me.return_value = SimpleNamespace(user_name="a@b.com")
    validate_auth(mock_client)
    mock_client.current_user.me.assert_called_once()


def test_validate_auth_failure_raises(mock_client: SimpleNamespace) -> None:
    mock_client.current_user.me.side_effect = RuntimeError("auth")
    with pytest.raises(AuthenticationError, match="Authentication validation failed"):
        validate_auth(mock_client)


def test_create_databricks_client_with_profile() -> None:
    with patch("schemax.providers.unity.auth.WorkspaceClient") as ws:
        client = Mock()
        ws.return_value = client
        with patch("schemax.providers.unity.auth.validate_auth"):
            result = create_databricks_client("DEV")
    ws.assert_called_once_with(profile="DEV")
    assert result is client


def test_create_databricks_client_wraps_error_message() -> None:
    with patch("schemax.providers.unity.auth.WorkspaceClient", side_effect=RuntimeError("boom")):
        with patch("schemax.providers.unity.auth.check_profile_exists", return_value=False):
            with pytest.raises(AuthenticationError, match="Failed to authenticate"):
                create_databricks_client("DEV")


def test_get_current_user_and_workspace_url_fallbacks(mock_client: SimpleNamespace) -> None:
    mock_client.current_user.me.return_value = SimpleNamespace(user_name=None, display_name="Jane")
    assert get_current_user(mock_client) == "Jane"
    mock_client.current_user.me.side_effect = RuntimeError("nope")
    assert get_current_user(mock_client) == "unknown"
    mock_client.config.host = "https://example.cloud.databricks.com"
    assert get_workspace_url(mock_client) == "https://example.cloud.databricks.com"


@patch("os.path.exists")
@patch("builtins.open", create=True)
def test_check_profile_exists_and_missing(mock_open: Mock, mock_exists: Mock) -> None:
    mock_exists.return_value = True
    mock_open.return_value.__enter__.return_value.read.return_value = "[DEV]\n[PROD]\n"
    assert check_profile_exists("DEV")
    assert not check_profile_exists("QA")
    mock_exists.return_value = False
    assert not check_profile_exists("DEV")


def test_format_auth_error_default_profile_has_troubleshooting() -> None:
    with patch("schemax.providers.unity.auth.check_profile_exists", return_value=False):
        with patch.dict("os.environ", {}, clear=True):
            error = _format_auth_error(RuntimeError("bad creds"), None)
    assert "No authentication configured" in error
    assert "bad creds" in error


def test_execute_single_statement_core_success(executor: UnitySQLExecutor) -> None:
    executor.client.statement_execution.execute_statement.return_value = _mk_resp(
        state=StatementState.PENDING, statement_id="stmt_1"
    )
    executor.client.statement_execution.get_statement.side_effect = [
        _mk_resp(state=StatementState.RUNNING, statement_id="stmt_1"),
        _mk_resp(
            state=StatementState.SUCCEEDED,
            statement_id="stmt_1",
            data_array=[[1, "x"]],
            columns=["id", "name"],
        ),
    ]
    with patch("schemax.providers.unity.executor.time.sleep", return_value=None):
        result = executor._execute_single_statement("SELECT 1", _mk_config(), 1)
    assert result.status == "success"
    assert result.result_data == [{"id": 1, "name": "x"}]


def test_execute_single_statement_core_failed_terminal(executor: UnitySQLExecutor) -> None:
    executor.client.statement_execution.execute_statement.return_value = _mk_resp(
        state=StatementState.PENDING, statement_id="stmt_x"
    )
    executor.client.statement_execution.get_statement.return_value = _mk_resp(
        state=StatementState.FAILED,
        statement_id="stmt_x",
        error_message="syntax error",
    )
    with patch("schemax.providers.unity.executor.time.sleep", return_value=None):
        result = executor._execute_single_statement("BAD SQL", _mk_config(), 1)
    assert result.status == "failed"
    assert "syntax error" in str(result.error_message)


def test_execute_single_statement_timeout(executor: UnitySQLExecutor) -> None:
    executor.client.statement_execution.execute_statement.return_value = _mk_resp(
        state=StatementState.PENDING, statement_id="stmt_t"
    )
    executor.client.statement_execution.get_statement.return_value = _mk_resp(
        state=StatementState.RUNNING,
        statement_id="stmt_t",
    )
    with patch("schemax.providers.unity.executor.time.sleep", return_value=None):
        result = executor._execute_single_statement("SELECT 1", _mk_config(timeout_seconds=1), 1)
    assert result.status == "failed"
    assert "timed out" in str(result.error_message)


def test_execute_single_statement_missing_statement_id_raises(executor: UnitySQLExecutor) -> None:
    executor.client.statement_execution.execute_statement.return_value = _mk_resp(
        state=StatementState.PENDING, statement_id=""
    )
    with pytest.raises(ExecutionError, match="did not return a statement ID"):
        executor._execute_single_statement_core("SELECT 1", _mk_config(), exec_start=0.0)


def test_execute_statements_partial_and_success(executor: UnitySQLExecutor) -> None:
    config = _mk_config()

    with patch.object(
        executor,
        "_execute_single_statement",
        side_effect=[
            StatementResult(
                statement_id="s1",
                sql="A",
                status="success",
                execution_time_ms=1,
                error_message=None,
                result_data=None,
                rows_affected=None,
            ),
            StatementResult(
                statement_id="s2",
                sql="B",
                status="failed",
                execution_time_ms=1,
                error_message="boom",
                result_data=None,
                rows_affected=None,
            ),
        ],
    ):
        partial = executor.execute_statements(["A", "B", "C"], config)
    assert partial.status == "partial"
    assert partial.successful_statements == 1
    assert partial.failed_statement_index == 1

    with patch.object(
        executor,
        "_execute_single_statement",
        return_value=StatementResult(
            statement_id="s1",
            sql="A",
            status="success",
            execution_time_ms=1,
            error_message=None,
            result_data=None,
            rows_affected=None,
        ),
    ):
        ok = executor.execute_statements(["A"], config)
    assert ok.status == "success"
    assert ok.successful_statements == 1


def test_execute_statements_unexpected_exception_path(executor: UnitySQLExecutor) -> None:
    with patch.object(executor, "_execute_single_statement", side_effect=RuntimeError("boom")):
        result = executor.execute_statements(["A"], _mk_config())
    assert result.status == "failed"
    assert result.failed_statement_index == 0
    assert "Unexpected error executing statement 1" in str(result.error_message)


def test_execute_single_statement_wraps_non_execution_error(executor: UnitySQLExecutor) -> None:
    with patch.object(executor, "_execute_single_statement_core", side_effect=RuntimeError("x")):
        result = executor._execute_single_statement("A", _mk_config(), statement_num=9)
    assert result.statement_id == "stmt_error_9"
    assert "API error: x" in str(result.error_message)


def test_execute_single_statement_reraises_execution_error(executor: UnitySQLExecutor) -> None:
    with patch.object(executor, "_execute_single_statement_core", side_effect=ExecutionError("x")):
        with pytest.raises(ExecutionError):
            executor._execute_single_statement("A", _mk_config(), statement_num=1)


def test_poll_until_terminal_raises_when_status_missing(executor: UnitySQLExecutor) -> None:
    executor.client.statement_execution.get_statement.return_value = SimpleNamespace(status=None)
    with pytest.raises(ExecutionError, match="Failed to get statement status"):
        executor._poll_until_terminal("stmt_1", timeout_seconds=1)


def test_handle_terminal_state_canceled(executor: UnitySQLExecutor) -> None:
    canceled = _mk_resp(state=StatementState.CANCELED, statement_id="stmt_c")
    result = executor._handle_terminal_state(canceled, "stmt_c", "SELECT 1", exec_start=0.0)
    assert result.status == "failed"
    assert result.error_message == "Statement was canceled"


def test_parse_result_data_returns_none_when_manifest_missing() -> None:
    response = SimpleNamespace(result=SimpleNamespace(data_array=[[1]]), manifest=None)
    assert UnitySQLExecutor._parse_result_data(response) is None
