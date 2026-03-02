"""Unit tests for deployment tracker helpers and SQL tracking flows."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.sql import StatementState

from schemax.core.deployment import (
    DeploymentTracker,
    _is_expected_not_found_error,
    _normalize_deployed_at,
    _parse_ops_response,
    _row_to_deployment_record,
)
from schemax.providers.base.executor import ExecutionResult, StatementResult
from schemax.providers.base.operations import Operation


def _mk_response(
    *,
    state: StatementState = StatementState.SUCCEEDED,
    error_message: str | None = None,
    data_array: list[list] | None = None,
    statement_id: str = "stmt_1",
) -> SimpleNamespace:
    status = SimpleNamespace(state=state, error=None)
    if error_message is not None:
        status.error = SimpleNamespace(message=error_message)
    result = None if data_array is None else SimpleNamespace(data_array=data_array)
    return SimpleNamespace(status=status, result=result, statement_id=statement_id)


@pytest.fixture
def tracker() -> DeploymentTracker:
    client = SimpleNamespace(statement_execution=Mock())
    return DeploymentTracker(client=client, catalog="cat", warehouse_id="wh")


def test_expected_not_found_error_patterns() -> None:
    assert _is_expected_not_found_error("CATALOG does not exist")
    assert _is_expected_not_found_error("table_or_view_not_found")
    assert not _is_expected_not_found_error("permission denied")


def test_row_to_deployment_record_maps_columns() -> None:
    row = [
        "dep_1",
        "dev",
        "v1",
        "v0",
        "dep_0",
        "2026-01-01 01:02:03",
        "user@example.com",
        "success",
        3,
        None,
        1234,
    ]
    record = _row_to_deployment_record(row)
    assert record["id"] == "dep_1"
    assert record["environment"] == "dev"
    assert record["executionTimeMs"] == 1234


def test_parse_ops_response_parses_payload_and_failed_index() -> None:
    response = _mk_response(
        data_array=[
            ["op_1", "unity.add_table", "tbl_1", '{"a":1}', "success", 1, None],
            ["op_2", "unity.drop_table", "tbl_2", "{not-json", "failed", 2, "boom"],
        ]
    )
    ops_applied, ops_details, failed_index = _parse_ops_response(response)
    assert ops_applied == ["op_1", "op_2"]
    assert ops_details[0]["payload"] == {"a": 1}
    assert ops_details[1]["payload"] == {}
    assert failed_index == 1


def test_normalize_deployed_at_variants() -> None:
    assert _normalize_deployed_at(None) is None
    dt = datetime(2026, 1, 2, 3, 4, 5, 123456)
    assert _normalize_deployed_at(dt) == "2026-01-02 03:04:05.123456"
    assert _normalize_deployed_at("2026-01-01T12:00:00Z") == "2026-01-01 12:00:00"
    assert _normalize_deployed_at("2026-01-01T12:00:00.1+00:00") == "2026-01-01 12:00:00.100000"


def test_execute_statement_sync_success(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.execute_statement.return_value = _mk_response(
        data_array=[["v1", "dep_1", "2026-01-01 00:00:00"]]
    )
    result = tracker._execute_statement_sync("SELECT 1")
    assert result is not None


def test_execute_statement_sync_handles_expected_not_found_exception(
    tracker: DeploymentTracker,
) -> None:
    tracker.client.statement_execution.execute_statement.side_effect = RuntimeError(
        "catalog not found"
    )
    assert tracker._execute_statement_sync("SELECT 1") is None


def test_execute_statement_sync_raises_on_non_terminal_error(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.execute_statement.return_value = _mk_response(
        state=StatementState.FAILED,
        error_message="permission denied",
    )
    with pytest.raises(RuntimeError, match="Database query failed"):
        tracker._execute_statement_sync("SELECT 1")


def test_execute_statement_raise_raises_on_failure(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.execute_statement.return_value = _mk_response(
        state=StatementState.FAILED,
        error_message="boom",
    )
    with pytest.raises(RuntimeError, match="boom"):
        tracker._execute_statement_raise("SELECT 1")


def test_ensure_tracking_schema_auto_create_calls_ddl(tracker: DeploymentTracker) -> None:
    with patch.object(tracker, "_execute_ddl") as execute_ddl:
        tracker.ensure_tracking_schema(auto_create=True)
    assert execute_ddl.call_count == 3


def test_ensure_tracking_schema_disabled_does_nothing(tracker: DeploymentTracker) -> None:
    with patch.object(tracker, "_execute_ddl") as execute_ddl:
        tracker.ensure_tracking_schema(auto_create=False)
    execute_ddl.assert_not_called()


def test_start_deployment_inserts_pending_record(tracker: DeploymentTracker) -> None:
    with patch.object(tracker, "_execute_ddl") as execute_ddl:
        tracker.start_deployment(
            deployment_id="dep_1",
            environment="dev",
            snapshot_version="v1",
            project_name="proj",
            provider_type="unity",
            provider_version="1.0.0",
            from_snapshot_version="v0",
            previous_deployment_id="prev_'1",
        )
    sql = execute_ddl.call_args[0][0]
    assert "INSERT INTO `cat`.`schemax`.deployments" in sql
    assert "prev_''1" in sql
    assert "'pending'" in sql


def test_record_operation_serializes_payload_and_error(tracker: DeploymentTracker) -> None:
    operation = Operation(
        id="op_'1",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.add_table",
        target="tbl_'1",
        payload={"name": "users", "schema": "raw"},
    )
    result = StatementResult(
        statement_id="stmt_1",
        sql="CREATE TABLE `x`",
        status="failed",
        execution_time_ms=1,
        error_message="can't",
        rows_affected=None,
    )
    with patch.object(tracker, "_execute_ddl") as execute_ddl:
        tracker.record_operation(
            deployment_id="dep_1",
            operation=operation,
            sql_stmt="CREATE TABLE x",
            result=result,
            execution_order=3,
        )
    sql = execute_ddl.call_args[0][0]
    assert "op_''1" in sql
    assert "tbl_''1" in sql
    assert "'failed'" in sql
    assert "can''t" in sql


def test_complete_deployment_updates_status(tracker: DeploymentTracker) -> None:
    exec_result = ExecutionResult(
        deployment_id="dep_1",
        total_statements=2,
        successful_statements=1,
        failed_statement_index=1,
        statement_results=[],
        total_execution_time_ms=100,
        status="partial",
        error_message="bad",
    )
    with patch.object(tracker, "_execute_ddl") as execute_ddl:
        tracker.complete_deployment("dep_1", exec_result, error_message="can't")
    sql = execute_ddl.call_args[0][0]
    assert "status = 'partial'" in sql
    assert "can''t" in sql


def test_get_latest_deployment_returns_version_and_id(tracker: DeploymentTracker) -> None:
    with patch.object(
        tracker,
        "_execute_statement_sync",
        return_value=_mk_response(data_array=[["v2", "dep_2", "2026-01-01 00:00:00"]]),
    ):
        latest = tracker.get_latest_deployment("dev")
    assert latest == {"version": "v2", "id": "dep_2"}


def test_get_latest_deployment_none_on_missing_catalog(tracker: DeploymentTracker) -> None:
    with patch.object(tracker, "_execute_statement_sync", return_value=None):
        assert tracker.get_latest_deployment("dev") is None


def test_get_most_recent_deployment_id(tracker: DeploymentTracker) -> None:
    with patch.object(
        tracker,
        "_execute_statement_sync",
        return_value=_mk_response(data_array=[["dep_3"]]),
    ):
        assert tracker.get_most_recent_deployment_id("dev") == "dep_3"


def test_get_deployment_by_id_merges_ops(tracker: DeploymentTracker) -> None:
    dep_response = _mk_response(
        data_array=[
            [
                "dep_1",
                "dev",
                "v1",
                None,
                None,
                "2026-01-01 00:00:00",
                "user",
                "success",
                2,
                None,
                123,
            ]
        ]
    )
    ops_response = _mk_response(
        data_array=[
            ["op_1", "unity.add_table", "tbl_1", '{"a":1}', "success", 1, None],
            ["op_2", "unity.drop_table", "tbl_2", "{bad", "failed", 2, "x"],
        ]
    )
    with patch.object(tracker, "_execute_statement_sync", return_value=dep_response):
        with patch.object(tracker, "_execute_statement_raise", return_value=ops_response):
            deployment = tracker.get_deployment_by_id("dep_1")
    assert deployment is not None
    assert deployment["opsApplied"] == ["op_1", "op_2"]
    assert deployment["successfulStatements"] == 1
    assert deployment["failedStatementIndex"] == 1


def test_get_deployment_by_id_none_on_expected_not_found(tracker: DeploymentTracker) -> None:
    with patch.object(
        tracker, "_execute_statement_sync", side_effect=RuntimeError("schema not found")
    ):
        assert tracker.get_deployment_by_id("dep_1") is None


def test_get_previous_deployment_none_when_current_missing(tracker: DeploymentTracker) -> None:
    with patch.object(tracker, "get_deployment_by_id", return_value=None):
        assert tracker.get_previous_deployment("dev", "dep_x") is None


def test_get_previous_deployment_returns_previous_record(tracker: DeploymentTracker) -> None:
    current = {"id": "dep_2", "deployedAt": "2026-01-01 00:00:00"}
    previous = {"id": "dep_1"}
    with patch.object(tracker, "get_deployment_by_id", side_effect=[current, previous]):
        with patch.object(tracker, "_query_previous_deployment_id", return_value="dep_1"):
            result = tracker.get_previous_deployment("dev", "dep_2")
    assert result == previous


def test_wait_for_statement_success(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.get_statement.return_value = _mk_response()
    tracker._wait_for_statement("stmt_1")


def test_wait_for_statement_failed_raises(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.get_statement.return_value = _mk_response(
        state=StatementState.FAILED, error_message="ddl failed"
    )
    with pytest.raises(RuntimeError, match="DDL execution failed"):
        tracker._wait_for_statement("stmt_1")


def test_wait_for_statement_timeout(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.get_statement.return_value = _mk_response(
        state=StatementState.RUNNING
    )
    with patch("schemax.core.deployment.time.sleep", return_value=None):
        with pytest.raises(TimeoutError):
            tracker._wait_for_statement("stmt_1", max_wait_seconds=1)


def test_execute_ddl_waits_when_not_immediately_succeeded(tracker: DeploymentTracker) -> None:
    tracker.client.statement_execution.execute_statement.return_value = _mk_response(
        state=StatementState.RUNNING,
        statement_id="stmt_22",
    )
    with patch.object(tracker, "_wait_for_statement") as wait_for_statement:
        tracker._execute_ddl("CREATE TABLE x")
    wait_for_statement.assert_called_once_with("stmt_22")


def test_deployment_table_ddls_contain_expected_columns(tracker: DeploymentTracker) -> None:
    deployments_ddl = tracker._get_deployments_table_ddl()
    ops_ddl = tracker._get_deployment_ops_table_ddl()
    assert "CREATE TABLE IF NOT EXISTS `cat`.`schemax`.deployments" in deployments_ddl
    assert "previous_deployment_id" in deployments_ddl
    assert "CREATE TABLE IF NOT EXISTS `cat`.`schemax`.deployment_ops" in ops_ddl
    assert "op_payload" in ops_ddl
