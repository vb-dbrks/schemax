"""Unit tests for application service serialization boundaries."""

from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from schemax.application.services import ApplyService
from schemax.providers.base.executor import ExecutionResult, StatementResult


def test_apply_service_serializes_statement_rows_affected(monkeypatch: MonkeyPatch) -> None:
    """ApplyService should serialize statement results using rows_affected."""

    def _fake_apply(**_kwargs: object) -> ExecutionResult:
        return ExecutionResult(
            deployment_id="dep_1",
            total_statements=1,
            successful_statements=1,
            failed_statement_index=None,
            statement_results=[
                StatementResult(
                    statement_id="stmt_1",
                    sql="SELECT 1",
                    status="success",
                    execution_time_ms=10,
                    rows_affected=7,
                    error_message=None,
                    result_data=None,
                )
            ],
            total_execution_time_ms=10,
            status="success",
            error_message=None,
        )

    monkeypatch.setattr("schemax.application.services.apply_to_environment", _fake_apply)

    result = ApplyService().run(
        workspace=Path("."),
        target_env="dev",
        profile="DEFAULT",
        warehouse_id="wh_1",
        dry_run=False,
        no_interaction=True,
        auto_rollback=False,
    )

    assert result.success is True
    assert result.data is not None
    statement = result.data["result"]["statement_results"][0]
    assert statement["rows_affected"] == 7
    assert "row_count" not in statement
