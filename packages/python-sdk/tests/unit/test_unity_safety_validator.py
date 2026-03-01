"""Unit tests for Unity safety validator classification and query helpers."""

from __future__ import annotations

from schemax.providers.base.executor import ExecutionConfig, ExecutionResult, StatementResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel
from schemax.providers.unity.safety_validator import SafetyValidator


def _mk_validator(results: list[ExecutionResult]) -> SafetyValidator:
    queue = list(results)

    class _Executor:
        def execute_statements(
            self, statements: list[str], config: ExecutionConfig
        ) -> ExecutionResult:
            del statements, config
            return queue.pop(0) if queue else ExecutionResult.empty()

    return SafetyValidator(
        executor=_Executor(),
        config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_1"),
    )


def _count_result(cnt: int | None) -> ExecutionResult:
    return ExecutionResult(
        deployment_id="dep",
        total_statements=1,
        successful_statements=1,
        failed_statement_index=None,
        statement_results=[
            StatementResult(
                statement_id="s1",
                sql="SELECT",
                status="success",
                execution_time_ms=1,
                error_message=None,
                rows_affected=None,
                result_data=[{"cnt": cnt}],
            )
        ],
        total_execution_time_ms=1,
        status="success",
        error_message=None,
    )


def _rows_result(rows: list[dict[str, object]]) -> ExecutionResult:
    return ExecutionResult(
        deployment_id="dep",
        total_statements=1,
        successful_statements=1,
        failed_statement_index=None,
        statement_results=[
            StatementResult(
                statement_id="s1",
                sql="SELECT",
                status="success",
                execution_time_ms=1,
                error_message=None,
                rows_affected=None,
                result_data=rows,
            )
        ],
        total_execution_time_ms=1,
        status="success",
        error_message=None,
    )


def test_validate_drop_table_safe_risky_destructive() -> None:
    op = Operation(
        id="op1",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_table",
        target="bronze.raw.events",
        payload={},
    )

    safe = _mk_validator([_count_result(0)]).validate(op, {"bronze": "dev_bronze"})
    assert safe.level == SafetyLevel.SAFE

    risky = _mk_validator([_count_result(5), _rows_result([{"id": 1}])]).validate(
        op, {"bronze": "dev_bronze"}
    )
    assert risky.level == SafetyLevel.RISKY
    assert risky.sample_data is not None

    destructive = _mk_validator([_count_result(5000), _rows_result([{"id": 1}])]).validate(
        op, {"bronze": "dev_bronze"}
    )
    assert destructive.level == SafetyLevel.DESTRUCTIVE


def test_validate_drop_schema_and_catalog_paths() -> None:
    drop_schema = Operation(
        id="op2",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_schema",
        target="bronze.raw",
        payload={},
    )
    report_schema = _mk_validator([_count_result(3)]).validate(
        drop_schema, {"bronze": "dev_bronze"}
    )
    assert report_schema.level == SafetyLevel.RISKY

    drop_catalog = Operation(
        id="op3",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_catalog",
        target="bronze",
        payload={},
    )
    report_catalog = _mk_validator([_count_result(2)]).validate(
        drop_catalog, {"bronze": "dev_bronze"}
    )
    assert report_catalog.level == SafetyLevel.DESTRUCTIVE


def test_validate_drop_column_and_type_change() -> None:
    drop_col = Operation(
        id="op4",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_column",
        target="col_target",
        payload={"tableId": "bronze.raw.events", "name": "event_type"},
    )
    report_col = _mk_validator([_count_result(4), _rows_result([{"event_type": "x"}])]).validate(
        drop_col, {"bronze": "dev_bronze"}
    )
    assert report_col.level == SafetyLevel.RISKY

    type_change = Operation(
        id="op5",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.change_column_type",
        target="x",
        payload={},
    )
    report_type = _mk_validator([]).validate(type_change)
    assert report_type.level == SafetyLevel.RISKY


def test_safe_fallback_and_helper_queries() -> None:
    noop = Operation(
        id="op6",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.add_grant",
        target="x",
        payload={},
    )
    validator = _mk_validator([_count_result(None), _rows_result([{"k": "v"}])])
    report = validator.validate(noop)
    assert report.level == SafetyLevel.SAFE
    assert validator._get_physical_name("bronze", {"bronze": "dev_bronze"}) == "dev_bronze"
    assert validator._query_count("SELECT COUNT(*) as cnt FROM x") == 0
    assert validator._query("SELECT * FROM x") == [{"k": "v"}]


def test_query_sample_handles_errors() -> None:
    class _BoomExecutor:
        def execute_statements(
            self, statements: list[str], config: ExecutionConfig
        ) -> ExecutionResult:
            del statements, config
            raise RuntimeError("boom")

    validator = SafetyValidator(
        executor=_BoomExecutor(),
        config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_1"),
    )
    assert validator._query_sample("`a`.`b`.`c`", limit=2) == []


def test_drop_catalog_and_schema_exception_paths() -> None:
    class _BoomExecutor:
        def execute_statements(
            self, statements: list[str], config: ExecutionConfig
        ) -> ExecutionResult:
            del statements, config
            raise RuntimeError("missing")

    validator = SafetyValidator(
        executor=_BoomExecutor(),
        config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_1"),
    )

    drop_catalog = Operation(
        id="op7",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_catalog",
        target="bronze",
        payload={},
    )
    report_catalog = validator.validate(drop_catalog, {"bronze": "dev_bronze"})
    assert report_catalog.level == SafetyLevel.SAFE

    drop_schema_bad = Operation(
        id="op8",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_schema",
        target="bad",
        payload={},
    )
    report_schema_bad = validator.validate(drop_schema_bad, {"bronze": "dev_bronze"})
    assert report_schema_bad.level == SafetyLevel.SAFE


def test_drop_schema_safe_and_destructive_paths() -> None:
    drop_schema = Operation(
        id="op9",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_schema",
        target="bronze.raw",
        payload={},
    )
    safe = _mk_validator([_count_result(0)]).validate(drop_schema, {"bronze": "dev_bronze"})
    assert safe.level == SafetyLevel.SAFE
    destructive = _mk_validator([_count_result(5000)]).validate(
        drop_schema, {"bronze": "dev_bronze"}
    )
    assert destructive.level == SafetyLevel.DESTRUCTIVE


def test_drop_table_invalid_and_exception_paths() -> None:
    drop_bad = Operation(
        id="op10",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_table",
        target="bad",
        payload={},
    )
    report_bad = _mk_validator([]).validate(drop_bad, {"bronze": "dev_bronze"})
    assert report_bad.level == SafetyLevel.SAFE

    class _BoomExecutor:
        def execute_statements(
            self, statements: list[str], config: ExecutionConfig
        ) -> ExecutionResult:
            del statements, config
            raise RuntimeError("boom")

    validator = SafetyValidator(
        executor=_BoomExecutor(),
        config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_1"),
    )
    drop_ok = Operation(
        id="op11",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_table",
        target="bronze.raw.events",
        payload={},
    )
    report_ok = validator.validate(drop_ok, {"bronze": "dev_bronze"})
    assert report_ok.level == SafetyLevel.SAFE


def test_drop_column_invalid_and_exception_and_destructive_paths() -> None:
    drop_invalid = Operation(
        id="op12",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op="unity.drop_column",
        target="x",
        payload={"tableId": "x", "name": ""},
    )
    report_invalid = _mk_validator([]).validate(drop_invalid)
    assert report_invalid.level == SafetyLevel.SAFE

    destructive = _mk_validator(
        [_count_result(5000), _rows_result([{"event_type": "x"}])]
    ).validate(
        Operation(
            id="op13",
            ts="2026-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_column",
            target="x",
            payload={"tableId": "bronze.raw.events", "name": "event_type"},
        ),
        {"bronze": "dev_bronze"},
    )
    assert destructive.level == SafetyLevel.DESTRUCTIVE

    class _BoomExecutor:
        def execute_statements(
            self, statements: list[str], config: ExecutionConfig
        ) -> ExecutionResult:
            del statements, config
            raise RuntimeError("boom")

    validator = SafetyValidator(
        executor=_BoomExecutor(),
        config=ExecutionConfig(target_env="dev", profile="DEFAULT", warehouse_id="wh_1"),
    )
    report_exception = validator.validate(
        Operation(
            id="op14",
            ts="2026-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_column",
            target="x",
            payload={"tableId": "bronze.raw.events", "name": "event_type"},
        ),
        {"bronze": "dev_bronze"},
    )
    assert report_exception.level == SafetyLevel.SAFE
