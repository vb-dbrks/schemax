"""Unit tests for LocalSQLRunner and RemoteSQLRunner."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from schemax.providers.unity.sql_runner import (
    LocalSQLRunner,
    RemoteSQLRunner,
)


class TestLocalSQLRunner:
    def test_run_sql_success_with_results(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.collect.return_value = [
            MagicMock(**{"__getitem__": lambda self, col: {"id": 1, "name": "a"}[col]}),
        ]
        # Make row[col] work
        row_mock = MagicMock()
        row_mock.__getitem__ = lambda self, col: {"id": 1, "name": "a"}[col]
        mock_df.collect.return_value = [row_mock]

        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df

        runner = LocalSQLRunner()
        with patch.object(runner, "_get_spark_session", return_value=mock_spark):
            result = runner.run_sql("SELECT 1 AS id, 'a' AS name")

        assert result.status == "success"
        assert result.columns == ["id", "name"]
        assert result.data_array == [[1, "a"]]

    def test_run_sql_ddl_success(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = []
        mock_df.collect.side_effect = Exception("No results for DDL")

        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df

        runner = LocalSQLRunner()
        with patch.object(runner, "_get_spark_session", return_value=mock_spark):
            result = runner.run_sql("CREATE TABLE test (id INT)")

        assert result.status == "success"

    def test_run_sql_failure(self) -> None:
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Table not found")

        runner = LocalSQLRunner()
        with patch.object(runner, "_get_spark_session", return_value=mock_spark):
            result = runner.run_sql("SELECT * FROM nonexistent")

        assert result.status == "failed"
        assert "Table not found" in (result.error_message or "")

    def test_no_pyspark_raises_clear_error(self) -> None:
        import schemax.providers.unity.sql_runner as runner_module

        runner = LocalSQLRunner()
        original = runner_module._PYSPARK_AVAILABLE
        try:
            runner_module._PYSPARK_AVAILABLE = False
            with pytest.raises(RuntimeError, match="PySpark"):
                runner._get_spark_session()
        finally:
            runner_module._PYSPARK_AVAILABLE = original

    def test_no_active_session_raises_clear_error(self) -> None:
        import schemax.providers.unity.sql_runner as runner_module

        runner = LocalSQLRunner()
        mock_session_cls = MagicMock()
        mock_session_cls.getActiveSession.return_value = None
        original_spark = runner_module.SparkSession
        original_available = runner_module._PYSPARK_AVAILABLE
        try:
            runner_module.SparkSession = mock_session_cls
            runner_module._PYSPARK_AVAILABLE = True
            with pytest.raises(RuntimeError, match="No active SparkSession"):
                runner._get_spark_session()
        finally:
            runner_module.SparkSession = original_spark
            runner_module._PYSPARK_AVAILABLE = original_available


class TestRemoteSQLRunner:
    def test_run_sql_success(self) -> None:
        from databricks.sdk.service.sql import StatementState

        mock_client = MagicMock()
        response = SimpleNamespace(
            status=SimpleNamespace(state=StatementState.SUCCEEDED, error=None),
            result=SimpleNamespace(data_array=[["val1", "val2"]]),
            manifest=SimpleNamespace(
                schema=SimpleNamespace(
                    columns=[
                        SimpleNamespace(name="col1"),
                        SimpleNamespace(name="col2"),
                    ]
                )
            ),
            statement_id="stmt_1",
        )
        mock_client.statement_execution.execute_statement.return_value = response

        runner = RemoteSQLRunner(mock_client, "wh_123")
        result = runner.run_sql("SELECT 1")

        assert result.status == "success"
        assert result.data_array == [["val1", "val2"]]
        assert result.columns == ["col1", "col2"]

    def test_run_sql_failure(self) -> None:
        from databricks.sdk.service.sql import StatementState

        mock_client = MagicMock()
        response = SimpleNamespace(
            status=SimpleNamespace(
                state=StatementState.FAILED,
                error=SimpleNamespace(message="syntax error"),
            ),
            result=None,
            manifest=None,
            statement_id="stmt_1",
        )
        mock_client.statement_execution.execute_statement.return_value = response

        runner = RemoteSQLRunner(mock_client, "wh_123")
        result = runner.run_sql("BAD SQL")

        assert result.status == "failed"
        assert "syntax error" in (result.error_message or "")

    def test_run_sql_exception(self) -> None:
        mock_client = MagicMock()
        mock_client.statement_execution.execute_statement.side_effect = Exception("connection lost")

        runner = RemoteSQLRunner(mock_client, "wh_123")
        result = runner.run_sql("SELECT 1")

        assert result.status == "failed"
        assert "connection lost" in (result.error_message or "")


class TestCreateRemoteSQLRunner:
    def test_creates_remote_runner(self) -> None:
        from schemax.providers.unity.sql_runner import create_remote_sql_runner

        mock_client = MagicMock()
        runner = create_remote_sql_runner(mock_client, "wh_123")
        assert isinstance(runner, RemoteSQLRunner)
        assert runner.warehouse_id == "wh_123"
