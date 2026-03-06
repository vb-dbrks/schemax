"""SQL Runner implementations for Unity Catalog.

Provides RemoteSQLRunner (Statement Execution API) and LocalSQLRunner (spark.sql)
behind the SQLRunner protocol, plus a factory function.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from databricks.sdk.service.sql import StatementState

from schemax.providers.base.executor import SQLRunResult

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

try:
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    _PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    _PYSPARK_AVAILABLE = False


class RemoteSQLRunner:
    """Executes SQL via the Databricks Statement Execution API."""

    def __init__(self, client: WorkspaceClient, warehouse_id: str) -> None:
        self.client = client
        self.warehouse_id = warehouse_id

    def run_sql(self, sql: str) -> SQLRunResult:
        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="30s",
            )
        except Exception as e:
            return SQLRunResult(status="failed", error_message=str(e))

        # Poll if still pending/running (e.g. warehouse cold start)
        if response.status and response.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            response = self._poll_statement(response)

        if not response.status or response.status.state != StatementState.SUCCEEDED:
            error_msg = ""
            if response.status and response.status.error:
                error_msg = str(getattr(response.status.error, "message", None) or "").strip()
            if not error_msg:
                state_obj = getattr(response.status, "state", None) if response.status else None
                state_str = (
                    getattr(state_obj, "name", None) or str(state_obj)
                    if state_obj is not None
                    else "unknown"
                )
                error_msg = f"Statement failed (state={state_str})"
            return SQLRunResult(status="failed", error_message=error_msg)

        # Extract result data
        data_array = None
        columns = None
        if response.result and response.result.data_array:
            data_array = response.result.data_array
        if response.manifest and response.manifest.schema and response.manifest.schema.columns:
            columns = [col.name for col in response.manifest.schema.columns]

        return SQLRunResult(status="success", data_array=data_array, columns=columns)

    def _poll_statement(self, response: Any, max_wait_seconds: int = 120) -> Any:
        statement_id = response.statement_id
        elapsed = 0
        while elapsed < max_wait_seconds:
            time.sleep(2)
            elapsed += 2
            response = self.client.statement_execution.get_statement(statement_id)
            if not response.status:
                continue
            if response.status.state not in (StatementState.PENDING, StatementState.RUNNING):
                return response
        return response


class LocalSQLRunner:
    """Executes SQL via spark.sql() on the active Spark session."""

    def run_sql(self, sql: str) -> SQLRunResult:
        spark = self._get_spark_session()
        try:
            df = spark.sql(sql)
            # Collect results for SELECT-like statements
            try:
                columns = list(df.columns)
                rows = [[row[col] for col in columns] for row in df.collect()]
                return SQLRunResult(
                    status="success",
                    data_array=rows if rows else None,
                    columns=columns if columns else None,
                )
            except Exception:
                # DDL statements may not return meaningful results
                return SQLRunResult(status="success")
        except Exception as e:
            return SQLRunResult(status="failed", error_message=str(e))

    @staticmethod
    def _get_spark_session() -> Any:
        if not _PYSPARK_AVAILABLE:
            raise RuntimeError(
                "Local execution mode requires PySpark. "
                "This mode is intended for use inside Databricks serverless jobs."
            )

        session = SparkSession.getActiveSession()
        if session is None:
            raise RuntimeError(
                "No active SparkSession found. "
                "Local execution mode requires a running Spark session "
                "(e.g. inside a Databricks serverless job)."
            )
        return session


def create_remote_sql_runner(client: WorkspaceClient, warehouse_id: str) -> RemoteSQLRunner:
    """Create a RemoteSQLRunner from a WorkspaceClient and warehouse_id."""
    return RemoteSQLRunner(client, warehouse_id)
