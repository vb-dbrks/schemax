"""
Unity Catalog SQL Executor

Executes SQL statements against Databricks Unity Catalog using
the Databricks SQL Statement Execution API.
"""

import time
from typing import Any
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from rich.console import Console

from schemax.providers.base.executor import ExecutionConfig, ExecutionResult, StatementResult

console = Console()


class ExecutionError(Exception):
    """Raised when SQL execution fails"""


class UnitySQLExecutor:
    """Execute SQL statements against Databricks Unity Catalog

    Uses Databricks SQL Statement Execution API v2.0 to submit and execute
    SQL statements. Polls for completion and handles errors with fail-fast behavior.

    Attributes:
        client: Authenticated Databricks WorkspaceClient
    """

    def __init__(self, client: WorkspaceClient) -> None:
        """Initialize executor with authenticated client

        Args:
            client: Authenticated Databricks WorkspaceClient
        """
        self.client = client

    def execute_statements(self, statements: list[str], config: ExecutionConfig) -> ExecutionResult:
        """Execute SQL statements sequentially with fail-fast behavior

        Executes each statement in order. If any statement fails, execution stops
        immediately and returns a partial result. Progress updates are shown
        during execution.

        Args:
            statements: List of SQL statements to execute
            config: Execution configuration (warehouse, timeout, etc.)

        Returns:
            ExecutionResult with detailed execution information

        Raises:
            ExecutionError: If execution fails critically
        """
        deployment_id = f"deploy_{uuid4().hex[:8]}"
        results: list[StatementResult] = []
        start_time = time.time()

        console.print(f"\n[bold cyan]Executing {len(statements)} statements...[/bold cyan]\n")

        for i, sql in enumerate(statements, 1):
            console.print(f"[cyan]Statement {i}/{len(statements)}[/cyan]")

            try:
                result = self._execute_single_statement(sql, config, i)
                results.append(result)

                # Show result
                if result.status == "success":
                    exec_time = result.execution_time_ms / 1000
                    console.print(f"  [green]✓[/green] Completed in {exec_time:.2f}s")
                elif result.status == "failed":
                    console.print(f"  [red]✗[/red] Failed: {result.error_message}")

                    # Fail-fast: stop on first error
                    total_time_ms = int((time.time() - start_time) * 1000)
                    successful_count = i - 1

                    # Determine status: "failed" if 0 succeeded, "partial" if some succeeded
                    status = "failed" if successful_count == 0 else "partial"

                    return ExecutionResult(
                        deployment_id=deployment_id,
                        total_statements=len(statements),
                        successful_statements=successful_count,
                        failed_statement_index=i - 1,
                        statement_results=results,
                        total_execution_time_ms=total_time_ms,
                        status=status,
                        error_message=result.error_message,
                    )

            except Exception as e:
                # Handle unexpected execution error
                error_msg = f"Unexpected error executing statement {i}: {e}"
                console.print(f"  [red]✗[/red] {error_msg}")

                results.append(
                    StatementResult(
                        statement_id=f"stmt_error_{i}",
                        sql=sql,
                        status="failed",
                        execution_time_ms=0,
                        error_message=str(e),
                        rows_affected=None,
                    )
                )

                total_time_ms = int((time.time() - start_time) * 1000)
                successful_count = i - 1

                # Determine status: "failed" if 0 succeeded, "partial" if some succeeded
                status = "failed" if successful_count == 0 else "partial"

                return ExecutionResult(
                    deployment_id=deployment_id,
                    total_statements=len(statements),
                    successful_statements=successful_count,
                    failed_statement_index=i - 1,
                    statement_results=results,
                    total_execution_time_ms=total_time_ms,
                    status=status,
                    error_message=error_msg,
                )

        # All statements succeeded
        total_time_ms = int((time.time() - start_time) * 1000)

        return ExecutionResult(
            deployment_id=deployment_id,
            total_statements=len(statements),
            successful_statements=len(statements),
            failed_statement_index=None,
            statement_results=results,
            total_execution_time_ms=total_time_ms,
            status="success",
            error_message=None,
        )

    def _execute_single_statement(
        self, sql: str, config: ExecutionConfig, statement_num: int
    ) -> StatementResult:
        """Execute a single SQL statement with polling

        Submits statement to Databricks, then polls for completion every 2 seconds.
        Times out after config.timeout_seconds.

        Args:
            sql: SQL statement to execute
            config: Execution configuration
            statement_num: Statement number for display

        Returns:
            StatementResult with execution details

        Raises:
            ExecutionError: If execution fails critically
        """
        exec_start = time.time()

        try:
            response = self._submit_statement(sql, config)
            statement_id = response.statement_id or ""
            terminal = self._poll_until_terminal(statement_id, config.timeout_seconds)

            if terminal is None:
                return self._make_failure_result(
                    statement_id,
                    sql,
                    exec_start,
                    f"Statement execution timed out after {config.timeout_seconds}s",
                )
            return self._handle_terminal_state(terminal, statement_id, sql, exec_start)
        except ExecutionError:
            raise
        except Exception as e:
            return self._make_failure_result(
                f"stmt_error_{statement_num}", sql, exec_start, f"API error: {e}"
            )

    def _submit_statement(self, sql: str, config: ExecutionConfig) -> Any:
        """Submit a SQL statement for async execution via the Databricks API."""
        return self.client.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=sql,
            wait_timeout="0s",
        )

    def _poll_until_terminal(self, statement_id: str, timeout_seconds: int) -> Any | None:
        """Poll until the statement reaches a terminal state or times out.

        Returns the final status response, or None on timeout.
        """
        start = time.time()
        while (time.time() - start) < timeout_seconds:
            resp = self.client.statement_execution.get_statement(statement_id)
            if not resp or not resp.status:
                raise ExecutionError("Failed to get statement status")
            state = resp.status.state
            if state in (StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED):
                return resp
            time.sleep(2)
        return None

    def _handle_terminal_state(
        self, response: Any, statement_id: str, sql: str, exec_start: float
    ) -> StatementResult:
        """Dispatch on a terminal statement state to build the appropriate result."""
        state = response.status.state

        if state == StatementState.SUCCEEDED:
            return StatementResult(
                statement_id=statement_id,
                sql=sql,
                status="success",
                execution_time_ms=int((time.time() - exec_start) * 1000),
                rows_affected=None,
                error_message=None,
                result_data=self._parse_result_data(response),
            )

        if state == StatementState.FAILED:
            error = response.status.error
            error_msg = error.message if error else "Unknown error"
            return self._make_failure_result(statement_id, sql, exec_start, error_msg)

        # CANCELED
        return self._make_failure_result(statement_id, sql, exec_start, "Statement was canceled")

    @staticmethod
    def _parse_result_data(response: Any) -> list[dict[str, object]] | None:
        """Extract tabular result data from a SUCCEEDED response."""
        if not (
            response.result
            and response.result.data_array
            and response.manifest
            and response.manifest.schema
            and response.manifest.schema.columns
        ):
            return None
        columns = [col.name for col in response.manifest.schema.columns]
        return [
            {columns[i]: v for i, v in enumerate(row) if i < len(columns)}
            for row in response.result.data_array
        ]

    @staticmethod
    def _make_failure_result(
        statement_id: str, sql: str, exec_start: float, error_message: str
    ) -> StatementResult:
        """Build a failed StatementResult."""
        return StatementResult(
            statement_id=statement_id,
            sql=sql,
            status="failed",
            execution_time_ms=int((time.time() - exec_start) * 1000),
            error_message=error_message,
            rows_affected=None,
        )
