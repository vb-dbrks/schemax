"""
Unity Catalog SQL Executor

Executes SQL statements against Databricks Unity Catalog using
the Databricks SQL Statement Execution API.
"""

import time
from typing import List
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from rich.console import Console

from ..base.executor import ExecutionConfig, ExecutionResult, StatementResult

console = Console()


class ExecutionError(Exception):
    """Raised when SQL execution fails"""

    pass


class UnitySQLExecutor:
    """Execute SQL statements against Databricks Unity Catalog

    Uses Databricks SQL Statement Execution API v2.0 to submit and execute
    SQL statements. Polls for completion and handles errors with fail-fast behavior.

    Attributes:
        client: Authenticated Databricks WorkspaceClient
    """

    def __init__(self, client: WorkspaceClient):
        """Initialize executor with authenticated client

        Args:
            client: Authenticated Databricks WorkspaceClient
        """
        self.client = client

    def execute_statements(self, statements: List[str], config: ExecutionConfig) -> ExecutionResult:
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
        results: List[StatementResult] = []
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

                    return ExecutionResult(
                        deployment_id=deployment_id,
                        total_statements=len(statements),
                        successful_statements=i - 1,
                        failed_statement_index=i - 1,
                        statement_results=results,
                        total_execution_time_ms=total_time_ms,
                        status="partial",
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

                return ExecutionResult(
                    deployment_id=deployment_id,
                    total_statements=len(statements),
                    successful_statements=i - 1,
                    failed_statement_index=i - 1,
                    statement_results=results,
                    total_execution_time_ms=total_time_ms,
                    status="partial",
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
            # Submit statement (non-blocking)
            response = self.client.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=sql,
                wait_timeout="0s",  # Don't wait, we'll poll manually
            )

            statement_id = response.statement_id

            # Poll for completion
            elapsed = 0
            while elapsed < config.timeout_seconds:
                status_response = self.client.statement_execution.get_statement(statement_id)

                state = status_response.status.state

                # Check if completed
                if state == StatementState.SUCCEEDED:
                    exec_time_ms = int((time.time() - exec_start) * 1000)

                    return StatementResult(
                        statement_id=statement_id,
                        sql=sql,
                        status="success",
                        execution_time_ms=exec_time_ms,
                        rows_affected=None,  # Could extract from result if needed
                    )

                elif state == StatementState.FAILED:
                    error = status_response.status.error
                    error_msg = error.message if error else "Unknown error"

                    return StatementResult(
                        statement_id=statement_id,
                        sql=sql,
                        status="failed",
                        execution_time_ms=int((time.time() - exec_start) * 1000),
                        error_message=error_msg,
                        rows_affected=None,
                    )

                elif state == StatementState.CANCELED:
                    return StatementResult(
                        statement_id=statement_id,
                        sql=sql,
                        status="failed",
                        execution_time_ms=int((time.time() - exec_start) * 1000),
                        error_message="Statement was canceled",
                        rows_affected=None,
                    )

                # Still running, wait and poll again
                time.sleep(2)
                elapsed = time.time() - exec_start

            # Timeout reached
            return StatementResult(
                statement_id=statement_id,
                sql=sql,
                status="failed",
                execution_time_ms=int((time.time() - exec_start) * 1000),
                error_message=f"Statement execution timed out after {config.timeout_seconds}s",
                rows_affected=None,
            )

        except Exception as e:
            # Catch any API errors
            return StatementResult(
                statement_id=f"stmt_error_{statement_num}",
                sql=sql,
                status="failed",
                execution_time_ms=int((time.time() - exec_start) * 1000),
                error_message=f"API error: {e}",
                rows_affected=None,
            )
