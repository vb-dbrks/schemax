"""
Base SQL Executor Protocol

Defines the contract for executing SQL statements against data catalogs.
Providers implement this protocol to support the `schematic apply` command.
"""

from typing import Literal, Protocol

from pydantic import BaseModel, Field


class ExecutionConfig(BaseModel):
    """Configuration for SQL execution

    Attributes:
        target_env: Target environment name (e.g., "dev", "prod")
        profile: Authentication profile name (e.g., Databricks profile)
        warehouse_id: SQL warehouse/endpoint ID for execution
        dry_run: If True, preview without executing
        no_interaction: If True, skip confirmation prompts
        timeout_seconds: Timeout for long-running statements
    """

    target_env: str = Field(..., description="Target environment name")
    profile: str = Field(..., description="Authentication profile name")
    warehouse_id: str = Field(..., description="SQL warehouse ID")
    dry_run: bool = Field(default=False, description="Preview without executing")
    no_interaction: bool = Field(default=False, description="Skip confirmation prompts")
    timeout_seconds: int = Field(default=300, description="Statement timeout in seconds")


class StatementResult(BaseModel):
    """Result of single statement execution

    Attributes:
        statement_id: Unique identifier for the statement execution
        sql: The SQL statement that was executed
        status: Execution status (success/failed/skipped)
        execution_time_ms: Time taken to execute in milliseconds
        error_message: Error details if execution failed
        rows_affected: Number of rows affected (if applicable)
        result_data: Query result data for SELECT statements (list of row dicts)
    """

    statement_id: str = Field(..., description="Statement execution ID")
    sql: str = Field(..., description="SQL statement")
    status: Literal["success", "failed", "skipped"] = Field(..., description="Execution status")
    execution_time_ms: int = Field(default=0, description="Execution time in milliseconds")
    error_message: str | None = Field(None, description="Error message if failed")
    rows_affected: int | None = Field(None, description="Rows affected")
    result_data: list[dict[str, object]] | None = Field(
        None, description="Query result data (for SELECT statements)"
    )


class ExecutionResult(BaseModel):
    """Result of full SQL execution

    Attributes:
        deployment_id: Unique deployment identifier
        total_statements: Total number of statements to execute
        successful_statements: Number of statements that succeeded
        failed_statement_index: Index of first failed statement (if any)
        statement_results: Detailed results for each statement
        total_execution_time_ms: Total execution time in milliseconds
        status: Overall execution status
        error_message: Summary error message (if failed)
    """

    deployment_id: str = Field(..., description="Deployment ID")
    total_statements: int = Field(..., description="Total statements")
    successful_statements: int = Field(default=0, description="Successful statements")
    failed_statement_index: int | None = Field(None, description="First failed statement index")
    statement_results: list[StatementResult] = Field(
        default_factory=list, description="Statement results"
    )
    total_execution_time_ms: int = Field(default=0, description="Total execution time (ms)")
    status: Literal["success", "failed", "partial"] = Field(..., description="Overall status")
    error_message: str | None = Field(None, description="Error summary")


class SQLExecutor(Protocol):
    """Protocol for executing SQL statements against a data catalog

    Providers implement this protocol to support SQL execution via `schematic apply`.
    Executors handle authentication, statement submission, polling, and error handling.
    """

    def execute_statements(self, statements: list[str], config: ExecutionConfig) -> ExecutionResult:
        """Execute SQL statements sequentially

        Args:
            statements: List of SQL statements to execute
            config: Execution configuration (auth, warehouse, etc.)

        Returns:
            ExecutionResult with detailed execution information

        Raises:
            AuthenticationError: If authentication fails
            ExecutionError: If statement execution fails
        """
        ...
