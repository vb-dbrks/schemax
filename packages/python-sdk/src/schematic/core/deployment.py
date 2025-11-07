"""
Deployment Tracker

Tracks deployments in the target catalog's schematic schema.
Provides database-backed deployment history and audit trail.
"""

from typing import Any

from databricks.sdk import WorkspaceClient

from schematic.providers.base.executor import ExecutionResult, StatementResult
from schematic.providers.base.operations import Operation


class DeploymentTracker:
    """Track deployments in target catalog's schematic schema

    Creates and manages deployment tracking tables in <catalog>.schematic:
    - deployments: Main deployment records
    - deployment_ops: Individual operation tracking

    Attributes:
        client: Authenticated Databricks WorkspaceClient
        catalog: Target catalog name
        schema: Schematic schema name (<catalog>.schematic)
        warehouse_id: SQL warehouse ID for execution
    """

    def __init__(self, client: WorkspaceClient, catalog: str, warehouse_id: str):
        """Initialize deployment tracker

        Args:
            client: Authenticated Databricks WorkspaceClient
            catalog: Target catalog name
            warehouse_id: SQL warehouse ID for DDL execution
        """
        self.client = client
        self.catalog = catalog
        self.schema = f"`{catalog}`.`schematic`"
        self.warehouse_id = warehouse_id

    def ensure_tracking_schema(self, auto_create: bool = True) -> None:
        """Create schematic schema and tables if needed

        Args:
            auto_create: If True, create schema and tables automatically
        """
        if not auto_create:
            return

        # Create schema
        self._execute_ddl(
            f"CREATE SCHEMA IF NOT EXISTS {self.schema} "
            f"COMMENT 'Schematic deployment tracking and metadata'"
        )

        # Create deployments table
        self._execute_ddl(self._get_deployments_table_ddl())

        # Create deployment_ops table
        self._execute_ddl(self._get_deployment_ops_table_ddl())

    def start_deployment(
        self,
        deployment_id: str,
        environment: str,
        snapshot_version: str,
        project_name: str,
        provider_type: str,
        provider_version: str,
        schematic_version: str = "0.2.0",
        from_snapshot_version: str | None = None,
    ) -> None:
        """Record deployment start (status: pending)

        Args:
            deployment_id: Unique deployment ID
            environment: Target environment (dev/test/prod)
            snapshot_version: Version being deployed (or 'changelog')
            project_name: Schematic project name
            provider_type: Provider type (unity/hms)
            provider_version: Provider version
            schematic_version: Schematic CLI version
            from_snapshot_version: Previous snapshot version (for diff tracking)
        """
        from_version_sql = f"'{from_snapshot_version}'" if from_snapshot_version else "NULL"
        
        sql = f"""
        INSERT INTO {self.schema}.deployments
        (id, environment, snapshot_version, from_snapshot_version, deployed_at, deployed_by,
         project_name, provider_type, provider_version, status, schematic_version)
        VALUES (
            '{deployment_id}',
            '{environment}',
            '{snapshot_version}',
            {from_version_sql},
            current_timestamp(),
            current_user(),
            '{project_name}',
            '{provider_type}',
            '{provider_version}',
            'pending',
            '{schematic_version}'
        )
        """
        self._execute_ddl(sql)

    def record_operation(
        self,
        deployment_id: str,
        op: Operation,
        sql_stmt: str,
        result: StatementResult,
        execution_order: int,
    ) -> None:
        """Record individual operation result

        Args:
            deployment_id: Parent deployment ID
            op: Operation that was executed
            sql_stmt: Generated SQL statement
            result: Statement execution result
            execution_order: Order of execution (1-indexed)
        """
        import json

        # Escape single quotes in strings
        op_id = op.id.replace("'", "''")
        op_type = op.op.replace("'", "''")
        op_target = op.target.replace("'", "''") if op.target else ""
        
        # Serialize payload as JSON for exact matching
        payload_json = json.dumps(op.payload, sort_keys=True)
        payload_escaped = payload_json.replace("'", "''")
        
        sql_escaped = sql_stmt.replace("'", "''")
        error_msg = result.error_message.replace("'", "''") if result.error_message else ""

        sql = f"""
        INSERT INTO {self.schema}.deployment_ops
        (deployment_id, op_id, op_type, op_target, op_payload, sql_statement,
         executed_at, execution_order, status, error_message)
        VALUES (
            '{deployment_id}',
            '{op_id}',
            '{op_type}',
            '{op_target}',
            '{payload_escaped}',
            '{sql_escaped}',
            current_timestamp(),
            {execution_order},
            '{result.status}',
            {f"'{error_msg}'" if result.error_message else "NULL"}
        )
        """
        self._execute_ddl(sql)

    def complete_deployment(
        self, deployment_id: str, result: ExecutionResult, error_message: str | None = None
    ) -> None:
        """Update deployment status (success/failed/partial)

        Args:
            deployment_id: Deployment ID to update
            result: Overall execution result
            error_message: Error summary (if failed)
        """
        error_escaped = error_message.replace("'", "''") if error_message else ""

        sql = f"""
        UPDATE {self.schema}.deployments
        SET
            status = '{result.status}',
            ops_count = {result.total_statements},
            execution_time_ms = {result.total_execution_time_ms},
            error_message = {f"'{error_escaped}'" if error_message else "NULL"}
        WHERE id = '{deployment_id}'
        """
        self._execute_ddl(sql)

    def get_latest_deployment(self, environment: str) -> dict[str, str] | None:
        """Get the latest successful deployment for an environment

        Queries the database (source of truth) to find the most recent
        successful deployment.

        Returns None if:
        - Catalog doesn't exist (expected on first deployment)
        - Tracking schema doesn't exist (expected on first deployment)
        - No successful deployments found (expected on first deployment)

        Raises exception for real errors:
        - Connection failures
        - Authentication errors
        - Timeout errors
        - Permission errors

        Args:
            environment: Target environment name (dev/test/prod)

        Returns:
            Deployment record with 'version' and 'id', or None if first deployment

        Raises:
            Exception: For database connection or query errors (not catalog-not-found)
        """
        from databricks.sdk.service.sql import StatementState

        sql = f"""
        SELECT snapshot_version, id, deployed_at
        FROM {self.schema}.deployments
        WHERE environment = '{environment}'
          AND status = 'success'
        ORDER BY deployed_at DESC
        LIMIT 1
        """

        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="10s",
            )

            # Check if query succeeded
            if not response.status or response.status.state != StatementState.SUCCEEDED:
                # Check error message to distinguish catalog-not-found from real errors
                error_msg = ""
                if response.status and response.status.error:
                    error_msg = str(response.status.error.message or "")

                # Expected errors on first deployment (catalog/schema doesn't exist)
                if any(
                    pattern in error_msg.lower()
                    for pattern in [
                        "catalog",
                        "schema",
                        "not found",
                        "does not exist",
                        "table_or_view_not_found",
                    ]
                ):
                    return None

                # Real error - raise it
                raise Exception(f"Database query failed: {error_msg}")

            # Parse results
            if response.result and response.result.data_array:
                # Result format: [['v0.3.0', 'deploy_xyz', '2025-11-06T...']]
                if len(response.result.data_array) > 0:
                    row = response.result.data_array[0]
                    return {"version": row[0], "id": row[1]}

            # No rows returned - first deployment
            return None

        except Exception as e:
            # Check if it's a catalog/schema not found error (expected on first deployment)
            error_str = str(e).lower()
            if any(
                pattern in error_str
                for pattern in [
                    "catalog",
                    "schema",
                    "not found",
                    "does not exist",
                    "table_or_view_not_found",
                ]
            ):
                return None

            # Real error (connection, auth, timeout, etc.) - re-raise
            raise

    def get_deployment_by_id(self, deployment_id: str) -> dict[str, Any] | None:
        """Get a specific deployment by ID from the database

        Queries the database (source of truth) to find a deployment by its ID.
        Also queries deployment_ops to get the list of operations and their statuses.

        Returns None if:
        - Catalog doesn't exist (expected on first deployment)
        - Tracking schema doesn't exist (expected on first deployment)
        - Deployment ID not found

        Raises exception for real errors (connection, auth, timeout, etc.)

        Args:
            deployment_id: Deployment ID to look up

        Returns:
            Deployment record with operations list, or None if not found

        Raises:
            Exception: For database connection or query errors (not catalog-not-found)
        """
        from databricks.sdk.service.sql import StatementState

        # Query deployments table for basic info
        sql = f"""
        SELECT 
            id, 
            environment, 
            snapshot_version,
            from_snapshot_version,
            deployed_at,
            deployed_by,
            status,
            ops_count,
            error_message,
            execution_time_ms
        FROM {self.schema}.deployments
        WHERE id = '{deployment_id}'
        LIMIT 1
        """

        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="10s",
            )

            # Check if query succeeded
            if not response.status or response.status.state != StatementState.SUCCEEDED:
                # Check error message to distinguish catalog-not-found from real errors
                error_msg = ""
                if response.status and response.status.error:
                    error_msg = str(response.status.error.message or "")

                # Expected errors (catalog/schema doesn't exist)
                if any(
                    pattern in error_msg.lower()
                    for pattern in [
                        "catalog",
                        "schema",
                        "not found",
                        "does not exist",
                        "table_or_view_not_found",
                    ]
                ):
                    return None

                # Real error - raise it
                raise Exception(f"Database query failed: {error_msg}")

            # Parse deployment record
            deployment = None
            if response.result and response.result.data_array:
                if len(response.result.data_array) > 0:
                    row = response.result.data_array[0]
                    deployment = {
                        "id": row[0],
                        "environment": row[1],
                        "version": row[2],
                        "fromVersion": row[3],  # Can be NULL
                        "deployedAt": row[4],
                        "deployedBy": row[5],
                        "status": row[6],
                        "statementCount": row[7],
                        "errorMessage": row[8],
                        "executionTimeMs": row[9],
                    }

            if not deployment:
                return None

            # Query deployment_ops to get operation details
            ops_sql = f"""
            SELECT 
                op_id,
                op_type,
                op_target,
                op_payload,
                status,
                execution_order,
                error_message
            FROM {self.schema}.deployment_ops
            WHERE deployment_id = '{deployment_id}'
            ORDER BY execution_order
            """

            ops_response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=ops_sql,
                wait_timeout="10s",
            )

            # Parse operations with full details
            import json

            ops_applied = []
            ops_details = []
            successful_ops = []
            failed_statement_index = None

            if ops_response.result and ops_response.result.data_array:
                for i, row in enumerate(ops_response.result.data_array):
                    op_id = row[0]
                    op_type = row[1]
                    op_target = row[2]
                    op_payload_json = row[3]  # JSON string
                    op_status = row[4]
                    execution_order = row[5]

                    # Parse payload from JSON
                    try:
                        op_payload = json.loads(op_payload_json) if op_payload_json else {}
                    except json.JSONDecodeError:
                        op_payload = {}

                    ops_applied.append(op_id)
                    ops_details.append(
                        {
                            "id": op_id,
                            "type": op_type,
                            "target": op_target,
                            "payload": op_payload,  # Include parsed payload
                            "status": op_status,
                            "executionOrder": execution_order,
                        }
                    )

                    if op_status == "success":
                        successful_ops.append(op_id)
                    elif op_status == "failed" and failed_statement_index is None:
                        failed_statement_index = i

            # Add operation details to deployment record
            deployment["opsApplied"] = ops_applied
            deployment["opsDetails"] = ops_details  # Full operation details for matching
            deployment["successfulStatements"] = len(successful_ops)
            deployment["failedStatementIndex"] = failed_statement_index

            return deployment

        except Exception as e:
            # Check if it's a catalog/schema not found error (expected)
            error_str = str(e).lower()
            if any(
                pattern in error_str
                for pattern in [
                    "catalog",
                    "schema",
                    "not found",
                    "does not exist",
                    "table_or_view_not_found",
                ]
            ):
                return None

            # Real error (connection, auth, timeout, etc.) - re-raise
            raise

    def _execute_ddl(self, sql: str) -> None:
        """Execute DDL statement and wait for completion

        Args:
            sql: SQL DDL statement to execute
        """
        from databricks.sdk.service.sql import StatementState

        # Execute statement
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql,
            wait_timeout="30s",  # Wait up to 30 seconds for DDL
        )

        # Wait for completion if not already done
        if response.status and response.status.state != StatementState.SUCCEEDED:
            # Poll for completion
            import time

            max_wait = 60  # Maximum 60 seconds
            elapsed = 0
            while elapsed < max_wait:
                status = self.client.statement_execution.get_statement(response.statement_id or "")

                if not status or not status.status:
                    raise RuntimeError("Failed to get statement status")

                if status.status.state == StatementState.SUCCEEDED:
                    return
                elif status.status.state in (StatementState.FAILED, StatementState.CANCELED):
                    error_msg = (
                        status.status.error.message if status.status.error else "Unknown error"
                    )
                    raise RuntimeError(f"DDL execution failed: {error_msg}")

                time.sleep(1)
                elapsed += 1

            raise TimeoutError(f"DDL execution timed out after {max_wait} seconds")

    def _get_deployments_table_ddl(self) -> str:
        """Get DDL for deployments table

        Returns:
            CREATE TABLE statement for deployments
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.deployments (
            id STRING COMMENT 'Unique deployment ID',
            environment STRING COMMENT 'Target environment (dev/test/prod)',
            snapshot_version STRING COMMENT 'Snapshot version deployed (e.g., v1.0.0 or changelog)',
            from_snapshot_version STRING COMMENT 'Previous snapshot version (source of diff)',
            snapshot_id STRING COMMENT 'Snapshot UUID',
            deployed_at TIMESTAMP COMMENT 'Deployment timestamp',
            deployed_by STRING COMMENT 'User/system that deployed',
            project_name STRING COMMENT 'Schematic project name',
            provider_type STRING COMMENT 'Provider type (unity/hms)',
            provider_version STRING COMMENT 'Provider schema version',
            ops_count INT COMMENT 'Number of operations applied',
            status STRING COMMENT 'pending/success/failed/partial',
            error_message STRING COMMENT 'Error details if failed',
            execution_time_ms BIGINT COMMENT 'Execution time in milliseconds',
            schematic_version STRING COMMENT 'Schematic CLI version',
            CONSTRAINT pk_deployments PRIMARY KEY (id)
        )
        COMMENT 'Schematic deployment history'
        """

    def _get_deployment_ops_table_ddl(self) -> str:
        """Get DDL for deployment_ops table

        Returns:
            CREATE TABLE statement for deployment_ops
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.deployment_ops (
            deployment_id STRING COMMENT 'Parent deployment ID',
            op_id STRING COMMENT 'Operation ID from changelog',
            op_type STRING COMMENT 'Operation type (e.g., unity.add_catalog)',
            op_target STRING COMMENT 'Target resource (catalog/schema/table ID)',
            op_payload STRING COMMENT 'Operation payload as JSON for exact matching',
            sql_statement STRING COMMENT 'Generated SQL for this operation',
            executed_at TIMESTAMP COMMENT 'Execution timestamp',
            execution_order INT COMMENT 'Order of execution',
            status STRING COMMENT 'pending/success/failed/skipped',
            error_message STRING COMMENT 'Error details if failed',
            CONSTRAINT pk_deployment_ops PRIMARY KEY (deployment_id, op_id)
        )
        COMMENT 'Individual operation tracking per deployment'
        """
