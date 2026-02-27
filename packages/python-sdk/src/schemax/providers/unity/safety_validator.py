"""
Safety Validator

Validates rollback safety by querying Unity Catalog to determine data impact.
Classifies operations as SAFE, RISKY, or DESTRUCTIVE based on actual data.
"""

from typing import Any, cast

from schemax.providers.base.executor import ExecutionConfig, SQLExecutor
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport

from .operations import UNITY_OPERATIONS

# Dispatch map: op_type -> method name (method takes self, operation, catalog_mapping)
_VALIDATE_HANDLERS: dict[str, str] = {
    UNITY_OPERATIONS["DROP_CATALOG"]: "_validate_drop_catalog",
    UNITY_OPERATIONS["DROP_SCHEMA"]: "_validate_drop_schema",
    UNITY_OPERATIONS["DROP_TABLE"]: "_validate_drop_table",
    UNITY_OPERATIONS["DROP_VIEW"]: "_safe_operation",
    UNITY_OPERATIONS["DROP_VOLUME"]: "_safe_operation",
    UNITY_OPERATIONS["DROP_FUNCTION"]: "_safe_operation",
    UNITY_OPERATIONS["DROP_MATERIALIZED_VIEW"]: "_safe_operation",
    UNITY_OPERATIONS["DROP_COLUMN"]: "_validate_drop_column",
    UNITY_OPERATIONS["CHANGE_COLUMN_TYPE"]: "_validate_change_column_type",
    UNITY_OPERATIONS["ADD_GRANT"]: "_safe_operation",
    UNITY_OPERATIONS["REVOKE_GRANT"]: "_safe_operation",
}


class SafetyValidator:
    """Validates safety of rollback operations by querying actual data

    Safety levels:
    - SAFE: No data loss (e.g., empty table drops, rename operations, NULL columns)
    - RISKY: Minor data loss (< 1000 rows affected)
    - DESTRUCTIVE: Significant data loss (≥ 1000 rows affected)
    """

    # Thresholds for safety classification
    RISKY_THRESHOLD = 1000  # Rows/values below this are RISKY
    SAFE_THRESHOLD = 1  # 0 rows/values = SAFE, >0 = at least RISKY

    def __init__(self, executor: SQLExecutor, config: ExecutionConfig) -> None:
        """Initialize safety validator

        Args:
            executor: SQL executor for querying Unity Catalog
            config: Execution configuration with warehouse ID and settings
        """
        self.executor = executor
        self.config = config

    def validate(
        self, operation: Operation, catalog_mapping: dict[str, str] | None = None
    ) -> SafetyReport:
        """Validate safety of a reverse operation

        Args:
            operation: Reverse operation to validate
            catalog_mapping: Mapping of logical to physical catalog names

        Returns:
            SafetyReport with safety level and details
        """
        op_type = operation.op
        method_name = _VALIDATE_HANDLERS.get(op_type, "_safe_operation")
        return cast(SafetyReport, getattr(self, method_name)(operation, catalog_mapping))

    def _safe_operation(
        self,
        operation: Operation,
        catalog_mapping: dict[str, str] | None = None,
    ) -> SafetyReport:
        """Mark operation as safe (no data loss)"""
        _ = catalog_mapping
        return SafetyReport(
            level=SafetyLevel.SAFE,
            reason=f"{operation.op} is a non-destructive operation",
            data_at_risk=0,
        )

    def _validate_drop_catalog(
        self, operation: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_CATALOG operation

        Catalogs should generally never be dropped during rollback unless they
        were just created and are empty.
        """
        catalog_id = operation.target
        catalog_name = self._get_physical_name(catalog_id, catalog_mapping)

        try:
            # Count schemas in catalog
            schema_count = self._query_count(
                f"SELECT COUNT(*) as cnt FROM system.information_schema.schemata "
                f"WHERE catalog_name = '{catalog_name}'"
            )

            if schema_count == 0:
                return SafetyReport(
                    level=SafetyLevel.SAFE,
                    reason=f"Catalog '{catalog_name}' is empty (0 schemas)",
                    data_at_risk=0,
                )
            return SafetyReport(
                level=SafetyLevel.DESTRUCTIVE,
                reason=f"Catalog '{catalog_name}' contains {schema_count} schemas",
                data_at_risk=schema_count,
            )
        except Exception:
            # If catalog doesn't exist, it's safe to "drop"
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Catalog '{catalog_name}' does not exist",
                data_at_risk=0,
            )

    def _validate_drop_schema(
        self, operation: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_SCHEMA operation

        Checks if schema contains any tables.
        """
        # Extract catalog and schema from target (format: "catalog_id.schema_id")
        parts = operation.target.split(".")
        if len(parts) != 2:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason="Schema target format invalid",
                data_at_risk=0,
            )

        catalog_id, schema_id = parts
        catalog_name = self._get_physical_name(catalog_id, catalog_mapping)
        # For now, assume schema_id is the schema name (TODO: proper lookup)
        schema_name = schema_id

        try:
            # Count tables in schema
            table_count = self._query_count(
                f"SELECT COUNT(*) as cnt FROM system.information_schema.tables "
                f"WHERE table_catalog = '{catalog_name}' "
                f"AND table_schema = '{schema_name}'"
            )

            if table_count == 0:
                return SafetyReport(
                    level=SafetyLevel.SAFE,
                    reason=f"Schema '{catalog_name}.{schema_name}' is empty (0 tables)",
                    data_at_risk=0,
                )
            if table_count < self.RISKY_THRESHOLD:
                return SafetyReport(
                    level=SafetyLevel.RISKY,
                    reason=f"Schema '{catalog_name}.{schema_name}' contains {table_count} tables",
                    data_at_risk=table_count,
                )
            return SafetyReport(
                level=SafetyLevel.DESTRUCTIVE,
                reason=f"Schema '{catalog_name}.{schema_name}' contains {table_count} tables",
                data_at_risk=table_count,
            )
        except Exception:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Schema '{catalog_name}.{schema_name}' does not exist",
                data_at_risk=0,
            )

    def _report_for_row_count(self, fully_qualified: str, row_count: int) -> SafetyReport:
        """Classify row count into SAFE/RISKY/DESTRUCTIVE report."""
        if row_count == 0:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Table {fully_qualified} is empty (0 rows)",
                data_at_risk=0,
            )
        if row_count < self.RISKY_THRESHOLD:
            sample = self._query_sample(fully_qualified, limit=3)
            return SafetyReport(
                level=SafetyLevel.RISKY,
                reason=f"Table {fully_qualified} has {row_count:,} rows",
                data_at_risk=row_count,
                sample_data=sample,
            )
        sample = self._query_sample(fully_qualified, limit=5)
        return SafetyReport(
            level=SafetyLevel.DESTRUCTIVE,
            reason=f"Table {fully_qualified} has {row_count:,} rows",
            data_at_risk=row_count,
            sample_data=sample,
        )

    def _validate_drop_table(
        self, operation: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_TABLE operation

        Queries table for row count and sample data.
        """
        # Extract fully qualified table name from target
        # Target format: "catalog_id.schema_id.table_id"
        parts = operation.target.split(".")
        if len(parts) != 3:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason="Table target format invalid",
                data_at_risk=0,
            )

        catalog_id, schema_id, table_id = parts
        catalog_name = self._get_physical_name(catalog_id, catalog_mapping)
        schema_name = schema_id
        table_name = table_id
        fully_qualified = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"

        try:
            row_count = self._query_count(f"SELECT COUNT(*) as cnt FROM {fully_qualified}")
            return self._report_for_row_count(fully_qualified, row_count)
        except Exception as e:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Table {fully_qualified} does not exist or is inaccessible: {e}",
                data_at_risk=0,
            )

    def _report_for_non_null_count(
        self, fully_qualified: str, column_name: str, non_null_count: int
    ) -> SafetyReport:
        """Classify non-NULL column count into SAFE/RISKY/DESTRUCTIVE report."""
        if non_null_count == 0:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Column `{column_name}` has all NULL values (no data loss)",
                data_at_risk=0,
            )
        if non_null_count < self.RISKY_THRESHOLD:
            sample = self._query(
                f"SELECT `{column_name}` FROM {fully_qualified} "
                f"WHERE `{column_name}` IS NOT NULL LIMIT 3"
            )
            return SafetyReport(
                level=SafetyLevel.RISKY,
                reason=f"Column `{column_name}` has {non_null_count:,} non-NULL values",
                data_at_risk=non_null_count,
                sample_data=sample,
            )
        sample = self._query(
            f"SELECT `{column_name}` FROM {fully_qualified} "
            f"WHERE `{column_name}` IS NOT NULL LIMIT 5"
        )
        return SafetyReport(
            level=SafetyLevel.DESTRUCTIVE,
            reason=f"Column `{column_name}` has {non_null_count:,} non-NULL values",
            data_at_risk=non_null_count,
            sample_data=sample,
        )

    def _validate_drop_column(
        self, operation: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_COLUMN operation

        Checks if column has non-NULL data that would be lost.
        """
        payload = operation.payload
        table_parts = payload.get("tableId", "").split(".")
        column_name = payload.get("name", "")

        if len(table_parts) != 3 or not column_name:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason="Invalid table or column reference",
                data_at_risk=0,
            )

        catalog_id, schema_id, table_id = table_parts
        catalog_name = self._get_physical_name(catalog_id, catalog_mapping)
        fully_qualified = f"`{catalog_name}`.`{schema_id}`.`{table_id}`"

        try:
            non_null_count = self._query_count(
                f"SELECT COUNT(*) as cnt FROM {fully_qualified} WHERE `{column_name}` IS NOT NULL"
            )
            return self._report_for_non_null_count(fully_qualified, column_name, non_null_count)
        except Exception as e:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Column `{column_name}` does not exist or table is inaccessible: {e}",
                data_at_risk=0,
            )

    def _validate_change_column_type(
        self,
        operation: Operation,
        catalog_mapping: dict[str, str] | None,
    ) -> SafetyReport:
        """Validate CHANGE_COLUMN_TYPE operation

        Type changes can cause data loss if the new type is narrower than the old type.
        For rollback, we're reverting to the old type, which is generally safer.
        """
        _ = catalog_mapping
        return SafetyReport(
            level=SafetyLevel.RISKY,
            reason=f"{operation.op}: type change during rollback may cause compatibility issues",
            data_at_risk=0,
        )

    def _get_physical_name(self, logical_id: str, catalog_mapping: dict[str, str] | None) -> str:
        """Get physical catalog name from logical ID

        Args:
            logical_id: Logical catalog ID or name
            catalog_mapping: Mapping of logical to physical names

        Returns:
            Physical catalog name
        """
        if catalog_mapping and logical_id in catalog_mapping:
            return catalog_mapping[logical_id]
        return logical_id  # Fallback to logical ID

    def _query_count(self, sql: str) -> int:
        """Execute COUNT query and return result

        Args:
            sql: SQL query that returns a 'cnt' column

        Returns:
            Count value
        """
        result = self.executor.execute_statements([sql], self.config)
        if (
            result.statement_results
            and len(result.statement_results) > 0
            and result.statement_results[0].result_data
            and len(result.statement_results[0].result_data) > 0
            and "cnt" in result.statement_results[0].result_data[0]
        ):
            cnt_value = result.statement_results[0].result_data[0]["cnt"]
            return int(cast(int, cnt_value)) if cnt_value is not None else 0
        return 0

    def _query(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL query and return results

        Args:
            sql: SQL query

        Returns:
            Query results as list of dictionaries
        """
        result = self.executor.execute_statements([sql], self.config)
        if result.statement_results and len(result.statement_results) > 0:
            return result.statement_results[0].result_data or []
        return []

    def _query_sample(self, from_clause: str, limit: int = 5) -> list[dict[str, Any]]:
        """Get sample rows from a table

        Args:
            from_clause: Fully qualified table name
            limit: Number of rows to sample

        Returns:
            Sample rows
        """
        try:
            return self._query(f"SELECT * FROM {from_clause} LIMIT {limit}")
        except Exception:
            return []
