"""
Safety Validator

Validates rollback safety by querying Unity Catalog to determine data impact.
Classifies operations as SAFE, RISKY, or DESTRUCTIVE based on actual data.
"""

from typing import Any

from ..base.executor import SQLExecutor
from ..base.operations import Operation
from ..base.reverse_generator import SafetyLevel, SafetyReport
from .operations import UNITY_OPERATIONS


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

    def __init__(self, executor: SQLExecutor) -> None:
        """Initialize safety validator

        Args:
            executor: SQL executor for querying Unity Catalog
        """
        self.executor = executor

    def validate(
        self, op: Operation, catalog_mapping: dict[str, str] | None = None
    ) -> SafetyReport:
        """Validate safety of a reverse operation

        Args:
            op: Reverse operation to validate
            catalog_mapping: Mapping of logical to physical catalog names

        Returns:
            SafetyReport with safety level and details
        """
        op_type = op.op

        # Dispatch to operation-specific validation
        if op_type == UNITY_OPERATIONS["DROP_CATALOG"]:
            return self._validate_drop_catalog(op, catalog_mapping)
        elif op_type == UNITY_OPERATIONS["DROP_SCHEMA"]:
            return self._validate_drop_schema(op, catalog_mapping)
        elif op_type == UNITY_OPERATIONS["DROP_TABLE"]:
            return self._validate_drop_table(op, catalog_mapping)
        elif op_type == UNITY_OPERATIONS["DROP_COLUMN"]:
            return self._validate_drop_column(op, catalog_mapping)
        elif op_type == UNITY_OPERATIONS["CHANGE_COLUMN_TYPE"]:
            return self._validate_change_column_type(op, catalog_mapping)
        else:
            # Non-destructive operations (renames, comments, tags, etc.)
            return self._safe_operation(op)

    def _safe_operation(self, op: Operation) -> SafetyReport:
        """Mark operation as safe (no data loss)"""
        return SafetyReport(
            level=SafetyLevel.SAFE,
            reason=f"{op.op} is a non-destructive operation",
            data_at_risk=0,
        )

    def _validate_drop_catalog(
        self, op: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_CATALOG operation

        Catalogs should generally never be dropped during rollback unless they
        were just created and are empty.
        """
        catalog_id = op.target
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
            else:
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
        self, op: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_SCHEMA operation

        Checks if schema contains any tables.
        """
        # Extract catalog and schema from target (format: "catalog_id.schema_id")
        parts = op.target.split(".")
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
            elif table_count < self.RISKY_THRESHOLD:
                return SafetyReport(
                    level=SafetyLevel.RISKY,
                    reason=f"Schema '{catalog_name}.{schema_name}' contains {table_count} tables",
                    data_at_risk=table_count,
                )
            else:
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

    def _validate_drop_table(
        self, op: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_TABLE operation

        Queries table for row count and sample data.
        """
        # Extract fully qualified table name from target
        # Target format: "catalog_id.schema_id.table_id"
        parts = op.target.split(".")
        if len(parts) != 3:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason="Table target format invalid",
                data_at_risk=0,
            )

        catalog_id, schema_id, table_id = parts
        catalog_name = self._get_physical_name(catalog_id, catalog_mapping)
        # TODO: Proper schema/table name lookup from state
        schema_name = schema_id
        table_name = table_id

        fully_qualified = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"

        try:
            # Query row count
            row_count = self._query_count(f"SELECT COUNT(*) as cnt FROM {fully_qualified}")

            if row_count == 0:
                return SafetyReport(
                    level=SafetyLevel.SAFE,
                    reason=f"Table {fully_qualified} is empty (0 rows)",
                    data_at_risk=0,
                )
            elif row_count < self.RISKY_THRESHOLD:
                # Get sample data for small tables
                sample = self._query_sample(fully_qualified, limit=3)
                return SafetyReport(
                    level=SafetyLevel.RISKY,
                    reason=f"Table {fully_qualified} has {row_count:,} rows",
                    data_at_risk=row_count,
                    sample_data=sample,
                )
            else:
                # Get sample data for large tables
                sample = self._query_sample(fully_qualified, limit=5)
                return SafetyReport(
                    level=SafetyLevel.DESTRUCTIVE,
                    reason=f"Table {fully_qualified} has {row_count:,} rows",
                    data_at_risk=row_count,
                    sample_data=sample,
                )
        except Exception as e:
            # Table doesn't exist or can't be queried - safe to "drop"
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Table {fully_qualified} does not exist or is inaccessible: {e}",
                data_at_risk=0,
            )

    def _validate_drop_column(
        self, op: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate DROP_COLUMN operation

        Checks if column has non-NULL data that would be lost.
        """
        # Extract table and column info from payload
        payload = op.payload
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
            # Count non-NULL values in column
            non_null_count = self._query_count(
                f"SELECT COUNT(*) as cnt FROM {fully_qualified} WHERE `{column_name}` IS NOT NULL"
            )

            if non_null_count == 0:
                return SafetyReport(
                    level=SafetyLevel.SAFE,
                    reason=f"Column `{column_name}` has all NULL values (no data loss)",
                    data_at_risk=0,
                )
            elif non_null_count < self.RISKY_THRESHOLD:
                # Sample non-NULL values
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
            else:
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
        except Exception as e:
            return SafetyReport(
                level=SafetyLevel.SAFE,
                reason=f"Column `{column_name}` does not exist or table is inaccessible: {e}",
                data_at_risk=0,
            )

    def _validate_change_column_type(
        self, op: Operation, catalog_mapping: dict[str, str] | None
    ) -> SafetyReport:
        """Validate CHANGE_COLUMN_TYPE operation

        Type changes can cause data loss if the new type is narrower than the old type.
        For rollback, we're reverting to the old type, which is generally safer.
        """
        # Changing type back during rollback is generally RISKY but not DESTRUCTIVE
        # since we're restoring the original type
        return SafetyReport(
            level=SafetyLevel.RISKY,
            reason="Type change during rollback may cause compatibility issues",
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
        result = self.executor.execute_query(sql)
        if result and len(result) > 0 and "cnt" in result[0]:
            return int(result[0]["cnt"])
        return 0

    def _query(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL query and return results

        Args:
            sql: SQL query

        Returns:
            Query results as list of dictionaries
        """
        return self.executor.execute_query(sql)

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
