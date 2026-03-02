"""SQL generation for Hive provider MVP."""

from typing import Any

from schemax.providers.base.operations import Operation
from schemax.providers.base.sql_generator import SQLGenerationResult, SQLGenerator


class HiveSQLGenerator(SQLGenerator):
    """Minimal SQL generator for core Hive operations."""

    def __init__(self, state: dict[str, Any]):
        super().__init__(state)

    def generate_sql(self, ops: list[Operation]) -> str:
        statements: list[str] = []
        for operation in ops:
            result = self.generate_sql_for_operation(operation)
            sql = result.sql.strip()
            if sql:
                statements.append(sql)
        return "\n".join(statements)

    def generate_sql_for_operation(self, operation: Operation) -> SQLGenerationResult:
        op_type = operation.op.replace("hive.", "")
        if op_type == "add_database":
            name = str(operation.payload.get("name", ""))
            if not name:
                return SQLGenerationResult(
                    sql="", warnings=["Missing database name"], is_idempotent=True
                )
            return SQLGenerationResult(
                sql=f"CREATE DATABASE IF NOT EXISTS `{name}`;",
                warnings=[],
                is_idempotent=True,
            )

        if op_type == "drop_database":
            db_name = self._database_name_from_target(operation.target)
            return SQLGenerationResult(
                sql=f"DROP DATABASE IF EXISTS `{db_name}` CASCADE;",
                warnings=[],
                is_idempotent=True,
            )

        if op_type == "add_table":
            db_name = str(operation.payload.get("database", ""))
            table_name = str(operation.payload.get("name", ""))
            columns = operation.payload.get("columns", [])
            column_defs = self._column_sql(columns)
            if not db_name or not table_name or not column_defs:
                return SQLGenerationResult(
                    sql="",
                    warnings=["Missing table metadata for Hive add_table operation"],
                    is_idempotent=False,
                )
            return SQLGenerationResult(
                sql=f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` ({column_defs});",
                warnings=[],
                is_idempotent=True,
            )

        if op_type == "drop_table":
            table_identifier = self._drop_table_identifier(operation)
            if table_identifier is None:
                return SQLGenerationResult(
                    sql="",
                    warnings=["Missing table metadata for Hive drop_table operation"],
                    is_idempotent=True,
                )
            return SQLGenerationResult(
                sql=f"DROP TABLE IF EXISTS {table_identifier};",
                warnings=[],
                is_idempotent=True,
            )

        return SQLGenerationResult(
            sql="",
            warnings=[f"Unsupported Hive operation: {operation.op}"],
            is_idempotent=False,
        )

    def can_generate_sql(self, operation: Operation) -> bool:
        return operation.op.startswith("hive.")

    def _database_name_from_target(self, target: str) -> str:
        if "." in target:
            return target.split(".", 1)[0]
        return target

    @staticmethod
    def _drop_table_identifier(operation: Operation) -> str | None:
        database_name = str(operation.payload.get("database", ""))
        table_name = str(operation.payload.get("name", ""))
        if database_name and table_name:
            return f"`{database_name}`.`{table_name}`"

        target = str(operation.target or "")
        if "." not in target:
            return None
        db_part, table_part = target.split(".", 1)
        if not db_part or not table_part:
            return None
        return f"`{db_part}`.`{table_part}`"

    @staticmethod
    def _column_sql(columns: Any) -> str:
        if not isinstance(columns, list):
            return ""
        defs: list[str] = []
        for column in columns:
            if not isinstance(column, dict):
                continue
            name = column.get("name")
            col_type = column.get("type")
            if not name or not col_type:
                continue
            defs.append(f"`{name}` {col_type}")
        return ", ".join(defs)
