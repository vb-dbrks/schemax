"""
Base SQL Generator Interface

Defines the contract for generating SQL DDL statements from operations.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from .batching import BatchInfo, OperationBatcher
from .models import ProviderState
from .operations import Operation
from .optimization import ColumnReorderOptimizer


class StatementInfo(BaseModel):
    """Information about a single SQL statement"""

    sql: str  # The SQL statement
    operation_ids: list[str]  # Operations that generated this statement
    execution_order: int  # Order in which this statement should be executed


class SQLGenerationResult(BaseModel):
    """SQL generation result with explicit operation mapping"""

    sql: str  # Generated SQL statements (combined script)
    statements: list[StatementInfo] = []  # Explicit statement-to-operation mapping
    warnings: list[str] = []  # Warnings or notes
    is_idempotent: bool = True  # Whether SQL is idempotent


class SQLGenerator(ABC):
    """Base SQL Generator interface"""

    def __init__(self, state: ProviderState):
        self.state = state

    @abstractmethod
    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL for a list of operations

        Args:
            ops: Operations to convert to SQL

        Returns:
            SQL script as string
        """
        pass

    def generate_sql_with_mapping(self, ops: list[Operation]) -> SQLGenerationResult:
        """
        Generate SQL with explicit operation-to-statement mapping

        Default implementation: calls generate_sql() and returns simple result.
        Providers should override for robust operation tracking.

        Args:
            ops: List of operations to convert to SQL

        Returns:
            SQLGenerationResult with sql and statements mapping
        """
        sql = self.generate_sql(ops)
        # Default: treat entire SQL as one statement with all operations
        statements = []
        if sql and sql.strip():
            statements = [
                StatementInfo(sql=sql, operation_ids=[op.id for op in ops], execution_order=1)
            ]
        return SQLGenerationResult(sql=sql, statements=statements)

    @abstractmethod
    def generate_sql_for_operation(self, op: Operation) -> SQLGenerationResult:
        """
        Generate SQL for a single operation

        Args:
            op: Operation to convert to SQL

        Returns:
            SQL generation result
        """
        pass

    @abstractmethod
    def can_generate_sql(self, op: Operation) -> bool:
        """
        Validate that an operation can be converted to SQL

        Args:
            op: Operation to validate

        Returns:
            True if operation can be converted to SQL
        """
        pass


class BaseSQLGenerator(SQLGenerator):
    """
    Enhanced base implementation with generic optimization algorithms.

    Provides:
    - Operation batching for optimized SQL generation
    - Column reorder optimization
    - Generic utilities (FQN building, escaping)
    - Template pattern for provider-specific SQL generation
    """

    def __init__(self, state: Any, name_mapping: dict[str, str] | None = None):
        """
        Initialize base SQL generator with optimization components.

        Args:
            state: Provider state (catalogs, schemas, tables, etc.) - can be Dict or BaseModel
            name_mapping: Optional name mapping (e.g., logical → physical catalog names)
        """
        super().__init__(state)
        self.name_mapping = name_mapping or {}
        self.batcher = OperationBatcher()
        self.optimizer = ColumnReorderOptimizer()

    # ====================
    # GENERIC UTILITIES
    # ====================

    def _build_fqn(self, *parts: str, separator: str = ".") -> str:
        """
        Build fully-qualified name with each part escaped separately.

        Generic utility that works for any provider. Separator is customizable
        (e.g., "." for most SQL databases, "::" for some systems).

        Args:
            *parts: Name parts (catalog, schema, table, column, etc.)
            separator: Separator between parts (default: ".")

        Returns:
            Escaped FQN like `catalog`.`schema`.`table`

        Example:
            >>> self._build_fqn("my_catalog", "my_schema", "my_table")
            '`my_catalog`.`my_schema`.`my_table`'
        """
        return separator.join(self.escape_identifier(part) for part in parts if part)

    # ====================
    # ABSTRACT METHODS - Providers must implement
    # ====================

    @abstractmethod
    def _get_target_object_id(self, op: Operation) -> str | None:
        """
        Extract target object ID from operation.

        Provider-specific logic to determine which object an operation targets.
        Used by batching algorithm to group operations.

        Args:
            op: Operation to analyze

        Returns:
            Object ID (e.g., table_id, schema_id) or None if no target

        Example (Unity):
            >>> def _get_target_object_id(self, op):
            ...     if op.op == "unity.add_table":
            ...         return op.target
            ...     elif op.op == "unity.add_column":
            ...         return op.payload.get("tableId")
            ...     return None
        """
        pass

    @abstractmethod
    def _is_create_operation(self, op: Operation) -> bool:
        """
        Check if operation creates a new object.

        Used by batching to distinguish CREATE from ALTER operations.

        Args:
            op: Operation to check

        Returns:
            True if operation creates new object (e.g., add_table, add_schema)

        Example (Unity):
            >>> def _is_create_operation(self, op):
            ...     return op.op in [
            ...         "unity.add_catalog",
            ...         "unity.add_schema",
            ...         "unity.add_table"
            ...     ]
        """
        pass

    @abstractmethod
    def _is_drop_operation(self, op: Operation) -> bool:
        """
        Check if operation drops/deletes an object.

        Used to handle DROP operations separately from batching since they
        cannot be batched with CREATE/ALTER operations.

        Args:
            op: Operation to check

        Returns:
            True if operation drops an object (e.g., drop_table, drop_schema, drop_catalog)

        Example (Unity):
            >>> def _is_drop_operation(self, op):
            ...     return op.op in [
            ...         "unity.drop_catalog",
            ...         "unity.drop_schema",
            ...         "unity.drop_table"
            ...     ]
        """
        pass

    @abstractmethod
    def _get_dependency_level(self, op: Operation) -> int:
        """
        Get dependency level for operation ordering.

        Lower numbers execute first (e.g., 0=catalog, 1=schema, 2=table).
        Ensures proper execution order (catalog before schema before table).

        Args:
            op: Operation to check

        Returns:
            Dependency level (0 = highest priority, execute first)

        Example (Unity):
            >>> def _get_dependency_level(self, op):
            ...     if "catalog" in op.op:
            ...         return 0
            ...     elif "schema" in op.op:
            ...         return 1
            ...     elif "add_table" in op.op:
            ...         return 2
            ...     else:
            ...         return 3
        """
        pass

    @abstractmethod
    def _generate_batched_create_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE statement with batched operations.

        For new objects, generate complete CREATE with all properties/columns
        included (not empty CREATE + multiple ALTERs).

        Args:
            object_id: ID of object being created
            batch_info: Batch of operations for this object

        Returns:
            SQL CREATE statement

        Example (Unity):
            >>> def _generate_batched_create_sql(self, object_id, batch_info):
            ...     # Generate: CREATE TABLE ... (col1 type1, col2 type2) USING DELTA
            ...     # Instead of: CREATE TABLE ... (); ALTER TABLE ADD col1; ALTER TABLE ADD col2;
        """
        pass

    @abstractmethod
    def _generate_batched_alter_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate ALTER statements for batched operations on existing object.

        Args:
            object_id: ID of object to alter
            batch_info: Batch of modification operations

        Returns:
            SQL ALTER statements (may be multiple, separated by `;`)
        """
        pass

    # ====================
    # DEFAULT IMPLEMENTATION (can be overridden)
    # ====================

    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL with basic operation-by-operation approach.

        This is the default implementation. Providers can override to use
        batching optimization by calling self.batcher.batch_operations().
        """
        statements = []

        for op in ops:
            if not self.can_generate_sql(op):
                print(f"Warning: Cannot generate SQL for operation: {op.op}")
                continue

            result = self.generate_sql_for_operation(op)

            # Add header comment with operation metadata
            header = f"-- Operation: {op.id} ({op.ts})\n-- Type: {op.op}"

            # Add warnings if any
            warnings_comment = ""
            if result.warnings:
                warnings_comment = f"\n-- Warnings: {', '.join(result.warnings)}"

            statements.append(f"{header}{warnings_comment}\n{result.sql};")

        return "\n\n".join(statements)

    @staticmethod
    def escape_identifier(identifier: str) -> str:
        """Helper to escape SQL identifiers"""
        return f"`{identifier.replace('`', '``')}`"

    @staticmethod
    def escape_string(s: str) -> str:
        """Helper to escape SQL string literals"""
        return s.replace("'", "''")
