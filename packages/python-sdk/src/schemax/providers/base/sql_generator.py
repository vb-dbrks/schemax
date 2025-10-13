"""
Base SQL Generator Interface

Defines the contract for generating SQL DDL statements from operations.
"""

from abc import ABC, abstractmethod
from typing import List

from pydantic import BaseModel

from .models import ProviderState
from .operations import Operation


class SQLGenerationResult(BaseModel):
    """SQL generation result"""

    sql: str  # Generated SQL statements
    warnings: List[str] = []  # Warnings or notes
    is_idempotent: bool = True  # Whether SQL is idempotent


class SQLGenerator(ABC):
    """Base SQL Generator interface"""

    def __init__(self, state: ProviderState):
        self.state = state

    @abstractmethod
    def generate_sql(self, ops: List[Operation]) -> str:
        """
        Generate SQL for a list of operations

        Args:
            ops: Operations to convert to SQL

        Returns:
            SQL script as string
        """
        pass

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
    """Base implementation with common functionality"""

    def generate_sql(self, ops: List[Operation]) -> str:
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
