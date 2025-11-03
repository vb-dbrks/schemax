"""
Reverse Operation Generator

Generates reverse operations for rollback with safety classification.
Supports automatic rollback for failed deployments.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .operations import Operation


class SafetyLevel(str, Enum):
    """Safety classification for reverse operations"""

    SAFE = "SAFE"  # No data loss, fully reversible
    RISKY = "RISKY"  # Potential data loss or compatibility issues
    DESTRUCTIVE = "DESTRUCTIVE"  # Significant data loss


@dataclass
class SafetyReport:
    """Report on the safety of a reverse operation"""

    level: SafetyLevel
    reason: str
    data_at_risk: int = 0  # Number of rows/values at risk
    sample_data: list[dict[str, Any]] | None = None  # Sample data that would be lost


class ReverseOperationGenerator(ABC):
    """Base class for generating reverse operations

    Implementations should provide provider-specific logic for:
    1. Generating reverse operations from forward operations
    2. Classifying safety level of reverse operations
    """

    @abstractmethod
    def generate_reverse(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Generate reverse operation for a given operation

        Args:
            op: The forward operation to reverse
            state: Provider state at time of original operation (for context)

        Returns:
            Reverse operation that undoes the forward operation

        Raises:
            ValueError: If operation cannot be reversed automatically
        """
        pass

    @abstractmethod
    def can_reverse(self, op: Operation) -> bool:
        """Check if operation can be automatically reversed

        Some operations (like data migrations, complex transformations) cannot
        be automatically reversed and require manual intervention.

        Args:
            op: Operation to check

        Returns:
            True if operation can be reversed automatically
        """
        pass

    def get_irreversible_reason(self, op: Operation) -> str:
        """Get explanation for why operation cannot be reversed

        Args:
            op: Operation that cannot be reversed

        Returns:
            Human-readable explanation
        """
        return f"Operation '{op.op}' cannot be automatically reversed"
