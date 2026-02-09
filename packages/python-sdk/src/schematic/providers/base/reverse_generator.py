"""
Safety Classification for Rollback Operations

Defines safety levels and reporting structures for validating rollback operations.
Used by SafetyValidator to classify data impact of rollback operations.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SafetyLevel(StrEnum):
    """Safety classification for rollback operations"""

    SAFE = "SAFE"  # No data loss, fully reversible
    RISKY = "RISKY"  # Potential data loss or compatibility issues
    DESTRUCTIVE = "DESTRUCTIVE"  # Significant data loss


@dataclass
class SafetyReport:
    """Report on the safety of a rollback operation"""

    level: SafetyLevel
    reason: str
    data_at_risk: int = 0  # Number of rows/values at risk
    sample_data: list[dict[str, Any]] | None = None  # Sample data that would be lost
