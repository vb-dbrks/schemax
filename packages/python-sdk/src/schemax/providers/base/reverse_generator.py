"""
Safety Classification for Rollback Operations

Defines safety levels and reporting structures for validating rollback operations.
Used by SafetyValidator to classify data impact of rollback operations.
"""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class SafetyLevel(StrEnum):
    """Safety classification for rollback operations"""

    SAFE = "SAFE"  # No data loss, fully reversible
    RISKY = "RISKY"  # Potential data loss or compatibility issues
    DESTRUCTIVE = "DESTRUCTIVE"  # Significant data loss


class SafetyReport(BaseModel):
    """Report on the safety of a rollback operation."""

    level: SafetyLevel
    reason: str
    data_at_risk: int = Field(default=0, description="Number of rows/values at risk")
    sample_data: list[dict[str, Any]] | None = Field(
        default=None, description="Sample data that would be lost"
    )
