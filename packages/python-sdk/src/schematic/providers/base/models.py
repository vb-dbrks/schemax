"""
Base Model Types for Provider System
"""

from typing import Any

from pydantic import BaseModel

from .executor import ExecutionConfig, ExecutionResult, SQLExecutor, StatementResult


class ValidationError(BaseModel):
    """Validation error details"""

    field: str
    message: str
    code: str | None = None


class ValidationResult(BaseModel):
    """Result of validation"""

    valid: bool
    errors: list[ValidationError] = []


# Provider state is just a dictionary - providers define their own structure
ProviderState = dict[str, Any]


__all__ = [
    "ValidationError",
    "ValidationResult",
    "ProviderState",
    "ExecutionConfig",
    "ExecutionResult",
    "SQLExecutor",
    "StatementResult",
]
