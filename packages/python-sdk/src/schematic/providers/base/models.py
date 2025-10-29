"""
Base Model Types for Provider System
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from .executor import ExecutionConfig, ExecutionResult, SQLExecutor, StatementResult


class ValidationError(BaseModel):
    """Validation error details"""

    field: str
    message: str
    code: Optional[str] = None


class ValidationResult(BaseModel):
    """Result of validation"""

    valid: bool
    errors: List[ValidationError] = []


# Provider state is just a dictionary - providers define their own structure
ProviderState = Dict[str, Any]


__all__ = [
    "ValidationError",
    "ValidationResult",
    "ProviderState",
    "ExecutionConfig",
    "ExecutionResult",
    "SQLExecutor",
    "StatementResult",
]
