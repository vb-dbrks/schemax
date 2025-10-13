"""
Base Model Types for Provider System
"""

from typing import Any, Dict, List
from pydantic import BaseModel


class ValidationError(BaseModel):
    """Validation error details"""

    field: str
    message: str
    code: str = None


class ValidationResult(BaseModel):
    """Result of validation"""

    valid: bool
    errors: List[ValidationError] = []


# Provider state is just a dictionary - providers define their own structure
ProviderState = Dict[str, Any]

