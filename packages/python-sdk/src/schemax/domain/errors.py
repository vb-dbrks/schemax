"""Unified domain error taxonomy for workflow orchestration."""

from dataclasses import dataclass


@dataclass(slots=True)
class SchemaXDomainError(Exception):
    """Base class for application/domain-level failures."""

    message: str
    code: str

    def __str__(self) -> str:
        return self.message


class WorkflowValidationError(SchemaXDomainError):
    """Raised for workflow input/configuration validation failures."""


class WorkflowPlanningError(SchemaXDomainError):
    """Raised for planning/diff/SQL build failures."""


class WorkflowExecutionError(SchemaXDomainError):
    """Raised for execution-time failures."""


class ProviderCapabilityError(SchemaXDomainError):
    """Raised when a provider cannot satisfy a requested capability."""


class StorageConflictError(SchemaXDomainError):
    """Raised when storage transaction/session conflicts occur."""
