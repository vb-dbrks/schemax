"""
Provider System for SchemaX Python SDK

This module implements the provider architecture that enables support for
multiple catalog types (Unity Catalog, Hive Metastore, PostgreSQL, etc.)
"""

from .base.hierarchy import Hierarchy, HierarchyLevel
from .base.models import ProviderState, ValidationError, ValidationResult
from .base.operations import Operation, OperationCategory, OperationMetadata
from .base.provider import Provider, ProviderCapabilities, ProviderInfo
from .registry import ProviderRegistry

# Import providers for auto-registration
from .unity import unity_provider

__all__ = [
    "ProviderRegistry",
    "Provider",
    "ProviderInfo",
    "ProviderCapabilities",
    "Hierarchy",
    "HierarchyLevel",
    "ProviderState",
    "ValidationError",
    "ValidationResult",
    "Operation",
    "OperationMetadata",
    "OperationCategory",
]


def initialize_providers() -> None:
    """Initialize and register all providers"""
    # Register Unity Catalog provider
    ProviderRegistry.register(unity_provider)

    # Future providers will be registered here when implemented.


# Auto-initialize on import
initialize_providers()
