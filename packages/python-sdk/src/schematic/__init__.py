"""
Schematic Python SDK

Python library and CLI for managing catalog schemas using a provider-based architecture.
Supports multiple catalog providers: Unity Catalog, Hive, PostgreSQL, and more.
"""

__version__ = "0.2.0"

# Provider system exports
from .providers import (
    Operation,
    Provider,
    ProviderInfo,
    ProviderRegistry,
    ProviderState,
    ValidationError,
    ValidationResult,
)

# Storage V3 exports
from .storage_v3 import (
    create_snapshot,
    ensure_project_file,
    get_last_deployment,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_deployment,
)

__all__ = [
    "__version__",
    # Provider system
    "Provider",
    "ProviderInfo",
    "ProviderRegistry",
    "ProviderState",
    "Operation",
    "ValidationError",
    "ValidationResult",
    # Storage
    "ensure_project_file",
    "read_project",
    "read_changelog",
    "read_snapshot",
    "load_current_state",
    "create_snapshot",
    "write_deployment",
    "get_last_deployment",
]
