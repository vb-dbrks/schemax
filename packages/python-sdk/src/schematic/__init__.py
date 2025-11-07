"""
Schematic Python SDK

Python library and CLI for managing catalog schemas using a provider-based architecture.
Supports multiple catalog providers: Unity Catalog, Hive, PostgreSQL, and more.
"""

from pathlib import Path

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

# Storage V4 exports (latest)
from .core.storage import (
    create_snapshot,
    ensure_project_file,
    get_last_deployment,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_deployment,
)


def generate_diff_operations(
    workspace_path: Path, from_version: str, to_version: str
) -> list[Operation]:
    """Generate diff operations between two snapshot versions

    Compares two snapshots and returns a list of operations that would
    transform the old state into the new state. Useful for understanding
    changes between versions and planning deployments.

    Args:
        workspace_path: Path to workspace directory
        from_version: Source snapshot version (e.g., "v0.1.0")
        to_version: Target snapshot version (e.g., "v0.10.0")

    Returns:
        List of operations representing the diff

    Example:
        ```python
        from pathlib import Path
        from schematic import generate_diff_operations

        operations = generate_diff_operations(
            workspace_path=Path.cwd(),
            from_version="v0.1.0",
            to_version="v0.10.0"
        )

        for op in operations:
            print(f"{op.op}: {op.target}")
        ```
    """
    # Load snapshots
    old_snap = read_snapshot(workspace_path, from_version)
    new_snap = read_snapshot(workspace_path, to_version)

    # Get provider
    project = read_project(workspace_path)
    provider = ProviderRegistry.get(project["provider"]["type"])

    if not provider:
        raise ValueError(f"Provider '{project['provider']['type']}' not found")

    # Generate diff
    differ = provider.get_state_differ(
        old_snap["state"],
        new_snap["state"],
        old_snap.get("operations", []),
        new_snap.get("operations", []),
    )

    return differ.generate_diff_operations()


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
    # Diff operations
    "generate_diff_operations",
]
