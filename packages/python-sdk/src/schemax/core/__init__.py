"""
Core Infrastructure

Foundational, provider-agnostic infrastructure for SchemaX:
- Storage: File I/O and project management
- Deployment: Deployment tracking and history
- Version: Semantic versioning utilities
- SQL utils: Statement splitting for scripts (no provider dependency)
"""

# Storage exports
# Deployment exports
from .deployment import DeploymentTracker

# SQL utils exports
from .sql_utils import split_sql_statements
from .storage import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    ensure_schemax_dir,
    get_changelog_file_path,
    get_environment_config,
    get_project_file_path,
    get_schemax_dir,
    get_snapshot_file_path,
    get_snapshots_dir,
    get_uncommitted_ops_count,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_changelog,
    write_project,
    write_snapshot,
)

# Version exports
from .version import (
    SemanticVersion,
    get_next_version,
    get_versions_between,
    parse_semantic_version,
)

__all__ = [
    # Storage
    "append_ops",
    "create_snapshot",
    "ensure_project_file",
    "ensure_schemax_dir",
    "get_changelog_file_path",
    "get_environment_config",
    "get_project_file_path",
    "get_schemax_dir",
    "get_snapshot_file_path",
    "get_snapshots_dir",
    "get_uncommitted_ops_count",
    "load_current_state",
    "read_changelog",
    "read_project",
    "read_snapshot",
    "write_changelog",
    "write_project",
    "write_snapshot",
    # SQL utils
    "split_sql_statements",
    # Deployment
    "DeploymentTracker",
    # Version
    "SemanticVersion",
    "get_next_version",
    "get_versions_between",
    "parse_semantic_version",
]
