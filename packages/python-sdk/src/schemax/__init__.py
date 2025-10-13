"""
SchemaX Python SDK

Python library and CLI for managing Databricks Unity Catalog schemas.
"""

__version__ = "0.1.0"

from .models import (
    Catalog,
    ChangelogFile,
    Column,
    ColumnMask,
    Constraint,
    Op,
    ProjectFile,
    RowFilter,
    Schema,
    SnapshotFile,
    Table,
)
from .sql_generator import SQLGenerator
from .storage import (
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
)

__all__ = [
    "__version__",
    "Catalog",
    "Schema",
    "Table",
    "Column",
    "Constraint",
    "RowFilter",
    "ColumnMask",
    "Op",
    "ProjectFile",
    "SnapshotFile",
    "ChangelogFile",
    "read_project",
    "read_changelog",
    "read_snapshot",
    "load_current_state",
    "SQLGenerator",
]
