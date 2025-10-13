"""
SchemaX Python SDK

Python library and CLI for managing Databricks Unity Catalog schemas.
"""

__version__ = "0.1.0"

from .models import (
    Catalog,
    Schema,
    Table,
    Column,
    Constraint,
    RowFilter,
    ColumnMask,
    Op,
    ProjectFile,
    SnapshotFile,
    ChangelogFile,
)
from .storage import (
    read_project,
    read_changelog,
    read_snapshot,
    load_current_state,
)
from .sql_generator import SQLGenerator

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
