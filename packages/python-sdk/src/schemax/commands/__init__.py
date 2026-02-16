"""
SchemaX CLI Commands

Modular command implementations for the SchemaX CLI.
Each command is implemented as a separate module for better organization
and testability. The CLI layer (cli.py) acts as a thin routing layer.
"""

from .apply import ApplyError, apply_to_environment
from .diff import DiffError, generate_diff
from .import_assets import ImportError, import_from_provider
from .rollback import RollbackError, RollbackResult, rollback_complete, rollback_partial
from .sql import SQLGenerationError, generate_sql_migration
from .validate import ValidationError, validate_project

__all__ = [
    "apply_to_environment",
    "ApplyError",
    "generate_diff",
    "DiffError",
    "import_from_provider",
    "ImportError",
    "generate_sql_migration",
    "SQLGenerationError",
    "validate_project",
    "ValidationError",
    "rollback_partial",
    "rollback_complete",
    "RollbackResult",
    "RollbackError",
]
