"""
Schematic CLI Commands

Modular command implementations for the Schematic CLI.
Each command is implemented as a separate module for better organization
and testability. The CLI layer (cli.py) acts as a thin routing layer.
"""

from .apply import ApplyError, apply_to_environment
from .deployment import DeploymentRecordingError, record_deployment_to_environment
from .sql import SQLGenerationError, generate_sql_migration
from .validate import ValidationError, validate_project

__all__ = [
    "apply_to_environment",
    "ApplyError",
    "generate_sql_migration",
    "SQLGenerationError",
    "validate_project",
    "ValidationError",
    "record_deployment_to_environment",
    "DeploymentRecordingError",
]
