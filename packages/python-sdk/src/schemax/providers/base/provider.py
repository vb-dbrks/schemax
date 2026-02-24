"""
Base Provider Interface

Defines the contract that all catalog providers must implement.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from .executor import ExecutionConfig, SQLExecutor
from .hierarchy import Hierarchy
from .models import ProviderState, ValidationResult
from .operations import Operation, OperationMetadata
from .sql_generator import SQLGenerator
from .state_differ import StateDiffer


class ProviderCapabilities(BaseModel):
    """Provider capabilities - defines what features this provider supports"""

    supported_operations: list[str]  # e.g., ['unity.add_catalog', 'unity.add_schema']
    supported_object_types: list[str]  # e.g., ['catalog', 'schema', 'table']
    hierarchy: Hierarchy  # Hierarchy definition

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Feature flags
    features: dict[str, bool] = {
        "constraints": False,
        "row_filters": False,
        "column_masks": False,
        "column_tags": False,
        "table_properties": False,
        "comments": False,
        "partitioning": False,
        "views": False,
        "materialized_views": False,
        "functions": False,
        "indexes": False,
        "baseline_adoption": False,
    }


class ProviderInfo(BaseModel):
    """Provider metadata"""

    id: str  # Unique provider ID (e.g., 'unity', 'hive')
    name: str  # Human-readable name
    version: str  # Provider version (semantic versioning)
    description: str  # Provider description
    author: str | None = None  # Provider author/maintainer
    docs_url: str | None = None  # Documentation URL


class Provider(ABC):
    """Main Provider interface"""

    @property
    @abstractmethod
    def info(self) -> ProviderInfo:
        """Provider metadata"""

    @property
    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """Provider capabilities"""

    @abstractmethod
    def get_operation_metadata(self, operation_type: str) -> OperationMetadata | None:
        """Get operation metadata for a specific operation type"""

    @abstractmethod
    def get_all_operations(self) -> list[OperationMetadata]:
        """Get all operation metadata"""

    @abstractmethod
    def validate_operation(self, operation: Operation) -> ValidationResult:
        """Validate an operation"""

    @abstractmethod
    def apply_operation(self, state: ProviderState, operation: Operation) -> ProviderState:
        """Apply an operation to state (state reducer)"""

    @abstractmethod
    def apply_operations(self, state: ProviderState, ops: list[Operation]) -> ProviderState:
        """Apply multiple operations to state"""

    @abstractmethod
    def get_sql_generator(
        self,
        state: ProviderState,
        name_mapping: dict[str, str] | None = None,
        managed_locations: dict[str, Any] | None = None,
        external_locations: dict[str, Any] | None = None,
        environment_name: str | None = None,
    ) -> SQLGenerator:
        """Get SQL generator for this provider

        Args:
            state: Provider state to generate SQL for
            name_mapping: Optional mapping for environment-specific names
                         (e.g., logical catalog → physical catalog)
            managed_locations: Optional project-level managed locations config
                              (for catalog/schema MANAGED LOCATION clauses)
            external_locations: Optional project-level external locations config
                               (for external table LOCATION clauses)
            environment_name: Optional target environment name for path resolution

        Returns:
            SQLGenerator instance
        """

    @abstractmethod
    def create_initial_state(self) -> ProviderState:
        """Create an empty/initial state for this provider"""

    @abstractmethod
    def validate_state(self, state: ProviderState) -> ValidationResult:
        """Validate the entire state structure"""

    @abstractmethod
    def get_sql_executor(self, config: ExecutionConfig) -> SQLExecutor:
        """Get SQL executor for this provider

        Args:
            config: Execution configuration (auth, warehouse, etc.)

        Returns:
            SQLExecutor instance for executing SQL statements

        Raises:
            NotImplementedError: If provider doesn't support SQL execution
        """

    @abstractmethod
    def validate_execution_config(self, config: ExecutionConfig) -> ValidationResult:
        """Validate execution configuration

        Validates authentication, warehouse/endpoint configuration, and
        other provider-specific execution requirements.

        Args:
            config: Execution configuration to validate

        Returns:
            ValidationResult with any configuration errors
        """

    @abstractmethod
    def get_state_differ(
        self,
        old_state: ProviderState,
        new_state: ProviderState,
        old_operations: list[Operation],
        new_operations: list[Operation],
    ) -> StateDiffer:
        """Get state differ for this provider

        Args:
            old_state: Previous state (source)
            new_state: Current state (target)
            old_operations: Operations that created old_state
            new_operations: Operations that created new_state

        Returns:
            StateDiffer instance for generating diff operations
        """

    def discover_state(
        self,
        config: ExecutionConfig,
        scope: dict[str, Any] | None = None,
    ) -> ProviderState:
        """Discover current state from the target provider.

        Providers can override this for import/adoption workflows.

        Args:
            config: Execution/auth context for connecting to provider
            scope: Optional discovery filter (e.g., catalog/schema/table)

        Returns:
            Discovered provider state
        """
        raise NotImplementedError(
            f"Provider '{self.info.id}' does not implement live state discovery yet"
        )

    def state_from_ddl(
        self,
        sql_path: Path | None = None,
        sql_statements: list[str] | None = None,
        dialect: str = "databricks",
    ) -> tuple[ProviderState, Any]:
        """Build provider state from a SQL DDL script (file or list of statements).

        Used for import-from-SQL workflows. Providers that support DDL import
        override this; default raises NotImplementedError.

        Args:
            sql_path: Path to a .sql file (read and split internally).
            sql_statements: Alternatively, a list of raw SQL statements.
            dialect: SQL dialect for parsing (e.g. databricks). May be ignored
                by providers that only support one dialect.

        Returns:
            Tuple of (provider_state, report). Report is provider-specific (e.g.
            counts, skipped indices, parse errors) for summary and warnings.

        Raises:
            NotImplementedError: If this provider does not support DDL import.
            ValueError: If neither sql_path nor sql_statements is provided.
        """
        del sql_path, sql_statements, dialect
        raise NotImplementedError(
            f"Provider '{self.info.id}' does not implement state_from_ddl (SQL file import) yet"
        )

    def validate_import_scope(self, scope: dict[str, Any]) -> ValidationResult:
        """Validate provider-specific import scope rules.

        Providers can override this to reject system objects or unsupported scopes.
        """
        del scope
        return ValidationResult(valid=True, errors=[])

    def prepare_import_state(
        self,
        local_state: ProviderState,
        discovered_state: ProviderState,
        env_config: dict[str, Any],
        mapping_overrides: dict[str, str] | None = None,
    ) -> tuple[ProviderState, dict[str, str], bool]:
        """Provider hook to normalize discovered state before diff.

        Returns:
            normalized_state, provider mappings, mappings_updated
        """
        del local_state, env_config, mapping_overrides
        return discovered_state, {}, False

    def collect_import_warnings(
        self,
        config: ExecutionConfig,
        scope: dict[str, Any],
        discovered_state: ProviderState,
    ) -> list[str]:
        """Provider hook to emit non-fatal import warnings."""
        del config, scope, discovered_state
        return []

    def update_env_import_mappings(
        self,
        env_config: dict[str, Any],
        mappings: dict[str, str],
    ) -> None:
        """Persist provider-specific import mappings into environment config."""
        del env_config, mappings

    def adopt_import_baseline(
        self,
        project: dict[str, Any],
        env_config: dict[str, Any],
        target_env: str,
        profile: str,
        warehouse_id: str,
        snapshot_version: str,
    ) -> str:
        """Record import baseline deployment for provider-specific tracking."""
        del project, env_config, target_env, profile, warehouse_id, snapshot_version
        raise NotImplementedError(
            f"Provider '{self.info.id}' does not support baseline adoption tracking"
        )


class BaseProvider(Provider):
    """Base implementation of Provider with common functionality"""

    def __init__(self) -> None:
        self.operation_metadata: dict[str, OperationMetadata] = {}

    def get_operation_metadata(self, operation_type: str) -> OperationMetadata | None:
        return self.operation_metadata.get(operation_type)

    def get_all_operations(self) -> list[OperationMetadata]:
        return list(self.operation_metadata.values())

    def apply_operations(self, state: ProviderState, ops: list[Operation]) -> ProviderState:
        current_state = state
        for operation in ops:
            current_state = self.apply_operation(current_state, operation)
        return current_state

    def register_operation(self, metadata: OperationMetadata) -> None:
        """Helper to register operation metadata"""
        self.operation_metadata[metadata.type] = metadata

    def is_operation_supported(self, operation_type: str) -> bool:
        """Helper to check if operation is supported"""
        return operation_type in self.capabilities.supported_operations
