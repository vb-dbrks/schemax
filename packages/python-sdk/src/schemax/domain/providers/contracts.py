"""Provider capability contracts for application-layer orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from schemax.providers.base.executor import ExecutionConfig, SQLExecutor
from schemax.providers.base.models import ProviderState, ValidationResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.sql_generator import SQLGenerationResult


@runtime_checkable
class StateReducerCapability(Protocol):
    """Apply one or more operations to provider state."""

    def apply_operation(self, state: ProviderState, operation: Operation) -> ProviderState: ...

    def apply_operations(self, state: ProviderState, ops: list[Operation]) -> ProviderState: ...


@runtime_checkable
class StateDifferCapability(Protocol):
    """Produce diff operations between two provider states."""

    def generate_diff_operations(
        self,
        old_state: ProviderState,
        new_state: ProviderState,
        old_operations: list[Operation],
        new_operations: list[Operation],
    ) -> list[Operation]: ...


@runtime_checkable
class SqlGenerationCapability(Protocol):
    """Generate SQL for provider operations."""

    def generate_sql(
        self,
        state: ProviderState,
        operations: list[Operation],
        name_mapping: dict[str, str] | None = None,
        managed_locations: dict[str, Any] | None = None,
        external_locations: dict[str, Any] | None = None,
        environment_name: str | None = None,
    ) -> SQLGenerationResult: ...


@runtime_checkable
class ExecutionCapability(Protocol):
    """Create executor and validate execution configuration."""

    def get_sql_executor(self, config: ExecutionConfig) -> SQLExecutor: ...

    def validate_execution_config(self, config: ExecutionConfig) -> ValidationResult: ...


@runtime_checkable
class DiscoveryCapability(Protocol):
    """Discover provider state from a live environment."""

    def discover_state(
        self,
        config: ExecutionConfig,
        scope: dict[str, Any] | None = None,
    ) -> ProviderState: ...


@runtime_checkable
class ImportTransformCapability(Protocol):
    """Normalize discovered state and persist provider import mappings."""

    def prepare_import_state(
        self,
        local_state: ProviderState,
        discovered_state: ProviderState,
        env_config: dict[str, Any],
        mapping_overrides: dict[str, str] | None = None,
    ) -> tuple[ProviderState, dict[str, str], bool]: ...

    def update_env_import_mappings(
        self,
        env_config: dict[str, Any],
        mappings: dict[str, str],
    ) -> None: ...


@runtime_checkable
class RollbackSafetyCapability(Protocol):
    """Provider-specific safety checks for rollback planning."""

    def assess_safety(
        self,
        operation: Operation,
        config: ExecutionConfig,
        catalog_mapping: dict[str, str] | None = None,
    ) -> tuple[str, str]: ...


@runtime_checkable
class DeploymentTrackingCapability(Protocol):
    """Provider-specific deployment tracking integration."""

    def adopt_import_baseline(
        self,
        project: dict[str, Any],
        env_config: dict[str, Any],
        target_env: str,
        profile: str,
        warehouse_id: str,
        snapshot_version: str,
    ) -> str: ...


@runtime_checkable
class DdlImportCapability(Protocol):
    """Build provider state from a SQL file or SQL statements."""

    def state_from_ddl(
        self,
        sql_path: Path | None = None,
        sql_statements: list[str] | None = None,
        dialect: str = "databricks",
    ) -> tuple[ProviderState, Any]: ...


@dataclass(slots=True)
class ProviderCapabilitySet:
    """Typed capability availability manifest for a provider."""

    reducer: bool = False
    differ: bool = False
    sql_generator: bool = False
    execution: bool = False
    discovery: bool = False
    import_transform: bool = False
    rollback_safety: bool = False
    deployment_tracking: bool = False
    ddl_import: bool = False


@dataclass(slots=True)
class ProviderManifest:
    """Provider metadata exposed to the application layer."""

    provider_id: str
    name: str
    version: str
    capabilities: ProviderCapabilitySet = field(default_factory=ProviderCapabilitySet)
