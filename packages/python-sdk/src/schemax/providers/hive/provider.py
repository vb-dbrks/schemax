"""Hive provider implementation (MVP breadth validator)."""

from pathlib import Path
from typing import Any

from schemax.providers.base.executor import ExecutionConfig, SQLExecutor
from schemax.providers.base.models import ProviderState, ValidationError, ValidationResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.provider import BaseProvider, ProviderCapabilities, ProviderInfo
from schemax.providers.base.sql_generator import SQLGenerator
from schemax.providers.base.state_differ import StateDiffer

from .hierarchy import hive_hierarchy
from .operations import HIVE_OPERATIONS, hive_operation_metadata
from .sql_generator import HiveSQLGenerator
from .state_differ import HiveStateDiffer
from .state_reducer import apply_operation, apply_operations


class HiveProvider(BaseProvider):
    """Provider for Hive Metastore logical modeling and SQL generation."""

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            id="hive",
            name="Hive Metastore",
            version="0.1.0",
            description="Hive provider (MVP) for multi-provider architecture validation",
            author="SchemaX Team",
            docs_url="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL",
        )

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supported_operations=list(HIVE_OPERATIONS.values()),
            supported_object_types=["database", "table", "column"],
            hierarchy=hive_hierarchy,
            features={
                "constraints": False,
                "row_filters": False,
                "column_masks": False,
                "column_tags": False,
                "table_properties": True,
                "comments": True,
                "partitioning": True,
                "views": False,
                "materialized_views": False,
                "functions": False,
                "indexes": False,
                "baseline_adoption": False,
            },
        )

    def __init__(self) -> None:
        super().__init__()
        for metadata in hive_operation_metadata:
            self.register_operation(metadata)

    def validate_operation(self, operation: Operation) -> ValidationResult:
        metadata = self.get_operation_metadata(operation.op)
        if metadata is None:
            return ValidationResult(
                valid=False,
                errors=[
                    ValidationError(
                        field="op",
                        message=f"Unsupported operation type: {operation.op}",
                        code="UNSUPPORTED_OPERATION",
                    )
                ],
            )

        errors: list[ValidationError] = []
        for required_field in metadata.required_fields:
            value = operation.payload.get(required_field)
            if value in {None, ""}:
                errors.append(
                    ValidationError(
                        field=f"payload.{required_field}",
                        message=f"Required field missing: {required_field}",
                        code="MISSING_REQUIRED_FIELD",
                    )
                )
        return ValidationResult(valid=not errors, errors=errors)

    def apply_operation(self, state: ProviderState, operation: Operation) -> ProviderState:
        return apply_operation(state, operation)

    def apply_operations(self, state: ProviderState, ops: list[Operation]) -> ProviderState:
        return apply_operations(state, ops)

    def get_sql_generator(
        self,
        state: ProviderState,
        name_mapping: dict[str, str] | None = None,
        managed_locations: dict[str, Any] | None = None,
        external_locations: dict[str, Any] | None = None,
        environment_name: str | None = None,
    ) -> SQLGenerator:
        del name_mapping, managed_locations, external_locations, environment_name
        return HiveSQLGenerator(state)

    def create_initial_state(self) -> ProviderState:
        return {"databases": []}

    def validate_state(self, state: ProviderState) -> ValidationResult:
        databases = state.get("databases")
        if not isinstance(databases, list):
            return ValidationResult(
                valid=False,
                errors=[
                    ValidationError(
                        field="databases",
                        message="Hive state must contain a 'databases' list",
                        code="INVALID_STATE_STRUCTURE",
                    )
                ],
            )
        return ValidationResult(valid=True, errors=[])

    def get_sql_executor(self, config: ExecutionConfig) -> SQLExecutor:
        del config
        raise NotImplementedError("Hive execution is not implemented yet")

    def validate_execution_config(self, config: ExecutionConfig) -> ValidationResult:
        del config
        return ValidationResult(valid=False, errors=[])

    def get_state_differ(
        self,
        old_state: ProviderState,
        new_state: ProviderState,
        old_operations: list[Operation],
        new_operations: list[Operation],
    ) -> StateDiffer:
        return HiveStateDiffer(old_state, new_state, old_operations, new_operations)

    def state_from_ddl(
        self,
        sql_path: Path | None = None,
        sql_statements: list[str] | None = None,
        dialect: str = "databricks",
    ) -> tuple[ProviderState, Any]:
        del sql_path, sql_statements, dialect
        raise NotImplementedError("Hive state_from_ddl is not implemented yet")
