"""
Unity Catalog Provider

Main provider implementation for Databricks Unity Catalog.
Implements the Provider interface to enable Unity Catalog support in Schematic.
"""

from ..base.executor import ExecutionConfig, SQLExecutor
from ..base.models import ProviderState, ValidationError, ValidationResult
from ..base.operations import Operation
from ..base.provider import (
    BaseProvider,
    ProviderCapabilities,
    ProviderInfo,
)
from ..base.sql_generator import SQLGenerator
from .auth import check_profile_exists, create_databricks_client
from .executor import UnitySQLExecutor
from .hierarchy import unity_hierarchy
from .models import UnityState
from .operations import UNITY_OPERATIONS, unity_operation_metadata
from .sql_generator import UnitySQLGenerator
from .state_reducer import apply_operation, apply_operations


class UnityProvider(BaseProvider):
    """Unity Catalog Provider Implementation"""

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            id="unity",
            name="Unity Catalog",
            version="1.0.0",
            description="Databricks Unity Catalog provider with full governance features",
            author="Schematic Team",
            docs_url="https://docs.databricks.com/data-governance/unity-catalog/index.html",
        )

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supported_operations=list(UNITY_OPERATIONS.values()),
            supported_object_types=["catalog", "schema", "table", "column"],
            hierarchy=unity_hierarchy,
            features={
                "constraints": True,
                "row_filters": True,
                "column_masks": True,
                "column_tags": True,
                "table_properties": True,
                "comments": True,
                "partitioning": False,  # Future
                "views": False,  # Future
                "materialized_views": False,  # Future
                "functions": False,  # Future
                "indexes": False,  # Not applicable to Unity Catalog
            },
        )

    def __init__(self) -> None:
        super().__init__()
        # Register all operation metadata
        for metadata in unity_operation_metadata:
            self.register_operation(metadata)

    def validate_operation(self, op: Operation) -> ValidationResult:
        """Validate an operation"""
        errors: list[ValidationError] = []

        # Check if operation is supported
        if not self.is_operation_supported(op.op):
            errors.append(
                ValidationError(
                    field="op",
                    message=f"Unsupported operation type: {op.op}",
                    code="UNSUPPORTED_OPERATION",
                )
            )
            return ValidationResult(valid=False, errors=errors)

        # Get operation metadata
        metadata = self.get_operation_metadata(op.op)
        if not metadata:
            errors.append(
                ValidationError(
                    field="op",
                    message=f"No metadata found for operation: {op.op}",
                    code="MISSING_METADATA",
                )
            )
            return ValidationResult(valid=False, errors=errors)

        # Validate required fields
        for field in metadata.required_fields:
            if field not in op.payload or op.payload[field] is None or op.payload[field] == "":
                errors.append(
                    ValidationError(
                        field=f"payload.{field}",
                        message=f"Required field missing: {field}",
                        code="MISSING_REQUIRED_FIELD",
                    )
                )

        # Add operation-specific validation logic here if needed

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def apply_operation(self, state: ProviderState, op: Operation) -> ProviderState:
        """Apply an operation to state"""
        unity_state = UnityState(**state) if not isinstance(state, UnityState) else state
        result_state = apply_operation(unity_state, op)
        return result_state.model_dump(by_alias=True)

    def apply_operations(self, state: ProviderState, ops: list[Operation]) -> ProviderState:
        """Apply multiple operations to state"""
        unity_state = UnityState(**state) if not isinstance(state, UnityState) else state
        result_state = apply_operations(unity_state, ops)
        return result_state.model_dump(by_alias=True)

    def get_sql_generator(
        self, state: ProviderState, catalog_name_mapping: dict[str, str] | None = None
    ) -> SQLGenerator:
        """Get SQL generator for Unity Catalog with optional catalog name mapping"""
        unity_state = UnityState(**state) if not isinstance(state, UnityState) else state
        return UnitySQLGenerator(unity_state, catalog_name_mapping)

    def create_initial_state(self) -> ProviderState:
        """Create an empty initial state"""
        return {"catalogs": []}

    def validate_state(self, state: ProviderState) -> ValidationResult:
        """Validate the entire state structure"""
        errors: list[ValidationError] = []

        # Validate state structure
        if "catalogs" not in state or not isinstance(state["catalogs"], list):
            errors.append(
                ValidationError(
                    field="catalogs",
                    message="Catalogs must be an array",
                    code="INVALID_STATE_STRUCTURE",
                )
            )
            return ValidationResult(valid=False, errors=errors)

        # Validate each catalog
        for i, catalog in enumerate(state["catalogs"]):
            if not catalog.get("id") or not catalog.get("name"):
                errors.append(
                    ValidationError(
                        field=f"catalogs[{i}]",
                        message="Catalog must have id and name",
                        code="INVALID_CATALOG",
                    )
                )

            if "schemas" not in catalog or not isinstance(catalog["schemas"], list):
                errors.append(
                    ValidationError(
                        field=f"catalogs[{i}].schemas",
                        message="Schemas must be an array",
                        code="INVALID_SCHEMA_STRUCTURE",
                    )
                )
                continue

            # Validate schemas
            for j, schema in enumerate(catalog["schemas"]):
                if not schema.get("id") or not schema.get("name"):
                    errors.append(
                        ValidationError(
                            field=f"catalogs[{i}].schemas[{j}]",
                            message="Schema must have id and name",
                            code="INVALID_SCHEMA",
                        )
                    )

                if "tables" not in schema or not isinstance(schema["tables"], list):
                    errors.append(
                        ValidationError(
                            field=f"catalogs[{i}].schemas[{j}].tables",
                            message="Tables must be an array",
                            code="INVALID_TABLE_STRUCTURE",
                        )
                    )

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def get_sql_executor(self, config: ExecutionConfig) -> SQLExecutor:
        """Get SQL executor for Unity Catalog

        Creates an authenticated Databricks client and returns a
        UnitySQLExecutor instance for executing SQL statements.

        Args:
            config: Execution configuration with profile and warehouse

        Returns:
            UnitySQLExecutor instance

        Raises:
            AuthenticationError: If authentication fails
        """
        # Create authenticated client using profile
        client = create_databricks_client(config.profile)

        # Return executor
        return UnitySQLExecutor(client)

    def validate_execution_config(self, config: ExecutionConfig) -> ValidationResult:
        """Validate execution configuration for Unity Catalog

        Validates:
        - Profile exists (if specified)
        - Warehouse ID is provided
        - Configuration is complete

        Args:
            config: Execution configuration to validate

        Returns:
            ValidationResult with any configuration errors
        """
        errors: list[ValidationError] = []

        # Validate warehouse ID
        if not config.warehouse_id:
            errors.append(
                ValidationError(
                    field="warehouse_id",
                    message="Warehouse ID is required for Unity Catalog execution",
                    code="MISSING_WAREHOUSE_ID",
                )
            )

        # Validate profile (if specified)
        if config.profile and not check_profile_exists(config.profile):
            errors.append(
                ValidationError(
                    field="profile",
                    message=f"Databricks profile '{config.profile}' not found in ~/.databrickscfg",
                    code="PROFILE_NOT_FOUND",
                )
            )

        # Validate timeout
        if config.timeout_seconds <= 0:
            errors.append(
                ValidationError(
                    field="timeout_seconds",
                    message="Timeout must be positive",
                    code="INVALID_TIMEOUT",
                )
            )

        return ValidationResult(valid=len(errors) == 0, errors=errors)


# Export singleton instance
unity_provider = UnityProvider()
