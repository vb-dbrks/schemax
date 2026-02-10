"""
Unity Catalog Provider

Main provider implementation for Databricks Unity Catalog.
Implements the Provider interface to enable Unity Catalog support in Schematic.
"""

import hashlib
from typing import Any

from databricks.sdk.service.sql import StatementState

from schematic.providers.base.executor import ExecutionConfig, SQLExecutor
from schematic.providers.base.models import ProviderState, ValidationError, ValidationResult
from schematic.providers.base.operations import Operation
from schematic.providers.base.provider import BaseProvider, ProviderCapabilities, ProviderInfo
from schematic.providers.base.sql_generator import SQLGenerator
from schematic.providers.base.state_differ import StateDiffer

from .auth import check_profile_exists, create_databricks_client
from .executor import UnitySQLExecutor
from .hierarchy import unity_hierarchy
from .models import UnityState
from .operations import UNITY_OPERATIONS, unity_operation_metadata
from .sql_generator import UnitySQLGenerator
from .state_differ import UnityStateDiffer
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
        self,
        state: ProviderState,
        name_mapping: dict[str, str] | None = None,
        managed_locations: dict[str, Any] | None = None,
        external_locations: dict[str, Any] | None = None,
        environment_name: str | None = None,
    ) -> SQLGenerator:
        """Get SQL generator for Unity Catalog with optional environment-specific configuration

        Args:
            state: Provider state to generate SQL for
            name_mapping: Optional catalog name mapping (logical → physical)
            managed_locations: Optional project-level managed locations config
            external_locations: Optional project-level external locations config
            environment_name: Optional target environment name for path resolution

        Returns:
            UnitySQLGenerator instance configured for the target environment
        """
        unity_state = UnityState(**state) if not isinstance(state, UnityState) else state
        return UnitySQLGenerator(
            unity_state,
            catalog_name_mapping=name_mapping,
            managed_locations=managed_locations,
            external_locations=external_locations,
            environment_name=environment_name,
        )

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
                    continue

                # Validate each table: no duplicate column names
                for k, table in enumerate(schema["tables"]):
                    if "columns" not in table or not isinstance(table["columns"], list):
                        continue
                    seen: dict[str, int] = {}
                    for col in table["columns"]:
                        name = col.get("name") if isinstance(col, dict) else None
                        if not name:
                            continue
                        if name in seen:
                            table_name = table.get("name", "?")
                            errors.append(
                                ValidationError(
                                    field=f"catalogs[{i}].schemas[{j}].tables[{k}].columns",
                                    message=(
                                        f"Table '{table_name}' has duplicate column name '{name}'. "
                                        "Column names must be unique within a table."
                                    ),
                                    code="DUPLICATE_COLUMN_NAME",
                                )
                            )
                            break
                        seen[name] = 1

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

    def discover_state(
        self,
        config: ExecutionConfig,
        scope: dict[str, Any] | None = None,
    ) -> ProviderState:
        """Discover live Unity Catalog state for import workflows."""
        client = create_databricks_client(config.profile)
        scope = scope or {}

        catalog_filter = scope.get("catalog")
        schema_filter = scope.get("schema")
        table_filter = scope.get("table")

        catalogs = self._fetch_catalogs(client, config.warehouse_id, catalog_filter)
        if not catalogs:
            return {"catalogs": []}

        state_catalogs = []
        for catalog_name in catalogs:
            schemas = self._fetch_schemas(client, config.warehouse_id, catalog_name, schema_filter)
            state_schemas = []
            for schema_name in schemas:
                table_rows = self._fetch_tables(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )
                columns_by_table = self._fetch_columns(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )

                tables = []
                views = []
                for row in table_rows:
                    table_name = str(row["table_name"])
                    table_kind = str(row.get("table_type", "BASE TABLE")).upper()

                    if table_kind == "VIEW":
                        views.append(
                            {
                                "id": self._stable_id(
                                    "view", catalog_name, schema_name, table_name
                                ),
                                "name": table_name,
                                "definition": "",
                                "comment": None,
                                "dependencies": None,
                                "extractedDependencies": None,
                                "tags": {},
                                "properties": {},
                            }
                        )
                        continue

                    column_rows = columns_by_table.get(table_name, [])
                    columns = []
                    for col in column_rows:
                        col_name = str(col["column_name"])
                        col_type = str(col.get("data_type", "STRING"))
                        nullable = str(col.get("is_nullable", "YES")).upper() == "YES"
                        columns.append(
                            {
                                "id": self._stable_id(
                                    "col", catalog_name, schema_name, table_name, col_name
                                ),
                                "name": col_name,
                                "type": col_type,
                                "nullable": nullable,
                                "comment": col.get("comment"),
                                "tags": {},
                                "maskId": None,
                            }
                        )

                    tables.append(
                        {
                            "id": self._stable_id(
                                "tbl", catalog_name, schema_name, table_name
                            ),
                            "name": table_name,
                            "format": "delta",
                            "external": None,
                            "externalLocationName": None,
                            "path": None,
                            "partitionColumns": None,
                            "clusterColumns": None,
                            "columnMapping": None,
                            "columns": columns,
                            "properties": {},
                            "tags": {},
                            "constraints": [],
                            "grants": [],
                            "comment": row.get("comment"),
                            "rowFilters": [],
                            "columnMasks": [],
                        }
                    )

                state_schemas.append(
                    {
                        "id": self._stable_id("sch", catalog_name, schema_name),
                        "name": schema_name,
                        "managedLocationName": None,
                        "comment": None,
                        "tags": {},
                        "tables": tables,
                        "views": views,
                    }
                )

            state_catalogs.append(
                {
                    "id": self._stable_id("cat", catalog_name),
                    "name": catalog_name,
                    "managedLocationName": None,
                    "comment": None,
                    "tags": {},
                    "schemas": state_schemas,
                }
            )

        return {"catalogs": state_catalogs}

    def _stable_id(self, prefix: str, *parts: str) -> str:
        """Create deterministic object ID from provider object identity."""
        normalized = "||".join(str(p).strip().lower() for p in parts)
        digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
        return f"{prefix}_{digest}"

    def _fetch_catalogs(
        self,
        client: Any,
        warehouse_id: str,
        catalog_filter: str | None,
    ) -> list[str]:
        sql = "SELECT catalog_name FROM system.information_schema.catalogs"
        if catalog_filter:
            safe = catalog_filter.replace("'", "''")
            sql += f" WHERE catalog_name = '{safe}'"
        sql += " ORDER BY catalog_name"
        rows = self._execute_query(client, warehouse_id, sql)
        return [str(r["catalog_name"]) for r in rows if r.get("catalog_name")]

    def _fetch_schemas(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_filter: str | None,
    ) -> list[str]:
        cat = catalog_name.replace("'", "''")
        sql = (
            "SELECT schema_name "
            "FROM system.information_schema.schemata "
            f"WHERE catalog_name = '{cat}'"
        )
        if schema_filter:
            sch = schema_filter.replace("'", "''")
            sql += f" AND schema_name = '{sch}'"
        sql += " ORDER BY schema_name"
        rows = self._execute_query(client, warehouse_id, sql)
        return [str(r["schema_name"]) for r in rows if r.get("schema_name")]

    def _fetch_tables(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_filter: str | None,
    ) -> list[dict[str, Any]]:
        cat = catalog_name.replace("'", "''")
        sch = schema_name.replace("'", "''")
        sql = (
            "SELECT table_name, table_type, comment "
            "FROM system.information_schema.tables "
            f"WHERE table_catalog = '{cat}' "
            f"AND table_schema = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name"
        return self._execute_query(client, warehouse_id, sql)

    def _fetch_columns(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_filter: str | None,
    ) -> dict[str, list[dict[str, Any]]]:
        cat = catalog_name.replace("'", "''")
        sch = schema_name.replace("'", "''")
        sql = (
            "SELECT table_name, column_name, data_type, is_nullable, comment, ordinal_position "
            "FROM system.information_schema.columns "
            f"WHERE table_catalog = '{cat}' "
            f"AND table_schema = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name, ordinal_position"
        rows = self._execute_query(client, warehouse_id, sql)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            table_name = str(row.get("table_name", ""))
            if not table_name:
                continue
            grouped.setdefault(table_name, []).append(row)
        return grouped

    def _execute_query(
        self,
        client: Any,
        warehouse_id: str,
        sql: str,
    ) -> list[dict[str, Any]]:
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        if not response.status or response.status.state != StatementState.SUCCEEDED:
            msg = "query failed"
            if response.status and response.status.error:
                msg = str(getattr(response.status.error, "message", msg) or msg)
            raise RuntimeError(f"Unity metadata discovery query failed: {msg}")

        if (
            not response.result
            or not response.result.data_array
            or not response.manifest
            or not response.manifest.schema
            or not response.manifest.schema.columns
        ):
            return []

        columns = [c.name for c in response.manifest.schema.columns]
        rows: list[dict[str, Any]] = []
        for row in response.result.data_array:
            row_dict: dict[str, Any] = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = value
            rows.append(row_dict)
        return rows

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

    def get_state_differ(
        self,
        old_state: ProviderState,
        new_state: ProviderState,
        old_operations: list[Operation],
        new_operations: list[Operation],
    ) -> StateDiffer:
        """Get state differ for Unity Catalog

        Args:
            old_state: Previous state (source)
            new_state: Current state (target)
            old_operations: Operations that created old_state
            new_operations: Operations that created new_state

        Returns:
            UnityStateDiffer instance for generating diff operations
        """
        return UnityStateDiffer(old_state, new_state, old_operations, new_operations)


# Export singleton instance
unity_provider = UnityProvider()
