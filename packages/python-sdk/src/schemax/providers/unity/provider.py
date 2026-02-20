"""
Unity Catalog Provider

Main provider implementation for Databricks Unity Catalog.
Implements the Provider interface to enable Unity Catalog support in SchemaX.
"""

import hashlib
import json
import os
import threading
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any

from databricks.sdk.service.sql import StatementState

from schemax.providers.base.executor import ExecutionConfig, ExecutionResult, SQLExecutor
from schemax.providers.base.models import ProviderState, ValidationError, ValidationResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.provider import BaseProvider, ProviderCapabilities, ProviderInfo
from schemax.providers.base.sql_generator import SQLGenerator
from schemax.providers.base.sql_parser import extract_table_references
from schemax.providers.base.state_differ import StateDiffer

from .auth import check_profile_exists, create_databricks_client
from .ddl_parser import state_from_ddl as unity_state_from_ddl
from .executor import UnitySQLExecutor
from .hierarchy import unity_hierarchy
from .models import UnityState
from .operations import UNITY_OPERATIONS, unity_operation_metadata
from .sql_generator import UnitySQLGenerator
from .state_differ import UnityStateDiffer
from .state_reducer import apply_operation, apply_operations


class UnityProvider(BaseProvider):
    """Unity Catalog Provider Implementation"""

    SYSTEM_SCHEMA_NAME = "information_schema"
    SYSTEM_CATALOG_NAMES = frozenset({"system"})
    VIEW_TABLE_TYPES = frozenset({"VIEW"})
    MATERIALIZED_VIEW_TABLE_TYPES = frozenset({"MATERIALIZED VIEW", "MATERIALIZED_VIEW"})
    UNSUPPORTED_DISCOVERY_TABLE_TYPES = frozenset(
        {
            "STREAMING TABLE",
            "STREAMING_TABLE",
        }
    )
    DEFAULT_IMPORT_PROPERTIES_WORKERS = 6

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            id="unity",
            name="Unity Catalog",
            version="1.0.0",
            description="Databricks Unity Catalog provider with full governance features",
            author="SchemaX Team",
            docs_url="https://docs.databricks.com/data-governance/unity-catalog/index.html",
        )

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supported_operations=list(UNITY_OPERATIONS.values()),
            supported_object_types=[
                "catalog",
                "schema",
                "table",
                "column",
                "view",
                "volume",
                "function",
                "materialized_view",
            ],
            hierarchy=unity_hierarchy,
            features={
                "constraints": True,
                "row_filters": True,
                "column_masks": True,
                "column_tags": True,
                "table_properties": True,
                "comments": True,
                "partitioning": False,  # Future
                "views": True,
                "volumes": True,
                "functions": True,
                "materialized_views": True,
                "indexes": False,  # Not applicable to Unity Catalog
                "baseline_adoption": True,
            },
        )

    def __init__(self) -> None:
        super().__init__()
        self._last_import_warnings: list[str] = []
        self._import_thread_local = threading.local()
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
        self._last_import_warnings = []
        # Reset per-run cached clients to avoid cross-run leakage.
        self._import_thread_local = threading.local()
        client = create_databricks_client(config.profile)
        scope = scope or {}

        catalog_filter = scope.get("catalog")
        schema_filter = scope.get("schema")
        table_filter = scope.get("table")

        catalogs = self._fetch_catalogs(client, config.warehouse_id, catalog_filter)
        if not catalogs:
            return {"catalogs": []}
        catalog_metadata = self._fetch_catalog_metadata(client, config.warehouse_id, catalog_filter)

        state_catalogs: list[dict[str, Any]] = []
        for catalog_name in catalogs:
            schema_metadata = self._fetch_schema_metadata(
                client,
                config.warehouse_id,
                catalog_name,
                schema_filter,
            )
            schemas = self._fetch_schemas(client, config.warehouse_id, catalog_name, schema_filter)
            state_schemas: list[dict[str, Any]] = []
            for schema_name in schemas:
                table_rows = self._fetch_tables(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )
                view_definitions = self._fetch_views(
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
                tags_by_table = self._fetch_table_tags(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )
                column_tags_by_table = self._fetch_column_tags(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )
                constraints_by_table = self._fetch_constraints(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_filter,
                )
                table_names = [
                    str(row.get("table_name", ""))
                    for row in table_rows
                    if str(row.get("table_type", "BASE TABLE")).upper() not in self.VIEW_TABLE_TYPES
                    and str(row.get("table_type", "BASE TABLE")).upper()
                    not in self.UNSUPPORTED_DISCOVERY_TABLE_TYPES
                    and row.get("table_name")
                ]
                view_names = [
                    str(row.get("table_name", ""))
                    for row in table_rows
                    if str(row.get("table_type", "BASE TABLE")).upper() in self.VIEW_TABLE_TYPES
                    and row.get("table_name")
                ]
                mv_names = [
                    str(row.get("table_name", ""))
                    for row in table_rows
                    if str(row.get("table_type", "BASE TABLE")).upper()
                    in self.MATERIALIZED_VIEW_TABLE_TYPES
                    and row.get("table_name")
                ]
                view_and_mv_names = view_names + mv_names
                properties_by_table = self._fetch_table_properties_for_tables(
                    config=config,
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_names=table_names,
                )
                constraints_by_table = self._merge_check_constraints_from_properties(
                    constraints_by_table,
                    properties_by_table,
                )
                table_details_by_table = self._fetch_table_details_for_tables(
                    config=config,
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_names=table_names,
                )
                properties_by_view = self._fetch_table_properties_for_tables(
                    config=config,
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_names=view_and_mv_names,
                )

                tables = []
                views: list[dict[str, Any]] = []
                materialized_views: list[dict[str, Any]] = []
                for row in table_rows:
                    table_name = str(row["table_name"])
                    table_kind = str(row.get("table_type", "BASE TABLE")).upper()

                    if table_kind in self.UNSUPPORTED_DISCOVERY_TABLE_TYPES:
                        self._last_import_warnings.append(
                            "Unsupported Unity object type discovered during import and skipped: "
                            f"{catalog_name}.{schema_name}.{table_name} ({table_kind})."
                        )
                        continue

                    if table_kind in self.MATERIALIZED_VIEW_TABLE_TYPES:
                        definition = str(
                            view_definitions.get(table_name, {}).get("definition") or ""
                        )
                        view_properties = properties_by_view.get(table_name, {})
                        comment = row.get("comment") or view_properties.get("comment")
                        extracted_dependencies = None
                        if definition:
                            extracted_dependencies = extract_table_references(
                                definition,
                                dialect="databricks",
                            )
                        materialized_views.append(
                            {
                                "id": self._stable_id(
                                    "mv", catalog_name, schema_name, table_name
                                ),
                                "name": table_name,
                                "definition": definition,
                                "comment": comment,
                                "refreshSchedule": view_properties.get(
                                    "refresh_schedule"
                                ) or view_properties.get("refreshSchedule"),
                                "dependencies": None,
                                "extractedDependencies": extracted_dependencies,
                                "grants": [],
                            }
                        )
                        continue

                    if table_kind in self.VIEW_TABLE_TYPES:
                        definition = str(
                            view_definitions.get(table_name, {}).get("definition") or ""
                        )
                        view_properties = properties_by_view.get(table_name, {})
                        comment = row.get("comment")
                        if not comment:
                            comment = view_properties.get("comment")
                        extracted_dependencies = None
                        if definition:
                            extracted_dependencies = extract_table_references(
                                definition,
                                dialect="databricks",
                            )
                        views.append(
                            {
                                "id": self._stable_id(
                                    "view", catalog_name, schema_name, table_name
                                ),
                                "name": table_name,
                                "definition": definition,
                                "comment": comment,
                                "dependencies": None,
                                "extractedDependencies": extracted_dependencies,
                                "tags": tags_by_table.get(table_name, {}),
                                "properties": view_properties,
                            }
                        )
                        continue

                    column_rows = columns_by_table.get(table_name, [])
                    column_tags = column_tags_by_table.get(table_name, {})
                    columns: list[dict[str, Any]] = []
                    column_id_by_name: dict[str, str] = {}
                    for col in column_rows:
                        col_name = str(col["column_name"])
                        col_type = str(col.get("data_type", "STRING"))
                        nullable = str(col.get("is_nullable", "YES")).upper() == "YES"
                        col_id = self._stable_id(
                            "col", catalog_name, schema_name, table_name, col_name
                        )
                        columns.append(
                            {
                                "id": col_id,
                                "name": col_name,
                                "type": col_type,
                                "nullable": nullable,
                                "comment": col.get("comment"),
                                "tags": column_tags.get(col_name, {}),
                                "maskId": None,
                            }
                        )
                        column_id_by_name[col_name] = col_id

                    properties = properties_by_table.get(table_name, {})
                    table_detail = table_details_by_table.get(table_name, {})
                    data_source_format = str(
                        table_detail.get("format")
                        or row.get("data_source_format")
                        or row.get("DATA_SOURCE_FORMAT")
                        or "DELTA"
                    ).lower()
                    if data_source_format not in {"delta", "iceberg"}:
                        self._last_import_warnings.append(
                            "Unsupported Unity table format discovered during import and skipped: "
                            f"{catalog_name}.{schema_name}.{table_name} ({data_source_format})."
                        )
                        continue
                    storage_path = row.get("storage_path")
                    if storage_path is None:
                        storage_path = row.get("STORAGE_PATH")
                    if storage_path is None:
                        storage_path = table_detail.get("location")
                    table_external: bool | None = None
                    if table_kind in {"EXTERNAL"}:
                        table_external = True
                    elif table_kind in {"MANAGED", "BASE TABLE", "BASE_TABLE"}:
                        table_external = False
                    constraints = self._build_table_constraints(
                        catalog_name,
                        schema_name,
                        table_name,
                        constraints_by_table.get(table_name, []),
                        column_id_by_name,
                    )

                    tables.append(
                        {
                            "id": self._stable_id("tbl", catalog_name, schema_name, table_name),
                            "name": table_name,
                            "format": data_source_format,
                            "external": table_external,
                            "externalLocationName": None,
                            "path": None if storage_path is None else str(storage_path),
                            "partitionColumns": table_detail.get("partitionColumns"),
                            "clusterColumns": table_detail.get("clusterColumns"),
                            "columnMapping": None,
                            "columns": columns,
                            "properties": properties,
                            "tags": tags_by_table.get(table_name, {}),
                            "constraints": constraints,
                            "grants": [],
                            "comment": row.get("comment"),
                            "rowFilters": [],
                            "columnMasks": [],
                        }
                    )

                volumes: list[dict[str, Any]] = []
                functions: list[dict[str, Any]] = []
                try:
                    volumes = self._discover_volumes_for_schema(
                        client, config.warehouse_id, catalog_name, schema_name
                    )
                except Exception as e:
                    self._last_import_warnings.append(
                        f"Could not discover volumes for {catalog_name}.{schema_name}: {e}"
                    )
                try:
                    functions = self._discover_functions_for_schema(
                        client, config.warehouse_id, catalog_name, schema_name
                    )
                except Exception as e:
                    self._last_import_warnings.append(
                        f"Could not discover functions for {catalog_name}.{schema_name}: {e}"
                    )
                schema_meta = schema_metadata.get(schema_name, {})
                state_schemas.append(
                    {
                        "id": self._stable_id("sch", catalog_name, schema_name),
                        "name": schema_name,
                        "managedLocationName": None,
                        "comment": schema_meta.get("comment"),
                        "tags": schema_meta.get("tags", {}),
                        "tables": tables,
                        "views": views,
                        "volumes": volumes,
                        "functions": functions,
                        "materialized_views": materialized_views,
                    }
                )

            cat_meta = catalog_metadata.get(catalog_name, {})
            state_catalogs.append(
                {
                    "id": self._stable_id("cat", catalog_name),
                    "name": catalog_name,
                    "managedLocationName": None,
                    "comment": cat_meta.get("comment"),
                    "tags": cat_meta.get("tags", {}),
                    "schemas": state_schemas,
                }
            )

        return {"catalogs": state_catalogs}

    def state_from_ddl(
        self,
        sql_path: Path | None = None,
        sql_statements: list[str] | None = None,
        dialect: str = "databricks",
    ) -> tuple[ProviderState, Any]:
        """Build Unity state from a SQL DDL file or list of statements."""
        return unity_state_from_ddl(
            sql_path=sql_path,
            sql_statements=sql_statements,
            dialect=dialect,
        )

    def collect_import_warnings(
        self,
        config: ExecutionConfig,
        scope: dict[str, Any],
        discovered_state: ProviderState,
    ) -> list[str]:
        del config, scope, discovered_state
        # Keep ordering stable but remove duplicates.
        seen: set[str] = set()
        ordered: list[str] = []
        for warning in self._last_import_warnings:
            if warning in seen:
                continue
            seen.add(warning)
            ordered.append(warning)
        return ordered

    def validate_import_scope(self, scope: dict[str, Any]) -> ValidationResult:
        catalog = str(scope.get("catalog") or "").strip().lower()
        if catalog in self.SYSTEM_CATALOG_NAMES:
            return ValidationResult(
                valid=False,
                errors=[
                    ValidationError(
                        field="catalog",
                        message=(
                            f"Catalog '{catalog}' is system-managed in Unity Catalog "
                            "and cannot be imported."
                        ),
                        code="UNSUPPORTED_IMPORT_SCOPE",
                    )
                ],
            )

        schema = str(scope.get("schema") or "").strip().lower()
        if schema == self.SYSTEM_SCHEMA_NAME:
            return ValidationResult(
                valid=False,
                errors=[
                    ValidationError(
                        field="schema",
                        message=(
                            "Schema 'information_schema' is system-managed in Unity Catalog "
                            "and cannot be imported."
                        ),
                        code="UNSUPPORTED_IMPORT_SCOPE",
                    )
                ],
            )
        return ValidationResult(valid=True, errors=[])

    def prepare_import_state(
        self,
        local_state: ProviderState,
        discovered_state: ProviderState,
        env_config: dict[str, Any],
        mapping_overrides: dict[str, str] | None = None,
    ) -> tuple[ProviderState, dict[str, str], bool]:
        top_level_name = env_config.get("topLevelName")
        existing_mappings = self._read_catalog_mappings(env_config)
        catalog_mappings = dict(existing_mappings)
        if mapping_overrides:
            for logical, physical in mapping_overrides.items():
                catalog_mappings[str(logical)] = str(physical)
        reverse_mappings = self._build_reverse_mappings(catalog_mappings)

        local_catalogs = (local_state or {}).get("catalogs", []) or []
        local_catalog_by_name = {
            str(cat.get("name")): cat
            for cat in local_catalogs
            if isinstance(cat, dict) and cat.get("name")
        }

        normalized = deepcopy(discovered_state or {})
        normalized_catalogs = []
        for discovered_catalog in normalized.get("catalogs", []) or []:
            if not isinstance(discovered_catalog, dict):
                continue

            physical_name = str(discovered_catalog.get("name", ""))
            if not physical_name:
                continue

            logical_name = reverse_mappings.get(physical_name)
            if logical_name is None:
                if (
                    top_level_name
                    and physical_name == top_level_name
                    and len(local_catalog_by_name) == 1
                ):
                    logical_name = next(iter(local_catalog_by_name.keys()))
                else:
                    logical_name = physical_name

                existing_physical = catalog_mappings.get(logical_name)
                if existing_physical and existing_physical != physical_name:
                    # Compatibility path for tests/local flows that pass already-logical
                    # discovered state into import preparation.
                    if physical_name == logical_name:
                        physical_name = existing_physical
                    else:
                        raise ValueError(
                            "Catalog mapping conflict for logical catalog "
                            f"'{logical_name}': '{existing_physical}' vs '{physical_name}'"
                        )

                if existing_physical is None:
                    catalog_mappings[logical_name] = physical_name
                    reverse_mappings[physical_name] = logical_name

            local_catalog = local_catalog_by_name.get(logical_name)
            normalized_catalog = self._normalize_catalog_tree(discovered_catalog, local_catalog)
            normalized_catalog["name"] = logical_name
            normalized_catalogs.append(normalized_catalog)

        normalized["catalogs"] = normalized_catalogs
        return normalized, catalog_mappings, catalog_mappings != existing_mappings

    def update_env_import_mappings(
        self,
        env_config: dict[str, Any],
        mappings: dict[str, str],
    ) -> None:
        env_config["catalogMappings"] = dict(sorted(mappings.items()))

    def adopt_import_baseline(
        self,
        project: dict[str, Any],
        env_config: dict[str, Any],
        target_env: str,
        profile: str,
        warehouse_id: str,
        snapshot_version: str,
    ) -> str:
        from schemax.core.deployment import DeploymentTracker

        deployment_catalog = env_config["topLevelName"]
        auto_create_schema = env_config.get("autoCreateSchemaxSchema", True)

        client = create_databricks_client(profile)
        tracker = DeploymentTracker(client, deployment_catalog, warehouse_id)
        tracker.ensure_tracking_schema(auto_create=auto_create_schema)

        latest_success = tracker.get_latest_deployment(target_env)
        from_snapshot_version = latest_success.get("version") if latest_success else None
        previous_deployment_id = tracker.get_most_recent_deployment_id(target_env)

        import uuid

        deployment_id = f"deploy_import_{uuid.uuid4().hex[:8]}"
        tracker.start_deployment(
            deployment_id=deployment_id,
            environment=target_env,
            snapshot_version=snapshot_version,
            project_name=project.get("name", "unknown"),
            provider_type=self.info.id,
            provider_version=self.info.version,
            schemax_version="0.2.0",
            from_snapshot_version=from_snapshot_version,
            previous_deployment_id=previous_deployment_id,
        )

        tracker.complete_deployment(
            deployment_id=deployment_id,
            result=ExecutionResult(
                deployment_id=deployment_id,
                total_statements=0,
                successful_statements=0,
                failed_statement_index=None,
                statement_results=[],
                total_execution_time_ms=0,
                status="success",
                error_message=None,
            ),
        )
        return deployment_id

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
        else:
            excluded = ", ".join(
                f"'{catalog_name}'" for catalog_name in sorted(self.SYSTEM_CATALOG_NAMES)
            )
            sql += f" WHERE lower(catalog_name) NOT IN ({excluded})"
        sql += " ORDER BY catalog_name"
        rows = self._execute_query(client, warehouse_id, sql)
        return [str(r["catalog_name"]) for r in rows if r.get("catalog_name")]

    def _fetch_catalog_metadata(
        self,
        client: Any,
        warehouse_id: str,
        catalog_filter: str | None,
    ) -> dict[str, dict[str, Any]]:
        sql_comments = "SELECT catalog_name, comment FROM system.information_schema.catalogs"
        if catalog_filter:
            safe = catalog_filter.replace("'", "''")
            sql_comments += f" WHERE catalog_name = '{safe}'"
        sql_comments += " ORDER BY catalog_name"
        comment_rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_comments,
            warning=(
                "Could not discover Unity catalog comments from information_schema.catalogs; "
                "continuing without catalog comments."
            ),
        )

        metadata: dict[str, dict[str, Any]] = {}
        for row in comment_rows:
            catalog_name = str(row.get("catalog_name") or "")
            if not catalog_name:
                continue
            metadata[catalog_name] = {
                "comment": row.get("comment"),
                "tags": {},
            }

        sql_tags = (
            "SELECT catalog_name, tag_name, tag_value FROM system.information_schema.catalog_tags"
        )
        if catalog_filter:
            safe = catalog_filter.replace("'", "''")
            sql_tags += f" WHERE catalog_name = '{safe}'"
        sql_tags += " ORDER BY catalog_name, tag_name"
        tag_rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_tags,
            warning=(
                "Could not discover Unity catalog tags from information_schema.catalog_tags; "
                "continuing without catalog tags."
            ),
        )
        for row in tag_rows:
            catalog_name = str(row.get("catalog_name") or "")
            tag_name = str(row.get("tag_name") or "")
            if not catalog_name or not tag_name:
                continue
            tag_value = row.get("tag_value")
            metadata.setdefault(catalog_name, {"comment": None, "tags": {}})["tags"][tag_name] = (
                "" if tag_value is None else str(tag_value)
            )

        return metadata

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
            f"WHERE catalog_name = '{cat}' "
            "AND lower(schema_name) <> 'information_schema'"
        )
        if schema_filter:
            sch = schema_filter.replace("'", "''")
            sql += f" AND schema_name = '{sch}'"
        sql += " ORDER BY schema_name"
        rows = self._execute_query(client, warehouse_id, sql)
        return [str(r["schema_name"]) for r in rows if r.get("schema_name")]

    def _fetch_schema_metadata(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_filter: str | None,
    ) -> dict[str, dict[str, Any]]:
        cat = catalog_name.replace("'", "''")
        sql_comments = (
            "SELECT schema_name, comment "
            "FROM system.information_schema.schemata "
            f"WHERE catalog_name = '{cat}' "
            "AND lower(schema_name) <> 'information_schema'"
        )
        if schema_filter:
            sch = schema_filter.replace("'", "''")
            sql_comments += f" AND schema_name = '{sch}'"
        sql_comments += " ORDER BY schema_name"
        comment_rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_comments,
            warning=(
                "Could not discover Unity schema comments from information_schema.schemata; "
                "continuing without schema comments."
            ),
        )

        metadata: dict[str, dict[str, Any]] = {}
        for row in comment_rows:
            schema_name = str(row.get("schema_name") or "")
            if not schema_name:
                continue
            metadata[schema_name] = {
                "comment": row.get("comment"),
                "tags": {},
            }

        sql_tags = (
            "SELECT schema_name, tag_name, tag_value "
            "FROM system.information_schema.schema_tags "
            f"WHERE catalog_name = '{cat}'"
        )
        if schema_filter:
            sch = schema_filter.replace("'", "''")
            sql_tags += f" AND schema_name = '{sch}'"
        sql_tags += " ORDER BY schema_name, tag_name"
        tag_rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_tags,
            warning=(
                "Could not discover Unity schema tags from information_schema.schema_tags; "
                "continuing without schema tags."
            ),
        )
        for row in tag_rows:
            schema_name = str(row.get("schema_name") or "")
            tag_name = str(row.get("tag_name") or "")
            if not schema_name or not tag_name:
                continue
            tag_value = row.get("tag_value")
            metadata.setdefault(schema_name, {"comment": None, "tags": {}})["tags"][tag_name] = (
                "" if tag_value is None else str(tag_value)
            )

        return metadata

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
            "SELECT table_name, table_type, comment, data_source_format, storage_path "
            "FROM system.information_schema.tables "
            f"WHERE table_catalog = '{cat}' "
            f"AND table_schema = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name"
        return self._execute_query(client, warehouse_id, sql)

    def _fetch_views(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_filter: str | None,
    ) -> dict[str, dict[str, Any]]:
        cat = catalog_name.replace("'", "''")
        sch = schema_name.replace("'", "''")
        sql = (
            "SELECT table_name, view_definition, is_materialized "
            "FROM system.information_schema.views "
            f"WHERE table_catalog = '{cat}' "
            f"AND table_schema = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name"
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                "Could not discover Unity view definitions from information_schema.views; "
                "continuing with empty view definitions."
            ),
        )

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            table_name = str(row.get("table_name") or "")
            if not table_name:
                continue
            grouped[table_name] = {
                "definition": row.get("view_definition"),
                "is_materialized": row.get("is_materialized"),
            }
        return grouped

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

    def _fetch_table_tags(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_filter: str | None,
    ) -> dict[str, dict[str, str]]:
        cat = catalog_name.replace("'", "''")
        sch = schema_name.replace("'", "''")
        # TABLE_TAGS uses CATALOG_NAME, SCHEMA_NAME (not table_catalog/table_schema)
        sql = (
            "SELECT table_name, tag_name, tag_value "
            "FROM system.information_schema.table_tags "
            f"WHERE catalog_name = '{cat}' "
            f"AND schema_name = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name, tag_name"
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                "Could not discover Unity table tags from information_schema.table_tags; "
                "continuing without table tags."
            ),
        )

        grouped: dict[str, dict[str, str]] = {}
        for row in rows:
            table_name = str(row.get("table_name") or row.get("TABLE_NAME") or "")
            tag_name = str(row.get("tag_name") or row.get("TAG_NAME") or "")
            if not table_name or not tag_name:
                continue
            tag_value = row.get("tag_value")
            if tag_value is None:
                tag_value = row.get("TAG_VALUE")
            grouped.setdefault(table_name, {})[tag_name] = (
                "" if tag_value is None else str(tag_value)
            )
        return grouped

    def _fetch_column_tags(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_filter: str | None,
    ) -> dict[str, dict[str, dict[str, str]]]:
        cat = catalog_name.replace("'", "''")
        sch = schema_name.replace("'", "''")
        # COLUMN_TAGS uses CATALOG_NAME, SCHEMA_NAME (not table_catalog/table_schema)
        sql = (
            "SELECT table_name, column_name, tag_name, tag_value "
            "FROM system.information_schema.column_tags "
            f"WHERE catalog_name = '{cat}' "
            f"AND schema_name = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND table_name = '{tbl}'"
        sql += " ORDER BY table_name, column_name, tag_name"

        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                "Could not discover Unity column tags from information_schema.column_tags; "
                "continuing without column tags."
            ),
        )

        grouped: dict[str, dict[str, dict[str, str]]] = {}
        for row in rows:
            table_name = str(row.get("table_name") or row.get("TABLE_NAME") or "")
            column_name = str(row.get("column_name") or row.get("COLUMN_NAME") or "")
            tag_name = str(row.get("tag_name") or row.get("TAG_NAME") or "")
            if not table_name or not column_name or not tag_name:
                continue
            tag_value = row.get("tag_value")
            if tag_value is None:
                tag_value = row.get("TAG_VALUE")
            grouped.setdefault(table_name, {}).setdefault(column_name, {})[tag_name] = (
                "" if tag_value is None else str(tag_value)
            )
        return grouped

    def _fetch_table_properties(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> dict[str, str]:
        try:
            return self._fetch_table_properties_strict(
                client,
                warehouse_id,
                catalog_name,
                schema_name,
                table_name,
            )
        except RuntimeError:
            self._last_import_warnings.append(
                "Could not discover table TBLPROPERTIES via SHOW TBLPROPERTIES; "
                "continuing without table properties."
            )
            return {}

    def _fetch_table_properties_strict(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> dict[str, str]:
        cat = catalog_name.replace("`", "``")
        sch = schema_name.replace("`", "``")
        tbl = table_name.replace("`", "``")
        sql = f"SHOW TBLPROPERTIES `{cat}`.`{sch}`.`{tbl}`"
        rows = self._execute_query(client, warehouse_id, sql)

        properties: dict[str, str] = {}
        for row in rows:
            key = row.get("key")
            if key is None:
                key = row.get("KEY")
            if key is None:
                key = row.get("col_name")
            value = row.get("value")
            if value is None:
                value = row.get("VALUE")
            if value is None:
                value = row.get("data_type")
            if key is None:
                continue
            key_str = str(key)
            # Ignore Spark pseudo-row emitted when no properties are defined.
            if key_str.lower().startswith("transient_lastddltime"):
                continue
            properties[key_str] = "" if value is None else str(value)
        return properties

    def _fetch_constraints(
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
            "SELECT "
            "tc.table_name, "
            "tc.constraint_name, "
            "tc.constraint_type, "
            "kcu.column_name AS child_column_name, "
            "kcu.ordinal_position AS child_ordinal_position, "
            "pk.table_name AS parent_table_name, "
            "pk.column_name AS parent_column_name, "
            "pk.ordinal_position AS parent_ordinal_position, "
            "rc.update_rule, "
            "rc.delete_rule "
            "FROM system.information_schema.table_constraints tc "
            "LEFT JOIN system.information_schema.key_column_usage kcu "
            "ON tc.constraint_catalog = kcu.constraint_catalog "
            "AND tc.constraint_schema = kcu.constraint_schema "
            "AND tc.constraint_name = kcu.constraint_name "
            "LEFT JOIN system.information_schema.referential_constraints rc "
            "ON tc.constraint_catalog = rc.constraint_catalog "
            "AND tc.constraint_schema = rc.constraint_schema "
            "AND tc.constraint_name = rc.constraint_name "
            "LEFT JOIN system.information_schema.key_column_usage pk "
            "ON rc.unique_constraint_catalog = pk.constraint_catalog "
            "AND rc.unique_constraint_schema = pk.constraint_schema "
            "AND rc.unique_constraint_name = pk.constraint_name "
            "AND kcu.position_in_unique_constraint = pk.ordinal_position "
            f"WHERE tc.table_catalog = '{cat}' "
            f"AND tc.table_schema = '{sch}' "
            "AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql += f" AND tc.table_name = '{tbl}'"
        sql += " ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position"
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                "Could not discover table constraints from information_schema.table_constraints; "
                "continuing without imported constraints."
            ),
        )

        grouped_raw: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            table_name = str(row.get("table_name", ""))
            constraint_name = str(row.get("constraint_name", ""))
            constraint_type = str(row.get("constraint_type", "")).upper()
            if (
                not table_name
                or not constraint_name
                or constraint_type not in ("PRIMARY KEY", "FOREIGN KEY")
            ):
                continue

            by_table = grouped_raw.setdefault(table_name, {})
            entry = by_table.setdefault(
                constraint_name,
                {
                    "name": constraint_name,
                    "type": constraint_type,
                    "columns": [],
                    "parent_table": row.get("parent_table_name"),
                    "parent_columns": [],
                    "on_update": row.get("update_rule"),
                    "on_delete": row.get("delete_rule"),
                },
            )

            child_col = row.get("child_column_name")
            if child_col and child_col not in entry["columns"]:
                entry["columns"].append(child_col)

            parent_col = row.get("parent_column_name")
            if parent_col and parent_col not in entry["parent_columns"]:
                entry["parent_columns"].append(parent_col)

            if not entry.get("parent_table") and row.get("parent_table_name"):
                entry["parent_table"] = row.get("parent_table_name")

        grouped: dict[str, list[dict[str, Any]]] = {}
        for table_name, constraints in grouped_raw.items():
            grouped[table_name] = list(constraints.values())

        sql_checks = (
            "SELECT tc.table_name, tc.constraint_name, cc.check_clause "
            "FROM system.information_schema.table_constraints tc "
            "JOIN system.information_schema.check_constraints cc "
            "ON tc.constraint_catalog = cc.constraint_catalog "
            "AND tc.constraint_schema = cc.constraint_schema "
            "AND tc.constraint_name = cc.constraint_name "
            f"WHERE tc.table_catalog = '{cat}' "
            f"AND tc.table_schema = '{sch}' "
            "AND tc.constraint_type = 'CHECK'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql_checks += f" AND tc.table_name = '{tbl}'"
        sql_checks += " ORDER BY tc.table_name, tc.constraint_name"
        check_rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_checks,
            warning=(
                "Could not discover check constraints from information_schema.check_constraints; "
                "continuing without imported check constraints."
            ),
        )
        for row in check_rows:
            table_name = str(row.get("table_name") or "")
            constraint_name = str(row.get("constraint_name") or "")
            check_clause = row.get("check_clause")
            if not table_name or not constraint_name or check_clause in (None, ""):
                continue
            grouped.setdefault(table_name, []).append(
                {
                    "name": constraint_name,
                    "type": "CHECK",
                    "columns": [],
                    "expression": None if check_clause is None else str(check_clause),
                }
            )
        return grouped

    def _build_table_constraints(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        raw_constraints: list[dict[str, Any]],
        column_id_by_name: dict[str, str],
    ) -> list[dict[str, Any]]:
        constraints: list[dict[str, Any]] = []
        for raw in raw_constraints:
            constraint_type = str(raw.get("type", "")).upper()
            constraint_name = raw.get("name")
            columns = [
                column_id_by_name[col_name]
                for col_name in raw.get("columns", [])
                if col_name in column_id_by_name
            ]
            if constraint_type != "CHECK" and not columns:
                continue

            constraint_id = self._stable_id(
                "con",
                catalog_name,
                schema_name,
                table_name,
                str(constraint_name or constraint_type),
                ",".join(columns),
            )
            if constraint_type == "PRIMARY KEY":
                constraints.append(
                    {
                        "id": constraint_id,
                        "type": "primary_key",
                        "name": constraint_name,
                        "columns": columns,
                    }
                )
                continue

            if constraint_type == "FOREIGN KEY":
                parent_table_name = raw.get("parent_table")
                if not parent_table_name:
                    continue
                parent_columns = [
                    self._stable_id(
                        "col",
                        catalog_name,
                        schema_name,
                        str(parent_table_name),
                        str(col_name),
                    )
                    for col_name in raw.get("parent_columns", [])
                    if col_name
                ]
                constraints.append(
                    {
                        "id": constraint_id,
                        "type": "foreign_key",
                        "name": constraint_name,
                        "columns": columns,
                        "parentTable": self._stable_id(
                            "tbl",
                            catalog_name,
                            schema_name,
                            str(parent_table_name),
                        ),
                        "parentColumns": parent_columns,
                        "onUpdate": str(raw.get("on_update") or "NO_ACTION").upper(),
                        "onDelete": str(raw.get("on_delete") or "NO_ACTION").upper(),
                    }
                )
                continue

            if constraint_type == "CHECK":
                constraints.append(
                    {
                        "id": constraint_id,
                        "type": "check",
                        "name": constraint_name,
                        "columns": columns,
                        "expression": raw.get("expression"),
                    }
                )

        return constraints

    def _merge_check_constraints_from_properties(
        self,
        constraints_by_table: dict[str, list[dict[str, Any]]],
        properties_by_table: dict[str, dict[str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        merged: dict[str, list[dict[str, Any]]] = {
            table_name: list(constraints)
            for table_name, constraints in constraints_by_table.items()
        }
        prefix = "delta.constraints."

        for table_name, properties in properties_by_table.items():
            existing_names = {
                str(constraint.get("name") or "")
                for constraint in merged.get(table_name, [])
                if str(constraint.get("type") or "").upper() == "CHECK"
            }
            for key, value in properties.items():
                if not key.startswith(prefix):
                    continue
                constraint_name = key[len(prefix) :]
                if not constraint_name or constraint_name in existing_names:
                    continue
                merged.setdefault(table_name, []).append(
                    {
                        "name": constraint_name,
                        "type": "CHECK",
                        "columns": [],
                        "expression": value,
                    }
                )

        return merged

    def _fetch_table_details(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        cat = catalog_name.replace("`", "``")
        sch = schema_name.replace("`", "``")
        tbl = table_name.replace("`", "``")
        sql = f"DESCRIBE DETAIL `{cat}`.`{sch}`.`{tbl}`"
        rows = self._execute_query(client, warehouse_id, sql)
        if not rows:
            return {}
        row = rows[0]
        return {
            "format": row.get("format") or row.get("FORMAT"),
            "location": row.get("location") or row.get("LOCATION"),
            "partitionColumns": self._coerce_string_list(
                row.get("partitionColumns") or row.get("PARTITIONCOLUMNS")
            ),
            "clusterColumns": self._coerce_string_list(
                row.get("clusteringColumns") or row.get("CLUSTERINGCOLUMNS")
            ),
        }

    def _fetch_table_details_for_tables(
        self,
        config: ExecutionConfig,
        catalog_name: str,
        schema_name: str,
        table_names: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not table_names:
            return {}

        unique_names = sorted(set(table_names))
        if len(unique_names) == 1:
            client = self._get_import_thread_client(config.profile)
            table_name = unique_names[0]
            try:
                details = self._fetch_table_details(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            except Exception:
                details = {}
            return {table_name: details}

        configured = os.getenv("SCHEMAX_IMPORT_DETAILS_WORKERS", "").strip()
        max_workers = self.DEFAULT_IMPORT_PROPERTIES_WORKERS
        if configured:
            try:
                parsed = int(configured)
                if parsed > 0:
                    max_workers = parsed
            except ValueError:
                pass
        cpu_count = os.cpu_count() or self.DEFAULT_IMPORT_PROPERTIES_WORKERS
        max_workers = max(1, min(max_workers, cpu_count * 2, len(unique_names)))

        def _load(table_name: str) -> tuple[str, dict[str, Any]]:
            client = self._get_import_thread_client(config.profile)
            try:
                details = self._fetch_table_details(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            except Exception:
                details = {}
            return table_name, details

        details_by_table: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for table_name, details in executor.map(_load, unique_names):
                details_by_table[table_name] = details
        return details_by_table

    def _fetch_table_properties_for_tables(
        self,
        config: ExecutionConfig,
        catalog_name: str,
        schema_name: str,
        table_names: list[str],
    ) -> dict[str, dict[str, str]]:
        if not table_names:
            return {}

        unique_names = sorted(set(table_names))
        if len(unique_names) == 1:
            client = self._get_import_thread_client(config.profile)
            table_name = unique_names[0]
            return {
                table_name: self._fetch_table_properties(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            }

        configured = os.getenv("SCHEMAX_IMPORT_PROPERTIES_WORKERS", "").strip()
        max_workers = self.DEFAULT_IMPORT_PROPERTIES_WORKERS
        if configured:
            try:
                parsed = int(configured)
                if parsed > 0:
                    max_workers = parsed
            except ValueError:
                pass
        cpu_count = os.cpu_count() or self.DEFAULT_IMPORT_PROPERTIES_WORKERS
        max_workers = max(1, min(max_workers, cpu_count * 2, len(unique_names)))

        def _load(table_name: str) -> tuple[str, dict[str, str]]:
            client = self._get_import_thread_client(config.profile)
            try:
                props = self._fetch_table_properties_strict(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            except Exception:
                props = {}
            return table_name, props

        properties_by_table: dict[str, dict[str, str]] = {}
        failed_tables: list[str] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for table_name, properties in executor.map(_load, unique_names):
                properties_by_table[table_name] = properties
                if not properties:
                    failed_tables.append(table_name)

        if failed_tables:
            client = self._get_import_thread_client(config.profile)
            for table_name in failed_tables:
                # Retry sequentially through tolerant path; captures warning if it fails.
                retry_props = self._fetch_table_properties(
                    client,
                    config.warehouse_id,
                    catalog_name,
                    schema_name,
                    table_name,
                )
                if retry_props:
                    properties_by_table[table_name] = retry_props
        return properties_by_table

    def _get_import_thread_client(self, profile: str) -> Any:
        client = getattr(self._import_thread_local, "client", None)
        client_profile = getattr(self._import_thread_local, "profile", None)
        if client is not None and client_profile == profile:
            return client
        client = create_databricks_client(profile)
        self._import_thread_local.client = client
        self._import_thread_local.profile = profile
        return client

    def _normalize_catalog_tree(
        self,
        discovered_catalog: dict[str, Any],
        local_catalog: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_catalog = deepcopy(discovered_catalog)
        if local_catalog and local_catalog.get("id"):
            normalized_catalog["id"] = local_catalog["id"]

        local_schemas = (local_catalog or {}).get("schemas", []) or []
        local_schema_by_name = {
            str(schema.get("name")): schema
            for schema in local_schemas
            if isinstance(schema, dict) and schema.get("name")
        }

        normalized_schemas = []
        for schema in normalized_catalog.get("schemas", []) or []:
            if not isinstance(schema, dict):
                continue
            local_schema = local_schema_by_name.get(str(schema.get("name", "")))
            normalized_schema = deepcopy(schema)
            if local_schema and local_schema.get("id"):
                normalized_schema["id"] = local_schema["id"]
            self._normalize_schema_tree(normalized_schema, local_schema)
            normalized_schemas.append(normalized_schema)

        normalized_catalog["schemas"] = normalized_schemas
        return normalized_catalog

    def _normalize_schema_tree(
        self,
        normalized_schema: dict[str, Any],
        local_schema: dict[str, Any] | None,
    ) -> None:
        local_tables = (local_schema or {}).get("tables", []) or []
        local_table_by_name = {
            str(table.get("name")): table
            for table in local_tables
            if isinstance(table, dict) and table.get("name")
        }
        normalized_tables = []
        for table in normalized_schema.get("tables", []) or []:
            if not isinstance(table, dict):
                continue
            local_table = local_table_by_name.get(str(table.get("name", "")))
            normalized_table = deepcopy(table)
            if local_table and local_table.get("id"):
                normalized_table["id"] = local_table["id"]
            self._normalize_table_tree(normalized_table, local_table)
            normalized_tables.append(normalized_table)
        normalized_schema["tables"] = normalized_tables

        local_views = (local_schema or {}).get("views", []) or []
        local_view_by_name = {
            str(view.get("name")): view
            for view in local_views
            if isinstance(view, dict) and view.get("name")
        }
        normalized_views = []
        for view in normalized_schema.get("views", []) or []:
            if not isinstance(view, dict):
                continue
            local_view = local_view_by_name.get(str(view.get("name", "")))
            normalized_view = deepcopy(view)
            if local_view and local_view.get("id"):
                normalized_view["id"] = local_view["id"]
            normalized_views.append(normalized_view)
        normalized_schema["views"] = normalized_views

    def _normalize_table_tree(
        self,
        normalized_table: dict[str, Any],
        local_table: dict[str, Any] | None,
    ) -> None:
        local_columns = (local_table or {}).get("columns", []) or []
        local_column_by_name = {
            str(column.get("name")): column
            for column in local_columns
            if isinstance(column, dict) and column.get("name")
        }
        normalized_columns = []
        for column in normalized_table.get("columns", []) or []:
            if not isinstance(column, dict):
                continue
            local_column = local_column_by_name.get(str(column.get("name", "")))
            normalized_column = deepcopy(column)
            if local_column and local_column.get("id"):
                normalized_column["id"] = local_column["id"]
            normalized_columns.append(normalized_column)
        normalized_table["columns"] = normalized_columns

    def _read_catalog_mappings(self, env_config: dict[str, Any]) -> dict[str, str]:
        raw = env_config.get("catalogMappings") or {}
        if not isinstance(raw, dict):
            raise ValueError(
                "Environment catalogMappings must be an object mapping logical->physical"
            )
        return {str(k): str(v) for k, v in raw.items()}

    def _build_reverse_mappings(self, mappings: dict[str, str]) -> dict[str, str]:
        reverse: dict[str, str] = {}
        for logical, physical in mappings.items():
            existing = reverse.get(physical)
            if existing and existing != logical:
                raise ValueError(
                    "Invalid catalogMappings: physical catalog "
                    f"'{physical}' is mapped from multiple logical catalogs "
                    f"('{existing}', '{logical}')"
                )
            reverse[physical] = logical
        return reverse

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

    def _execute_query_optional(
        self,
        client: Any,
        warehouse_id: str,
        sql: str,
        warning: str,
    ) -> list[dict[str, Any]]:
        try:
            return self._execute_query(client, warehouse_id, sql)
        except Exception:
            self._last_import_warnings.append(warning)
            return []

    def _discover_volumes_for_schema(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
    ) -> list[dict[str, Any]]:
        """Discover volumes in a schema via information_schema.volumes."""
        cat_esc = catalog_name.replace("'", "''")
        sch_esc = schema_name.replace("'", "''")
        sql = (
            f"SELECT volume_name, volume_type, storage_location, comment "
            f"FROM system.information_schema.volumes "
            f"WHERE volume_catalog = '{cat_esc}' AND volume_schema = '{sch_esc}'"
        )
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                f"Could not discover volumes for {catalog_name}.{schema_name}; "
                "continuing without volumes."
            ),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            vol_name = str(row.get("volume_name") or row.get("VOLUME_NAME") or "")
            if not vol_name:
                continue
            vol_type = str(row.get("volume_type") or row.get("VOLUME_TYPE") or "MANAGED").upper()
            result.append(
                {
                    "id": self._stable_id("vol", catalog_name, schema_name, vol_name),
                    "name": vol_name,
                    "volumeType": "external" if vol_type == "EXTERNAL" else "managed",
                    "comment": row.get("comment") or row.get("COMMENT"),
                    "location": row.get("storage_location") or row.get("STORAGE_LOCATION"),
                    "grants": [],
                }
            )
        return result

    def _discover_functions_for_schema(
        self,
        client: Any,
        warehouse_id: str,
        catalog_name: str,
        schema_name: str,
    ) -> list[dict[str, Any]]:
        """Discover functions in a schema via information_schema.routines."""
        cat_esc = catalog_name.replace("'", "''")
        sch_esc = schema_name.replace("'", "''")
        sql = (
            f"SELECT routine_name, routine_body, data_type, routine_definition, comment "
            f"FROM system.information_schema.routines "
            f"WHERE routine_catalog = '{cat_esc}' AND routine_schema = '{sch_esc}' "
            f"AND UPPER(COALESCE(routine_type, 'FUNCTION')) = 'FUNCTION'"
        )
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql,
            warning=(
                f"Could not discover functions for {catalog_name}.{schema_name}; "
                "continuing without functions."
            ),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            name = str(row.get("routine_name") or row.get("ROUTINE_NAME") or "")
            if not name:
                continue
            body = str(row.get("routine_body") or row.get("ROUTINE_BODY") or "SQL").upper()
            result.append(
                {
                    "id": self._stable_id("func", catalog_name, schema_name, name),
                    "name": name,
                    "language": "PYTHON" if body == "PYTHON" else "SQL",
                    "returnType": row.get("data_type") or row.get("DATA_TYPE") or "STRING",
                    "body": row.get("routine_definition") or row.get("ROUTINE_DEFINITION") or "NULL",
                    "comment": row.get("comment") or row.get("COMMENT"),
                    "parameters": [],
                    "grants": [],
                }
            )
        return result

    def _coerce_string_list(self, value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(v) for v in value]
        if isinstance(value, tuple):
            return [str(v) for v in value]
        if not isinstance(value, str):
            return [str(value)]

        text = value.strip()
        if not text:
            return []

        for parser in (json.loads, literal_eval):
            try:
                parsed = parser(text)
                if isinstance(parsed, (list, tuple)):
                    return [str(v) for v in parsed]
            except Exception:
                continue

        return [text]

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
