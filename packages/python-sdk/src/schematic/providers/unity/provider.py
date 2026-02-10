"""
Unity Catalog Provider

Main provider implementation for Databricks Unity Catalog.
Implements the Provider interface to enable Unity Catalog support in Schematic.
"""

import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Any

from databricks.sdk.service.sql import StatementState

from schematic.providers.base.executor import ExecutionConfig, ExecutionResult, SQLExecutor
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
    SYSTEM_SCHEMA_NAME = "information_schema"
    SYSTEM_CATALOG_NAMES = frozenset({"system"})
    VIEW_TABLE_TYPES = frozenset({"VIEW"})
    UNSUPPORTED_DISCOVERY_TABLE_TYPES = frozenset(
        {
            "MATERIALIZED VIEW",
            "STREAMING TABLE",
            "MATERIALIZED_VIEW",
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
                tags_by_table = self._fetch_table_tags(
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
                properties_by_table = self._fetch_table_properties_for_tables(
                    config=config,
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    table_names=table_names,
                )

                tables = []
                views = []
                for row in table_rows:
                    table_name = str(row["table_name"])
                    table_kind = str(row.get("table_type", "BASE TABLE")).upper()

                    if table_kind in self.UNSUPPORTED_DISCOVERY_TABLE_TYPES:
                        self._last_import_warnings.append(
                            "Unsupported Unity object type discovered during import and skipped: "
                            f"{catalog_name}.{schema_name}.{table_name} ({table_kind})."
                        )
                        continue

                    if table_kind in self.VIEW_TABLE_TYPES:
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
                    column_id_by_name: dict[str, str] = {}
                    for col in column_rows:
                        col_name = str(col["column_name"])
                        col_type = str(col.get("data_type", "STRING"))
                        nullable = str(col.get("is_nullable", "YES")).upper() == "YES"
                        col_id = self._stable_id("col", catalog_name, schema_name, table_name, col_name)
                        columns.append(
                            {
                                "id": col_id,
                                "name": col_name,
                                "type": col_type,
                                "nullable": nullable,
                                "comment": col.get("comment"),
                                "tags": {},
                                "maskId": None,
                            }
                        )
                        column_id_by_name[col_name] = col_id

                    properties = properties_by_table.get(table_name, {})
                    constraints = self._build_table_constraints(
                        catalog_name,
                        schema_name,
                        table_name,
                        constraints_by_table.get(table_name, []),
                        column_id_by_name,
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
                            "properties": properties,
                            "tags": tags_by_table.get(table_name, {}),
                            "constraints": constraints,
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
                    raise ValueError(
                        "Catalog mapping conflict for logical catalog "
                        f"'{logical_name}': '{existing_physical}' vs '{physical_name}'"
                    )

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
        from schematic.core.deployment import DeploymentTracker

        deployment_catalog = env_config["topLevelName"]
        auto_create_schema = env_config.get("autoCreateSchematicSchema", True)

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
            schematic_version="0.2.0",
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
        sql_primary = (
            "SELECT table_name, tag_name, tag_value "
            "FROM system.information_schema.table_tags "
            f"WHERE catalog_name = '{cat}' "
            f"AND schema_name = '{sch}'"
        )
        sql_fallback = (
            "SELECT table_name, tag_name, tag_value "
            "FROM system.information_schema.table_tags "
            f"WHERE table_catalog = '{cat}' "
            f"AND table_schema = '{sch}'"
        )
        if table_filter:
            tbl = table_filter.replace("'", "''")
            sql_primary += f" AND table_name = '{tbl}'"
            sql_fallback += f" AND table_name = '{tbl}'"
        sql_primary += " ORDER BY table_name, tag_name"
        sql_fallback += " ORDER BY table_name, tag_name"
        rows = self._execute_query_optional(
            client,
            warehouse_id,
            sql_primary,
            warning=(
                "Could not discover Unity table tags from information_schema.table_tags; "
                "retrying with alternate column names."
            ),
        )
        if not rows:
            rows = self._execute_query_optional(
                client,
                warehouse_id,
                sql_fallback,
                warning=(
                    "Could not discover Unity table tags from information_schema.table_tags; "
                    "continuing without table tags."
                ),
            )

        grouped: dict[str, dict[str, str]] = {}
        for row in rows:
            table_name = str(
                row.get("table_name")
                or row.get("TABLE_NAME")
                or ""
            )
            tag_name = str(
                row.get("tag_name")
                or row.get("TAG_NAME")
                or ""
            )
            if not table_name or not tag_name:
                continue
            tag_value = row.get("tag_value")
            if tag_value is None:
                tag_value = row.get("TAG_VALUE")
            grouped.setdefault(table_name, {})[tag_name] = "" if tag_value is None else str(tag_value)
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
            if not table_name or not constraint_name or constraint_type not in ("PRIMARY KEY", "FOREIGN KEY"):
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
            if not columns:
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

        return constraints

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

        configured = os.getenv("SCHEMATIC_IMPORT_PROPERTIES_WORKERS", "").strip()
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
            except RuntimeError:
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
        except RuntimeError:
            self._last_import_warnings.append(warning)
            return []

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
