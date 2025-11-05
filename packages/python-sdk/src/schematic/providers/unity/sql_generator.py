"""
Unity Catalog SQL Generator

Generates Databricks SQL DDL statements from operations.
Migrated from TypeScript sql-generator.ts
"""

from typing import Any, TypedDict

from ..base.batching import BatchInfo
from ..base.operations import Operation
from ..base.sql_generator import BaseSQLGenerator, SQLGenerationResult
from .models import UnityState
from .operations import UNITY_OPERATIONS


class LocationResolution(TypedDict):
    """Location resolution details"""

    resolved: str
    location_name: str
    relative_path: str | None


class LocationDefinition(TypedDict):
    """Project-level location definition with per-environment paths"""

    description: str | None
    paths: dict[str, str]  # environment_name -> physical_path


class UnitySQLGenerator(BaseSQLGenerator):
    """Unity Catalog SQL Generator"""

    def __init__(
        self,
        state: UnityState,
        catalog_name_mapping: dict[str, str] | None = None,
        managed_locations: dict[str, LocationDefinition] | None = None,
        external_locations: dict[str, LocationDefinition] | None = None,
        environment_name: str | None = None,
    ):
        # Pass catalog_name_mapping to base as name_mapping
        super().__init__(state, catalog_name_mapping)
        self.catalog_name_mapping = catalog_name_mapping or {}  # logical → physical
        self.managed_locations = managed_locations  # project-level
        self.external_locations = external_locations  # project-level
        self.environment_name = environment_name  # for path resolution
        # Base now provides self.batcher and self.optimizer
        self.id_name_map = self._build_id_name_map()

    def _build_id_name_map(self) -> dict[str, str]:
        """
        Build a mapping from IDs to fully-qualified names.

        Uses catalog_name_mapping to replace logical catalog names with physical names
        when generating environment-specific SQL.
        """
        id_map = {}

        # Handle both dict and Pydantic model state
        catalogs = (
            self.state.catalogs
            if hasattr(self.state, "catalogs")
            else self.state.get("catalogs", [])
        )

        for catalog in catalogs:
            # Handle both dict and Pydantic model
            cat_name = catalog.name if hasattr(catalog, "name") else catalog["name"]
            cat_id = catalog.id if hasattr(catalog, "id") else catalog["id"]
            # Apply catalog name mapping if present (for environment-specific SQL)
            catalog_name = self.catalog_name_mapping.get(cat_name, cat_name)
            id_map[cat_id] = catalog_name

            schemas = catalog.schemas if hasattr(catalog, "schemas") else catalog.get("schemas", [])
            for schema in schemas:
                schema_name = schema.name if hasattr(schema, "name") else schema["name"]
                schema_id = schema.id if hasattr(schema, "id") else schema["id"]
                id_map[schema_id] = f"{catalog_name}.{schema_name}"

                tables = schema.tables if hasattr(schema, "tables") else schema.get("tables", [])
                for table in tables:
                    table_name = table.name if hasattr(table, "name") else table["name"]
                    table_id = table.id if hasattr(table, "id") else table["id"]
                    table_fqn = f"{catalog_name}.{schema_name}.{table_name}"
                    id_map[table_id] = table_fqn

                    columns = (
                        table.columns if hasattr(table, "columns") else table.get("columns", [])
                    )
                    for column in columns:
                        col_name = column.name if hasattr(column, "name") else column["name"]
                        col_id = column.id if hasattr(column, "id") else column["id"]
                        id_map[col_id] = col_name

        return id_map

    def _resolve_table_location(
        self, external_location_name: str | None, path: str | None
    ) -> LocationResolution | None:
        """
        Resolve external location name and path to absolute path.

        Args:
            external_location_name: Logical location name from project config
            path: Relative path under the external location (optional)

        Returns:
            Location resolution details or None if not an external table

        Raises:
            ValueError: If external location is not found or config is missing
        """
        if not external_location_name:
            return None  # Not an external table

        if not self.external_locations:
            raise ValueError(
                f"External location '{external_location_name}' requires project-level "
                "externalLocations configuration."
            )

        loc_def = self.external_locations.get(external_location_name)
        if not loc_def:
            available = ", ".join(self.external_locations.keys()) or "(none)"
            raise ValueError(
                f"External location '{external_location_name}' not found in project. "
                f"Available: {available}"
            )

        if not self.environment_name:
            raise ValueError(
                f"Cannot resolve external location '{external_location_name}': "
                "environment name not provided."
            )

        env_path = loc_def["paths"].get(self.environment_name)
        if not env_path:
            available_envs = ", ".join(loc_def["paths"].keys()) or "(none)"
            raise ValueError(
                f"External location '{external_location_name}' does not have a path "
                f"configured for environment '{self.environment_name}'. "
                f"Configured environments: {available_envs}"
            )

        base_path = env_path.rstrip("/")  # Remove trailing slash

        if path:
            rel_path = path.lstrip("/")  # Remove leading slash
            return {
                "resolved": f"{base_path}/{rel_path}",
                "location_name": external_location_name,
                "relative_path": path,
            }

        return {
            "resolved": base_path,
            "location_name": external_location_name,
            "relative_path": None,
        }

    def _resolve_managed_location(self, location_name: str | None) -> LocationResolution | None:
        """
        Resolve managed location name to absolute path.

        Args:
            location_name: Logical managed location name from project config

        Returns:
            Location resolution details or None if no managed location

        Raises:
            ValueError: If managed location is not found or config is missing
        """
        if not location_name:
            return None

        if not self.managed_locations:
            raise ValueError(
                f"Managed location '{location_name}' requires project-level "
                "managedLocations configuration."
            )

        loc_def = self.managed_locations.get(location_name)
        if not loc_def:
            available = ", ".join(self.managed_locations.keys()) or "(none)"
            raise ValueError(
                f"Managed location '{location_name}' not found in project. Available: {available}"
            )

        if not self.environment_name:
            raise ValueError(
                f"Cannot resolve managed location '{location_name}': environment name not provided."
            )

        env_path = loc_def["paths"].get(self.environment_name)
        if not env_path:
            available_envs = ", ".join(loc_def["paths"].keys()) or "(none)"
            raise ValueError(
                f"Managed location '{location_name}' does not have a path "
                f"configured for environment '{self.environment_name}'. "
                f"Configured environments: {available_envs}"
            )

        return {
            "resolved": env_path.rstrip("/"),
            "location_name": location_name,
            "relative_path": None,
        }

    # _build_fqn() is now inherited from BaseSQLGenerator

    def can_generate_sql(self, op: Operation) -> bool:
        """Check if operation can be converted to SQL"""
        return op.op in UNITY_OPERATIONS.values()

    # ========================================
    # ABSTRACT METHOD IMPLEMENTATIONS (from BaseSQLGenerator)
    # ========================================

    def _get_dependency_level(self, op: Operation) -> int:
        """
        Get dependency level for Unity operation ordering.
        0 = catalog, 1 = schema, 2 = table, 3 = table modifications

        Implements abstract method from BaseSQLGenerator.
        """
        op_type = op.op
        if "catalog" in op_type:
            return 0
        if "schema" in op_type:
            return 1
        if "add_table" in op_type:
            return 2
        return 3  # All other table operations (columns, properties, etc.)

    def _get_target_object_id(self, op: Operation) -> str | None:
        """
        Extract target object ID from Unity operation.

        Implements abstract method from BaseSQLGenerator.
        Used by batching algorithm to group operations by catalog/schema/table.
        """
        op_type = op.op.replace("unity.", "")

        # Catalog-level operations
        if op_type in ["add_catalog", "rename_catalog", "update_catalog", "drop_catalog"]:
            return f"catalog:{op.target}"  # Prefix to avoid ID collisions

        # Schema-level operations
        if op_type in ["add_schema", "rename_schema", "update_schema", "drop_schema"]:
            return f"schema:{op.target}"  # Prefix to avoid ID collisions

        # Table-level operations
        if op_type in ["add_table", "rename_table", "drop_table", "set_table_comment"]:
            return f"table:{op.target}"  # Add prefix for consistency

        # Column and table property operations
        if op_type in [
            "add_column",
            "rename_column",
            "drop_column",
            "reorder_columns",
            "change_column_type",
            "set_nullable",
            "set_column_comment",
            "set_column_tag",
            "unset_column_tag",
            "set_table_property",
            "unset_table_property",
            "set_table_tag",
            "unset_table_tag",
            "add_constraint",
            "drop_constraint",
            "add_row_filter",
            "update_row_filter",
            "remove_row_filter",
            "add_column_mask",
            "update_column_mask",
            "remove_column_mask",
        ]:
            table_id = op.payload.get("tableId")
            return f"table:{table_id}" if table_id else None

        return None  # Global operations with no specific target

    def _is_create_operation(self, op: Operation) -> bool:
        """
        Check if Unity operation creates a new object.

        Implements abstract method from BaseSQLGenerator.
        """
        return op.op in ["unity.add_catalog", "unity.add_schema", "unity.add_table"]

    def _generate_batched_create_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE statement with batched modifications for Unity Catalog objects.

        Implements abstract method from BaseSQLGenerator.
        This is the optimization that creates complete objects instead of CREATE + ALTERs.

        Handles:
        - Catalogs: CREATE CATALOG with MANAGED LOCATION
        - Schemas: CREATE SCHEMA with MANAGED LOCATION
        - Tables: CREATE TABLE with columns, properties, constraints
        """
        # Determine object type from ID prefix
        if object_id.startswith("catalog:"):
            return self._generate_create_catalog_batched(object_id, batch_info)
        elif object_id.startswith("schema:"):
            return self._generate_create_schema_batched(object_id, batch_info)
        elif object_id.startswith("table:"):
            # Delegate to existing _generate_create_table_with_columns method
            batch_dict = {
                "is_new_table": batch_info.is_new,
                "table_op": batch_info.create_op,
                "column_ops": [op for op in batch_info.modify_ops if op.op == "unity.add_column"],
                "property_ops": [op for op in batch_info.modify_ops if "property" in op.op],
                "constraint_ops": [op for op in batch_info.modify_ops if "constraint" in op.op],
                "op_ids": batch_info.op_ids,
                "operation_types": list(batch_info.operation_types),
            }
            return self._generate_create_table_with_columns(object_id, batch_dict)
        else:
            # Fallback for objects without prefix (shouldn't happen after refactor)
            return ""

    def _generate_batched_alter_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate ALTER statements for Unity table modifications.

        Implements abstract method from BaseSQLGenerator.
        """
        # Delegate to existing _generate_alter_statements_for_table method
        batch_dict = {
            "column_ops": [op for op in batch_info.modify_ops if "column" in op.op],
            "property_ops": [op for op in batch_info.modify_ops if "property" in op.op],
            "constraint_ops": [op for op in batch_info.modify_ops if "constraint" in op.op],
            "reorder_ops": [op for op in batch_info.modify_ops if "reorder" in op.op],
            "governance_ops": [
                op for op in batch_info.modify_ops if "filter" in op.op or "mask" in op.op
            ],
            "other_ops": [],
            "op_ids": batch_info.op_ids,
            "operation_types": list(batch_info.operation_types),
        }
        return self._generate_alter_statements_for_table(object_id, batch_dict)

    # ========================================
    # END ABSTRACT METHOD IMPLEMENTATIONS
    # ========================================

    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL statements with comprehensive batch optimizations.

        Optimizations include:
        - Dependency-ordered operations (catalog → schema → table)
        - Batched CREATE + UPDATE operations (squashed into single CREATE)
        - Complete CREATE TABLE statements (no empty tables + ALTERs)
        - Batched column reordering (minimal ALTER statements)
        - Table property consolidation
        """
        # Sort operations by dependency level first
        sorted_ops = sorted(ops, key=lambda op: self._get_dependency_level(op))

        # Use base class's generic batcher (no duplication!)
        batches = self.batcher.batch_operations(
            sorted_ops, self._get_target_object_id, self._is_create_operation
        )
        processed_op_ids = set()
        statements = []

        # Separate batches by object type for proper ordering
        catalog_stmts = []
        schema_stmts = []
        table_stmts = []
        other_ops = []

        # Process batched operations
        for object_id, batch_info in batches.items():
            op_ids = batch_info.op_ids
            processed_op_ids.update(op_ids)

            # Determine object type from ID prefix (catalog:, schema:, table:)
            if object_id.startswith("catalog:"):
                object_type = "catalog"
            elif object_id.startswith("schema:"):
                object_type = "schema"
            elif object_id.startswith("table:"):
                object_type = "table"
            else:
                # Skip unknown object types
                continue

            sql = ""
            operation_types = set(op.replace("unity.", "") for op in batch_info.operation_types)
            header = (
                f"-- Batch {object_type.capitalize()} Operations: {len(op_ids)} operations\n"
                f"-- Object: {object_id}\n"
                f"-- Types: {', '.join(sorted(operation_types))}\n"
                f"-- Operations: {', '.join(op_ids)}"
            )

            if object_type == "catalog":
                sql = self._generate_create_catalog_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    catalog_stmts.append(f"{header}\n{sql};")
            elif object_type == "schema":
                sql = self._generate_create_schema_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    schema_stmts.append(f"{header}\n{sql};")
            elif object_type == "table":
                sql = self._generate_optimized_table_sql(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    table_stmts.append(f"{header}\n{sql};")

        # Process unbatched operations
        for op in sorted_ops:
            if op.id in processed_op_ids:
                continue

            if not self.can_generate_sql(op):
                print(f"Warning: Cannot generate SQL for operation: {op.op}")
                continue

            other_ops.append(op)

        # Generate SQL in dependency order: catalogs → schemas → tables → others
        statements.extend(catalog_stmts)
        statements.extend(schema_stmts)
        statements.extend(table_stmts)

        for op in other_ops:
            result = self.generate_sql_for_operation(op)
            header = f"-- Operation: {op.id} ({op.ts})\n-- Type: {op.op}"
            warnings_comment = ""
            if result.warnings:
                warnings_comment = f"\n-- Warnings: {', '.join(result.warnings)}"
            statements.append(f"{header}{warnings_comment}\n{result.sql};")

        return "\n\n".join(statements)

    def generate_sql_for_operation(self, op: Operation) -> SQLGenerationResult:
        """Generate SQL for a single operation"""
        # Strip provider prefix
        op_type = op.op.replace("unity.", "")

        try:
            sql = self._generate_sql_for_op_type(op_type, op)
            return SQLGenerationResult(sql=sql, warnings=[], is_idempotent=True)
        except Exception as e:
            return SQLGenerationResult(
                sql=f"-- Error generating SQL: {e}",
                warnings=[str(e)],
                is_idempotent=False,
            )

    def _generate_sql_for_op_type(self, op_type: str, op: Operation) -> str:
        """Generate SQL based on operation type"""
        # Catalog operations
        if op_type == "add_catalog":
            return self._add_catalog(op)
        elif op_type == "rename_catalog":
            return self._rename_catalog(op)
        elif op_type == "update_catalog":
            return self._update_catalog(op)
        elif op_type == "drop_catalog":
            return self._drop_catalog(op)

        # Schema operations
        elif op_type == "add_schema":
            return self._add_schema(op)
        elif op_type == "rename_schema":
            return self._rename_schema(op)
        elif op_type == "update_schema":
            return self._update_schema(op)
        elif op_type == "drop_schema":
            return self._drop_schema(op)

        # Table operations
        elif op_type == "add_table":
            return self._add_table(op)
        elif op_type == "rename_table":
            return self._rename_table(op)
        elif op_type == "drop_table":
            return self._drop_table(op)
        elif op_type == "set_table_comment":
            return self._set_table_comment(op)
        elif op_type == "set_table_property":
            return self._set_table_property(op)
        elif op_type == "unset_table_property":
            return self._unset_table_property(op)
        elif op_type == "set_table_tag":
            return self._set_table_tag(op)
        elif op_type == "unset_table_tag":
            return self._unset_table_tag(op)

        # Column operations
        elif op_type == "add_column":
            return self._add_column(op)
        elif op_type == "rename_column":
            return self._rename_column(op)
        elif op_type == "drop_column":
            return self._drop_column(op)
        elif op_type == "reorder_columns":
            return self._reorder_columns(op)
        elif op_type == "change_column_type":
            return self._change_column_type(op)
        elif op_type == "set_nullable":
            return self._set_nullable(op)
        elif op_type == "set_column_comment":
            return self._set_column_comment(op)

        # Column tag operations
        elif op_type == "set_column_tag":
            return self._set_column_tag(op)
        elif op_type == "unset_column_tag":
            return self._unset_column_tag(op)

        # Constraint operations
        elif op_type == "add_constraint":
            return self._add_constraint(op)
        elif op_type == "drop_constraint":
            return self._drop_constraint(op)

        # Row filter operations
        elif op_type == "add_row_filter":
            return self._add_row_filter(op)
        elif op_type == "update_row_filter":
            return self._update_row_filter(op)
        elif op_type == "remove_row_filter":
            return self._remove_row_filter(op)

        # Column mask operations
        elif op_type == "add_column_mask":
            return self._add_column_mask(op)
        elif op_type == "update_column_mask":
            return self._update_column_mask(op)
        elif op_type == "remove_column_mask":
            return self._remove_column_mask(op)

        raise ValueError(f"Unsupported operation type: {op_type}")

    # Catalog operations
    def _add_catalog(self, op: Operation) -> str:
        # Use mapped name from id_name_map (handles __implicit__ → physical catalog)
        name = self.id_name_map.get(op.target, op.payload["name"])

        # Fallback: If the catalog doesn't exist in id_name_map yet (e.g., from diff operations),
        # apply catalog_name_mapping to convert logical → physical name
        if op.target not in self.id_name_map and op.payload["name"] in self.catalog_name_mapping:
            name = self.catalog_name_mapping[op.payload["name"]]

        # Build CREATE CATALOG statement
        sql = f"CREATE CATALOG IF NOT EXISTS {self.escape_identifier(name)}"

        # Add managed location if specified
        managed_location_name = op.payload.get("managedLocationName")
        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        return sql

    def _rename_catalog(self, op: Operation) -> str:
        old_name = op.payload["oldName"]
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER CATALOG {old_esc} RENAME TO {new_esc}"

    def _update_catalog(self, op: Operation) -> str:
        """Update catalog properties (e.g., managed location)"""
        name = self.id_name_map.get(op.target, op.target)
        managed_location_name = op.payload.get("managedLocationName")

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                return (
                    f"ALTER CATALOG {self.escape_identifier(name)} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )
            else:
                return f"-- Warning: Managed location '{managed_location_name}' not configured"
        else:
            return "-- Warning: No managed location specified for catalog update"

    def _drop_catalog(self, op: Operation) -> str:
        name = self.id_name_map.get(op.target, op.target)
        return f"DROP CATALOG IF EXISTS {self.escape_identifier(name)}"

    # Schema operations
    def _add_schema(self, op: Operation) -> str:
        catalog_name = self.id_name_map.get(op.payload["catalogId"], "unknown")
        schema_name = op.payload["name"]
        managed_location_name = op.payload.get("managedLocationName")

        catalog_esc = self.escape_identifier(catalog_name)
        schema_esc = self.escape_identifier(schema_name)
        sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_esc}.{schema_esc}"

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        return sql

    def _generate_create_catalog_batched(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE CATALOG with batched updates (e.g., managed location from update_catalog).

        Squashes CREATE CATALOG + UPDATE_CATALOG into single CREATE statement.
        """
        if not batch_info.create_op:
            return ""

        create_op = batch_info.create_op
        name = self.id_name_map.get(create_op.target, create_op.payload["name"])

        # Check for update_catalog operations in modify_ops
        managed_location_name = create_op.payload.get("managedLocationName")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_catalog" and "managedLocationName" in op.payload:
                # Squash: Use the updated value instead
                managed_location_name = op.payload.get("managedLocationName")
                break

        sql = f"CREATE CATALOG IF NOT EXISTS {self.escape_identifier(name)}"

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        return sql

    def _generate_create_schema_batched(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE SCHEMA with batched updates (e.g., managed location from update_schema).

        Squashes CREATE SCHEMA + UPDATE_SCHEMA into single CREATE statement.
        """
        if not batch_info.create_op:
            return ""

        create_op = batch_info.create_op
        catalog_name = self.id_name_map.get(create_op.payload["catalogId"], "unknown")
        schema_name = create_op.payload["name"]

        # Check for update_schema operations in modify_ops
        managed_location_name = create_op.payload.get("managedLocationName")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_schema" and "managedLocationName" in op.payload:
                # Squash: Use the updated value instead
                managed_location_name = op.payload.get("managedLocationName")
                break

        catalog_esc = self.escape_identifier(catalog_name)
        schema_esc = self.escape_identifier(schema_name)
        sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_esc}.{schema_esc}"

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        return sql

    def _rename_schema(self, op: Operation) -> str:
        old_name = op.payload["oldName"]
        new_name = op.payload["newName"]
        # Get catalog name from idNameMap (catalog doesn't change during schema rename)
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
        catalog_name = fqn.split(".")[0]

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, old_name)
        new_esc = self._build_fqn(catalog_name, new_name)

        return f"ALTER SCHEMA {old_esc} RENAME TO {new_esc}"

    def _update_schema(self, op: Operation) -> str:
        """Update schema properties (e.g., managed location)"""
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"

        managed_location_name = op.payload.get("managedLocationName")

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                fqn_esc = self._build_fqn(catalog_name, schema_name)
                return (
                    f"ALTER SCHEMA {fqn_esc} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )
            else:
                return f"-- Warning: Managed location '{managed_location_name}' not configured"
        else:
            return "-- Warning: No managed location specified for schema update"

    def _drop_schema(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"

        # Use _build_fqn for consistent formatting
        fqn_esc = self._build_fqn(catalog_name, schema_name)

        return f"DROP SCHEMA IF EXISTS {fqn_esc}"

    # Table operations
    def _add_table(self, op: Operation) -> str:
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        table_name = op.payload["name"]
        table_format = op.payload["format"].upper()
        is_external = op.payload.get("external", False)

        # Resolve location
        location_info = (
            self._resolve_table_location(
                op.payload.get("externalLocationName"), op.payload.get("path")
            )
            if is_external
            else None
        )

        partition_cols = op.payload.get("partitionColumns", [])
        cluster_cols = op.payload.get("clusterColumns", [])

        # Build SQL clauses
        external_keyword = "EXTERNAL " if is_external else ""
        location_clause = (
            f" LOCATION '{self.escape_string(location_info['resolved'])}'" if location_info else ""
        )
        partition_clause = (
            f" PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
        )
        cluster_clause = f" CLUSTER BY ({', '.join(cluster_cols)})" if cluster_cols else ""

        # Add warnings and metadata as SQL comments
        warnings = ""
        if is_external and location_info:
            warnings = (
                f"-- External Table: {table_name}\n"
                f"-- Location Name: {location_info['location_name']}\n"
            )

            if location_info["relative_path"]:
                warnings += f"-- Relative Path: {location_info['relative_path']}\n"

            warnings += (
                f"-- Resolved Location: {location_info['resolved']}\n"
                "-- WARNING: External tables must reference pre-configured external locations\n"
                "-- WARNING: Databricks recommends using managed tables for optimal performance\n"
                "-- Learn more: https://learn.microsoft.com/en-gb/azure/databricks/tables/managed\n"
            )

        # Use _build_fqn for consistent formatting
        fqn_esc = self._build_fqn(catalog_name, schema_name, table_name)

        # Create empty table (columns added via add_column ops)
        return (
            f"{warnings}"
            f"CREATE {external_keyword}TABLE IF NOT EXISTS {fqn_esc} () "
            f"USING {table_format}{partition_clause}{cluster_clause}{location_clause}"
        )

    def _rename_table(self, op: Operation) -> str:
        old_name = op.payload["oldName"]
        new_name = op.payload["newName"]
        # Get catalog and schema names from idNameMap (they don't change during table rename)
        fqn = self.id_name_map.get(op.target, "unknown.unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, schema_name, old_name)
        new_esc = self._build_fqn(catalog_name, schema_name, new_name)

        return f"ALTER TABLE {old_esc} RENAME TO {new_esc}"

    def _drop_table(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.target, "unknown.unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        table_name = parts[2] if len(parts) > 2 else "unknown"

        # Use _build_fqn for consistent formatting
        fqn_esc = self._build_fqn(catalog_name, schema_name, table_name)

        return f"DROP TABLE IF EXISTS {fqn_esc}"

    def _set_table_comment(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        comment = self.escape_string(op.payload["comment"])
        return f"COMMENT ON TABLE {fqn_esc} IS '{comment}'"

    def _set_table_property(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        key = op.payload["key"]
        value = op.payload["value"]
        return f"ALTER TABLE {fqn_esc} SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_table_property(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        key = op.payload["key"]
        return f"ALTER TABLE {fqn_esc} UNSET TBLPROPERTIES ('{key}')"

    def _set_table_tag(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        tag_name = op.payload["tagName"]
        tag_value = self.escape_string(op.payload["tagValue"])
        return f"ALTER TABLE {fqn_esc} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_table_tag(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        tag_name = op.payload["tagName"]
        return f"ALTER TABLE {fqn_esc} UNSET TAGS ('{tag_name}')"

    # Column operations
    def _add_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = op.payload["name"]
        col_type = op.payload["type"]
        comment = op.payload.get("comment", "")

        # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMN for Delta tables
        # New columns added to existing tables must be nullable
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
        col_esc = self.escape_identifier(col_name)

        sql = f"ALTER TABLE {table_esc} ADD COLUMN {col_esc} {col_type}"
        return f"{sql}{comment_clause}"

    def _rename_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        old_name = op.payload["oldName"]
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER TABLE {table_esc} RENAME COLUMN {old_esc} TO {new_esc}"

    def _drop_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (for dropped columns not in current state)
        col_name = op.payload.get("name", self.id_name_map.get(op.target, "unknown"))
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} DROP COLUMN {col_esc}"

    def _reorder_columns(self, op: Operation) -> str:
        # Databricks doesn't support direct column reordering
        return "-- Column reordering not directly supported in Databricks SQL"

    def _batch_reorder_operations(self, ops: list[Operation]) -> dict[str, Any]:
        """
        Batch reorder_columns operations by table to generate minimal SQL.

        Returns dict mapping table_id to:
        {
            "original_order": [...],  # Column order before any reorder ops
            "final_order": [...],     # Column order after all reorder ops
            "op_ids": [...]          # List of operation IDs involved
        }
        """
        reorder_batches: dict[str, Any] = {}

        for op in ops:
            op_type = op.op.replace("unity.", "")

            if op_type == "reorder_columns":
                table_id = op.payload["tableId"]
                desired_order = op.payload["order"]

                if table_id not in reorder_batches:
                    # Find original column order from current state
                    original_order = self._get_table_column_order(table_id)
                    reorder_batches[table_id] = {
                        "original_order": original_order,
                        "final_order": original_order.copy(),  # Will be updated
                        "op_ids": [],
                    }

                # Update final order and track operation
                reorder_batches[table_id]["final_order"] = desired_order
                reorder_batches[table_id]["op_ids"].append(op.id)

        return reorder_batches

    def _get_table_column_order(self, table_id: str) -> list[str]:
        """Get current column order for a table from state"""
        for catalog in self.state["catalogs"]:
            for schema in catalog.get("schemas", []):
                for table in schema.get("tables", []):
                    if table["id"] == table_id:
                        return [col["id"] for col in table.get("columns", [])]
        return []

    def _generate_optimized_reorder_sql(
        self, table_id: str, original_order: list[str], final_order: list[str], op_ids: list[str]
    ) -> str:
        """Generate minimal SQL to reorder columns from original to final order"""

        if not original_order or not final_order:
            return "-- No columns to reorder"

        if original_order == final_order:
            return "-- Column order unchanged"

        # Get table name for ALTER statements
        table_fqn = self.id_name_map.get(table_id, "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))

        # OPTIMIZATION: Use base optimizer to detect single-column drag
        # Novel algorithm from base.optimization.ColumnReorderOptimizer
        single_move = self.optimizer.detect_single_column_move(original_order, final_order)

        # If we detected a single column drag, generate optimal SQL (1 statement)
        if single_move:
            col_id, orig_pos, new_pos = single_move
            col_name = self.id_name_map.get(col_id, col_id)
            col_esc = self.escape_identifier(col_name)

            if new_pos == 0:
                # Column moved to first position
                return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} FIRST"
            else:
                # Column moved after another column
                prev_col_id = final_order[new_pos - 1]
                prev_col_name = self.id_name_map.get(prev_col_id, prev_col_id)
                prev_col_esc = self.escape_identifier(prev_col_name)
                return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} AFTER {prev_col_esc}"

        # Multiple columns moved - use general algorithm
        statements = []
        current_order = original_order.copy()

        # Process columns in reverse order to handle dependencies correctly
        for i in range(len(final_order) - 1, -1, -1):
            col_id = final_order[i]
            current_pos = current_order.index(col_id) if col_id in current_order else -1

            if current_pos == -1:
                continue  # Column not found

            # If column is already in correct position, skip
            if current_pos == i:
                continue

            col_name = self.id_name_map.get(col_id, col_id)
            col_esc = self.escape_identifier(col_name)

            if i == 0:
                # Move to first position
                statements.append(f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} FIRST")
            else:
                # Move after the previous column
                prev_col_id = final_order[i - 1]
                prev_col_name = self.id_name_map.get(prev_col_id, prev_col_id)
                prev_col_esc = self.escape_identifier(prev_col_name)
                statements.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} AFTER {prev_col_esc}"
                )

            # Update current_order to reflect the change for next iteration
            current_order.pop(current_pos)
            current_order.insert(i, col_id)

        if not statements:
            return "-- No column reordering needed"

        return ";\n".join(statements)

    def _batch_table_operations(self, ops: list[Operation]) -> dict[str, Any]:
        """
        Batch operations by table to generate optimal DDL.

        Groups table-related operations (add_table, add_column, reorder_columns,
        set_table_property, etc.) to generate complete CREATE TABLE statements
        for new tables or efficient ALTER statements for existing tables.

        Returns dict mapping table_id to batch info.
        """
        table_batches: dict[str, Any] = {}

        for op in ops:
            op_type = op.op.replace("unity.", "")

            # Identify table-related operations
            table_id = None
            if op_type in ["add_table", "rename_table", "drop_table", "set_table_comment"]:
                table_id = op.target
            elif op_type in [
                "add_column",
                "rename_column",
                "drop_column",
                "reorder_columns",
                "change_column_type",
                "set_nullable",
                "set_column_comment",
                "set_column_tag",
                "unset_column_tag",
                "set_table_property",
                "unset_table_property",
                "set_table_tag",
                "unset_table_tag",
            ]:
                table_id = op.payload.get("tableId")
            elif op_type in [
                "add_constraint",
                "drop_constraint",
                "add_row_filter",
                "update_row_filter",
                "remove_row_filter",
                "add_column_mask",
                "update_column_mask",
                "remove_column_mask",
            ]:
                table_id = op.payload.get("tableId")

            if not table_id:
                continue  # Not a table operation

            if table_id not in table_batches:
                table_batches[table_id] = {
                    "is_new_table": False,
                    "table_op": None,
                    "column_ops": [],
                    "property_ops": [],
                    "reorder_ops": [],
                    "constraint_ops": [],
                    "governance_ops": [],
                    "other_ops": [],
                    "op_ids": [],
                    "operation_types": [],
                }

            batch = table_batches[table_id]
            batch["op_ids"].append(op.id)
            batch["operation_types"].append(op.op)

            # Categorize operation
            if op_type == "add_table":
                batch["is_new_table"] = True
                batch["table_op"] = op
            elif op_type == "add_column":
                batch["column_ops"].append(op)
            elif op_type in ["set_table_property", "unset_table_property"]:
                batch["property_ops"].append(op)
            elif op_type in ["set_table_tag", "unset_table_tag"]:
                # Table tags must be set AFTER table creation
                batch["other_ops"].append(op)
            elif op_type == "set_table_comment":
                # Table comments can be included in CREATE TABLE
                batch["other_ops"].append(op)
            elif op_type == "reorder_columns":
                batch["reorder_ops"].append(op)
            elif op_type in ["add_constraint", "drop_constraint"]:
                batch["constraint_ops"].append(op)
            elif op_type in [
                "add_row_filter",
                "update_row_filter",
                "remove_row_filter",
                "add_column_mask",
                "update_column_mask",
                "remove_column_mask",
            ]:
                batch["governance_ops"].append(op)
            else:
                batch["other_ops"].append(op)

        return table_batches

    def _generate_optimized_table_sql(
        self, table_id: str, batch_info: dict[str, Any] | BatchInfo
    ) -> str:
        """Generate optimal SQL for table operations"""

        # Handle both dict (old table batching) and BatchInfo (new unified batching)
        if isinstance(batch_info, BatchInfo):
            # Convert BatchInfo to dict format expected by table methods
            # Categorize all modify operations
            column_ops = []
            other_ops = []
            for op in batch_info.modify_ops:
                op_type = op.op.replace("unity.", "")
                if op_type in [
                    "add_column",
                    "rename_column",
                    "drop_column",
                    "change_column_type",
                    "set_nullable",
                    "set_column_comment",
                    "set_column_tag",
                    "unset_column_tag",
                ]:
                    column_ops.append(op)
                # Exclude operations that will be handled by dedicated lists below
                # (property_ops, constraint_ops, reorder_ops, governance_ops)
                elif op_type not in ["set_table_property", "unset_table_property", "add_constraint", "drop_constraint", "reorder_columns", "add_row_filter", "update_row_filter", "remove_row_filter", "add_column_mask", "update_column_mask", "remove_column_mask"]:
                    other_ops.append(op)

            batch_dict = {
                "is_new_table": batch_info.is_new,
                "table_op": batch_info.create_op,
                "column_ops": column_ops,
                "property_ops": [op for op in batch_info.modify_ops if "property" in op.op],
                "constraint_ops": [op for op in batch_info.modify_ops if "constraint" in op.op],
                "reorder_ops": [op for op in batch_info.modify_ops if "reorder" in op.op],
                "governance_ops": [
                    op for op in batch_info.modify_ops if "filter" in op.op or "mask" in op.op
                ],
                "other_ops": other_ops,
                "op_ids": batch_info.op_ids,
                "operation_types": list(batch_info.operation_types),
            }
        else:
            batch_dict = batch_info

        if batch_dict["is_new_table"]:
            # Generate complete CREATE TABLE statement
            return self._generate_create_table_with_columns(table_id, batch_dict)
        else:
            # Generate optimized ALTER statements for existing table
            return self._generate_alter_statements_for_table(table_id, batch_dict)

    def _generate_create_table_with_columns(self, table_id: str, batch_info: dict[str, Any]) -> str:
        """Generate complete CREATE TABLE statement with all columns included, plus ALTER statements for column tags"""
        table_op = batch_info["table_op"]
        column_ops = batch_info["column_ops"]
        property_ops = batch_info["property_ops"]
        reorder_ops = batch_info.get("reorder_ops", [])
        other_ops = batch_info.get("other_ops", [])

        if not table_op:
            return "-- Error: No table creation operation found"

        # Get table name and schema info
        table_name = table_op.payload.get("name", "unknown")
        schema_id = table_op.payload.get("schemaId")
        schema_fqn = (
            self.id_name_map.get(schema_id, "unknown.unknown") if schema_id else "unknown.unknown"
        )
        table_fqn = f"{schema_fqn}.{table_name}"
        table_esc = self._build_fqn(*table_fqn.split("."))

        # Separate add_column operations from other column operations (like tags)
        add_column_ops = [op for op in column_ops if op.op.endswith("add_column")]
        other_column_ops = [op for op in column_ops if not op.op.endswith("add_column")]
        
        # Build column definitions as a dictionary (by column ID - use op.target for add_column)
        columns_dict = {}
        for col_op in add_column_ops:
            col_id = col_op.target  # Column ID is in op.target for add_column operations
            col_name = self.escape_identifier(col_op.payload["name"])
            col_type = col_op.payload["type"]
            nullable = "" if col_op.payload.get("nullable", True) else " NOT NULL"
            comment = (
                f" COMMENT '{self.escape_string(col_op.payload['comment'])}'"
                if col_op.payload.get("comment")
                else ""
            )
            columns_dict[col_id] = f"  {col_name} {col_type}{nullable}{comment}"

        # Apply column reordering if present
        # Use the final order from the last reorder operation
        if reorder_ops:
            final_order = reorder_ops[-1].payload.get("order", [])
            # Include columns from the reorder in their specified order
            columns = [columns_dict[col_id] for col_id in final_order if col_id in columns_dict]
            # Append any columns added AFTER the reorder (not in the reorder list)
            for col_id in columns_dict.keys():
                if col_id not in final_order:
                    columns.append(columns_dict[col_id])
        else:
            # No reorder: use the order columns were added
            columns = [
                columns_dict[col_id]
                for col_id in columns_dict.keys()
            ]

        # Build table format
        table_format = table_op.payload.get("format", "DELTA").upper()
        is_external = table_op.payload.get("external", False)

        # Resolve location for external tables
        location_info = (
            self._resolve_table_location(
                table_op.payload.get("externalLocationName"), table_op.payload.get("path")
            )
            if is_external
            else None
        )

        partition_cols = table_op.payload.get("partitionColumns", [])
        cluster_cols = table_op.payload.get("clusterColumns", [])

        # Build SQL clauses
        external_keyword = "EXTERNAL " if is_external else ""
        location_clause = (
            f" LOCATION '{self.escape_string(location_info['resolved'])}'" if location_info else ""
        )
        partition_clause = (
            f"\nPARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
        )
        cluster_clause = f"\nCLUSTER BY ({', '.join(cluster_cols)})" if cluster_cols else ""

        # Build table properties
        properties = []
        for prop_op in property_ops:
            if prop_op.op.endswith("set_table_property"):
                key = prop_op.payload["key"]
                value = prop_op.payload["value"]
                properties.append(f"'{key}' = '{self.escape_string(value)}'")

        # Build table comment
        # Check both table_op payload and set_table_comment operations in other_ops
        table_comment = ""
        comment_value = table_op.payload.get("comment")
        
        # Check if there's a set_table_comment operation
        for op in other_ops:
            if op.op.endswith("set_table_comment"):
                comment_value = op.payload.get("comment")
                break
        
        if comment_value:
            table_comment = f"\nCOMMENT '{self.escape_string(comment_value)}'"

        # Add warnings for external tables
        warnings = ""
        if is_external and location_info:
            warnings = (
                f"-- External Table: {table_name}\n"
                f"-- Location Name: {location_info['location_name']}\n"
            )

            if location_info["relative_path"]:
                warnings += f"-- Relative Path: {location_info['relative_path']}\n"

            warnings += (
                f"-- Resolved Location: {location_info['resolved']}\n"
                "-- WARNING: External tables must reference pre-configured external locations\n"
                "-- WARNING: Databricks recommends using managed tables for optimal performance\n"
                "-- Learn more: https://learn.microsoft.com/en-gb/azure/databricks/tables/managed\n"
            )

        # Assemble CREATE TABLE statement
        columns_sql = ",\n".join(columns) if columns else ""
        properties_sql = f"\nTBLPROPERTIES ({', '.join(properties)})" if properties else ""

        if columns_sql:
            create_sql = f"""{warnings}CREATE {external_keyword}TABLE IF NOT EXISTS {table_esc} (
{columns_sql}
) USING {table_format}{table_comment}{partition_clause}{cluster_clause}{properties_sql}{location_clause}"""
        else:
            # No columns yet - create empty table (fallback to original behavior)
            create_sql = (
                f"{warnings}"
                f"CREATE {external_keyword}TABLE IF NOT EXISTS {table_esc} () "
                f"USING {table_format}{table_comment}{partition_clause}{cluster_clause}{properties_sql}{location_clause}"
            )
        
        # Generate ALTER TABLE statements for operations that must happen after table creation
        # (e.g., table tags, column tags)
        # Skip set_table_comment since it's already included in CREATE TABLE
        statements = [create_sql]
        
        # Process other column operations (like column tags) after table creation
        for op in other_column_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")
        
        # Process other operations (like table tags)
        for op in other_ops:
            op_type = op.op.replace("unity.", "")
            # Skip set_table_comment as it's already in CREATE TABLE
            if op_type == "set_table_comment":
                continue
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")
        
        return ";\n".join(statements)

    def _generate_alter_statements_for_table(
        self, table_id: str, batch_info: dict[str, Any]
    ) -> str:
        """Generate optimized ALTER statements for existing table modifications"""
        statements = []

        # Handle column reordering first (using existing optimization)
        # For existing tables, reorder_columns generates ALTER statements
        if batch_info["reorder_ops"]:
            last_reorder_op = batch_info["reorder_ops"][-1]
            # Use previousOrder from operation payload if available
            # (prevents comparing state with itself)
            original_order = last_reorder_op.payload.get("previousOrder")
            if not original_order:
                # Fallback: derive from current state
                # (for backward compatibility with old operations)
                original_order = self._get_table_column_order(table_id)
            final_order = last_reorder_op.payload["order"]
            reorder_sql = self._generate_optimized_reorder_sql(
                table_id, original_order, final_order, [op.id for op in batch_info["reorder_ops"]]
            )
            if reorder_sql and not reorder_sql.startswith("--"):
                statements.append(reorder_sql)

        # Batch ADD COLUMN operations if multiple exist
        add_column_ops = [op for op in batch_info["column_ops"] if op.op.endswith("add_column")]

        if len(add_column_ops) > 1:
            # Multiple ADD COLUMN operations - batch them into single ALTER TABLE ADD COLUMNS
            table_fqn = self.id_name_map.get(add_column_ops[0].payload["tableId"], "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            column_defs = []

            for op in add_column_ops:
                col_name = op.payload["name"]
                col_type = op.payload["type"]
                comment = op.payload.get("comment", "")

                # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMNS for Delta tables
                # New columns added to existing tables must be nullable
                comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
                col_esc = self.escape_identifier(col_name)

                column_defs.append(f"    {col_esc} {col_type}{comment_clause}")

            batched_sql = (
                f"ALTER TABLE {table_esc}\nADD COLUMNS (\n" + ",\n".join(column_defs) + "\n)"
            )
            statements.append(batched_sql)
        elif len(add_column_ops) == 1:
            # Single ADD COLUMN - use existing method
            try:
                sql = self._add_column(add_column_ops[0])
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {add_column_ops[0].id}: {e}")

        # Batch DROP COLUMN operations if multiple exist
        drop_column_ops = [op for op in batch_info["column_ops"] if op.op.endswith("drop_column")]

        if len(drop_column_ops) > 1:
            # Multiple DROP COLUMN operations - batch them into single ALTER TABLE DROP COLUMNS
            table_fqn = self.id_name_map.get(drop_column_ops[0].payload["tableId"], "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))

            column_names = []
            for op in drop_column_ops:
                # Get column name from payload (for dropped columns not in current state)
                col_name = op.payload.get("name", self.id_name_map.get(op.target, "unknown"))
                col_esc = self.escape_identifier(col_name)
                column_names.append(col_esc)

            batched_sql = f"ALTER TABLE {table_esc}\nDROP COLUMNS (" + ", ".join(column_names) + ")"
            statements.append(batched_sql)
        elif len(drop_column_ops) == 1:
            # Single DROP COLUMN - use existing method
            try:
                sql = self._drop_column(drop_column_ops[0])
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {drop_column_ops[0].id}: {e}")

        # Handle other column operations (non-ADD/DROP COLUMN)
        other_column_ops = [
            op
            for op in batch_info["column_ops"]
            if not op.op.endswith("add_column") and not op.op.endswith("drop_column")
        ]

        # Handle all other operations normally
        for op in (
            other_column_ops
            + batch_info["property_ops"]
            + batch_info["constraint_ops"]
            + batch_info["governance_ops"]
            + batch_info["other_ops"]
        ):
            op_type = op.op.replace("unity.", "")

            # Skip reorder operations (already handled)
            if op_type == "reorder_columns":
                continue

            # Generate SQL for individual operation
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")

        return ";\n".join(statements) if statements else "-- No ALTER statements needed"

    def _change_column_type(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(op.target, "unknown")
        new_type = op.payload["newType"]
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} TYPE {new_type}"

    def _set_nullable(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(op.target, "unknown")
        nullable = op.payload["nullable"]
        col_esc = self.escape_identifier(col_name)

        if nullable:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} DROP NOT NULL"
        else:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET NOT NULL"

    def _set_column_comment(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(op.target, "unknown")
        comment = self.escape_string(op.payload["comment"])
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} COMMENT '{comment}'"

    # Column tag operations
    def _set_column_tag(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (fallback for new columns) or id_name_map
        col_name = op.payload.get("name", self.id_name_map.get(op.target, "unknown"))
        tag_name = op.payload["tagName"]
        tag_value = self.escape_string(op.payload["tagValue"])
        col_esc = self.escape_identifier(col_name)
        sql = f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc}"
        return f"{sql} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_column_tag(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (fallback for new columns) or id_name_map
        col_name = op.payload.get("name", self.id_name_map.get(op.target, "unknown"))
        tag_name = op.payload["tagName"]
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} UNSET TAGS ('{tag_name}')"

    # Constraint operations
    def _add_constraint(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        constraint_type = op.payload["type"]
        constraint_name = op.payload.get("name", "")
        columns = [self.id_name_map.get(cid, cid) for cid in op.payload["columns"]]

        name_clause = (
            f"CONSTRAINT {self.escape_identifier(constraint_name)} " if constraint_name else ""
        )

        if constraint_type == "primary_key":
            timeseries = " TIMESERIES" if op.payload.get("timeseries") else ""
            cols = ", ".join(self.escape_identifier(c) for c in columns)
            return f"ALTER TABLE {table_esc} ADD {name_clause}PRIMARY KEY({cols}){timeseries}"

        elif constraint_type == "foreign_key":
            parent_table = self.id_name_map.get(op.payload.get("parentTable", ""), "unknown")
            parent_esc = self._build_fqn(*parent_table.split("."))
            parent_columns = [
                self.id_name_map.get(cid, cid) for cid in op.payload.get("parentColumns", [])
            ]
            cols = ", ".join(self.escape_identifier(c) for c in columns)
            parent_cols = ", ".join(self.escape_identifier(c) for c in parent_columns)
            return (
                f"ALTER TABLE {table_esc} ADD {name_clause}"
                f"FOREIGN KEY({cols}) REFERENCES {parent_esc}({parent_cols})"
            )

        elif constraint_type == "check":
            expression = op.payload.get("expression", "TRUE")
            return f"ALTER TABLE {table_esc} ADD {name_clause}CHECK ({expression})"

        return ""

    def _drop_constraint(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Would need constraint name from state
        return f"-- ALTER TABLE {table_esc} DROP CONSTRAINT (constraint name lookup needed)"

    # Row filter operations
    def _add_row_filter(self, op: Operation) -> str:
        # Row filters require UDF creation first
        return f"-- Row filter: {op.payload['name']} - UDF: {op.payload['udfExpression']}"

    def _update_row_filter(self, op: Operation) -> str:
        return "-- Row filter update"

    def _remove_row_filter(self, op: Operation) -> str:
        return "-- Row filter removal"

    # Column mask operations
    def _add_column_mask(self, op: Operation) -> str:
        # Column masks require UDF creation first
        return f"-- Column mask: {op.payload['name']} - Function: {op.payload['maskFunction']}"

    def _update_column_mask(self, op: Operation) -> str:
        return "-- Column mask update"

    def _remove_column_mask(self, op: Operation) -> str:
        return "-- Column mask removal"
