"""
Unity Catalog SQL Generator

Generates Databricks SQL DDL statements from operations.
Migrated from TypeScript sql-generator.ts
"""

from typing import Any, TypedDict, cast

import sqlglot
from sqlglot import expressions as exp

from schemax.providers.base.batching import BatchInfo
from schemax.providers.base.dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)
from schemax.providers.base.exceptions import CircularDependencyError, SchemaXProviderError
from schemax.providers.base.operations import Operation
from schemax.providers.base.sql_generator import (
    BaseSQLGenerator,
    SQLGenerationResult,
    StatementInfo,
)

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

    _OP_HANDLERS: dict[str, str] = {
        "add_catalog": "_add_catalog",
        "rename_catalog": "_rename_catalog",
        "update_catalog": "_update_catalog",
        "drop_catalog": "_drop_catalog",
        "add_schema": "_add_schema",
        "rename_schema": "_rename_schema",
        "update_schema": "_update_schema",
        "drop_schema": "_drop_schema",
        "add_table": "_add_table",
        "rename_table": "_rename_table",
        "drop_table": "_drop_table",
        "set_table_comment": "_set_table_comment",
        "set_table_property": "_set_table_property",
        "unset_table_property": "_unset_table_property",
        "set_table_tag": "_set_table_tag",
        "unset_table_tag": "_unset_table_tag",
        "add_view": "_add_view",
        "rename_view": "_rename_view",
        "drop_view": "_drop_view",
        "update_view": "_update_view",
        "set_view_comment": "_set_view_comment",
        "set_view_property": "_set_view_property",
        "unset_view_property": "_unset_view_property",
        "add_volume": "_add_volume",
        "rename_volume": "_rename_volume",
        "update_volume": "_update_volume",
        "drop_volume": "_drop_volume",
        "add_function": "_add_function",
        "rename_function": "_rename_function",
        "update_function": "_update_function",
        "drop_function": "_drop_function",
        "set_function_comment": "_set_function_comment",
        "add_materialized_view": "_add_materialized_view",
        "rename_materialized_view": "_rename_materialized_view",
        "update_materialized_view": "_update_materialized_view",
        "drop_materialized_view": "_drop_materialized_view",
        "set_materialized_view_comment": "_set_materialized_view_comment",
        "add_column": "_add_column",
        "rename_column": "_rename_column",
        "drop_column": "_drop_column",
        "reorder_columns": "_reorder_columns",
        "change_column_type": "_change_column_type",
        "set_nullable": "_set_nullable",
        "set_column_comment": "_set_column_comment",
        "set_column_tag": "_set_column_tag",
        "unset_column_tag": "_unset_column_tag",
        "add_constraint": "_add_constraint",
        "drop_constraint": "_drop_constraint",
        "add_row_filter": "_add_row_filter",
        "update_row_filter": "_update_row_filter",
        "remove_row_filter": "_remove_row_filter",
        "add_column_mask": "_add_column_mask",
        "update_column_mask": "_update_column_mask",
        "remove_column_mask": "_remove_column_mask",
        "add_grant": "_add_grant",
        "revoke_grant": "_revoke_grant",
    }

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

    def _add_catalog_to_id_name_map(self, catalog: Any, id_map: dict[str, str]) -> None:
        """Populate id_map with entries for one catalog and its schemas/tables/views/volumes/etc."""
        cat_name = catalog.name if hasattr(catalog, "name") else catalog["name"]
        cat_id = catalog.id if hasattr(catalog, "id") else catalog["id"]
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
                id_map[table_id] = f"{catalog_name}.{schema_name}.{table_name}"
                for column in (
                    table.columns if hasattr(table, "columns") else table.get("columns", [])
                ):
                    col_name = column.name if hasattr(column, "name") else column["name"]
                    col_id = column.id if hasattr(column, "id") else column["id"]
                    id_map[col_id] = col_name

            views = schema.views if hasattr(schema, "views") else schema.get("views", [])
            for view in views:
                view_name = view.name if hasattr(view, "name") else view["name"]
                view_id = view.id if hasattr(view, "id") else view["id"]
                id_map[view_id] = f"{catalog_name}.{schema_name}.{view_name}"

            for attr in ("volumes", "functions", "materialized_views"):
                items = (
                    schema.get(attr, []) if isinstance(schema, dict) else getattr(schema, attr, [])
                )
                for item in items:
                    item_name = item.name if hasattr(item, "name") else item["name"]
                    item_id = item.id if hasattr(item, "id") else item["id"]
                    id_map[item_id] = f"{catalog_name}.{schema_name}.{item_name}"

    def _build_id_name_map(self) -> dict[str, str]:
        """
        Build a mapping from IDs to fully-qualified names.

        Uses catalog_name_mapping to replace logical catalog names with physical names
        when generating environment-specific SQL.
        """
        id_map: dict[str, str] = {}
        catalogs = (
            self.state.catalogs
            if hasattr(self.state, "catalogs")
            else self.state.get("catalogs", [])
        )
        for catalog in catalogs:
            self._add_catalog_to_id_name_map(catalog, id_map)
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

    def can_generate_sql(self, operation: Operation) -> bool:
        """Check if operation can be converted to SQL"""
        return operation.op in UNITY_OPERATIONS.values()

    # ========================================
    # ABSTRACT METHOD IMPLEMENTATIONS (from BaseSQLGenerator)
    # ========================================

    def _get_dependency_level(self, operation: Operation) -> int:
        """
        Get dependency level for Unity operation ordering.
        0 = catalog, 1 = schema, 2 = table/view creation, 3 = table/view modifications

        Implements abstract method from BaseSQLGenerator.
        """
        op_type = operation.op
        if "catalog" in op_type:
            return 0
        if "schema" in op_type:
            return 1
        if (
            "add_table" in op_type
            or "add_view" in op_type
            or "add_volume" in op_type
            or "add_function" in op_type
            or "add_materialized_view" in op_type
        ):
            return 2
        return 3  # All other table/view/volume/function/MV operations

    def _get_target_object_id(self, operation: Operation) -> str | None:
        """
        Extract target object ID from Unity operation.

        Implements abstract method from BaseSQLGenerator.
        Used by batching algorithm to group operations by catalog/schema/table.
        """
        op_type = operation.op.replace("unity.", "")

        # Catalog-level operations
        if op_type in {"add_catalog", "rename_catalog", "update_catalog", "drop_catalog"}:
            return f"catalog:{operation.target}"  # Prefix to avoid ID collisions

        # Schema-level operations
        if op_type in {"add_schema", "rename_schema", "update_schema", "drop_schema"}:
            return f"schema:{operation.target}"  # Prefix to avoid ID collisions

        # Table-level operations
        if op_type in {"add_table", "rename_table", "drop_table", "set_table_comment"}:
            return f"table:{operation.target}"  # Add prefix for consistency

        # View-level operations
        if op_type in {
            "add_view",
            "rename_view",
            "drop_view",
            "update_view",
            "set_view_comment",
            "set_view_property",
            "unset_view_property",
        }:
            return f"view:{operation.target}"  # Prefix to avoid ID collisions

        # Volume-level operations
        if op_type in {"add_volume", "rename_volume", "update_volume", "drop_volume"}:
            return f"volume:{operation.target}"

        # Function-level operations
        if op_type in {
            "add_function",
            "rename_function",
            "update_function",
            "drop_function",
            "set_function_comment",
        }:
            return f"function:{operation.target}"

        # Materialized view-level operations
        if op_type in {
            "add_materialized_view",
            "rename_materialized_view",
            "update_materialized_view",
            "drop_materialized_view",
            "set_materialized_view_comment",
        }:
            return f"materialized_view:{operation.target}"

        # Column and table property operations
        if op_type in {
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
        }:
            table_id = operation.payload.get("tableId")
            return f"table:{table_id}" if table_id else None

        return None  # Global operations with no specific target

    def _is_create_operation(self, operation: Operation) -> bool:
        """
        Check if Unity operation creates a new object.

        Implements abstract method from BaseSQLGenerator.
        """
        return operation.op in {
            "unity.add_catalog",
            "unity.add_schema",
            "unity.add_table",
            "unity.add_view",
            "unity.add_volume",
            "unity.add_function",
            "unity.add_materialized_view",
        }

    def _is_drop_operation(self, operation: Operation) -> bool:
        """
        Check if Unity operation drops an object.

        Implements abstract method from BaseSQLGenerator.
        DROP operations cannot be batched with CREATE/ALTER and must be handled separately.

        Note: drop_column and drop_constraint are NOT included here because they are
        table modifications (processed in order with other table operations), not object drops.
        """
        return operation.op in {
            "unity.drop_catalog",
            "unity.drop_schema",
            "unity.drop_table",
            "unity.drop_view",
            "unity.drop_volume",
            "unity.drop_function",
            "unity.drop_materialized_view",
        }

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
        if object_id.startswith("schema:"):
            return self._generate_create_schema_batched(object_id, batch_info)
        if object_id.startswith("table:"):
            # Delegate to existing _generate_create_table_with_columns method
            # Categorize operations properly
            column_ops = []
            property_ops = []
            constraint_ops = []
            reorder_ops = []
            governance_ops = []
            other_ops = []

            for operation in batch_info.modify_ops:
                op_type = operation.op.replace("unity.", "")
                if op_type in {
                    "add_column",
                    "rename_column",
                    "drop_column",
                    "change_column_type",
                    "set_nullable",
                    "set_column_comment",
                    "set_column_tag",
                    "unset_column_tag",
                }:
                    column_ops.append(operation)
                elif op_type in {"set_table_property", "unset_table_property"}:
                    property_ops.append(operation)
                elif op_type in {"add_constraint", "drop_constraint"}:
                    constraint_ops.append(operation)
                if op_type == "reorder_columns":
                    reorder_ops.append(operation)
                elif op_type in {
                    "add_row_filter",
                    "update_row_filter",
                    "remove_row_filter",
                    "add_column_mask",
                    "update_column_mask",
                    "remove_column_mask",
                }:
                    governance_ops.append(operation)
                else:
                    # Everything else goes to other_ops (includes set_table_tag, set_table_comment, etc.)
                    other_ops.append(operation)

            batch_dict = {
                "is_new_table": batch_info.is_new,
                "table_op": batch_info.create_op,
                "column_ops": column_ops,
                "property_ops": property_ops,
                "constraint_ops": constraint_ops,
                "reorder_ops": reorder_ops,
                "governance_ops": governance_ops,
                "other_ops": other_ops,
                "op_ids": batch_info.op_ids,
                "operation_types": list(batch_info.operation_types),
            }
            return self._generate_create_table_with_columns(object_id, batch_dict)
        # Fallback for objects without prefix (shouldn't happen after refactor)
        return ""

    def _generate_batched_alter_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate ALTER statements for Unity table modifications.

        Implements abstract method from BaseSQLGenerator.
        """
        # Delegate to existing _generate_alter_statements_for_table method
        # Categorize operations properly
        column_ops = []
        property_ops = []
        constraint_ops = []
        reorder_ops = []
        governance_ops = []
        other_ops = []

        table_tag_ops = []
        for operation in batch_info.modify_ops:
            op_type = operation.op.replace("unity.", "")
            if op_type in {
                "add_column",
                "rename_column",
                "drop_column",
                "change_column_type",
                "set_nullable",
                "set_column_comment",
                "set_column_tag",
                "unset_column_tag",
            }:
                column_ops.append(operation)
            elif op_type in {"set_table_property", "unset_table_property"}:
                property_ops.append(operation)
            elif op_type in {"add_constraint", "drop_constraint"}:
                constraint_ops.append(operation)
            if op_type == "reorder_columns":
                reorder_ops.append(operation)
            elif op_type in {
                "add_row_filter",
                "update_row_filter",
                "remove_row_filter",
                "add_column_mask",
                "update_column_mask",
                "remove_column_mask",
            }:
                governance_ops.append(operation)
            elif op_type in {"set_table_tag", "unset_table_tag"}:
                table_tag_ops.append(operation)
            else:
                # Everything else goes to other_ops (set_table_comment, etc.)
                other_ops.append(operation)

        batch_dict = {
            "column_ops": column_ops,
            "property_ops": property_ops,
            "constraint_ops": constraint_ops,
            "reorder_ops": reorder_ops,
            "governance_ops": governance_ops,
            "table_tag_ops": table_tag_ops,
            "other_ops": other_ops,
            "op_ids": batch_info.op_ids,
            "operation_types": list(batch_info.operation_types),
        }
        return self._generate_alter_statements_for_table(object_id, batch_dict)

    # ========================================
    # END ABSTRACT METHOD IMPLEMENTATIONS
    # ========================================

    _CANCEL_PAIRS: list[tuple[str, str]] = [
        ("unity.add_catalog", "unity.drop_catalog"),
        ("unity.add_schema", "unity.drop_schema"),
        ("unity.add_table", "unity.drop_table"),
        ("unity.add_view", "unity.drop_view"),
        ("unity.add_volume", "unity.drop_volume"),
        ("unity.add_function", "unity.drop_function"),
        ("unity.add_materialized_view", "unity.drop_materialized_view"),
        ("unity.add_column", "unity.drop_column"),
    ]

    def _group_ops_by_target(self, ops: list[Operation]) -> dict[str, list[Operation]]:
        """Group operations by target object ID (or __global__)."""
        by_target: dict[str, list[Operation]] = {}
        for operation in ops:
            target_id = self._get_target_object_id(operation)
            key = target_id if target_id else "__global__"
            by_target.setdefault(key, []).append(operation)
        return by_target

    def _is_target_cancelled(self, target_ops: list[Operation]) -> bool:
        """Return True if this target's ops are a cancelled create+drop pair."""
        if len(target_ops) < 2:
            return False
        create_op: Operation | None = None
        drop_op: Operation | None = None
        for operation in target_ops:
            for create_type, drop_type in self._CANCEL_PAIRS:
                if operation.op == create_type:
                    create_op = operation
                elif operation.op == drop_type:
                    drop_op = operation
        if create_op and drop_op and create_op.ts < drop_op.ts:
            return True
        if len(target_ops) == 2:
            if (
                target_ops[0].op == "unity.add_column"
                and target_ops[1].op == "unity.drop_column"
                and target_ops[0].target == target_ops[1].target
                and target_ops[0].ts < target_ops[1].ts
            ):
                return True
        return False

    def _filter_cancelled_operations(self, ops: list[Operation]) -> list[Operation]:
        """
        Filter out create+drop pairs that cancel each other.

        If an object is created and then dropped in the same changeset,
        skip ALL operations for that object (they cancel out).

        This is timestamp-aware: cancellation only occurs if CREATE timestamp < DROP timestamp
        (chronological order). If DROP comes before CREATE, they don't cancel.

        This prevents errors when trying to drop objects that were never
        created in the database (e.g., table in non-existent schema).
        """
        filtered: list[Operation] = []
        for _target_id, target_ops in self._group_ops_by_target(ops).items():
            if not target_ops:
                continue
            if not self._is_target_cancelled(target_ops):
                filtered.extend(target_ops)
        return filtered

    def _build_name_to_node_id_map(self) -> dict[str, str]:
        """
        Build a map from table/view/MV name (or qualified name or raw ID) to graph node ID.

        Used to resolve extractedDependencies (names from UI) and dependencies (IDs) to
        node IDs like "table:xyz" so the dependency graph can order table before view/MV.
        """
        name_to_node: dict[str, str] = {}
        catalogs = (
            self.state.catalogs
            if hasattr(self.state, "catalogs")
            else self.state.get("catalogs", [])
        )
        for catalog in catalogs:
            cat_name = catalog.name if hasattr(catalog, "name") else catalog["name"]
            catalog_name = self.catalog_name_mapping.get(cat_name, cat_name)
            schemas = catalog.schemas if hasattr(catalog, "schemas") else catalog.get("schemas", [])
            for schema in schemas:
                schema_name = schema.name if hasattr(schema, "name") else schema["name"]
                for attr, prefix in (
                    ("tables", "table"),
                    ("views", "view"),
                    ("materialized_views", "materialized_view"),
                ):
                    items = (
                        schema.get(attr, [])
                        if isinstance(schema, dict)
                        else getattr(schema, attr, [])
                    )
                    for item in items:
                        item_name = item.name if hasattr(item, "name") else item["name"]
                        item_id = item.id if hasattr(item, "id") else item["id"]
                        node_id = f"{prefix}:{item_id}"
                        name_to_node[item_id] = node_id
                        name_to_node[item_name] = node_id
                        name_to_node[f"{schema_name}.{item_name}"] = node_id
                        name_to_node[f"{catalog_name}.{schema_name}.{item_name}"] = node_id
        return name_to_node

    def _resolve_dependency_to_node_id(self, name_or_id: str) -> str | None:
        """Resolve a dependency name or object ID to a graph node ID (e.g. table:xyz)."""
        if not name_or_id or not isinstance(name_or_id, str):
            return None
        name_or_id = name_or_id.strip()
        if not name_or_id:
            return None
        if getattr(self, "_name_to_node_id_map", None) is None:
            self._name_to_node_id_map = self._build_name_to_node_id_map()
        node_id = self._name_to_node_id_map.get(name_or_id)
        if node_id:
            return node_id
        # Already a node ID (table:xyz)?
        if name_or_id.startswith(("table:", "view:", "materialized_view:")):
            return name_or_id
        return None

    def _resolve_view_dep(
        self, name_or_id: str
    ) -> tuple[str, DependencyType, DependencyEnforcement] | None:
        """Resolve a name or ID to a view/table dependency tuple, or None."""
        node_id = self._resolve_dependency_to_node_id(name_or_id)
        if node_id:
            return (node_id, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
        return None

    def _extract_view_dependencies(
        self, operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """Extract view dependencies from add_view payload (dependencies + extractedDependencies)."""
        out: list[tuple[str, DependencyType, DependencyEnforcement]] = []
        dep = operation.payload.get("dependencies", [])
        extracted = operation.payload.get("extractedDependencies", {})
        if isinstance(dep, list):
            for name_or_id in dep:
                t = self._resolve_view_dep(name_or_id)
                if t:
                    out.append(t)
        if isinstance(extracted, dict):
            for name_or_id in extracted.get("tables", []) + extracted.get("views", []):
                t = self._resolve_view_dep(name_or_id)
                if t:
                    out.append(t)
        return out

    def _extract_mv_dependencies(
        self, operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """Extract materialized view dependencies from add_materialized_view payload."""
        out: list[tuple[str, DependencyType, DependencyEnforcement]] = []
        dep_ids = operation.payload.get("dependencies", []) or operation.payload.get(
            "extractedDependencies", {}
        )
        if isinstance(dep_ids, list):
            for name_or_id in dep_ids:
                t = self._resolve_view_dep(name_or_id)
                if t:
                    out.append(t)
        elif isinstance(dep_ids, dict):
            for name_or_id in dep_ids.get("tables", []) + dep_ids.get("views", []):
                t = self._resolve_view_dep(name_or_id)
                if t:
                    out.append(t)
        return out

    def _extract_fk_dependencies(
        self, operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """Extract foreign key parent-table dependency from add_constraint payload."""
        out: list[tuple[str, DependencyType, DependencyEnforcement]] = []
        if operation.payload.get("type") != "foreign_key":
            return out
        parent_table_id = operation.payload.get("parentTable")
        if parent_table_id:
            parent_table_node_id = f"table:{parent_table_id}"
            out.append(
                (
                    parent_table_node_id,
                    DependencyType.FOREIGN_KEY,
                    DependencyEnforcement.ENFORCED,
                )
            )
        return out

    def _extract_operation_dependencies(
        self, operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """
        Extract dependencies from Unity Catalog operations.

        Supports view/MV dependencies on tables/views and foreign key dependencies.
        """
        if operation.op == "unity.add_view":
            return self._extract_view_dependencies(operation)
        if operation.op == "unity.add_materialized_view":
            return self._extract_mv_dependencies(operation)
        if operation.op == "unity.add_constraint":
            return self._extract_fk_dependencies(operation)
        return []

    def _add_constraint_ordering_edges(self, graph: DependencyGraph, ops: list[Operation]) -> None:
        """Add per-constraint-op nodes and edges so DROP executes before ADD on same table."""
        ops_by_target = graph.metadata.get("ops_by_target", {})
        table_ops: dict[str, list[Operation]] = {}
        for operation in ops:
            if operation.op in {"unity.add_constraint", "unity.drop_constraint"}:
                table_id = operation.payload.get("tableId")
                if table_id:
                    target_id = f"table:{table_id}"
                    table_ops.setdefault(target_id, []).append(operation)

        for table_node_id, table_constraint_ops in table_ops.items():
            drop_ops = [o for o in table_constraint_ops if o.op == "unity.drop_constraint"]
            add_ops = [o for o in table_constraint_ops if o.op == "unity.add_constraint"]
            if not drop_ops or not add_ops:
                continue
            if table_node_id in ops_by_target:
                drop_set = set(drop_ops)
                add_set = set(add_ops)
                remaining = [
                    o
                    for o in ops_by_target[table_node_id]
                    if o not in drop_set and o not in add_set
                ]
                ops_by_target[table_node_id] = remaining

            level = self._get_dependency_level(drop_ops[0])
            for drop_op in drop_ops:
                if drop_op.id not in graph.nodes:
                    graph.add_node(
                        DependencyNode(
                            id=drop_op.id,
                            type="constraint",
                            hierarchy_level=level,
                            operation=drop_op,
                            metadata={"op_type": drop_op.op},
                        )
                    )
                    ops_by_target[drop_op.id] = [drop_op]
                graph.add_edge(
                    table_node_id,
                    drop_op.id,
                    DependencyType.CONSTRAINT_ORDERING,
                    DependencyEnforcement.ENFORCED,
                )
            for add_op in add_ops:
                if add_op.id not in graph.nodes:
                    graph.add_node(
                        DependencyNode(
                            id=add_op.id,
                            type="constraint",
                            hierarchy_level=level,
                            operation=add_op,
                            metadata={"op_type": add_op.op},
                        )
                    )
                    ops_by_target[add_op.id] = [add_op]
                for drop_op in drop_ops:
                    graph.add_edge(
                        drop_op.id,
                        add_op.id,
                        DependencyType.CONSTRAINT_ORDERING,
                        DependencyEnforcement.ENFORCED,
                    )

    def _build_dependency_graph(self, ops: list[Operation]) -> DependencyGraph:
        """
        Build a dependency graph from operations, including constraint ordering.

        Overrides base class to add special handling: DROP constraint before ADD on same table.
        """
        graph = super()._build_dependency_graph(ops)
        self._add_constraint_ordering_edges(graph, ops)
        return graph

    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL statements with comprehensive batch optimizations.

        Returns just the SQL string for backward compatibility.
        Use generate_sql_with_mapping() for structured results with operation tracking.

        Optimizations include:
        - Dependency-ordered operations (catalog → schema → table)
        - CREATE + DROP cancellation (skip objects created then dropped)
        - Batched CREATE + UPDATE operations (squashed into single CREATE)
        - Complete CREATE TABLE statements (no empty tables + ALTERs)
        - Batched column reordering (minimal ALTER statements)
        - Table property consolidation
        """
        result = self.generate_sql_with_mapping(ops)
        return result.sql

    def _topological_sort_with_fallback(
        self, ops: list[Operation]
    ) -> tuple[list[Operation], list[str]]:
        """
        Sort operations using topological sort based on dependencies.
        Falls back to level-based sorting if cycles are detected.

        Args:
            ops: Operations to sort

        Returns:
            Tuple of (sorted operations, list of warning messages)
        """
        warnings: list[str] = []

        try:
            # Build dependency graph
            graph = self._build_dependency_graph(ops)

            # Check for cycles
            cycles = graph.detect_cycles()
            if cycles:
                # Format cycles for error message
                cycle_paths: list[list[str]] = []
                for cycle in cycles:
                    # Get names from id_name_map
                    cycle_names: list[str] = []
                    for node_id in cycle:
                        name = self.id_name_map.get(node_id, node_id)
                        cycle_names.append(name)
                    cycle_paths.append(cycle_names)  # Append list, not string

                raise CircularDependencyError(cycle_paths)

            # Use topological sort (only includes ops with a target object in the graph)
            sorted_ops = graph.topological_sort()
            # Include ops without a target (e.g. add_grant, revoke_grant) - append after CREATEs
            op_ids_in_result = {operation.id for operation in sorted_ops}
            for operation in ops:
                if operation.id not in op_ids_in_result:
                    sorted_ops.append(operation)
            return sorted_ops, warnings

        except CircularDependencyError:
            # Re-raise to be handled by caller
            raise
        except Exception as e:
            # Unexpected error - warn and fall back
            warnings.append(
                f"Dependency analysis failed: {e}. Falling back to level-based sorting."
            )
            return sorted(
                ops, key=lambda operation: (self._get_dependency_level(operation), operation.ts)
            ), warnings

    @staticmethod
    def _split_sql_statements(sql: str) -> list[str]:
        """Split a possibly multi-statement SQL string into single statements.

        Handles generators that return ';\\n'-separated statements (e.g. _update_catalog,
        _update_schema, _add_column with NOT NULL). Ensures each StatementInfo has one
        statement so the final join does not produce double semicolons.
        """
        if not sql or not sql.strip():
            return []
        parts = [p.strip() for p in sql.split(";\n") if p.strip()]
        return parts if parts else [sql.strip()]

    def _sort_ops_with_fallback(self, ops: list[Operation]) -> tuple[list[Operation], list[str]]:
        """Sort ops with topological sort; on CircularDependencyError, fallback to level sort."""
        try:
            return self._topological_sort_with_fallback(ops)
        except CircularDependencyError as e:
            warning_msg = (
                "\n" + "=" * 70 + "\n"
                "⚠️  CRITICAL WARNING: CIRCULAR DEPENDENCIES DETECTED\n"
                "=" * 70 + "\n"
                f"{e}\n"
                "→ Falling back to level-based sorting (dependencies IGNORED)\n"
                "→ Run 'schemax validate' to see full details\n"
                "→ SQL execution may FAIL if dependencies are not met\n"
                "=" * 70
            )
            print(warning_msg)
            fallback = sorted(
                ops, key=lambda operation: (self._get_dependency_level(operation), operation.ts)
            )
            return fallback, ["Circular dependencies detected - dependencies ignored"]
        except Exception as e:
            print(f"Warning: Dependency analysis failed ({e}), using level-based sort")
            fallback = sorted(
                ops, key=lambda operation: (self._get_dependency_level(operation), operation.ts)
            )
            return fallback, [f"Dependency analysis failed: {e}"]

    def _collect_batched_sql_by_type(
        self,
        batches: dict[str, Any],
        processed_op_ids: set[str],
    ) -> tuple[
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
        list[tuple[str, list[str]]],
    ]:
        """Process batches and return seven statement lists (catalog, schema, table, view, mv, volume, function)."""
        catalog_stmts: list[tuple[str, list[str]]] = []
        schema_stmts: list[tuple[str, list[str]]] = []
        table_stmts: list[tuple[str, list[str]]] = []
        view_stmts: list[tuple[str, list[str]]] = []
        materialized_view_stmts: list[tuple[str, list[str]]] = []
        volume_stmts: list[tuple[str, list[str]]] = []
        function_stmts: list[tuple[str, list[str]]] = []

        prefix_to_type = {
            "catalog:": "catalog",
            "schema:": "schema",
            "table:": "table",
            "view:": "view",
            "materialized_view:": "materialized_view",
            "volume:": "volume",
            "function:": "function",
        }

        for object_id, batch_info in batches.items():
            object_type = None
            for prefix, typ in prefix_to_type.items():
                if object_id.startswith(prefix):
                    object_type = typ
                    break
            if object_type is None:
                continue
            op_ids = batch_info.op_ids
            processed_op_ids.update(op_ids)

            if object_type == "catalog":
                sql = self._generate_create_catalog_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    catalog_stmts.append((sql, op_ids))
            elif object_type == "schema":
                sql = self._generate_create_schema_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    schema_stmts.append((sql, op_ids))
            elif object_type == "table":
                table_result = self._generate_table_sql_with_mapping(object_id, batch_info)
                table_stmts.extend(table_result)
            elif object_type == "view":
                view_result = self._generate_view_sql_with_mapping(object_id, batch_info)
                view_stmts.extend(view_result)
            elif object_type == "materialized_view":
                mv_result = self._generate_materialized_view_sql_with_mapping(object_id, batch_info)
                materialized_view_stmts.extend(mv_result)
            elif object_type == "volume":
                vol_result = self._generate_volume_sql_with_mapping(object_id, batch_info)
                volume_stmts.extend(vol_result)
            elif object_type == "function":
                func_result = self._generate_function_sql_with_mapping(object_id, batch_info)
                function_stmts.extend(func_result)

        return (
            catalog_stmts,
            schema_stmts,
            table_stmts,
            view_stmts,
            materialized_view_stmts,
            volume_stmts,
            function_stmts,
        )

    def _build_statement_infos_from_batched(
        self,
        catalog_stmts: list[tuple[str, list[str]]],
        schema_stmts: list[tuple[str, list[str]]],
        table_stmts: list[tuple[str, list[str]]],
        view_stmts: list[tuple[str, list[str]]],
        materialized_view_stmts: list[tuple[str, list[str]]],
        volume_stmts: list[tuple[str, list[str]]],
        function_stmts: list[tuple[str, list[str]]],
        execution_order_start: int,
    ) -> tuple[list[StatementInfo], int]:
        """Build StatementInfo list from the seven batched statement lists; return (infos, next_order)."""
        statement_infos: list[StatementInfo] = []
        order = execution_order_start
        for sql, op_ids in catalog_stmts:
            for sql_part in self._split_sql_statements(sql):
                order += 1
                statement_infos.append(
                    StatementInfo(sql=sql_part, operation_ids=op_ids, execution_order=order)
                )
        for sql, op_ids in schema_stmts:
            for sql_part in self._split_sql_statements(sql):
                order += 1
                statement_infos.append(
                    StatementInfo(sql=sql_part, operation_ids=op_ids, execution_order=order)
                )
        for sql, op_ids in table_stmts:
            order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=order)
            )
        for sql, op_ids in view_stmts:
            order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=order)
            )
        for sql, op_ids in materialized_view_stmts:
            order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=order)
            )
        for sql, op_ids in volume_stmts:
            order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=order)
            )
        for sql, op_ids in function_stmts:
            order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=order)
            )
        return statement_infos, order

    def _append_statement_infos_for_other_ops(
        self,
        other_ops: list[Operation],
        statement_infos: list[StatementInfo],
        execution_order: int,
    ) -> int:
        """Append StatementInfos for unbatched non-DROP ops; return next execution_order."""
        order = execution_order
        for operation in other_ops:
            result = self.generate_sql_for_operation(operation)
            if not result.sql:
                continue
            sql_stripped = result.sql.strip()
            if sql_stripped.startswith("-- Error") or sql_stripped.startswith("-- No "):
                raise SchemaXProviderError(sql_stripped)
            if not sql_stripped.startswith("--"):
                for sql_part in self._split_sql_statements(result.sql):
                    order += 1
                    statement_infos.append(
                        StatementInfo(
                            sql=sql_part,
                            operation_ids=[operation.id],
                            execution_order=order,
                        )
                    )
        return order

    def _append_statement_infos_for_drops(
        self,
        drop_ops: list[Operation],
        statement_infos: list[StatementInfo],
        execution_order: int,
    ) -> int:
        """Append StatementInfos for DROP ops (reverse dependency order); return next order."""
        order = execution_order
        for operation in sorted(
            drop_ops, key=lambda operation: -self._get_dependency_level(operation)
        ):
            result = self.generate_sql_for_operation(operation)
            if result.sql and not result.sql.startswith("--"):
                for sql_part in self._split_sql_statements(result.sql):
                    order += 1
                    statement_infos.append(
                        StatementInfo(
                            sql=sql_part,
                            operation_ids=[operation.id],
                            execution_order=order,
                        )
                    )
        return order

    def generate_sql_with_mapping(self, ops: list[Operation]) -> "SQLGenerationResult":
        """
        Generate SQL with explicit operation-to-statement mapping.

        Returns SQLGenerationResult with:
        - sql: Combined SQL script
        - statements: List of StatementInfo (sql + operation_ids + execution_order)
        """
        global_warnings: list[str] = []
        sorted_ops, sort_warnings = self._sort_ops_with_fallback(ops)
        global_warnings.extend(sort_warnings)

        sorted_ops = self._filter_cancelled_operations(sorted_ops)
        drop_ops = [operation for operation in sorted_ops if self._is_drop_operation(operation)]
        non_drop_ops = [
            operation for operation in sorted_ops if not self._is_drop_operation(operation)
        ]
        batches = self.batcher.batch_operations(
            non_drop_ops, self._get_target_object_id, self._is_create_operation
        )
        processed_op_ids: set[str] = set()

        (
            catalog_stmts,
            schema_stmts,
            table_stmts,
            view_stmts,
            materialized_view_stmts,
            volume_stmts,
            function_stmts,
        ) = self._collect_batched_sql_by_type(batches, processed_op_ids)

        other_ops: list[Operation] = []
        for operation in non_drop_ops:
            if operation.id in processed_op_ids:
                continue
            if not self.can_generate_sql(operation):
                print(f"Warning: Cannot generate SQL for operation: {operation.op}")
                continue
            other_ops.append(operation)

        statement_infos, execution_order = self._build_statement_infos_from_batched(
            catalog_stmts,
            schema_stmts,
            table_stmts,
            view_stmts,
            materialized_view_stmts,
            volume_stmts,
            function_stmts,
            0,
        )
        execution_order = self._append_statement_infos_for_other_ops(
            other_ops, statement_infos, execution_order
        )
        execution_order = self._append_statement_infos_for_drops(
            drop_ops, statement_infos, execution_order
        )

        combined_sql = ";\n\n".join(stmt.sql for stmt in statement_infos)
        if combined_sql:
            combined_sql += ";"
        return SQLGenerationResult(
            sql=combined_sql,
            statements=statement_infos,
            warnings=global_warnings,
            is_idempotent=True,
        )

    def generate_sql_for_operation(self, operation: Operation) -> SQLGenerationResult:
        """Generate SQL for a single operation"""
        # Strip provider prefix
        op_type = operation.op.replace("unity.", "")

        try:
            sql = self._generate_sql_for_op_type(op_type, operation)
            return SQLGenerationResult(sql=sql, warnings=[], is_idempotent=True)
        except Exception as e:
            return SQLGenerationResult(
                sql=f"-- Error generating SQL: {e}",
                warnings=[str(e)],
                is_idempotent=False,
            )

    def _generate_sql_for_op_type(self, op_type: str, operation: Operation) -> str:
        """Generate SQL based on operation type via dispatch map."""
        handler_name = self._OP_HANDLERS.get(op_type)
        if handler_name is None:
            raise ValueError(f"Unsupported operation type: {op_type}")
        return cast(str, getattr(self, handler_name)(operation))

    # Catalog operations
    def _add_catalog(self, operation: Operation) -> str:
        # Use mapped name from id_name_map (handles __implicit__ → physical catalog)
        name = self.id_name_map.get(operation.target, operation.payload["name"])

        # Fallback: If the catalog doesn't exist in id_name_map yet (e.g., from diff operations),
        # apply catalog_name_mapping to convert logical → physical name
        if (
            operation.target not in self.id_name_map
            and operation.payload["name"] in self.catalog_name_mapping
        ):
            name = self.catalog_name_mapping[operation.payload["name"]]

        # Build CREATE CATALOG statement
        sql = f"CREATE CATALOG IF NOT EXISTS {self.escape_identifier(name)}"

        # Add managed location if specified
        managed_location_name = operation.payload.get("managedLocationName")
        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        # Add comment if specified
        comment = operation.payload.get("comment")
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags need to be set via ALTER after creation
        result = sql
        tags = operation.payload.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"

        return result

    def _rename_catalog(self, operation: Operation) -> str:
        old_name = operation.payload["oldName"]
        new_name = operation.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER CATALOG {old_esc} RENAME TO {new_esc}"

    def _update_catalog(self, operation: Operation) -> str:
        """Update catalog properties (managed location, comment, tags)"""
        name = self.id_name_map.get(operation.target, operation.target)
        statements = []

        # Handle managed location
        managed_location_name = operation.payload.get("managedLocationName")
        if managed_location_name is not None:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                statements.append(
                    f"ALTER CATALOG {self.escape_identifier(name)} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )

        # Handle comment
        comment = operation.payload.get("comment")
        if comment is not None:
            if comment:
                statements.append(
                    f"ALTER CATALOG {self.escape_identifier(name)} "
                    f"SET COMMENT '{self.escape_string(comment)}'"
                )
            else:
                statements.append(f"ALTER CATALOG {self.escape_identifier(name)} UNSET COMMENT")

        # Handle tags
        tags = operation.payload.get("tags")
        if tags is not None and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            statements.append(
                f"ALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"
            )

        if statements:
            return ";\n".join(statements)
        return "-- Warning: No updates specified for catalog"

    def _drop_catalog(self, operation: Operation) -> str:
        # Resolve catalog name: for diff-generated drops the payload has "name" (logical);
        # id_name_map may not contain the catalog when desired state is empty (e.g. drop-only snapshot).
        logical_name = (operation.payload or {}).get("name")
        if logical_name is not None and str(logical_name).strip():
            name = self.catalog_name_mapping.get(logical_name, logical_name)
        else:
            name = self.id_name_map.get(operation.target, operation.target)
        # Use CASCADE to ensure catalog drops even if it contains schemas/tables
        # This handles drift scenarios where catalog may have objects we don't track
        # CASCADE is safe for rollback: we're reverting to a previous known state
        # Note: In Unity Catalog, CASCADE soft-deletes managed tables (cleanup in 7-30 days)
        return f"DROP CATALOG IF EXISTS {self.escape_identifier(name)} CASCADE"

    # Schema operations
    def _add_schema(self, operation: Operation) -> str:
        catalog_name = self.id_name_map.get(operation.payload["catalogId"], "unknown")
        schema_name = operation.payload["name"]
        managed_location_name = operation.payload.get("managedLocationName")

        catalog_esc = self.escape_identifier(catalog_name)
        schema_esc = self.escape_identifier(schema_name)
        sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_esc}.{schema_esc}"

        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"

        # Add comment if specified
        comment = operation.payload.get("comment")
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags need to be set via ALTER after creation
        result = sql
        tags = operation.payload.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER SCHEMA {catalog_esc}.{schema_esc} SET TAGS ({tag_entries})"

        return result

    def _squash_managed_location_from_modify_ops(
        self, batch_info: BatchInfo, update_op_type: str
    ) -> str | None:
        """Return managed location from create_op or first matching modify_op."""
        value = (
            batch_info.create_op.payload.get("managedLocationName")
            if batch_info.create_op
            else None
        )
        for operation in batch_info.modify_ops:
            if operation.op == update_op_type and "managedLocationName" in operation.payload:
                return operation.payload.get("managedLocationName")
        return value

    def _squash_comment_from_modify_ops(
        self, batch_info: BatchInfo, update_op_type: str
    ) -> str | None:
        """Return comment from create_op or first matching modify_op."""
        value = batch_info.create_op.payload.get("comment") if batch_info.create_op else None
        for operation in batch_info.modify_ops:
            if operation.op == update_op_type and "comment" in operation.payload:
                return operation.payload.get("comment")
        return value

    def _squash_tags_from_modify_ops(
        self, batch_info: BatchInfo, update_op_type: str
    ) -> dict[str, str] | None:
        """Return tags from create_op or first matching modify_op."""
        value = batch_info.create_op.payload.get("tags") if batch_info.create_op else None
        for operation in batch_info.modify_ops:
            if operation.op == update_op_type and "tags" in operation.payload:
                return operation.payload.get("tags")
        return value

    def _generate_create_catalog_batched(self, _object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE CATALOG with batched updates (e.g., managed location from update_catalog).

        Squashes CREATE CATALOG + UPDATE_CATALOG into single CREATE statement.
        """
        if not batch_info.create_op:
            return ""
        create_op = batch_info.create_op
        name = self.id_name_map.get(create_op.target, create_op.payload["name"])
        managed_location_name = self._squash_managed_location_from_modify_ops(
            batch_info, "unity.update_catalog"
        )
        comment = self._squash_comment_from_modify_ops(batch_info, "unity.update_catalog")
        tags = self._squash_tags_from_modify_ops(batch_info, "unity.update_catalog")

        sql = f"CREATE CATALOG IF NOT EXISTS {self.escape_identifier(name)}"
        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"
        result = sql
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"
        return result

    def _generate_create_schema_batched(self, _object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE SCHEMA with batched updates (e.g., managed location from update_schema).

        Squashes CREATE SCHEMA + UPDATE_SCHEMA into single CREATE statement.
        """
        if not batch_info.create_op:
            return ""
        create_op = batch_info.create_op
        catalog_name = self.id_name_map.get(create_op.payload["catalogId"], "unknown")
        schema_name = create_op.payload["name"]
        managed_location_name = self._squash_managed_location_from_modify_ops(
            batch_info, "unity.update_schema"
        )
        comment = self._squash_comment_from_modify_ops(batch_info, "unity.update_schema")
        tags = self._squash_tags_from_modify_ops(batch_info, "unity.update_schema")

        catalog_esc = self.escape_identifier(catalog_name)
        schema_esc = self.escape_identifier(schema_name)
        sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_esc}.{schema_esc}"
        if managed_location_name:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                sql += f" MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"
        result = sql
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER SCHEMA {catalog_esc}.{schema_esc} SET TAGS ({tag_entries})"
        return result

    def _rename_schema(self, operation: Operation) -> str:
        old_name = operation.payload["oldName"]
        new_name = operation.payload["newName"]
        # Get catalog name from idNameMap (catalog doesn't change during schema rename)
        fqn = self.id_name_map.get(operation.target, "unknown.unknown")
        catalog_name = fqn.split(".")[0]

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, old_name)
        new_esc = self._build_fqn(catalog_name, new_name)

        return f"ALTER SCHEMA {old_esc} RENAME TO {new_esc}"

    def _update_schema(self, operation: Operation) -> str:
        """Update schema properties (managed location, comment, tags)"""
        fqn = self.id_name_map.get(operation.target, "unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        fqn_esc = self._build_fqn(catalog_name, schema_name)
        statements = []

        # Handle managed location
        managed_location_name = operation.payload.get("managedLocationName")
        if managed_location_name is not None:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                statements.append(
                    f"ALTER SCHEMA {fqn_esc} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )

        # Handle comment
        comment = operation.payload.get("comment")
        if comment is not None:
            if comment:
                statements.append(
                    f"ALTER SCHEMA {fqn_esc} SET COMMENT '{self.escape_string(comment)}'"
                )
            else:
                statements.append(f"ALTER SCHEMA {fqn_esc} UNSET COMMENT")

        # Handle tags
        tags = operation.payload.get("tags")
        if tags is not None and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            statements.append(f"ALTER SCHEMA {fqn_esc} SET TAGS ({tag_entries})")

        if statements:
            return ";\n".join(statements)
        return "-- Warning: No updates specified for schema"

    def _drop_schema(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.target, "unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"

        # Use _build_fqn for consistent formatting
        fqn_esc = self._build_fqn(catalog_name, schema_name)

        # Use CASCADE to ensure schema drops even if it contains objects
        # This handles drift scenarios where schema may have tables we don't track
        # CASCADE is safe for rollback: we're reverting to a previous known state
        return f"DROP SCHEMA IF EXISTS {fqn_esc} CASCADE"

    # Table operations
    def _add_table(self, operation: Operation) -> str:
        schema_fqn = self.id_name_map.get(operation.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        table_name = operation.payload["name"]
        table_format = operation.payload["format"].upper()
        is_external = operation.payload.get("external", False)

        # Resolve location
        location_info = (
            self._resolve_table_location(
                operation.payload.get("externalLocationName"), operation.payload.get("path")
            )
            if is_external
            else None
        )

        partition_cols = operation.payload.get("partitionColumns", [])
        cluster_cols = operation.payload.get("clusterColumns", [])

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

        # Add table comment if present
        comment = operation.payload.get("comment", "")
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""

        # Create empty table (columns added via add_column ops)
        using_clause = (
            f"USING {table_format}{comment_clause}{partition_clause}"
            f"{cluster_clause}{location_clause}"
        )
        return f"{warnings}CREATE {external_keyword}TABLE IF NOT EXISTS {fqn_esc} () {using_clause}"

    def _rename_table(self, operation: Operation) -> str:
        old_name = operation.payload["oldName"]
        new_name = operation.payload["newName"]
        # Get catalog and schema names from idNameMap (they don't change during table rename)
        fqn = self.id_name_map.get(operation.target, "unknown.unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, schema_name, old_name)
        new_esc = self._build_fqn(catalog_name, schema_name, new_name)

        return f"ALTER TABLE {old_esc} RENAME TO {new_esc}"

    def _drop_table(self, operation: Operation) -> str:
        # Get table FQN from id_name_map
        # SQL generator MUST be created with state containing objects to be dropped
        # (e.g., use current_state during rollback, not target_state)
        table_fqn = self.id_name_map.get(operation.target)

        if not table_fqn or "." not in table_fqn:
            # This should never happen if SQL generator is used correctly
            # If it does, it indicates a bug in the calling code
            raise ValueError(
                f"Cannot generate DROP TABLE for {operation.target}: table not found in state.\n"
                f"Hint: SQL generator must be created with state containing objects to be dropped.\n"
                f"For rollback operations, use current_state (not target_state)."
            )

        fqn_esc = self._build_fqn(*table_fqn.split("."))
        return f"DROP TABLE IF EXISTS {fqn_esc}"

    def _set_table_comment(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        comment = self.escape_string(operation.payload["comment"])
        return f"COMMENT ON TABLE {fqn_esc} IS '{comment}'"

    def _set_table_property(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        key = operation.payload["key"]
        value = operation.payload["value"]
        return f"ALTER TABLE {fqn_esc} SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_table_property(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        key = operation.payload["key"]
        return f"ALTER TABLE {fqn_esc} UNSET TBLPROPERTIES ('{key}')"

    @staticmethod
    def _aggregate_table_tag_ops(
        table_tag_ops: list[Operation],
    ) -> tuple[dict[str, dict[str, str]], dict[str, set[str]]]:
        """Aggregate set/unset table tag ops by table_id. Returns (set_by_table, unset_by_table)."""
        set_by_table: dict[str, dict[str, str]] = {}
        unset_by_table: dict[str, set[str]] = {}
        for operation in table_tag_ops:
            table_id = operation.payload.get("tableId", "")
            op_type = operation.op.replace("unity.", "")
            if op_type == "set_table_tag":
                tag_name = operation.payload["tagName"]
                tag_value = operation.payload["tagValue"]
                set_by_table.setdefault(table_id, {})[tag_name] = tag_value
                if table_id in unset_by_table:
                    unset_by_table[table_id].discard(tag_name)
            if op_type == "unset_table_tag":
                tag_name = operation.payload["tagName"]
                unset_by_table.setdefault(table_id, set()).add(tag_name)
                if table_id in set_by_table:
                    set_by_table[table_id].pop(tag_name, None)
        return set_by_table, unset_by_table

    def _emit_table_tag_alter_sql(
        self,
        set_by_table: dict[str, dict[str, str]],
        unset_by_table: dict[str, set[str]],
    ) -> list[str]:
        """Emit ALTER TABLE ... SET TAGS / UNSET TAGS statements from aggregated tag maps."""
        parts: list[str] = []
        for tid in set(set_by_table) | set(unset_by_table):
            table_fqn = self.id_name_map.get(tid, "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            if tid in unset_by_table and unset_by_table[tid]:
                tag_list = ", ".join(f"'{self.escape_string(t)}'" for t in unset_by_table[tid])
                parts.append(f"ALTER TABLE {table_esc} UNSET TAGS ({tag_list})")
            if tid in set_by_table and set_by_table[tid]:
                tag_list = ", ".join(
                    f"'{self.escape_string(k)}' = '{v}'" for k, v in set_by_table[tid].items()
                )
                parts.append(f"ALTER TABLE {table_esc} SET TAGS ({tag_list})")
        return parts

    def _generate_batched_table_tag_sql(self, table_tag_ops: list[Operation]) -> str:
        """Batch table tag ops by table_id: one SET TAGS and one UNSET TAGS per table."""
        if not table_tag_ops:
            return ""
        set_by_table, unset_by_table = self._aggregate_table_tag_ops(table_tag_ops)
        # Escape tag values when emitting (aggregate stores raw)
        set_escaped: dict[str, dict[str, str]] = {}
        for tid, tags in set_by_table.items():
            set_escaped[tid] = {k: self.escape_string(v) for k, v in tags.items()}
        parts = self._emit_table_tag_alter_sql(set_escaped, unset_by_table)
        return ";\n".join(parts) if parts else ""

    def _set_table_tag(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        tag_name = operation.payload["tagName"]
        tag_value = self.escape_string(operation.payload["tagValue"])
        return f"ALTER TABLE {fqn_esc} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_table_tag(self, operation: Operation) -> str:
        fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        fqn_esc = self._build_fqn(*fqn.split("."))
        tag_name = operation.payload["tagName"]
        return f"ALTER TABLE {fqn_esc} UNSET TAGS ('{tag_name}')"

    # View operations
    def _build_name_to_fqn_map(self) -> dict[str, str]:
        """Build mapping from unqualified/partially qualified names to FQN from id_name_map."""
        name_to_fqn: dict[str, str] = {}
        for _oid, fqn in self.id_name_map.items():
            if not fqn or "." not in fqn:
                continue
            parts = fqn.split(".")
            if len(parts) == 3:
                _cat, schema, name = parts
                name_to_fqn[name] = fqn
                name_to_fqn[f"{schema}.{name}"] = fqn
        return name_to_fqn

    def _apply_fqn_to_parsed(self, parsed: Any, name_to_fqn: dict[str, str]) -> None:
        """Mutate parsed SQL tree: qualify each Table node using name_to_fqn or catalog mapping."""
        for table_node in parsed.find_all(exp.Table):
            current_ref_parts = []
            if table_node.catalog:
                current_ref_parts.append(table_node.catalog)
            if table_node.db:
                current_ref_parts.append(table_node.db)
            if table_node.name:
                current_ref_parts.append(table_node.name)
            current_ref = ".".join(current_ref_parts)

            if current_ref in name_to_fqn:
                fqn = name_to_fqn[current_ref]
                parts = fqn.split(".")
                if len(parts) == 3:
                    table_node.set("catalog", exp.to_identifier(parts[0], quoted=True))
                    table_node.set("db", exp.to_identifier(parts[1], quoted=True))
                    table_node.set("this", exp.to_identifier(parts[2], quoted=True))
            elif table_node.catalog:
                logical_catalog = table_node.catalog
                if logical_catalog in self.catalog_name_mapping:
                    physical = self.catalog_name_mapping[logical_catalog]
                    table_node.set("catalog", exp.to_identifier(physical, quoted=True))
                else:
                    table_node.set("catalog", exp.to_identifier(logical_catalog, quoted=True))
                if table_node.db:
                    table_node.set("db", exp.to_identifier(table_node.db, quoted=True))
                if table_node.name:
                    table_node.set("this", exp.to_identifier(table_node.name, quoted=True))

    def _qualify_view_definition(
        self, definition: str, _extracted_deps: dict[str, list[str]]
    ) -> str:
        """
        Qualify unqualified table/view references in view SQL with fully-qualified names.

        This ensures views work correctly when the current catalog/schema context
        is not set (e.g. SQL Statement Execution API).
        """
        try:
            parsed = sqlglot.parse_one(definition, dialect="databricks")
        except Exception:
            return definition
        name_to_fqn = self._build_name_to_fqn_map()
        self._apply_fqn_to_parsed(parsed, name_to_fqn)
        return parsed.sql(dialect="databricks", pretty=True)

    def _add_view(self, operation: Operation) -> str:
        """Generate CREATE VIEW statement"""
        # Get schema FQN and extract catalog/schema names
        schema_fqn = self.id_name_map.get(operation.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        view_name = operation.payload["name"]

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)

        # Build fully qualified view name
        view_esc = self._build_fqn(catalog_name, schema_name, view_name)
        definition = operation.payload.get("definition", "")
        comment = operation.payload.get("comment")

        # Always qualify table/view references in the definition
        # (even if extractedDependencies is missing, we use all objects from id_name_map)
        extracted_deps = operation.payload.get("extractedDependencies", {})
        definition = self._qualify_view_definition(definition, extracted_deps)

        # Build CREATE VIEW statement
        sql = f"CREATE VIEW IF NOT EXISTS {view_esc}"

        # Add comment if provided
        if comment:
            sql += f" COMMENT '{comment}'"

        # Add AS clause
        sql += f" AS\n{definition}"

        # Add dependency comment if extracted dependencies exist
        tables = extracted_deps.get("tables", [])
        views = extracted_deps.get("views", [])

        dep_list = []
        if tables:
            dep_list.extend(tables)
        if views:
            dep_list.extend(views)

        if dep_list:
            deps_str = ", ".join(dep_list)
            sql = f"-- View depends on: {deps_str}\n{sql}"

        return sql

    def _generate_create_or_replace_view(self, create_op: Operation, update_op: Operation) -> str:
        """
        Generate CREATE OR REPLACE VIEW statement by batching create + update.

        Uses the final definition from update_op and dependencies from update_op.
        This optimization squashes CREATE + UPDATE_VIEW into a single statement.
        """
        # Get schema FQN and extract catalog/schema names
        schema_fqn = self.id_name_map.get(create_op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        view_name = create_op.payload["name"]

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)

        # Build fully qualified view name
        view_esc = self._build_fqn(catalog_name, schema_name, view_name)

        # Use updated definition from update_op
        definition = update_op.payload.get("definition", "")

        # Always qualify table/view references in the definition
        extracted_deps = update_op.payload.get("extractedDependencies", {})
        definition = self._qualify_view_definition(definition, extracted_deps)

        # Use comment from create_op (if any)
        comment = create_op.payload.get("comment")

        # Build CREATE OR REPLACE VIEW statement
        sql = f"CREATE OR REPLACE VIEW {view_esc}"

        # Add comment if provided
        if comment:
            sql += f" COMMENT '{comment}'"

        # Add AS clause
        sql += f" AS\n{definition}"

        # Add dependency comment from update_op (most recent dependencies)
        tables = extracted_deps.get("tables", [])
        views = extracted_deps.get("views", [])

        dep_list = []
        if tables:
            dep_list.extend(tables)
        if views:
            dep_list.extend(views)

        if dep_list:
            deps_str = ", ".join(dep_list)
            sql = f"-- View depends on: {deps_str}\n{sql}"

        return sql

    def _rename_view(self, operation: Operation) -> str:
        """Generate ALTER VIEW RENAME statement"""
        old_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = old_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        old_esc = self._build_fqn(*parts)

        # Build new FQN with new name
        parts[-1] = operation.payload["newName"]
        new_esc = self._build_fqn(*parts)

        return f"ALTER VIEW {old_esc} RENAME TO {new_esc}"

    def _resolve_fqn_for_drop(self, operation: Operation) -> str:
        """
        Resolve escaped FQN for DROP of a schema-level object.
        Uses id_name_map when available; otherwise builds from payload (name, catalogId, schemaId).
        """
        raw = self.id_name_map.get(operation.target)
        if not raw or raw == "unknown":
            name = operation.payload.get("name")
            catalog_id = operation.payload.get("catalogId")
            schema_id = operation.payload.get("schemaId")
            if not name:
                return self._build_fqn("unknown", "unknown", "unknown")
            schema_fqn = self.id_name_map.get(schema_id or "", "unknown.unknown")
            catalog_name = self.id_name_map.get(catalog_id or "", "unknown")
            parts_schema = schema_fqn.split(".", 1)
            cat_from_schema = parts_schema[0] if parts_schema else catalog_name
            catalog_physical = self.catalog_name_mapping.get(cat_from_schema, cat_from_schema)
            schema_part = parts_schema[1] if len(parts_schema) == 2 else "unknown"
            raw = f"{catalog_physical}.{schema_part}.{name}"
        parts = raw.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        return self._build_fqn(*parts)

    def _drop_view(self, operation: Operation) -> str:
        """Generate DROP VIEW statement"""
        view_esc = self._resolve_fqn_for_drop(operation)
        return f"DROP VIEW IF EXISTS {view_esc}"

    def _update_view(self, operation: Operation) -> str:
        """Generate CREATE OR REPLACE VIEW statement to update definition"""
        view_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)

        # Reconstruct FQN with physical catalog name
        parts[0] = catalog_name
        view_esc = self._build_fqn(*parts)
        definition = operation.payload.get("definition", "")

        # Always qualify table/view references in the definition
        extracted_deps = operation.payload.get("extractedDependencies", {})
        definition = self._qualify_view_definition(definition, extracted_deps)

        # Use CREATE OR REPLACE for updates
        sql = f"CREATE OR REPLACE VIEW {view_esc} AS\n{definition}"

        # Add dependency comment if provided
        if extracted_deps:
            tables = extracted_deps.get("tables", [])
            views = extracted_deps.get("views", [])

            dep_list = []
            if tables:
                dep_list.extend(tables)
            if views:
                dep_list.extend(views)

            if dep_list:
                deps_str = ", ".join(dep_list)
                sql = f"-- View depends on: {deps_str}\n{sql}"

        return sql

    def _set_view_comment(self, operation: Operation) -> str:
        """Generate ALTER VIEW SET TBLPROPERTIES for comment"""
        view_fqn = self.id_name_map.get(operation.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        comment = operation.payload["comment"].replace("'", "\\'")
        return f"COMMENT ON VIEW {view_esc} IS '{comment}'"

    def _set_view_property(self, operation: Operation) -> str:
        """Generate ALTER VIEW SET TBLPROPERTIES"""
        view_fqn = self.id_name_map.get(operation.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        key = operation.payload["key"]
        value = operation.payload["value"].replace("'", "\\'")
        return f"ALTER VIEW {view_esc} SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_view_property(self, operation: Operation) -> str:
        """Generate ALTER VIEW UNSET TBLPROPERTIES"""
        view_fqn = self.id_name_map.get(operation.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        key = operation.payload["key"]
        return f"ALTER VIEW {view_esc} UNSET TBLPROPERTIES ('{key}')"

    # Volume operations
    def _add_volume(self, operation: Operation) -> str:
        """Generate CREATE [EXTERNAL] VOLUME statement"""
        schema_fqn = self.id_name_map.get(operation.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        vol_name = operation.payload["name"]
        vol_esc = self._build_fqn(*parts, vol_name)
        volume_type = operation.payload.get("volumeType", "managed")
        external = volume_type == "external"
        sql = "CREATE EXTERNAL VOLUME" if external else "CREATE VOLUME"
        sql += f" IF NOT EXISTS {vol_esc}"
        if operation.payload.get("location") and external:
            sql += f" LOCATION '{self.escape_string(operation.payload['location'])}'"
        if operation.payload.get("comment"):
            sql += f" COMMENT '{self.escape_string(operation.payload['comment'])}'"
        return sql

    def _rename_volume(self, operation: Operation) -> str:
        """Generate ALTER VOLUME RENAME statement"""
        old_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = operation.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER VOLUME {old_esc} RENAME TO {new_esc}"

    def _update_volume(self, operation: Operation) -> str:
        """Generate ALTER VOLUME SET COMMENT / LOCATION statements"""
        vol_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = vol_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        vol_esc = self._build_fqn(*parts)
        stmts = []
        if "comment" in operation.payload:
            comment_val = operation.payload.get("comment") or ""
            stmts.append(f"ALTER VOLUME {vol_esc} SET COMMENT '{self.escape_string(comment_val)}'")
        if "location" in operation.payload and operation.payload.get("location"):
            stmts.append(
                f"ALTER VOLUME {vol_esc} SET LOCATION '{self.escape_string(operation.payload['location'])}'"
            )
        return ";\n".join(stmts) if stmts else "-- No volume updates specified"

    def _drop_volume(self, operation: Operation) -> str:
        """Generate DROP VOLUME statement"""
        vol_esc = self._resolve_fqn_for_drop(operation)
        return f"DROP VOLUME IF EXISTS {vol_esc}"

    # Function operations
    def _add_function(self, operation: Operation) -> str:
        """Generate CREATE OR REPLACE FUNCTION statement"""
        schema_fqn = self.id_name_map.get(operation.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_name = operation.payload["name"]
        func_esc = self._build_fqn(*parts, func_name)
        language = (operation.payload.get("language") or "SQL").upper()
        return_type = operation.payload.get("returnType") or "STRING"
        body = operation.payload.get("body") or "NULL"
        params = operation.payload.get("parameters") or []
        param_str = ", ".join(
            f"{self.escape_identifier(p.get('name', 'x'))} {p.get('dataType', 'STRING')}"
            for p in params
            if isinstance(p, dict)
        )
        if language == "SQL":
            sql = f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE SQL RETURN ({body});"
        else:
            sql = f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE PYTHON AS $$ {body} $$;"
        return sql

    def _rename_function(self, operation: Operation) -> str:
        """Generate ALTER FUNCTION RENAME statement (Databricks: recreate or ALTER if supported)"""
        old_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = operation.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER FUNCTION {old_esc} RENAME TO {new_esc}"

    def _update_function(self, operation: Operation) -> str:
        """Generate CREATE OR REPLACE FUNCTION with updated body/return type from payload."""
        func_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = func_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_esc = self._build_fqn(*parts)
        language = (operation.payload.get("language") or "SQL").upper()
        return_type = operation.payload.get("returnType") or "STRING"
        body = operation.payload.get("body") or "NULL"
        params = operation.payload.get("parameters") or []
        param_str = ", ".join(
            f"{self.escape_identifier(p.get('name', 'x'))} {p.get('dataType', 'STRING')}"
            for p in params
            if isinstance(p, dict)
        )
        if language == "SQL":
            return f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE SQL RETURN ({body});"
        return f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE PYTHON AS $$ {body} $$;"

    def _drop_function(self, operation: Operation) -> str:
        """Generate DROP FUNCTION statement"""
        func_esc = self._resolve_fqn_for_drop(operation)
        return f"DROP FUNCTION IF EXISTS {func_esc}"

    def _set_function_comment(self, operation: Operation) -> str:
        """Generate COMMENT ON FUNCTION statement"""
        func_id = operation.payload.get("functionId", operation.target)
        func_fqn = self.id_name_map.get(func_id, "unknown")
        parts = func_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_esc = self._build_fqn(*parts)
        comment = (operation.payload.get("comment") or "").replace("'", "\\'")
        return f"COMMENT ON FUNCTION {func_esc} IS '{comment}'"

    # Materialized view operations
    def _add_materialized_view(self, operation: Operation) -> str:
        """Generate CREATE MATERIALIZED VIEW statement"""
        schema_fqn = self.id_name_map.get(operation.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_name = operation.payload["name"]
        mv_esc = self._build_fqn(*parts, mv_name)
        definition = operation.payload.get("definition") or "SELECT 1"
        extracted_deps = operation.payload.get("extractedDependencies", {})
        definition = self._qualify_view_definition(definition, extracted_deps)
        comment_clause = ""
        if operation.payload.get("comment"):
            comment_clause = f" COMMENT '{self.escape_string(operation.payload['comment'])}'"
        sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_esc}{comment_clause} AS\n{definition}"
        schedule = operation.payload.get("refreshSchedule")
        if schedule:
            sql += f"\nSCHEDULE {schedule}"
        return sql

    def _rename_materialized_view(self, operation: Operation) -> str:
        """Generate ALTER MATERIALIZED VIEW RENAME (or DROP + CREATE if needed)"""
        old_fqn = self.id_name_map.get(operation.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = operation.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER MATERIALIZED VIEW {old_esc} RENAME TO {new_esc}"

    def _update_materialized_view(self, operation: Operation) -> str:
        """Generate CREATE OR REPLACE MATERIALIZED VIEW or ALTER for schedule/comment"""
        mv_id = operation.target
        definition = operation.payload.get("definition")
        if definition is not None:
            mv_fqn = self.id_name_map.get(mv_id, "unknown")
            parts = mv_fqn.split(".")
            catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
            parts[0] = catalog_physical
            mv_esc = self._build_fqn(*parts)
            extracted_deps = operation.payload.get("extractedDependencies", {})
            definition = self._qualify_view_definition(definition, extracted_deps)
            comment_clause = ""
            if operation.payload.get("comment"):
                comment_clause = f" COMMENT '{self.escape_string(operation.payload['comment'])}'"
            sql = f"CREATE OR REPLACE MATERIALIZED VIEW {mv_esc}{comment_clause} AS\n{definition}"
            if operation.payload.get("refreshSchedule"):
                sql += f"\nSCHEDULE {operation.payload['refreshSchedule']}"
            return sql
        stmts = []
        mv_fqn = self.id_name_map.get(mv_id, "unknown")
        parts = mv_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_esc = self._build_fqn(*parts)
        if "refreshSchedule" in operation.payload:
            refresh_schedule = operation.payload.get("refreshSchedule")
            if refresh_schedule:
                stmts.append(f"ALTER MATERIALIZED VIEW {mv_esc} SET SCHEDULE {refresh_schedule}")
            else:
                stmts.append(f"ALTER MATERIALIZED VIEW {mv_esc} UNSET SCHEDULE")
        if "comment" in operation.payload:
            comment_val = operation.payload.get("comment") or ""
            stmts.append(
                f"COMMENT ON MATERIALIZED VIEW {mv_esc} IS '{self.escape_string(comment_val)}'"
            )
        return ";\n".join(stmts) if stmts else "-- No materialized view updates specified"

    def _drop_materialized_view(self, operation: Operation) -> str:
        """Generate DROP MATERIALIZED VIEW statement (Databricks requires this, not DROP VIEW)"""
        mv_esc = self._resolve_fqn_for_drop(operation)
        return f"DROP MATERIALIZED VIEW IF EXISTS {mv_esc}"

    def _set_materialized_view_comment(self, operation: Operation) -> str:
        """Generate COMMENT ON MATERIALIZED VIEW statement"""
        mv_id = operation.payload.get("materializedViewId", operation.target)
        mv_fqn = self.id_name_map.get(mv_id, "unknown")
        parts = mv_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_esc = self._build_fqn(*parts)
        comment = (operation.payload.get("comment") or "").replace("'", "\\'")
        return f"COMMENT ON MATERIALIZED VIEW {mv_esc} IS '{comment}'"

    # Column operations
    def _add_column(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(
            operation.payload.get("tableId", operation.target), "unknown"
        )
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = operation.payload.get("name", operation.target)
        col_type = operation.payload.get("type", "STRING")
        comment = operation.payload.get("comment", "")
        nullable = operation.payload.get("nullable", True)

        # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMN for Delta tables
        # New columns added to existing tables must be nullable initially
        # Then we use ALTER COLUMN SET NOT NULL as a second statement
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
        col_esc = self.escape_identifier(col_name)

        sql = f"ALTER TABLE {table_esc} ADD COLUMN {col_esc} {col_type}{comment_clause}"

        # If column should be NOT NULL, add a second statement to set it
        if not nullable:
            sql += f";\nALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET NOT NULL"

        return sql

    def _rename_column(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        old_name = operation.payload["oldName"]
        new_name = operation.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER TABLE {table_esc} RENAME COLUMN {old_esc} TO {new_esc}"

    def _drop_column(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (for dropped columns not in current state)
        col_name = operation.payload.get("name", self.id_name_map.get(operation.target, "unknown"))
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} DROP COLUMN {col_esc}"

    def _reorder_columns(self, _operation: Operation) -> str:
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

        for operation in ops:
            op_type = operation.op.replace("unity.", "")

            if op_type == "reorder_columns":
                table_id = operation.payload["tableId"]
                desired_order = operation.payload["order"]

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
                reorder_batches[table_id]["op_ids"].append(operation.id)

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

    def _get_table_id_for_operation(self, operation: Operation) -> str | None:
        """Return table_id for a table-related operation, or None."""
        op_type = operation.op.replace("unity.", "")
        if op_type in {"add_table", "rename_table", "drop_table", "set_table_comment"}:
            return operation.target
        if op_type in {
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
        }:
            return operation.payload.get("tableId")
        if op_type in {
            "add_constraint",
            "drop_constraint",
            "add_row_filter",
            "update_row_filter",
            "remove_row_filter",
            "add_column_mask",
            "update_column_mask",
            "remove_column_mask",
        }:
            return operation.payload.get("tableId")
        return None

    def _categorize_table_op(
        self, op_type: str, operation: Operation, batch: dict[str, Any]
    ) -> None:
        """Place operation into the correct list in batch by op_type."""
        if op_type == "add_table":
            batch["is_new_table"] = True
            batch["table_op"] = operation
        if op_type == "add_column":
            batch["column_ops"].append(operation)
        elif op_type in {"set_table_property", "unset_table_property"}:
            batch["property_ops"].append(operation)
        elif op_type in {"set_table_tag", "unset_table_tag"}:
            batch["other_ops"].append(operation)
        if op_type == "set_table_comment":
            batch["other_ops"].append(operation)
        if op_type == "reorder_columns":
            batch["reorder_ops"].append(operation)
        elif op_type in {"add_constraint", "drop_constraint"}:
            batch["constraint_ops"].append(operation)
        elif op_type in {
            "add_row_filter",
            "update_row_filter",
            "remove_row_filter",
            "add_column_mask",
            "update_column_mask",
            "remove_column_mask",
        }:
            batch["governance_ops"].append(operation)
        else:
            batch["other_ops"].append(operation)

    def _batch_table_operations(self, ops: list[Operation]) -> dict[str, Any]:
        """
        Batch operations by table to generate optimal DDL.

        Groups table-related operations to generate complete CREATE TABLE statements
        for new tables or efficient ALTER statements for existing tables.

        Returns dict mapping table_id to batch info.
        """
        table_batches: dict[str, Any] = {}
        for operation in ops:
            table_id = self._get_table_id_for_operation(operation)
            if not table_id:
                continue
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
            batch["op_ids"].append(operation.id)
            batch["operation_types"].append(operation.op)
            self._categorize_table_op(operation.op.replace("unity.", ""), operation, batch)
        return table_batches

    def _generate_table_sql_with_mapping(
        self, table_id: str, batch_info: BatchInfo
    ) -> list[tuple[str, list[str]]]:
        """Generate SQL for table operations with explicit operation mapping.

        Returns list of (sql, operation_ids) tuples since table operations
        can produce multiple statements (CREATE TABLE + ALTER statements).
        """
        # Reuse existing logic but track which operations produce which statements
        sql = self._generate_optimized_table_sql(table_id, batch_info)

        # Parse the combined SQL into individual statements
        # and map them to operations
        statements = []

        # Split by semicolon to get individual statements
        raw_stmts = sql.split(";")

        for stmt in raw_stmts:
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue

            # For now, attribute all statements in this batch to all operations in the batch
            # This is conservative but correct - all operations contributed to this batch
            statements.append((stmt, batch_info.op_ids))

        return statements

    def _generate_view_sql_with_mapping(
        self, view_id: str, batch_info: BatchInfo
    ) -> list[tuple[str, list[str]]]:
        """Generate SQL for view operations with explicit operation mapping.

        Returns list of (sql, operation_ids) tuples.

        Optimization: If there's a CREATE + UPDATE_VIEW in the same batch,
        squash them into a single CREATE OR REPLACE VIEW with the final definition.
        """
        statements = []

        # Check if we have both create and update_view operations
        has_create = batch_info.create_op is not None
        update_view_ops = [
            operation
            for operation in batch_info.modify_ops
            if operation.op.replace("unity.", "") == "update_view"
        ]

        # Optimization: Squash CREATE + UPDATE_VIEW into single statement
        if has_create and update_view_ops and batch_info.create_op:
            # Use the LAST update_view operation (most recent definition)
            final_update_op = update_view_ops[-1]

            # Generate CREATE OR REPLACE VIEW with final definition
            sql = self._generate_create_or_replace_view(batch_info.create_op, final_update_op)
            if sql:
                # Track all operation IDs (create + all updates)
                op_ids = [batch_info.create_op.id] + [operation.id for operation in update_view_ops]
                statements.append((sql, op_ids))

            # Process remaining modify operations (excluding update_view)
            remaining_ops = [
                operation
                for operation in batch_info.modify_ops
                if operation.op.replace("unity.", "") != "update_view"
            ]
        else:
            # No batching needed - process normally
            if batch_info.create_op:
                create_op = batch_info.create_op
                sql = self._add_view(create_op)
                if sql:
                    statements.append((sql, [create_op.id]))

            remaining_ops = batch_info.modify_ops

        # Process remaining modify operations (rename, drop, set properties, etc.)
        for operation in remaining_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [operation.id]))
            except Exception as e:
                statements.append(
                    (f"-- Error generating SQL for {operation.id}: {e}", [operation.id])
                )

        return statements

    def _generate_volume_sql_with_mapping(
        self, object_id: str, batch_info: BatchInfo
    ) -> list[tuple[str, list[str]]]:
        """Generate SQL for volume operations. Returns list of (sql, op_ids)."""
        statements = []
        if batch_info.create_op:
            sql = self._add_volume(batch_info.create_op)
            if sql and not sql.startswith("--"):
                statements.append((sql, [batch_info.create_op.id]))
        for operation in batch_info.modify_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [operation.id]))
            except Exception as e:
                statements.append(
                    (f"-- Error generating SQL for {operation.id}: {e}", [operation.id])
                )
        return statements

    def _generate_function_sql_with_mapping(
        self, object_id: str, batch_info: BatchInfo
    ) -> list[tuple[str, list[str]]]:
        """Generate SQL for function operations. Returns list of (sql, op_ids)."""
        statements = []
        if batch_info.create_op:
            sql = self._add_function(batch_info.create_op)
            if sql and not sql.startswith("--"):
                statements.append((sql, [batch_info.create_op.id]))
        for operation in batch_info.modify_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [operation.id]))
            except Exception as e:
                statements.append(
                    (f"-- Error generating SQL for {operation.id}: {e}", [operation.id])
                )
        return statements

    def _generate_materialized_view_sql_with_mapping(
        self, object_id: str, batch_info: BatchInfo
    ) -> list[tuple[str, list[str]]]:
        """Generate SQL for materialized view operations. Returns list of (sql, op_ids)."""
        statements = []
        if batch_info.create_op:
            sql = self._add_materialized_view(batch_info.create_op)
            if sql and not sql.startswith("--"):
                statements.append((sql, [batch_info.create_op.id]))
        for operation in batch_info.modify_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [operation.id]))
            except Exception as e:
                statements.append(
                    (f"-- Error generating SQL for {operation.id}: {e}", [operation.id])
                )
        return statements

    def _generate_optimized_table_sql(
        self, table_id: str, batch_info: dict[str, Any] | BatchInfo
    ) -> str:
        """Generate optimal SQL for table operations"""

        # Handle both dict (old table batching) and BatchInfo (new unified batching)
        if isinstance(batch_info, BatchInfo):
            # Convert BatchInfo to dict format expected by table methods
            # Categorize all modify operations
            column_ops = []
            tag_ops = []
            table_tag_ops = []
            other_ops = []
            for operation in batch_info.modify_ops:
                op_type = operation.op.replace("unity.", "")
                if op_type in {
                    "add_column",
                    "rename_column",
                    "drop_column",
                    "change_column_type",
                    "set_nullable",
                    "set_column_comment",
                }:
                    column_ops.append(operation)
                elif op_type in {"set_column_tag", "unset_column_tag"}:
                    tag_ops.append(operation)
                elif op_type in {"set_table_tag", "unset_table_tag"}:
                    table_tag_ops.append(operation)
                # Exclude operations that will be handled by dedicated lists below
                # (property_ops, constraint_ops, reorder_ops, governance_ops)
                elif op_type not in {
                    "set_table_property",
                    "unset_table_property",
                    "add_constraint",
                    "drop_constraint",
                    "reorder_columns",
                    "add_row_filter",
                    "update_row_filter",
                    "remove_row_filter",
                    "add_column_mask",
                    "update_column_mask",
                    "remove_column_mask",
                }:
                    other_ops.append(operation)

            # Sort constraint operations to ensure DROP comes before ADD
            constraint_ops = [
                operation for operation in batch_info.modify_ops if "constraint" in operation.op
            ]
            constraint_ops_sorted = sorted(
                constraint_ops,
                key=lambda operation: (
                    0 if operation.op == "unity.drop_constraint" else 1,
                    operation.ts,
                ),
            )

            batch_dict = {
                "is_new_table": batch_info.is_new,
                "table_op": batch_info.create_op,
                "column_ops": column_ops,
                "tag_ops": tag_ops,
                "table_tag_ops": table_tag_ops,
                "property_ops": [
                    operation for operation in batch_info.modify_ops if "property" in operation.op
                ],
                "constraint_ops": constraint_ops_sorted,  # Use sorted list with DROP before ADD
                "reorder_ops": [
                    operation for operation in batch_info.modify_ops if "reorder" in operation.op
                ],
                "governance_ops": [
                    operation
                    for operation in batch_info.modify_ops
                    if "filter" in operation.op or "mask" in operation.op
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

    def _append_post_create_alter_statements(
        self,
        batch_info: dict[str, Any],
        table_fqn: str,
        add_column_ops: list[Operation],
        column_ops: list[Operation],
        other_column_ops: list[Operation],
        constraint_ops: list[Operation],
        other_ops: list[Operation],
        statements: list[str],
    ) -> None:
        """Append ALTER statements after CREATE TABLE: id_name_map update, tags, constraints, governance, other."""
        table_operation = batch_info["table_op"]
        table_id_from_op = (
            table_operation.payload.get("tableId") or table_operation.target
            if table_operation
            else None
        )
        if table_id_from_op:
            self.id_name_map[table_id_from_op] = table_fqn
            for col_op in add_column_ops:
                col_id = col_op.target
                col_name = col_op.payload.get("name", col_id)
                self.id_name_map[col_id] = col_name

        tag_ops = batch_info.get("tag_ops") or [
            o
            for o in column_ops
            if o.op.endswith("set_column_tag") or o.op.endswith("unset_column_tag")
        ]
        batched_tag_sql = self._generate_batched_column_tag_sql(tag_ops)
        if batched_tag_sql:
            for stmt in batched_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        table_tag_ops = batch_info.get("table_tag_ops") or [
            o
            for o in batch_info.get("other_ops", [])
            if o.op.endswith("set_table_tag") or o.op.endswith("unset_table_tag")
        ]
        batched_table_tag_sql = self._generate_batched_table_tag_sql(table_tag_ops)
        if batched_table_tag_sql:
            for stmt in batched_table_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        for operation in other_column_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {operation.id}: {e}")

        for operation in constraint_ops:
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {operation.id}: {e}")

        for operation in batch_info.get("governance_ops", []):
            op_type = operation.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {operation.id}: {e}")

        for operation in other_ops:
            op_type = operation.op.replace("unity.", "")
            if op_type == "set_table_comment" or op_type in {"set_table_tag", "unset_table_tag"}:
                continue
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {operation.id}: {e}")

    def _generate_create_table_with_columns(self, table_id: str, batch_info: dict[str, Any]) -> str:
        """Generate complete CREATE TABLE with columns and ALTER statements for constraints/tags"""
        table_operation = batch_info["table_op"]
        column_ops = batch_info["column_ops"]
        property_ops = batch_info["property_ops"]
        constraint_ops = batch_info.get("constraint_ops", [])
        reorder_ops = batch_info.get("reorder_ops", [])
        other_ops = batch_info.get("other_ops", [])

        if not table_operation:
            return "-- Error: No table creation operation found"

        # Get table name and schema info
        table_name = table_operation.payload.get("name", "unknown")
        schema_id = table_operation.payload.get("schemaId")
        schema_fqn = (
            self.id_name_map.get(schema_id, "unknown.unknown") if schema_id else "unknown.unknown"
        )
        table_fqn = f"{schema_fqn}.{table_name}"
        table_esc = self._build_fqn(*table_fqn.split("."))

        # Separate add_column, tag ops, and other column operations
        add_column_ops = [
            operation for operation in column_ops if operation.op.endswith("add_column")
        ]
        other_column_ops = [
            operation
            for operation in column_ops
            if not operation.op.endswith("add_column")
            and not operation.op.endswith("set_column_tag")
            and not operation.op.endswith("unset_column_tag")
        ]

        # Build column definitions as a dictionary (by column ID - use operation.target for add_column)
        columns_dict = {}
        for col_operation in add_column_ops:
            col_id = (
                col_operation.target
            )  # Column ID is in operation.target for add_column operations
            col_name = self.escape_identifier(col_operation.payload.get("name", col_id))
            col_type = col_operation.payload.get("type", "STRING")
            nullable = "" if col_operation.payload.get("nullable", True) else " NOT NULL"
            comment = (
                f" COMMENT '{self.escape_string(col_operation.payload['comment'])}'"
                if col_operation.payload.get("comment")
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
            columns = [columns_dict[col_id] for col_id in columns_dict.keys()]

        # Build table format
        table_format = table_operation.payload.get("format", "DELTA").upper()
        is_external = table_operation.payload.get("external", False)

        # Resolve location for external tables
        location_info = (
            self._resolve_table_location(
                table_operation.payload.get("externalLocationName"),
                table_operation.payload.get("path"),
            )
            if is_external
            else None
        )

        partition_cols = table_operation.payload.get("partitionColumns", [])
        cluster_cols = table_operation.payload.get("clusterColumns", [])

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
        comment_value = table_operation.payload.get("comment")

        # Check if there's a set_table_comment operation
        for operation in other_ops:
            if operation.op.endswith("set_table_comment"):
                comment_value = operation.payload.get("comment")
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
            # Build using clause
            using_clause = (
                f"USING {table_format}{table_comment}{partition_clause}"
                f"{cluster_clause}{properties_sql}{location_clause}"
            )
            create_sql = f"""{warnings}CREATE {external_keyword}TABLE IF NOT EXISTS {table_esc} (
{columns_sql}
) {using_clause}"""
        else:
            # No columns yet - create empty table (fallback to original behavior)
            using_clause = (
                f"USING {table_format}{table_comment}{partition_clause}"
                f"{cluster_clause}{properties_sql}{location_clause}"
            )
            create_sql = (
                f"{warnings}"
                f"CREATE {external_keyword}TABLE IF NOT EXISTS {table_esc} () "
                f"{using_clause}"
            )

        statements = [create_sql]
        self._append_post_create_alter_statements(
            batch_info,
            table_fqn,
            add_column_ops,
            column_ops,
            other_column_ops,
            constraint_ops,
            other_ops,
            statements,
        )
        return ";\n".join(statements)

    def _alter_reorder_block(self, batch_info: dict[str, Any], actual_table_id: str) -> list[str]:
        """Return ALTER statements for column reordering."""
        out: list[str] = []
        if not batch_info["reorder_ops"]:
            return out
        last_reorder = batch_info["reorder_ops"][-1]
        original_order = last_reorder.payload.get("previousOrder")
        if not original_order:
            original_order = self._get_table_column_order(actual_table_id)
        final_order = last_reorder.payload["order"]
        reorder_sql = self._generate_optimized_reorder_sql(
            actual_table_id,
            original_order,
            final_order,
            [o.id for o in batch_info["reorder_ops"]],
        )
        if reorder_sql and not reorder_sql.startswith("--"):
            out.append(reorder_sql)
        return out

    def _alter_add_columns_block(self, batch_info: dict[str, Any]) -> list[str]:
        """Return ALTER statements for batched or single ADD COLUMN."""
        out: list[str] = []
        add_column_ops = [o for o in batch_info["column_ops"] if o.op.endswith("add_column")]
        if len(add_column_ops) > 1:
            table_fqn = self.id_name_map.get(add_column_ops[0].payload["tableId"], "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            column_defs = []
            not_null_columns = []
            for operation in add_column_ops:
                col_name = operation.payload.get("name", operation.target)
                col_type = operation.payload.get("type", "STRING")
                comment = operation.payload.get("comment", "")
                nullable = operation.payload.get("nullable", True)
                comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
                column_defs.append(
                    f"    {self.escape_identifier(col_name)} {col_type}{comment_clause}"
                )
                if not nullable:
                    not_null_columns.append(col_name)
            out.append(
                f"ALTER TABLE {table_esc}\nADD COLUMNS (\n" + ",\n".join(column_defs) + "\n)"
            )
            for col_name in not_null_columns:
                out.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {self.escape_identifier(col_name)} SET NOT NULL"
                )
        elif len(add_column_ops) == 1:
            try:
                sql = self._add_column(add_column_ops[0])
                if sql and not sql.startswith("--"):
                    out.append(sql)
            except Exception as e:
                out.append(f"-- Error generating SQL for {add_column_ops[0].id}: {e}")
        return out

    def _alter_drop_columns_block(self, batch_info: dict[str, Any]) -> list[str]:
        """Return ALTER statements for batched or single DROP COLUMN."""
        out: list[str] = []
        drop_column_ops = [o for o in batch_info["column_ops"] if o.op.endswith("drop_column")]
        if len(drop_column_ops) > 1:
            table_fqn = self.id_name_map.get(drop_column_ops[0].payload["tableId"], "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            column_names = [
                self.escape_identifier(
                    o.payload.get("name", self.id_name_map.get(o.target, "unknown"))
                )
                for o in drop_column_ops
            ]
            out.append(f"ALTER TABLE {table_esc}\nDROP COLUMNS (" + ", ".join(column_names) + ")")
        elif len(drop_column_ops) == 1:
            try:
                sql = self._drop_column(drop_column_ops[0])
                if sql and not sql.startswith("--"):
                    out.append(sql)
            except Exception as e:
                out.append(f"-- Error generating SQL for {drop_column_ops[0].id}: {e}")
        return out

    def _alter_tags_and_rest_block(self, batch_info: dict[str, Any]) -> list[str]:
        """Return ALTER statements for column tags, table tags, and other ops (property, constraint, governance)."""
        out: list[str] = []
        tag_ops = batch_info.get("tag_ops") or [
            o
            for o in batch_info["column_ops"]
            if o.op.endswith("set_column_tag") or o.op.endswith("unset_column_tag")
        ]
        for stmt in (self._generate_batched_column_tag_sql(tag_ops) or "").split(";\n"):
            if stmt.strip():
                out.append(stmt.strip())
        for stmt in (
            self._generate_batched_table_tag_sql(batch_info.get("table_tag_ops", [])) or ""
        ).split(";\n"):
            if stmt.strip():
                out.append(stmt.strip())
        other_column_ops = [
            o
            for o in batch_info["column_ops"]
            if not o.op.endswith("add_column")
            and not o.op.endswith("drop_column")
            and not o.op.endswith("set_column_tag")
            and not o.op.endswith("unset_column_tag")
        ]
        constraint_ops_sorted = sorted(
            batch_info["constraint_ops"],
            key=lambda o: (0 if o.op == "unity.drop_constraint" else 1, o.ts),
        )
        for operation in (
            other_column_ops
            + batch_info["property_ops"]
            + constraint_ops_sorted
            + batch_info["governance_ops"]
            + batch_info["other_ops"]
        ):
            op_type = operation.op.replace("unity.", "")
            if op_type == "reorder_columns" or op_type in {"set_table_tag", "unset_table_tag"}:
                continue
            try:
                sql = self._generate_sql_for_op_type(op_type, operation)
                if sql and not sql.startswith("--"):
                    out.append(sql)
            except Exception as e:
                out.append(f"-- Error generating SQL for {operation.id}: {e}")
        return out

    def _generate_alter_statements_for_table(
        self, table_id: str, batch_info: dict[str, Any]
    ) -> str:
        """Generate optimized ALTER statements for existing table modifications."""
        actual_table_id = table_id.removeprefix("table:")
        statements: list[str] = []
        statements.extend(self._alter_reorder_block(batch_info, actual_table_id))
        statements.extend(self._alter_add_columns_block(batch_info))
        statements.extend(self._alter_drop_columns_block(batch_info))
        statements.extend(self._alter_tags_and_rest_block(batch_info))
        return ";\n".join(statements) if statements else "-- No ALTER statements needed"

    def _change_column_type(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(operation.target, "unknown")
        new_type = operation.payload["newType"]
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} TYPE {new_type}"

    def _set_nullable(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(operation.target, "unknown")
        nullable = operation.payload["nullable"]
        col_esc = self.escape_identifier(col_name)

        if nullable:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} DROP NOT NULL"
        else:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET NOT NULL"

    def _set_column_comment(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(operation.target, "unknown")
        comment = self.escape_string(operation.payload["comment"])
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} COMMENT '{comment}'"

    @staticmethod
    def _aggregate_column_tag_ops(
        tag_ops: list[Operation],
    ) -> tuple[dict[str, dict[str, str]], dict[str, set[str]]]:
        """Aggregate set/unset column tag ops by (table_id, col_id). Returns (set_by_col, unset_by_col)."""
        set_by_col: dict[str, dict[str, str]] = {}
        unset_by_col: dict[str, set[str]] = {}
        for operation in tag_ops:
            table_id = operation.payload.get("tableId", "")
            col_id = operation.target
            key = f"{table_id}:{col_id}"
            op_type = operation.op.replace("unity.", "")
            if op_type == "set_column_tag":
                tag_name = operation.payload["tagName"]
                tag_value = operation.payload["tagValue"]
                set_by_col.setdefault(key, {})[tag_name] = tag_value
                if key in unset_by_col:
                    unset_by_col[key].discard(tag_name)
            if op_type == "unset_column_tag":
                tag_name = operation.payload["tagName"]
                unset_by_col.setdefault(key, set()).add(tag_name)
                if key in set_by_col:
                    set_by_col[key].pop(tag_name, None)
        return set_by_col, unset_by_col

    def _emit_column_tag_alter_sql(
        self,
        set_by_col: dict[str, dict[str, str]],
        unset_by_col: dict[str, set[str]],
    ) -> list[str]:
        """Emit ALTER TABLE ... ALTER COLUMN ... SET TAGS / UNSET TAGS from aggregated maps."""
        parts: list[str] = []
        for key in set(set_by_col) | set(unset_by_col):
            table_id, col_id = key.split(":", 1)
            table_fqn = self.id_name_map.get(table_id, "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            col_name = self.id_name_map.get(col_id, col_id)
            col_esc = self.escape_identifier(col_name)
            if key in unset_by_col and unset_by_col[key]:
                tag_list = ", ".join(f"'{self.escape_string(t)}'" for t in unset_by_col[key])
                parts.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} UNSET TAGS ({tag_list})"
                )
            if key in set_by_col and set_by_col[key]:
                tag_list = ", ".join(
                    f"'{self.escape_string(k)}' = '{v}'" for k, v in set_by_col[key].items()
                )
                parts.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET TAGS ({tag_list})"
                )
        return parts

    def _generate_batched_column_tag_sql(self, tag_ops: list[Operation]) -> str:
        """Batch column tag ops by (table_id, col_id): one SET TAGS and one UNSET TAGS per column."""
        if not tag_ops:
            return ""
        set_by_col, unset_by_col = self._aggregate_column_tag_ops(tag_ops)
        set_escaped: dict[str, dict[str, str]] = {
            k: {tk: self.escape_string(tv) for tk, tv in v.items()} for k, v in set_by_col.items()
        }
        parts = self._emit_column_tag_alter_sql(set_escaped, unset_by_col)
        return ";\n".join(parts) if parts else ""

    # Column tag operations
    def _set_column_tag(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (fallback for new columns) or id_name_map
        col_name = operation.payload.get("name", self.id_name_map.get(operation.target, "unknown"))
        tag_name = operation.payload["tagName"]
        tag_value = self.escape_string(operation.payload["tagValue"])
        col_esc = self.escape_identifier(col_name)
        sql = f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc}"
        return f"{sql} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_column_tag(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        # Get column name from payload (fallback for new columns) or id_name_map
        col_name = operation.payload.get("name", self.id_name_map.get(operation.target, "unknown"))
        tag_name = operation.payload["tagName"]
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} UNSET TAGS ('{tag_name}')"

    # Constraint operations
    def _constraint_options_suffix(self, operation: Operation) -> str:
        """Build constraint option suffix (NOT ENFORCED, RELY, DEFERRABLE, INITIALLY DEFERRED)."""
        options: list[str] = []
        if operation.payload.get("notEnforced"):
            options.append("NOT ENFORCED")
        if operation.payload.get("rely"):
            options.append("RELY")
        if operation.payload.get("deferrable"):
            options.append("DEFERRABLE")
        if operation.payload.get("initiallyDeferred"):
            options.append("INITIALLY DEFERRED")
        return " " + " ".join(options) if options else ""

    def _add_constraint(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        constraint_type = operation.payload["type"]
        constraint_name = operation.payload.get("name", "")
        columns = [self.id_name_map.get(cid, cid) for cid in operation.payload["columns"]]

        name_clause = (
            f"CONSTRAINT {self.escape_identifier(constraint_name)} " if constraint_name else ""
        )
        opts = self._constraint_options_suffix(operation)

        if constraint_type == "primary_key":
            # Databricks: TIMESERIES is per-column: PRIMARY KEY ( col1 [ TIMESERIES ] [, ...] )
            col_parts = []
            for i, c in enumerate(columns):
                esc = self.escape_identifier(c)
                if i == 0 and operation.payload.get("timeseries"):
                    col_parts.append(f"{esc} TIMESERIES")
                else:
                    col_parts.append(esc)
            cols = ", ".join(col_parts)
            return f"ALTER TABLE {table_esc} ADD {name_clause}PRIMARY KEY({cols}){opts}"

        elif constraint_type == "foreign_key":
            parent_table = self.id_name_map.get(operation.payload.get("parentTable", ""), "unknown")
            parent_esc = self._build_fqn(*parent_table.split("."))
            parent_columns = [
                self.id_name_map.get(cid, cid) for cid in operation.payload.get("parentColumns", [])
            ]
            cols = ", ".join(self.escape_identifier(c) for c in columns)
            parent_cols = ", ".join(self.escape_identifier(c) for c in parent_columns)
            return (
                f"ALTER TABLE {table_esc} ADD {name_clause}"
                f"FOREIGN KEY({cols}) REFERENCES {parent_esc}({parent_cols}){opts}"
            )

        elif constraint_type == "check":
            expression = operation.payload.get("expression", "TRUE")
            return f"ALTER TABLE {table_esc} ADD {name_clause}CHECK ({expression}){opts}"

        return ""

    def _drop_constraint(self, operation: Operation) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT SQL

        Gets the constraint name from the operation payload (preferred) or
        looks it up from the current state (fallback for backward compatibility).
        """
        table_id = operation.payload["tableId"]
        constraint_id = operation.target
        table_fqn = self.id_name_map.get(table_id, "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))

        # Get constraint name from payload (if provided) or look it up from state
        constraint_name = operation.payload.get("name")
        if not constraint_name:
            # Fallback: look up constraint name from state (for backward compatibility)
            constraint_name = self._find_constraint_name(table_id, constraint_id)

        if not constraint_name:
            return f"-- ERROR: Constraint {constraint_id} not found in table {table_id}"

        constraint_name_esc = self.escape_identifier(constraint_name)
        return f"ALTER TABLE {table_esc} DROP CONSTRAINT {constraint_name_esc}"

    def _find_constraint_name(self, table_id: str, constraint_id: str) -> str | None:
        """Find constraint name by ID in the current state

        Args:
            table_id: Table ID
            constraint_id: Constraint ID

        Returns:
            Constraint name or None if not found
        """
        # Handle both dict and Pydantic model state
        catalogs = (
            self.state.catalogs
            if hasattr(self.state, "catalogs")
            else self.state.get("catalogs", [])
        )

        for catalog in catalogs:
            schemas = catalog.schemas if hasattr(catalog, "schemas") else catalog.get("schemas", [])
            for schema in schemas:
                tables = schema.tables if hasattr(schema, "tables") else schema.get("tables", [])
                for table in tables:
                    table_id_check = table.id if hasattr(table, "id") else table.get("id")
                    if table_id_check == table_id:
                        constraints = (
                            table.constraints
                            if hasattr(table, "constraints")
                            else table.get("constraints", [])
                        )
                        for constraint in constraints:
                            constraint_id_check = (
                                constraint.id if hasattr(constraint, "id") else constraint.get("id")
                            )
                            if constraint_id_check == constraint_id:
                                name = (
                                    constraint.name
                                    if hasattr(constraint, "name")
                                    else constraint.get("name")
                                )
                                return str(name) if name else None
        return None

    # Row filter operations (Unity: ALTER TABLE ... SET ROW FILTER func ON (cols) | DROP ROW FILTER)
    def _add_row_filter(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        func_name = operation.payload.get("name", "row_filter")
        func_esc = (
            self._build_fqn(*func_name.split("."))
            if "." in func_name
            else self.escape_identifier(func_name)
        )
        column_names = operation.payload.get("columnNames") or []
        cols_sql = ", ".join(self.escape_identifier(c) for c in column_names)
        return f"ALTER TABLE {fqn_esc} SET ROW FILTER {func_esc} ON ({cols_sql})"

    def _update_row_filter(self, operation: Operation) -> str:
        return self._add_row_filter(operation)

    def _remove_row_filter(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        return f"ALTER TABLE {fqn_esc} DROP ROW FILTER"

    # Column mask operations (Unity: ALTER TABLE ... ALTER COLUMN col SET MASK func | DROP MASK)
    def _add_column_mask(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        col_name = self.id_name_map.get(
            operation.payload["columnId"], operation.payload.get("columnId", "unknown")
        )
        col_esc = self.escape_identifier(col_name)
        mask_func = operation.payload["maskFunction"]
        func_esc = (
            self._build_fqn(*mask_func.split("."))
            if "." in mask_func
            else self.escape_identifier(mask_func)
        )
        using = operation.payload.get("usingColumns")
        if using:
            using_sql = ", ".join(self.escape_identifier(c) for c in using)
            return f"ALTER TABLE {fqn_esc} ALTER COLUMN {col_esc} SET MASK {func_esc} USING COLUMNS ({using_sql})"
        return f"ALTER TABLE {fqn_esc} ALTER COLUMN {col_esc} SET MASK {func_esc}"

    def _update_column_mask(self, operation: Operation) -> str:
        return self._add_column_mask(operation)

    def _remove_column_mask(self, operation: Operation) -> str:
        table_fqn = self.id_name_map.get(operation.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        col_name = self.id_name_map.get(
            operation.payload["columnId"], operation.payload.get("columnId", "unknown")
        )
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {fqn_esc} ALTER COLUMN {col_esc} DROP MASK"

    # Grant operations (Unity: GRANT / REVOKE on CATALOG | SCHEMA | TABLE | VIEW)
    def _grant_object_kind(self, target_type: str) -> str:
        """Map targetType to SQL object kind (uppercase)."""
        return {
            "catalog": "CATALOG",
            "schema": "SCHEMA",
            "table": "TABLE",
            "view": "VIEW",
            "volume": "VOLUME",
            "function": "FUNCTION",
            "materialized_view": "VIEW",
        }.get(target_type, "TABLE")

    def _grant_fqn(self, target_type: str, target_id: str) -> str:
        """Resolve fully-qualified name for grant target from id_name_map, with fallback from state."""
        fqn = self.id_name_map.get(target_id, "")
        if fqn:
            parts = fqn.split(".")
            return self._build_fqn(*parts) if len(parts) >= 1 else self.escape_identifier(fqn)
        # Fallback: resolve from state when id_name_map misses (e.g. CLI without --target)
        return self._grant_fqn_from_state(target_type, target_id)

    def _resolve_grant_fqn_in_schema(
        self,
        schema: Any,
        catalog_name: str,
        schema_name: str,
        target_type: str,
        target_id: str,
    ) -> str | None:
        """Resolve FQN for table/view/volume/function/materialized_view within one schema."""
        if target_type == "table":
            for table in schema.tables if hasattr(schema, "tables") else schema.get("tables", []):
                tid = table.id if hasattr(table, "id") else table["id"]
                tname = table.name if hasattr(table, "name") else table["name"]
                if tid == target_id:
                    return self._build_fqn(catalog_name, schema_name, tname)
        if target_type == "view":
            for view in schema.views if hasattr(schema, "views") else schema.get("views", []):
                vid = view.id if hasattr(view, "id") else view["id"]
                vname = view.name if hasattr(view, "name") else view["name"]
                if vid == target_id:
                    return self._build_fqn(catalog_name, schema_name, vname)
        if target_type == "volume":
            for vol in getattr(schema, "volumes", None) or schema.get("volumes", []):
                vol_id = vol.id if hasattr(vol, "id") else vol["id"]
                vol_name = vol.name if hasattr(vol, "name") else vol["name"]
                if vol_id == target_id:
                    return self._build_fqn(catalog_name, schema_name, vol_name)
        if target_type == "function":
            for func in getattr(schema, "functions", None) or schema.get("functions", []):
                fid = func.id if hasattr(func, "id") else func["id"]
                fname = func.name if hasattr(func, "name") else func["name"]
                if fid == target_id:
                    return self._build_fqn(catalog_name, schema_name, fname)
        if target_type == "materialized_view":
            for mv in getattr(schema, "materialized_views", None) or schema.get(
                "materialized_views", []
            ):
                mv_id = mv.id if hasattr(mv, "id") else mv["id"]
                mv_name = mv.name if hasattr(mv, "name") else mv["name"]
                if mv_id == target_id:
                    return self._build_fqn(catalog_name, schema_name, mv_name)
        return None

    def _grant_fqn_from_state(self, target_type: str, target_id: str) -> str:
        """Resolve grant target FQN by walking state (catalogs/schemas/tables/views)."""
        catalogs = (
            self.state.catalogs
            if hasattr(self.state, "catalogs")
            else self.state.get("catalogs", [])
        )
        for catalog in catalogs:
            cat_name = catalog.name if hasattr(catalog, "name") else catalog["name"]
            cat_id = catalog.id if hasattr(catalog, "id") else catalog["id"]
            catalog_name = self.catalog_name_mapping.get(cat_name, cat_name)
            if target_type == "catalog" and cat_id == target_id:
                return self._build_fqn(catalog_name)
            for schema in (
                catalog.schemas if hasattr(catalog, "schemas") else catalog.get("schemas", [])
            ):
                schema_name = schema.name if hasattr(schema, "name") else schema["name"]
                schema_id = schema.id if hasattr(schema, "id") else schema["id"]
                if target_type == "schema" and schema_id == target_id:
                    return self._build_fqn(catalog_name, schema_name)
                fqn = self._resolve_grant_fqn_in_schema(
                    schema, catalog_name, schema_name, target_type, target_id
                )
                if fqn:
                    return fqn
        return ""

    def _escape_principal(self, principal: str) -> str:
        """Escape principal for GRANT/REVOKE (backticks if special characters)."""
        if not principal:
            return self.escape_identifier("unknown")
        return self.escape_identifier(principal)

    def _add_grant(self, operation: Operation) -> str:
        target_type = operation.payload.get("targetType", "table")
        target_id = operation.payload.get("targetId")
        principal = operation.payload.get("principal", "")
        privileges = operation.payload.get("privileges") or []
        if not target_id:
            return "-- Error: add_grant missing targetId"
        kind = self._grant_object_kind(target_type)
        fqn = self._grant_fqn(target_type, target_id)
        if not fqn:
            return f"-- Error: could not resolve FQN for targetId {target_id}"
        principal_esc = self._escape_principal(principal)
        priv_list = ", ".join(self.escape_identifier(p) for p in privileges)
        if not priv_list:
            return "-- No privileges specified for add_grant"
        return f"GRANT {priv_list} ON {kind} {fqn} TO {principal_esc}"

    def _revoke_grant(self, operation: Operation) -> str:
        target_type = operation.payload.get("targetType", "table")
        target_id = operation.payload.get("targetId")
        principal = operation.payload.get("principal", "")
        privileges = operation.payload.get("privileges")  # None or list; None = revoke all
        if not target_id:
            return "-- Error: revoke_grant missing targetId"
        kind = self._grant_object_kind(target_type)
        fqn = self._grant_fqn(target_type, target_id)
        if not fqn:
            return f"-- Error: could not resolve FQN for targetId {target_id}"
        principal_esc = self._escape_principal(principal)
        if not privileges or len(privileges) == 0:
            priv_clause = "ALL PRIVILEGES"
        else:
            priv_clause = ", ".join(self.escape_identifier(p) for p in privileges)
        return f"REVOKE {priv_clause} ON {kind} {fqn} FROM {principal_esc}"
