"""
Unity Catalog SQL Generator

Generates Databricks SQL DDL statements from operations.
Migrated from TypeScript sql-generator.ts
"""

from typing import Any, TypedDict

from schemax.providers.base.batching import BatchInfo
from schemax.providers.base.dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)
from schemax.providers.base.exceptions import SchemaXProviderError
from schemax.providers.base.operations import Operation
from schemax.providers.base.sql_generator import BaseSQLGenerator, SQLGenerationResult

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

                # Process views in this schema
                views = schema.views if hasattr(schema, "views") else schema.get("views", [])
                for view in views:
                    view_name = view.name if hasattr(view, "name") else view["name"]
                    view_id = view.id if hasattr(view, "id") else view["id"]
                    view_fqn = f"{catalog_name}.{schema_name}.{view_name}"
                    id_map[view_id] = view_fqn

                # Process volumes, functions, materialized views in this schema
                for attr in ("volumes", "functions", "materialized_views"):
                    items = (
                        schema.get(attr, [])
                        if isinstance(schema, dict)
                        else getattr(schema, attr, [])
                    )
                    for item in items:
                        item_name = item.name if hasattr(item, "name") else item["name"]
                        item_id = item.id if hasattr(item, "id") else item["id"]
                        item_fqn = f"{catalog_name}.{schema_name}.{item_name}"
                        id_map[item_id] = item_fqn

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
        0 = catalog, 1 = schema, 2 = table/view creation, 3 = table/view modifications

        Implements abstract method from BaseSQLGenerator.
        """
        op_type = op.op
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

        # View-level operations
        if op_type in [
            "add_view",
            "rename_view",
            "drop_view",
            "update_view",
            "set_view_comment",
            "set_view_property",
            "unset_view_property",
        ]:
            return f"view:{op.target}"  # Prefix to avoid ID collisions

        # Volume-level operations
        if op_type in [
            "add_volume",
            "rename_volume",
            "update_volume",
            "drop_volume",
        ]:
            return f"volume:{op.target}"

        # Function-level operations
        if op_type in [
            "add_function",
            "rename_function",
            "update_function",
            "drop_function",
            "set_function_comment",
        ]:
            return f"function:{op.target}"

        # Materialized view-level operations
        if op_type in [
            "add_materialized_view",
            "rename_materialized_view",
            "update_materialized_view",
            "drop_materialized_view",
            "set_materialized_view_comment",
        ]:
            return f"materialized_view:{op.target}"

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
        return op.op in [
            "unity.add_catalog",
            "unity.add_schema",
            "unity.add_table",
            "unity.add_view",
            "unity.add_volume",
            "unity.add_function",
            "unity.add_materialized_view",
        ]

    def _is_drop_operation(self, op: Operation) -> bool:
        """
        Check if Unity operation drops an object.

        Implements abstract method from BaseSQLGenerator.
        DROP operations cannot be batched with CREATE/ALTER and must be handled separately.

        Note: drop_column and drop_constraint are NOT included here because they are
        table modifications (processed in order with other table operations), not object drops.
        """
        return op.op in [
            "unity.drop_catalog",
            "unity.drop_schema",
            "unity.drop_table",
            "unity.drop_view",
            "unity.drop_volume",
            "unity.drop_function",
            "unity.drop_materialized_view",
        ]

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
            # Categorize operations properly
            column_ops = []
            property_ops = []
            constraint_ops = []
            reorder_ops = []
            governance_ops = []
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
                elif op_type in ["set_table_property", "unset_table_property"]:
                    property_ops.append(op)
                elif op_type in ["add_constraint", "drop_constraint"]:
                    constraint_ops.append(op)
                elif op_type == "reorder_columns":
                    reorder_ops.append(op)
                elif op_type in [
                    "add_row_filter",
                    "update_row_filter",
                    "remove_row_filter",
                    "add_column_mask",
                    "update_column_mask",
                    "remove_column_mask",
                ]:
                    governance_ops.append(op)
                else:
                    # Everything else goes to other_ops (includes set_table_tag, set_table_comment, etc.)
                    other_ops.append(op)

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
        else:
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
            elif op_type in ["set_table_property", "unset_table_property"]:
                property_ops.append(op)
            elif op_type in ["add_constraint", "drop_constraint"]:
                constraint_ops.append(op)
            elif op_type == "reorder_columns":
                reorder_ops.append(op)
            elif op_type in [
                "add_row_filter",
                "update_row_filter",
                "remove_row_filter",
                "add_column_mask",
                "update_column_mask",
                "remove_column_mask",
            ]:
                governance_ops.append(op)
            elif op_type in ["set_table_tag", "unset_table_tag"]:
                table_tag_ops.append(op)
            else:
                # Everything else goes to other_ops (set_table_comment, etc.)
                other_ops.append(op)

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

    def _filter_cancelled_operations(self, ops: list[Operation]) -> list[Operation]:
        """
        Filter out create+drop pairs that cancel each other.

        If an object is created and then dropped in the same changeset,
        skip ALL operations for that object (they cancel out).

        This is timestamp-aware: cancellation only occurs if CREATE timestamp < DROP timestamp
        (chronological order). If DROP comes before CREATE, they don't cancel.

        Examples:
        - add_table (T1) + drop_table (T2 where T2 > T1) → skip both
        - drop_table (T1) + add_table (T2 where T2 > T1) → keep both (recreate)
        - add_catalog + drop_catalog → skip both
        - add_schema + drop_schema → skip both
        Examples:
        - add_table + drop_table → skip both (and any operations in between)
        - add_catalog + drop_catalog → skip both
        - add_schema + drop_schema → skip both
        - add_column + drop_column → skip both

        This prevents errors when trying to drop objects that were never
        created in the database (e.g., table in non-existent schema).
        """
        # Group operations by target object
        by_target: dict[str, list[Operation]] = {}
        for op in ops:
            target_id = self._get_target_object_id(op)
            if target_id:
                by_target.setdefault(target_id, []).append(op)
            else:
                # Keep operations without specific target (global operations)
                by_target.setdefault("__global__", []).append(op)

        # Filter each group
        filtered = []
        for target_id, target_ops in by_target.items():
            # Skip empty groups
            if not target_ops:
                continue

            # Find CREATE and DROP operations (timestamp-aware)
            if len(target_ops) >= 2:
                # Define create/drop pairs
                cancel_pairs = [
                    ("unity.add_catalog", "unity.drop_catalog"),
                    ("unity.add_schema", "unity.drop_schema"),
                    ("unity.add_table", "unity.drop_table"),
                    ("unity.add_view", "unity.drop_view"),
                    ("unity.add_volume", "unity.drop_volume"),
                    ("unity.add_function", "unity.drop_function"),
                    ("unity.add_materialized_view", "unity.drop_materialized_view"),
                    ("unity.add_column", "unity.drop_column"),
                ]

                # Find the CREATE and DROP operations
                create_op = None
                drop_op = None
                for op in target_ops:
                    for create_type, drop_type in cancel_pairs:
                        if op.op == create_type:
                            create_op = op
                        elif op.op == drop_type:
                            drop_op = op

                # Check if cancellation occurs (CREATE must come before DROP chronologically)
                cancelled = False
                if create_op and drop_op and create_op.ts < drop_op.ts:
                    # Cancel out: skip ALL operations for this object
                    cancelled = True

                # Special handling for columns: only cancel if adding and dropping THE SAME column
                if not cancelled and len(target_ops) == 2:
                    if (
                        target_ops[0].op == "unity.add_column"
                        and target_ops[1].op == "unity.drop_column"
                        and target_ops[0].target == target_ops[1].target
                        and target_ops[0].ts < target_ops[1].ts  # Chronological
                    ):
                        # Same column ID, chronological order - cancel it
                        cancelled = True

                if not cancelled:
                    # Not cancelled - keep all operations
                    filtered.extend(target_ops)
            else:
                # Single operation or global - keep it
                filtered.extend(target_ops)

        return filtered

    def _extract_operation_dependencies(
        self, op: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """
        Extract dependencies from Unity Catalog operations.

        Currently supports:
        - View dependencies on tables/views (from add_view operation)
        - Foreign key dependencies (from add_constraint operation with type=foreign_key)
        - Constraint modification dependencies (add_constraint depends on prior drop_constraint)

        Args:
            op: Operation to analyze

        Returns:
            List of tuples: (dependency_id, dependency_type, enforcement)
        """
        dependencies: list[tuple[str, DependencyType, DependencyEnforcement]] = []

        # Extract view dependencies
        if op.op == "unity.add_view":
            # Dependencies field contains list of table/view IDs this view depends on
            dep_ids = op.payload.get("dependencies", [])
            if isinstance(dep_ids, list):
                for dep_id in dep_ids:
                    dependencies.append(
                        (dep_id, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
                    )

        # Extract materialized view dependencies (same pattern as views)
        if op.op == "unity.add_materialized_view":
            dep_ids = op.payload.get("dependencies", []) or op.payload.get(
                "extractedDependencies", {}
            )
            if isinstance(dep_ids, list):
                for dep_id in dep_ids:
                    dependencies.append(
                        (dep_id, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
                    )
            elif isinstance(dep_ids, dict):
                # extractedDependencies may be { "tables": [...], "views": [...] }
                for tid in dep_ids.get("tables", []):
                    dependencies.append(
                        (tid, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
                    )
                for vid in dep_ids.get("views", []):
                    dependencies.append(
                        (vid, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
                    )

        # Extract foreign key dependencies
        elif op.op == "unity.add_constraint":
            constraint_type = op.payload.get("type")
            if constraint_type == "foreign_key":
                # Foreign key creates a dependency on the parent table
                parent_table_id = op.payload.get("parentTable")
                if parent_table_id:
                    # Add table: prefix to match the format used by _get_target_object_id
                    parent_table_node_id = f"table:{parent_table_id}"
                    dependencies.append(
                        (
                            parent_table_node_id,
                            DependencyType.FOREIGN_KEY,
                            DependencyEnforcement.ENFORCED,
                        )
                    )

        return dependencies

    def _build_dependency_graph(self, ops: list[Operation]) -> DependencyGraph:
        """
        Build a dependency graph from operations, including constraint ordering.

        Overrides base class to add special handling for constraint modifications:
        - When add_constraint follows drop_constraint on the same table,
          create a dependency so DROP executes before ADD.
        - Graph nodes are object IDs (e.g. table:xyz); we add per-operation nodes
          for constraint ops so we can order DROP before ADD.
        """
        # Call parent implementation
        graph = super()._build_dependency_graph(ops)
        ops_by_target = graph.metadata.get("ops_by_target", {})

        # Group constraint operations by table (use same target ID format as base)
        table_ops: dict[str, list[Operation]] = {}
        for op in ops:
            if op.op in ("unity.add_constraint", "unity.drop_constraint"):
                table_id = op.payload.get("tableId")
                if table_id:
                    target_id = f"table:{table_id}"
                    if target_id not in table_ops:
                        table_ops[target_id] = []
                    table_ops[target_id].append(op)

        # For each table with both drop and add constraint ops, add per-op nodes
        # and edges so DROP executes before ADD (graph uses object IDs as nodes).
        for table_node_id, table_constraint_ops in table_ops.items():
            drop_ops = [op for op in table_constraint_ops if op.op == "unity.drop_constraint"]
            add_ops = [op for op in table_constraint_ops if op.op == "unity.add_constraint"]
            if not drop_ops or not add_ops:
                continue

            # Remove these ops from the table node so we don't duplicate them
            if table_node_id in ops_by_target:
                remaining = [
                    o
                    for o in ops_by_target[table_node_id]
                    if o not in drop_ops and o not in add_ops
                ]
                ops_by_target[table_node_id] = remaining

            level = self._get_dependency_level(drop_ops[0])
            # Add a node per constraint operation so we can order drop before add
            for drop_op in drop_ops:
                if drop_op.id not in graph.nodes:
                    node = DependencyNode(
                        id=drop_op.id,
                        type="constraint",
                        hierarchy_level=level,
                        operation=drop_op,
                        metadata={"op_type": drop_op.op},
                    )
                    graph.add_node(node)
                    ops_by_target[drop_op.id] = [drop_op]
                graph.add_edge(
                    table_node_id,
                    drop_op.id,
                    DependencyType.CONSTRAINT_ORDERING,
                    DependencyEnforcement.ENFORCED,
                )
            for add_op in add_ops:
                if add_op.id not in graph.nodes:
                    node = DependencyNode(
                        id=add_op.id,
                        type="constraint",
                        hierarchy_level=level,
                        operation=add_op,
                        metadata={"op_type": add_op.op},
                    )
                    graph.add_node(node)
                    ops_by_target[add_op.id] = [add_op]
                for drop_op in drop_ops:
                    graph.add_edge(
                        drop_op.id,
                        add_op.id,
                        DependencyType.CONSTRAINT_ORDERING,
                        DependencyEnforcement.ENFORCED,
                    )

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
        from ..base.exceptions import CircularDependencyError

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
            op_ids_in_result = {op.id for op in sorted_ops}
            for op in ops:
                if op.id not in op_ids_in_result:
                    sorted_ops.append(op)
            return sorted_ops, warnings

        except CircularDependencyError:
            # Re-raise to be handled by caller
            raise
        except Exception as e:
            # Unexpected error - warn and fall back
            warnings.append(
                f"Dependency analysis failed: {e}. Falling back to level-based sorting."
            )
            return sorted(ops, key=lambda op: (self._get_dependency_level(op), op.ts)), warnings

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

    def generate_sql_with_mapping(self, ops: list[Operation]) -> "SQLGenerationResult":
        """
        Generate SQL with explicit operation-to-statement mapping.

        Returns SQLGenerationResult with:
        - sql: Combined SQL script
        - statements: List of StatementInfo (sql + operation_ids + execution_order)
        """
        from ..base.sql_generator import SQLGenerationResult, StatementInfo

        # Use dependency-aware topological sorting for correct execution order
        # This handles view dependencies, foreign keys, and hierarchy
        global_warnings = []

        try:
            # Build dependency graph and get topologically sorted operations
            sorted_ops, sort_warnings = self._topological_sort_with_fallback(ops)
            global_warnings.extend(sort_warnings)
        except Exception as e:
            # Handle circular dependencies with loud warning
            from ..base.exceptions import CircularDependencyError

            if isinstance(e, CircularDependencyError):
                # Format critical warning
                warning_msg = "\n" + "=" * 70 + "\n"
                warning_msg += "⚠️  CRITICAL WARNING: CIRCULAR DEPENDENCIES DETECTED\n"
                warning_msg += "=" * 70 + "\n"
                warning_msg += str(e)
                warning_msg += "\n→ Falling back to level-based sorting (dependencies IGNORED)"
                warning_msg += "\n→ Run 'schemax validate' to see full details"
                warning_msg += "\n→ SQL execution may FAIL if dependencies are not met"
                warning_msg += "\n" + "=" * 70
                print(warning_msg)
                global_warnings.append("Circular dependencies detected - dependencies ignored")
            else:
                print(f"Warning: Dependency analysis failed ({e}), using level-based sort")
                global_warnings.append(f"Dependency analysis failed: {e}")

            sorted_ops = sorted(ops, key=lambda op: (self._get_dependency_level(op), op.ts))

        # Filter out cancelled operations (CREATE + DROP for same object)
        sorted_ops = self._filter_cancelled_operations(sorted_ops)

        # Separate DROP operations from CREATE/ALTER operations
        # DROP operations cannot be batched and must be processed individually
        drop_ops = [op for op in sorted_ops if self._is_drop_operation(op)]
        non_drop_ops = [op for op in sorted_ops if not self._is_drop_operation(op)]

        # Use base class's generic batcher (no duplication!)
        # Only batch non-DROP operations
        batches = self.batcher.batch_operations(
            non_drop_ops, self._get_target_object_id, self._is_create_operation
        )
        processed_op_ids = set()

        # Track statements with their operations
        statement_infos: list[StatementInfo] = []
        execution_order = 0

        # Separate batches by object type for proper ordering
        catalog_stmts = []
        schema_stmts = []
        table_stmts = []
        view_stmts = []
        materialized_view_stmts = []
        volume_stmts = []
        function_stmts = []
        other_ops = []

        # Process batched operations (CREATE and ALTER only)
        for object_id, batch_info in batches.items():
            op_ids = batch_info.op_ids
            processed_op_ids.update(op_ids)

            # Determine object type from ID prefix
            if object_id.startswith("catalog:"):
                object_type = "catalog"
            elif object_id.startswith("schema:"):
                object_type = "schema"
            elif object_id.startswith("table:"):
                object_type = "table"
            elif object_id.startswith("view:"):
                object_type = "view"
            elif object_id.startswith("materialized_view:"):
                object_type = "materialized_view"
            elif object_id.startswith("volume:"):
                object_type = "volume"
            elif object_id.startswith("function:"):
                object_type = "function"
            else:
                # Skip unknown object types
                continue

            if object_type == "catalog":
                sql = self._generate_create_catalog_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    catalog_stmts.append((sql, op_ids))
            elif object_type == "schema":
                sql = self._generate_create_schema_batched(object_id, batch_info)
                if sql and not sql.startswith("--"):
                    schema_stmts.append((sql, op_ids))
            elif object_type == "table":
                # Table operations can produce multiple statements
                table_result = self._generate_table_sql_with_mapping(object_id, batch_info)
                table_stmts.extend(table_result)  # List of (sql, op_ids) tuples
            elif object_type == "view":
                # View operations - simpler than tables
                view_result = self._generate_view_sql_with_mapping(object_id, batch_info)
                view_stmts.extend(view_result)  # List of (sql, op_ids) tuples
            elif object_type == "materialized_view":
                mv_result = self._generate_materialized_view_sql_with_mapping(
                    object_id, batch_info
                )
                materialized_view_stmts.extend(mv_result)
            elif object_type == "volume":
                vol_result = self._generate_volume_sql_with_mapping(object_id, batch_info)
                volume_stmts.extend(vol_result)
            elif object_type == "function":
                func_result = self._generate_function_sql_with_mapping(object_id, batch_info)
                function_stmts.extend(func_result)

        # Process unbatched non-DROP operations
        for op in non_drop_ops:
            if op.id in processed_op_ids:
                continue

            if not self.can_generate_sql(op):
                print(f"Warning: Cannot generate SQL for operation: {op.op}")
                continue

            other_ops.append(op)

        # Build statement infos in dependency order (split CREATE+ALTER so each runs as one statement)
        for sql, op_ids in catalog_stmts:
            for sql_part in self._split_sql_statements(sql):
                execution_order += 1
                statement_infos.append(
                    StatementInfo(
                        sql=sql_part, operation_ids=op_ids, execution_order=execution_order
                    )
                )

        for sql, op_ids in schema_stmts:
            for sql_part in self._split_sql_statements(sql):
                execution_order += 1
                statement_infos.append(
                    StatementInfo(
                        sql=sql_part, operation_ids=op_ids, execution_order=execution_order
                    )
                )

        for sql, op_ids in table_stmts:
            execution_order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=execution_order)
            )

        for sql, op_ids in view_stmts:
            execution_order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=execution_order)
            )

        for sql, op_ids in materialized_view_stmts:
            execution_order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=execution_order)
            )

        for sql, op_ids in volume_stmts:
            execution_order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=execution_order)
            )

        for sql, op_ids in function_stmts:
            execution_order += 1
            statement_infos.append(
                StatementInfo(sql=sql, operation_ids=op_ids, execution_order=execution_order)
            )

        for op in other_ops:
            result = self.generate_sql_for_operation(op)
            if result.sql:
                sql_stripped = result.sql.strip()
                if sql_stripped.startswith("-- Error") or sql_stripped.startswith("-- No "):
                    raise SchemaXProviderError(sql_stripped)
                if not sql_stripped.startswith("--"):
                    for sql_part in self._split_sql_statements(result.sql):
                        execution_order += 1
                        statement_infos.append(
                            StatementInfo(
                                sql=sql_part,
                                operation_ids=[op.id],
                                execution_order=execution_order,
                            )
                        )

        # Process DROP operations last (in reverse dependency order: table → schema → catalog)
        # This ensures we drop dependent objects before their parents
        for op in sorted(drop_ops, key=lambda op: -self._get_dependency_level(op)):
            result = self.generate_sql_for_operation(op)
            if result.sql and not result.sql.startswith("--"):
                for sql_part in self._split_sql_statements(result.sql):
                    execution_order += 1
                    statement_infos.append(
                        StatementInfo(
                            sql=sql_part,
                            operation_ids=[op.id],
                            execution_order=execution_order,
                        )
                    )

        # Build combined SQL script
        combined_sql = ";\n\n".join(stmt.sql for stmt in statement_infos)
        if combined_sql:
            combined_sql += ";"

        return SQLGenerationResult(
            sql=combined_sql,
            statements=statement_infos,
            warnings=global_warnings,
            is_idempotent=True,
        )

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

        # View operations
        elif op_type == "add_view":
            return self._add_view(op)
        elif op_type == "rename_view":
            return self._rename_view(op)
        elif op_type == "drop_view":
            return self._drop_view(op)
        elif op_type == "update_view":
            return self._update_view(op)
        elif op_type == "set_view_comment":
            return self._set_view_comment(op)
        elif op_type == "set_view_property":
            return self._set_view_property(op)
        elif op_type == "unset_view_property":
            return self._unset_view_property(op)

        # Volume operations
        elif op_type == "add_volume":
            return self._add_volume(op)
        elif op_type == "rename_volume":
            return self._rename_volume(op)
        elif op_type == "update_volume":
            return self._update_volume(op)
        elif op_type == "drop_volume":
            return self._drop_volume(op)

        # Function operations
        elif op_type == "add_function":
            return self._add_function(op)
        elif op_type == "rename_function":
            return self._rename_function(op)
        elif op_type == "update_function":
            return self._update_function(op)
        elif op_type == "drop_function":
            return self._drop_function(op)
        elif op_type == "set_function_comment":
            return self._set_function_comment(op)

        # Materialized view operations
        elif op_type == "add_materialized_view":
            return self._add_materialized_view(op)
        elif op_type == "rename_materialized_view":
            return self._rename_materialized_view(op)
        elif op_type == "update_materialized_view":
            return self._update_materialized_view(op)
        elif op_type == "drop_materialized_view":
            return self._drop_materialized_view(op)
        elif op_type == "set_materialized_view_comment":
            return self._set_materialized_view_comment(op)

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

        # Grant operations
        elif op_type == "add_grant":
            return self._add_grant(op)
        elif op_type == "revoke_grant":
            return self._revoke_grant(op)

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

        # Add comment if specified
        comment = op.payload.get("comment")
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags need to be set via ALTER after creation
        result = sql
        tags = op.payload.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"

        return result

    def _rename_catalog(self, op: Operation) -> str:
        old_name = op.payload["oldName"]
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER CATALOG {old_esc} RENAME TO {new_esc}"

    def _update_catalog(self, op: Operation) -> str:
        """Update catalog properties (managed location, comment, tags)"""
        name = self.id_name_map.get(op.target, op.target)
        statements = []

        # Handle managed location
        managed_location_name = op.payload.get("managedLocationName")
        if managed_location_name is not None:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                statements.append(
                    f"ALTER CATALOG {self.escape_identifier(name)} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )

        # Handle comment
        comment = op.payload.get("comment")
        if comment is not None:
            if comment:
                statements.append(
                    f"ALTER CATALOG {self.escape_identifier(name)} "
                    f"SET COMMENT '{self.escape_string(comment)}'"
                )
            else:
                statements.append(f"ALTER CATALOG {self.escape_identifier(name)} UNSET COMMENT")

        # Handle tags
        tags = op.payload.get("tags")
        if tags is not None and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            statements.append(
                f"ALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"
            )

        if statements:
            return ";\n".join(statements)
        else:
            return "-- Warning: No updates specified for catalog"

    def _drop_catalog(self, op: Operation) -> str:
        name = self.id_name_map.get(op.target, op.target)
        # Use CASCADE to ensure catalog drops even if it contains schemas/tables
        # This handles drift scenarios where catalog may have objects we don't track
        # CASCADE is safe for rollback: we're reverting to a previous known state
        # Note: In Unity Catalog, CASCADE soft-deletes managed tables (cleanup in 7-30 days)
        return f"DROP CATALOG IF EXISTS {self.escape_identifier(name)} CASCADE"

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

        # Add comment if specified
        comment = op.payload.get("comment")
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags need to be set via ALTER after creation
        result = sql
        tags = op.payload.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER SCHEMA {catalog_esc}.{schema_esc} SET TAGS ({tag_entries})"

        return result

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

        # Comment from create_op or squashed update_catalog
        comment = create_op.payload.get("comment")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_catalog" and "comment" in op.payload:
                comment = op.payload.get("comment")
                break
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags: set via ALTER after creation (create_op or squashed update_catalog)
        tags = create_op.payload.get("tags")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_catalog" and "tags" in op.payload:
                tags = op.payload.get("tags")
                break
        result = sql
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER CATALOG {self.escape_identifier(name)} SET TAGS ({tag_entries})"

        return result

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

        # Comment from create_op or squashed update_schema
        comment = create_op.payload.get("comment")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_schema" and "comment" in op.payload:
                comment = op.payload.get("comment")
                break
        if comment:
            sql += f" COMMENT '{self.escape_string(comment)}'"

        # Tags: set via ALTER after creation
        tags = create_op.payload.get("tags")
        for op in batch_info.modify_ops:
            if op.op == "unity.update_schema" and "tags" in op.payload:
                tags = op.payload.get("tags")
                break
        result = sql
        if tags and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            result += f";\nALTER SCHEMA {catalog_esc}.{schema_esc} SET TAGS ({tag_entries})"

        return result

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
        """Update schema properties (managed location, comment, tags)"""
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
        parts = fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        fqn_esc = self._build_fqn(catalog_name, schema_name)
        statements = []

        # Handle managed location
        managed_location_name = op.payload.get("managedLocationName")
        if managed_location_name is not None:
            location = self._resolve_managed_location(managed_location_name)
            if location:
                statements.append(
                    f"ALTER SCHEMA {fqn_esc} "
                    f"SET MANAGED LOCATION '{self.escape_string(location['resolved'])}'"
                )

        # Handle comment
        comment = op.payload.get("comment")
        if comment is not None:
            if comment:
                statements.append(
                    f"ALTER SCHEMA {fqn_esc} SET COMMENT '{self.escape_string(comment)}'"
                )
            else:
                statements.append(f"ALTER SCHEMA {fqn_esc} UNSET COMMENT")

        # Handle tags
        tags = op.payload.get("tags")
        if tags is not None and isinstance(tags, dict) and len(tags) > 0:
            tag_entries = ", ".join(
                f"'{self.escape_string(k)}' = '{self.escape_string(v)}'" for k, v in tags.items()
            )
            statements.append(f"ALTER SCHEMA {fqn_esc} SET TAGS ({tag_entries})")

        if statements:
            return ";\n".join(statements)
        else:
            return "-- Warning: No updates specified for schema"

    def _drop_schema(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
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

        # Add table comment if present
        comment = op.payload.get("comment", "")
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""

        # Create empty table (columns added via add_column ops)
        using_clause = (
            f"USING {table_format}{comment_clause}{partition_clause}"
            f"{cluster_clause}{location_clause}"
        )
        return f"{warnings}CREATE {external_keyword}TABLE IF NOT EXISTS {fqn_esc} () {using_clause}"

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
        # Get table FQN from id_name_map
        # SQL generator MUST be created with state containing objects to be dropped
        # (e.g., use current_state during rollback, not target_state)
        table_fqn = self.id_name_map.get(op.target)

        if not table_fqn or "." not in table_fqn:
            # This should never happen if SQL generator is used correctly
            # If it does, it indicates a bug in the calling code
            raise ValueError(
                f"Cannot generate DROP TABLE for {op.target}: table not found in state.\n"
                f"Hint: SQL generator must be created with state containing objects to be dropped.\n"
                f"For rollback operations, use current_state (not target_state)."
            )

        fqn_esc = self._build_fqn(*table_fqn.split("."))
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

    def _generate_batched_table_tag_sql(self, table_tag_ops: list[Operation]) -> str:
        """Batch table tag ops by table_id: one SET TAGS and one UNSET TAGS per table."""
        if not table_tag_ops:
            return ""

        set_by_table: dict[str, dict[str, str]] = {}
        unset_by_table: dict[str, set[str]] = {}

        for op in table_tag_ops:
            table_id = op.payload.get("tableId", "")
            op_type = op.op.replace("unity.", "")
            if op_type == "set_table_tag":
                tag_name = op.payload["tagName"]
                tag_value = self.escape_string(op.payload["tagValue"])
                if table_id not in set_by_table:
                    set_by_table[table_id] = {}
                set_by_table[table_id][tag_name] = tag_value
                if table_id in unset_by_table:
                    unset_by_table[table_id].discard(tag_name)
            elif op_type == "unset_table_tag":
                tag_name = op.payload["tagName"]
                if table_id not in unset_by_table:
                    unset_by_table[table_id] = set()
                unset_by_table[table_id].add(tag_name)
                if table_id in set_by_table:
                    set_by_table[table_id].pop(tag_name, None)

        parts: list[str] = []
        for tid in set(set_by_table.keys()) | set(unset_by_table.keys()):
            table_fqn = self.id_name_map.get(tid, "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            unset_tags = unset_by_table.get(tid)
            if unset_tags:
                tag_list = ", ".join(f"'{self.escape_string(t)}'" for t in unset_tags)
                parts.append(f"ALTER TABLE {table_esc} UNSET TAGS ({tag_list})")
            set_tags = set_by_table.get(tid)
            if set_tags:
                tag_list = ", ".join(
                    f"'{self.escape_string(k)}' = '{v}'" for k, v in set_tags.items()
                )
                parts.append(f"ALTER TABLE {table_esc} SET TAGS ({tag_list})")
        return ";\n".join(parts) if parts else ""

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

    # View operations
    def _qualify_view_definition(
        self, definition: str, extracted_deps: dict[str, list[str]]
    ) -> str:
        """
        Qualify unqualified table/view references in view SQL with fully-qualified names.

        This ensures views work correctly even when the current catalog/schema context
        is not set, which is the case with SQL Statement Execution API.

        Args:
            definition: Raw view SQL (may contain unqualified table names)
            extracted_deps: Extracted dependencies with table/view names (from frontend)

        Returns:
            SQL with all table/view references fully qualified
        """
        import sqlglot
        from sqlglot import expressions as exp

        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(definition, dialect="databricks")
        except Exception:
            # If parsing fails, return original (let Databricks handle the error)
            return definition

        # Build mapping from unqualified names to FQNs
        # Note: extracted_deps contains table/view NAMES (not IDs) from frontend parsing
        name_to_fqn: dict[str, str] = {}

        # Build reverse map: name -> FQN from id_name_map
        # id_name_map is like: {"tbl1": "catalog.schema.table1", ...}
        for object_id, fqn in self.id_name_map.items():
            if not fqn or "." not in fqn:
                continue

            parts = fqn.split(".")
            if len(parts) == 3:
                catalog, schema, name = parts
                # Map: name -> catalog.schema.name (for unqualified refs)
                name_to_fqn[name] = fqn
                # Map: schema.name -> catalog.schema.name (for partially qualified refs)
                name_to_fqn[f"{schema}.{name}"] = fqn

        # Note: We don't need to filter by extracted_deps anymore since we built
        # a comprehensive name->FQN map from all objects in id_name_map

        # Replace table references with FQNs
        for table_node in parsed.find_all(exp.Table):
            # Build current reference string
            current_ref_parts = []
            if table_node.catalog:
                current_ref_parts.append(table_node.catalog)
            if table_node.db:  # schema
                current_ref_parts.append(table_node.db)
            if table_node.name:
                current_ref_parts.append(table_node.name)

            current_ref = ".".join(current_ref_parts)

            # Look up FQN
            if current_ref in name_to_fqn:
                fqn = name_to_fqn[current_ref]
                parts = fqn.split(".")
                if len(parts) == 3:
                    # Update the table node with qualified names (quoted for Databricks)
                    table_node.set("catalog", exp.to_identifier(parts[0], quoted=True))
                    table_node.set("db", exp.to_identifier(parts[1], quoted=True))  # schema
                    table_node.set("this", exp.to_identifier(parts[2], quoted=True))  # table name
            elif table_node.catalog:
                # If catalog is explicitly specified but not in our map,
                # check if it's a logical catalog name and map it to physical name
                logical_catalog = table_node.catalog
                if logical_catalog in self.catalog_name_mapping:
                    physical_catalog = self.catalog_name_mapping[logical_catalog]
                    table_node.set("catalog", exp.to_identifier(physical_catalog, quoted=True))
                else:
                    # External catalog - preserve name but add backticks
                    table_node.set("catalog", exp.to_identifier(logical_catalog, quoted=True))

                # Always add backticks to schema and table (even for external refs)
                if table_node.db:
                    table_node.set("db", exp.to_identifier(table_node.db, quoted=True))
                if table_node.name:
                    table_node.set("this", exp.to_identifier(table_node.name, quoted=True))

        # Generate SQL with qualified names
        qualified_sql = parsed.sql(dialect="databricks", pretty=True)
        return qualified_sql

    def _add_view(self, op: Operation) -> str:
        """Generate CREATE VIEW statement"""
        # Get schema FQN and extract catalog/schema names
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        view_name = op.payload["name"]

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)

        # Build fully qualified view name
        view_esc = self._build_fqn(catalog_name, schema_name, view_name)
        definition = op.payload.get("definition", "")
        comment = op.payload.get("comment")

        # Always qualify table/view references in the definition
        # (even if extractedDependencies is missing, we use all objects from id_name_map)
        extracted_deps = op.payload.get("extractedDependencies", {})
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

    def _rename_view(self, op: Operation) -> str:
        """Generate ALTER VIEW RENAME statement"""
        old_fqn = self.id_name_map.get(op.target, "unknown")
        parts = old_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        old_esc = self._build_fqn(*parts)

        # Build new FQN with new name
        parts[-1] = op.payload["newName"]
        new_esc = self._build_fqn(*parts)

        return f"ALTER VIEW {old_esc} RENAME TO {new_esc}"

    def _resolve_fqn_for_drop(self, op: Operation) -> str:
        """
        Resolve escaped FQN for DROP of a schema-level object.
        Uses id_name_map when available; otherwise builds from payload (name, catalogId, schemaId).
        """
        raw = self.id_name_map.get(op.target)
        if not raw or raw == "unknown":
            name = op.payload.get("name")
            catalog_id = op.payload.get("catalogId")
            schema_id = op.payload.get("schemaId")
            if not name:
                return self._build_fqn("unknown", "unknown", "unknown")
            schema_fqn = self.id_name_map.get(schema_id, "unknown.unknown")
            catalog_name = self.id_name_map.get(catalog_id, "unknown")
            parts_schema = schema_fqn.split(".", 1)
            cat_from_schema = parts_schema[0] if parts_schema else catalog_name
            catalog_physical = self.catalog_name_mapping.get(cat_from_schema, cat_from_schema)
            schema_part = parts_schema[1] if len(parts_schema) == 2 else "unknown"
            raw = f"{catalog_physical}.{schema_part}.{name}"
        parts = raw.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        return self._build_fqn(*parts)

    def _drop_view(self, op: Operation) -> str:
        """Generate DROP VIEW statement"""
        view_esc = self._resolve_fqn_for_drop(op)
        return f"DROP VIEW IF EXISTS {view_esc}"

    def _update_view(self, op: Operation) -> str:
        """Generate CREATE OR REPLACE VIEW statement to update definition"""
        view_fqn = self.id_name_map.get(op.target, "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)

        # Reconstruct FQN with physical catalog name
        parts[0] = catalog_name
        view_esc = self._build_fqn(*parts)
        definition = op.payload.get("definition", "")

        # Always qualify table/view references in the definition
        extracted_deps = op.payload.get("extractedDependencies", {})
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

    def _set_view_comment(self, op: Operation) -> str:
        """Generate ALTER VIEW SET TBLPROPERTIES for comment"""
        view_fqn = self.id_name_map.get(op.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        comment = op.payload["comment"].replace("'", "\\'")
        return f"COMMENT ON VIEW {view_esc} IS '{comment}'"

    def _set_view_property(self, op: Operation) -> str:
        """Generate ALTER VIEW SET TBLPROPERTIES"""
        view_fqn = self.id_name_map.get(op.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        key = op.payload["key"]
        value = op.payload["value"].replace("'", "\\'")
        return f"ALTER VIEW {view_esc} SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_view_property(self, op: Operation) -> str:
        """Generate ALTER VIEW UNSET TBLPROPERTIES"""
        view_fqn = self.id_name_map.get(op.payload["viewId"], "unknown")
        parts = view_fqn.split(".")
        catalog_name = parts[0] if len(parts) > 0 else "unknown"

        # Apply catalog name mapping (logical → physical)
        catalog_name = self.catalog_name_mapping.get(catalog_name, catalog_name)
        parts[0] = catalog_name

        view_esc = self._build_fqn(*parts)
        key = op.payload["key"]
        return f"ALTER VIEW {view_esc} UNSET TBLPROPERTIES ('{key}')"

    # Volume operations
    def _add_volume(self, op: Operation) -> str:
        """Generate CREATE [EXTERNAL] VOLUME statement"""
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        vol_name = op.payload["name"]
        vol_esc = self._build_fqn(*parts, vol_name)
        volume_type = op.payload.get("volumeType", "managed")
        external = volume_type == "external"
        sql = "CREATE EXTERNAL VOLUME" if external else "CREATE VOLUME"
        sql += f" IF NOT EXISTS {vol_esc}"
        if op.payload.get("location") and external:
            sql += f" LOCATION '{self.escape_string(op.payload['location'])}'"
        if op.payload.get("comment"):
            sql += f" COMMENT '{self.escape_string(op.payload['comment'])}'"
        return sql

    def _rename_volume(self, op: Operation) -> str:
        """Generate ALTER VOLUME RENAME statement"""
        old_fqn = self.id_name_map.get(op.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = op.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER VOLUME {old_esc} RENAME TO {new_esc}"

    def _update_volume(self, op: Operation) -> str:
        """Generate ALTER VOLUME SET COMMENT / LOCATION statements"""
        vol_fqn = self.id_name_map.get(op.target, "unknown")
        parts = vol_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        vol_esc = self._build_fqn(*parts)
        stmts = []
        if "comment" in op.payload:
            c = op.payload.get("comment") or ""
            stmts.append(f"ALTER VOLUME {vol_esc} SET COMMENT '{self.escape_string(c)}'")
        if "location" in op.payload and op.payload.get("location"):
            stmts.append(
                f"ALTER VOLUME {vol_esc} SET LOCATION '{self.escape_string(op.payload['location'])}'"
            )
        return ";\n".join(stmts) if stmts else "-- No volume updates specified"

    def _drop_volume(self, op: Operation) -> str:
        """Generate DROP VOLUME statement"""
        vol_esc = self._resolve_fqn_for_drop(op)
        return f"DROP VOLUME IF EXISTS {vol_esc}"

    # Function operations
    def _add_function(self, op: Operation) -> str:
        """Generate CREATE OR REPLACE FUNCTION statement"""
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_name = op.payload["name"]
        func_esc = self._build_fqn(*parts, func_name)
        language = (op.payload.get("language") or "SQL").upper()
        return_type = op.payload.get("returnType") or "STRING"
        body = op.payload.get("body") or "NULL"
        params = op.payload.get("parameters") or []
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

    def _rename_function(self, op: Operation) -> str:
        """Generate ALTER FUNCTION RENAME statement (Databricks: recreate or ALTER if supported)"""
        old_fqn = self.id_name_map.get(op.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = op.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER FUNCTION {old_esc} RENAME TO {new_esc}"

    def _update_function(self, op: Operation) -> str:
        """Generate CREATE OR REPLACE FUNCTION with updated body/return type from payload."""
        func_fqn = self.id_name_map.get(op.target, "unknown")
        parts = func_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_esc = self._build_fqn(*parts)
        language = (op.payload.get("language") or "SQL").upper()
        return_type = op.payload.get("returnType") or "STRING"
        body = op.payload.get("body") or "NULL"
        params = op.payload.get("parameters") or []
        param_str = ", ".join(
            f"{self.escape_identifier(p.get('name', 'x'))} {p.get('dataType', 'STRING')}"
            for p in params
            if isinstance(p, dict)
        )
        if language == "SQL":
            return f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE SQL RETURN ({body});"
        return f"CREATE OR REPLACE FUNCTION {func_esc}({param_str}) RETURNS {return_type} LANGUAGE PYTHON AS $$ {body} $$;"

    def _drop_function(self, op: Operation) -> str:
        """Generate DROP FUNCTION statement"""
        func_esc = self._resolve_fqn_for_drop(op)
        return f"DROP FUNCTION IF EXISTS {func_esc}"

    def _set_function_comment(self, op: Operation) -> str:
        """Generate COMMENT ON FUNCTION statement"""
        func_id = op.payload.get("functionId", op.target)
        func_fqn = self.id_name_map.get(func_id, "unknown")
        parts = func_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        func_esc = self._build_fqn(*parts)
        comment = (op.payload.get("comment") or "").replace("'", "\\'")
        return f"COMMENT ON FUNCTION {func_esc} IS '{comment}'"

    # Materialized view operations
    def _add_materialized_view(self, op: Operation) -> str:
        """Generate CREATE MATERIALIZED VIEW statement"""
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        parts = schema_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_name = op.payload["name"]
        mv_esc = self._build_fqn(*parts, mv_name)
        definition = op.payload.get("definition") or "SELECT 1"
        extracted_deps = op.payload.get("extractedDependencies", {})
        definition = self._qualify_view_definition(definition, extracted_deps)
        comment_clause = ""
        if op.payload.get("comment"):
            comment_clause = f" COMMENT '{self.escape_string(op.payload['comment'])}'"
        sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_esc}{comment_clause} AS\n{definition}"
        schedule = op.payload.get("refreshSchedule")
        if schedule:
            sql += f"\nSCHEDULE {schedule}"
        return sql

    def _rename_materialized_view(self, op: Operation) -> str:
        """Generate ALTER MATERIALIZED VIEW RENAME (or DROP + CREATE if needed)"""
        old_fqn = self.id_name_map.get(op.target, "unknown")
        parts = old_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        old_esc = self._build_fqn(*parts)
        parts[-1] = op.payload["newName"]
        new_esc = self._build_fqn(*parts)
        return f"ALTER MATERIALIZED VIEW {old_esc} RENAME TO {new_esc}"

    def _update_materialized_view(self, op: Operation) -> str:
        """Generate CREATE OR REPLACE MATERIALIZED VIEW or ALTER for schedule/comment"""
        schema_id = op.payload.get("schemaId")
        mv_id = op.target
        definition = op.payload.get("definition")
        if definition is not None:
            mv_fqn = self.id_name_map.get(mv_id, "unknown")
            parts = mv_fqn.split(".")
            catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
            parts[0] = catalog_physical
            mv_esc = self._build_fqn(*parts)
            extracted_deps = op.payload.get("extractedDependencies", {})
            definition = self._qualify_view_definition(definition, extracted_deps)
            comment_clause = ""
            if op.payload.get("comment"):
                comment_clause = f" COMMENT '{self.escape_string(op.payload['comment'])}'"
            sql = f"CREATE OR REPLACE MATERIALIZED VIEW {mv_esc}{comment_clause} AS\n{definition}"
            if op.payload.get("refreshSchedule"):
                sql += f"\nSCHEDULE {op.payload['refreshSchedule']}"
            return sql
        stmts = []
        mv_fqn = self.id_name_map.get(mv_id, "unknown")
        parts = mv_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_esc = self._build_fqn(*parts)
        if "refreshSchedule" in op.payload:
            s = op.payload.get("refreshSchedule")
            if s:
                stmts.append(f"ALTER MATERIALIZED VIEW {mv_esc} SET SCHEDULE {s}")
            else:
                stmts.append(f"ALTER MATERIALIZED VIEW {mv_esc} UNSET SCHEDULE")
        if "comment" in op.payload:
            c = op.payload.get("comment") or ""
            stmts.append(f"COMMENT ON MATERIALIZED VIEW {mv_esc} IS '{self.escape_string(c)}'")
        return ";\n".join(stmts) if stmts else "-- No materialized view updates specified"

    def _drop_materialized_view(self, op: Operation) -> str:
        """Generate DROP MATERIALIZED VIEW statement (Databricks requires this, not DROP VIEW)"""
        mv_esc = self._resolve_fqn_for_drop(op)
        return f"DROP MATERIALIZED VIEW IF EXISTS {mv_esc}"

    def _set_materialized_view_comment(self, op: Operation) -> str:
        """Generate COMMENT ON MATERIALIZED VIEW statement"""
        mv_id = op.payload.get("materializedViewId", op.target)
        mv_fqn = self.id_name_map.get(mv_id, "unknown")
        parts = mv_fqn.split(".")
        catalog_physical = self.catalog_name_mapping.get(parts[0], parts[0])
        parts[0] = catalog_physical
        mv_esc = self._build_fqn(*parts)
        comment = (op.payload.get("comment") or "").replace("'", "\\'")
        return f"COMMENT ON MATERIALIZED VIEW {mv_esc} IS '{comment}'"

    # Column operations
    def _add_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload.get("tableId", op.target), "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = op.payload.get("name", op.target)
        col_type = op.payload.get("type", "STRING")
        comment = op.payload.get("comment", "")
        nullable = op.payload.get("nullable", True)

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
            op for op in batch_info.modify_ops if op.op.replace("unity.", "") == "update_view"
        ]

        # Optimization: Squash CREATE + UPDATE_VIEW into single statement
        if has_create and update_view_ops and batch_info.create_op:
            # Use the LAST update_view operation (most recent definition)
            final_update_op = update_view_ops[-1]

            # Generate CREATE OR REPLACE VIEW with final definition
            sql = self._generate_create_or_replace_view(batch_info.create_op, final_update_op)
            if sql:
                # Track all operation IDs (create + all updates)
                op_ids = [batch_info.create_op.id] + [op.id for op in update_view_ops]
                statements.append((sql, op_ids))

            # Process remaining modify operations (excluding update_view)
            remaining_ops = [
                op for op in batch_info.modify_ops if op.op.replace("unity.", "") != "update_view"
            ]
        else:
            # No batching needed - process normally
            if batch_info.create_op:
                op = batch_info.create_op
                sql = self._add_view(op)
                if sql:
                    statements.append((sql, [op.id]))

            remaining_ops = batch_info.modify_ops

        # Process remaining modify operations (rename, drop, set properties, etc.)
        for op in remaining_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [op.id]))
            except Exception as e:
                statements.append((f"-- Error generating SQL for {op.id}: {e}", [op.id]))

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
        for op in batch_info.modify_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [op.id]))
            except Exception as e:
                statements.append((f"-- Error generating SQL for {op.id}: {e}", [op.id]))
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
        for op in batch_info.modify_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [op.id]))
            except Exception as e:
                statements.append((f"-- Error generating SQL for {op.id}: {e}", [op.id]))
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
        for op in batch_info.modify_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append((sql, [op.id]))
            except Exception as e:
                statements.append((f"-- Error generating SQL for {op.id}: {e}", [op.id]))
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
            for op in batch_info.modify_ops:
                op_type = op.op.replace("unity.", "")
                if op_type in [
                    "add_column",
                    "rename_column",
                    "drop_column",
                    "change_column_type",
                    "set_nullable",
                    "set_column_comment",
                ]:
                    column_ops.append(op)
                elif op_type in ["set_column_tag", "unset_column_tag"]:
                    tag_ops.append(op)
                elif op_type in ["set_table_tag", "unset_table_tag"]:
                    table_tag_ops.append(op)
                # Exclude operations that will be handled by dedicated lists below
                # (property_ops, constraint_ops, reorder_ops, governance_ops)
                elif op_type not in [
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
                ]:
                    other_ops.append(op)

            # Sort constraint operations to ensure DROP comes before ADD
            constraint_ops = [op for op in batch_info.modify_ops if "constraint" in op.op]
            constraint_ops_sorted = sorted(
                constraint_ops, key=lambda op: (0 if op.op == "unity.drop_constraint" else 1, op.ts)
            )

            batch_dict = {
                "is_new_table": batch_info.is_new,
                "table_op": batch_info.create_op,
                "column_ops": column_ops,
                "tag_ops": tag_ops,
                "table_tag_ops": table_tag_ops,
                "property_ops": [op for op in batch_info.modify_ops if "property" in op.op],
                "constraint_ops": constraint_ops_sorted,  # Use sorted list with DROP before ADD
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
        """Generate complete CREATE TABLE with columns and ALTER statements for constraints/tags"""
        table_op = batch_info["table_op"]
        column_ops = batch_info["column_ops"]
        property_ops = batch_info["property_ops"]
        constraint_ops = batch_info.get("constraint_ops", [])
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

        # Separate add_column, tag ops, and other column operations
        add_column_ops = [op for op in column_ops if op.op.endswith("add_column")]
        other_column_ops = [
            op
            for op in column_ops
            if not op.op.endswith("add_column")
            and not op.op.endswith("set_column_tag")
            and not op.op.endswith("unset_column_tag")
        ]

        # Build column definitions as a dictionary (by column ID - use op.target for add_column)
        columns_dict = {}
        for col_op in add_column_ops:
            col_id = col_op.target  # Column ID is in op.target for add_column operations
            col_name = self.escape_identifier(col_op.payload.get("name", col_id))
            col_type = col_op.payload.get("type", "STRING")
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
            columns = [columns_dict[col_id] for col_id in columns_dict.keys()]

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

        # Generate ALTER TABLE statements for operations that must happen after table creation
        # (e.g., table tags, column tags, constraints)
        # Skip set_table_comment since it's already included in CREATE TABLE
        statements = [create_sql]

        # Temporarily add the new table and its columns to id_name_map so constraint operations can find them
        # This is necessary because constraints reference the table and columns being created
        table_id_from_op = table_op.payload.get("tableId") or table_op.target
        if table_id_from_op:
            self.id_name_map[table_id_from_op] = table_fqn
            # Also add column mappings
            for col_op in add_column_ops:
                col_id = col_op.target
                col_name = col_op.payload.get("name", col_id)
                self.id_name_map[col_id] = col_name

        # Batched column tags after table creation (one SET TAGS / UNSET TAGS per column)
        tag_ops = batch_info.get("tag_ops") or [
            op
            for op in column_ops
            if op.op.endswith("set_column_tag") or op.op.endswith("unset_column_tag")
        ]
        batched_tag_sql = self._generate_batched_column_tag_sql(tag_ops)
        if batched_tag_sql:
            for stmt in batched_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        # Batched table tags (one SET TAGS / UNSET TAGS per table)
        table_tag_ops = batch_info.get("table_tag_ops") or [
            op
            for op in batch_info.get("other_ops", [])
            if op.op.endswith("set_table_tag") or op.op.endswith("unset_table_tag")
        ]
        batched_table_tag_sql = self._generate_batched_table_tag_sql(table_tag_ops)
        if batched_table_tag_sql:
            for stmt in batched_table_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        # Process other column operations (non-tag; tags are batched above)
        for op in other_column_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")

        # Process constraint operations after table creation
        for op in constraint_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")

        # Process governance operations (row filters, column masks) after table creation
        governance_ops = batch_info.get("governance_ops", [])
        for op in governance_ops:
            op_type = op.op.replace("unity.", "")
            try:
                sql = self._generate_sql_for_op_type(op_type, op)
                if sql and not sql.startswith("--"):
                    statements.append(sql)
            except Exception as e:
                statements.append(f"-- Error generating SQL for {op.id}: {e}")

        # Process other operations (skip table tags — batched above; skip set_table_comment — in CREATE)
        for op in other_ops:
            op_type = op.op.replace("unity.", "")
            if op_type == "set_table_comment":
                continue
            if op_type in ["set_table_tag", "unset_table_tag"]:
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

        # Strip "table:" prefix if present (from batching algorithm)
        # The id_name_map uses raw IDs without prefixes
        actual_table_id = table_id.removeprefix("table:")

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
                original_order = self._get_table_column_order(actual_table_id)
            final_order = last_reorder_op.payload["order"]
            reorder_sql = self._generate_optimized_reorder_sql(
                actual_table_id,
                original_order,
                final_order,
                [op.id for op in batch_info["reorder_ops"]],
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
            not_null_columns = []  # Track columns that need SET NOT NULL

            for op in add_column_ops:
                col_name = op.payload.get("name", op.target)
                col_type = op.payload.get("type", "STRING")
                comment = op.payload.get("comment", "")
                nullable = op.payload.get("nullable", True)

                # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMNS for Delta tables
                # New columns added to existing tables must be nullable initially
                comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
                col_esc = self.escape_identifier(col_name)

                column_defs.append(f"    {col_esc} {col_type}{comment_clause}")

                # If column should be NOT NULL, track it for follow-up statements
                if not nullable:
                    not_null_columns.append(col_name)

            batched_sql = (
                f"ALTER TABLE {table_esc}\nADD COLUMNS (\n" + ",\n".join(column_defs) + "\n)"
            )
            statements.append(batched_sql)

            # Add ALTER COLUMN SET NOT NULL statements for non-nullable columns
            for col_name in not_null_columns:
                col_esc = self.escape_identifier(col_name)
                statements.append(f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET NOT NULL")
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

        # Batched column tags (one SET TAGS / UNSET TAGS per column)
        tag_ops = batch_info.get("tag_ops") or [
            op
            for op in batch_info["column_ops"]
            if op.op.endswith("set_column_tag") or op.op.endswith("unset_column_tag")
        ]
        batched_tag_sql = self._generate_batched_column_tag_sql(tag_ops)
        if batched_tag_sql:
            for stmt in batched_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        # Batched table tags (one SET TAGS / UNSET TAGS per table)
        table_tag_ops = batch_info.get("table_tag_ops", [])
        batched_table_tag_sql = self._generate_batched_table_tag_sql(table_tag_ops)
        if batched_table_tag_sql:
            for stmt in batched_table_tag_sql.split(";\n"):
                if stmt.strip():
                    statements.append(stmt.strip())

        # Handle other column operations (non-ADD/DROP, non-tag; tags are batched above)
        other_column_ops = [
            op
            for op in batch_info["column_ops"]
            if not op.op.endswith("add_column")
            and not op.op.endswith("drop_column")
            and not op.op.endswith("set_column_tag")
            and not op.op.endswith("unset_column_tag")
        ]

        # Sort constraint operations to ensure DROP comes before ADD
        # This is necessary because operations batched to the same table
        # are processed together, and we need to ensure correct order within the batch
        constraint_ops_sorted = sorted(
            batch_info["constraint_ops"],
            key=lambda op: (0 if op.op == "unity.drop_constraint" else 1, op.ts),
        )

        # Handle all other operations normally (tag_ops already emitted above)
        for op in (
            other_column_ops
            + batch_info["property_ops"]
            + constraint_ops_sorted
            + batch_info["governance_ops"]
            + batch_info["other_ops"]
        ):
            op_type = op.op.replace("unity.", "")

            # Skip reorder operations (already handled)
            if op_type == "reorder_columns":
                continue
            # Skip table tag operations (batched above)
            if op_type in ["set_table_tag", "unset_table_tag"]:
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

    def _generate_batched_column_tag_sql(self, tag_ops: list[Operation]) -> str:
        """Batch column tag ops by (table_id, col_id): one SET TAGS and one UNSET TAGS per column.
        Databricks supports multiple tags: SET TAGS ('k1'='v1', 'k2'='v2'), UNSET TAGS ('k1', 'k2').
        """
        if not tag_ops:
            return ""

        set_by_col: dict[str, dict[str, str]] = {}
        unset_by_col: dict[str, set[str]] = {}

        for op in tag_ops:
            table_id = op.payload.get("tableId", "")
            col_id = op.target
            key = f"{table_id}:{col_id}"
            op_type = op.op.replace("unity.", "")
            if op_type == "set_column_tag":
                tag_name = op.payload["tagName"]
                tag_value = self.escape_string(op.payload["tagValue"])
                if key not in set_by_col:
                    set_by_col[key] = {}
                set_by_col[key][tag_name] = tag_value
                if key in unset_by_col:
                    unset_by_col[key].discard(tag_name)
            elif op_type == "unset_column_tag":
                tag_name = op.payload["tagName"]
                if key not in unset_by_col:
                    unset_by_col[key] = set()
                unset_by_col[key].add(tag_name)
                if key in set_by_col:
                    set_by_col[key].pop(tag_name, None)

        parts: list[str] = []
        for key in set(set_by_col.keys()) | set(unset_by_col.keys()):
            table_id, col_id = key.split(":", 1)
            table_fqn = self.id_name_map.get(table_id, "unknown")
            table_esc = self._build_fqn(*table_fqn.split("."))
            col_name = self.id_name_map.get(col_id, col_id)
            col_esc = self.escape_identifier(col_name)

            unset_tags = unset_by_col.get(key)
            if unset_tags:
                tag_list = ", ".join(f"'{self.escape_string(t)}'" for t in unset_tags)
                parts.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} UNSET TAGS ({tag_list})"
                )
            set_tags = set_by_col.get(key)
            if set_tags:
                tag_list = ", ".join(
                    f"'{self.escape_string(k)}' = '{v}'" for k, v in set_tags.items()
                )
                parts.append(
                    f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET TAGS ({tag_list})"
                )
        return ";\n".join(parts) if parts else ""

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
    def _constraint_options_suffix(self, op: Operation) -> str:
        """Build constraint option suffix (NOT ENFORCED, RELY, DEFERRABLE, INITIALLY DEFERRED)."""
        options: list[str] = []
        if op.payload.get("notEnforced"):
            options.append("NOT ENFORCED")
        if op.payload.get("rely"):
            options.append("RELY")
        if op.payload.get("deferrable"):
            options.append("DEFERRABLE")
        if op.payload.get("initiallyDeferred"):
            options.append("INITIALLY DEFERRED")
        return " " + " ".join(options) if options else ""

    def _add_constraint(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        constraint_type = op.payload["type"]
        constraint_name = op.payload.get("name", "")
        columns = [self.id_name_map.get(cid, cid) for cid in op.payload["columns"]]

        name_clause = (
            f"CONSTRAINT {self.escape_identifier(constraint_name)} " if constraint_name else ""
        )
        opts = self._constraint_options_suffix(op)

        if constraint_type == "primary_key":
            # Databricks: TIMESERIES is per-column: PRIMARY KEY ( col1 [ TIMESERIES ] [, ...] )
            col_parts = []
            for i, c in enumerate(columns):
                esc = self.escape_identifier(c)
                if i == 0 and op.payload.get("timeseries"):
                    col_parts.append(f"{esc} TIMESERIES")
                else:
                    col_parts.append(esc)
            cols = ", ".join(col_parts)
            return f"ALTER TABLE {table_esc} ADD {name_clause}PRIMARY KEY({cols}){opts}"

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
                f"FOREIGN KEY({cols}) REFERENCES {parent_esc}({parent_cols}){opts}"
            )

        elif constraint_type == "check":
            expression = op.payload.get("expression", "TRUE")
            return f"ALTER TABLE {table_esc} ADD {name_clause}CHECK ({expression}){opts}"

        return ""

    def _drop_constraint(self, op: Operation) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT SQL

        Gets the constraint name from the operation payload (preferred) or
        looks it up from the current state (fallback for backward compatibility).
        """
        table_id = op.payload["tableId"]
        constraint_id = op.target
        table_fqn = self.id_name_map.get(table_id, "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))

        # Get constraint name from payload (if provided) or look it up from state
        constraint_name = op.payload.get("name")
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
    def _add_row_filter(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        func_name = op.payload.get("name", "row_filter")
        func_esc = (
            self._build_fqn(*func_name.split("."))
            if "." in func_name
            else self.escape_identifier(func_name)
        )
        column_names = op.payload.get("columnNames") or []
        cols_sql = ", ".join(self.escape_identifier(c) for c in column_names)
        return f"ALTER TABLE {fqn_esc} SET ROW FILTER {func_esc} ON ({cols_sql})"

    def _update_row_filter(self, op: Operation) -> str:
        return self._add_row_filter(op)

    def _remove_row_filter(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        return f"ALTER TABLE {fqn_esc} DROP ROW FILTER"

    # Column mask operations (Unity: ALTER TABLE ... ALTER COLUMN col SET MASK func | DROP MASK)
    def _add_column_mask(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        col_name = self.id_name_map.get(
            op.payload["columnId"], op.payload.get("columnId", "unknown")
        )
        col_esc = self.escape_identifier(col_name)
        mask_func = op.payload["maskFunction"]
        func_esc = (
            self._build_fqn(*mask_func.split("."))
            if "." in mask_func
            else self.escape_identifier(mask_func)
        )
        using = op.payload.get("usingColumns")
        if using:
            using_sql = ", ".join(self.escape_identifier(c) for c in using)
            return f"ALTER TABLE {fqn_esc} ALTER COLUMN {col_esc} SET MASK {func_esc} USING COLUMNS ({using_sql})"
        return f"ALTER TABLE {fqn_esc} ALTER COLUMN {col_esc} SET MASK {func_esc}"

    def _update_column_mask(self, op: Operation) -> str:
        return self._add_column_mask(op)

    def _remove_column_mask(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown.unknown.unknown")
        parts = table_fqn.split(".")
        fqn_esc = self._build_fqn(*parts) if len(parts) >= 3 else self._build_fqn(table_fqn)
        col_name = self.id_name_map.get(
            op.payload["columnId"], op.payload.get("columnId", "unknown")
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
            schemas = catalog.schemas if hasattr(catalog, "schemas") else catalog.get("schemas", [])
            for schema in schemas:
                schema_name = schema.name if hasattr(schema, "name") else schema["name"]
                schema_id = schema.id if hasattr(schema, "id") else schema["id"]
                if target_type == "schema" and schema_id == target_id:
                    return self._build_fqn(catalog_name, schema_name)
                tables = schema.tables if hasattr(schema, "tables") else schema.get("tables", [])
                for table in tables:
                    table_id = table.id if hasattr(table, "id") else table["id"]
                    table_name = table.name if hasattr(table, "name") else table["name"]
                    if target_type == "table" and table_id == target_id:
                        return self._build_fqn(catalog_name, schema_name, table_name)
                views = schema.views if hasattr(schema, "views") else schema.get("views", [])
                for view in views:
                    view_id = view.id if hasattr(view, "id") else view["id"]
                    view_name = view.name if hasattr(view, "name") else view["name"]
                    if target_type == "view" and view_id == target_id:
                        return self._build_fqn(catalog_name, schema_name, view_name)
                volumes = getattr(schema, "volumes", None) or schema.get("volumes", [])
                for vol in volumes:
                    vol_id = vol.id if hasattr(vol, "id") else vol["id"]
                    vol_name = vol.name if hasattr(vol, "name") else vol["name"]
                    if target_type == "volume" and vol_id == target_id:
                        return self._build_fqn(catalog_name, schema_name, vol_name)
                functions = getattr(schema, "functions", None) or schema.get("functions", [])
                for func in functions:
                    func_id = func.id if hasattr(func, "id") else func["id"]
                    func_name = func.name if hasattr(func, "name") else func["name"]
                    if target_type == "function" and func_id == target_id:
                        return self._build_fqn(catalog_name, schema_name, func_name)
                mvs = getattr(schema, "materialized_views", None) or schema.get(
                    "materialized_views", []
                )
                for mv in mvs:
                    mv_id = mv.id if hasattr(mv, "id") else mv["id"]
                    mv_name = mv.name if hasattr(mv, "name") else mv["name"]
                    if target_type == "materialized_view" and mv_id == target_id:
                        return self._build_fqn(catalog_name, schema_name, mv_name)
        return ""

    def _escape_principal(self, principal: str) -> str:
        """Escape principal for GRANT/REVOKE (backticks if special characters)."""
        if not principal:
            return self.escape_identifier("unknown")
        return self.escape_identifier(principal)

    def _add_grant(self, op: Operation) -> str:
        target_type = op.payload.get("targetType", "table")
        target_id = op.payload.get("targetId")
        principal = op.payload.get("principal", "")
        privileges = op.payload.get("privileges") or []
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

    def _revoke_grant(self, op: Operation) -> str:
        target_type = op.payload.get("targetType", "table")
        target_id = op.payload.get("targetId")
        principal = op.payload.get("principal", "")
        privileges = op.payload.get("privileges")  # None or list; None = revoke all
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
