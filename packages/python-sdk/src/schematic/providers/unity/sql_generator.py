"""
Unity Catalog SQL Generator

Generates Databricks SQL DDL statements from operations.
Migrated from TypeScript sql-generator.ts
"""

from typing import Any, Dict, List, Optional

from ..base.batching import BatchInfo
from ..base.operations import Operation
from ..base.sql_generator import BaseSQLGenerator, SQLGenerationResult
from .models import UnityState
from .operations import UNITY_OPERATIONS


class UnitySQLGenerator(BaseSQLGenerator):
    """Unity Catalog SQL Generator"""

    def __init__(self, state: UnityState, catalog_name_mapping: Optional[Dict[str, str]] = None):
        # Pass catalog_name_mapping to base as name_mapping
        super().__init__(state, catalog_name_mapping)
        self.catalog_name_mapping = catalog_name_mapping or {}  # logical → physical
        # Base now provides self.batcher and self.optimizer
        self.id_name_map = self._build_id_name_map()

    def _build_id_name_map(self) -> Dict[str, str]:
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

    def _get_target_object_id(self, op: Operation) -> Optional[str]:
        """
        Extract target table ID from Unity operation.

        Implements abstract method from BaseSQLGenerator.
        Used by batching algorithm to group operations by table.
        """
        op_type = op.op.replace("unity.", "")

        # Table-level operations
        if op_type in ["add_table", "rename_table", "drop_table", "set_table_comment"]:
            return op.target

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
            "add_constraint",
            "drop_constraint",
            "add_row_filter",
            "update_row_filter",
            "remove_row_filter",
            "add_column_mask",
            "update_column_mask",
            "remove_column_mask",
        ]:
            return op.payload.get("tableId")

        return None  # Not a table operation (catalog, schema operations)

    def _is_create_operation(self, op: Operation) -> bool:
        """
        Check if Unity operation creates a new object.

        Implements abstract method from BaseSQLGenerator.
        """
        return op.op in ["unity.add_catalog", "unity.add_schema", "unity.add_table"]

    def _generate_batched_create_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE TABLE with batched columns for Unity.

        Implements abstract method from BaseSQLGenerator.
        This is the optimization that creates complete tables instead of empty + ALTERs.
        """
        # Delegate to existing _generate_create_table_with_columns method
        # Convert BatchInfo back to dict format for compatibility
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

    def generate_sql(self, ops: List[Operation]) -> str:
        """
        Generate SQL statements with comprehensive batch optimizations.

        Optimizations include:
        - Dependency-ordered operations (catalog → schema → table)
        - Complete CREATE TABLE statements (no empty tables + ALTERs)
        - Batched column reordering (minimal ALTER statements)
        - Table property consolidation
        """
        # Sort operations by dependency level first
        sorted_ops = sorted(ops, key=lambda op: self._get_dependency_level(op))

        # Pre-process: batch operations by table and type
        table_batches = self._batch_table_operations(sorted_ops)
        processed_op_ids = set()
        statements = []

        # Separate operations by level
        catalog_ops = []
        schema_ops = []
        table_stmts = []
        other_ops = []

        # Process batched table operations
        for table_id, batch_info in table_batches.items():
            op_ids = batch_info["op_ids"]

            # Mark these operations as processed
            processed_op_ids.update(op_ids)

            # Generate optimized SQL for this table
            table_sql = self._generate_optimized_table_sql(table_id, batch_info)

            if table_sql and not table_sql.startswith("--"):
                # Add batch header comment
                operation_types = set(
                    op.replace("unity.", "") for op in batch_info["operation_types"]
                )
                header = (
                    f"-- Batch Table Operations: {len(op_ids)} operations\n"
                    f"-- Table: {table_id}\n"
                    f"-- Types: {', '.join(sorted(operation_types))}\n"
                    f"-- Operations: {', '.join(op_ids)}"
                )
                table_stmts.append(f"{header}\n{table_sql};")

        # Categorize remaining operations
        for op in sorted_ops:
            if op.id in processed_op_ids:
                continue  # Skip already processed table operations

            if not self.can_generate_sql(op):
                print(f"Warning: Cannot generate SQL for operation: {op.op}")
                continue

            level = self._get_dependency_level(op)
            if level == 0:
                catalog_ops.append(op)
            elif level == 1:
                schema_ops.append(op)
            else:
                other_ops.append(op)

        # Generate SQL in dependency order: catalogs → schemas → tables → others
        for op in catalog_ops:
            result = self.generate_sql_for_operation(op)
            header = f"-- Operation: {op.id} ({op.ts})\n-- Type: {op.op}"
            warnings_comment = ""
            if result.warnings:
                warnings_comment = f"\n-- Warnings: {', '.join(result.warnings)}"
            statements.append(f"{header}{warnings_comment}\n{result.sql};")

        for op in schema_ops:
            result = self.generate_sql_for_operation(op)
            header = f"-- Operation: {op.id} ({op.ts})\n-- Type: {op.op}"
            warnings_comment = ""
            if result.warnings:
                warnings_comment = f"\n-- Warnings: {', '.join(result.warnings)}"
            statements.append(f"{header}{warnings_comment}\n{result.sql};")

        # Add table operations
        statements.extend(table_stmts)

        # Add other operations
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
        elif op_type == "drop_catalog":
            return self._drop_catalog(op)

        # Schema operations
        elif op_type == "add_schema":
            return self._add_schema(op)
        elif op_type == "rename_schema":
            return self._rename_schema(op)
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
        return f"CREATE CATALOG IF NOT EXISTS {self.escape_identifier(name)}"

    def _rename_catalog(self, op: Operation) -> str:
        old_name = self.id_name_map.get(op.target, op.target)
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER CATALOG {old_esc} RENAME TO {new_esc}"

    def _drop_catalog(self, op: Operation) -> str:
        name = self.id_name_map.get(op.target, op.target)
        return f"DROP CATALOG IF EXISTS {self.escape_identifier(name)}"

    # Schema operations
    def _add_schema(self, op: Operation) -> str:
        catalog_name = self.id_name_map.get(op.payload["catalogId"], "unknown")
        schema_name = op.payload["name"]
        catalog_esc = self.escape_identifier(catalog_name)
        schema_esc = self.escape_identifier(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {catalog_esc}.{schema_esc}"

    def _rename_schema(self, op: Operation) -> str:
        old_fqn = self.id_name_map.get(op.target, "unknown.unknown")
        parts = old_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        new_name = op.payload["newName"]

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, schema_name)
        new_esc = self._build_fqn(catalog_name, new_name)

        return f"ALTER SCHEMA {old_esc} RENAME TO {new_esc}"

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

        # Use _build_fqn for consistent formatting
        fqn_esc = self._build_fqn(catalog_name, schema_name, table_name)

        # Create empty table (columns added via add_column ops)
        return f"CREATE TABLE IF NOT EXISTS {fqn_esc} () USING {table_format}"

    def _rename_table(self, op: Operation) -> str:
        old_fqn = self.id_name_map.get(op.target, "unknown.unknown.unknown")
        parts = old_fqn.split(".")
        catalog_name = parts[0]
        schema_name = parts[1] if len(parts) > 1 else "unknown"
        table_name = parts[2] if len(parts) > 2 else "unknown"
        new_name = op.payload["newName"]

        # Use _build_fqn for consistent formatting
        old_esc = self._build_fqn(catalog_name, schema_name, table_name)
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
        return f"ALTER TABLE {fqn_esc} SET COMMENT '{comment}'"

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

    # Column operations
    def _add_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = op.payload["name"]
        col_type = op.payload["type"]
        nullable = op.payload["nullable"]
        comment = op.payload.get("comment", "")

        null_clause = "" if nullable else " NOT NULL"
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
        col_esc = self.escape_identifier(col_name)

        sql = f"ALTER TABLE {table_esc} ADD COLUMN {col_esc} {col_type}"
        return f"{sql}{null_clause}{comment_clause}"

    def _rename_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        old_name = self.id_name_map.get(op.target, "unknown")
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER TABLE {table_esc} RENAME COLUMN {old_esc} TO {new_esc}"

    def _drop_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(op.target, "unknown")
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} DROP COLUMN {col_esc}"

    def _reorder_columns(self, op: Operation) -> str:
        # Databricks doesn't support direct column reordering
        return "-- Column reordering not directly supported in Databricks SQL"

    def _batch_reorder_operations(self, ops: List[Operation]) -> Dict:
        """
        Batch reorder_columns operations by table to generate minimal SQL.

        Returns dict mapping table_id to:
        {
            "original_order": [...],  # Column order before any reorder ops
            "final_order": [...],     # Column order after all reorder ops
            "op_ids": [...]          # List of operation IDs involved
        }
        """
        reorder_batches = {}

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

    def _get_table_column_order(self, table_id: str) -> List[str]:
        """Get current column order for a table from state"""
        for catalog in self.state["catalogs"]:
            for schema in catalog.get("schemas", []):
                for table in schema.get("tables", []):
                    if table["id"] == table_id:
                        return [col["id"] for col in table.get("columns", [])]
        return []

    def _generate_optimized_reorder_sql(
        self, table_id: str, original_order: List[str], final_order: List[str], op_ids: List[str]
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

    def _batch_table_operations(self, ops: List[Operation]) -> Dict:
        """
        Batch operations by table to generate optimal DDL.

        Groups table-related operations (add_table, add_column, reorder_columns,
        set_table_property, etc.) to generate complete CREATE TABLE statements
        for new tables or efficient ALTER statements for existing tables.

        Returns dict mapping table_id to batch info.
        """
        table_batches: Dict[str, Any] = {}

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

    def _generate_optimized_table_sql(self, table_id: str, batch_info: Dict) -> str:
        """Generate optimal SQL for table operations"""

        if batch_info["is_new_table"]:
            # Generate complete CREATE TABLE statement
            return self._generate_create_table_with_columns(table_id, batch_info)
        else:
            # Generate optimized ALTER statements for existing table
            return self._generate_alter_statements_for_table(table_id, batch_info)

    def _generate_create_table_with_columns(self, table_id: str, batch_info: Dict) -> str:
        """Generate complete CREATE TABLE statement with all columns included"""
        table_op = batch_info["table_op"]
        column_ops = batch_info["column_ops"]
        property_ops = batch_info["property_ops"]
        reorder_ops = batch_info.get("reorder_ops", [])

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

        # Build column definitions as a dictionary (by column ID)
        columns_dict = {}
        for col_op in column_ops:
            col_id = col_op.payload.get("colId")
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
                columns_dict[col_op.payload.get("colId")]
                for col_op in column_ops
                if col_op.payload.get("colId") in columns_dict
            ]

        # Build table format
        table_format = table_op.payload.get("format", "DELTA").upper()

        # Build table properties
        properties = []
        for prop_op in property_ops:
            if prop_op.op.endswith("set_table_property"):
                key = prop_op.payload["key"]
                value = prop_op.payload["value"]
                properties.append(f"'{key}' = '{self.escape_string(value)}'")

        # Build table comment
        table_comment = ""
        if table_op.payload.get("comment"):
            table_comment = f" COMMENT '{self.escape_string(table_op.payload['comment'])}'"

        # Assemble CREATE TABLE statement
        columns_sql = ",\n".join(columns) if columns else ""
        properties_sql = f"\nTBLPROPERTIES ({', '.join(properties)})" if properties else ""

        if columns_sql:
            return f"""CREATE TABLE IF NOT EXISTS {table_esc} (
{columns_sql}
) USING {table_format}{table_comment}{properties_sql}"""
        else:
            # No columns yet - create empty table (fallback to original behavior)
            return (
                f"CREATE TABLE IF NOT EXISTS {table_esc} () "
                f"USING {table_format}{table_comment}{properties_sql}"
            )

    def _generate_alter_statements_for_table(self, table_id: str, batch_info: Dict) -> str:
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
                nullable = op.payload["nullable"]
                comment = op.payload.get("comment", "")

                null_clause = "" if nullable else " NOT NULL"
                comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
                col_esc = self.escape_identifier(col_name)

                column_defs.append(f"    {col_esc} {col_type}{null_clause}{comment_clause}")

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

        # Handle other column operations (non-ADD COLUMN)
        other_column_ops = [
            op for op in batch_info["column_ops"] if not op.op.endswith("add_column")
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
        col_name = self.id_name_map.get(op.target, "unknown")
        tag_name = op.payload["tagName"]
        tag_value = self.escape_string(op.payload["tagValue"])
        col_esc = self.escape_identifier(col_name)
        sql = f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc}"
        return f"{sql} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_column_tag(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        table_esc = self._build_fqn(*table_fqn.split("."))
        col_name = self.id_name_map.get(op.target, "unknown")
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
