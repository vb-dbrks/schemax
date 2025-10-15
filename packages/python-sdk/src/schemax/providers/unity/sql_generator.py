"""
Unity Catalog SQL Generator

Generates Databricks SQL DDL statements from operations.
Migrated from TypeScript sql-generator.ts
"""

from typing import Dict, List

from ..base.operations import Operation
from ..base.sql_generator import BaseSQLGenerator, SQLGenerationResult
from .models import UnityState
from .operations import UNITY_OPERATIONS


class UnitySQLGenerator(BaseSQLGenerator):
    """Unity Catalog SQL Generator"""

    def __init__(self, state: UnityState):
        super().__init__(state)
        self.id_name_map = self._build_id_name_map()

    def _build_id_name_map(self) -> Dict[str, str]:
        """Build a mapping from IDs to fully-qualified names"""
        id_map = {}

        for catalog in self.state["catalogs"]:
            id_map[catalog["id"]] = catalog["name"]

            for schema in catalog.get("schemas", []):
                id_map[schema["id"]] = f"{catalog['name']}.{schema['name']}"

                for table in schema.get("tables", []):
                    table_fqn = f"{catalog['name']}.{schema['name']}.{table['name']}"
                    id_map[table["id"]] = table_fqn

                    for column in table.get("columns", []):
                        id_map[column["id"]] = column["name"]

        return id_map

    def can_generate_sql(self, op: Operation) -> bool:
        """Check if operation can be converted to SQL"""
        return op.op in UNITY_OPERATIONS.values()

    def generate_sql(self, ops: List[Operation]) -> str:
        """
        Generate SQL statements with comprehensive batch optimizations.

        Optimizations include:
        - Complete CREATE TABLE statements (no empty tables + ALTERs)
        - Batched column reordering (minimal ALTER statements)
        - Table property consolidation
        """
        # Pre-process: batch operations by table and type
        table_batches = self._batch_table_operations(ops)
        processed_op_ids = set()
        statements = []

        # Generate SQL for batched table operations
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
                statements.append(f"{header}\n{table_sql};")

        # Process remaining operations normally (catalogs, schemas, etc.)
        for op in ops:
            if op.id in processed_op_ids:
                continue  # Skip already processed table operations

            if not self.can_generate_sql(op):
                print(f"Warning: Cannot generate SQL for operation: {op.op}")
                continue

            result = self.generate_sql_for_operation(op)

            # Add header comment with operation metadata
            header = f"-- Operation: {op.id} ({op.ts})\n-- Type: {op.op}"

            # Add warnings if any
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
        name = op.payload["name"]
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
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_fqn)
        cat_esc = self.escape_identifier(catalog_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER SCHEMA {old_esc} RENAME TO {cat_esc}.{new_esc}"

    def _drop_schema(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.target, "unknown.unknown")
        return f"DROP SCHEMA IF EXISTS {self.escape_identifier(fqn)}"

    # Table operations
    def _add_table(self, op: Operation) -> str:
        schema_fqn = self.id_name_map.get(op.payload["schemaId"], "unknown.unknown")
        table_name = op.payload["name"]
        table_format = op.payload["format"].upper()
        fqn = f"{schema_fqn}.{table_name}"

        # Create empty table (columns added via add_column ops)
        return f"CREATE TABLE IF NOT EXISTS {self.escape_identifier(fqn)} () USING {table_format}"

    def _rename_table(self, op: Operation) -> str:
        old_fqn = self.id_name_map.get(op.target, "unknown.unknown.unknown")
        parts = old_fqn.split(".")
        schema_fqn = ".".join(parts[:2])
        new_name = op.payload["newName"]
        old_esc = self.escape_identifier(old_fqn)
        schema_esc = self.escape_identifier(schema_fqn)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER TABLE {old_esc} RENAME TO {schema_esc}.{new_esc}"

    def _drop_table(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.target, "unknown.unknown.unknown")
        return f"DROP TABLE IF EXISTS {self.escape_identifier(fqn)}"

    def _set_table_comment(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        comment = self.escape_string(op.payload["comment"])
        return f"ALTER TABLE {self.escape_identifier(fqn)} SET COMMENT '{comment}'"

    def _set_table_property(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        key = op.payload["key"]
        value = op.payload["value"]
        return f"ALTER TABLE {self.escape_identifier(fqn)} SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_table_property(self, op: Operation) -> str:
        fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        key = op.payload["key"]
        return f"ALTER TABLE {self.escape_identifier(fqn)} UNSET TBLPROPERTIES ('{key}')"

    # Column operations
    def _add_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = op.payload["name"]
        col_type = op.payload["type"]
        nullable = op.payload["nullable"]
        comment = op.payload.get("comment", "")

        null_clause = "" if nullable else " NOT NULL"
        comment_clause = f" COMMENT '{self.escape_string(comment)}'" if comment else ""
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)

        sql = f"ALTER TABLE {table_esc} ADD COLUMN {col_esc} {col_type}"
        return f"{sql}{null_clause}{comment_clause}"

    def _rename_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        old_name = self.id_name_map.get(op.target, "unknown")
        new_name = op.payload["newName"]
        table_esc = self.escape_identifier(table_fqn)
        old_esc = self.escape_identifier(old_name)
        new_esc = self.escape_identifier(new_name)
        return f"ALTER TABLE {table_esc} RENAME COLUMN {old_esc} TO {new_esc}"

    def _drop_column(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = self.id_name_map.get(op.target, "unknown")
        table_esc = self.escape_identifier(table_fqn)
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
                        "op_ids": []
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
        table_esc = self.escape_identifier(table_fqn)

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
        table_batches = {}

        for op in ops:
            op_type = op.op.replace("unity.", "")

            # Identify table-related operations
            table_id = None
            if op_type in ["add_table", "rename_table", "drop_table", "set_table_comment"]:
                table_id = op.target
            elif op_type in ["add_column", "rename_column", "drop_column", "reorder_columns",
                           "change_column_type", "set_nullable", "set_column_comment",
                           "set_column_tag", "unset_column_tag", "set_table_property",
                           "unset_table_property"]:
                table_id = op.payload.get("tableId")
            elif op_type in ["add_constraint", "drop_constraint", "add_row_filter",
                           "update_row_filter", "remove_row_filter", "add_column_mask",
                           "update_column_mask", "remove_column_mask"]:
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
                    "operation_types": []
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
            elif op_type in ["add_row_filter", "update_row_filter", "remove_row_filter",
                           "add_column_mask", "update_column_mask", "remove_column_mask"]:
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

        if not table_op:
            return "-- Error: No table creation operation found"

        # Get table name and schema info
        table_name = table_op.payload.get("name", "unknown")
        schema_id = table_op.payload.get("schemaId")
        schema_fqn = (
            self.id_name_map.get(schema_id, "unknown.unknown")
            if schema_id else "unknown.unknown"
        )
        table_fqn = f"{schema_fqn}.{table_name}"
        table_esc = self.escape_identifier(table_fqn)

        # Build column definitions
        columns = []
        for col_op in column_ops:
            col_name = self.escape_identifier(col_op.payload["name"])
            col_type = col_op.payload["type"]
            nullable = "" if col_op.payload.get("nullable", True) else " NOT NULL"
            comment = (
                f" COMMENT '{self.escape_string(col_op.payload['comment'])}'"
                if col_op.payload.get("comment") else ""
            )
            columns.append(f"  {col_name} {col_type}{nullable}{comment}")

        # Note: reorder_columns operations are ignored for new table creation
        # Column order for CREATE TABLE is determined by the order columns were added
        # reorder_columns only applies to existing tables via ALTER statements

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
            original_order = self._get_table_column_order(table_id)
            final_order = batch_info["reorder_ops"][-1].payload["order"]
            reorder_sql = self._generate_optimized_reorder_sql(
                table_id, original_order, final_order,
                [op.id for op in batch_info["reorder_ops"]]
            )
            if reorder_sql and not reorder_sql.startswith("--"):
                statements.append(reorder_sql)

        # Handle other operations normally
        for op in (batch_info["column_ops"] + batch_info["property_ops"] +
                  batch_info["constraint_ops"] + batch_info["governance_ops"] +
                  batch_info["other_ops"]):

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
        col_name = self.id_name_map.get(op.target, "unknown")
        new_type = op.payload["newType"]
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} TYPE {new_type}"

    def _set_nullable(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = self.id_name_map.get(op.target, "unknown")
        nullable = op.payload["nullable"]
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)

        if nullable:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} DROP NOT NULL"
        else:
            return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} SET NOT NULL"

    def _set_column_comment(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = self.id_name_map.get(op.target, "unknown")
        comment = self.escape_string(op.payload["comment"])
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} COMMENT '{comment}'"

    # Column tag operations
    def _set_column_tag(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = self.id_name_map.get(op.target, "unknown")
        tag_name = op.payload["tagName"]
        tag_value = self.escape_string(op.payload["tagValue"])
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)
        sql = f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc}"
        return f"{sql} SET TAGS ('{tag_name}' = '{tag_value}')"

    def _unset_column_tag(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        col_name = self.id_name_map.get(op.target, "unknown")
        tag_name = op.payload["tagName"]
        table_esc = self.escape_identifier(table_fqn)
        col_esc = self.escape_identifier(col_name)
        return f"ALTER TABLE {table_esc} ALTER COLUMN {col_esc} UNSET TAGS ('{tag_name}')"

    # Constraint operations
    def _add_constraint(self, op: Operation) -> str:
        table_fqn = self.id_name_map.get(op.payload["tableId"], "unknown")
        constraint_type = op.payload["type"]
        constraint_name = op.payload.get("name", "")
        columns = [self.id_name_map.get(cid, cid) for cid in op.payload["columns"]]

        name_clause = (
            f"CONSTRAINT {self.escape_identifier(constraint_name)} " if constraint_name else ""
        )
        table_esc = self.escape_identifier(table_fqn)

        if constraint_type == "primary_key":
            timeseries = " TIMESERIES" if op.payload.get("timeseries") else ""
            cols = ", ".join(self.escape_identifier(c) for c in columns)
            return f"ALTER TABLE {table_esc} ADD {name_clause}PRIMARY KEY({cols}){timeseries}"

        elif constraint_type == "foreign_key":
            parent_table = self.id_name_map.get(op.payload.get("parentTable", ""), "unknown")
            parent_columns = [
                self.id_name_map.get(cid, cid) for cid in op.payload.get("parentColumns", [])
            ]
            cols = ", ".join(self.escape_identifier(c) for c in columns)
            parent_cols = ", ".join(self.escape_identifier(c) for c in parent_columns)
            parent_esc = self.escape_identifier(parent_table)
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
        # Would need constraint name from state
        table_esc = self.escape_identifier(table_fqn)
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
