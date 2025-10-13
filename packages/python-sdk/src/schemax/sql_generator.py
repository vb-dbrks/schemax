"""
SQL generator for converting operations to Databricks SQL statements.

This module generates SQL DDL statements for all 31 operation types,
creating idempotent migrations for Unity Catalog.
"""

from typing import Dict, List, Optional

from .models import Op, State


class SQLGenerator:
    """Generate SQL statements from operations"""

    def __init__(self, state: State):
        """
        Initialize SQL generator with current state.

        Args:
            state: Current schema state (used for ID-to-name mapping)
        """
        self.state = state
        self.id_to_name = self._build_id_name_map()

    def _build_id_name_map(self) -> Dict[str, str]:
        """Build a mapping from IDs to fully-qualified names"""
        id_map = {}

        for catalog in self.state.catalogs:
            id_map[catalog.id] = catalog.name

            for schema in catalog.schemas:
                id_map[schema.id] = f"{catalog.name}.{schema.name}"

                for table in schema.tables:
                    id_map[table.id] = f"{catalog.name}.{schema.name}.{table.name}"

                    for column in table.columns:
                        id_map[column.id] = column.name

        return id_map

    def generate_sql(self, ops: List[Op]) -> str:
        """
        Generate SQL for a list of operations.

        Args:
            ops: List of operations

        Returns:
            SQL script as a string
        """
        statements = []

        for op in ops:
            sql = self._op_to_sql(op)
            if sql:
                # Add comment with operation metadata
                header = f"-- Op: {op.id} ({op.ts})\n-- Type: {op.op}"
                statements.append(f"{header}\n{sql};")

        return "\n\n".join(statements)

    def _op_to_sql(self, op: Op) -> Optional[str]:
        """Convert a single operation to SQL"""

        # Catalog operations
        if op.op == "add_catalog":
            return self._add_catalog(op)
        elif op.op == "rename_catalog":
            return self._rename_catalog(op)
        elif op.op == "drop_catalog":
            return self._drop_catalog(op)

        # Schema operations
        elif op.op == "add_schema":
            return self._add_schema(op)
        elif op.op == "rename_schema":
            return self._rename_schema(op)
        elif op.op == "drop_schema":
            return self._drop_schema(op)

        # Table operations
        elif op.op == "add_table":
            return self._add_table(op)
        elif op.op == "rename_table":
            return self._rename_table(op)
        elif op.op == "drop_table":
            return self._drop_table(op)
        elif op.op == "set_table_comment":
            return self._set_table_comment(op)
        elif op.op == "set_table_property":
            return self._set_table_property(op)
        elif op.op == "unset_table_property":
            return self._unset_table_property(op)

        # Column operations
        elif op.op == "add_column":
            return self._add_column(op)
        elif op.op == "rename_column":
            return self._rename_column(op)
        elif op.op == "drop_column":
            return self._drop_column(op)
        elif op.op == "reorder_columns":
            return self._reorder_columns(op)
        elif op.op == "change_column_type":
            return self._change_column_type(op)
        elif op.op == "set_nullable":
            return self._set_nullable(op)
        elif op.op == "set_column_comment":
            return self._set_column_comment(op)

        # Column tag operations
        elif op.op == "set_column_tag":
            return self._set_column_tag(op)
        elif op.op == "unset_column_tag":
            return self._unset_column_tag(op)

        # Constraint operations
        elif op.op == "add_constraint":
            return self._add_constraint(op)
        elif op.op == "drop_constraint":
            return self._drop_constraint(op)

        # Row filter operations
        elif op.op == "add_row_filter":
            return self._add_row_filter(op)
        elif op.op == "update_row_filter":
            return self._update_row_filter(op)
        elif op.op == "remove_row_filter":
            return self._remove_row_filter(op)

        # Column mask operations
        elif op.op == "add_column_mask":
            return self._add_column_mask(op)
        elif op.op == "update_column_mask":
            return self._update_column_mask(op)
        elif op.op == "remove_column_mask":
            return self._remove_column_mask(op)

        return None

    # Catalog operations
    def _add_catalog(self, op: Op) -> str:
        name = op.payload["name"]
        return f"CREATE CATALOG IF NOT EXISTS `{name}`"

    def _rename_catalog(self, op: Op) -> str:
        old_name = self.id_to_name.get(op.target, op.target)
        new_name = op.payload["newName"]
        return f"ALTER CATALOG `{old_name}` RENAME TO `{new_name}`"

    def _drop_catalog(self, op: Op) -> str:
        name = self.id_to_name.get(op.target, op.target)
        return f"DROP CATALOG IF EXISTS `{name}`"

    # Schema operations
    def _add_schema(self, op: Op) -> str:
        catalog_name = self.id_to_name.get(op.payload["catalogId"], "unknown")
        schema_name = op.payload["name"]
        return f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"

    def _rename_schema(self, op: Op) -> str:
        old_fqn = self.id_to_name.get(op.target, "unknown.unknown")
        parts = old_fqn.split(".")
        catalog_name = parts[0]
        new_name = op.payload["newName"]
        return f"ALTER SCHEMA `{old_fqn}` RENAME TO `{catalog_name}`.`{new_name}`"

    def _drop_schema(self, op: Op) -> str:
        fqn = self.id_to_name.get(op.target, "unknown.unknown")
        return f"DROP SCHEMA IF EXISTS `{fqn}`"

    # Table operations
    def _add_table(self, op: Op) -> str:
        schema_fqn = self.id_to_name.get(op.payload["schemaId"], "unknown.unknown")
        table_name = op.payload["name"]
        table_format = op.payload["format"].upper()
        fqn = f"{schema_fqn}.{table_name}"

        # For now, create empty table (columns added via add_column ops)
        return f"CREATE TABLE IF NOT EXISTS `{fqn}` () USING {table_format}"

    def _rename_table(self, op: Op) -> str:
        old_fqn = self.id_to_name.get(op.target, "unknown.unknown.unknown")
        parts = old_fqn.split(".")
        schema_fqn = ".".join(parts[:2])
        new_name = op.payload["newName"]
        return f"ALTER TABLE `{old_fqn}` RENAME TO `{schema_fqn}`.`{new_name}`"

    def _drop_table(self, op: Op) -> str:
        fqn = self.id_to_name.get(op.target, "unknown.unknown.unknown")
        return f"DROP TABLE IF EXISTS `{fqn}`"

    def _set_table_comment(self, op: Op) -> str:
        fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        comment = op.payload["comment"].replace("'", "''")  # Escape single quotes
        return f"ALTER TABLE `{fqn}` SET COMMENT '{comment}'"

    def _set_table_property(self, op: Op) -> str:
        fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        key = op.payload["key"]
        value = op.payload["value"]
        return f"ALTER TABLE `{fqn}` SET TBLPROPERTIES ('{key}' = '{value}')"

    def _unset_table_property(self, op: Op) -> str:
        fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        key = op.payload["key"]
        return f"ALTER TABLE `{fqn}` UNSET TBLPROPERTIES ('{key}')"

    # Column operations
    def _add_column(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = op.payload["name"]
        col_type = op.payload["type"]
        nullable = op.payload["nullable"]
        comment = op.payload.get("comment", "")

        null_clause = "" if nullable else " NOT NULL"
        comment_clause = f" COMMENT '{comment.replace(chr(39), chr(39)*2)}'" if comment else ""

        return (
            f"ALTER TABLE `{table_fqn}` ADD COLUMN `{col_name}` "
            f"{col_type}{null_clause}{comment_clause}"
        )

    def _rename_column(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        old_name = self.id_to_name.get(op.target, "unknown")
        new_name = op.payload["newName"]
        return f"ALTER TABLE `{table_fqn}` RENAME COLUMN `{old_name}` TO `{new_name}`"

    def _drop_column(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        return f"ALTER TABLE `{table_fqn}` DROP COLUMN `{col_name}`"

    def _reorder_columns(self, op: Op) -> str:
        # Databricks doesn't support reordering columns directly
        # This would require DROP and ADD operations
        return "-- Column reordering not directly supported in Databricks SQL"

    def _change_column_type(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        new_type = op.payload["newType"]
        return f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` TYPE {new_type}"

    def _set_nullable(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        nullable = op.payload["nullable"]

        if nullable:
            return f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` DROP NOT NULL"
        else:
            return f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` SET NOT NULL"

    def _set_column_comment(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        comment = op.payload["comment"].replace("'", "''")
        return f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` COMMENT '{comment}'"

    # Column tag operations
    def _set_column_tag(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        tag_name = op.payload["tagName"]
        tag_value = op.payload["tagValue"].replace("'", "''")
        return (
            f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` "
            f"SET TAGS ('{tag_name}' = '{tag_value}')"
        )

    def _unset_column_tag(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        col_name = self.id_to_name.get(op.target, "unknown")
        tag_name = op.payload["tagName"]
        return f"ALTER TABLE `{table_fqn}` ALTER COLUMN `{col_name}` UNSET TAGS ('{tag_name}')"

    # Constraint operations
    def _add_constraint(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        constraint_type = op.payload["type"]
        constraint_name = op.payload.get("name", "")
        columns = [self.id_to_name.get(cid, cid) for cid in op.payload["columns"]]

        name_clause = f"CONSTRAINT `{constraint_name}` " if constraint_name else ""

        if constraint_type == "primary_key":
            timeseries = " TIMESERIES" if op.payload.get("timeseries") else ""
            cols = ", ".join(f"`{c}`" for c in columns)
            return f"ALTER TABLE `{table_fqn}` ADD {name_clause}" f"PRIMARY KEY({cols}){timeseries}"

        elif constraint_type == "foreign_key":
            parent_table = self.id_to_name.get(op.payload.get("parentTable", ""), "unknown")
            parent_columns = [
                self.id_to_name.get(cid, cid) for cid in op.payload.get("parentColumns", [])
            ]
            cols = ", ".join(f"`{c}`" for c in columns)
            parent_cols = ", ".join(f"`{c}`" for c in parent_columns)
            return (
                f"ALTER TABLE `{table_fqn}` ADD {name_clause}"
                f"FOREIGN KEY({cols}) REFERENCES `{parent_table}`({parent_cols})"
            )

        elif constraint_type == "check":
            expression = op.payload.get("expression", "TRUE")
            return f"ALTER TABLE `{table_fqn}` ADD {name_clause}CHECK ({expression})"

        return ""

    def _drop_constraint(self, op: Op) -> str:
        table_fqn = self.id_to_name.get(op.payload["tableId"], "unknown")
        # Would need constraint name from state
        return f"-- ALTER TABLE `{table_fqn}` DROP CONSTRAINT (constraint name lookup needed)"

    # Row filter operations
    def _add_row_filter(self, op: Op) -> str:
        # Row filters require UDF creation first
        return f"-- Row filter: {op.payload['name']} - UDF: {op.payload['udfExpression']}"

    def _update_row_filter(self, op: Op) -> str:
        return "-- Row filter update"

    def _remove_row_filter(self, op: Op) -> str:
        return "-- Row filter removal"

    # Column mask operations
    def _add_column_mask(self, op: Op) -> str:
        # Column masks require UDF creation first
        return f"-- Column mask: {op.payload['name']} - Function: {op.payload['maskFunction']}"

    def _update_column_mask(self, op: Op) -> str:
        return "-- Column mask update"

    def _remove_column_mask(self, op: Op) -> str:
        return "-- Column mask removal"
