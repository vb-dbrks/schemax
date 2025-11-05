"""
Unity Catalog State Differ

Compares two Unity Catalog states and generates operations representing the changes.
Handles catalogs, schemas, tables, and columns.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from ..base.operations import Operation
from ..base.state_differ import StateDiffer


class UnityStateDiffer(StateDiffer):
    """Unity Catalog state differ

    Compares two Unity Catalog states and generates Unity-specific operations
    for the differences (add/remove/rename catalogs, schemas, tables, columns).
    """

    def generate_diff_operations(self) -> list[Operation]:
        """Generate operations representing the diff between states

        Returns:
            List of Unity Catalog operations (add_catalog, add_schema, etc.)
        """
        ops: list[Operation] = []

        # Compare catalogs
        ops.extend(self._diff_catalogs())

        return ops

    def _diff_catalogs(self) -> list[Operation]:
        """Compare catalogs between old and new state"""
        ops: list[Operation] = []

        old_state_dict = self.old_state if isinstance(self.old_state, dict) else {}
        new_state_dict = self.new_state if isinstance(self.new_state, dict) else {}

        old_cats = self._build_id_map(old_state_dict.get("catalogs", []))
        new_cats = self._build_id_map(new_state_dict.get("catalogs", []))

        # Detect added catalogs
        for cat_id, cat in new_cats.items():
            if cat_id not in old_cats:
                ops.append(self._create_add_catalog_op(cat))
                # Add all schemas in this new catalog
                ops.extend(self._add_all_schemas_in_catalog(cat_id, cat))
            else:
                # Catalog exists in both - check for changes
                old_cat = old_cats[cat_id]

                # Check for rename
                if old_cat["name"] != cat["name"]:
                    if self._detect_rename(
                        cat_id, cat_id, old_cat["name"], cat["name"], "rename_catalog"
                    ):
                        ops.append(
                            self._create_rename_catalog_op(cat_id, old_cat["name"], cat["name"])
                        )

                # Compare schemas within catalog
                ops.extend(self._diff_schemas(cat_id, old_cat, cat))

        # Detect removed catalogs
        for cat_id, cat in old_cats.items():
            if cat_id not in new_cats:
                ops.append(self._create_drop_catalog_op(cat))

        return ops

    def _diff_schemas(
        self, catalog_id: str, old_catalog: dict[str, Any], new_catalog: dict[str, Any]
    ) -> list[Operation]:
        """Compare schemas within a catalog"""
        ops: list[Operation] = []

        old_schemas = self._build_id_map(old_catalog.get("schemas", []))
        new_schemas = self._build_id_map(new_catalog.get("schemas", []))

        # Detect added schemas
        for sch_id, sch in new_schemas.items():
            if sch_id not in old_schemas:
                ops.append(self._create_add_schema_op(sch, catalog_id))
                # Add all tables in this new schema
                ops.extend(self._add_all_tables_in_schema(sch_id, sch))
            else:
                # Schema exists in both - check for changes
                old_sch = old_schemas[sch_id]

                # Check for rename
                if old_sch["name"] != sch["name"]:
                    if self._detect_rename(
                        sch_id, sch_id, old_sch["name"], sch["name"], "rename_schema"
                    ):
                        ops.append(
                            self._create_rename_schema_op(sch_id, old_sch["name"], sch["name"])
                        )

                # Compare tables within schema
                ops.extend(self._diff_tables(sch_id, old_sch, sch))

        # Detect removed schemas
        for sch_id, sch in old_schemas.items():
            if sch_id not in new_schemas:
                ops.append(self._create_drop_schema_op(sch))

        return ops

    def _diff_tables(
        self, schema_id: str, old_schema: dict[str, Any], new_schema: dict[str, Any]
    ) -> list[Operation]:
        """Compare tables within a schema"""
        ops: list[Operation] = []

        old_tables = self._build_id_map(old_schema.get("tables", []))
        new_tables = self._build_id_map(new_schema.get("tables", []))

        # Detect added tables
        for tbl_id, tbl in new_tables.items():
            if tbl_id not in old_tables:
                ops.append(self._create_add_table_op(tbl, schema_id))
                # Add all columns in this new table
                ops.extend(self._add_all_columns_in_table(tbl_id, tbl))
            else:
                # Table exists in both - check for changes
                old_tbl = old_tables[tbl_id]

                # Check for rename
                if old_tbl["name"] != tbl["name"]:
                    if self._detect_rename(
                        tbl_id, tbl_id, old_tbl["name"], tbl["name"], "rename_table"
                    ):
                        ops.append(
                            self._create_rename_table_op(tbl_id, old_tbl["name"], tbl["name"])
                        )

                # Check for table comment change
                if old_tbl.get("comment") != tbl.get("comment"):
                    ops.append(self._create_set_table_comment_op(tbl_id, tbl.get("comment")))

                # Check for table property changes (TBLPROPERTIES / tags)
                ops.extend(
                    self._diff_table_properties(
                        tbl_id, old_tbl.get("properties", {}), tbl.get("properties", {})
                    )
                )

                # Check for table tag changes
                ops.extend(
                    self._diff_table_tags(
                        tbl_id, old_tbl.get("tags", {}), tbl.get("tags", {})
                    )
                )

                # Compare columns within table
                ops.extend(self._diff_columns(tbl_id, old_tbl, tbl))

        # Detect removed tables
        for tbl_id, tbl in old_tables.items():
            if tbl_id not in new_tables:
                ops.append(self._create_drop_table_op(tbl))

        return ops

    def _diff_columns(
        self, table_id: str, old_table: dict[str, Any], new_table: dict[str, Any]
    ) -> list[Operation]:
        """Compare columns within a table"""
        ops: list[Operation] = []

        old_columns = self._build_id_map(old_table.get("columns", []))
        new_columns = self._build_id_map(new_table.get("columns", []))

        # Detect added columns
        for col_id, col in new_columns.items():
            if col_id not in old_columns:
                # Safety check - skip columns missing required fields
                if all(key in col for key in ["id", "name", "type"]):
                    ops.append(self._create_add_column_op(col, table_id))
            else:
                # Column exists in both - check for changes
                old_col = old_columns[col_id]
                
                try:

                    # Check for rename (only if both have name)
                    if "name" in old_col and "name" in col and old_col["name"] != col["name"]:
                        if self._detect_rename(
                            col_id, col_id, old_col["name"], col["name"], "rename_column"
                        ):
                            ops.append(
                                self._create_rename_column_op(
                                    col_id, table_id, old_col["name"], col["name"]
                                )
                            )

                    # Check for type change
                    if old_col.get("type") != col.get("type") and "type" in col:
                        ops.append(self._create_change_column_type_op(col_id, table_id, col["type"]))

                    # Check for nullable change
                    if old_col.get("nullable") != col.get("nullable"):
                        ops.append(self._create_set_nullable_op(col_id, table_id, col["nullable"]))

                    # Check for comment change
                    if old_col.get("comment") != col.get("comment"):
                        ops.append(
                            self._create_set_column_comment_op(col_id, table_id, col.get("comment"))
                        )

                    # Check for column tag changes (only if column has name)
                    if "name" in col:
                        ops.extend(
                            self._diff_column_tags(
                                col_id,
                                table_id,
                                col["name"],  # Pass column name for SQL generation
                                old_col.get("tags", {}),
                                col.get("tags", {}),
                            )
                        )
                except Exception:
                    # Re-raise to maintain error handling behavior
                    raise

        # Detect removed columns
        for col_id, col in old_columns.items():
            if col_id not in new_columns:
                ops.append(self._create_drop_column_op(col, table_id))

        return ops

    def _diff_table_properties(
        self, table_id: str, old_props: dict[str, Any], new_props: dict[str, Any]
    ) -> list[Operation]:
        """Compare table properties (TBLPROPERTIES)"""
        ops: list[Operation] = []

        # Added or updated properties
        for key, value in new_props.items():
            if key not in old_props or old_props[key] != value:
                ops.append(self._create_set_table_property_op(table_id, key, value))

        # Removed properties
        for key in old_props:
            if key not in new_props:
                ops.append(self._create_unset_table_property_op(table_id, key))

        return ops

    def _diff_table_tags(
        self, table_id: str, old_tags: dict[str, Any], new_tags: dict[str, Any]
    ) -> list[Operation]:
        """Compare table tags (Unity Catalog governance tags)"""
        ops: list[Operation] = []

        # Added or updated tags
        for tag_name, tag_value in new_tags.items():
            if tag_name not in old_tags or old_tags[tag_name] != tag_value:
                ops.append(self._create_set_table_tag_op(table_id, tag_name, str(tag_value)))

        # Removed tags
        for tag_name in old_tags:
            if tag_name not in new_tags:
                ops.append(self._create_unset_table_tag_op(table_id, tag_name))

        return ops

    def _diff_column_tags(
        self,
        column_id: str,
        table_id: str,
        column_name: str,
        old_tags: dict[str, Any],
        new_tags: dict[str, Any],
    ) -> list[Operation]:
        """Compare column tags
        
        Args:
            column_id: Column ID
            table_id: Table ID
            column_name: Column name (for SQL generation)
            old_tags: Old tags dict
            new_tags: New tags dict
        """
        ops: list[Operation] = []

        # Added or updated tags
        for tag_name, tag_value in new_tags.items():
            if tag_name not in old_tags or old_tags[tag_name] != tag_value:
                ops.append(
                    self._create_set_column_tag_op(
                        column_id, table_id, column_name, tag_name, str(tag_value)
                    )
                )

        # Removed tags
        for tag_name in old_tags:
            if tag_name not in new_tags:
                ops.append(
                    self._create_unset_column_tag_op(column_id, table_id, column_name, tag_name)
                )

        return ops

    # Helper methods for adding all objects in newly created containers

    def _add_all_schemas_in_catalog(
        self, catalog_id: str, catalog: dict[str, Any]
    ) -> list[Operation]:
        """Add all schemas in a newly created catalog"""
        ops: list[Operation] = []

        for schema in catalog.get("schemas", []):
            ops.append(self._create_add_schema_op(schema, catalog_id))
            # Add all tables in this new schema
            ops.extend(self._add_all_tables_in_schema(schema["id"], schema))

        return ops

    def _add_all_tables_in_schema(self, schema_id: str, schema: dict[str, Any]) -> list[Operation]:
        """Add all tables in a newly created schema"""
        ops: list[Operation] = []

        for table in schema.get("tables", []):
            ops.append(self._create_add_table_op(table, schema_id))
            # Add all columns in this new table
            ops.extend(self._add_all_columns_in_table(table["id"], table))
            # Add all tags for this table
            ops.extend(self._add_all_tags_for_table(table["id"], table))

        return ops

    def _add_all_columns_in_table(self, table_id: str, table: dict[str, Any]) -> list[Operation]:
        """Add all columns in a newly created table"""
        ops: list[Operation] = []

        for column in table.get("columns", []):
            # Safety check - skip columns missing required fields
            if not all(key in column for key in ["id", "name", "type"]):
                continue
                
            ops.append(self._create_add_column_op(column, table_id))
            
            # Add column tags if present
            if column.get("tags"):
                column_name = column.get("name")
                if column_name:  # Safety check - column must have name
                    for tag_name, tag_value in column["tags"].items():
                        ops.append(
                            self._create_set_column_tag_op(
                                column["id"], table_id, column_name, tag_name, str(tag_value)
                            )
                        )

        return ops
    
    def _add_all_tags_for_table(self, table_id: str, table: dict[str, Any]) -> list[Operation]:
        """Add all tags for a newly created table"""
        ops: list[Operation] = []
        
        for tag_name, tag_value in table.get("tags", {}).items():
            ops.append(self._create_set_table_tag_op(table_id, tag_name, str(tag_value)))
        
        return ops

    # Operation creation helpers

    def _create_add_catalog_op(self, catalog: dict[str, Any]) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_catalog",
            target=catalog["id"],
            payload={"catalogId": catalog["id"], "name": catalog["name"]},
        )

    def _create_rename_catalog_op(self, catalog_id: str, old_name: str, new_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_catalog",
            target=catalog_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_drop_catalog_op(self, catalog: dict[str, Any]) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_catalog",
            target=catalog["id"],
            payload={},
        )

    def _create_add_schema_op(self, schema: dict[str, Any], catalog_id: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_schema",
            target=schema["id"],
            payload={
                "schemaId": schema["id"],
                "name": schema["name"],
                "catalogId": catalog_id,
            },
        )

    def _create_rename_schema_op(self, schema_id: str, old_name: str, new_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_schema",
            target=schema_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_drop_schema_op(self, schema: dict[str, Any]) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_schema",
            target=schema["id"],
            payload={},
        )

    def _create_add_table_op(self, table: dict[str, Any], schema_id: str) -> Operation:
        payload = {
            "tableId": table["id"],
            "name": table["name"],
            "schemaId": schema_id,
            "format": table.get("format", "delta"),
        }
        
        # Include optional fields if present
        if "comment" in table and table["comment"]:
            payload["comment"] = table["comment"]
        if "external" in table:
            payload["external"] = table["external"]
        if "externalLocationName" in table:
            payload["externalLocationName"] = table["externalLocationName"]
        if "path" in table:
            payload["path"] = table["path"]
        if "partitionColumns" in table:
            payload["partitionColumns"] = table["partitionColumns"]
        if "clusterColumns" in table:
            payload["clusterColumns"] = table["clusterColumns"]
        
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_table",
            target=table["id"],
            payload=payload,
        )

    def _create_rename_table_op(self, table_id: str, old_name: str, new_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_table",
            target=table_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_drop_table_op(self, table: dict[str, Any]) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_table",
            target=table["id"],
            payload={},
        )

    def _create_add_column_op(self, column: dict[str, Any], table_id: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_column",
            target=column["id"],
            payload={
                "tableId": table_id,
                "colId": column["id"],
                "name": column["name"],
                "type": column["type"],
                "nullable": column.get("nullable", True),
                "comment": column.get("comment"),
            },
        )

    def _create_rename_column_op(
        self, column_id: str, table_id: str, old_name: str, new_name: str
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_column",
            target=column_id,
            payload={
                "tableId": table_id,
                "oldName": old_name,
                "newName": new_name,
            },
        )

    def _create_change_column_type_op(
        self, column_id: str, table_id: str, new_type: str
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.change_column_type",
            target=column_id,
            payload={"tableId": table_id, "newType": new_type},
        )

    def _create_set_nullable_op(self, column_id: str, table_id: str, nullable: bool) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_nullable",
            target=column_id,
            payload={"tableId": table_id, "nullable": nullable},
        )

    def _create_set_column_comment_op(
        self, column_id: str, table_id: str, comment: str | None
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_column_comment",
            target=column_id,
            payload={"tableId": table_id, "comment": comment or ""},
        )

    def _create_drop_column_op(self, column: dict[str, Any], table_id: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_column",
            target=column["id"],
            payload={"tableId": table_id, "name": column["name"]},
        )

    def _create_set_table_comment_op(self, table_id: str, comment: str | None) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_table_comment",
            target=table_id,
            payload={"tableId": table_id, "comment": comment or ""},
        )

    def _create_set_table_property_op(self, table_id: str, key: str, value: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_table_property",
            target=table_id,
            payload={"tableId": table_id, "key": key, "value": value},
        )

    def _create_unset_table_property_op(self, table_id: str, key: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.unset_table_property",
            target=table_id,
            payload={"tableId": table_id, "key": key},
        )

    def _create_set_table_tag_op(
        self, table_id: str, tag_name: str, tag_value: str
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name, "tagValue": tag_value},
        )

    def _create_unset_table_tag_op(self, table_id: str, tag_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.unset_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name},
        )

    def _create_set_column_tag_op(
        self, column_id: str, table_id: str, column_name: str, tag_name: str, tag_value: str
    ) -> Operation:
        """Create set_column_tag operation
        
        Args:
            column_id: Column ID
            table_id: Table ID
            column_name: Column name (included as fallback for SQL generation)
            tag_name: Tag name
            tag_value: Tag value
        """
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_column_tag",
            target=column_id,
            payload={
                "tableId": table_id,
                "name": column_name,  # Include for SQL generation fallback
                "tagName": tag_name,
                "tagValue": tag_value,
            },
        )

    def _create_unset_column_tag_op(
        self, column_id: str, table_id: str, column_name: str, tag_name: str
    ) -> Operation:
        """Create unset_column_tag operation
        
        Args:
            column_id: Column ID
            table_id: Table ID
            column_name: Column name (included as fallback for SQL generation)
            tag_name: Tag name
        """
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.unset_column_tag",
            target=column_id,
            payload={
                "tableId": table_id,
                "name": column_name,  # Include for SQL generation fallback
                "tagName": tag_name,
            },
        )
