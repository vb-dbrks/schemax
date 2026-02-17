"""
Unity Catalog State Differ

Compares two Unity Catalog states and generates operations representing the changes.
Handles catalogs, schemas, tables, and columns.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from schemax.providers.base.operations import Operation
from schemax.providers.base.state_differ import StateDiffer


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

    def _state_to_dict(self, state: Any) -> dict[str, Any]:
        """Normalize state to dict for consistent access (handles Pydantic models)."""
        if state is None:
            return {}
        if isinstance(state, dict):
            return state
        if hasattr(state, "model_dump"):
            return state.model_dump(by_alias=True)
        return {}

    def _diff_catalogs(self) -> list[Operation]:
        """Compare catalogs between old and new state"""
        ops: list[Operation] = []

        old_state_dict = self._state_to_dict(self.old_state)
        new_state_dict = self._state_to_dict(self.new_state)

        old_cats = self._build_id_map(old_state_dict.get("catalogs", []))
        new_cats = self._build_id_map(new_state_dict.get("catalogs", []))

        # Detect added catalogs
        for cat_id, cat in new_cats.items():
            if cat_id not in old_cats:
                ops.append(self._create_add_catalog_op(cat))
                # Add all schemas in this new catalog
                ops.extend(self._add_all_schemas_in_catalog(cat_id, cat))
                # Add catalog grants
                ops.extend(self._diff_grants("catalog", cat_id, [], cat.get("grants", [])))
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

                # Compare catalog grants
                ops.extend(
                    self._diff_grants(
                        "catalog",
                        cat_id,
                        old_cat.get("grants", []),
                        cat.get("grants", []),
                    )
                )

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
                # Add all views in this new schema
                ops.extend(self._add_all_views_in_schema(sch_id, sch))
                # Add schema grants
                ops.extend(self._diff_grants("schema", sch_id, [], sch.get("grants", [])))
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
                ops.extend(self._diff_tables(catalog_id, sch_id, old_sch, sch))

                # Compare views within schema
                ops.extend(self._diff_views(catalog_id, sch_id, old_sch, sch))

                # Compare schema grants
                ops.extend(
                    self._diff_grants(
                        "schema",
                        sch_id,
                        old_sch.get("grants", []),
                        sch.get("grants", []),
                    )
                )

        # Detect removed schemas
        for sch_id, sch in old_schemas.items():
            if sch_id not in new_schemas:
                ops.append(self._create_drop_schema_op(sch))

        return ops

    def _diff_tables(
        self,
        catalog_id: str,
        schema_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
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
                # Add all tags for this table
                ops.extend(self._add_all_tags_for_table(tbl_id, tbl))
                # Add all constraints for this table
                ops.extend(self._add_all_constraints_for_table(tbl_id, tbl))
                # Add table grants
                ops.extend(self._diff_grants("table", tbl_id, [], tbl.get("grants", [])))
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
                    self._diff_table_tags(tbl_id, old_tbl.get("tags", {}), tbl.get("tags", {}))
                )

                # Compare columns within table
                ops.extend(self._diff_columns(tbl_id, old_tbl, tbl))

                # Compare constraints within table
                ops.extend(
                    self._diff_constraints(
                        tbl_id, old_tbl.get("constraints", []), tbl.get("constraints", [])
                    )
                )

                # Compare table grants
                ops.extend(
                    self._diff_grants(
                        "table",
                        tbl_id,
                        old_tbl.get("grants", []),
                        tbl.get("grants", []),
                    )
                )

        # Detect removed tables
        for tbl_id, tbl in old_tables.items():
            if tbl_id not in new_tables:
                ops.append(self._create_drop_table_op(tbl, catalog_id, schema_id))

        return ops

    def _diff_views(
        self,
        catalog_id: str,
        schema_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
    ) -> list[Operation]:
        """Compare views within a schema"""
        ops: list[Operation] = []

        old_views = self._build_id_map(old_schema.get("views", []))
        new_views = self._build_id_map(new_schema.get("views", []))

        # Detect added views
        for view_id, view in new_views.items():
            if view_id not in old_views:
                ops.append(self._create_add_view_op(view, schema_id))
                # Add view grants
                ops.extend(self._diff_grants("view", view_id, [], view.get("grants", [])))
            else:
                # View exists in both - check for changes
                old_view = old_views[view_id]

                # Check for rename
                if old_view["name"] != view["name"]:
                    if self._detect_rename(
                        view_id, view_id, old_view["name"], view["name"], "rename_view"
                    ):
                        ops.append(
                            self._create_rename_view_op(view_id, old_view["name"], view["name"])
                        )

                # Check for definition change
                if old_view.get("definition") != view.get("definition"):
                    ops.append(self._create_update_view_op(view_id, view))

                # Check for comment change
                if old_view.get("comment") != view.get("comment"):
                    ops.append(self._create_set_view_comment_op(view_id, view.get("comment")))

                # Compare view grants
                ops.extend(
                    self._diff_grants(
                        "view",
                        view_id,
                        old_view.get("grants", []),
                        view.get("grants", []),
                    )
                )

        # Detect removed views
        for view_id, view in old_views.items():
            if view_id not in new_views:
                ops.append(self._create_drop_view_op(view))

        return ops

    def _diff_grants(
        self,
        target_type: str,
        target_id: str,
        old_grants: list[Any],
        new_grants: list[Any],
    ) -> list[Operation]:
        """Compare grants on a securable object. Emit add_grant/revoke_grant ops.

        Grants with empty principal are skipped (invalid for GRANT/REVOKE SQL).
        """
        ops: list[Operation] = []

        def norm(g: Any) -> tuple[str, list[str]]:
            if isinstance(g, dict):
                principal = (g.get("principal") or "").strip()
                privs = g.get("privileges") or []
                return (principal, list(privs) if isinstance(privs, list) else [])
            return ("", [])

        def valid_principal(principal: str) -> bool:
            """Explicitly reject empty principal (invalid for SQL; skip in diff)."""
            return bool(principal and principal.strip())

        old_map = {
            norm(g)[0]: norm(g)[1]
            for g in (old_grants or [])
            if valid_principal(norm(g)[0])
        }
        new_map = {
            norm(g)[0]: norm(g)[1]
            for g in (new_grants or [])
            if valid_principal(norm(g)[0])
        }

        all_principals = set(old_map) | set(new_map)
        for principal in all_principals:
            old_privs = set(old_map.get(principal, []))
            new_privs = set(new_map.get(principal, []))
            removed = old_privs - new_privs
            added = new_privs - old_privs
            if principal not in old_map and new_privs:
                ops.append(
                    self._create_add_grant_op(target_type, target_id, principal, list(new_privs))
                )
            elif principal not in new_map and old_privs:
                ops.append(self._create_revoke_grant_op(target_type, target_id, principal, None))
            else:
                if removed:
                    ops.append(
                        self._create_revoke_grant_op(
                            target_type, target_id, principal, list(removed)
                        )
                    )
                if added:
                    ops.append(
                        self._create_add_grant_op(
                            target_type, target_id, principal, list(added)
                        )
                    )

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
                        ops.append(
                            self._create_change_column_type_op(col_id, table_id, col["type"])
                        )

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

        # Check for column order changes
        # Only generate reorder operation if:
        # 1. Both states have columns
        # 2. Same columns exist in both (no adds/removes in this diff cycle)
        # 3. Order is different
        old_column_order = [col["id"] for col in old_table.get("columns", [])]
        new_column_order = [col["id"] for col in new_table.get("columns", [])]

        if (
            old_column_order
            and new_column_order
            and set(old_column_order) == set(new_column_order)
            and old_column_order != new_column_order
        ):
            ops.append(
                self._create_reorder_columns_op(table_id, new_column_order, old_column_order)
            )

        return ops

    def _diff_table_properties(
        self, table_id: str, old_props: dict[str, Any], new_props: dict[str, Any]
    ) -> list[Operation]:
        """Compare table properties (TBLPROPERTIES)"""
        ops: list[Operation] = []

        # Handle None values - treat as empty dict
        old_props = old_props or {}
        new_props = new_props or {}

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

        # Handle None values - treat as empty dict
        old_tags = old_tags or {}
        new_tags = new_tags or {}

        # Added or updated tags
        for tag_name, tag_value in new_tags.items():
            if tag_name not in old_tags or old_tags[tag_name] != tag_value:
                ops.append(self._create_set_table_tag_op(table_id, tag_name, str(tag_value)))

        # Removed tags
        for tag_name in old_tags:
            if tag_name not in new_tags:
                ops.append(self._create_unset_table_tag_op(table_id, tag_name))

        return ops

    def _diff_constraints(
        self,
        table_id: str,
        old_constraints: list[dict[str, Any]],
        new_constraints: list[dict[str, Any]],
    ) -> list[Operation]:
        """Compare table constraints (PRIMARY KEY, FOREIGN KEY, CHECK)"""
        ops: list[Operation] = []

        # Handle None values - treat as empty list
        old_constraints = old_constraints or []
        new_constraints = new_constraints or []

        # Build ID maps for comparison
        old_constraint_map = {c["id"]: c for c in old_constraints}
        new_constraint_map = {c["id"]: c for c in new_constraints}

        # Added constraints
        for constraint_id, constraint in new_constraint_map.items():
            if constraint_id not in old_constraint_map:
                ops.append(self._create_add_constraint_op(constraint, table_id))

        # Removed constraints
        for constraint_id, constraint in old_constraint_map.items():
            if constraint_id not in new_constraint_map:
                ops.append(self._create_drop_constraint_op(constraint, table_id))

        # Modified constraints (Unity Catalog doesn't support ALTER CONSTRAINT,
        # so we must DROP and re-ADD to modify)
        for constraint_id, new_constraint in new_constraint_map.items():
            if constraint_id in old_constraint_map:
                old_constraint = old_constraint_map[constraint_id]
                # Check if constraint definition has changed
                if self._constraint_has_changed(old_constraint, new_constraint):
                    # Generate DROP then ADD operations
                    # The SQL generator will sort by timestamp to ensure DROP comes before ADD
                    ops.append(self._create_drop_constraint_op(old_constraint, table_id))
                    ops.append(self._create_add_constraint_op(new_constraint, table_id))

        return ops

    def _constraint_has_changed(
        self, old_constraint: dict[str, Any], new_constraint: dict[str, Any]
    ) -> bool:
        """Check if constraint definition has changed (requires DROP + ADD)

        Compares all relevant fields:
        - type (primary_key, foreign_key, check)
        - columns (list of column IDs)
        - name (constraint name)
        - timeseries (for PRIMARY KEY)
        - parentTable, parentColumns (for FOREIGN KEY)
        - expression (for CHECK)
        """
        # Check core fields
        if old_constraint.get("type") != new_constraint.get("type"):
            return True
        if old_constraint.get("name") != new_constraint.get("name"):
            return True

        # Check columns (order matters for PRIMARY KEY)
        old_cols = old_constraint.get("columns", [])
        new_cols = new_constraint.get("columns", [])
        if old_cols != new_cols:
            return True

        # Type-specific checks
        constraint_type = old_constraint.get("type")

        if constraint_type == "primary_key":
            if old_constraint.get("timeseries") != new_constraint.get("timeseries"):
                return True

        elif constraint_type == "foreign_key":
            if old_constraint.get("parentTable") != new_constraint.get("parentTable"):
                return True
            old_parent_cols = old_constraint.get("parentColumns", [])
            new_parent_cols = new_constraint.get("parentColumns", [])
            if old_parent_cols != new_parent_cols:
                return True

        elif constraint_type == "check":
            if old_constraint.get("expression") != new_constraint.get("expression"):
                return True

        return False

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

        # Handle None values - treat as empty dict
        old_tags = old_tags or {}
        new_tags = new_tags or {}

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
            sch_id = schema["id"]
            ops.append(self._create_add_schema_op(schema, catalog_id))
            # Add all tables in this new schema
            ops.extend(self._add_all_tables_in_schema(sch_id, schema))
            # Add all views in this new schema
            ops.extend(self._add_all_views_in_schema(sch_id, schema))
            # Add schema grants (Bug 2: schema-level grants were omitted for new catalogs)
            ops.extend(self._diff_grants("schema", sch_id, [], schema.get("grants", [])))

        return ops

    def _add_all_tables_in_schema(self, schema_id: str, schema: dict[str, Any]) -> list[Operation]:
        """Add all tables in a newly created schema"""
        ops: list[Operation] = []

        for table in schema.get("tables", []):
            tbl_id = table["id"]
            ops.append(self._create_add_table_op(table, schema_id))
            # Add all columns in this new table
            ops.extend(self._add_all_columns_in_table(tbl_id, table))
            # Add all tags for this table
            ops.extend(self._add_all_tags_for_table(tbl_id, table))
            # Add all constraints for this table
            ops.extend(self._add_all_constraints_for_table(tbl_id, table))
            # Add table grants
            ops.extend(self._diff_grants("table", tbl_id, [], table.get("grants", [])))

        return ops

    def _add_all_views_in_schema(self, schema_id: str, schema: dict[str, Any]) -> list[Operation]:
        """Add all views in a newly created schema"""
        ops: list[Operation] = []

        for view in schema.get("views", []):
            view_id = view["id"]
            ops.append(self._create_add_view_op(view, schema_id))
            # Add view grants
            ops.extend(self._diff_grants("view", view_id, [], view.get("grants", [])))

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
            column_tags = column.get("tags")
            if column_tags and isinstance(column_tags, dict):
                column_name = column.get("name")
                if column_name:  # Safety check - column must have name
                    for tag_name, tag_value in column_tags.items():
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

    def _add_all_constraints_for_table(
        self, table_id: str, table: dict[str, Any]
    ) -> list[Operation]:
        """Add all constraints for a newly created table"""
        ops: list[Operation] = []

        for constraint in table.get("constraints", []):
            # Safety check - skip constraints missing required fields
            if not all(key in constraint for key in ["id", "type", "columns"]):
                continue

            ops.append(self._create_add_constraint_op(constraint, table_id))

        return ops

    # Operation creation helpers

    def _create_add_catalog_op(self, catalog: dict[str, Any]) -> Operation:
        payload: dict[str, Any] = {"catalogId": catalog["id"], "name": catalog["name"]}
        managed_loc = catalog.get("managedLocationName") or catalog.get("managed_location_name")
        if managed_loc is not None:
            payload["managedLocationName"] = managed_loc
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_catalog",
            target=catalog["id"],
            payload=payload,
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

    def _create_drop_table_op(
        self, table: dict[str, Any], catalog_id: str, schema_id: str
    ) -> Operation:
        # Build payload with table metadata (for SQL generation when table no longer in state)
        payload: dict[str, Any] = {}

        # Add table name (required for SQL generation)
        if "name" in table:
            payload["name"] = table["name"]

        # Add catalog and schema IDs from context (tables are nested, these aren't fields)
        payload["catalogId"] = catalog_id
        payload["schemaId"] = schema_id

        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_table",
            target=table["id"],
            payload=payload,
        )

    # View operation creators
    def _create_add_view_op(self, view: dict[str, Any], schema_id: str) -> Operation:
        payload = {
            "viewId": view["id"],
            "name": view["name"],
            "schemaId": schema_id,
            "definition": view.get("definition", ""),
        }

        # Include optional fields if present
        if "comment" in view and view["comment"]:
            payload["comment"] = view["comment"]
        if "dependencies" in view:
            payload["dependencies"] = view["dependencies"]
        if "extractedDependencies" in view:
            payload["extractedDependencies"] = view["extractedDependencies"]

        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_view",
            target=view["id"],
            payload=payload,
        )

    def _create_rename_view_op(self, view_id: str, old_name: str, new_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_view",
            target=view_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_update_view_op(self, view_id: str, view: dict[str, Any]) -> Operation:
        payload = {"definition": view.get("definition", "")}

        # Include extracted dependencies if present
        if "extractedDependencies" in view:
            payload["extractedDependencies"] = view["extractedDependencies"]

        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.update_view",
            target=view_id,
            payload=payload,
        )

    def _create_set_view_comment_op(self, view_id: str, comment: str | None) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_view_comment",
            target=view_id,
            payload={"viewId": view_id, "comment": comment},
        )

    def _create_drop_view_op(self, view: dict[str, Any]) -> Operation:
        # Build payload with view metadata
        payload: dict[str, Any] = {}

        # Add view name (required for SQL generation)
        if "name" in view:
            payload["name"] = view["name"]

        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_view",
            target=view["id"],
            payload=payload,
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

    def _create_reorder_columns_op(
        self, table_id: str, new_order: list[str], previous_order: list[str]
    ) -> Operation:
        """Create reorder_columns operation for state differ"""
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.reorder_columns",
            target=table_id,
            payload={
                "tableId": table_id,
                "order": new_order,
                "previousOrder": previous_order,
            },
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

    def _create_set_table_tag_op(self, table_id: str, tag_name: str, tag_value: str) -> Operation:
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

    def _create_add_constraint_op(self, constraint: dict[str, Any], table_id: str) -> Operation:
        """Create add_constraint operation

        Args:
            constraint: Constraint definition
            table_id: Table ID
        """
        payload: dict[str, Any] = {
            "tableId": table_id,
            "constraintId": constraint["id"],
            "type": constraint["type"],
            "columns": constraint["columns"],
        }

        # Add optional fields
        if "name" in constraint and constraint["name"]:
            payload["name"] = constraint["name"]
        if "timeseries" in constraint:
            payload["timeseries"] = constraint["timeseries"]
        if "parentTable" in constraint:
            payload["parentTable"] = constraint["parentTable"]
        if "parentColumns" in constraint:
            payload["parentColumns"] = constraint["parentColumns"]
        if "expression" in constraint:
            payload["expression"] = constraint["expression"]

        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_constraint",
            target=constraint["id"],
            payload=payload,
        )

    def _create_drop_constraint_op(self, constraint: dict[str, Any], table_id: str) -> Operation:
        """Create drop_constraint operation

        Args:
            constraint: Constraint definition (from old state)
            table_id: Table ID
        """
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_constraint",
            target=constraint["id"],
            payload={
                "tableId": table_id,
                "name": constraint.get("name"),  # Include name for SQL generation
            },
        )

    def _create_add_grant_op(
        self,
        target_type: str,
        target_id: str,
        principal: str,
        privileges: list[str],
    ) -> Operation:
        """Create add_grant operation."""
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_grant",
            target=target_id,
            payload={
                "targetType": target_type,
                "targetId": target_id,
                "principal": principal,
                "privileges": privileges,
            },
        )

    def _create_revoke_grant_op(
        self,
        target_type: str,
        target_id: str,
        principal: str,
        privileges: list[str] | None,
    ) -> Operation:
        """Create revoke_grant operation. privileges=None means revoke all."""
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.revoke_grant",
            target=target_id,
            payload={
                "targetType": target_type,
                "targetId": target_id,
                "principal": principal,
                "privileges": privileges,
            },
        )
