"""
Unity Catalog State Differ

Compares two Unity Catalog states and generates operations representing the changes.
Handles catalogs, schemas, tables, and columns.
"""

from datetime import UTC, datetime
from typing import Any, cast
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
            return cast(dict[str, Any], state)
        if hasattr(state, "model_dump"):
            return cast(dict[str, Any], state.model_dump(by_alias=True))
        return {}

    def _diff_catalogs(self) -> list[Operation]:
        """Compare catalogs between old and new state"""
        ops: list[Operation] = []

        old_state_dict = self._state_to_dict(self.old_state)
        new_state_dict = self._state_to_dict(self.new_state)

        old_cats = self._build_id_map(old_state_dict.get("catalogs", []))
        new_cats = self._build_id_map(new_state_dict.get("catalogs", []))

        for cat_id, cat in new_cats.items():
            if cat_id not in old_cats:
                ops.append(self._create_add_catalog_op(cat))
                ops.extend(self._add_all_schemas_in_catalog(cat_id, cat))
                ops.extend(self._diff_grants("catalog", cat_id, [], cat.get("grants", [])))
                continue

            old_cat = old_cats[cat_id]
            old_name = old_cat["name"]
            new_name = cat["name"]
            if old_name != new_name and self._detect_rename(
                cat_id, cat_id, old_name, new_name, "rename_catalog"
            ):
                ops.append(self._create_rename_catalog_op(cat_id, old_name, new_name))
            ops.extend(self._diff_schemas(cat_id, old_cat, cat))
            ops.extend(
                self._diff_grants(
                    "catalog",
                    cat_id,
                    old_cat.get("grants", []),
                    cat.get("grants", []),
                )
            )

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

        for sch_id, sch in new_schemas.items():
            if sch_id not in old_schemas:
                ops.append(self._create_add_schema_op(sch, catalog_id))
                ops.extend(self._add_all_tables_in_schema(sch_id, sch))
                ops.extend(self._add_all_views_in_schema(sch_id, sch))
                ops.extend(self._add_all_volumes_in_schema(sch_id, sch))
                ops.extend(self._add_all_functions_in_schema(sch_id, sch))
                ops.extend(self._add_all_materialized_views_in_schema(sch_id, sch))
                ops.extend(self._diff_grants("schema", sch_id, [], sch.get("grants", [])))
                continue

            old_sch = old_schemas[sch_id]
            old_name = old_sch["name"]
            new_name = sch["name"]
            if old_name != new_name and self._detect_rename(
                sch_id, sch_id, old_name, new_name, "rename_schema"
            ):
                ops.append(self._create_rename_schema_op(sch_id, old_name, new_name))
            ops.extend(self._diff_tables(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_views(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_volumes(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_functions(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_materialized_views(catalog_id, sch_id, old_sch, sch))
            ops.extend(
                self._diff_grants(
                    "schema",
                    sch_id,
                    old_sch.get("grants", []),
                    sch.get("grants", []),
                )
            )

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

        for tbl_id, tbl in new_tables.items():
            if tbl_id not in old_tables:
                ops.append(self._create_add_table_op(tbl, schema_id))
                ops.extend(self._add_all_columns_in_table(tbl_id, tbl))
                ops.extend(self._add_all_tags_for_table(tbl_id, tbl))
                ops.extend(self._add_all_constraints_for_table(tbl_id, tbl))
                ops.extend(self._diff_grants("table", tbl_id, [], tbl.get("grants", [])))
                continue

            old_tbl = old_tables[tbl_id]
            old_name = old_tbl["name"]
            new_name = tbl["name"]
            if old_name != new_name and self._detect_rename(
                tbl_id, tbl_id, old_name, new_name, "rename_table"
            ):
                ops.append(self._create_rename_table_op(tbl_id, old_name, new_name))
            if old_tbl.get("comment") != tbl.get("comment"):
                ops.append(self._create_set_table_comment_op(tbl_id, tbl.get("comment")))
            ops.extend(
                self._diff_table_properties(
                    tbl_id, old_tbl.get("properties", {}), tbl.get("properties", {})
                )
            )
            ops.extend(self._diff_table_tags(tbl_id, old_tbl.get("tags", {}), tbl.get("tags", {})))
            ops.extend(self._diff_columns(tbl_id, old_tbl, tbl))
            ops.extend(
                self._diff_constraints(
                    tbl_id, old_tbl.get("constraints", []), tbl.get("constraints", [])
                )
            )
            ops.extend(
                self._diff_grants(
                    "table",
                    tbl_id,
                    old_tbl.get("grants", []),
                    tbl.get("grants", []),
                )
            )

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

        for view_id, view in new_views.items():
            if view_id not in old_views:
                ops.append(self._create_add_view_op(view, schema_id))
                ops.extend(self._diff_grants("view", view_id, [], view.get("grants", [])))
                continue

            old_view = old_views[view_id]
            old_name = old_view["name"]
            new_name = view["name"]
            if old_name != new_name and self._detect_rename(
                view_id, view_id, old_name, new_name, "rename_view"
            ):
                ops.append(self._create_rename_view_op(view_id, old_name, new_name))
            if old_view.get("definition") != view.get("definition"):
                ops.append(self._create_update_view_op(view_id, view))
            if old_view.get("comment") != view.get("comment"):
                ops.append(self._create_set_view_comment_op(view_id, view.get("comment")))
            ops.extend(
                self._diff_grants(
                    "view",
                    view_id,
                    old_view.get("grants", []),
                    view.get("grants", []),
                )
            )

        for view_id, view in old_views.items():
            if view_id not in new_views:
                ops.append(self._create_drop_view_op(view, catalog_id, schema_id))

        return ops

    def _diff_volumes(
        self,
        catalog_id: str,
        schema_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
    ) -> list[Operation]:
        """Compare volumes within a schema"""
        ops: list[Operation] = []
        old_volumes = self._build_id_map(old_schema.get("volumes", []))
        new_volumes = self._build_id_map(new_schema.get("volumes", []))

        for volume_id, volume in new_volumes.items():
            if volume_id not in old_volumes:
                ops.append(self._create_add_volume_op(volume, schema_id))
                ops.extend(self._diff_grants("volume", volume_id, [], volume.get("grants", [])))
                continue

            old_volume = old_volumes[volume_id]
            old_name = old_volume.get("name", "")
            new_name = volume.get("name", "")
            if old_name != new_name and self._detect_rename(
                volume_id, volume_id, old_name, new_name, "rename_volume"
            ):
                ops.append(self._create_rename_volume_op(volume_id, old_name, new_name))
            if old_volume.get("comment") != volume.get("comment") or old_volume.get(
                "location"
            ) != volume.get("location"):
                ops.append(self._create_update_volume_op(volume_id, volume))
            ops.extend(
                self._diff_grants(
                    "volume",
                    volume_id,
                    old_volume.get("grants", []),
                    volume.get("grants", []),
                )
            )

        for volume_id, volume in old_volumes.items():
            if volume_id not in new_volumes:
                ops.append(self._create_drop_volume_op(volume, catalog_id, schema_id))
        return ops

    def _diff_functions(
        self,
        catalog_id: str,
        schema_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
    ) -> list[Operation]:
        """Compare functions within a schema"""
        ops: list[Operation] = []
        old_functions = self._build_id_map(old_schema.get("functions", []))
        new_functions = self._build_id_map(new_schema.get("functions", []))

        for function_id, function in new_functions.items():
            if function_id not in old_functions:
                ops.append(self._create_add_function_op(function, schema_id))
                ops.extend(
                    self._diff_grants("function", function_id, [], function.get("grants", []))
                )
                continue

            old_function = old_functions[function_id]
            old_name = old_function.get("name", "")
            new_name = function.get("name", "")
            if old_name != new_name and self._detect_rename(
                function_id, function_id, old_name, new_name, "rename_function"
            ):
                ops.append(self._create_rename_function_op(function_id, old_name, new_name))
            if old_function.get("body") != function.get("body") or old_function.get(
                "returnType"
            ) != function.get("returnType"):
                ops.append(self._create_update_function_op(function_id, function))
            if old_function.get("comment") != function.get("comment"):
                ops.append(
                    self._create_set_function_comment_op(function_id, function.get("comment"))
                )
            ops.extend(
                self._diff_grants(
                    "function",
                    function_id,
                    old_function.get("grants", []),
                    function.get("grants", []),
                )
            )

        for function_id, function in old_functions.items():
            if function_id not in new_functions:
                ops.append(self._create_drop_function_op(function, catalog_id, schema_id))
        return ops

    def _diff_materialized_views(
        self,
        catalog_id: str,
        schema_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
    ) -> list[Operation]:
        """Compare materialized views within a schema"""
        ops: list[Operation] = []
        old_materialized_views = self._build_id_map(
            old_schema.get("materialized_views", old_schema.get("materializedViews", []))
        )
        new_materialized_views = self._build_id_map(
            new_schema.get("materialized_views", new_schema.get("materializedViews", []))
        )

        for materialized_view_id, materialized_view in new_materialized_views.items():
            if materialized_view_id not in old_materialized_views:
                ops.append(self._create_add_materialized_view_op(materialized_view, schema_id))
                ops.extend(
                    self._diff_grants(
                        "materialized_view",
                        materialized_view_id,
                        [],
                        materialized_view.get("grants", []),
                    )
                )
                continue

            old_materialized_view = old_materialized_views[materialized_view_id]
            old_name = old_materialized_view.get("name", "")
            new_name = materialized_view.get("name", "")
            if old_name != new_name and self._detect_rename(
                materialized_view_id,
                materialized_view_id,
                old_name,
                new_name,
                "rename_materialized_view",
            ):
                ops.append(
                    self._create_rename_materialized_view_op(
                        materialized_view_id, old_name, new_name
                    )
                )
            if old_materialized_view.get("definition") != materialized_view.get(
                "definition"
            ) or old_materialized_view.get("refreshSchedule") != materialized_view.get(
                "refreshSchedule"
            ):
                ops.append(
                    self._create_update_materialized_view_op(
                        materialized_view_id, materialized_view
                    )
                )
            if old_materialized_view.get("comment") != materialized_view.get("comment"):
                ops.append(
                    self._create_set_materialized_view_comment_op(
                        materialized_view_id, materialized_view.get("comment")
                    )
                )
            ops.extend(
                self._diff_grants(
                    "materialized_view",
                    materialized_view_id,
                    old_materialized_view.get("grants", []),
                    materialized_view.get("grants", []),
                )
            )

        for materialized_view_id, materialized_view in old_materialized_views.items():
            if materialized_view_id not in new_materialized_views:
                ops.append(
                    self._create_drop_materialized_view_op(materialized_view, catalog_id, schema_id)
                )
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

        def normalize_grant(grant: Any) -> tuple[str, list[str]]:
            if isinstance(grant, dict):
                principal = (grant.get("principal") or "").strip()
                privs = grant.get("privileges") or []
                return (principal, list(privs) if isinstance(privs, list) else [])
            return ("", [])

        def valid_principal(principal: str) -> bool:
            """Explicitly reject empty principal (invalid for SQL; skip in diff)."""
            return bool(principal and principal.strip())

        def build_grant_map(grants: list[Any]) -> dict[str, list[str]]:
            grant_map: dict[str, list[str]] = {}
            for grant in grants:
                principal, privileges = normalize_grant(grant)
                if valid_principal(principal):
                    grant_map[principal] = privileges
            return grant_map

        old_map = build_grant_map(old_grants or [])
        new_map = build_grant_map(new_grants or [])

        all_principals = set(old_map) | set(new_map)
        for principal in all_principals:
            ops.extend(
                self._grant_delta_operations(
                    target_type=target_type,
                    target_id=target_id,
                    principal=principal,
                    old_map=old_map,
                    new_map=new_map,
                )
            )

        return ops

    def _grant_delta_operations(
        self,
        target_type: str,
        target_id: str,
        principal: str,
        old_map: dict[str, list[str]],
        new_map: dict[str, list[str]],
    ) -> list[Operation]:
        """Generate grant delta operations for one principal."""
        old_privs = set(old_map.get(principal, []))
        new_privs = set(new_map.get(principal, []))
        if principal not in old_map and new_privs:
            return [self._create_add_grant_op(target_type, target_id, principal, list(new_privs))]
        if principal not in new_map and old_privs:
            return [self._create_revoke_grant_op(target_type, target_id, principal, None)]
        return self._changed_privilege_operations(
            target_type=target_type,
            target_id=target_id,
            principal=principal,
            removed=old_privs - new_privs,
            added=new_privs - old_privs,
        )

    def _changed_privilege_operations(
        self,
        target_type: str,
        target_id: str,
        principal: str,
        removed: set[str],
        added: set[str],
    ) -> list[Operation]:
        """Generate revoke/add operations when principal remains but privileges changed."""
        ops: list[Operation] = []
        if removed:
            ops.append(
                self._create_revoke_grant_op(target_type, target_id, principal, list(removed))
            )
        if added:
            ops.append(self._create_add_grant_op(target_type, target_id, principal, list(added)))
        return ops

    def _diff_columns(
        self, table_id: str, old_table: dict[str, Any], new_table: dict[str, Any]
    ) -> list[Operation]:
        """Compare columns within a table"""
        ops: list[Operation] = []

        old_columns = self._build_id_map(old_table.get("columns", []))
        new_columns = self._build_id_map(new_table.get("columns", []))

        for col_id, col in new_columns.items():
            if col_id not in old_columns:
                if all(key in col for key in ("id", "name", "type")):
                    ops.append(self._create_add_column_op(col, table_id))
                continue
            ops.extend(self._diff_existing_column(col_id, table_id, old_columns[col_id], col))

        for col_id, col in old_columns.items():
            if col_id not in new_columns:
                ops.append(self._create_drop_column_op(col, table_id))

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

    def _diff_existing_column(
        self,
        col_id: str,
        table_id: str,
        old_col: dict[str, Any],
        new_col: dict[str, Any],
    ) -> list[Operation]:
        """Compare an existing column and return update operations."""
        ops: list[Operation] = []
        old_name = old_col.get("name")
        new_name = new_col.get("name")
        if (
            old_name
            and new_name
            and old_name != new_name
            and self._detect_rename(col_id, col_id, old_name, new_name, "rename_column")
        ):
            ops.append(self._create_rename_column_op(col_id, table_id, old_name, new_name))
        if old_col.get("type") != new_col.get("type") and "type" in new_col:
            ops.append(self._create_change_column_type_op(col_id, table_id, new_col["type"]))
        if old_col.get("nullable") != new_col.get("nullable"):
            ops.append(self._create_set_nullable_op(col_id, table_id, new_col["nullable"]))
        if old_col.get("comment") != new_col.get("comment"):
            ops.append(self._create_set_column_comment_op(col_id, table_id, new_col.get("comment")))
        if "name" in new_col:
            ops.extend(
                self._diff_column_tags(
                    col_id,
                    table_id,
                    new_col["name"],
                    old_col.get("tags", {}),
                    new_col.get("tags", {}),
                )
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
        if old_constraint.get("type") != new_constraint.get("type"):
            return True
        if old_constraint.get("name") != new_constraint.get("name"):
            return True
        if old_constraint.get("columns", []) != new_constraint.get("columns", []):
            return True

        constraint_type = old_constraint.get("type")
        if constraint_type == "primary_key":
            return old_constraint.get("timeseries") != new_constraint.get("timeseries")
        if constraint_type == "foreign_key":
            if old_constraint.get("parentTable") != new_constraint.get("parentTable"):
                return True
            old_parent_cols = cast(list[Any], old_constraint.get("parentColumns", []))
            new_parent_cols = cast(list[Any], new_constraint.get("parentColumns", []))
            return old_parent_cols != new_parent_cols
        if constraint_type == "check":
            return old_constraint.get("expression") != new_constraint.get("expression")
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
            # Add all views, volumes, functions, materialized views in this new schema
            ops.extend(self._add_all_views_in_schema(sch_id, schema))
            ops.extend(self._add_all_volumes_in_schema(sch_id, schema))
            ops.extend(self._add_all_functions_in_schema(sch_id, schema))
            ops.extend(self._add_all_materialized_views_in_schema(sch_id, schema))
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

    def _add_all_volumes_in_schema(self, schema_id: str, schema: dict[str, Any]) -> list[Operation]:
        """Add all volumes in a newly created schema"""
        ops: list[Operation] = []
        for vol in schema.get("volumes", []):
            vol_id = vol["id"]
            ops.append(self._create_add_volume_op(vol, schema_id))
            ops.extend(self._diff_grants("volume", vol_id, [], vol.get("grants", [])))
        return ops

    def _add_all_functions_in_schema(
        self, schema_id: str, schema: dict[str, Any]
    ) -> list[Operation]:
        """Add all functions in a newly created schema"""
        ops: list[Operation] = []
        for func in schema.get("functions", []):
            func_id = func["id"]
            ops.append(self._create_add_function_op(func, schema_id))
            ops.extend(self._diff_grants("function", func_id, [], func.get("grants", [])))
        return ops

    def _add_all_materialized_views_in_schema(
        self, schema_id: str, schema: dict[str, Any]
    ) -> list[Operation]:
        """Add all materialized views in a newly created schema"""
        ops: list[Operation] = []
        mvs = schema.get("materialized_views", schema.get("materializedViews", []))
        for materialized_view in mvs:
            materialized_view_id = materialized_view["id"]
            ops.append(self._create_add_materialized_view_op(materialized_view, schema_id))
            ops.extend(
                self._diff_grants(
                    "materialized_view",
                    materialized_view_id,
                    [],
                    materialized_view.get("grants", []),
                )
            )
        return ops

    def _add_all_columns_in_table(self, table_id: str, table: dict[str, Any]) -> list[Operation]:
        """Add all columns in a newly created table"""
        ops: list[Operation] = []

        for column in table.get("columns", []):
            if not all(key in column for key in ("id", "name", "type")):
                continue
            ops.append(self._create_add_column_op(column, table_id))
            ops.extend(self._create_column_tag_ops(column, table_id))

        return ops

    def _create_column_tag_ops(self, column: dict[str, Any], table_id: str) -> list[Operation]:
        """Create tag operations for one column."""
        column_tags = column.get("tags")
        column_name = column.get("name")
        if not column_tags or not isinstance(column_tags, dict) or not column_name:
            return []
        ops: list[Operation] = []
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
            if not all(key in constraint for key in ("id", "type", "columns")):
                continue

            ops.append(self._create_add_constraint_op(constraint, table_id))

        return ops

    # Operation creation helpers

    def _create_add_catalog_op(self, catalog: dict[str, Any]) -> Operation:
        payload: dict[str, Any] = {"catalogId": catalog["id"], "name": catalog["name"]}
        managed_loc = catalog.get("managedLocationName") or catalog.get("managed_location_name")
        if managed_loc is not None:
            payload["managedLocationName"] = managed_loc
        comment = catalog.get("comment")
        if comment is not None:
            payload["comment"] = comment
        tags = catalog.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            payload["tags"] = dict(tags)
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
            payload={"name": catalog.get("name", "")},
        )

    def _create_add_schema_op(self, schema: dict[str, Any], catalog_id: str) -> Operation:
        payload: dict[str, Any] = {
            "schemaId": schema["id"],
            "name": schema["name"],
            "catalogId": catalog_id,
        }
        managed_loc = schema.get("managedLocationName") or schema.get("managed_location_name")
        if managed_loc is not None:
            payload["managedLocationName"] = managed_loc
        comment = schema.get("comment")
        if comment is not None:
            payload["comment"] = comment
        tags = schema.get("tags")
        if tags and isinstance(tags, dict) and len(tags) > 0:
            payload["tags"] = dict(tags)
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_schema",
            target=schema["id"],
            payload=payload,
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

    def _create_drop_view_op(
        self, view: dict[str, Any], catalog_id: str, schema_id: str
    ) -> Operation:
        payload: dict[str, Any] = {}
        if "name" in view:
            payload["name"] = view["name"]
        payload["catalogId"] = catalog_id
        payload["schemaId"] = schema_id
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_view",
            target=view["id"],
            payload=payload,
        )

    # Volume operation creators
    def _create_add_volume_op(self, volume: dict[str, Any], schema_id: str) -> Operation:
        vol_id = volume.get("id", "")
        payload: dict[str, Any] = {
            "volumeId": vol_id,
            "name": volume.get("name", ""),
            "schemaId": schema_id,
            "volumeType": volume.get("volumeType", volume.get("volume_type", "managed")),
        }
        if volume.get("comment") is not None:
            payload["comment"] = volume["comment"]
        if volume.get("location") is not None:
            payload["location"] = volume["location"]
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_volume",
            target=vol_id,
            payload=payload,
        )

    def _create_rename_volume_op(self, volume_id: str, old_name: str, new_name: str) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_volume",
            target=volume_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_update_volume_op(self, volume_id: str, volume: dict[str, Any]) -> Operation:
        payload: dict[str, Any] = {}
        if "comment" in volume:
            payload["comment"] = volume.get("comment")
        if "location" in volume:
            payload["location"] = volume.get("location")
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.update_volume",
            target=volume_id,
            payload=payload,
        )

    def _create_drop_volume_op(
        self, volume: dict[str, Any], catalog_id: str, schema_id: str
    ) -> Operation:
        payload: dict[str, Any] = {
            "name": volume.get("name", ""),
            "catalogId": catalog_id,
            "schemaId": schema_id,
        }
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_volume",
            target=volume["id"],
            payload=payload,
        )

    # Function operation creators
    def _create_add_function_op(self, func: dict[str, Any], schema_id: str) -> Operation:
        func_id = func.get("id", "")
        payload: dict[str, Any] = {
            "functionId": func_id,
            "name": func.get("name", ""),
            "schemaId": schema_id,
            "language": func.get("language", "SQL"),
            "returnType": func.get("returnType", func.get("return_type")),
            "body": func.get("body", ""),
        }
        if func.get("comment") is not None:
            payload["comment"] = func["comment"]
        if func.get("parameters") is not None:
            payload["parameters"] = func["parameters"]
        if func.get("returnsTable") is not None:
            payload["returnsTable"] = func["returnsTable"]
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_function",
            target=func_id,
            payload=payload,
        )

    def _create_rename_function_op(
        self, function_id: str, old_name: str, new_name: str
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_function",
            target=function_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_update_function_op(self, function_id: str, func: dict[str, Any]) -> Operation:
        payload: dict[str, Any] = {
            "body": func.get("body"),
            "returnType": func.get("returnType", func.get("return_type")),
            "parameters": func.get("parameters"),
            "comment": func.get("comment"),
        }
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.update_function",
            target=function_id,
            payload=payload,
        )

    def _create_set_function_comment_op(self, function_id: str, comment: str | None) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_function_comment",
            target=function_id,
            payload={"functionId": function_id, "comment": comment},
        )

    def _create_drop_function_op(
        self, func: dict[str, Any], catalog_id: str, schema_id: str
    ) -> Operation:
        payload: dict[str, Any] = {
            "name": func.get("name", ""),
            "catalogId": catalog_id,
            "schemaId": schema_id,
        }
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_function",
            target=func["id"],
            payload=payload,
        )

    # Materialized view operation creators
    def _create_add_materialized_view_op(
        self, materialized_view: dict[str, Any], schema_id: str
    ) -> Operation:
        materialized_view_id = materialized_view.get("id", "")
        payload: dict[str, Any] = {
            "materializedViewId": materialized_view_id,
            "name": materialized_view.get("name", ""),
            "schemaId": schema_id,
            "definition": materialized_view.get("definition", "SELECT 1"),
        }
        if materialized_view.get("comment") is not None:
            payload["comment"] = materialized_view["comment"]
        if materialized_view.get("refreshSchedule") is not None:
            payload["refreshSchedule"] = materialized_view["refreshSchedule"]
        if materialized_view.get("refresh_schedule") is not None:
            payload["refreshSchedule"] = materialized_view["refresh_schedule"]
        if materialized_view.get("dependencies") is not None:
            payload["dependencies"] = materialized_view["dependencies"]
        if materialized_view.get("extractedDependencies") is not None:
            payload["extractedDependencies"] = materialized_view["extractedDependencies"]
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.add_materialized_view",
            target=materialized_view_id,
            payload=payload,
        )

    def _create_rename_materialized_view_op(
        self, materialized_view_id: str, old_name: str, new_name: str
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_materialized_view",
            target=materialized_view_id,
            payload={"oldName": old_name, "newName": new_name},
        )

    def _create_update_materialized_view_op(
        self, materialized_view_id: str, materialized_view: dict[str, Any]
    ) -> Operation:
        payload: dict[str, Any] = {
            "definition": materialized_view.get("definition"),
            "refreshSchedule": materialized_view.get(
                "refreshSchedule", materialized_view.get("refresh_schedule")
            ),
            "extractedDependencies": materialized_view.get("extractedDependencies"),
        }
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.update_materialized_view",
            target=materialized_view_id,
            payload=payload,
        )

    def _create_set_materialized_view_comment_op(
        self, materialized_view_id: str, comment: str | None
    ) -> Operation:
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.set_materialized_view_comment",
            target=materialized_view_id,
            payload={"materializedViewId": materialized_view_id, "comment": comment},
        )

    def _create_drop_materialized_view_op(
        self, materialized_view: dict[str, Any], catalog_id: str, schema_id: str
    ) -> Operation:
        payload: dict[str, Any] = {
            "name": materialized_view.get("name", ""),
            "catalogId": catalog_id,
            "schemaId": schema_id,
        }
        return Operation(
            id=f"op_diff_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.drop_materialized_view",
            target=materialized_view["id"],
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
