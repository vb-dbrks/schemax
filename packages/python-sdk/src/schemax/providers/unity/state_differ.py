"""
Unity Catalog State Differ

Compares two Unity Catalog states and generates operations representing the changes.
Handles catalogs, schemas, tables, and columns.

Delegates to extracted modules for:
- operation_builders: pure data construction for all operation types
- grant_differ: cross-cutting grant comparison logic
- metadata_differ: metadata/property/tag/constraint/column diffs
- bulk_operations: recursive traversal for newly created containers
"""

from typing import Any, cast

from schemax.providers.base.operations import Operation
from schemax.providers.base.state_differ import StateDiffer

from .bulk_operations import (
    add_all_columns_in_table,
    add_all_constraints_for_table,
    add_all_functions_in_schema,
    add_all_materialized_views_in_schema,
    add_all_schemas_in_catalog,
    add_all_tables_in_schema,
    add_all_tags_for_table,
    add_all_tags_for_view,
    add_all_views_in_schema,
    add_all_volumes_in_schema,
)
from .grant_differ import diff_grants
from .metadata_differ import (
    diff_catalog_metadata,
    diff_constraints,
    diff_existing_column,
    diff_schema_metadata,
    diff_table_properties,
    diff_table_tags,
    diff_view_properties,
    diff_view_tags,
)
from .operation_builders import (
    create_add_catalog_op,
    create_add_column_op,
    create_add_function_op,
    create_add_materialized_view_op,
    create_add_schema_op,
    create_add_table_op,
    create_add_view_op,
    create_add_volume_op,
    create_drop_catalog_op,
    create_drop_column_op,
    create_drop_function_op,
    create_drop_materialized_view_op,
    create_drop_schema_op,
    create_drop_table_op,
    create_drop_view_op,
    create_drop_volume_op,
    create_rename_catalog_op,
    create_rename_function_op,
    create_rename_materialized_view_op,
    create_rename_schema_op,
    create_rename_table_op,
    create_rename_view_op,
    create_rename_volume_op,
    create_reorder_columns_op,
    create_set_function_comment_op,
    create_set_materialized_view_comment_op,
    create_set_table_comment_op,
    create_set_view_comment_op,
    create_update_function_op,
    create_update_materialized_view_op,
    create_update_view_op,
    create_update_volume_op,
)


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
                ops.append(create_add_catalog_op(cat))
                ops.extend(add_all_schemas_in_catalog(cat_id, cat))
                ops.extend(diff_grants("catalog", cat_id, [], cat.get("grants", [])))
                continue

            old_cat = old_cats[cat_id]
            old_name = old_cat["name"]
            new_name = cat["name"]
            if old_name != new_name and self._detect_rename(
                cat_id, cat_id, old_name, new_name, "rename_catalog"
            ):
                ops.append(create_rename_catalog_op(cat_id, old_name, new_name))
            # Diff catalog metadata (comment, tags, managedLocationName)
            ops.extend(diff_catalog_metadata(cat_id, old_cat, cat))
            ops.extend(self._diff_schemas(cat_id, old_cat, cat))
            ops.extend(
                diff_grants(
                    "catalog",
                    cat_id,
                    old_cat.get("grants", []),
                    cat.get("grants", []),
                )
            )

        for cat_id, cat in old_cats.items():
            if cat_id not in new_cats:
                ops.append(create_drop_catalog_op(cat))

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
                ops.append(create_add_schema_op(sch, catalog_id))
                ops.extend(add_all_tables_in_schema(sch_id, sch))
                ops.extend(add_all_views_in_schema(sch_id, sch))
                ops.extend(add_all_volumes_in_schema(sch_id, sch))
                ops.extend(add_all_functions_in_schema(sch_id, sch))
                ops.extend(add_all_materialized_views_in_schema(sch_id, sch))
                ops.extend(diff_grants("schema", sch_id, [], sch.get("grants", [])))
                continue

            old_sch = old_schemas[sch_id]
            old_name = old_sch["name"]
            new_name = sch["name"]
            if old_name != new_name and self._detect_rename(
                sch_id, sch_id, old_name, new_name, "rename_schema"
            ):
                ops.append(create_rename_schema_op(sch_id, old_name, new_name))
            # Diff schema metadata (comment, tags, managedLocationName)
            ops.extend(diff_schema_metadata(sch_id, old_sch, sch))
            ops.extend(self._diff_tables(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_views(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_volumes(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_functions(catalog_id, sch_id, old_sch, sch))
            ops.extend(self._diff_materialized_views(catalog_id, sch_id, old_sch, sch))
            ops.extend(
                diff_grants(
                    "schema",
                    sch_id,
                    old_sch.get("grants", []),
                    sch.get("grants", []),
                )
            )

        for sch_id, sch in old_schemas.items():
            if sch_id not in new_schemas:
                ops.append(create_drop_schema_op(sch))

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
                ops.append(create_add_table_op(tbl, schema_id))
                ops.extend(add_all_columns_in_table(tbl_id, tbl))
                ops.extend(add_all_tags_for_table(tbl_id, tbl))
                ops.extend(add_all_constraints_for_table(tbl_id, tbl))
                ops.extend(diff_grants("table", tbl_id, [], tbl.get("grants", [])))
                continue

            old_tbl = old_tables[tbl_id]
            old_name = old_tbl["name"]
            new_name = tbl["name"]
            if old_name != new_name and self._detect_rename(
                tbl_id, tbl_id, old_name, new_name, "rename_table"
            ):
                ops.append(create_rename_table_op(tbl_id, old_name, new_name))
            if old_tbl.get("comment") != tbl.get("comment"):
                ops.append(create_set_table_comment_op(tbl_id, tbl.get("comment")))
            ops.extend(
                diff_table_properties(
                    tbl_id, old_tbl.get("properties", {}), tbl.get("properties", {})
                )
            )
            ops.extend(diff_table_tags(tbl_id, old_tbl.get("tags", {}), tbl.get("tags", {})))
            ops.extend(self._diff_columns(tbl_id, old_tbl, tbl))
            ops.extend(
                diff_constraints(tbl_id, old_tbl.get("constraints", []), tbl.get("constraints", []))
            )
            ops.extend(
                diff_grants(
                    "table",
                    tbl_id,
                    old_tbl.get("grants", []),
                    tbl.get("grants", []),
                )
            )

        for tbl_id, tbl in old_tables.items():
            if tbl_id not in new_tables:
                ops.append(create_drop_table_op(tbl, catalog_id, schema_id))

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
                ops.append(create_add_view_op(view, schema_id))
                ops.extend(diff_view_properties(view_id, {}, view.get("properties", {})))
                ops.extend(add_all_tags_for_view(view_id, view))
                ops.extend(diff_grants("view", view_id, [], view.get("grants", [])))
                continue

            old_view = old_views[view_id]
            old_name = old_view["name"]
            new_name = view["name"]
            if old_name != new_name and self._detect_rename(
                view_id, view_id, old_name, new_name, "rename_view"
            ):
                ops.append(create_rename_view_op(view_id, old_name, new_name))
            if old_view.get("definition") != view.get("definition"):
                ops.append(create_update_view_op(view_id, view))
            if old_view.get("comment") != view.get("comment"):
                ops.append(create_set_view_comment_op(view_id, view.get("comment")))
            ops.extend(
                diff_view_properties(
                    view_id, old_view.get("properties", {}), view.get("properties", {})
                )
            )
            ops.extend(diff_view_tags(view_id, old_view.get("tags", {}), view.get("tags", {})))
            ops.extend(
                diff_grants(
                    "view",
                    view_id,
                    old_view.get("grants", []),
                    view.get("grants", []),
                )
            )

        for view_id, view in old_views.items():
            if view_id not in new_views:
                ops.append(create_drop_view_op(view, catalog_id, schema_id))

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
                ops.append(create_add_volume_op(volume, schema_id))
                ops.extend(diff_grants("volume", volume_id, [], volume.get("grants", [])))
                continue

            old_volume = old_volumes[volume_id]
            old_name = old_volume.get("name", "")
            new_name = volume.get("name", "")
            if old_name != new_name and self._detect_rename(
                volume_id, volume_id, old_name, new_name, "rename_volume"
            ):
                ops.append(create_rename_volume_op(volume_id, old_name, new_name))
            if old_volume.get("comment") != volume.get("comment") or old_volume.get(
                "location"
            ) != volume.get("location"):
                ops.append(create_update_volume_op(volume_id, volume))
            ops.extend(
                diff_grants(
                    "volume",
                    volume_id,
                    old_volume.get("grants", []),
                    volume.get("grants", []),
                )
            )

        for volume_id, volume in old_volumes.items():
            if volume_id not in new_volumes:
                ops.append(create_drop_volume_op(volume, catalog_id, schema_id))
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
                ops.append(create_add_function_op(function, schema_id))
                ops.extend(diff_grants("function", function_id, [], function.get("grants", [])))
                continue

            old_function = old_functions[function_id]
            old_name = old_function.get("name", "")
            new_name = function.get("name", "")
            if old_name != new_name and self._detect_rename(
                function_id, function_id, old_name, new_name, "rename_function"
            ):
                ops.append(create_rename_function_op(function_id, old_name, new_name))
            if old_function.get("body") != function.get("body") or old_function.get(
                "returnType"
            ) != function.get("returnType"):
                ops.append(create_update_function_op(function_id, function))
            if old_function.get("comment") != function.get("comment"):
                ops.append(create_set_function_comment_op(function_id, function.get("comment")))
            ops.extend(
                diff_grants(
                    "function",
                    function_id,
                    old_function.get("grants", []),
                    function.get("grants", []),
                )
            )

        for function_id, function in old_functions.items():
            if function_id not in new_functions:
                ops.append(create_drop_function_op(function, catalog_id, schema_id))
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
                ops.append(create_add_materialized_view_op(materialized_view, schema_id))
                ops.extend(
                    diff_grants(
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
                    create_rename_materialized_view_op(materialized_view_id, old_name, new_name)
                )
            if old_materialized_view.get("definition") != materialized_view.get(
                "definition"
            ) or old_materialized_view.get("refreshSchedule") != materialized_view.get(
                "refreshSchedule"
            ):
                ops.append(
                    create_update_materialized_view_op(materialized_view_id, materialized_view)
                )
            if old_materialized_view.get("comment") != materialized_view.get("comment"):
                ops.append(
                    create_set_materialized_view_comment_op(
                        materialized_view_id, materialized_view.get("comment")
                    )
                )
            ops.extend(
                diff_grants(
                    "materialized_view",
                    materialized_view_id,
                    old_materialized_view.get("grants", []),
                    materialized_view.get("grants", []),
                )
            )

        for materialized_view_id, materialized_view in old_materialized_views.items():
            if materialized_view_id not in new_materialized_views:
                ops.append(
                    create_drop_materialized_view_op(materialized_view, catalog_id, schema_id)
                )
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
                    ops.append(create_add_column_op(col, table_id))
                continue
            ops.extend(
                diff_existing_column(
                    col_id, table_id, old_columns[col_id], col, self._detect_rename
                )
            )

        for col_id, col in old_columns.items():
            if col_id not in new_columns:
                ops.append(create_drop_column_op(col, table_id))

        old_column_order = [col["id"] for col in old_table.get("columns", [])]
        new_column_order = [col["id"] for col in new_table.get("columns", [])]

        if (
            old_column_order
            and new_column_order
            and set(old_column_order) == set(new_column_order)
            and old_column_order != new_column_order
        ):
            ops.append(create_reorder_columns_op(table_id, new_column_order, old_column_order))

        return ops
