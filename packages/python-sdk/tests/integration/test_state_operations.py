"""Integration tests for Unity state reducer and state differ covering
grants, constraints, row filters, column masks, volumes, functions, MVs.
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.state_differ import UnityStateDiffer
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import (
    OperationBuilder,
    ops_catalog_schema_table,
    ops_catalog_schema_with_function,
    ops_catalog_schema_with_mv,
    ops_table_with_pk,
    ops_table_with_ssn_mask,
)


def _empty():
    return UnityState(catalogs=[])


def _apply_model(ops: list):
    """Apply ops and return Pydantic model (for differ which accepts models)."""
    return apply_operations(_empty(), ops)


def _apply(ops: list) -> dict:
    """Apply ops and return dict (for subscript access in assertions)."""
    return apply_operations(_empty(), ops).model_dump()


@pytest.mark.integration
class TestStateReducerGrants:
    def test_add_grant_to_catalog(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.grant.add_grant("catalog", "cat_1", "team", ["USE CATALOG"], op_id="op_2"),
        ]
        state = _apply(ops)
        cat = state["catalogs"][0]
        assert "grants" in cat
        assert len(cat["grants"]) == 1
        assert cat["grants"][0]["principal"] == "team"

    def test_revoke_grant_from_catalog(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.grant.add_grant(
                "catalog", "cat_1", "team", ["USE CATALOG", "CREATE SCHEMA"], op_id="op_2"
            ),
            builder.grant.revoke_grant("catalog", "cat_1", "team", ["USE CATALOG"], op_id="op_3"),
        ]
        state = _apply(ops)
        cat = state["catalogs"][0]
        grants = cat.get("grants", [])
        if grants:
            team_grant = next((g for g in grants if g["principal"] == "team"), None)
            if team_grant:
                assert "USE CATALOG" not in team_grant["privileges"]

    def test_add_grant_to_table(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.grant.add_grant("table", "t1", "readers", ["SELECT"], op_id="op_4"),
        ]
        state = _apply(ops)
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert "grants" in table
        assert len(table["grants"]) == 1


@pytest.mark.integration
class TestStateReducerConstraints:
    def test_add_primary_key(self) -> None:
        builder = OperationBuilder()
        state = _apply(ops_table_with_pk(builder))
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert "constraints" in table
        assert len(table["constraints"]) == 1
        assert table["constraints"][0]["type"] == "primary_key"

    def test_drop_constraint(self) -> None:
        builder = OperationBuilder()
        ops = ops_table_with_pk(builder) + [
            builder.constraint.drop_constraint("pk_1", "t1", op_id="op_6"),
        ]
        state = _apply(ops)
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        constraints = table.get("constraints", [])
        assert len(constraints) == 0


@pytest.mark.integration
class TestStateReducerRowFilters:
    def test_add_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.row_filter.add_row_filter(
                "rf1",
                "t1",
                "region_filter",
                "region = 'US'",
                description="US only",
                op_id="op_4",
            ),
        ]
        state = _apply(ops)
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert "row_filters" in table
        assert len(table["row_filters"]) == 1
        assert table["row_filters"][0]["name"] == "region_filter"

    def test_update_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.row_filter.add_row_filter("rf1", "t1", "rf", "1=1", op_id="op_4"),
            builder.row_filter.update_row_filter(
                "rf1",
                "t1",
                name="updated_rf",
                enabled=False,
                op_id="op_5",
            ),
        ]
        state = _apply(ops)
        row_filter = state["catalogs"][0]["schemas"][0]["tables"][0]["row_filters"][0]
        assert row_filter["name"] == "updated_rf"
        assert row_filter["enabled"] is False

    def test_remove_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.row_filter.add_row_filter("rf1", "t1", "rf", "1=1", op_id="op_4"),
            builder.row_filter.remove_row_filter("rf1", "t1", op_id="op_5"),
        ]
        state = _apply(ops)
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert len(table.get("row_filters", [])) == 0


@pytest.mark.integration
class TestStateReducerColumnMasks:
    def test_add_column_mask(self) -> None:
        builder = OperationBuilder()
        state = _apply(ops_table_with_ssn_mask(builder))
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert "column_masks" in table
        assert len(table["column_masks"]) == 1

    def test_update_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = ops_table_with_ssn_mask(builder) + [
            builder.column_mask.update_column_mask(
                "cm1",
                "t1",
                name="updated_mask",
                enabled=False,
                op_id="op_6",
            ),
        ]
        state = _apply(ops)
        col_mask = state["catalogs"][0]["schemas"][0]["tables"][0]["column_masks"][0]
        assert col_mask["name"] == "updated_mask"

    def test_remove_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = ops_table_with_ssn_mask(builder) + [
            builder.column_mask.remove_column_mask("cm1", "t1", "c1", op_id="op_6"),
        ]
        state = _apply(ops)
        table = state["catalogs"][0]["schemas"][0]["tables"][0]
        assert len(table.get("column_masks", [])) == 0


@pytest.mark.integration
class TestStateReducerVolumes:
    def test_add_and_update_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume("v1", "data_files", "s1", "managed", op_id="op_3"),
            builder.volume.update_volume("v1", comment="Updated comment", op_id="op_4"),
        ]
        state = _apply(ops)
        vol = state["catalogs"][0]["schemas"][0]["volumes"][0]
        assert vol["name"] == "data_files"
        assert vol["comment"] == "Updated comment"


@pytest.mark.integration
class TestStateReducerFunctions:
    def test_add_and_update_function(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_function(builder) + [
            builder.function.update_function("f1", body="x * 3", op_id="op_4"),
        ]
        state = _apply(ops)
        func = state["catalogs"][0]["schemas"][0]["functions"][0]
        assert func["name"] == "double_it"
        assert func["body"] == "x * 3"


@pytest.mark.integration
class TestStateReducerMaterializedViews:
    def test_add_and_update_mv(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_mv(builder) + [
            builder.materialized_view.update_materialized_view(
                "mv1",
                definition="SELECT 2",
                op_id="op_4",
            ),
        ]
        state = _apply(ops)
        mat_view = state["catalogs"][0]["schemas"][0]["materialized_views"][0]
        assert mat_view["definition"] == "SELECT 2"


@pytest.mark.integration
class TestStateDiffer:
    def test_diff_detects_added_table(self) -> None:
        builder = OperationBuilder()
        old_ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ]
        new_ops = old_ops + [
            builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3"),
        ]
        old_state = _apply_model(old_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, old_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_table" in t for t in op_types)

    def test_diff_detects_dropped_table(self) -> None:
        builder = OperationBuilder()
        old_ops = ops_catalog_schema_table(builder)
        new_ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ]
        old_state = _apply_model(old_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, old_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("drop_table" in t for t in op_types)

    def test_diff_detects_added_column(self) -> None:
        builder = OperationBuilder()
        base_ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ]
        new_ops = base_ops + [
            builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_column" in t for t in op_types)

    def test_diff_detects_added_grant(self) -> None:
        builder = OperationBuilder()
        base_ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
        ]
        new_ops = base_ops + [
            builder.grant.add_grant("catalog", "cat_1", "team", ["USE CATALOG"], op_id="op_2"),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_grant" in t for t in op_types)

    def test_diff_detects_added_volume(self) -> None:
        builder = OperationBuilder()
        base_ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ]
        new_ops = base_ops + [
            builder.volume.add_volume("v1", "data_files", "s1", "managed", op_id="op_3"),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_volume" in t for t in op_types)

    def test_diff_detects_added_function(self) -> None:
        builder = OperationBuilder()
        base_ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ]
        new_ops = base_ops + [
            builder.function.add_function(
                "f1",
                "my_fn",
                "s1",
                "SQL",
                "INT",
                "1",
                op_id="op_3",
            ),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_function" in t for t in op_types)

    def test_diff_no_changes(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ]
        state = _apply_model(ops)
        differ = UnityStateDiffer(state, state, ops, ops)
        diff_ops = differ.generate_diff_operations()
        assert len(diff_ops) == 0

    def test_diff_detects_added_constraint(self) -> None:
        builder = OperationBuilder()
        base_ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
        ]
        new_ops = base_ops + [
            builder.constraint.add_constraint(
                "pk_1", "t1", "primary_key", ["c1"], name="pk_orders", op_id="op_5"
            ),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("add_constraint" in t for t in op_types)

    def test_diff_detects_table_property_change(self) -> None:
        builder = OperationBuilder()
        base_ops = ops_catalog_schema_table(builder)
        new_ops = base_ops + [
            builder.table.set_table_property(
                "t1", "delta.autoOptimize.optimizeWrite", "true", op_id="op_4"
            ),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("set_table_property" in t for t in op_types)

    def test_diff_detects_table_tag_change(self) -> None:
        builder = OperationBuilder()
        base_ops = ops_catalog_schema_table(builder)
        new_ops = base_ops + [
            builder.table.set_table_tag("t1", "pii", "true", op_id="op_4"),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("set_table_tag" in t for t in op_types)

    def test_diff_detects_column_tag_change(self) -> None:
        builder = OperationBuilder()
        base_ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "email", "STRING", op_id="op_4"),
        ]
        new_ops = base_ops + [
            builder.column.set_column_tag("c1", "t1", "pii", "email", op_id="op_5"),
        ]
        old_state = _apply_model(base_ops)
        new_state = _apply_model(new_ops)
        differ = UnityStateDiffer(old_state, new_state, base_ops, new_ops)
        diff_ops = differ.generate_diff_operations()
        op_types = [diff_op.op for diff_op in diff_ops]
        assert any("set_column_tag" in t for t in op_types)
