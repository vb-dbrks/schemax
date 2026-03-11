"""Integration tests for state reducer edge cases.

Covers uncovered branches in state_reducer.py:
- unset_table_tag, unset_view_tag, unset_column_tag
- change_column_type, set_nullable, set_column_comment
- reorder_columns with missing columns
- set_column_tag on columns with None tags
- update_view, update_materialized_view
- add_constraint with insertAt
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder


def _empty():
    return UnityState(catalogs=[])


def _base_table_ops(builder):
    """Create base ops for a catalog + schema + table with columns."""
    return [
        builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
        builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
        builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
        builder.column.add_column("c3", "t1", "ts", "TIMESTAMP", op_id="op_6"),
    ]


# ── Tag Operations ──────────────────────────────────────────────────


@pytest.mark.integration
class TestUnsetTableTag:
    def test_unset_existing_tag(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.table.set_table_tag("t1", "pii", "true", op_id="op_7"),
            builder.table.unset_table_tag("t1", "pii", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        table = state.catalogs[0].schemas[0].tables[0]
        assert "pii" not in table.tags

    def test_unset_nonexistent_tag(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.table.unset_table_tag("t1", "nonexistent", op_id="op_7"),
        ]
        # Should not raise
        state = apply_operations(_empty(), ops)
        table = state.catalogs[0].schemas[0].tables[0]
        assert "nonexistent" not in table.tags


@pytest.mark.integration
class TestSetAndUnsetColumnTag:
    def test_set_column_tag(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            # col builder: (col_id, table_id, tag_name, tag_value)
            builder.column.set_column_tag("c1", "t1", "pii", "true", op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.tags["pii"] == "true"

    def test_unset_column_tag(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.set_column_tag("c1", "t1", "pii", "true", op_id="op_7"),
            # unset_column_tag: (col_id, table_id, tag_name)
            builder.column.unset_column_tag("c1", "t1", "pii", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert "pii" not in (col.tags or {})


# ── Column Operations ───────────────────────────────────────────────


@pytest.mark.integration
class TestColumnTypeChange:
    def test_change_column_type(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            # change_column_type: (col_id, table_id, new_type)
            builder.column.change_column_type("c1", "t1", "BIGINT", op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.type == "BIGINT"

    def test_change_column_type_string_to_binary(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.change_column_type("c2", "t1", "BINARY", op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[1]
        assert col.type == "BINARY"


@pytest.mark.integration
class TestSetNullable:
    def test_set_nullable_false(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            # set_nullable: (col_id, table_id, nullable)
            builder.column.set_nullable("c1", "t1", False, op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.nullable is False

    def test_set_nullable_true(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.set_nullable("c1", "t1", False, op_id="op_7"),
            builder.column.set_nullable("c1", "t1", True, op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.nullable is True


@pytest.mark.integration
class TestSetColumnComment:
    def test_set_comment(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            # set_column_comment: (col_id, table_id, comment)
            builder.column.set_column_comment("c1", "t1", "Primary key", op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.comment == "Primary key"

    def test_update_comment(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.set_column_comment("c1", "t1", "First", op_id="op_7"),
            builder.column.set_column_comment("c1", "t1", "Updated", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        col = state.catalogs[0].schemas[0].tables[0].columns[0]
        assert col.comment == "Updated"


# ── Reorder Columns ─────────────────────────────────────────────────


@pytest.mark.integration
class TestReorderColumns:
    def test_reorder_all_columns(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.reorder_columns("t1", ["c3", "c1", "c2"], op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        cols = state.catalogs[0].schemas[0].tables[0].columns
        assert [c.id for c in cols] == ["c3", "c1", "c2"]

    def test_reorder_partial(self) -> None:
        """Columns not in the order list go to the end."""
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column.reorder_columns("t1", ["c2", "c1"], op_id="op_7"),
        ]
        state = apply_operations(_empty(), ops)
        cols = state.catalogs[0].schemas[0].tables[0].columns
        # c2 and c1 first, c3 last (not in order list)
        assert cols[0].id == "c2"
        assert cols[1].id == "c1"
        assert cols[2].id == "c3"


# ── View Operations ─────────────────────────────────────────────────


@pytest.mark.integration
class TestViewOperations:
    def test_update_view_definition(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view(
                "v1", "user_summary", "s1", "SELECT 1", op_id="op_3"
            ),
            builder.view.update_view(
                "v1", definition="SELECT id, name FROM users", op_id="op_4"
            ),
        ]
        state = apply_operations(_empty(), ops)
        view = state.catalogs[0].schemas[0].views[0]
        assert view.definition == "SELECT id, name FROM users"

    def test_set_view_tag(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view(
                "v1", "user_summary", "s1", "SELECT 1", op_id="op_3"
            ),
            builder.view.set_view_tag("v1", "domain", "analytics", op_id="op_4"),
        ]
        state = apply_operations(_empty(), ops)
        view = state.catalogs[0].schemas[0].views[0]
        assert view.tags["domain"] == "analytics"

    def test_unset_view_tag(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view(
                "v1", "user_summary", "s1", "SELECT 1", op_id="op_3"
            ),
            builder.view.set_view_tag("v1", "domain", "analytics", op_id="op_4"),
            builder.view.unset_view_tag("v1", "domain", op_id="op_5"),
        ]
        state = apply_operations(_empty(), ops)
        view = state.catalogs[0].schemas[0].views[0]
        assert "domain" not in view.tags

    def test_rename_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view(
                "v1", "user_summary", "s1", "SELECT 1", op_id="op_3"
            ),
            # rename_view: (view_id, new_name, old_name)
            builder.view.rename_view("v1", "user_overview", "user_summary", op_id="op_4"),
        ]
        state = apply_operations(_empty(), ops)
        view = state.catalogs[0].schemas[0].views[0]
        assert view.name == "user_overview"


# ── Materialized View Operations ────────────────────────────────────


@pytest.mark.integration
class TestMaterializedViewOperations:
    def test_update_materialized_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1", "agg_users", "s1", "SELECT count(*) FROM users", op_id="op_3"
            ),
            builder.materialized_view.update_materialized_view(
                "mv1",
                definition="SELECT count(*) as cnt FROM users",
                op_id="op_4",
            ),
        ]
        state = apply_operations(_empty(), ops)
        mv = state.catalogs[0].schemas[0].materialized_views[0]
        assert mv.definition == "SELECT count(*) as cnt FROM users"

    def test_set_materialized_view_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1", "agg_users", "s1", "SELECT count(*) FROM users", op_id="op_3"
            ),
            builder.materialized_view.set_materialized_view_comment(
                "mv1", "Aggregated user counts", op_id="op_4"
            ),
        ]
        state = apply_operations(_empty(), ops)
        mv = state.catalogs[0].schemas[0].materialized_views[0]
        assert mv.comment == "Aggregated user counts"


# ── Constraint with insertAt ────────────────────────────────────────


@pytest.mark.integration
class TestConstraintInsertAt:
    def test_add_constraint_at_position(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.constraint.add_constraint(
                "ck1", "t1", "check", ["c1"],
                name="ck_positive", expression="id > 0", op_id="op_7"
            ),
            builder.constraint.add_constraint(
                "pk1", "t1", "primary_key", ["c1"],
                name="pk_events", insertAt=0, op_id="op_8"
            ),
        ]
        state = apply_operations(_empty(), ops)
        constraints = state.catalogs[0].schemas[0].tables[0].constraints
        assert len(constraints) == 2
        # pk inserted at position 0
        assert constraints[0].id == "pk1"
        assert constraints[1].id == "ck1"

    def test_drop_constraint(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.constraint.add_constraint(
                "pk1", "t1", "primary_key", ["c1"],
                name="pk_events", op_id="op_7"
            ),
            builder.constraint.drop_constraint("pk1", "t1", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        constraints = state.catalogs[0].schemas[0].tables[0].constraints
        assert len(constraints) == 0


# ── Row Filter and Column Mask ──────────────────────────────────────


@pytest.mark.integration
class TestRowFilterAndColumnMask:
    def test_update_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = 'US'",
                enabled=True, op_id="op_7"
            ),
            # update_row_filter: (filter_id, table_id, **kwargs)
            builder.row_filter.update_row_filter(
                "rf1", "t1", op_id="op_8", udfExpression="region IN ('US', 'CA')"
            ),
        ]
        state = apply_operations(_empty(), ops)
        rf = state.catalogs[0].schemas[0].tables[0].row_filters[0]
        assert "CA" in rf.udf_expression

    def test_remove_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = 'US'",
                enabled=True, op_id="op_7"
            ),
            builder.row_filter.remove_row_filter("rf1", "t1", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        assert len(state.catalogs[0].schemas[0].tables[0].row_filters) == 0

    def test_update_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column_mask.add_column_mask(
                "cm1", "t1", "c2", "name_mask",
                "CASE WHEN true THEN name ELSE '***' END",
                enabled=True, op_id="op_7"
            ),
            # update_column_mask: (mask_id, table_id, **kwargs)
            builder.column_mask.update_column_mask(
                "cm1", "t1", op_id="op_8",
                maskFunction="CASE WHEN is_admin() THEN name ELSE 'REDACTED' END"
            ),
        ]
        state = apply_operations(_empty(), ops)
        cm = state.catalogs[0].schemas[0].tables[0].column_masks[0]
        assert "REDACTED" in cm.mask_function

    def test_remove_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = _base_table_ops(builder) + [
            builder.column_mask.add_column_mask(
                "cm1", "t1", "c2", "name_mask",
                "CASE WHEN true THEN name ELSE '***' END",
                enabled=True, op_id="op_7"
            ),
            builder.column_mask.remove_column_mask("cm1", "t1", "c2", op_id="op_8"),
        ]
        state = apply_operations(_empty(), ops)
        assert len(state.catalogs[0].schemas[0].tables[0].column_masks) == 0
