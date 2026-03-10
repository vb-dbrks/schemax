"""
Table-focused state differ tests: table mutations, column mutations,
constraint edge cases, table structural limitations.
"""

from schemax.providers.unity.state_differ import UnityStateDiffer
from tests.unit.state_differ_helpers import (
    _base_state,
    _catalog,
    _col,
    _constraint,
    _grant,
    _make_op,
    _op_types,
    _ops_of_type,
    _schema,
    _table,
)

# ─── 4. Table Mutations ──────────────────────────────────────────────────────


class TestTableMutations:
    def _wrap(self, tables_old, tables_new, **differ_kwargs):
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tables_old)])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tables_new)])])
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_table(self):
        ops = self._wrap([], [_table("t1", "users")])
        assert ops[0].op == "unity.add_table"

    def test_drop_table(self):
        ops = self._wrap([_table("t1", "users")], [])
        assert ops[0].op == "unity.drop_table"

    def test_rename_table_with_history(self):
        rename_op = _make_op("rename_table", "t1", {"oldName": "users", "newName": "customers"})
        ops = self._wrap(
            [_table("t1", "users")],
            [_table("t1", "customers")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_table"

    def test_rename_table_without_history(self):
        ops = self._wrap([_table("t1", "users")], [_table("t1", "customers")])
        rename_ops = _ops_of_type(ops, "unity.rename_table")
        assert len(rename_ops) == 0

    def test_table_comment_change(self):
        ops = self._wrap(
            [_table("t1", "users", comment="old")], [_table("t1", "users", comment="new")]
        )
        assert any(o.op == "unity.set_table_comment" for o in ops)

    def test_table_comment_add(self):
        ops = self._wrap([_table("t1", "users")], [_table("t1", "users", comment="new comment")])
        assert any(o.op == "unity.set_table_comment" for o in ops)

    def test_table_comment_remove(self):
        ops = self._wrap(
            [_table("t1", "users", comment="old")], [_table("t1", "users", comment=None)]
        )
        comment_ops = _ops_of_type(ops, "unity.set_table_comment")
        assert len(comment_ops) == 1

    def test_table_property_add(self):
        ops = self._wrap(
            [_table("t1", "users", properties={})],
            [_table("t1", "users", properties={"delta.autoOptimize.optimizeWrite": "true"})],
        )
        assert any(o.op == "unity.set_table_property" for o in ops)

    def test_table_property_remove(self):
        ops = self._wrap(
            [_table("t1", "users", properties={"key": "val"})],
            [_table("t1", "users", properties={})],
        )
        assert any(o.op == "unity.unset_table_property" for o in ops)

    def test_table_property_change_value(self):
        ops = self._wrap(
            [_table("t1", "users", properties={"key": "old"})],
            [_table("t1", "users", properties={"key": "new"})],
        )
        set_ops = _ops_of_type(ops, "unity.set_table_property")
        assert len(set_ops) == 1
        assert set_ops[0].payload["value"] == "new"

    def test_table_tag_add(self):
        ops = self._wrap(
            [_table("t1", "users", tags={})], [_table("t1", "users", tags={"env": "prod"})]
        )
        assert any(o.op == "unity.set_table_tag" for o in ops)

    def test_table_tag_remove(self):
        ops = self._wrap(
            [_table("t1", "users", tags={"env": "prod"})], [_table("t1", "users", tags={})]
        )
        assert any(o.op == "unity.unset_table_tag" for o in ops)

    def test_table_tag_change_value(self):
        ops = self._wrap(
            [_table("t1", "users", tags={"env": "dev"})],
            [_table("t1", "users", tags={"env": "prod"})],
        )
        tag_ops = _ops_of_type(ops, "unity.set_table_tag")
        assert len(tag_ops) == 1

    def test_table_multiple_property_changes(self):
        """Add one, remove one, change one property"""
        ops = self._wrap(
            [_table("t1", "users", properties={"keep": "same", "remove": "val", "change": "old"})],
            [_table("t1", "users", properties={"keep": "same", "add": "new", "change": "new"})],
        )
        set_ops = _ops_of_type(ops, "unity.set_table_property")
        unset_ops = _ops_of_type(ops, "unity.unset_table_property")
        assert len(set_ops) == 2  # add + change
        assert len(unset_ops) == 1  # remove

    def test_add_constraint_pk(self):
        ops = self._wrap(
            [_table("t1", "users", [_col("c1", "id")], constraints=[])],
            [
                _table(
                    "t1",
                    "users",
                    [_col("c1", "id")],
                    constraints=[_constraint("pk1", "primary_key", ["c1"])],
                )
            ],
        )
        assert any(o.op == "unity.add_constraint" for o in ops)

    def test_drop_constraint(self):
        ops = self._wrap(
            [
                _table(
                    "t1",
                    "users",
                    [_col("c1", "id")],
                    constraints=[_constraint("pk1", "primary_key", ["c1"])],
                )
            ],
            [_table("t1", "users", [_col("c1", "id")], constraints=[])],
        )
        assert any(o.op == "unity.drop_constraint" for o in ops)

    def test_modify_constraint_columns(self):
        """Changing constraint columns should DROP + ADD"""
        ops = self._wrap(
            [
                _table(
                    "t1",
                    "users",
                    [_col("c1", "id"), _col("c2", "name")],
                    constraints=[_constraint("pk1", "primary_key", ["c1"])],
                )
            ],
            [
                _table(
                    "t1",
                    "users",
                    [_col("c1", "id"), _col("c2", "name")],
                    constraints=[_constraint("pk1", "primary_key", ["c1", "c2"])],
                )
            ],
        )
        drop_ops = _ops_of_type(ops, "unity.drop_constraint")
        add_ops = _ops_of_type(ops, "unity.add_constraint")
        assert len(drop_ops) == 1
        assert len(add_ops) == 1

    def test_table_grants_added(self):
        ops = self._wrap(
            [_table("t1", "users", grants=[])],
            [_table("t1", "users", grants=[_grant("analysts", ["SELECT"])])],
        )
        assert any(o.op == "unity.add_grant" for o in ops)

    def test_table_grants_privilege_change(self):
        """Same principal, different privileges"""
        ops = self._wrap(
            [_table("t1", "users", grants=[_grant("analysts", ["SELECT"])])],
            [_table("t1", "users", grants=[_grant("analysts", ["SELECT", "MODIFY"])])],
        )
        grant_ops = [o for o in ops if "grant" in o.op]
        assert len(grant_ops) >= 1  # Should have add_grant for MODIFY


# ─── 5. Column Mutations ─────────────────────────────────────────────────────


class TestColumnMutations:
    def _wrap(self, cols_old, cols_new, **differ_kwargs):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", [_table("t1", "users", cols_old)])])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", [_table("t1", "users", cols_new)])])]
        )
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_column(self):
        ops = self._wrap(
            [_col("c1", "id", "BIGINT")], [_col("c1", "id", "BIGINT"), _col("c2", "name", "STRING")]
        )
        add_ops = _ops_of_type(ops, "unity.add_column")
        assert len(add_ops) == 1
        assert add_ops[0].payload["name"] == "name"

    def test_drop_column(self):
        ops = self._wrap([_col("c1", "id"), _col("c2", "name")], [_col("c1", "id")])
        drop_ops = _ops_of_type(ops, "unity.drop_column")
        assert len(drop_ops) == 1
        assert drop_ops[0].target == "c2"

    def test_rename_column_with_history(self):
        rename_op = _make_op(
            "rename_column", "c1", {"oldName": "user_id", "newName": "customer_id", "tableId": "t1"}
        )
        ops = self._wrap(
            [_col("c1", "user_id", "BIGINT")],
            [_col("c1", "customer_id", "BIGINT")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_column"

    def test_rename_column_without_history(self):
        """Same ID, different name, no history"""
        ops = self._wrap([_col("c1", "user_id", "BIGINT")], [_col("c1", "customer_id", "BIGINT")])
        rename_ops = _ops_of_type(ops, "unity.rename_column")
        assert len(rename_ops) == 0

    def test_change_type_widen(self):
        ops = self._wrap([_col("c1", "age", "INT")], [_col("c1", "age", "BIGINT")])
        assert any(o.op == "unity.change_column_type" for o in ops)
        type_op = _ops_of_type(ops, "unity.change_column_type")[0]
        assert type_op.payload["newType"] == "BIGINT"

    def test_change_type_narrow(self):
        ops = self._wrap([_col("c1", "age", "BIGINT")], [_col("c1", "age", "INT")])
        type_ops = _ops_of_type(ops, "unity.change_column_type")
        assert len(type_ops) == 1

    def test_change_type_incompatible(self):
        ops = self._wrap([_col("c1", "val", "STRING")], [_col("c1", "val", "INT")])
        assert any(o.op == "unity.change_column_type" for o in ops)

    def test_change_type_complex_struct(self):
        ops = self._wrap(
            [_col("c1", "data", "STRUCT<a:INT,b:STRING>")],
            [_col("c1", "data", "STRUCT<a:BIGINT,b:STRING,c:DOUBLE>")],
        )
        assert any(o.op == "unity.change_column_type" for o in ops)

    def test_change_type_array(self):
        ops = self._wrap(
            [_col("c1", "items", "ARRAY<INT>")], [_col("c1", "items", "ARRAY<BIGINT>")]
        )
        assert any(o.op == "unity.change_column_type" for o in ops)

    def test_change_type_map(self):
        ops = self._wrap(
            [_col("c1", "meta", "MAP<STRING,INT>")], [_col("c1", "meta", "MAP<STRING,BIGINT>")]
        )
        assert any(o.op == "unity.change_column_type" for o in ops)

    def test_not_null_to_nullable(self):
        ops = self._wrap(
            [_col("c1", "id", "BIGINT", nullable=False)],
            [_col("c1", "id", "BIGINT", nullable=True)],
        )
        null_ops = _ops_of_type(ops, "unity.set_nullable")
        assert len(null_ops) == 1
        assert null_ops[0].payload["nullable"] is True

    def test_nullable_to_not_null(self):
        ops = self._wrap(
            [_col("c1", "id", "BIGINT", nullable=True)],
            [_col("c1", "id", "BIGINT", nullable=False)],
        )
        null_ops = _ops_of_type(ops, "unity.set_nullable")
        assert len(null_ops) == 1
        assert null_ops[0].payload["nullable"] is False

    def test_column_comment_change(self):
        ops = self._wrap([_col("c1", "id", comment="old")], [_col("c1", "id", comment="new")])
        assert any(o.op == "unity.set_column_comment" for o in ops)

    def test_column_comment_add(self):
        ops = self._wrap([_col("c1", "id")], [_col("c1", "id", comment="new comment")])
        comment_ops = _ops_of_type(ops, "unity.set_column_comment")
        assert len(comment_ops) == 1

    def test_column_comment_remove(self):
        ops = self._wrap([_col("c1", "id", comment="old")], [_col("c1", "id", comment=None)])
        comment_ops = _ops_of_type(ops, "unity.set_column_comment")
        assert len(comment_ops) == 1

    def test_column_tag_add(self):
        ops = self._wrap([_col("c1", "id", tags={})], [_col("c1", "id", tags={"pii": "true"})])
        tag_ops = _ops_of_type(ops, "unity.set_column_tag")
        assert len(tag_ops) == 1

    def test_column_tag_remove(self):
        ops = self._wrap([_col("c1", "id", tags={"pii": "true"})], [_col("c1", "id", tags={})])
        tag_ops = _ops_of_type(ops, "unity.unset_column_tag")
        assert len(tag_ops) == 1

    def test_column_tag_change_value(self):
        ops = self._wrap(
            [_col("c1", "id", tags={"pii": "low"})], [_col("c1", "id", tags={"pii": "high"})]
        )
        tag_ops = _ops_of_type(ops, "unity.set_column_tag")
        assert len(tag_ops) == 1

    def test_rename_plus_type_change(self):
        """4.21 - Rename + type change simultaneously"""
        rename_op = _make_op(
            "rename_column", "c1", {"oldName": "user_id", "newName": "customer_id", "tableId": "t1"}
        )
        ops = self._wrap(
            [_col("c1", "user_id", "INT")],
            [_col("c1", "customer_id", "BIGINT")],
            old_operations=[],
            new_operations=[rename_op],
        )
        types = _op_types(ops)
        assert "unity.rename_column" in types
        assert "unity.change_column_type" in types

    def test_rename_plus_nullable_change(self):
        """4.22 - Rename + nullable change simultaneously"""
        rename_op = _make_op(
            "rename_column", "c1", {"oldName": "user_id", "newName": "customer_id", "tableId": "t1"}
        )
        ops = self._wrap(
            [_col("c1", "user_id", "BIGINT", nullable=True)],
            [_col("c1", "customer_id", "BIGINT", nullable=False)],
            old_operations=[],
            new_operations=[rename_op],
        )
        types = _op_types(ops)
        assert "unity.rename_column" in types
        assert "unity.set_nullable" in types

    def test_reorder_columns(self):
        ops = self._wrap(
            [_col("c1", "id"), _col("c2", "name"), _col("c3", "email")],
            [_col("c3", "email"), _col("c1", "id"), _col("c2", "name")],
        )
        reorder_ops = _ops_of_type(ops, "unity.reorder_columns")
        assert len(reorder_ops) == 1

    def test_reorder_with_add_no_reorder(self):
        """Adding a column changes the set, so no reorder should be emitted"""
        ops = self._wrap(
            [_col("c1", "id"), _col("c2", "name")],
            [_col("c2", "name"), _col("c1", "id"), _col("c3", "email")],
        )
        reorder_ops = _ops_of_type(ops, "unity.reorder_columns")
        assert len(reorder_ops) == 0

    def test_add_multiple_columns(self):
        ops = self._wrap(
            [_col("c1", "id")],
            [_col("c1", "id"), _col("c2", "name"), _col("c3", "email"), _col("c4", "age")],
        )
        add_ops = _ops_of_type(ops, "unity.add_column")
        assert len(add_ops) == 3

    def test_drop_multiple_columns(self):
        ops = self._wrap(
            [_col("c1", "id"), _col("c2", "name"), _col("c3", "email")], [_col("c1", "id")]
        )
        drop_ops = _ops_of_type(ops, "unity.drop_column")
        assert len(drop_ops) == 2

    def test_column_no_changes(self):
        cols = [_col("c1", "id", "BIGINT", False, comment="pk")]
        ops = self._wrap(cols, cols)
        assert ops == []

    def test_all_column_fields_change(self):
        """Change everything at once: type, nullable, comment"""
        ops = self._wrap(
            [_col("c1", "val", "INT", True, comment="old")],
            [_col("c1", "val", "BIGINT", False, comment="new")],
        )
        types = _op_types(ops)
        assert "unity.change_column_type" in types
        assert "unity.set_nullable" in types
        assert "unity.set_column_comment" in types


# ─── 11. Constraint Edge Cases ───────────────────────────────────────────────


class TestConstraintEdgeCases:
    def _wrap(self, constraints_old, constraints_new):
        old = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1",
                            "raw",
                            [
                                _table(
                                    "t1",
                                    "users",
                                    [_col("c1", "id"), _col("c2", "name")],
                                    constraints=constraints_old,
                                )
                            ],
                        )
                    ],
                )
            ]
        )
        new = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1",
                            "raw",
                            [
                                _table(
                                    "t1",
                                    "users",
                                    [_col("c1", "id"), _col("c2", "name")],
                                    constraints=constraints_new,
                                )
                            ],
                        )
                    ],
                )
            ]
        )
        return UnityStateDiffer(old, new).generate_diff_operations()

    def test_add_foreign_key(self):
        ops = self._wrap(
            [], [_constraint("fk1", "foreign_key", ["c2"], parentTable="t2", parentColumns=["c1"])]
        )
        add_ops = _ops_of_type(ops, "unity.add_constraint")
        assert len(add_ops) == 1
        assert add_ops[0].payload["type"] == "foreign_key"

    def test_add_check_constraint(self):
        ops = self._wrap([], [_constraint("ck1", "check", ["c1"], expression="c1 > 0")])
        add_ops = _ops_of_type(ops, "unity.add_constraint")
        assert len(add_ops) == 1
        assert add_ops[0].payload["expression"] == "c1 > 0"

    def test_change_constraint_expression(self):
        """Modifying a CHECK expression should drop + add"""
        ops = self._wrap(
            [_constraint("ck1", "check", ["c1"], expression="c1 > 0")],
            [_constraint("ck1", "check", ["c1"], expression="c1 > 10")],
        )
        drop_ops = _ops_of_type(ops, "unity.drop_constraint")
        add_ops = _ops_of_type(ops, "unity.add_constraint")
        assert len(drop_ops) == 1
        assert len(add_ops) == 1

    def test_change_fk_parent_table(self):
        """Changing FK parent should drop + add"""
        ops = self._wrap(
            [_constraint("fk1", "foreign_key", ["c2"], parentTable="t2", parentColumns=["c1"])],
            [_constraint("fk1", "foreign_key", ["c2"], parentTable="t3", parentColumns=["c1"])],
        )
        assert len(_ops_of_type(ops, "unity.drop_constraint")) == 1
        assert len(_ops_of_type(ops, "unity.add_constraint")) == 1

    def test_constraint_unchanged(self):
        c = [_constraint("pk1", "primary_key", ["c1"])]
        ops = self._wrap(c, c)
        constraint_ops = [o for o in ops if "constraint" in o.op]
        assert len(constraint_ops) == 0

    def test_swap_constraints(self):
        """Remove one constraint, add a different one"""
        ops = self._wrap(
            [_constraint("pk1", "primary_key", ["c1"])],
            [_constraint("ck1", "check", ["c2"], expression="c2 IS NOT NULL")],
        )
        assert len(_ops_of_type(ops, "unity.drop_constraint")) == 1
        assert len(_ops_of_type(ops, "unity.add_constraint")) == 1


# ─── 15. Table Format/External Changes ───────────────────────────────────────


class TestTableStructuralLimitations:
    """Table format/partition/cluster/external changes are not diffed."""

    def _wrap(self, tbl_old, tbl_new):
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", [tbl_old])])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", [tbl_new])])])
        return UnityStateDiffer(old, new).generate_diff_operations()

    def test_format_change_not_diffed(self):
        ops = self._wrap(_table("t1", "users", fmt="delta"), _table("t1", "users", fmt="iceberg"))
        assert len(ops) == 0, "Format changes should not produce ops (UC limitation)"

    def test_external_location_change_not_diffed(self):
        ops = self._wrap(
            _table("t1", "users", external=True, externalLocationName="loc1"),
            _table("t1", "users", external=True, externalLocationName="loc2"),
        )
        assert len(ops) == 0

    def test_partition_columns_change_not_diffed(self):
        ops = self._wrap(
            _table("t1", "users", partitionColumns=["col1"]),
            _table("t1", "users", partitionColumns=["col1", "col2"]),
        )
        assert len(ops) == 0

    def test_cluster_columns_change_not_diffed(self):
        ops = self._wrap(
            _table("t1", "users", clusterColumns=["col1"]),
            _table("t1", "users", clusterColumns=["col2"]),
        )
        assert len(ops) == 0
