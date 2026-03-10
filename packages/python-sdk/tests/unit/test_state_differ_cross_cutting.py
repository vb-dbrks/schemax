"""
Cross-cutting state differ tests: grant edge cases, metadata diffs, compound scenarios.
"""

from schemax.providers.unity.state_differ import UnityStateDiffer
from tests.unit.state_differ_helpers import (
    _base_state,
    _catalog,
    _col,
    _constraint,
    _function,
    _grant,
    _make_op,
    _mv,
    _op_types,
    _ops_of_type,
    _schema,
    _table,
    _view,
    _volume,
)

# ─── 10. Grant Edge Cases ────────────────────────────────────────────────────


class TestGrantEdgeCases:
    def _wrap_table_grants(self, grants_old, grants_new):
        old = _base_state(
            [
                _catalog(
                    "c1", "main", [_schema("s1", "raw", [_table("t1", "users", grants=grants_old)])]
                )
            ]
        )
        new = _base_state(
            [
                _catalog(
                    "c1", "main", [_schema("s1", "raw", [_table("t1", "users", grants=grants_new)])]
                )
            ]
        )
        return UnityStateDiffer(old, new).generate_diff_operations()

    def test_empty_principal_skipped(self):
        ops = self._wrap_table_grants([], [{"principal": "", "privileges": ["SELECT"]}])
        grant_ops = [o for o in ops if "grant" in o.op]
        assert len(grant_ops) == 0, "Empty principal should be skipped"

    def test_whitespace_principal_skipped(self):
        ops = self._wrap_table_grants([], [{"principal": "   ", "privileges": ["SELECT"]}])
        grant_ops = [o for o in ops if "grant" in o.op]
        assert len(grant_ops) == 0

    def test_multiple_principals(self):
        ops = self._wrap_table_grants(
            [], [_grant("analysts", ["SELECT"]), _grant("engineers", ["SELECT", "MODIFY"])]
        )
        grant_ops = _ops_of_type(ops, "unity.add_grant")
        assert len(grant_ops) == 2

    def test_partial_privilege_revoke(self):
        """Remove one privilege but keep others"""
        ops = self._wrap_table_grants(
            [_grant("analysts", ["SELECT", "MODIFY"])], [_grant("analysts", ["SELECT"])]
        )
        revoke_ops = _ops_of_type(ops, "unity.revoke_grant")
        assert len(revoke_ops) == 1
        assert "MODIFY" in revoke_ops[0].payload["privileges"]

    def test_full_revoke(self):
        """Remove all privileges from a principal"""
        ops = self._wrap_table_grants([_grant("analysts", ["SELECT"])], [])
        revoke_ops = _ops_of_type(ops, "unity.revoke_grant")
        assert len(revoke_ops) == 1

    def test_grant_add_and_revoke_different_principals(self):
        ops = self._wrap_table_grants(
            [_grant("old_team", ["SELECT"])], [_grant("new_team", ["SELECT"])]
        )
        types = _op_types(ops)
        assert "unity.add_grant" in types
        assert "unity.revoke_grant" in types

    def test_grants_unchanged(self):
        g = [_grant("analysts", ["SELECT", "MODIFY"])]
        ops = self._wrap_table_grants(g, g)
        grant_ops = [o for o in ops if "grant" in o.op]
        assert len(grant_ops) == 0


# ─── 13. Catalog/Schema Metadata Diff ────────────────────────────────────────


class TestMetadataDiff:
    """Test catalog/schema metadata diff support (comment, tags, managedLocationName)."""

    def test_existing_catalog_comment_diff(self):
        """Differ should detect comment changes on existing catalogs"""
        old = _base_state([_catalog("c1", "main", comment="old")])
        new = _base_state([_catalog("c1", "main", comment="new")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"
        assert ops[0].payload["comment"] == "new"

    def test_existing_catalog_comment_remove(self):
        """Removing catalog comment should produce update_catalog"""
        old = _base_state([_catalog("c1", "main", comment="old")])
        new = _base_state([_catalog("c1", "main", comment=None)])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"
        assert ops[0].payload["comment"] == ""

    def test_existing_catalog_tag_diff(self):
        """Differ should detect tag changes on existing catalogs"""
        old = _base_state([_catalog("c1", "main", tags={"env": "dev"})])
        new = _base_state([_catalog("c1", "main", tags={"env": "prod"})])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"
        assert ops[0].payload["tags"] == {"env": "prod"}

    def test_existing_catalog_tag_add(self):
        old = _base_state([_catalog("c1", "main", tags={})])
        new = _base_state([_catalog("c1", "main", tags={"env": "prod"})])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"

    def test_existing_catalog_tag_remove(self):
        old = _base_state([_catalog("c1", "main", tags={"env": "prod"})])
        new = _base_state([_catalog("c1", "main", tags={})])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"
        assert ops[0].payload["tags"] == {}

    def test_existing_catalog_managed_location_diff(self):
        """Differ should detect managedLocationName changes on existing catalogs"""
        old = _base_state([_catalog("c1", "main", managedLocationName="loc_a")])
        new = _base_state([_catalog("c1", "main", managedLocationName="loc_b")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_catalog"
        assert ops[0].payload["managedLocationName"] == "loc_b"

    def test_catalog_multiple_metadata_changes(self):
        """Comment + tags + location all change -> single update_catalog op"""
        old = _base_state(
            [_catalog("c1", "main", comment="old", tags={"a": "1"}, managedLocationName="loc_a")]
        )
        new = _base_state(
            [_catalog("c1", "main", comment="new", tags={"b": "2"}, managedLocationName="loc_b")]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        update_ops = _ops_of_type(ops, "unity.update_catalog")
        assert len(update_ops) == 1
        p = update_ops[0].payload
        assert p["comment"] == "new"
        assert p["tags"] == {"b": "2"}
        assert p["managedLocationName"] == "loc_b"

    def test_catalog_metadata_unchanged(self):
        old = _base_state([_catalog("c1", "main", comment="same", tags={"a": "1"})])
        new = _base_state([_catalog("c1", "main", comment="same", tags={"a": "1"})])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert ops == []

    def test_existing_schema_comment_diff(self):
        """Differ should detect comment changes on existing schemas"""
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", comment="old")])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", comment="new")])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_schema"
        assert ops[0].payload["comment"] == "new"

    def test_existing_schema_tag_diff(self):
        """Differ should detect tag changes on existing schemas"""
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tags={"a": "1"})])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tags={"a": "2"})])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_schema"
        assert ops[0].payload["tags"] == {"a": "2"}

    def test_existing_schema_managed_location_diff(self):
        """Differ should detect managedLocationName changes on existing schemas"""
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", managedLocationName="loc_a")])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", managedLocationName="loc_b")])]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.update_schema"
        assert ops[0].payload["managedLocationName"] == "loc_b"

    def test_schema_metadata_unchanged(self):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", comment="same", tags={"a": "1"})])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", comment="same", tags={"a": "1"})])]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert ops == []


# ─── 12. Compound / Cross-Cutting Edge Cases ─────────────────────────────────


class TestCompoundEdgeCases:
    def test_multiple_renames_at_same_level(self):
        """7.4 - Two columns renamed in the same table"""
        rename_op1 = _make_op(
            "rename_column", "c1", {"oldName": "a", "newName": "x", "tableId": "t1"}
        )
        rename_op2 = _make_op(
            "rename_column", "c2", {"oldName": "b", "newName": "y", "tableId": "t1"}
        )
        old = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1", "raw", [_table("t1", "users", [_col("c1", "a"), _col("c2", "b")])]
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
                            "s1", "raw", [_table("t1", "users", [_col("c1", "x"), _col("c2", "y")])]
                        )
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old, new, [], [rename_op1, rename_op2]).generate_diff_operations()
        rename_ops = _ops_of_type(ops, "unity.rename_column")
        assert len(rename_ops) == 2
        renamed_targets = {o.target for o in rename_ops}
        assert renamed_targets == {"c1", "c2"}

    def test_rename_plus_add_with_old_name(self):
        """7.5 - Rename col_a -> col_b, then add new col_a"""
        rename_op = _make_op(
            "rename_column", "c1", {"oldName": "col_a", "newName": "col_b", "tableId": "t1"}
        )
        old = _base_state(
            [
                _catalog(
                    "c1", "main", [_schema("s1", "raw", [_table("t1", "t", [_col("c1", "col_a")])])]
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
                            [_table("t1", "t", [_col("c1", "col_b"), _col("c2", "col_a")])],
                        )
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old, new, [], [rename_op]).generate_diff_operations()
        types = _op_types(ops)
        assert "unity.rename_column" in types
        assert "unity.add_column" in types
        add_ops = _ops_of_type(ops, "unity.add_column")
        assert add_ops[0].payload["name"] == "col_a"

    def test_idempotent_reimport(self):
        """7.12 - Import same state twice produces zero new ops"""
        state = _base_state(
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
                                    [
                                        _col("c1", "id", "BIGINT", False),
                                        _col("c2", "name", "STRING", True),
                                    ],
                                    comment="Users table",
                                    properties={"delta.logRetentionDuration": "interval 30 days"},
                                    tags={"domain": "identity"},
                                    constraints=[_constraint("pk1", "primary_key", ["c1"])],
                                    grants=[_grant("analysts", ["SELECT"])],
                                )
                            ],
                            views=[_view("v1", "vw", "SELECT 1")],
                            volumes=[_volume("vol1", "data")],
                            functions=[_function("f1", "fn")],
                            materialized_views=[_mv("mv1", "agg")],
                            grants=[_grant("admins", ["ALL_PRIVILEGES"])],
                        )
                    ],
                    grants=[_grant("admin_group", ["USE_CATALOG"])],
                )
            ]
        )
        ops = UnityStateDiffer(state, state).generate_diff_operations()
        assert ops == [], f"Idempotent reimport should produce zero ops, got: {_op_types(ops)}"

    def test_unicode_in_names_and_comments(self):
        """7.14 - Unicode characters"""
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
                                    "t1", "users", [_col("c1", "名前", "STRING", comment="日本語")]
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
                            [_table("t1", "users", [_col("c1", "名前", "STRING", comment="中文")])],
                        )
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        comment_ops = _ops_of_type(ops, "unity.set_column_comment")
        assert len(comment_ops) == 1

    def test_many_tables_performance(self):
        """7.13 - Many tables"""
        tables = [
            _table(
                f"t{i}",
                f"table_{i}",
                [
                    _col(f"t{i}_c1", "id", "BIGINT"),
                    _col(f"t{i}_c2", "val", "STRING"),
                ],
            )
            for i in range(100)
        ]
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tables)])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", tables)])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert ops == []

    def test_many_tables_with_changes(self):
        """100 tables, each with a column type change"""
        old_tables = [
            _table(f"t{i}", f"table_{i}", [_col(f"t{i}_c1", "id", "INT")]) for i in range(100)
        ]
        new_tables = [
            _table(f"t{i}", f"table_{i}", [_col(f"t{i}_c1", "id", "BIGINT")]) for i in range(100)
        ]
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw", old_tables)])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw", new_tables)])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        type_ops = _ops_of_type(ops, "unity.change_column_type")
        assert len(type_ops) == 100

    def test_mixed_adds_and_drops_across_schemas(self):
        """Add table in one schema, drop table in another"""
        old = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema("s1", "raw", [_table("t1", "users")]),
                        _schema("s2", "curated", [_table("t2", "orders")]),
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
                        _schema("s1", "raw", [_table("t1", "users"), _table("t3", "events")]),
                        _schema("s2", "curated", []),
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert any(o.op == "unity.add_table" and o.target == "t3" for o in ops)
        assert any(o.op == "unity.drop_table" and o.target == "t2" for o in ops)

    def test_new_catalog_with_deeply_nested_objects(self):
        """New catalog should recursively add all children"""
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
                                    [
                                        _col("col1", "id", "BIGINT", tags={"pii": "false"}),
                                        _col("col2", "email", "STRING", tags={"pii": "true"}),
                                    ],
                                    tags={"env": "prod"},
                                    constraints=[_constraint("pk1", "primary_key", ["col1"])],
                                    grants=[_grant("team", ["SELECT"])],
                                ),
                            ],
                            views=[_view("v1", "vw")],
                            volumes=[_volume("vol1", "d")],
                            functions=[_function("f1", "fn")],
                            materialized_views=[_mv("mv1", "agg")],
                            grants=[_grant("schema_admin", ["ALL_PRIVILEGES"])],
                        ),
                    ],
                    grants=[_grant("cat_admin", ["USE_CATALOG"])],
                )
            ]
        )
        ops = UnityStateDiffer(_base_state(), new).generate_diff_operations()
        types = _op_types(ops)
        expected_ops = {
            "unity.add_catalog",
            "unity.add_schema",
            "unity.add_table",
            "unity.add_column",
            "unity.add_view",
            "unity.add_volume",
            "unity.add_function",
            "unity.add_materialized_view",
            "unity.set_table_tag",
            "unity.set_column_tag",
            "unity.add_constraint",
            "unity.add_grant",
        }
        for expected in expected_ops:
            assert expected in types, f"Missing {expected} in greenfield import"

    def test_pydantic_model_input(self):
        """State differ should work with Pydantic models too"""
        from schemax.providers.unity.models import (
            UnityCatalog,
            UnityColumn,
            UnitySchema,
            UnityState,
            UnityTable,
        )

        old_state = UnityState(catalogs=[])
        new_state = UnityState(
            catalogs=[
                UnityCatalog(
                    id="c1",
                    name="main",
                    schemas=[
                        UnitySchema(
                            id="s1",
                            name="raw",
                            tables=[
                                UnityTable(
                                    id="t1",
                                    name="users",
                                    format="delta",
                                    columns=[
                                        UnityColumn(
                                            id="col1", name="id", type="BIGINT", nullable=False
                                        ),
                                    ],
                                )
                            ],
                        )
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old_state, new_state).generate_diff_operations()
        types = _op_types(ops)
        assert "unity.add_catalog" in types
        assert "unity.add_schema" in types
        assert "unity.add_table" in types
        assert "unity.add_column" in types
