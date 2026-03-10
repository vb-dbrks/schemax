"""
Basic state differ tests: boundary cases, catalog mutations, schema mutations.
"""

from schemax.providers.unity.state_differ import UnityStateDiffer
from tests.unit.state_differ_helpers import (
    _base_state,
    _catalog,
    _col,
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

# ─── 1. Boundary Cases ───────────────────────────────────────────────────────


class TestBoundaryCases:
    """Test empty/identical/full boundary scenarios"""

    def test_identical_states_produce_no_ops(self):
        """7.1 - No changes at all"""
        state = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1",
                            "raw",
                            [_table("t1", "users", [_col("col1", "id", "BIGINT", False)])],
                        )
                    ],
                )
            ]
        )
        differ = UnityStateDiffer(state, state)
        ops = differ.generate_diff_operations()
        assert ops == [], f"Expected no ops, got: {_op_types(ops)}"

    def test_empty_to_empty_no_ops(self):
        """Both states empty"""
        differ = UnityStateDiffer(_base_state(), _base_state())
        assert differ.generate_diff_operations() == []

    def test_empty_to_full_greenfield(self):
        """7.2 - Full greenfield import"""
        old = _base_state()
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
                                        _col("col1", "id", "BIGINT", False),
                                        _col("col2", "name", "STRING", True),
                                    ],
                                    tags={"env": "prod"},
                                    properties={"delta.autoOptimize.optimizeWrite": "true"},
                                    constraints=[
                                        {"id": "pk1", "type": "primary_key", "columns": ["col1"]}
                                    ],
                                    grants=[_grant("analysts", ["SELECT"])],
                                ),
                            ],
                            views=[
                                _view("v1", "user_summary", "SELECT id FROM users"),
                            ],
                            volumes=[
                                _volume("vol1", "raw_data"),
                            ],
                            functions=[
                                _function("f1", "double_it", "RETURN x * 2"),
                            ],
                            materialized_views=[
                                _mv("mv1", "user_agg", "SELECT count(*) FROM users"),
                            ],
                            grants=[_grant("admins", ["ALL_PRIVILEGES"])],
                        ),
                    ],
                    grants=[_grant("admin_group", ["USE_CATALOG"])],
                ),
            ]
        )
        differ = UnityStateDiffer(old, new)
        ops = differ.generate_diff_operations()

        types = _op_types(ops)
        assert "unity.add_catalog" in types
        assert "unity.add_schema" in types
        assert "unity.add_table" in types
        assert "unity.add_column" in types
        assert "unity.add_view" in types
        assert "unity.add_volume" in types
        assert "unity.add_function" in types
        assert "unity.add_materialized_view" in types
        assert "unity.set_table_tag" in types
        assert "unity.add_constraint" in types
        assert "unity.add_grant" in types

    def test_full_to_empty_drop_everything(self):
        """7.3 - Drop everything"""
        full = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1",
                            "raw",
                            [_table("t1", "users", [_col("col1", "id")])],
                            views=[_view("v1", "v")],
                            volumes=[_volume("vol1", "d")],
                            functions=[_function("f1", "fn")],
                            materialized_views=[_mv("mv1", "mv_agg")],
                        )
                    ],
                )
            ]
        )
        empty = _base_state()
        differ = UnityStateDiffer(full, empty)
        ops = differ.generate_diff_operations()
        types = _op_types(ops)
        assert "unity.drop_catalog" in types

    def test_none_states(self):
        """Handles None state gracefully"""
        differ = UnityStateDiffer(None, None)
        assert differ.generate_diff_operations() == []

    def test_none_old_state(self):
        """None old state = everything is new"""
        new = _base_state([_catalog("c1", "main")])
        differ = UnityStateDiffer(None, new)
        ops = differ.generate_diff_operations()
        assert len(ops) >= 1
        assert ops[0].op == "unity.add_catalog"


# ─── 2. Catalog Mutations ────────────────────────────────────────────────────


class TestCatalogMutations:
    def test_add_catalog(self):
        old = _base_state()
        new = _base_state([_catalog("c1", "bronze")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.add_catalog"

    def test_drop_catalog(self):
        old = _base_state([_catalog("c1", "bronze")])
        new = _base_state()
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.drop_catalog"

    def test_rename_catalog_with_history(self):
        old = _base_state([_catalog("c1", "bronze")])
        new = _base_state([_catalog("c1", "silver")])
        rename_op = _make_op("rename_catalog", "c1", {"oldName": "bronze", "newName": "silver"})
        ops = UnityStateDiffer(old, new, [], [rename_op]).generate_diff_operations()
        assert len(ops) == 1
        assert ops[0].op == "unity.rename_catalog"

    def test_rename_catalog_without_history_no_rename_op(self):
        """1.4 - Without op history, name change on same ID should NOT produce rename"""
        old = _base_state([_catalog("c1", "bronze")])
        new = _base_state([_catalog("c1", "silver")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        rename_ops = _ops_of_type(ops, "unity.rename_catalog")
        assert len(rename_ops) == 0, "Should not rename without op history"

    def test_catalog_comment_change(self):
        old = _base_state([_catalog("c1", "main", comment="Old comment")])
        new = _base_state([_catalog("c1", "main", comment="New comment")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        update_ops = _ops_of_type(ops, "unity.update_catalog")
        assert len(update_ops) == 1
        assert update_ops[0].payload.get("comment") == "New comment"

    def test_catalog_tags_added(self):
        old = _base_state([_catalog("c1", "main", tags={})])
        new = _base_state([_catalog("c1", "main", tags={"env": "prod"})])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert any(o.op == "unity.update_catalog" for o in ops)

    def test_catalog_managed_location_change(self):
        old = _base_state([_catalog("c1", "main", managedLocationName="loc1")])
        new = _base_state([_catalog("c1", "main", managedLocationName="loc2")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert any(o.op == "unity.update_catalog" for o in ops)

    def test_catalog_grants_added(self):
        old = _base_state([_catalog("c1", "main", grants=[])])
        new = _base_state([_catalog("c1", "main", grants=[_grant("admin", ["USE_CATALOG"])])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert any(o.op == "unity.add_grant" for o in ops)

    def test_catalog_grants_revoked(self):
        old = _base_state([_catalog("c1", "main", grants=[_grant("admin", ["USE_CATALOG"])])])
        new = _base_state([_catalog("c1", "main", grants=[])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert any(o.op == "unity.revoke_grant" for o in ops)

    def test_add_multiple_catalogs(self):
        old = _base_state()
        new = _base_state(
            [_catalog("c1", "bronze"), _catalog("c2", "silver"), _catalog("c3", "gold")]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        add_ops = _ops_of_type(ops, "unity.add_catalog")
        assert len(add_ops) == 3


# ─── 3. Schema Mutations ─────────────────────────────────────────────────────


class TestSchemaMutations:
    def test_add_schema(self):
        old = _base_state([_catalog("c1", "main")])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "raw")])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert ops[0].op == "unity.add_schema"
        assert ops[0].payload["catalogId"] == "c1"

    def test_drop_schema(self):
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw")])])
        new = _base_state([_catalog("c1", "main")])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        assert ops[0].op == "unity.drop_schema"

    def test_rename_schema_with_history(self):
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw")])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "bronze")])])
        rename_op = _make_op("rename_schema", "s1", {"oldName": "raw", "newName": "bronze"})
        ops = UnityStateDiffer(old, new, [], [rename_op]).generate_diff_operations()
        assert ops[0].op == "unity.rename_schema"

    def test_rename_schema_without_history(self):
        """Same ID, different name, no history -> should NOT rename"""
        old = _base_state([_catalog("c1", "main", [_schema("s1", "raw")])])
        new = _base_state([_catalog("c1", "main", [_schema("s1", "bronze")])])
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        rename_ops = _ops_of_type(ops, "unity.rename_schema")
        assert len(rename_ops) == 0

    def test_schema_comment_and_tags(self):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", comment="old", tags={"a": "1"})])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", comment="new", tags={"b": "2"})])]
        )
        assert len(UnityStateDiffer(old, new).generate_diff_operations()) > 0

    def test_add_schema_with_all_children(self):
        """New schema should include all nested objects"""
        old = _base_state([_catalog("c1", "main")])
        new = _base_state(
            [
                _catalog(
                    "c1",
                    "main",
                    [
                        _schema(
                            "s1",
                            "raw",
                            [_table("t1", "users", [_col("col1", "id")])],
                            views=[_view("v1", "vw")],
                            volumes=[_volume("vol1", "data")],
                            functions=[_function("f1", "fn")],
                            materialized_views=[_mv("mv1", "agg")],
                        )
                    ],
                )
            ]
        )
        ops = UnityStateDiffer(old, new).generate_diff_operations()
        types = set(_op_types(ops))
        assert "unity.add_schema" in types
        assert "unity.add_table" in types
        assert "unity.add_column" in types
        assert "unity.add_view" in types
        assert "unity.add_volume" in types
        assert "unity.add_function" in types
        assert "unity.add_materialized_view" in types
