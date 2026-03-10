"""
Object-level state differ tests: views, volumes, functions, materialized views,
view property/tag mutations.
"""

from schemax.providers.unity.state_differ import UnityStateDiffer
from tests.unit.state_differ_helpers import (
    _base_state,
    _catalog,
    _function,
    _grant,
    _make_op,
    _mv,
    _op_types,
    _ops_of_type,
    _schema,
    _view,
    _volume,
)

# ─── 6. View Mutations ───────────────────────────────────────────────────────


class TestViewMutations:
    def _wrap(self, views_old, views_new, **differ_kwargs):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], views=views_old)])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], views=views_new)])]
        )
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_view(self):
        ops = self._wrap([], [_view("v1", "summary", "SELECT 1")])
        assert ops[0].op == "unity.add_view"

    def test_add_view_with_tags(self):
        """New view with tags should generate add_view + set_view_tag ops."""
        ops = self._wrap(
            [], [_view("v1", "summary", "SELECT 1", tags={"env": "prod", "team": "data"})]
        )
        assert ops[0].op == "unity.add_view"
        tag_ops = _ops_of_type(ops, "unity.set_view_tag")
        assert len(tag_ops) == 2
        tag_names = {o.payload["tagName"] for o in tag_ops}
        assert tag_names == {"env", "team"}

    def test_drop_view(self):
        ops = self._wrap([_view("v1", "summary")], [])
        assert ops[0].op == "unity.drop_view"

    def test_rename_view_with_history(self):
        rename_op = _make_op("rename_view", "v1", {"oldName": "old_view", "newName": "new_view"})
        ops = self._wrap(
            [_view("v1", "old_view")],
            [_view("v1", "new_view")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_view"

    def test_view_definition_change(self):
        ops = self._wrap(
            [_view("v1", "summary", "SELECT 1")],
            [_view("v1", "summary", "SELECT count(*) FROM users")],
        )
        assert any(o.op == "unity.update_view" for o in ops)

    def test_view_comment_change(self):
        ops = self._wrap(
            [_view("v1", "summary", comment="old")], [_view("v1", "summary", comment="new")]
        )
        assert any(o.op == "unity.set_view_comment" for o in ops)

    def test_view_no_change(self):
        v = [_view("v1", "summary", "SELECT 1", comment="test")]
        ops = self._wrap(v, v)
        assert ops == []

    def test_view_grants_added(self):
        ops = self._wrap(
            [_view("v1", "summary", grants=[])],
            [_view("v1", "summary", grants=[_grant("analysts", ["SELECT"])])],
        )
        assert any(o.op == "unity.add_grant" for o in ops)

    def test_view_definition_and_comment_change(self):
        """Both definition and comment change simultaneously"""
        ops = self._wrap(
            [_view("v1", "summary", "SELECT 1", comment="old")],
            [_view("v1", "summary", "SELECT 2", comment="new")],
        )
        types = _op_types(ops)
        assert "unity.update_view" in types
        assert "unity.set_view_comment" in types


# ─── 7. Volume Mutations ─────────────────────────────────────────────────────


class TestVolumeMutations:
    def _wrap(self, vols_old, vols_new, **differ_kwargs):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], volumes=vols_old)])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], volumes=vols_new)])]
        )
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_volume(self):
        ops = self._wrap([], [_volume("vol1", "raw_data")])
        assert ops[0].op == "unity.add_volume"

    def test_drop_volume(self):
        ops = self._wrap([_volume("vol1", "raw_data")], [])
        assert ops[0].op == "unity.drop_volume"

    def test_rename_volume_with_history(self):
        rename_op = _make_op("rename_volume", "vol1", {"oldName": "old_vol", "newName": "new_vol"})
        ops = self._wrap(
            [_volume("vol1", "old_vol")],
            [_volume("vol1", "new_vol")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_volume"

    def test_volume_comment_change(self):
        ops = self._wrap(
            [_volume("vol1", "data", comment="old")], [_volume("vol1", "data", comment="new")]
        )
        assert any(o.op == "unity.update_volume" for o in ops)

    def test_volume_location_change(self):
        ops = self._wrap(
            [_volume("vol1", "data", volume_type="external", location="s3://old")],
            [_volume("vol1", "data", volume_type="external", location="s3://new")],
        )
        assert any(o.op == "unity.update_volume" for o in ops)

    def test_volume_no_change(self):
        v = [_volume("vol1", "data", comment="test")]
        ops = self._wrap(v, v)
        assert not _ops_of_type(ops, "unity.update_volume")

    def test_volume_grants(self):
        ops = self._wrap(
            [_volume("vol1", "data", grants=[])],
            [_volume("vol1", "data", grants=[_grant("team", ["READ_VOLUME"])])],
        )
        assert any(o.op == "unity.add_grant" for o in ops)


# ─── 8. Function Mutations ───────────────────────────────────────────────────


class TestFunctionMutations:
    def _wrap(self, funcs_old, funcs_new, **differ_kwargs):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], functions=funcs_old)])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], functions=funcs_new)])]
        )
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_function(self):
        ops = self._wrap([], [_function("f1", "double_it")])
        assert ops[0].op == "unity.add_function"

    def test_drop_function(self):
        ops = self._wrap([_function("f1", "double_it")], [])
        assert ops[0].op == "unity.drop_function"

    def test_rename_function_with_history(self):
        rename_op = _make_op("rename_function", "f1", {"oldName": "old_fn", "newName": "new_fn"})
        ops = self._wrap(
            [_function("f1", "old_fn")],
            [_function("f1", "new_fn")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_function"

    def test_function_body_change(self):
        ops = self._wrap(
            [_function("f1", "fn", body="RETURN 1")], [_function("f1", "fn", body="RETURN 2")]
        )
        assert any(o.op == "unity.update_function" for o in ops)

    def test_function_return_type_change(self):
        ops = self._wrap(
            [_function("f1", "fn", returnType="INT")], [_function("f1", "fn", returnType="BIGINT")]
        )
        assert any(o.op == "unity.update_function" for o in ops)

    def test_function_comment_change(self):
        ops = self._wrap(
            [_function("f1", "fn", comment="old")], [_function("f1", "fn", comment="new")]
        )
        assert any(o.op == "unity.set_function_comment" for o in ops)

    def test_function_no_change(self):
        f = [_function("f1", "fn", body="RETURN 1")]
        ops = self._wrap(f, f)
        assert ops == []

    def test_function_grants(self):
        ops = self._wrap(
            [_function("f1", "fn", grants=[])],
            [_function("f1", "fn", grants=[_grant("team", ["EXECUTE"])])],
        )
        assert any(o.op == "unity.add_grant" for o in ops)


# ─── 9. Materialized View Mutations ──────────────────────────────────────────


class TestMaterializedViewMutations:
    def _wrap(self, mvs_old, mvs_new, **differ_kwargs):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], materialized_views=mvs_old)])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], materialized_views=mvs_new)])]
        )
        return UnityStateDiffer(old, new, **differ_kwargs).generate_diff_operations()

    def test_add_mv(self):
        ops = self._wrap([], [_mv("mv1", "agg", "SELECT count(*) FROM t")])
        assert ops[0].op == "unity.add_materialized_view"

    def test_drop_mv(self):
        ops = self._wrap([_mv("mv1", "agg")], [])
        assert ops[0].op == "unity.drop_materialized_view"

    def test_rename_mv_with_history(self):
        rename_op = _make_op(
            "rename_materialized_view", "mv1", {"oldName": "old_mv", "newName": "new_mv"}
        )
        ops = self._wrap(
            [_mv("mv1", "old_mv")],
            [_mv("mv1", "new_mv")],
            old_operations=[],
            new_operations=[rename_op],
        )
        assert ops[0].op == "unity.rename_materialized_view"

    def test_mv_definition_change(self):
        ops = self._wrap(
            [_mv("mv1", "agg", "SELECT 1")], [_mv("mv1", "agg", "SELECT count(*) FROM users")]
        )
        assert any(o.op == "unity.update_materialized_view" for o in ops)

    def test_mv_refresh_schedule_change(self):
        ops = self._wrap(
            [_mv("mv1", "agg", refreshSchedule="CRON '0 * * * *'")],
            [_mv("mv1", "agg", refreshSchedule="CRON '0 0 * * *'")],
        )
        assert any(o.op == "unity.update_materialized_view" for o in ops)

    def test_mv_comment_change(self):
        ops = self._wrap([_mv("mv1", "agg", comment="old")], [_mv("mv1", "agg", comment="new")])
        assert any(o.op == "unity.set_materialized_view_comment" for o in ops)

    def test_mv_no_change(self):
        mv = [_mv("mv1", "agg", "SELECT 1", comment="test")]
        ops = self._wrap(mv, mv)
        assert ops == []

    def test_mv_grants(self):
        ops = self._wrap(
            [_mv("mv1", "agg", grants=[])], [_mv("mv1", "agg", grants=[_grant("team", ["SELECT"])])]
        )
        assert any(o.op == "unity.add_grant" for o in ops)


# ─── 14. View Property/Tag Mutations ─────────────────────────────────────────


class TestViewPropertyTagMutations:
    """Test view property and tag diffs."""

    def _wrap(self, views_old, views_new):
        old = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], views=views_old)])]
        )
        new = _base_state(
            [_catalog("c1", "main", [_schema("s1", "raw", tables=[], views=views_new)])]
        )
        return UnityStateDiffer(old, new).generate_diff_operations()

    def test_view_property_add(self):
        ops = self._wrap(
            [_view("v1", "vw", properties={})], [_view("v1", "vw", properties={"key": "val"})]
        )
        prop_ops = _ops_of_type(ops, "unity.set_view_property")
        assert len(prop_ops) == 1
        assert prop_ops[0].payload["key"] == "key"
        assert prop_ops[0].payload["value"] == "val"

    def test_view_property_remove(self):
        ops = self._wrap(
            [_view("v1", "vw", properties={"key": "val"})], [_view("v1", "vw", properties={})]
        )
        prop_ops = _ops_of_type(ops, "unity.unset_view_property")
        assert len(prop_ops) == 1

    def test_view_property_change(self):
        ops = self._wrap(
            [_view("v1", "vw", properties={"key": "old"})],
            [_view("v1", "vw", properties={"key": "new"})],
        )
        prop_ops = _ops_of_type(ops, "unity.set_view_property")
        assert len(prop_ops) == 1
        assert prop_ops[0].payload["value"] == "new"

    def test_view_property_unchanged(self):
        ops = self._wrap(
            [_view("v1", "vw", properties={"key": "val"})],
            [_view("v1", "vw", properties={"key": "val"})],
        )
        prop_ops = [o for o in ops if "property" in o.op]
        assert len(prop_ops) == 0

    def test_view_tag_add(self):
        ops = self._wrap([_view("v1", "vw", tags={})], [_view("v1", "vw", tags={"env": "prod"})])
        tag_ops = _ops_of_type(ops, "unity.set_view_tag")
        assert len(tag_ops) == 1
        assert tag_ops[0].payload["tagName"] == "env"
        assert tag_ops[0].payload["tagValue"] == "prod"

    def test_view_tag_remove(self):
        ops = self._wrap([_view("v1", "vw", tags={"env": "prod"})], [_view("v1", "vw", tags={})])
        tag_ops = _ops_of_type(ops, "unity.unset_view_tag")
        assert len(tag_ops) == 1
        assert tag_ops[0].payload["tagName"] == "env"

    def test_view_tag_change(self):
        ops = self._wrap(
            [_view("v1", "vw", tags={"env": "dev"})], [_view("v1", "vw", tags={"env": "prod"})]
        )
        tag_ops = _ops_of_type(ops, "unity.set_view_tag")
        assert len(tag_ops) == 1
        assert tag_ops[0].payload["tagValue"] == "prod"

    def test_view_tag_unchanged(self):
        ops = self._wrap(
            [_view("v1", "vw", tags={"env": "prod"})], [_view("v1", "vw", tags={"env": "prod"})]
        )
        tag_ops = [o for o in ops if "view_tag" in o.op]
        assert len(tag_ops) == 0
