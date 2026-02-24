"""
Integration tests for bulk table tag applied to tables and views.

Bulk "Add table tag" applies the same tag to all tables and views in scope.
set_table_tag / unset_table_tag work for both table ids and view ids (state + SQL).
"""

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder


def test_bulk_table_tag_applies_to_table_and_view():
    """Bulk table tag: set_table_tag for table and view updates state and generates correct SQL."""
    builder = OperationBuilder()
    setup_ops = [
        builder.catalog.add_catalog("cat_1", "c1", op_id="op_1"),
        builder.schema.add_schema("sch_1", "s1", "cat_1", op_id="op_2"),
        builder.table.add_table("tbl_1", "t1", "sch_1", "delta", op_id="op_3"),
        builder.view.add_view("view_1", "v1", "sch_1", "SELECT 1", comment=None, op_id="op_4"),
    ]
    state = apply_operations(UnityState(catalogs=[]), setup_ops)

    tag_ops = [
        builder.table.set_table_tag("tbl_1", "env", "dev", op_id="op_5"),
        builder.table.set_table_tag("view_1", "env", "dev", op_id="op_6"),
    ]
    state_tagged = apply_operations(state, tag_ops)

    table = state_tagged.catalogs[0].schemas[0].tables[0]
    view = state_tagged.catalogs[0].schemas[0].views[0]
    assert table.tags.get("env") == "dev"
    assert view.tags.get("env") == "dev"

    generator = UnitySQLGenerator(state.model_dump(by_alias=True))
    sql = generator.generate_sql(tag_ops)
    assert "ALTER TABLE" in sql
    assert "SET TAGS" in sql
    assert "`t1`" in sql or "t1" in sql
    assert "`v1`" in sql or "v1" in sql
    assert sql.count("SET TAGS") >= 2 or (sql.count("ALTER TABLE") >= 2)
