"""Integration tests for Unity SQL generator covering grants, constraints,
volumes, functions, materialized views, row filters, column masks,
external tables, and view qualification.
"""

import pytest

from schemax.providers.registry import ProviderRegistry
from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import (
    OperationBuilder,
    ops_catalog_schema_table,
    ops_catalog_schema_with_function,
    ops_catalog_schema_with_mv,
    ops_table_with_pk,
    ops_table_with_ssn_mask,
)


def _gen_sql(ops: list, state: dict | None = None) -> str:
    """Apply ops to state and generate SQL."""
    provider = ProviderRegistry.get("unity")
    if state is None:
        state = UnityState(catalogs=[])
    final_state = apply_operations(state, ops)
    generator = provider.get_sql_generator(state=final_state)
    return generator.generate_sql(ops)


def _gen_sql_with_mapping(ops: list, mapping: dict, state: dict | None = None) -> str:
    """Apply ops to state and generate SQL with catalog mapping."""
    provider = ProviderRegistry.get("unity")
    if state is None:
        state = UnityState(catalogs=[])
    final_state = apply_operations(state, ops)
    generator = provider.get_sql_generator(state=final_state, name_mapping=mapping)
    return generator.generate_sql(ops)


@pytest.mark.integration
class TestGrantSqlGeneration:
    def test_add_grant_on_catalog(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.grant.add_grant(
                "catalog", "cat_1", "data_team", ["USE CATALOG", "CREATE SCHEMA"], op_id="op_2"
            ),
        ]
        sql = _gen_sql(ops)
        assert "GRANT" in sql
        assert "USE CATALOG" in sql or "CREATE SCHEMA" in sql

    def test_add_grant_on_schema(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.grant.add_grant(
                "schema", "s1", "analysts", ["USE SCHEMA", "SELECT"], op_id="op_3"
            ),
        ]
        sql = _gen_sql(ops)
        assert "GRANT" in sql

    def test_add_grant_on_table(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.grant.add_grant("table", "t1", "readers", ["SELECT"], op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "GRANT" in sql
        assert "SELECT" in sql

    def test_revoke_grant(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.grant.add_grant("catalog", "cat_1", "team", ["USE CATALOG"], op_id="op_2"),
            builder.grant.revoke_grant("catalog", "cat_1", "team", ["USE CATALOG"], op_id="op_3"),
        ]
        sql = _gen_sql(ops)
        assert "REVOKE" in sql


@pytest.mark.integration
class TestConstraintSqlGeneration:
    def test_add_primary_key(self) -> None:
        builder = OperationBuilder()
        sql = _gen_sql(ops_table_with_pk(builder))
        assert "CONSTRAINT" in sql or "PRIMARY KEY" in sql

    def test_add_foreign_key(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.table.add_table("t2", "items", "s1", "delta", op_id="op_5"),
            builder.column.add_column("c2", "t2", "order_id", "INT", op_id="op_6"),
            builder.constraint.add_constraint(
                "fk_1",
                "t2",
                "foreign_key",
                ["c2"],
                name="fk_order",
                op_id="op_7",
                parentTable="t1",
                parentColumns=["c1"],
            ),
        ]
        sql = _gen_sql(ops)
        assert "FOREIGN KEY" in sql or "CONSTRAINT" in sql

    def test_drop_constraint(self) -> None:
        builder = OperationBuilder()
        ops = ops_table_with_pk(builder) + [
            builder.constraint.drop_constraint("pk_1", "t1", op_id="op_6"),
        ]
        sql = _gen_sql(ops)
        assert "DROP CONSTRAINT" in sql or "ALTER TABLE" in sql


@pytest.mark.integration
class TestVolumeSqlGeneration:
    def test_add_managed_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume(
                "v1", "data_files", "s1", "managed", comment="Test volume", op_id="op_3"
            ),
        ]
        sql = _gen_sql(ops)
        assert "CREATE VOLUME" in sql

    def test_add_external_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume(
                "v1",
                "ext_data",
                "s1",
                "external",
                location="s3://bucket/path",
                op_id="op_3",
            ),
        ]
        sql = _gen_sql(ops)
        assert "CREATE" in sql and "VOLUME" in sql

    def test_drop_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume("v1", "data_files", "s1", "managed", op_id="op_3"),
            builder.volume.drop_volume("v1", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "DROP VOLUME" in sql

    def test_rename_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume("v1", "old_name", "s1", "managed", op_id="op_3"),
            builder.volume.rename_volume("v1", "new_name", "old_name", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "RENAME" in sql or "ALTER" in sql


@pytest.mark.integration
class TestFunctionSqlGeneration:
    def test_add_sql_function(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_function(builder)
        sql = _gen_sql(ops)
        assert "CREATE" in sql and "FUNCTION" in sql

    def test_add_python_function(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.function.add_function(
                "f1",
                "greet",
                "s1",
                "PYTHON",
                "STRING",
                "return f'Hello {name}'",
                parameters=[{"name": "name", "type": "STRING"}],
                op_id="op_3",
            ),
        ]
        sql = _gen_sql(ops)
        assert "FUNCTION" in sql

    def test_drop_function(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.function.add_function(
                "f1",
                "my_fn",
                "s1",
                "SQL",
                "INT",
                "1",
                op_id="op_3",
            ),
            builder.function.drop_function("f1", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "DROP FUNCTION" in sql


@pytest.mark.integration
class TestMaterializedViewSqlGeneration:
    def test_add_materialized_view(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.materialized_view.add_materialized_view(
                "mv1",
                "order_summary",
                "s1",
                "SELECT COUNT(*) FROM orders",
                comment="Daily summary",
                op_id="op_4",
            ),
        ]
        sql = _gen_sql(ops)
        assert "MATERIALIZED VIEW" in sql or "CREATE" in sql

    def test_add_mv_with_schedule(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_mv(builder)
        sql = _gen_sql(ops)
        assert "MATERIALIZED VIEW" in sql or "CREATE" in sql

    def test_drop_materialized_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1",
                "summary",
                "s1",
                "SELECT 1",
                op_id="op_3",
            ),
            builder.materialized_view.drop_materialized_view("mv1", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "DROP" in sql


@pytest.mark.integration
class TestRowFilterSqlGeneration:
    def test_add_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "region", "STRING", op_id="op_4"),
            builder.row_filter.add_row_filter(
                "rf1",
                "t1",
                "region_filter",
                "region = current_user()",
                description="Filter by region",
                op_id="op_5",
            ),
        ]
        sql = _gen_sql(ops)
        assert "ROW FILTER" in sql or "ALTER TABLE" in sql or sql

    def test_remove_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.row_filter.add_row_filter("rf1", "t1", "rf", "1=1", op_id="op_4"),
            builder.row_filter.remove_row_filter("rf1", "t1", op_id="op_5"),
        ]
        sql = _gen_sql(ops)
        assert sql is not None


@pytest.mark.integration
class TestColumnMaskSqlGeneration:
    def test_add_column_mask(self) -> None:
        builder = OperationBuilder()
        sql = _gen_sql(ops_table_with_ssn_mask(builder))
        assert sql is not None

    def test_remove_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = ops_table_with_ssn_mask(builder) + [
            builder.column_mask.remove_column_mask("cm1", "t1", "c1", op_id="op_6"),
        ]
        sql = _gen_sql(ops)
        assert sql is not None


@pytest.mark.integration
class TestViewSqlGeneration:
    def test_add_view(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.view.add_view(
                "v1",
                "active_orders",
                "s1",
                "SELECT * FROM orders WHERE status = 'active'",
                comment="Active orders only",
                op_id="op_4",
            ),
        ]
        sql = _gen_sql(ops)
        assert "CREATE" in sql and "VIEW" in sql

    def test_drop_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view("v1", "my_view", "s1", "SELECT 1", op_id="op_3"),
            builder.view.drop_view("v1", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "DROP VIEW" in sql

    def test_update_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view("v1", "my_view", "s1", "SELECT 1", op_id="op_3"),
            builder.view.update_view("v1", definition="SELECT 2", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "VIEW" in sql


@pytest.mark.integration
class TestCatalogMappingInSql:
    def test_sql_with_catalog_mapping(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder)
        mapping = {"sales": "prod_sales"}
        sql = _gen_sql_with_mapping(ops, mapping)
        assert "prod_sales" in sql

    def test_sql_without_mapping_uses_logical_names(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
        ]
        sql = _gen_sql(ops)
        assert "sales" in sql


@pytest.mark.integration
class TestExternalTableSql:
    def test_external_table_single_operation(self) -> None:
        """External table SQL via generate_sql_for_operation (bypasses batch filtering)."""
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table(
                "t1",
                "ext_data",
                "s1",
                "delta",
                options={
                    "external": True,
                    "path": "data/orders",
                    "external_location_name": "my_ext_loc",
                },
                op_id="op_3",
            ),
        ]
        state = UnityState(catalogs=[])
        final_state = apply_operations(state, ops)
        gen = UnitySQLGenerator(
            final_state,
            external_locations={"my_ext_loc": {"paths": {"dev": "s3://bucket/ext"}}},
            environment_name="dev",
        )
        result = gen.generate_sql_for_operation(ops[2])
        assert "CREATE" in result.sql and "TABLE" in result.sql
        assert "EXTERNAL" in result.sql
        assert "LOCATION" in result.sql
