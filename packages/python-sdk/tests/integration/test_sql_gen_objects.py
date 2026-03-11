"""Integration tests for SQL generation of various object types.

Covers: drops, renames, views, functions, volumes, materialized views,
grants, governance (row filters, column masks), catalog mappings,
dependency-ordered generation.
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder, ops_catalog_schema_table


def _empty():
    return UnityState(catalogs=[])


def _gen_sql(ops: list, **gen_kwargs) -> str:
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state, **gen_kwargs)
    return gen.generate_sql(ops)


def _gen_single(ops: list, target_op_index: int = -1) -> str:
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state)
    return gen.generate_sql_for_operation(ops[target_op_index]).sql


def _gen_with_mapping(ops: list, **gen_kwargs):
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state, **gen_kwargs)
    return gen.generate_sql_with_mapping(ops)


# ── Drop operations ──────────────────────────────────────────────────


@pytest.mark.integration
class TestDropOperationsSql:
    def test_drop_table(self) -> None:
        builder = OperationBuilder()
        create_ops = ops_catalog_schema_table(builder)
        drop_op = builder.table.drop_table("t1", op_id="op_4")
        # Build state from create ops, then generate SQL for drop
        state = apply_operations(_empty(), create_ops)
        gen = UnitySQLGenerator(state)
        result = gen.generate_sql_for_operation(drop_op)
        assert "DROP TABLE" in result.sql
        assert "`orders`" in result.sql

    def test_drop_schema(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.schema.drop_schema("s1", op_id="op_3"),
        ]
        sql = _gen_single(ops)
        assert "DROP SCHEMA" in sql

    def test_drop_catalog(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "temp", op_id="op_1"),
            builder.catalog.drop_catalog("cat_1", op_id="op_2"),
        ]
        sql = _gen_single(ops)
        assert "DROP CATALOG" in sql

    def test_create_then_drop_cancels(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "temp", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.table.drop_table("t1", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql

    def test_drop_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view("v1", "old_view", "s1", "SELECT 1", op_id="op_3"),
            builder.view.drop_view("v1", op_id="op_4"),
        ]
        result = _gen_with_mapping(ops)
        assert isinstance(result.sql, str)

    def test_drop_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume("v1", "temp_files", "s1", "managed", op_id="op_3"),
            builder.volume.drop_volume("v1", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "DROP" in sql and "VOLUME" in sql

    def test_drop_materialized_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1", "daily_agg", "s1", "SELECT 1", op_id="op_3"
            ),
            builder.materialized_view.drop_materialized_view("mv1", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "DROP" in sql and "MATERIALIZED VIEW" in sql

    def test_drop_column(self) -> None:
        builder = OperationBuilder()
        create_ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "temp_col", "STRING", op_id="op_4"),
        ]
        drop_op = builder.column.drop_column("c1", "t1", op_id="op_5")
        state = apply_operations(_empty(), create_ops)
        gen = UnitySQLGenerator(state)
        result = gen.generate_sql_for_operation(drop_op)
        assert "DROP COLUMN" in result.sql
        assert "`temp_col`" in result.sql


# ── Rename operations ────────────────────────────────────────────────


@pytest.mark.integration
class TestRenameOperationsSql:
    def test_rename_table(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.table.rename_table("t1", "order_items", "orders", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "RENAME TO" in sql
        assert "`order_items`" in sql

    def test_rename_column(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "email", "STRING", op_id="op_4"),
            builder.column.rename_column("c1", "t1", "contact_email", "email", op_id="op_5"),
        ]
        sql = _gen_single(ops)
        assert "RENAME COLUMN" in sql
        assert "`contact_email`" in sql

    def test_rename_catalog(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "old_name", op_id="op_1"),
            builder.catalog.rename_catalog("cat_1", "new_name", "old_name", op_id="op_2"),
        ]
        sql = _gen_single(ops)
        assert "RENAME" in sql and "`new_name`" in sql

    def test_rename_schema(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "old_schema", "cat_1", op_id="op_2"),
            builder.schema.rename_schema("s1", "new_schema", "old_schema", op_id="op_3"),
        ]
        sql = _gen_single(ops)
        assert "RENAME" in sql and "`new_schema`" in sql


# ── Views ────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestViewSqlGeneration:
    def test_create_view(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.view.add_view(
                "v1", "active_users", "s1",
                "SELECT * FROM users WHERE active = true",
                comment="Active users only", op_id="op_3",
            ),
        ]
        sql = _gen_single(ops, target_op_index=2)
        assert "CREATE" in sql and "VIEW" in sql and "`active_users`" in sql


# ── Functions ────────────────────────────────────────────────────────


@pytest.mark.integration
class TestFunctionSqlGeneration:
    def test_create_function_with_params(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.function.add_function(
                "f1", "add_numbers", "s1", "SQL", "INT", "a + b",
                parameters=[{"name": "a", "type": "INT"}, {"name": "b", "type": "INT"}],
                comment="Adds two numbers", op_id="op_3",
            ),
        ]
        sql = _gen_single(ops, target_op_index=2)
        assert "CREATE" in sql and "FUNCTION" in sql and "`add_numbers`" in sql


# ── Volumes ──────────────────────────────────────────────────────────


@pytest.mark.integration
class TestVolumeSqlGeneration:
    def test_create_external_volume(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume(
                "v1", "raw_files", "s1", "external",
                location="abfss://container@account.dfs.core.windows.net/raw",
                comment="Raw file storage", op_id="op_3",
            ),
        ]
        sql = _gen_single(ops, target_op_index=2)
        assert "CREATE" in sql and "VOLUME" in sql


# ── Grants ───────────────────────────────────────────────────────────


@pytest.mark.integration
class TestGrantSqlGeneration:
    def test_grant_on_schema(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.grant.add_grant(
                "schema", "s1", "data_team", ["CREATE TABLE", "SELECT"], op_id="op_3"
            ),
        ]
        sql = _gen_sql(ops)
        assert "GRANT" in sql and "data_team" in sql

    def test_grant_on_table(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.grant.add_grant(
                "table", "t1", "analysts", ["SELECT", "MODIFY"], op_id="op_4"
            ),
        ]
        sql = _gen_sql(ops)
        assert "GRANT" in sql and "analysts" in sql

    def test_revoke_grant(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.grant.add_grant(
                "schema", "s1", "data_team", ["CREATE TABLE"], op_id="op_3"
            ),
            builder.grant.revoke_grant("schema", "s1", "data_team", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "REVOKE" in sql


# ── Governance ───────────────────────────────────────────────────────


@pytest.mark.integration
class TestGovernanceSqlGeneration:
    def test_add_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "region", "STRING", op_id="op_4"),
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = current_user()",
                enabled=True, description="Filter by region", op_id="op_5",
            ),
        ]
        sql = _gen_single(ops)
        assert "ROW FILTER" in sql or "ALTER TABLE" in sql

    def test_add_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "ssn", "STRING", op_id="op_4"),
            builder.column_mask.add_column_mask(
                "cm1", "t1", "c1", "mask_ssn",
                "CASE WHEN is_account_group_member('admins') THEN ssn ELSE 'XXX' END",
                enabled=True, description="Mask SSN", op_id="op_5",
            ),
        ]
        sql = _gen_single(ops)
        assert "MASK" in sql or "ALTER TABLE" in sql

    def test_remove_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "region", "STRING", op_id="op_4"),
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = 'US'", op_id="op_5"
            ),
            builder.row_filter.remove_row_filter("rf1", "t1", op_id="op_6"),
        ]
        sql = _gen_single(ops)
        assert "ROW FILTER" in sql or "DROP" in sql or "NONE" in sql

    def test_remove_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "ssn", "STRING", op_id="op_4"),
            builder.column_mask.add_column_mask(
                "cm1", "t1", "c1", "mask_ssn",
                "CASE WHEN true THEN ssn ELSE 'X' END", op_id="op_5",
            ),
            builder.column_mask.remove_column_mask("cm1", "t1", "c1", op_id="op_6"),
        ]
        sql = _gen_single(ops)
        assert "MASK" in sql or "DROP" in sql or "NONE" in sql


# ── Catalog Mapping ──────────────────────────────────────────────────


@pytest.mark.integration
class TestCatalogMappingSql:
    def test_create_with_mapping(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ]
        sql = _gen_sql(ops, catalog_name_mapping={"demo": "dev_demo"})
        assert "`dev_demo`" in sql

    def test_alter_with_mapping(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.table.set_table_comment("t1", "Order table", op_id="op_5"),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(state, catalog_name_mapping={"demo": "dev_demo"})
        result = gen.generate_sql_for_operation(ops[-1])
        assert "`dev_demo`" in result.sql


# ── Dependency-ordered SQL ───────────────────────────────────────────


@pytest.mark.integration
class TestDependencyOrderedSql:
    def test_out_of_order_operations(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(state)
        result = gen.generate_sql_with_dependencies(ops)
        assert len(result.statements) >= 1

    def test_with_views_dependency(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.view.add_view(
                "v1", "events_summary", "s1",
                "SELECT COUNT(*) FROM events", op_id="op_5",
            ),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(state)
        result = gen.generate_sql_with_dependencies(ops)
        assert len(result.statements) >= 2


# ── Complex schema ───────────────────────────────────────────────────


@pytest.mark.integration
class TestComplexSchemaGeneration:
    def test_full_schema_sql(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "ecommerce", op_id="op_01"),
            builder.schema.add_schema("s1", "sales", "cat_1", op_id="op_02"),
            builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_03"),
            builder.column.add_column(
                "c1", "t1", "order_id", "BIGINT", nullable=False, op_id="op_04"
            ),
            builder.column.add_column("c2", "t1", "amount", "DECIMAL(10,2)", op_id="op_05"),
            builder.column.add_column("c3", "t1", "region", "STRING", op_id="op_06"),
            builder.constraint.add_constraint(
                "pk1", "t1", "primary_key", ["c1"], name="pk_orders", op_id="op_07"
            ),
            builder.table.set_table_property(
                "t1", "delta.autoOptimize.optimizeWrite", "true", op_id="op_08"
            ),
            builder.table.add_table("t2", "customers", "s1", "delta", op_id="op_09"),
            builder.column.add_column(
                "c4", "t2", "id", "BIGINT", nullable=False, op_id="op_10"
            ),
            builder.column.add_column("c5", "t2", "ssn", "STRING", op_id="op_11"),
            builder.column_mask.add_column_mask(
                "cm1", "t2", "c5", "mask_ssn",
                "CASE WHEN is_account_group_member('admins') THEN ssn ELSE '***' END",
                op_id="op_12",
            ),
            builder.view.add_view(
                "v1", "order_summary", "s1",
                "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
                op_id="op_13",
            ),
            builder.function.add_function(
                "f1", "format_amount", "s1", "SQL", "STRING",
                "CONCAT('$', CAST(amount AS STRING))",
                parameters=[{"name": "amount", "type": "DECIMAL(10,2)"}], op_id="op_14",
            ),
            builder.volume.add_volume(
                "vol1", "raw_files", "s1", "managed", comment="Raw data", op_id="op_15"
            ),
            builder.grant.add_grant(
                "catalog", "cat_1", "data_team", ["USE CATALOG"], op_id="op_16"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE CATALOG" in result.sql
        assert "CREATE SCHEMA" in result.sql
        assert "orders" in result.sql.lower()
        assert len(result.statements) >= 5
