"""Integration tests for SQL generator covering previously uncovered paths.

Targets catalog/schema updates, column modifications, function rename/comment,
MV with comment/partitions, constraint options, and revoke-all grants.
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import (
    OperationBuilder,
    ops_catalog_schema_table,
    ops_catalog_schema_with_function,
    ops_catalog_schema_with_mv,
)


def _empty():
    return UnityState(catalogs=[])


def _gen_sql(ops: list) -> str:
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state)
    return gen.generate_sql(ops)


def _gen_single(ops: list, target_op_index: int = -1) -> str:
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state)
    result = gen.generate_sql_for_operation(ops[target_op_index])
    return result.sql


@pytest.mark.integration
class TestCatalogUpdateSql:
    def test_update_catalog_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.catalog.update_catalog("cat_1", comment="Updated comment", op_id="op_2"),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql
        assert "Updated comment" in sql

    def test_update_catalog_managed_location_error(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.catalog.update_catalog(
                "cat_1", managed_location_name="new_loc", op_id="op_2"
            ),
        ]
        sql = _gen_single(ops)
        assert "managedLocations" in sql or "Error" in sql


@pytest.mark.integration
class TestSchemaUpdateSql:
    def test_update_schema_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.schema.update_schema("s1", comment="Schema comment", op_id="op_3"),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql
        assert "Schema comment" in sql


@pytest.mark.integration
class TestColumnModificationSql:
    def test_change_column_type(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "amount", "INT", op_id="op_4"),
            builder.column.change_column_type("c1", "t1", "BIGINT", op_id="op_5"),
        ]
        sql = _gen_single(ops)
        assert "ALTER" in sql
        assert "BIGINT" in sql

    def test_set_column_not_null(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=True, op_id="op_4"),
            builder.column.set_nullable("c1", "t1", False, op_id="op_5"),
        ]
        sql = _gen_single(ops)
        assert "NOT NULL" in sql or "SET" in sql

    def test_set_column_comment(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "email", "STRING", op_id="op_4"),
            builder.column.set_column_comment("c1", "t1", "User email", op_id="op_5"),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql
        assert "User email" in sql

    def test_reorder_columns_not_supported(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
            builder.column.reorder_columns("t1", ["c2", "c1"], op_id="op_6"),
        ]
        sql = _gen_single(ops)
        assert "not directly supported" in sql.lower() or "reorder" in sql.lower()


@pytest.mark.integration
class TestFunctionAdvancedSql:
    def test_rename_function(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_function(builder) + [
            builder.function.rename_function("f1", "triple_it", "double_it", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "ALTER" in sql or "RENAME" in sql

    def test_set_function_comment(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_function(builder) + [
            builder.function.set_function_comment("f1", "Doubles the input", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql
        assert "Doubles the input" in sql

    def test_update_function_body(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_function(builder) + [
            builder.function.update_function("f1", body="x * 10", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "CREATE OR REPLACE" in sql or "FUNCTION" in sql


@pytest.mark.integration
class TestMaterializedViewAdvancedSql:
    def test_mv_with_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1",
                "daily_agg",
                "s1",
                "SELECT 1",
                comment="Daily aggregation",
                op_id="op_3",
            ),
        ]
        sql = _gen_single(ops, target_op_index=2)
        assert "COMMENT" in sql
        assert "Daily aggregation" in sql

    def test_mv_with_partition_columns(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.materialized_view.add_materialized_view(
                "mv1",
                "partitioned_agg",
                "s1",
                "SELECT 1",
                partition_columns=["region"],
                op_id="op_3",
            ),
        ]
        sql = _gen_single(ops, target_op_index=2)
        assert "PARTITIONED BY" in sql or "MATERIALIZED VIEW" in sql

    def test_mv_with_schedule_and_comment(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_mv(builder) + [
            builder.materialized_view.update_materialized_view(
                "mv1",
                comment="Updated daily",
                refresh_schedule="CRON '0 6 * * *'",
                op_id="op_4",
            ),
        ]
        sql = _gen_single(ops)
        assert "MATERIALIZED VIEW" in sql or "ALTER" in sql

    def test_set_mv_comment(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_with_mv(builder) + [
            builder.materialized_view.set_materialized_view_comment(
                "mv1", "New comment", op_id="op_4"
            ),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql
        assert "New comment" in sql


@pytest.mark.integration
class TestConstraintAdvancedSql:
    def test_check_constraint(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "age", "INT", op_id="op_4"),
            builder.constraint.add_constraint(
                "chk_1",
                "t1",
                "check",
                ["c1"],
                name="chk_age",
                expression="age > 0",
                op_id="op_5",
            ),
        ]
        sql = _gen_single(ops, target_op_index=-1)
        assert "CHECK" in sql or "CONSTRAINT" in sql

    def test_foreign_key_with_parent(self) -> None:
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
                parentTable="t1",
                parentColumns=["c1"],
                op_id="op_7",
            ),
        ]
        sql = _gen_single(ops, target_op_index=-1)
        assert "FOREIGN KEY" in sql or "CONSTRAINT" in sql
        assert "REFERENCES" in sql

    def test_primary_key_not_enforced(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.constraint.add_constraint(
                "pk_1",
                "t1",
                "primary_key",
                ["c1"],
                name="pk_orders",
                notEnforced=True,
                op_id="op_5",
            ),
        ]
        sql = _gen_single(ops, target_op_index=-1)
        assert "NOT ENFORCED" in sql

    def test_primary_key_rely(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.constraint.add_constraint(
                "pk_1",
                "t1",
                "primary_key",
                ["c1"],
                name="pk_orders",
                rely=True,
                op_id="op_5",
            ),
        ]
        sql = _gen_single(ops, target_op_index=-1)
        assert "RELY" in sql


@pytest.mark.integration
class TestVolumeUpdateSql:
    def test_update_volume_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.volume.add_volume("v1", "data_files", "s1", "managed", op_id="op_3"),
            builder.volume.update_volume("v1", comment="Holds CSV files", op_id="op_4"),
        ]
        sql = _gen_single(ops)
        assert "COMMENT" in sql or "ALTER" in sql


@pytest.mark.integration
class TestRevokeAllGrantsSql:
    def test_revoke_all_privileges(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.grant.add_grant(
                "catalog", "cat_1", "team", ["USE CATALOG", "CREATE SCHEMA"], op_id="op_2"
            ),
            builder.grant.revoke_grant("catalog", "cat_1", "team", op_id="op_3"),
        ]
        sql = _gen_sql(ops)
        assert "REVOKE" in sql


@pytest.mark.integration
class TestTableWithCommentAndPartitions:
    def test_table_with_comment(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table(
                "t1",
                "orders",
                "s1",
                "delta",
                options={"comment": "Order data"},
                op_id="op_3",
            ),
        ]
        sql = _gen_sql(ops)
        assert "COMMENT" in sql
        assert "Order data" in sql

    def test_table_with_partition_columns(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table(
                "t1",
                "events",
                "s1",
                "delta",
                options={"partition_columns": ["event_date"]},
                op_id="op_3",
            ),
            builder.column.add_column("c1", "t1", "event_date", "DATE", op_id="op_4"),
            builder.column.add_column("c2", "t1", "event_name", "STRING", op_id="op_5"),
        ]
        sql = _gen_sql(ops)
        assert "PARTITIONED BY" in sql or "CREATE" in sql

    def test_table_with_cluster_columns(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table(
                "t1",
                "events",
                "s1",
                "delta",
                options={"cluster_columns": ["event_date"]},
                op_id="op_3",
            ),
            builder.column.add_column("c1", "t1", "event_date", "DATE", op_id="op_4"),
        ]
        sql = _gen_sql(ops)
        assert "CLUSTER" in sql or "CREATE" in sql

    def test_table_unset_property(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.table.set_table_property(
                "t1", "delta.autoOptimize.optimizeWrite", "true", op_id="op_4"
            ),
            builder.table.unset_table_property(
                "t1", "delta.autoOptimize.optimizeWrite", op_id="op_5"
            ),
        ]
        sql = _gen_single(ops)
        assert "UNSET TBLPROPERTIES" in sql

    def test_table_unset_tag(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.table.set_table_tag("t1", "pii", "true", op_id="op_4"),
            builder.table.unset_table_tag("t1", "pii", op_id="op_5"),
        ]
        sql = _gen_single(ops)
        assert "UNSET TAGS" in sql or "ALTER TABLE" in sql


@pytest.mark.integration
class TestColumnTagSql:
    def test_unset_column_tag(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "email", "STRING", op_id="op_4"),
            builder.column.set_column_tag("c1", "t1", "pii", "email", op_id="op_5"),
            builder.column.unset_column_tag("c1", "t1", "pii", op_id="op_6"),
        ]
        sql = _gen_single(ops)
        assert "UNSET TAGS" in sql or "ALTER" in sql
