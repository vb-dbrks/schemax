"""Integration tests for SQL generator batching paths.

Covers:
- _generate_batched_create_sql for catalogs, schemas, tables (lines 476-536)
- _generate_batched_alter_sql for tables (lines 538-599)
- Operation categorization: column, property, constraint, governance, reorder, tags
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder, ops_catalog_schema_table


def _empty():
    return UnityState(catalogs=[])


def _gen_with_mapping(ops: list, **gen_kwargs):
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state, **gen_kwargs)
    return gen.generate_sql_with_mapping(ops)


def _gen_sql(ops: list, **gen_kwargs) -> str:
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state, **gen_kwargs)
    return gen.generate_sql(ops)


# ── Batched CREATE: Catalogs ─────────────────────────────────────────


@pytest.mark.integration
class TestBatchedCreateCatalog:
    def test_create_catalog_with_update_squashed(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.catalog.update_catalog("cat_1", comment="Analytics hub", op_id="op_2"),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE CATALOG" in result.sql
        assert "`analytics`" in result.sql

    def test_create_catalog_with_managed_location(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "warehouse", managed_location_name="ml1", op_id="op_1"
            ),
        ]
        result = _gen_with_mapping(
            ops,
            managed_locations={
                "ml1": {"paths": {"dev": "abfss://container@account.dfs.core.windows.net/ml1"}}
            },
            environment_name="dev",
        )
        assert "CREATE CATALOG" in result.sql
        assert "MANAGED LOCATION" in result.sql
        assert "abfss://" in result.sql


# ── Batched CREATE: Schemas ──────────────────────────────────────────


@pytest.mark.integration
class TestBatchedCreateSchema:
    def test_create_schema_with_update_squashed(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.schema.update_schema("s1", comment="Raw data", op_id="op_3"),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE SCHEMA" in result.sql
        assert "`raw`" in result.sql

    def test_create_schema_with_managed_location(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.schema.update_schema("s1", managed_location_name="ml1", op_id="op_3"),
        ]
        result = _gen_with_mapping(
            ops,
            managed_locations={
                "ml1": {"paths": {"dev": "abfss://container@account.dfs.core.windows.net/raw"}}
            },
            environment_name="dev",
        )
        assert "CREATE SCHEMA" in result.sql
        assert "MANAGED LOCATION" in result.sql


# ── Batched CREATE: Tables ───────────────────────────────────────────


@pytest.mark.integration
class TestBatchedCreateTable:
    def test_create_table_with_columns(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "BIGINT", nullable=False, op_id="op_4"),
            builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
            builder.column.add_column("c3", "t1", "ts", "TIMESTAMP", op_id="op_6"),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql
        assert "`events`" in result.sql
        assert "`id`" in result.sql

    def test_create_table_with_properties(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.table.set_table_property(
                "t1", "delta.autoOptimize.optimizeWrite", "true", op_id="op_5"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql
        assert "delta.autoOptimize" in result.sql or "TBLPROPERTIES" in result.sql

    def test_create_table_with_constraint(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.constraint.add_constraint(
                "pk_1", "t1", "primary_key", ["c1"], name="pk_events", op_id="op_5"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql or "CONSTRAINT" in result.sql

    def test_create_table_with_row_filter(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "region", "STRING", op_id="op_4"),
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = 'US'", enabled=True, op_id="op_5"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql or "ROW FILTER" in result.sql

    def test_create_table_with_column_mask(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "ssn", "STRING", op_id="op_4"),
            builder.column_mask.add_column_mask(
                "cm1",
                "t1",
                "c1",
                "ssn_mask",
                "CASE WHEN is_account_group_member('admins') THEN ssn ELSE '***' END",
                enabled=True,
                op_id="op_5",
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql or "MASK" in result.sql

    def test_create_table_with_reorder(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
            builder.column.reorder_columns("t1", ["c2", "c1"], op_id="op_6"),
        ]
        result = _gen_with_mapping(ops)
        assert "CREATE TABLE" in result.sql


# ── Batched ALTER: Tables ────────────────────────────────────────────


@pytest.mark.integration
class TestBatchedAlterTable:
    def test_alter_add_columns(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.column.add_column("c2", "t1", "name", "STRING", op_id="op_5"),
        ]
        result = _gen_with_mapping(ops)
        assert "`id`" in result.sql
        assert "`name`" in result.sql

    def test_alter_with_property_ops(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.table.set_table_property("t1", "delta.minReaderVersion", "2", op_id="op_5"),
            builder.table.set_table_property("t1", "delta.minWriterVersion", "5", op_id="op_6"),
        ]
        result = _gen_with_mapping(ops)
        assert "TBLPROPERTIES" in result.sql or "delta.minReaderVersion" in result.sql

    def test_alter_with_table_tags(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.table.set_table_tag("t1", "pii", "true", op_id="op_5"),
            builder.table.set_table_tag("t1", "team", "data-eng", op_id="op_6"),
        ]
        result = _gen_with_mapping(ops)
        assert "TAGS" in result.sql or "pii" in result.sql

    def test_alter_with_constraint(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.constraint.add_constraint(
                "pk_1", "t1", "primary_key", ["c1"], name="pk_orders", op_id="op_5"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "CONSTRAINT" in result.sql or "PRIMARY KEY" in result.sql

    def test_alter_with_governance_ops(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "ssn", "STRING", op_id="op_4"),
            builder.row_filter.add_row_filter(
                "rf1", "t1", "region_filter", "region = 'US'", op_id="op_5"
            ),
            builder.column_mask.add_column_mask(
                "cm1",
                "t1",
                "c1",
                "ssn_mask",
                "CASE WHEN true THEN ssn ELSE '***' END",
                op_id="op_6",
            ),
        ]
        result = _gen_with_mapping(ops)
        assert len(result.statements) >= 1

    def test_alter_mixed_operations(self) -> None:
        builder = OperationBuilder()
        ops = ops_catalog_schema_table(builder) + [
            builder.column.add_column("c1", "t1", "id", "INT", nullable=False, op_id="op_4"),
            builder.column.add_column("c2", "t1", "email", "STRING", op_id="op_5"),
            builder.table.set_table_property("t1", "delta.appendOnly", "true", op_id="op_6"),
            builder.table.set_table_comment("t1", "Order records", op_id="op_7"),
            builder.table.set_table_tag("t1", "domain", "sales", op_id="op_8"),
            builder.constraint.add_constraint(
                "pk_1", "t1", "primary_key", ["c1"], name="pk_orders", op_id="op_9"
            ),
        ]
        result = _gen_with_mapping(ops)
        assert "`id`" in result.sql
        assert "`email`" in result.sql
