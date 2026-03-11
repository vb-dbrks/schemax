"""Integration tests for SQL generator location resolution.

Covers:
- _resolve_table_location (lines 211-256)
- _resolve_managed_location (lines 258-305)
- All error paths: missing config, not found, no env, env not configured
"""

import pytest

from schemax.providers.unity.models import UnityState
from schemax.providers.unity.sql_generator import UnitySQLGenerator
from schemax.providers.unity.state_reducer import apply_operations
from tests.utils import OperationBuilder


def _empty():
    return UnityState(catalogs=[])


# ── External Location Resolution ─────────────────────────────────────


@pytest.mark.integration
class TestExternalLocationResolution:
    def _external_table_ops(self, builder, ext_loc_name, path=None):
        options = {"external": True, "external_location_name": ext_loc_name}
        if path:
            options["path"] = path
        return [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "events", "s1", "delta", options=options, op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ]

    def test_resolve_with_path(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "landing", "events/v1")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            external_locations={
                "landing": {"paths": {"dev": "abfss://landing@storage.dfs.core.windows.net/data"}}
            },
            environment_name="dev",
        )
        # Use generate_sql_for_operation for the table op to exercise location resolution
        result = gen.generate_sql_for_operation(ops[2])  # add_table op
        assert "LOCATION" in result.sql or "Resolved Location" in result.sql
        assert "abfss://" in result.sql

    def test_resolve_without_path(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "landing")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            external_locations={
                "landing": {"paths": {"dev": "abfss://landing@storage.dfs.core.windows.net/data/"}}
            },
            environment_name="dev",
        )
        result = gen.generate_sql_for_operation(ops[2])  # add_table op
        assert "LOCATION" in result.sql or "Resolved Location" in result.sql

    def test_missing_config_returns_error(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "missing")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(state)
        result = gen.generate_sql_for_operation(ops[2])
        assert "externalLocations" in result.sql

    def test_location_not_found_returns_error(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "missing")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            external_locations={"other": {"paths": {"dev": "abfss://x"}}},
            environment_name="dev",
        )
        result = gen.generate_sql_for_operation(ops[2])
        assert "not found" in result.sql

    def test_no_environment_name_returns_error(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "landing")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            external_locations={"landing": {"paths": {"dev": "abfss://x"}}},
        )
        result = gen.generate_sql_for_operation(ops[2])
        assert "environment name" in result.sql

    def test_env_not_configured_returns_error(self) -> None:
        builder = OperationBuilder()
        ops = self._external_table_ops(builder, "landing")
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            external_locations={"landing": {"paths": {"prod": "abfss://x"}}},
            environment_name="dev",
        )
        result = gen.generate_sql_for_operation(ops[2])
        assert "Configured environments" in result.sql


# ── Managed Location Resolution ──────────────────────────────────────


@pytest.mark.integration
class TestManagedLocationResolution:
    def test_catalog_with_managed_location(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "warehouse", managed_location_name="ml1", op_id="op_1"
            ),
        ]
        result_sql = _gen_sql_with_locations(
            ops,
            managed_locations={
                "ml1": {"paths": {"dev": "abfss://container@account.dfs.core.windows.net/ml1"}}
            },
            environment_name="dev",
        )
        assert "CREATE CATALOG" in result_sql
        assert "MANAGED LOCATION" in result_sql

    def test_schema_with_managed_location(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.schema.update_schema("s1", managed_location_name="ml1", op_id="op_3"),
        ]
        result_sql = _gen_sql_with_locations(
            ops,
            managed_locations={
                "ml1": {"paths": {"dev": "abfss://container@account.dfs.core.windows.net/raw"}}
            },
            environment_name="dev",
        )
        assert "CREATE SCHEMA" in result_sql
        assert "MANAGED LOCATION" in result_sql

    def test_missing_config_raises(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "analytics", managed_location_name="missing", op_id="op_1"
            ),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(state)
        with pytest.raises(ValueError, match="managedLocations"):
            gen.generate_sql(ops)

    def test_location_not_found_raises(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "analytics", managed_location_name="missing", op_id="op_1"
            ),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            managed_locations={"other": {"paths": {"dev": "abfss://x"}}},
            environment_name="dev",
        )
        with pytest.raises(ValueError, match="not found"):
            gen.generate_sql(ops)

    def test_no_environment_name_raises(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "analytics", managed_location_name="ml1", op_id="op_1"
            ),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            managed_locations={"ml1": {"paths": {"dev": "abfss://x"}}},
        )
        with pytest.raises(ValueError, match="environment name"):
            gen.generate_sql(ops)

    def test_env_not_configured_raises(self) -> None:
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog(
                "cat_1", "analytics", managed_location_name="ml1", op_id="op_1"
            ),
        ]
        state = apply_operations(_empty(), ops)
        gen = UnitySQLGenerator(
            state,
            managed_locations={"ml1": {"paths": {"prod": "abfss://x"}}},
            environment_name="dev",
        )
        with pytest.raises(ValueError, match="Configured environments"):
            gen.generate_sql(ops)


def _gen_sql_with_locations(ops, **gen_kwargs):
    state = apply_operations(_empty(), ops)
    gen = UnitySQLGenerator(state, **gen_kwargs)
    return gen.generate_sql(ops)
