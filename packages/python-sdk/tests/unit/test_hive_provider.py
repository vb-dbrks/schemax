"""Smoke tests for Hive provider MVP."""

from schemax.providers.base.executor import ExecutionConfig
from schemax.providers.base.operations import create_operation
from schemax.providers.hive import hive_provider


def test_hive_provider_registered_capabilities() -> None:
    assert hive_provider.info.id == "hive"
    assert "hive.add_database" in hive_provider.capabilities.supported_operations


def test_hive_state_reducer_add_database() -> None:
    state = hive_provider.create_initial_state()
    operation = create_operation(
        provider="hive",
        op_type="add_database",
        target="db_analytics",
        payload={"name": "analytics"},
    )

    new_state = hive_provider.apply_operation(state, operation)

    assert new_state["databases"][0]["name"] == "analytics"


def test_hive_sql_generator_create_database() -> None:
    generator = hive_provider.get_sql_generator(hive_provider.create_initial_state())
    operation = create_operation(
        provider="hive",
        op_type="add_database",
        target="db_analytics",
        payload={"name": "analytics"},
    )

    sql = generator.generate_sql([operation])
    assert "CREATE DATABASE IF NOT EXISTS `analytics`;" in sql


def test_hive_validate_execution_config_returns_explicit_not_implemented_error() -> None:
    result = hive_provider.validate_execution_config(
        ExecutionConfig(
            target_env="dev",
            profile="DEFAULT",
            warehouse_id="wh_1",
            dry_run=True,
            no_interaction=True,
        )
    )
    assert result.valid is False
    assert result.errors
    assert result.errors[0].code == "HIVE_EXECUTION_NOT_IMPLEMENTED"


def test_hive_sql_generator_drop_table_uses_payload_database_and_name() -> None:
    generator = hive_provider.get_sql_generator(hive_provider.create_initial_state())
    operation = create_operation(
        provider="hive",
        op_type="drop_table",
        target="tbl_legacy_id",
        payload={"database": "analytics", "name": "events"},
    )

    sql = generator.generate_sql([operation])
    assert "DROP TABLE IF EXISTS `analytics`.`events`;" in sql


def test_hive_sql_generator_drop_table_warns_when_metadata_is_missing() -> None:
    generator = hive_provider.get_sql_generator(hive_provider.create_initial_state())
    operation = create_operation(
        provider="hive",
        op_type="drop_table",
        target="tbl_legacy_id",
        payload={},
    )

    result = generator.generate_sql_for_operation(operation)
    assert result.sql == ""
    assert "Missing table metadata for Hive drop_table operation" in result.warnings
