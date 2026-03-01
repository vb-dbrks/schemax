"""Smoke tests for Hive provider MVP."""

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
