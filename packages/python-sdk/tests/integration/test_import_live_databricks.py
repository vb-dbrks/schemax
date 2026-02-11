"""
Live Databricks integration test for import workflow.

This test is opt-in and skipped by default. It seeds Unity Catalog objects via
SQL fixture, then validates `schematic import` against real provider state.
"""

import os
import random
import string
from pathlib import Path

import pytest

from schematic.commands.import_assets import import_from_provider
from schematic.core.storage import ensure_project_file
from schematic.providers.base.executor import ExecutionConfig
from schematic.providers.unity.auth import create_databricks_client
from schematic.providers.unity.executor import UnitySQLExecutor


def _require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        pytest.skip(f"{var_name} is not set")
    return value


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL script into statements while preserving quoted semicolons."""
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue

        for char in line:
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            if char == ";" and not in_single_quote and not in_double_quote:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
        current.append("\n")

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def _make_random(length: int = 8) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))  # noqa: S311


@pytest.mark.integration
def test_import_from_live_databricks_fixture_sql(temp_workspace):
    if os.getenv("SCHEMATIC_RUN_LIVE_IMPORT_TESTS") != "1":
        pytest.skip("Set SCHEMATIC_RUN_LIVE_IMPORT_TESTS=1 to run live Databricks import test")

    profile = _require_env("DATABRICKS_PROFILE")
    warehouse_id = _require_env("DATABRICKS_WAREHOUSE_ID")
    managed_location_root = _require_env("DATABRICKS_MANAGED_LOCATION")

    ensure_project_file(temp_workspace, provider_id="unity")

    suffix = _make_random(8)
    main_catalog = f"test_import_fixture_{suffix}"
    aux_catalog = f"test_import_aux_{suffix}"

    fixture_sql_path = (
        Path(__file__).resolve().parents[1] / "resources" / "sql" / "unity_import_fixture.sql"
    )
    sql_text = fixture_sql_path.read_text()
    sql_text = sql_text.replace("test_import_fixture", main_catalog)
    sql_text = sql_text.replace("test_import_aux", aux_catalog)
    managed_run_root = f"{managed_location_root.rstrip('/')}/schematic-import-live/{suffix}"
    sql_text = sql_text.replace("__MANAGED_ROOT__", managed_run_root)
    statements = _split_sql_statements(sql_text)

    client = create_databricks_client(profile=profile)
    executor = UnitySQLExecutor(client)
    config = ExecutionConfig(
        target_env="dev",
        profile=profile,
        warehouse_id=warehouse_id,
        dry_run=False,
        no_interaction=True,
        timeout_seconds=300,
    )

    cleanup_statements = [
        f"DROP CATALOG IF EXISTS {aux_catalog} CASCADE",
        f"DROP CATALOG IF EXISTS {main_catalog} CASCADE",
    ]

    try:
        seed_result = executor.execute_statements(statements=statements, config=config)
        assert seed_result.status == "success"

        summary = import_from_provider(
            workspace=temp_workspace,
            target_env="dev",
            profile=profile,
            warehouse_id=warehouse_id,
            catalog=main_catalog,
            dry_run=True,
            adopt_baseline=False,
        )

        assert summary["provider"] == "unity"
        assert summary["operations_generated"] > 0
        assert summary["object_counts"]["catalogs"] == 1
        assert summary["object_counts"]["schemas"] >= 3
        assert summary["object_counts"]["tables"] >= 4
        assert summary["object_counts"]["views"] >= 2
    finally:
        # Best-effort cleanup so repeated local runs stay predictable.
        executor.execute_statements(statements=cleanup_statements, config=config)
