"""Live Databricks parity checks for parser-driven SQL-file import workflows."""

from __future__ import annotations

import json
import os
import random
import string
from pathlib import Path

import pytest
from click.testing import CliRunner

from schemax.cli import cli
from schemax.core.storage import ensure_project_file
from schemax.providers.base.executor import ExecutionConfig
from schemax.providers.unity.auth import create_databricks_client
from schemax.providers.unity.executor import UnitySQLExecutor


def _invoke_cli(*args: str):
    return CliRunner().invoke(cli, list(args), catch_exceptions=True)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


def _random_suffix(length: int = 8) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))  # noqa: S311


def _build_execution_config(profile: str, warehouse_id: str) -> ExecutionConfig:
    return ExecutionConfig(
        target_env="dev",
        profile=profile,
        warehouse_id=warehouse_id,
        dry_run=False,
        no_interaction=True,
        timeout_seconds=300,
    )


def _create_executor(profile: str) -> UnitySQLExecutor:
    return UnitySQLExecutor(create_databricks_client(profile=profile))


def _cleanup_catalog(
    executor: UnitySQLExecutor, profile: str, warehouse_id: str, catalog: str
) -> None:
    executor.execute_statements(
        statements=[f"DROP CATALOG IF EXISTS {catalog} CASCADE"],
        config=_build_execution_config(profile, warehouse_id),
    )


def _run_live_parity_flow(
    *,
    temp_workspace: Path,
    parser_workspace: Path,
    sql_file: Path,
    catalog: str,
    profile: str,
    warehouse_id: str,
) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    live_result = _invoke_cli(
        "import",
        "--json",
        "--target",
        "dev",
        "--profile",
        profile,
        "--warehouse-id",
        warehouse_id,
        "--catalog",
        catalog,
        "--catalog-map",
        f"{catalog}={catalog}",
        "--dry-run",
        str(temp_workspace),
    )
    assert live_result.exit_code == 0, live_result.output
    live_envelope = json.loads(live_result.output)
    live_summary = live_envelope["data"]["summary"]
    assert live_summary["operations_generated"] > 0
    assert live_summary["object_counts"]["catalogs"] >= 1
    assert live_summary["object_counts"]["schemas"] >= 1
    assert live_summary["object_counts"]["tables"] >= 1
    assert live_summary["object_counts"]["views"] >= 1

    ensure_project_file(parser_workspace, provider_id="unity")
    parser_result = _invoke_cli(
        "import",
        "--json",
        "--from-sql",
        str(sql_file),
        "--mode",
        "replace",
        str(parser_workspace),
    )
    assert parser_result.exit_code == 0, parser_result.output
    parser_envelope = json.loads(parser_result.output)
    parser_summary = parser_envelope["data"]["summary"]
    assert parser_summary["operations_generated"] > 0
    assert parser_summary["object_counts"]["catalogs"] >= 1
    assert parser_summary["object_counts"]["schemas"] >= 1
    assert parser_summary["object_counts"]["tables"] >= 1
    assert parser_summary["object_counts"]["views"] >= 1

    validate_result = _invoke_cli("validate", "--json", str(parser_workspace))
    assert validate_result.exit_code in {0, 1}
    validate_envelope = json.loads(validate_result.output)
    assert validate_envelope["command"] == "validate"

    sql_result = _invoke_cli("sql", "--json", str(parser_workspace))
    assert sql_result.exit_code == 0, sql_result.output
    sql_envelope = json.loads(sql_result.output)
    assert sql_envelope["command"] == "sql"
    assert sql_envelope["status"] == "success"


@pytest.mark.integration
def test_live_sql_file_import_parity_with_provider_discovery(
    temp_workspace: Path, tmp_path: Path
) -> None:
    """Seed live UC objects, compare live-provider import with SQL-file parser import, and verify follow-up commands."""
    if os.getenv("SCHEMAX_RUN_LIVE_IMPORT_TESTS") != "1":
        pytest.skip("Set SCHEMAX_RUN_LIVE_IMPORT_TESTS=1 to run live parser parity test")

    profile = _require_env("DATABRICKS_PROFILE")
    warehouse_id = _require_env("DATABRICKS_WAREHOUSE_ID")
    catalog = f"schemax_live_parser_{_random_suffix()}"
    schema_name = "core"
    table_name = "orders"
    view_name = "orders_view"
    executor = _create_executor(profile)

    seed_statements = [
        f"CREATE CATALOG IF NOT EXISTS {catalog}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}",
        (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{schema_name}.{table_name} "
            "(id BIGINT, amount DECIMAL(18,2)) USING DELTA"
        ),
        (
            f"CREATE VIEW IF NOT EXISTS {catalog}.{schema_name}.{view_name} "
            f"AS SELECT id, amount FROM {catalog}.{schema_name}.{table_name}"
        ),
    ]

    sql_file = tmp_path / "live_parser_fixture.sql"
    sql_file.write_text(";\n".join(seed_statements) + ";\n", encoding="utf-8")

    try:
        seed_result = executor.execute_statements(
            statements=seed_statements,
            config=_build_execution_config(profile, warehouse_id),
        )
        assert seed_result.status == "success", seed_result.error_message
        parser_workspace = tmp_path / "parser_workspace"
        parser_workspace.mkdir()
        _run_live_parity_flow(
            temp_workspace=temp_workspace,
            parser_workspace=parser_workspace,
            sql_file=sql_file,
            catalog=catalog,
            profile=profile,
            warehouse_id=warehouse_id,
        )
    finally:
        _cleanup_catalog(executor, profile, warehouse_id, catalog)
