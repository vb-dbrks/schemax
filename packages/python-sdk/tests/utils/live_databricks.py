"""Live Databricks test helpers with deterministic naming and cleanup."""

from __future__ import annotations

import os
import random
import string
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from schemax.core.sql_utils import split_sql_statements
from schemax.providers.base.executor import ExecutionConfig
from schemax.providers.unity.auth import create_databricks_client
from schemax.providers.unity.executor import UnitySQLExecutor


def _prompt_env_if_tty(var_name: str, prompt: str | None = None) -> str | None:
    """If stdin is a TTY and var is unset, prompt and return the value (caller should set os.environ)."""
    if os.getenv(var_name):
        return None
    if not sys.stdin.isatty():
        return None
    msg = prompt or f"{var_name} (required for live integration tests): "
    try:
        return input(msg).strip() or None
    except (EOFError, OSError):
        return None


@dataclass(frozen=True)
class LiveDatabricksConfig:
    profile: str
    warehouse_id: str
    managed_location: str
    resource_prefix: str
    timeout_seconds: int


def require_live_command_tests() -> LiveDatabricksConfig:
    """Return validated env config or skip test when live command testing is off.

    If required env vars are already set (e.g. export or .env), no prompts are shown.
    When stdin is a TTY and a var is missing, prompts the user interactively and sets
    the value in os.environ so subsequent tests see it.
    """
    run_live = os.getenv("SCHEMAX_RUN_LIVE_COMMAND_TESTS")
    if run_live != "1":
        if sys.stdin.isatty():
            val = _prompt_env_if_tty(
                "SCHEMAX_RUN_LIVE_COMMAND_TESTS",
                "Enable live integration tests? Set SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 (or press Enter to skip): ",
            )
            if val == "1":
                os.environ["SCHEMAX_RUN_LIVE_COMMAND_TESTS"] = "1"
            else:
                pytest.skip("Set SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 to run live command tests")
        else:
            pytest.skip("Set SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 to run live command tests")

    profile = _require_env(
        "DATABRICKS_PROFILE",
        "DATABRICKS_PROFILE (Databricks CLI profile name, e.g. DEFAULT): ",
    )
    warehouse_id = _require_env(
        "DATABRICKS_WAREHOUSE_ID",
        "DATABRICKS_WAREHOUSE_ID (SQL warehouse ID): ",
    )
    managed_location = _require_env(
        "DATABRICKS_MANAGED_LOCATION",
        "DATABRICKS_MANAGED_LOCATION (managed location path for test catalogs): ",
    )
    resource_prefix = os.getenv("SCHEMAX_LIVE_RESOURCE_PREFIX", "schemax_live")

    timeout_raw = os.getenv("SCHEMAX_LIVE_TEST_TIMEOUT_SECONDS", "300")
    try:
        timeout_seconds = int(timeout_raw)
    except ValueError:
        timeout_seconds = 300

    return LiveDatabricksConfig(
        profile=profile,
        warehouse_id=warehouse_id,
        managed_location=managed_location,
        resource_prefix=resource_prefix,
        timeout_seconds=timeout_seconds,
    )


def build_execution_config(
    config: LiveDatabricksConfig, *, dry_run: bool = False
) -> ExecutionConfig:
    return ExecutionConfig(
        target_env="dev",
        profile=config.profile,
        warehouse_id=config.warehouse_id,
        dry_run=dry_run,
        no_interaction=True,
        timeout_seconds=config.timeout_seconds,
    )


def create_executor(config: LiveDatabricksConfig) -> UnitySQLExecutor:
    client = create_databricks_client(profile=config.profile)
    return UnitySQLExecutor(client)


def make_random(length: int = 8) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))  # noqa: S311


def make_namespaced_id(config: LiveDatabricksConfig, suffix: str | None = None) -> str:
    token = suffix or make_random(8)
    return f"{config.resource_prefix}_{token}"


def load_sql_fixture(path: Path, replacements: dict[str, str]) -> list[str]:
    sql_text = path.read_text()
    for old, new in replacements.items():
        sql_text = sql_text.replace(old, new)
    return split_sql_statements(sql_text)


def cleanup_objects(
    executor: UnitySQLExecutor, config: LiveDatabricksConfig, catalogs: list[str]
) -> None:
    statements = [f"DROP CATALOG IF EXISTS {catalog} CASCADE" for catalog in catalogs]
    if not statements:
        return
    executor.execute_statements(statements=statements, config=build_execution_config(config))


def _require_env(var_name: str, prompt: str | None = None) -> str:
    value = os.getenv(var_name)
    if value:
        return value  # already set: skip prompt
    if sys.stdin.isatty():
        prompted = _prompt_env_if_tty(var_name, prompt)
        if prompted:
            os.environ[var_name] = prompted
            return prompted
    value = os.getenv(var_name)
    if not value:
        pytest.skip(f"{var_name} is not set (set it or run with a TTY to be prompted)")
    return value
