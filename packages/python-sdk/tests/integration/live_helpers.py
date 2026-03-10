"""Shared helpers for live Databricks integration tests."""

import json
from pathlib import Path
from typing import Any

from databricks.sdk.service.sql import StatementState

from schemax.providers.unity.auth import create_databricks_client
from tests.utils.live_databricks import (
    LiveDatabricksConfig,
    build_execution_config,
    make_namespaced_id,
)


def make_suffix(config: LiveDatabricksConfig) -> str:
    """Return a short suffix for unique resource names (e.g. from make_namespaced_id)."""
    return make_namespaced_id(config).split("_", 2)[-1]


def make_workspace(tmp_path: Path, name: str, suffix: str) -> Path:
    """Create and return workspace directory: tmp_path / f'workspace_{name}_{suffix}'."""
    workspace = tmp_path / f"workspace_{name}_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def write_project_env_overrides(
    workspace: Path,
    *,
    top_level_name: str,
    catalog_mappings: dict[str, str],
) -> None:
    """Override dev env topLevelName and catalogMappings in project.json."""
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    dev_env = project["provider"]["environments"]["dev"]
    dev_env["topLevelName"] = top_level_name
    dev_env["catalogMappings"] = catalog_mappings
    project_path.write_text(json.dumps(project, indent=2))


def write_project_managed_scope(
    workspace: Path,
    *,
    managed_categories: list[str] | None = None,
    existing_catalogs: list[str] | None = None,
) -> None:
    """Set dev env managedCategories and/or existingObjects.catalog for scope filter tests."""
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    dev_env = project["provider"]["environments"]["dev"]
    if managed_categories is not None:
        dev_env["managedCategories"] = managed_categories
    if existing_catalogs is not None:
        dev_env["existingObjects"] = {"catalog": existing_catalogs}
    project_path.write_text(json.dumps(project, indent=2))


def write_project_promote_envs(
    workspace: Path,
    *,
    suffix: str,
    resource_prefix: str,
    logical_catalog: str = "bronze",
) -> tuple[str, str, str, str, str, str]:
    """Set dev/test/prod env overrides for promote test.

    Returns (tracking_dev, physical_dev, tracking_test, physical_test, tracking_prod, physical_prod).
    """
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    envs = project["provider"]["environments"]

    tracking_dev = f"{resource_prefix}_promote_track_dev_{suffix}"
    physical_dev = f"{resource_prefix}_promote_dev_{suffix}"
    tracking_test = f"{resource_prefix}_promote_track_test_{suffix}"
    physical_test = f"{resource_prefix}_promote_test_{suffix}"
    tracking_prod = f"{resource_prefix}_promote_track_prod_{suffix}"
    physical_prod = f"{resource_prefix}_promote_prod_{suffix}"

    envs["dev"]["topLevelName"] = tracking_dev
    envs["dev"]["catalogMappings"] = {logical_catalog: physical_dev}
    envs["test"]["topLevelName"] = tracking_test
    envs["test"]["catalogMappings"] = {logical_catalog: physical_test}
    envs["prod"]["topLevelName"] = tracking_prod
    envs["prod"]["catalogMappings"] = {logical_catalog: physical_prod}

    project_path.write_text(json.dumps(project, indent=2))
    return tracking_dev, physical_dev, tracking_test, physical_test, tracking_prod, physical_prod


def write_project_promote_managed_locations(workspace: Path, managed_root: str) -> None:
    """Add managedLocations for promote test so CREATE CATALOG emits MANAGED LOCATION.

    Use when the workspace requires an explicit managed location (e.g. Default Storage).
    Paths are per-environment subpaths under managed_root so apply to dev/test/prod works.
    """
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    root = managed_root.rstrip("/")
    project["managedLocations"] = {
        "catalog_root": {
            "paths": {
                "dev": f"{root}/physical_dev",
                "test": f"{root}/physical_test",
                "prod": f"{root}/physical_prod",
            }
        }
    }
    project_path.write_text(json.dumps(project, indent=2))


def _quick_query(config: LiveDatabricksConfig, sql: str) -> list[list[Any]]:
    """Execute a single SQL query and return raw data rows (lightweight, no discover_state).

    Raises on query failure or timeout so that callers get clear errors instead
    of silently treating failures as "object does not exist".
    """
    client = create_databricks_client(profile=config.profile)
    response = client.statement_execution.execute_statement(
        warehouse_id=config.warehouse_id,
        statement=sql,
        wait_timeout="60s",
    )
    if not response.status:
        raise RuntimeError(f"_quick_query: no status returned for: {sql}")
    state = response.status.state
    if state != StatementState.SUCCEEDED:
        error_msg = ""
        if response.status.error:
            error_msg = f": {response.status.error.message}"
        raise RuntimeError(f"_quick_query: statement {state}{error_msg} for: {sql}")
    if not response.result or not response.result.data_array:
        return []
    return list(response.result.data_array)


def _ident(name: str) -> str:
    """Backtick-escape an identifier to prevent SQL injection in FROM clauses."""
    return f"`{name.replace('`', '``')}`"


def assert_schema_exists(
    config: LiveDatabricksConfig, physical_catalog: str, schema_name: str
) -> None:
    """Assert that a schema exists in the given catalog (lightweight SQL check)."""
    cat_esc = physical_catalog.replace("'", "''")
    sch_esc = schema_name.replace("'", "''")
    cat_id = _ident(physical_catalog)
    rows = _quick_query(
        config,
        f"SELECT schema_name FROM {cat_id}.information_schema.schemata "
        f"WHERE catalog_name = '{cat_esc}' AND schema_name = '{sch_esc}'",
    )
    assert rows, f"Schema {schema_name} not found in catalog {physical_catalog}"


def table_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    table_name: str,
) -> bool:
    """Return True if a table exists in the given catalog.schema (lightweight SQL check)."""
    cat_esc = physical_catalog.replace("'", "''")
    sch_esc = schema_name.replace("'", "''")
    tbl_esc = table_name.replace("'", "''")
    cat_id = _ident(physical_catalog)
    rows = _quick_query(
        config,
        f"SELECT table_name FROM {cat_id}.information_schema.tables "
        f"WHERE table_catalog = '{cat_esc}' AND table_schema = '{sch_esc}' "
        f"AND table_name = '{tbl_esc}' AND table_type IN ('TABLE', 'MANAGED', 'EXTERNAL')",
    )
    return len(rows) > 0


def volume_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    volume_name: str,
) -> bool:
    """Return True if a volume exists in the given catalog.schema (lightweight SQL check)."""
    cat_esc = physical_catalog.replace("'", "''")
    sch_esc = schema_name.replace("'", "''")
    vol_esc = volume_name.replace("'", "''")
    rows = _quick_query(
        config,
        f"SELECT volume_name FROM system.information_schema.volumes "
        f"WHERE volume_catalog = '{cat_esc}' AND volume_schema = '{sch_esc}' "
        f"AND volume_name = '{vol_esc}'",
    )
    return len(rows) > 0


def function_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    function_name: str,
) -> bool:
    """Return True if a function exists in the given catalog.schema (lightweight SQL check)."""
    cat_esc = physical_catalog.replace("'", "''")
    sch_esc = schema_name.replace("'", "''")
    func_esc = function_name.replace("'", "''")
    cat_id = _ident(physical_catalog)
    rows = _quick_query(
        config,
        f"SELECT routine_name FROM {cat_id}.information_schema.routines "
        f"WHERE routine_catalog = '{cat_esc}' AND routine_schema = '{sch_esc}' "
        f"AND routine_name = '{func_esc}'",
    )
    return len(rows) > 0


def materialized_view_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    mv_name: str,
) -> bool:
    """Return True if a materialized view exists in the given catalog.schema (lightweight SQL check)."""
    cat_esc = physical_catalog.replace("'", "''")
    sch_esc = schema_name.replace("'", "''")
    mv_esc = mv_name.replace("'", "''")
    cat_id = _ident(physical_catalog)
    rows = _quick_query(
        config,
        f"SELECT table_name FROM {cat_id}.information_schema.tables "
        f"WHERE table_catalog = '{cat_esc}' AND table_schema = '{sch_esc}' "
        f"AND table_name = '{mv_esc}' AND table_type = 'MATERIALIZED_VIEW'",
    )
    return len(rows) > 0


def preseed_tracking_only(
    executor: Any,
    config: LiveDatabricksConfig,
    *,
    tracking_catalog: str,
    managed_root: str,
) -> Any:
    """Create only the tracking catalog so apply can record deployments.

    Returns the result of execute_statements so callers can assert status.
    """
    statements = [
        f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
        f"MANAGED LOCATION '{managed_root.rstrip('/')}/tracking'",
    ]
    return executor.execute_statements(
        statements=statements,
        config=build_execution_config(config),
    )


def preseed_catalog_schema(
    executor: Any,
    config: LiveDatabricksConfig,
    *,
    physical_catalog: str,
    tracking_catalog: str,
    schema_name: str,
    managed_root: str,
    clear_changelog_in: Path | None = None,
) -> Any:
    """Create tracking catalog, physical catalog, and schema via SQL; optionally clear changelog.

    Returns the result of execute_statements so callers can assert status.
    """
    statements = [
        f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
        f"MANAGED LOCATION '{managed_root}/tracking'",
        f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
        f"MANAGED LOCATION '{managed_root}/physical'",
        f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
    ]
    result = executor.execute_statements(
        statements=statements,
        config=build_execution_config(config),
    )
    if clear_changelog_in is not None:
        changelog_path = clear_changelog_in / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))
    return result
