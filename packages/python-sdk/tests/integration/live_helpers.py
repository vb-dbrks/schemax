"""Shared helpers for live Databricks integration tests."""

import json
from pathlib import Path
from typing import Any

from schemax.providers import ProviderRegistry
from schemax.providers.base.executor import ExecutionConfig

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


def assert_schema_exists(
    config: LiveDatabricksConfig, physical_catalog: str, schema_name: str
) -> None:
    """Assert that a schema exists in the given catalog (live discovery)."""
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    discovered = provider.discover_state(
        config=ExecutionConfig(
            target_env="dev",
            profile=config.profile,
            warehouse_id=config.warehouse_id,
        ),
        scope={"catalog": physical_catalog},
    )
    catalogs = discovered.get("catalogs", [])
    assert catalogs, f"Catalog not found: {physical_catalog}"
    schemas = catalogs[0].get("schemas", [])
    assert any(schema.get("name") == schema_name for schema in schemas)


def table_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    table_name: str,
) -> bool:
    """Return True if a table exists in the given catalog.schema (live discovery)."""
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    discovered = provider.discover_state(
        config=ExecutionConfig(
            target_env="dev",
            profile=config.profile,
            warehouse_id=config.warehouse_id,
        ),
        scope={"catalog": physical_catalog, "schema": schema_name},
    )
    catalogs = discovered.get("catalogs", [])
    if not catalogs:
        return False
    schemas = catalogs[0].get("schemas", [])
    if not schemas:
        return False
    tables = schemas[0].get("tables", [])
    return any(table.get("name") == table_name for table in tables)


def volume_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    volume_name: str,
) -> bool:
    """Return True if a volume exists in the given catalog.schema (live discovery)."""
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    discovered = provider.discover_state(
        config=ExecutionConfig(
            target_env="dev",
            profile=config.profile,
            warehouse_id=config.warehouse_id,
        ),
        scope={"catalog": physical_catalog, "schema": schema_name},
    )
    catalogs = discovered.get("catalogs", [])
    if not catalogs:
        return False
    for schema in catalogs[0].get("schemas", []):
        if schema.get("name") != schema_name:
            continue
        volumes = schema.get("volumes", [])
        return any(v.get("name") == volume_name for v in volumes)
    return False


def function_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    function_name: str,
) -> bool:
    """Return True if a function exists in the given catalog.schema (live discovery)."""
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    discovered = provider.discover_state(
        config=ExecutionConfig(
            target_env="dev",
            profile=config.profile,
            warehouse_id=config.warehouse_id,
        ),
        scope={"catalog": physical_catalog, "schema": schema_name},
    )
    catalogs = discovered.get("catalogs", [])
    if not catalogs:
        return False
    for schema in catalogs[0].get("schemas", []):
        if schema.get("name") != schema_name:
            continue
        functions = schema.get("functions", [])
        return any(f.get("name") == function_name for f in functions)
    return False


def materialized_view_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    mv_name: str,
) -> bool:
    """Return True if a materialized view exists in the given catalog.schema (live discovery)."""
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    discovered = provider.discover_state(
        config=ExecutionConfig(
            target_env="dev",
            profile=config.profile,
            warehouse_id=config.warehouse_id,
        ),
        scope={"catalog": physical_catalog, "schema": schema_name},
    )
    catalogs = discovered.get("catalogs", [])
    if not catalogs:
        return False
    for schema in catalogs[0].get("schemas", []):
        if schema.get("name") != schema_name:
            continue
        mvs = schema.get("materialized_views", schema.get("materializedViews", []))
        return any(m.get("name") == mv_name for m in mvs)
    return False


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
