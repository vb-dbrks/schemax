"""Opt-in live Databricks command matrix coverage."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, load_current_state
from schemax.providers import ProviderRegistry
from schemax.providers.base.executor import ExecutionConfig
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    LiveDatabricksConfig,
    build_execution_config,
    cleanup_objects,
    create_executor,
    load_sql_fixture,
    make_namespaced_id,
    require_live_command_tests,
)


def _write_project_env_overrides(
    workspace: Path,
    *,
    top_level_name: str,
    catalog_mappings: dict[str, str],
) -> None:
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    dev_env = project["provider"]["environments"]["dev"]
    dev_env["topLevelName"] = top_level_name
    dev_env["catalogMappings"] = catalog_mappings
    project_path.write_text(json.dumps(project, indent=2))


def _write_project_managed_scope(
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


def _write_project_promote_envs(
    workspace: Path,
    *,
    suffix: str,
    resource_prefix: str,
    logical_catalog: str = "bronze",
) -> tuple[str, str, str, str, str, str]:
    """Set dev/test/prod env overrides for promote test. Returns (tracking_dev, physical_dev, tracking_test, physical_test, tracking_prod, physical_prod)."""
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


def _assert_schema_exists(
    config: LiveDatabricksConfig, physical_catalog: str, schema_name: str
) -> None:
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


def _table_exists(
    config: LiveDatabricksConfig,
    physical_catalog: str,
    schema_name: str,
    table_name: str,
) -> bool:
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


def _volume_exists(
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


def _function_exists(
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


def _materialized_view_exists(
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


@pytest.mark.integration
def test_live_command_matrix(tmp_path: Path) -> None:
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    catalog = f"test_cmd_fixture_{suffix}"
    fixture_path = (
        Path(__file__).resolve().parents[1] / "resources" / "sql" / "unity_command_fixture.sql"
    )
    managed_root = f"{config.managed_location.rstrip('/')}/schemax-command-live/{suffix}"

    statements = load_sql_fixture(
        fixture_path,
        {
            "test_cmd_fixture": catalog,
            "__MANAGED_ROOT__": managed_root,
        },
    )

    executor = create_executor(config)

    try:
        seed = executor.execute_statements(
            statements=statements,
            config=build_execution_config(config),
        )
        if seed.status != "success":
            failed = [
                (
                    stmt.sql.splitlines()[0] if stmt.sql else "<unknown>",
                    stmt.error_message or "unknown error",
                )
                for stmt in seed.statement_results
                if stmt.status != "success"
            ]
            pytest.fail(
                f"Fixture seed failed with status={seed.status}, "
                f"error={seed.error_message}, failed_statements={failed}"
            )

        init_result = invoke_cli("init", "--provider", "unity", str(workspace))
        assert init_result.exit_code == 0

        import_preview = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            catalog,
            "--catalog-map",
            f"{catalog}={catalog}",
            "--dry-run",
            str(workspace),
        )
        assert import_preview.exit_code == 0

        import_write = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            catalog,
            "--catalog-map",
            f"{catalog}={catalog}",
            str(workspace),
        )
        assert import_write.exit_code == 0

        snapshot_1 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Live baseline",
            "--version",
            "v0.1.0",
            str(workspace),
        )
        assert snapshot_1.exit_code == 0

        validate_result = invoke_cli("validate", str(workspace))
        assert validate_result.exit_code == 0

        sql_result = invoke_cli(
            "sql",
            "--snapshot",
            "latest",
            "--target",
            "dev",
            str(workspace),
        )
        assert sql_result.exit_code == 0

        # Create a local-only change to build a second snapshot for diff/rollback command coverage.
        builder = OperationBuilder()
        append_ops(
            workspace,
            [builder.add_catalog("cat_local_live", f"local_live_{suffix}", op_id="op_local_1")],
        )

        snapshot_2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Live local delta",
            "--version",
            "v0.2.0",
            str(workspace),
        )
        assert snapshot_2.exit_code == 0, snapshot_2.output

        # Keep environment mappings complete for all logical catalogs in state.
        project_path = workspace / ".schemax" / "project.json"
        project = json.loads(project_path.read_text())
        dev_env = project["provider"]["environments"]["dev"]
        mappings = dict(dev_env.get("catalogMappings") or {})
        mappings[f"local_live_{suffix}"] = f"local_live_{suffix}"
        dev_env["catalogMappings"] = mappings
        project_path.write_text(json.dumps(project, indent=2))

        diff_result = invoke_cli(
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            str(workspace),
        )
        assert diff_result.exit_code == 0, diff_result.output

        apply_result = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--dry-run",
            "--no-interaction",
            str(workspace),
        )
        assert apply_result.exit_code == 0, apply_result.output

        rollback_result = invoke_cli(
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--dry-run",
            "--no-interaction",
            str(workspace),
        )
        assert rollback_result.exit_code == 0, rollback_result.output

        snapshot_validate = invoke_cli("snapshot", "validate", str(workspace))
        assert snapshot_validate.exit_code == 0

        # bundle is deterministic contract-only today, but included for matrix completeness.
        bundle_result = invoke_cli("bundle", "--target", "dev", "--version", "0.1.0")
        assert bundle_result.exit_code == 0
    finally:
        cleanup_objects(executor, config, [catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_snapshot_rebase_explicit_skip() -> None:
    require_live_command_tests()
    pytest.skip("Live snapshot rebase requires git-rebase divergence setup; not run in this matrix")


@pytest.mark.integration
def test_live_apply_and_rollback_non_dry_run(tmp_path: Path) -> None:
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_mutating_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = f"logical_live_{suffix}"
    physical_catalog = f"{config.resource_prefix}_apply_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_{suffix}"
    schema_name = "core"
    table_name = "events"

    ensure_project_file(workspace, provider_id="unity")
    _write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )

    builder = OperationBuilder()
    table_id = f"table_{suffix}"
    col_id_id = f"col_id_{suffix}"
    col_val_id = f"col_val_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/mutating/{suffix}"
        )
        preseed = executor.execute_statements(
            statements=[
                f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
                f"MANAGED LOCATION '{managed_root}/tracking'",
                f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
                f"MANAGED LOCATION '{managed_root}/physical'",
                f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
            ],
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}

        # Remove init-time implicit catalog op from changelog to keep test deterministic
        # in workspaces where CREATE CATALOG without MANAGED LOCATION is not allowed.
        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        import_result = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            physical_catalog,
            "--catalog-map",
            f"{logical_catalog}={physical_catalog}",
            "--adopt-baseline",
            str(workspace),
        )
        assert import_result.exit_code == 0, import_result.output

        project_path = workspace / ".schemax" / "project.json"
        project = json.loads(project_path.read_text())
        baseline_version = project.get("latestSnapshot")
        assert baseline_version, "Baseline snapshot version not found after import adoption"

        apply_v1 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            "--dry-run",
            str(workspace),
        )
        assert apply_v1.exit_code == 0, apply_v1.output
        _assert_schema_exists(config, physical_catalog, schema_name)

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalog_state = next(
            catalog for catalog in state["catalogs"] if catalog.get("name") == logical_catalog
        )
        schema_state = next(
            schema
            for schema in catalog_state.get("schemas", [])
            if schema.get("name") == schema_name
        )
        schema_id = schema_state["id"]

        append_ops(
            workspace,
            [
                builder.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_{suffix}",
                ),
                builder.add_column(
                    col_id_id,
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_{suffix}",
                ),
                builder.add_column(
                    col_val_id,
                    table_id,
                    "event_type",
                    "STRING",
                    nullable=True,
                    comment="Event type",
                    op_id=f"op_col_val_{suffix}",
                ),
            ],
        )

        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Mutating delta",
            str(workspace),
        )
        assert snapshot_v2.exit_code == 0, snapshot_v2.output

        apply_v2 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_v2.exit_code == 0, apply_v2.output
        assert _table_exists(config, physical_catalog, schema_name, table_name)

        rollback = invoke_cli(
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            baseline_version,
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert rollback.exit_code == 0, rollback.output
        assert not _table_exists(config, physical_catalog, schema_name, table_name)
        _assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, [physical_catalog, tracking_catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_e2e_create_apply_rollback_with_grants(tmp_path: Path) -> None:
    """Live E2E: create catalog/schema/table → apply → add grants → apply → rollback (real Databricks).

    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_e2e_grants_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = f"logical_e2e_{suffix}"
    physical_catalog = f"{config.resource_prefix}_e2e_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_e2e_track_{suffix}"
    schema_name = "core"
    table_name = "events"

    ensure_project_file(workspace, provider_id="unity")
    _write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )

    builder = OperationBuilder()
    table_id = f"table_e2e_{suffix}"
    col_id_id = f"col_id_e2e_{suffix}"
    col_val_id = f"col_val_e2e_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/e2e-grants/{suffix}"
        )
        preseed = executor.execute_statements(
            statements=[
                f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
                f"MANAGED LOCATION '{managed_root}/tracking'",
                f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
                f"MANAGED LOCATION '{managed_root}/physical'",
                f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
            ],
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        import_result = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            physical_catalog,
            "--catalog-map",
            f"{logical_catalog}={physical_catalog}",
            "--adopt-baseline",
            str(workspace),
        )
        assert import_result.exit_code == 0, import_result.output

        project_path = workspace / ".schemax" / "project.json"
        project = json.loads(project_path.read_text())
        baseline_version = project.get("latestSnapshot")
        assert baseline_version, "Baseline snapshot version not found after import adoption"

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalog_state = next(c for c in state["catalogs"] if c.get("name") == logical_catalog)
        schema_state = next(
            s for s in catalog_state.get("schemas", []) if s.get("name") == schema_name
        )
        schema_id = schema_state["id"]

        append_ops(
            workspace,
            [
                builder.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_e2e_{suffix}",
                ),
                builder.add_column(
                    col_id_id,
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_e2e_{suffix}",
                ),
                builder.add_column(
                    col_val_id,
                    table_id,
                    "event_type",
                    "STRING",
                    nullable=True,
                    comment="Event type",
                    op_id=f"op_col_val_e2e_{suffix}",
                ),
            ],
        )

        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "E2E table delta",
            "--version",
            "v0.2.0",
            str(workspace),
        )
        assert snapshot_v2.exit_code == 0, snapshot_v2.output

        apply_v2 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_v2.exit_code == 0, apply_v2.output
        assert _table_exists(config, physical_catalog, schema_name, table_name)

        append_ops(
            workspace,
            [
                builder.add_grant(
                    "table",
                    table_id,
                    "account users",
                    ["SELECT", "MODIFY"],
                    op_id=f"op_grant_e2e_{suffix}",
                ),
            ],
        )
        snapshot_v3 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "E2E grants",
            "--version",
            "v0.3.0",
            str(workspace),
        )
        assert snapshot_v3.exit_code == 0, snapshot_v3.output

        apply_v3 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_v3.exit_code == 0, apply_v3.output

        rollback_result = invoke_cli(
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            baseline_version,
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert rollback_result.exit_code == 0, rollback_result.output
        assert not _table_exists(config, physical_catalog, schema_name, table_name)
        _assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, [physical_catalog, tracking_catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_apply_governance_only(tmp_path: Path) -> None:
    """Live E2E: managedCategories = ['governance'] → apply runs only governance SQL (e.g. COMMENT, GRANT).

    Preseed catalog/schema/table, import adopt-baseline, add table+columns and apply (v0.2.0).
    Then add set_table_comment, set managedCategories to ['governance'], snapshot v0.3.0, apply.
    Apply should only execute governance DDL (no CREATE CATALOG/SCHEMA/TABLE).
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_governance_only_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = f"logical_gov_{suffix}"
    physical_catalog = f"{config.resource_prefix}_gov_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_gov_track_{suffix}"
    schema_name = "core"
    table_name = "events"

    ensure_project_file(workspace, provider_id="unity")
    _write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )

    builder = OperationBuilder()
    table_id = f"table_gov_{suffix}"
    col_id_id = f"col_id_gov_{suffix}"
    col_val_id = f"col_val_gov_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/governance-only/{suffix}"
        )
        preseed = executor.execute_statements(
            statements=[
                f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
                f"MANAGED LOCATION '{managed_root}/tracking'",
                f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
                f"MANAGED LOCATION '{managed_root}/physical'",
                f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
            ],
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        import_result = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            physical_catalog,
            "--catalog-map",
            f"{logical_catalog}={physical_catalog}",
            "--adopt-baseline",
            str(workspace),
        )
        assert import_result.exit_code == 0, import_result.output

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalog_state = next(c for c in state["catalogs"] if c.get("name") == logical_catalog)
        schema_state = next(
            s for s in catalog_state.get("schemas", []) if s.get("name") == schema_name
        )
        schema_id = schema_state["id"]

        append_ops(
            workspace,
            [
                builder.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_gov_{suffix}",
                ),
                builder.add_column(
                    col_id_id,
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_gov_{suffix}",
                ),
                builder.add_column(
                    col_val_id,
                    table_id,
                    "event_type",
                    "STRING",
                    nullable=True,
                    comment="Event type",
                    op_id=f"op_col_val_gov_{suffix}",
                ),
            ],
        )

        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Governance-only table delta",
            "--version",
            "v0.2.0",
            str(workspace),
        )
        assert snapshot_v2.exit_code == 0, snapshot_v2.output

        apply_v2 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_v2.exit_code == 0, apply_v2.output
        assert _table_exists(config, physical_catalog, schema_name, table_name)

        append_ops(
            workspace,
            [
                builder.set_table_comment(
                    table_id,
                    "Governance-only test comment",
                    op_id=f"op_comment_gov_{suffix}",
                ),
            ],
        )
        _write_project_managed_scope(workspace, managed_categories=["governance"])

        # Run SQL before creating snapshot: sql uses changelog ops, and snapshot create clears changelog
        sql_out = invoke_cli(
            "sql",
            "--target",
            "dev",
            str(workspace),
        )
        assert sql_out.exit_code == 0, sql_out.output
        assert "CREATE CATALOG" not in sql_out.output
        assert "CREATE SCHEMA" not in sql_out.output
        assert "CREATE TABLE" not in sql_out.output
        assert "COMMENT" in sql_out.output or "comment" in sql_out.output.lower()

        snapshot_v3 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Governance-only comment",
            "--version",
            "v0.3.0",
            str(workspace),
        )
        assert snapshot_v3.exit_code == 0, snapshot_v3.output

        apply_v3 = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_v3.exit_code == 0, apply_v3.output
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, [physical_catalog, tracking_catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_apply_existing_catalog_skips_create(tmp_path: Path) -> None:
    """Live E2E: existingObjects.catalog = [logical] → apply skips CREATE CATALOG for that catalog.

    Preseed tracking + physical catalog + schema (catalog "already exists").
    Set existingObjects.catalog = [logical], add ops: add_catalog + add_schema + add_table + column.
    Apply should filter out add_catalog and only run CREATE SCHEMA, CREATE TABLE, ADD COLUMN.
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_existing_cat_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = f"logical_existing_{suffix}"
    physical_catalog = f"{config.resource_prefix}_existing_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_existing_track_{suffix}"
    schema_name = "core"
    table_name = "events"

    ensure_project_file(workspace, provider_id="unity")
    _write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )
    _write_project_managed_scope(workspace, existing_catalogs=[logical_catalog])

    builder = OperationBuilder()
    cat_id = f"cat_existing_{suffix}"
    sch_id = f"sch_existing_{suffix}"
    tbl_id = f"tbl_existing_{suffix}"
    col_id = f"col_existing_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/existing-catalog/{suffix}"
        )
        preseed = executor.execute_statements(
            statements=[
                f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
                f"MANAGED LOCATION '{managed_root}/tracking'",
                f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
                f"MANAGED LOCATION '{managed_root}/physical'",
                f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
            ],
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        append_ops(
            workspace,
            [
                builder.add_catalog(cat_id, logical_catalog, op_id=f"op_cat_existing_{suffix}"),
                builder.add_schema(sch_id, schema_name, cat_id, op_id=f"op_sch_existing_{suffix}"),
                builder.add_table(
                    tbl_id, table_name, sch_id, "delta", op_id=f"op_tbl_existing_{suffix}"
                ),
                builder.add_column(
                    col_id,
                    tbl_id,
                    "event_id",
                    "BIGINT",
                    False,
                    "Event ID",
                    op_id=f"op_col_existing_{suffix}",
                ),
            ],
        )

        sql_out = invoke_cli("sql", "--target", "dev", str(workspace))
        assert sql_out.exit_code == 0, sql_out.output
        assert "CREATE CATALOG" not in sql_out.output, (
            "existingObjects.catalog should filter out add_catalog; SQL must not contain CREATE CATALOG"
        )
        assert "CREATE SCHEMA" in sql_out.output or "CREATE TABLE" in sql_out.output

        snapshot_v1 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Existing catalog baseline",
            "--version",
            "v0.1.0",
            str(workspace),
        )
        assert snapshot_v1.exit_code == 0, snapshot_v1.output

        apply_result = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_result.exit_code == 0, apply_result.output
        assert _table_exists(config, physical_catalog, schema_name, table_name)
        _assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, [physical_catalog, tracking_catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_greenfield_promote_dev_test_prod_then_rollback_prod(tmp_path: Path) -> None:
    """Live E2E: greenfield project → apply to dev → test → prod → add change → promote again → rollback prod only.

    Creates a new dev greenfield SchemaX project (catalog/schema/table), publishes to dev, then test, then prod.
    Adds a second table, promotes to all three, then rollbacks only prod to previous snapshot.
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_promote_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = "bronze"
    schema_name = "raw"
    table_users = "users"
    table_orders = "orders"

    ensure_project_file(workspace, provider_id="unity")
    (
        tracking_dev,
        physical_dev,
        tracking_test,
        physical_test,
        tracking_prod,
        physical_prod,
    ) = _write_project_promote_envs(
        workspace, suffix=suffix, resource_prefix=config.resource_prefix
    )

    builder = OperationBuilder()
    cat_id = f"cat_promote_{suffix}"
    sch_id = f"sch_promote_{suffix}"
    tbl_users_id = f"tbl_users_{suffix}"
    tbl_orders_id = f"tbl_orders_{suffix}"

    all_catalogs = [
        tracking_dev,
        physical_dev,
        tracking_test,
        physical_test,
        tracking_prod,
        physical_prod,
    ]

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/promote/{suffix}"
        )
        preseed_statements = [
            f"CREATE CATALOG IF NOT EXISTS {tracking_dev} MANAGED LOCATION '{managed_root}/tracking_dev'",
            f"CREATE CATALOG IF NOT EXISTS {physical_dev} MANAGED LOCATION '{managed_root}/physical_dev'",
            f"CREATE SCHEMA IF NOT EXISTS {physical_dev}.{schema_name}",
            f"CREATE CATALOG IF NOT EXISTS {tracking_test} MANAGED LOCATION '{managed_root}/tracking_test'",
            f"CREATE CATALOG IF NOT EXISTS {physical_test} MANAGED LOCATION '{managed_root}/physical_test'",
            f"CREATE SCHEMA IF NOT EXISTS {physical_test}.{schema_name}",
            f"CREATE CATALOG IF NOT EXISTS {tracking_prod} MANAGED LOCATION '{managed_root}/tracking_prod'",
            f"CREATE CATALOG IF NOT EXISTS {physical_prod} MANAGED LOCATION '{managed_root}/physical_prod'",
            f"CREATE SCHEMA IF NOT EXISTS {physical_prod}.{schema_name}",
        ]
        preseed = executor.execute_statements(
            statements=preseed_statements,
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        append_ops(
            workspace,
            [
                builder.add_catalog(cat_id, logical_catalog, op_id=f"op_cat_{suffix}"),
                builder.add_schema(sch_id, schema_name, cat_id, op_id=f"op_sch_{suffix}"),
                builder.add_table(
                    tbl_users_id, table_users, sch_id, "delta", op_id=f"op_tbl_users_{suffix}"
                ),
                builder.add_column(
                    f"col_id_{suffix}",
                    tbl_users_id,
                    "user_id",
                    "BIGINT",
                    False,
                    "User ID",
                    op_id=f"op_col_id_{suffix}",
                ),
                builder.add_column(
                    f"col_email_{suffix}",
                    tbl_users_id,
                    "email",
                    "STRING",
                    True,
                    "Email",
                    op_id=f"op_col_email_{suffix}",
                ),
            ],
        )

        snapshot_v1 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Promote v1",
            "--version",
            "v0.1.0",
            str(workspace),
        )
        assert snapshot_v1.exit_code == 0, snapshot_v1.output

        for target in ("dev", "test", "prod"):
            apply_r = invoke_cli(
                "apply",
                "--target",
                target,
                "--profile",
                config.profile,
                "--warehouse-id",
                config.warehouse_id,
                "--no-interaction",
                str(workspace),
            )
            assert apply_r.exit_code == 0, f"apply --target {target}: {apply_r.output}"

        assert _table_exists(config, physical_dev, schema_name, table_users)
        assert _table_exists(config, physical_test, schema_name, table_users)
        assert _table_exists(config, physical_prod, schema_name, table_users)

        append_ops(
            workspace,
            [
                builder.add_table(
                    tbl_orders_id,
                    table_orders,
                    sch_id,
                    "delta",
                    op_id=f"op_tbl_orders_{suffix}",
                ),
                builder.add_column(
                    f"col_order_id_{suffix}",
                    tbl_orders_id,
                    "order_id",
                    "BIGINT",
                    False,
                    "Order ID",
                    op_id=f"op_col_ord_{suffix}",
                ),
            ],
        )

        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Promote v2 add orders",
            "--version",
            "v0.2.0",
            str(workspace),
        )
        assert snapshot_v2.exit_code == 0, snapshot_v2.output

        for target in ("dev", "test", "prod"):
            apply_r = invoke_cli(
                "apply",
                "--target",
                target,
                "--profile",
                config.profile,
                "--warehouse-id",
                config.warehouse_id,
                "--no-interaction",
                str(workspace),
            )
            assert apply_r.exit_code == 0, f"apply --target {target}: {apply_r.output}"

        assert _table_exists(config, physical_dev, schema_name, table_orders)
        assert _table_exists(config, physical_test, schema_name, table_orders)
        assert _table_exists(config, physical_prod, schema_name, table_orders)

        rollback_prod = invoke_cli(
            "rollback",
            "--target",
            "prod",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert rollback_prod.exit_code == 0, rollback_prod.output

        assert _table_exists(config, physical_dev, schema_name, table_orders)
        assert _table_exists(config, physical_test, schema_name, table_orders)
        assert not _table_exists(config, physical_prod, schema_name, table_orders)
        assert _table_exists(config, physical_prod, schema_name, table_users)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, all_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_import_sees_volume_function_materialized_view(tmp_path: Path) -> None:
    """Live E2E: Seed catalog/schema/table/volume/function/MV via SQL → init → import → validate/sql.

    Asserts that import discovers volumes, functions, and materialized views and that
    validate and sql run successfully. Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_uc_objects_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    catalog = f"test_uc_objects_{suffix}"
    schema_name = "uc_core"
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "resources"
        / "sql"
        / "unity_uc_objects_fixture.sql"
    )
    managed_root = (
        f"{config.managed_location.rstrip('/')}/schemax-uc-objects-live/{suffix}"
    )

    statements = load_sql_fixture(
        fixture_path,
        {
            "test_uc_objects": catalog,
            "__MANAGED_ROOT__": managed_root,
        },
    )

    executor = create_executor(config)
    try:
        seed = executor.execute_statements(
            statements=statements,
            config=build_execution_config(config),
        )
        if seed.status not in ("success", "partial"):
            failed = [
                (
                    stmt.sql.splitlines()[0] if stmt.sql else "<unknown>",
                    stmt.error_message or "unknown error",
                )
                for stmt in seed.statement_results
                if stmt.status != "success"
            ]
            pytest.fail(
                f"UC objects fixture seed failed: {seed.status}, "
                f"error={getattr(seed, 'error_message', '')}, failed={failed}"
            )

        init_result = invoke_cli("init", "--provider", "unity", str(workspace))
        assert init_result.exit_code == 0, init_result.output

        import_result = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            catalog,
            "--catalog-map",
            f"{catalog}={catalog}",
            str(workspace),
        )
        assert import_result.exit_code == 0, import_result.output

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalogs = state.get("catalogs", [])
        assert catalogs, "No catalogs after import"
        schemas = catalogs[0].get("schemas", [])
        schema_uc = next((s for s in schemas if s.get("name") == schema_name), None)
        assert schema_uc is not None, f"Schema {schema_name} not found after import"

        volumes = schema_uc.get("volumes", [])
        functions = schema_uc.get("functions", [])
        mvs = schema_uc.get("materialized_views", schema_uc.get("materializedViews", []))

        assert any(v.get("name") == "managed_vol" for v in volumes), (
            f"Volume managed_vol not in state: {[v.get('name') for v in volumes]}"
        )
        assert any(f.get("name") == "live_e2e_func" for f in functions), (
            f"Function live_e2e_func not in state: {[f.get('name') for f in functions]}"
        )
        assert any(m.get("name") == "live_e2e_mv" for m in mvs), (
            f"Materialized view live_e2e_mv not in state: {[m.get('name') for m in mvs]}"
        )

        validate_result = invoke_cli("validate", str(workspace))
        assert validate_result.exit_code == 0, validate_result.output

        sql_result = invoke_cli("sql", "--target", "dev", str(workspace))
        assert sql_result.exit_code == 0, sql_result.output
    finally:
        cleanup_objects(executor, config, [catalog])
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_e2e_apply_volume_function_materialized_view(tmp_path: Path) -> None:
    """Live E2E: Create project, add table + volume + function + MV → apply → assert all exist → rollback.

    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_namespaced_id(config).split("_", 2)[-1]

    workspace = tmp_path / f"workspace_apply_uc_{suffix}"
    workspace.mkdir(parents=True, exist_ok=True)

    logical_catalog = f"logical_uc_{suffix}"
    physical_catalog = f"{config.resource_prefix}_apply_uc_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_uc_{suffix}"
    schema_name = "core"
    table_name = "events"
    volume_name = "e2e_vol"
    function_name = "e2e_func"
    mv_name = "e2e_mv"

    ensure_project_file(workspace, provider_id="unity")
    _write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )

    builder = OperationBuilder()
    table_id = f"table_uc_{suffix}"
    vol_id = f"vol_uc_{suffix}"
    func_id = f"func_uc_{suffix}"
    mv_id = f"mv_uc_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-apply-uc-live/{suffix}"
        )
        preseed = executor.execute_statements(
            statements=[
                f"CREATE CATALOG IF NOT EXISTS {tracking_catalog} "
                f"MANAGED LOCATION '{managed_root}/tracking'",
                f"CREATE CATALOG IF NOT EXISTS {physical_catalog} "
                f"MANAGED LOCATION '{managed_root}/physical'",
                f"CREATE SCHEMA IF NOT EXISTS {physical_catalog}.{schema_name}",
            ],
            config=build_execution_config(config),
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

        changelog_path = workspace / ".schemax" / "changelog.json"
        changelog = json.loads(changelog_path.read_text())
        changelog["ops"] = []
        changelog_path.write_text(json.dumps(changelog, indent=2))

        import_result = invoke_cli(
            "import",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--catalog",
            physical_catalog,
            "--catalog-map",
            f"{logical_catalog}={physical_catalog}",
            "--adopt-baseline",
            str(workspace),
        )
        assert import_result.exit_code == 0, import_result.output

        baseline_version = json.loads(
            (workspace / ".schemax" / "project.json").read_text()
        ).get("latestSnapshot")
        assert baseline_version, "No baseline snapshot after import adoption"

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalog_state = next(c for c in state["catalogs"] if c.get("name") == logical_catalog)
        schema_state = next(
            s for s in catalog_state.get("schemas", []) if s.get("name") == schema_name
        )
        schema_id = schema_state["id"]

        append_ops(
            workspace,
            [
                builder.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_uc_{suffix}",
                ),
                builder.add_column(
                    f"col_id_uc_{suffix}",
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_uc_{suffix}",
                ),
                builder.add_column(
                    f"col_val_uc_{suffix}",
                    table_id,
                    "event_type",
                    "STRING",
                    nullable=True,
                    comment="Event type",
                    op_id=f"op_col_val_uc_{suffix}",
                ),
                builder.add_volume(
                    vol_id,
                    volume_name,
                    schema_id,
                    "managed",
                    comment="E2E volume",
                    op_id=f"op_vol_uc_{suffix}",
                ),
                builder.add_function(
                    func_id,
                    function_name,
                    schema_id,
                    "SQL",
                    "INT",
                    "1",
                    comment="E2E function",
                    op_id=f"op_func_uc_{suffix}",
                ),
                builder.add_materialized_view(
                    mv_id,
                    mv_name,
                    schema_id,
                    f"SELECT * FROM {table_name}",
                    comment="E2E MV",
                    op_id=f"op_mv_uc_{suffix}",
                ),
            ],
        )

        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "UC objects delta",
            "--version",
            "v0.2.0",
            str(workspace),
        )
        assert snapshot_v2.exit_code == 0, snapshot_v2.output

        apply_result = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert apply_result.exit_code == 0, apply_result.output

        assert _table_exists(config, physical_catalog, schema_name, table_name)
        assert _volume_exists(config, physical_catalog, schema_name, volume_name)
        assert _function_exists(config, physical_catalog, schema_name, function_name)
        assert _materialized_view_exists(config, physical_catalog, schema_name, mv_name)

        rollback_result = invoke_cli(
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            baseline_version,
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert rollback_result.exit_code == 0, rollback_result.output

        assert not _table_exists(config, physical_catalog, schema_name, table_name)
        assert not _volume_exists(config, physical_catalog, schema_name, volume_name)
        assert not _function_exists(config, physical_catalog, schema_name, function_name)
        assert not _materialized_view_exists(config, physical_catalog, schema_name, mv_name)
        _assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, [physical_catalog, tracking_catalog])
        shutil.rmtree(workspace, ignore_errors=True)
