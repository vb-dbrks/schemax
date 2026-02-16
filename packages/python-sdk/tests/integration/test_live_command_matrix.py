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


def _assert_schema_exists(config, physical_catalog: str, schema_name: str) -> None:
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


def _table_exists(config, physical_catalog: str, schema_name: str, table_name: str) -> bool:
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
