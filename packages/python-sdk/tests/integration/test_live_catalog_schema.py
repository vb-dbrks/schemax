"""Live integration tests: catalog and schema (existingObjects scope)."""

import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file
from tests.integration.live_helpers import (
    assert_schema_exists,
    make_suffix,
    make_workspace,
    preseed_catalog_schema,
    table_exists,
    write_project_env_overrides,
    write_project_managed_scope,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    LiveDatabricksConfig,
    cleanup_objects,
    create_executor,
    require_live_command_tests,
)


@pytest.mark.integration
def test_live_apply_existing_catalog_skips_create(tmp_path: Path) -> None:
    """Live E2E: existingObjects.catalog = [logical] → apply skips CREATE CATALOG.

    Preseed tracking + physical catalog + schema (catalog "already exists").
    Set existingObjects.catalog = [logical], add ops: add_catalog + add_schema + add_table + column.
    Apply should filter out add_catalog and only run CREATE SCHEMA, CREATE TABLE, ADD COLUMN.
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "existing_cat", suffix)

    logical_catalog = f"logical_existing_{suffix}"
    physical_catalog = f"{config.resource_prefix}_existing_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_existing_track_{suffix}"
    schema_name = "core"
    table_name = "events"
    cleanup_catalogs = [physical_catalog, tracking_catalog]

    ensure_project_file(workspace, provider_id="unity")
    write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )
    write_project_managed_scope(workspace, existing_catalogs=[logical_catalog])

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
        preseed = preseed_catalog_schema(
            executor,
            config,
            physical_catalog=physical_catalog,
            tracking_catalog=tracking_catalog,
            schema_name=schema_name,
            managed_root=managed_root,
            clear_changelog_in=workspace,
        )
        assert preseed.status in {"success", "partial"}, (
            f"Preseed failed: {preseed.status} {getattr(preseed, 'error_message', '')}"
        )

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
        assert table_exists(config, physical_catalog, schema_name, table_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
