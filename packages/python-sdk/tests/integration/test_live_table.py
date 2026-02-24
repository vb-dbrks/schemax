"""Live integration tests: table and columns (apply + rollback)."""

import json
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, load_current_state
from tests.integration.live_helpers import (
    assert_schema_exists,
    make_suffix,
    make_workspace,
    preseed_catalog_schema,
    table_exists,
    write_project_env_overrides,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    cleanup_objects,
    create_executor,
    require_live_command_tests,
)


@pytest.mark.integration
def test_live_apply_and_rollback_non_dry_run(tmp_path: Path) -> None:
    """Live E2E: preseed catalog+schema, import adopt-baseline, add table+columns, apply, rollback."""
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "mutating", suffix)

    logical_catalog = f"logical_live_{suffix}"
    physical_catalog = f"{config.resource_prefix}_apply_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_{suffix}"
    schema_name = "core"
    table_name = "events"
    cleanup_catalogs = [physical_catalog, tracking_catalog]

    ensure_project_file(workspace, provider_id="unity")
    write_project_env_overrides(
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
        preseed = preseed_catalog_schema(
            executor,
            config,
            physical_catalog=physical_catalog,
            tracking_catalog=tracking_catalog,
            schema_name=schema_name,
            managed_root=managed_root,
            clear_changelog_in=workspace,
        )
        assert preseed.status in {"success", "partial"}

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
        assert_schema_exists(config, physical_catalog, schema_name)

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
                builder.table.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_{suffix}",
                ),
                builder.column.add_column(
                    col_id_id,
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_{suffix}",
                ),
                builder.column.add_column(
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
        assert table_exists(config, physical_catalog, schema_name, table_name)

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
        assert not table_exists(config, physical_catalog, schema_name, table_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
