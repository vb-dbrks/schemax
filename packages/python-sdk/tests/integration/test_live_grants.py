"""Live integration tests: grants on table (apply + rollback)."""

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
def test_live_e2e_create_apply_rollback_with_grants(tmp_path: Path) -> None:
    """Live E2E: create catalog/schema/table → apply → add grants → apply → rollback (real Databricks).

    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "e2e_grants", suffix)

    logical_catalog = f"logical_e2e_{suffix}"
    physical_catalog = f"{config.resource_prefix}_e2e_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_e2e_track_{suffix}"
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
    table_id = f"table_e2e_{suffix}"
    col_id_id = f"col_id_e2e_{suffix}"
    col_val_id = f"col_val_e2e_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/e2e-grants/{suffix}"
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
        assert table_exists(config, physical_catalog, schema_name, table_name)

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
        assert not table_exists(config, physical_catalog, schema_name, table_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
