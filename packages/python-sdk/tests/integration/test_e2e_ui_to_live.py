"""E2E tests: UI-equivalent state → CLI → SDK → live environment.

These tests validate the full pipeline:

   UI (Designer) → .schemax/ state → CLI (schemax apply/rollback) → SDK → live Databricks

We do not automate the VS Code webview. Instead we build the same state the UI would
produce: project.json, changelog.json with ops in the same shape the extension writes
(using OperationBuilder, append_ops). Then we run the CLI against a live workspace and
assert objects exist after apply and are gone after rollback.

So: UI-equivalent state → CLI → SDK → live env is tested end-to-end.
"""

import json
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, load_current_state
from tests.integration.live_helpers import (
    assert_schema_exists,
    function_exists,
    make_suffix,
    make_workspace,
    preseed_catalog_schema,
    table_exists,
    volume_exists,
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
def test_e2e_ui_equivalent_state_to_cli_to_live_apply_rollback(tmp_path: Path) -> None:
    """E2E: UI-equivalent changelog (table + volume + function) → CLI apply → live env → rollback.

    Builds .schemax state as the Designer would (same op shapes via OperationBuilder),
    runs schemax apply, asserts table/volume/function exist in live Databricks, then
    rollback and asserts they are removed. Validates the full UI → CLI → SDK → live pipeline.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "e2e_ui_live", suffix)
    logical_catalog = f"logical_e2e_{suffix}"
    physical_catalog = f"{config.resource_prefix}_e2e_ui_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_e2e_ui_{suffix}"
    schema_name = "core"
    table_name = "e2e_table"
    volume_name = "e2e_vol"
    function_name = "e2e_func"
    cleanup_catalogs = [physical_catalog, tracking_catalog]

    ensure_project_file(workspace, provider_id="unity")
    write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )
    builder = OperationBuilder()
    table_id = f"table_{suffix}"
    vol_id = f"vol_{suffix}"
    func_id = f"func_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = f"{config.managed_location.rstrip('/')}/schemax-e2e-ui-live/{suffix}"
        preseed = preseed_catalog_schema(
            executor,
            config,
            physical_catalog=physical_catalog,
            tracking_catalog=tracking_catalog,
            schema_name=schema_name,
            managed_root=managed_root,
            clear_changelog_in=workspace,
        )
        assert preseed.status in {"success", "partial"}, f"Preseed failed: {preseed.status}"

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
        baseline_version = json.loads((workspace / ".schemax" / "project.json").read_text()).get(
            "latestSnapshot"
        )
        assert baseline_version, "No baseline snapshot after import adoption"

        state, _, _, _ = load_current_state(workspace, validate=False)
        catalog_state = next(c for c in state["catalogs"] if c.get("name") == logical_catalog)
        schema_state = next(
            s for s in catalog_state.get("schemas", []) if s.get("name") == schema_name
        )
        schema_id = schema_state["id"]

        # Same op shapes the UI would emit (Designer → append-ops → changelog.json)
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
                    f"col_id_{suffix}",
                    table_id,
                    "id",
                    "BIGINT",
                    nullable=False,
                    op_id=f"op_col_id_{suffix}",
                ),
                builder.add_volume(
                    vol_id,
                    volume_name,
                    schema_id,
                    "managed",
                    comment="E2E volume",
                    op_id=f"op_vol_{suffix}",
                ),
                builder.add_function(
                    func_id,
                    function_name,
                    schema_id,
                    "SQL",
                    "INT",
                    "1",
                    comment="E2E function",
                    op_id=f"op_func_{suffix}",
                ),
            ],
        )
        assert (
            invoke_cli(
                "snapshot",
                "create",
                "--name",
                "E2E UI delta",
                "--version",
                "v0.2.0",
                str(workspace),
            ).exit_code
            == 0
        )

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
        assert volume_exists(config, physical_catalog, schema_name, volume_name)
        assert function_exists(config, physical_catalog, schema_name, function_name)

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
        assert not volume_exists(config, physical_catalog, schema_name, volume_name)
        assert not function_exists(config, physical_catalog, schema_name, function_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
