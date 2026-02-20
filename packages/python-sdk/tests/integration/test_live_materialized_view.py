"""Live integration tests: materialized view (import + apply/rollback)."""

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
    materialized_view_exists,
    preseed_catalog_schema,
    table_exists,
    volume_exists,
    write_project_env_overrides,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    build_execution_config,
    cleanup_objects,
    create_executor,
    load_sql_fixture,
    require_live_command_tests,
)


def _seed_uc_objects_fixture(executor, config: object, catalog: str, managed_root: str) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1] / "resources" / "sql" / "unity_uc_objects_fixture.sql"
    )
    statements = load_sql_fixture(
        fixture_path,
        {"test_uc_objects": catalog, "__MANAGED_ROOT__": managed_root},
    )
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


@pytest.mark.integration
def test_live_import_sees_materialized_view(tmp_path: Path) -> None:
    """Live: Seed UC objects fixture → init → import → assert state has materialized view; validate and sql."""
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "uc_mv", suffix)
    catalog = f"test_uc_objects_{suffix}"
    schema_name = "uc_core"
    managed_root = f"{config.managed_location.rstrip('/')}/schemax-uc-objects-live/{suffix}"

    executor = create_executor(config)
    try:
        _seed_uc_objects_fixture(executor, config, catalog, managed_root)
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
        mvs = schema_uc.get("materialized_views", schema_uc.get("materializedViews", []))
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
def test_live_e2e_apply_materialized_view(tmp_path: Path) -> None:
    """Live E2E: Preseed → import adopt-baseline → add table + columns + MV → apply → rollback."""
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "apply_mv", suffix)
    logical_catalog = f"logical_mv_{suffix}"
    physical_catalog = f"{config.resource_prefix}_apply_mv_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_mv_{suffix}"
    schema_name = "core"
    table_name = "events"
    mv_name = "e2e_mv"
    cleanup_catalogs = [physical_catalog, tracking_catalog]

    ensure_project_file(workspace, provider_id="unity")
    write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={logical_catalog: physical_catalog},
    )
    builder = OperationBuilder()
    table_id = f"table_mv_{suffix}"
    mv_id = f"mv_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = f"{config.managed_location.rstrip('/')}/schemax-apply-mv-live/{suffix}"
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
        append_ops(
            workspace,
            [
                builder.add_table(
                    table_id,
                    table_name,
                    schema_id,
                    "delta",
                    op_id=f"op_table_mv_{suffix}",
                ),
                builder.add_column(
                    f"col_id_mv_{suffix}",
                    table_id,
                    "event_id",
                    "BIGINT",
                    nullable=False,
                    comment="Event id",
                    op_id=f"op_col_id_mv_{suffix}",
                ),
                builder.add_column(
                    f"col_val_mv_{suffix}",
                    table_id,
                    "event_type",
                    "STRING",
                    nullable=True,
                    comment="Event type",
                    op_id=f"op_col_val_mv_{suffix}",
                ),
                builder.add_materialized_view(
                    mv_id,
                    mv_name,
                    schema_id,
                    f"SELECT * FROM {table_name}",
                    comment="E2E MV",
                    op_id=f"op_mv_{suffix}",
                ),
            ],
        )
        snapshot_v2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "MV delta",
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
        assert table_exists(config, physical_catalog, schema_name, table_name)
        assert materialized_view_exists(config, physical_catalog, schema_name, mv_name)
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
        assert not materialized_view_exists(config, physical_catalog, schema_name, mv_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_e2e_apply_volume_function_materialized_view(tmp_path: Path) -> None:
    """Live E2E: Add table + volume + function + MV in one snapshot → apply → rollback (dependency order)."""
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "apply_uc", suffix)
    logical_catalog = f"logical_uc_{suffix}"
    physical_catalog = f"{config.resource_prefix}_apply_uc_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_track_uc_{suffix}"
    schema_name = "core"
    table_name = "events"
    volume_name = "e2e_vol"
    function_name = "e2e_func"
    mv_name = "e2e_mv"
    cleanup_catalogs = [physical_catalog, tracking_catalog]

    ensure_project_file(workspace, provider_id="unity")
    write_project_env_overrides(
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
        managed_root = f"{config.managed_location.rstrip('/')}/schemax-apply-uc-live/{suffix}"
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
        assert table_exists(config, physical_catalog, schema_name, table_name)
        assert volume_exists(config, physical_catalog, schema_name, volume_name)
        assert function_exists(config, physical_catalog, schema_name, function_name)
        assert materialized_view_exists(config, physical_catalog, schema_name, mv_name)
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
        assert not materialized_view_exists(config, physical_catalog, schema_name, mv_name)
        assert_schema_exists(config, physical_catalog, schema_name)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
