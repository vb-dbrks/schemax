"""Live integration tests: scope (managedCategories, governance) and multi-env (promote, rollback)."""

import json
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, load_current_state
from tests.integration.live_helpers import (
    make_suffix,
    make_workspace,
    preseed_catalog_schema,
    table_exists,
    write_project_env_overrides,
    write_project_managed_scope,
    write_project_promote_envs,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    build_execution_config,
    cleanup_objects,
    create_executor,
    require_live_command_tests,
)


@pytest.mark.integration
def test_live_apply_governance_only(tmp_path: Path) -> None:
    """Live E2E: managedCategories = ['governance'] → apply runs only governance SQL (e.g. COMMENT, GRANT).

    Preseed catalog/schema/table, import adopt-baseline, add table+columns and apply (v0.2.0).
    Then add set_table_comment, set managedCategories to ['governance'], snapshot v0.3.0, apply.
    Apply should only execute governance DDL (no CREATE CATALOG/SCHEMA/TABLE).
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "governance_only", suffix)

    logical_catalog = f"logical_gov_{suffix}"
    physical_catalog = f"{config.resource_prefix}_gov_{suffix}"
    tracking_catalog = f"{config.resource_prefix}_gov_track_{suffix}"
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
    table_id = f"table_gov_{suffix}"
    col_id_id = f"col_id_gov_{suffix}"
    col_val_id = f"col_val_gov_{suffix}"

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/governance-only/{suffix}"
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
        assert table_exists(config, physical_catalog, schema_name, table_name)

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
        write_project_managed_scope(workspace, managed_categories=["governance"])

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
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.integration
def test_live_greenfield_promote_dev_test_prod_then_rollback_prod(tmp_path: Path) -> None:
    """Live E2E: greenfield project → apply to dev → test → prod → add change → promote again → rollback prod only.

    Creates a new dev greenfield SchemaX project (catalog/schema/table), publishes to dev, then test, then prod.
    Adds a second table, promotes to all three, then rollbacks only prod to previous snapshot.
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "promote", suffix)

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
    ) = write_project_promote_envs(
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

        assert table_exists(config, physical_dev, schema_name, table_users)
        assert table_exists(config, physical_test, schema_name, table_users)
        assert table_exists(config, physical_prod, schema_name, table_users)

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

        assert table_exists(config, physical_dev, schema_name, table_orders)
        assert table_exists(config, physical_test, schema_name, table_orders)
        assert table_exists(config, physical_prod, schema_name, table_orders)

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

        assert table_exists(config, physical_dev, schema_name, table_orders)
        assert table_exists(config, physical_test, schema_name, table_orders)
        assert not table_exists(config, physical_prod, schema_name, table_orders)
        assert table_exists(config, physical_prod, schema_name, table_users)
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, all_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
