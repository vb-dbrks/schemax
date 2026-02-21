"""Live E2E: full CLI command matrix (seed → init → import → snapshot → validate → sql → diff → apply dry-run → rollback dry-run → snapshot validate → bundle)."""

import json
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import append_ops
from tests.integration.live_helpers import make_suffix, make_workspace
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    build_execution_config,
    cleanup_objects,
    create_executor,
    load_sql_fixture,
    require_live_command_tests,
)


@pytest.mark.integration
def test_live_command_matrix(tmp_path: Path) -> None:
    """Full CLI sweep: seed from SQL fixture, init, import, snapshot, validate, sql, diff, apply dry-run, rollback dry-run, snapshot validate, bundle."""
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "cmd_matrix", suffix)

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
