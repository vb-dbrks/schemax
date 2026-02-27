"""Live integration tests: failed apply and partial rollback (no patching)."""

import json
import re
import shutil
from pathlib import Path

import pytest

from schemax.core.storage import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_changelog_file_path,
    read_project,
)
from tests.integration.live_helpers import (
    make_suffix,
    make_workspace,
    preseed_tracking_only,
    write_project_env_overrides,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli
from tests.utils.live_databricks import (
    cleanup_objects,
    create_executor,
    require_live_command_tests,
)


def _add_bad_managed_location(workspace: Path) -> None:
    """Add a managed location with an invalid/inaccessible path so CREATE CATALOG fails at runtime.

    This simulates a genuine runtime failure real users can hit: wrong path, typo in storage
    account, or no access to the location. We do not use a syntax-error SQL hack.
    """
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    project["managedLocations"] = {
        "bad_loc": {
            "paths": {
                "dev": "abfss://fakecontainer@nonexistentstorage.dfs.core.windows.net/fake",
            },
        },
    }
    project_path.write_text(json.dumps(project, indent=2))


def _parse_deployment_id_from_apply_output(output: str) -> str | None:
    """Extract deployment ID from apply failure output (e.g. 'Deployment ID: deploy_abc123')."""
    match = re.search(r"Deployment ID:\s*(deploy_[a-f0-9]+)", output, re.IGNORECASE)
    return match.group(1) if match else None


@pytest.mark.integration
def test_live_failed_apply_then_partial_rollback(tmp_path: Path) -> None:
    """Live E2E: apply fails on second statement → partial rollback reverts the first (no mocks).

    Uses a genuine runtime failure: first CREATE CATALOG succeeds, second uses a managed
    location path that does not exist / is not accessible, so Databricks fails at execution
    (same class of error real users see: wrong path, typo, or no access). Then runs
    rollback --partial with the failed deployment ID and asserts success.
    Requires SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 and DATABRICKS_* env vars.
    """
    config = require_live_command_tests()
    suffix = make_suffix(config)
    workspace = make_workspace(tmp_path, "rollback_partial", suffix)

    tracking_catalog = f"{config.resource_prefix}_roll_track_{suffix}"
    physical_ok = f"{config.resource_prefix}_roll_ok_{suffix}"
    physical_bad = f"{config.resource_prefix}_roll_bad_{suffix}"
    cleanup_catalogs = [tracking_catalog, physical_ok, physical_bad]

    ensure_project_file(workspace, provider_id="unity")
    _add_bad_managed_location(workspace)
    write_project_env_overrides(
        workspace,
        top_level_name=tracking_catalog,
        catalog_mappings={"rollback_ok": physical_ok, "rollback_bad": physical_bad},
    )
    project = read_project(workspace)
    project["settings"]["catalogMode"] = "multi"
    (workspace / ".schemax" / "project.json").write_text(json.dumps(project, indent=2))

    # Changelog starts with implicit catalog op in single mode; clear and add only our two catalogs
    changelog_path = get_changelog_file_path(workspace)
    changelog = json.loads(changelog_path.read_text())
    changelog["ops"] = []
    changelog["sinceSnapshot"] = None
    changelog_path.write_text(json.dumps(changelog, indent=2))

    builder = OperationBuilder()
    append_ops(
        workspace,
        [
            builder.catalog.add_catalog("cat_ok", "rollback_ok", op_id="op_1"),
            builder.catalog.add_catalog(
                "cat_bad",
                "rollback_bad",
                managed_location_name="bad_loc",
                op_id="op_2",
            ),
        ],
    )
    create_snapshot(workspace, "v1", version="v0.1.0")

    try:
        executor = create_executor(config)
        managed_root = (
            f"{config.managed_location.rstrip('/')}/schemax-command-live/rollback-partial/{suffix}"
        )
        preseed_result = preseed_tracking_only(
            executor,
            config,
            tracking_catalog=tracking_catalog,
            managed_root=managed_root,
        )
        assert preseed_result.status == "success", (
            f"Preseed failed: {getattr(preseed_result, 'error_message', '')}"
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
        assert apply_result.exit_code != 0, (
            "Apply was expected to fail on second CREATE CATALOG (invalid managed location)"
        )
        deployment_id = _parse_deployment_id_from_apply_output(apply_result.output)
        assert deployment_id, (
            f"Could not parse deployment ID from apply output: {apply_result.output[:500]}"
        )

        rollback_result = invoke_cli(
            "rollback",
            "--deployment",
            deployment_id,
            "--partial",
            "--target",
            "dev",
            "--profile",
            config.profile,
            "--warehouse-id",
            config.warehouse_id,
            "--no-interaction",
            str(workspace),
        )
        assert rollback_result.exit_code == 0, rollback_result.output
    finally:
        executor = create_executor(config)
        cleanup_objects(executor, config, cleanup_catalogs)
        shutil.rmtree(workspace, ignore_errors=True)
