"""Integration tests for command preflight/error paths (non-live)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


@pytest.mark.integration
def test_apply_json_fails_with_auth_error_non_live(temp_workspace: Path) -> None:
    """Apply should return a structured DB/auth error envelope in non-live runs."""
    ensure_project_file(temp_workspace, provider_id="unity")

    result = invoke_cli(
        "apply",
        "--target",
        "dev",
        "--profile",
        "dummy",
        "--warehouse-id",
        "dummy",
        "--no-interaction",
        "--json",
        str(temp_workspace),
    )

    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert envelope["command"] == "apply"
    assert envelope["status"] == "error"
    assert envelope["errors"][0]["code"] == "APPLY_FAILED"
    assert "Database query failed" in envelope["errors"][0]["message"]


@pytest.mark.integration
def test_apply_json_fails_for_missing_catalog_mapping(temp_workspace: Path) -> None:
    """Apply should fail with actionable mapping error before DB calls."""
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [
            builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1"),
            builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2"),
        ],
    )

    create_result = invoke_cli(
        "snapshot",
        "create",
        "--name",
        "Base",
        "--version",
        "v0.1.0",
        str(temp_workspace),
    )
    assert create_result.exit_code == 0, create_result.output

    result = invoke_cli(
        "apply",
        "--target",
        "dev",
        "--profile",
        "dummy",
        "--warehouse-id",
        "dummy",
        "--no-interaction",
        "--json",
        str(temp_workspace),
    )

    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert envelope["command"] == "apply"
    assert envelope["status"] == "error"
    assert envelope["errors"][0]["code"] == "APPLY_FAILED"
    assert "Missing catalog mapping(s)" in envelope["errors"][0]["message"]
    assert "bronze" in envelope["errors"][0]["message"]


@pytest.mark.integration
def test_snapshot_rebase_fails_for_root_snapshot(temp_workspace: Path) -> None:
    """Rebase should fail when snapshot has no previousSnapshot base."""
    ensure_project_file(temp_workspace, provider_id="unity")
    create_result = invoke_cli(
        "snapshot",
        "create",
        "--name",
        "Base",
        "--version",
        "v0.1.0",
        str(temp_workspace),
    )
    assert create_result.exit_code == 0, create_result.output

    result = invoke_cli("snapshot", "rebase", "v0.1.0", str(temp_workspace))
    assert result.exit_code == 1
    assert "has no previousSnapshot" in result.output


@pytest.mark.integration
def test_snapshot_rebase_noop_when_same_base(temp_workspace: Path) -> None:
    """Rebase returns success/no-op when new base matches existing base."""
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [builder.catalog.add_catalog("cat_1", "schemax_demo", op_id="op_1")],
    )
    snap_one = invoke_cli(
        "snapshot",
        "create",
        "--name",
        "S1",
        "--version",
        "v0.1.0",
        str(temp_workspace),
    )
    assert snap_one.exit_code == 0, snap_one.output

    append_ops(
        temp_workspace,
        [builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2")],
    )
    snap_two = invoke_cli(
        "snapshot",
        "create",
        "--name",
        "S2",
        "--version",
        "v0.2.0",
        str(temp_workspace),
    )
    assert snap_two.exit_code == 0, snap_two.output

    result = invoke_cli("snapshot", "rebase", "v0.2.0", "--base", "v0.1.0", str(temp_workspace))
    assert result.exit_code == 0
    assert "No rebase needed" in result.output
