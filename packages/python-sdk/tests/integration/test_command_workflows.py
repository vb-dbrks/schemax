"""Deterministic command workflow integration tests."""

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


@pytest.mark.integration
class TestCommandWorkflows:
    def test_end_to_end_local_workflow(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2"),
                builder.table.add_table("table_1", "users", "schema_1", "delta", op_id="op_3"),
            ],
        )

        validate_result = invoke_cli("validate", str(temp_workspace))
        assert validate_result.exit_code == 0

        workspace_state_result = invoke_cli(
            "workspace-state",
            "--json",
            "--validate-dependencies",
            str(temp_workspace),
        )
        assert workspace_state_result.exit_code == 0
        workspace_state_envelope = json.loads(workspace_state_result.output)
        assert workspace_state_envelope["status"] == "success"
        workspace_state = workspace_state_envelope["data"]
        assert workspace_state["provider"]["id"] == "unity"
        assert len(workspace_state["changelog"]["ops"]) >= 3

        sql_file = temp_workspace / "migration.sql"
        sql_result = invoke_cli("sql", "--output", str(sql_file), str(temp_workspace))
        assert sql_result.exit_code == 0
        assert sql_file.exists()

        snapshot_1 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "First",
            "--version",
            "v0.1.0",
            str(temp_workspace),
        )
        assert snapshot_1.exit_code == 0

        append_ops(
            temp_workspace,
            [builder.table.add_table("table_2", "orders", "schema_1", "delta", op_id="op_4")],
        )
        snapshot_2 = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Second",
            "--version",
            "v0.2.0",
            str(temp_workspace),
        )
        assert snapshot_2.exit_code == 0

        diff_result = invoke_cli(
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            str(temp_workspace),
        )
        assert diff_result.exit_code == 0
        assert "Diff generated successfully" in diff_result.output
