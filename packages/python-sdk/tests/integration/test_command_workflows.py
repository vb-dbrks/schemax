"""Deterministic command workflow integration tests."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from schematic.core.storage import append_ops, ensure_project_file
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
                builder.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.add_schema("schema_1", "core", "cat_1", op_id="op_2"),
                builder.add_table("table_1", "users", "schema_1", "delta", op_id="op_3"),
            ],
        )

        validate_result = invoke_cli("validate", str(temp_workspace))
        assert validate_result.exit_code == 0

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
            [builder.add_table("table_2", "orders", "schema_1", "delta", op_id="op_4")],
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

    def test_apply_and_rollback_cli_sequence_with_stubbed_execution(
        self,
        monkeypatch: pytest.MonkeyPatch,
        temp_workspace: Path,
    ) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")

        monkeypatch.setattr(
            "schematic.cli.apply_to_environment",
            lambda **kwargs: SimpleNamespace(status="success"),
        )
        monkeypatch.setattr(
            "schematic.cli.rollback_complete",
            lambda **kwargs: SimpleNamespace(
                success=True, operations_rolled_back=0, error_message=None
            ),
        )

        apply_result = invoke_cli(
            "apply",
            "--target",
            "dev",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh_123",
            "--dry-run",
            str(temp_workspace),
        )
        assert apply_result.exit_code == 0

        rollback_result = invoke_cli(
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh_123",
            "--dry-run",
            str(temp_workspace),
        )
        assert rollback_result.exit_code == 0
