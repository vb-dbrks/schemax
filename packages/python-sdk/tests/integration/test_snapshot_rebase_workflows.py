"""Integration tests for snapshot rebase via CLI.

Covers full rebase workflows using the CLI (no mocks).
Unit tests for rebase internals are in tests/unit/test_snapshot_rebase.py.
"""

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, read_project, write_project
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


@pytest.mark.integration
class TestRebaseFullWorkflow:
    def test_successful_rebase_via_cli(self, temp_workspace: Path) -> None:
        """Full rebase: create v0.1.0, v0.2.0, v0.3.0, rebase v0.3.0 onto v0.2.0."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        # v0.1.0
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "sales", op_id="op_1")],
        )
        assert (
            invoke_cli(
                "snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace)
            ).exit_code
            == 0
        )

        # v0.2.0
        append_ops(
            temp_workspace,
            [builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2")],
        )
        assert (
            invoke_cli(
                "snapshot", "create", "--name", "S2", "--version", "v0.2.0", str(temp_workspace)
            ).exit_code
            == 0
        )

        # v0.3.0 based on v0.2.0
        append_ops(
            temp_workspace,
            [builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3")],
        )
        assert (
            invoke_cli(
                "snapshot", "create", "--name", "S3", "--version", "v0.3.0", str(temp_workspace)
            ).exit_code
            == 0
        )

        # Rebase v0.3.0 onto v0.2.0 (should be no-op since already based on v0.2.0)
        result = invoke_cli("snapshot", "rebase", "v0.3.0", "--base", "v0.2.0", str(temp_workspace))
        assert result.exit_code == 0
        assert "No rebase needed" in result.output

    def test_rebase_onto_different_base(self, temp_workspace: Path) -> None:
        """Rebase where base actually changes (v0.3.0 based on v0.1.0 -> rebase onto v0.2.0)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        # v0.1.0
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "sales", op_id="op_1")],
        )
        assert (
            invoke_cli(
                "snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace)
            ).exit_code
            == 0
        )

        # v0.2.0 builds on v0.1.0
        append_ops(
            temp_workspace,
            [builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2")],
        )
        assert (
            invoke_cli(
                "snapshot", "create", "--name", "S2", "--version", "v0.2.0", str(temp_workspace)
            ).exit_code
            == 0
        )

        # Manually create a v0.3.0 snapshot based on v0.1.0 (simulating a branch)
        snap_v1 = json.loads(
            (temp_workspace / ".schemax" / "snapshots" / "v0.1.0.json").read_text()
        )
        snap_v3 = {
            **snap_v1,
            "id": "snap_v3",
            "version": "v0.3.0",
            "name": "S3",
            "previousSnapshot": "v0.1.0",
            "operations": [
                {
                    "id": "op_3",
                    "ts": "2026-01-03T00:00:00Z",
                    "provider": "unity",
                    "op": "unity.add_table",
                    "target": "t1",
                    "payload": {
                        "tableId": "t1",
                        "name": "users",
                        "schemaId": "s1",
                        "format": "delta",
                    },
                }
            ],
        }
        (temp_workspace / ".schemax" / "snapshots" / "v0.3.0.json").write_text(
            json.dumps(snap_v3, indent=2)
        )
        project = read_project(temp_workspace)
        project["snapshots"].append(
            {
                "id": "snap_v3",
                "version": "v0.3.0",
                "name": "S3",
                "ts": "2026-01-03T00:00:00Z",
                "createdBy": "test",
                "file": ".schemax/snapshots/v0.3.0.json",
                "previousSnapshot": "v0.1.0",
                "opsCount": 1,
            }
        )
        write_project(temp_workspace, project)

        # Rebase v0.3.0 onto v0.2.0
        result = invoke_cli("snapshot", "rebase", "v0.3.0", "--base", "v0.2.0", str(temp_workspace))
        assert result.exit_code == 0
        assert "Rebased" in result.output or "All operations applied" in result.output
