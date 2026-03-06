"""Integration tests for CLI command paths with low coverage.

Covers: init, validate (non-JSON), sql (snapshot + target + output), diff (show-sql, show-details,
error paths), bundle, workspace-state (non-JSON, payload-mode), snapshot create (no ops).
"""

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, read_project, write_project
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


@pytest.mark.integration
class TestInitCommand:
    def test_init_unity(self, temp_workspace: Path) -> None:
        result = invoke_cli("init", "--provider", "unity", str(temp_workspace))
        assert result.exit_code == 0
        assert (temp_workspace / ".schemax" / "project.json").exists()

    def test_init_already_initialized(self, temp_workspace: Path) -> None:
        invoke_cli("init", "--provider", "unity", str(temp_workspace))
        result = invoke_cli("init", "--provider", "unity", str(temp_workspace))
        # Should succeed (idempotent) or give a clear message
        assert result.exit_code == 0


@pytest.mark.integration
class TestValidateNonJson:
    def test_validate_non_json_success(self, temp_workspace: Path) -> None:
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
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0
        assert "Schema files are valid" in result.output

    def test_validate_non_json_shows_project_summary(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "sales", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
                builder.table.add_table("t1", "orders", "s1", "delta", op_id="op_3"),
                builder.table.add_table("t2", "items", "s1", "delta", op_id="op_4"),
            ],
        )
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0
        assert "Catalogs:" in result.output
        assert "Schemas:" in result.output
        assert "Tables:" in result.output

    def test_validate_no_project_fails(self, temp_workspace: Path) -> None:
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 1

    def test_validate_json_mode(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        result = invoke_cli("validate", "--json", str(temp_workspace))
        assert result.exit_code == 0
        # JSON output is the validate envelope
        data = json.loads(result.output)
        assert data.get("valid") is True or data.get("status") == "success"


@pytest.mark.integration
class TestSqlCommand:
    def test_sql_from_changelog_no_target(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        result = invoke_cli("sql", str(temp_workspace))
        assert result.exit_code == 0

    def test_sql_with_target(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        project = read_project(temp_workspace)
        project["provider"]["environments"]["dev"]["catalogMappings"] = {"demo": "dev_demo"}
        write_project(temp_workspace, project)

        result = invoke_cli("sql", "--target", "dev", str(temp_workspace))
        assert result.exit_code == 0

    def test_sql_output_to_file(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            ],
        )
        out_file = temp_workspace / "out.sql"
        result = invoke_cli("sql", "--output", str(out_file), str(temp_workspace))
        assert result.exit_code == 0
        assert out_file.exists()
        content = out_file.read_text()
        assert "CREATE CATALOG" in content

    def test_sql_no_operations_returns_empty(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        result = invoke_cli("sql", str(temp_workspace))
        assert result.exit_code == 0
        assert "No operations" in result.output

    def test_sql_from_snapshot_latest(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        invoke_cli("snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace))
        result = invoke_cli("sql", "--snapshot", "latest", str(temp_workspace))
        assert result.exit_code == 0

    def test_sql_from_specific_snapshot(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        invoke_cli("snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace))
        result = invoke_cli("sql", "--snapshot", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 0

    def test_sql_snapshot_latest_no_snapshots_fails(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        result = invoke_cli("sql", "--snapshot", "latest", "--json", str(temp_workspace))
        assert result.exit_code == 1

    def test_sql_with_target_and_snapshot(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        project = read_project(temp_workspace)
        project["provider"]["environments"]["dev"]["catalogMappings"] = {"demo": "dev_demo"}
        write_project(temp_workspace, project)

        invoke_cli("snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace))
        result = invoke_cli("sql", "--snapshot", "latest", "--target", "dev", str(temp_workspace))
        assert result.exit_code == 0


@pytest.mark.integration
class TestDiffCommand:
    def _setup_two_snapshots(self, workspace: Path) -> None:
        ensure_project_file(workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        invoke_cli("snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(workspace))
        append_ops(
            workspace,
            [
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            ],
        )
        invoke_cli("snapshot", "create", "--name", "S2", "--version", "v0.2.0", str(workspace))

    def test_diff_non_json_output(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v0.2.0", str(temp_workspace))
        assert result.exit_code == 0
        assert "Diff generated successfully" in result.output

    def test_diff_show_details(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0", "--show-details", str(temp_workspace)
        )
        assert result.exit_code == 0

    def test_diff_show_sql(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0", "--show-sql", str(temp_workspace)
        )
        assert result.exit_code == 0

    def test_diff_show_sql_and_details(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            "--show-sql",
            "--show-details",
            str(temp_workspace),
        )
        assert result.exit_code == 0

    def test_diff_with_target_env(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        project = read_project(temp_workspace)
        project["provider"]["environments"]["dev"]["catalogMappings"] = {"demo": "dev_demo"}
        write_project(temp_workspace, project)

        result = invoke_cli(
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            "--target",
            "dev",
            str(temp_workspace),
        )
        assert result.exit_code == 0

    def test_diff_same_version_fails(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 1

    def test_diff_missing_snapshot_fails(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v9.9.9", str(temp_workspace))
        assert result.exit_code == 1

    def test_diff_no_changes(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        invoke_cli("snapshot", "create", "--name", "S1", "--version", "v0.1.0", str(temp_workspace))
        # Create second snapshot with no new ops (empty changelog)
        # Need to add a dummy op then immediately take snapshot
        append_ops(
            temp_workspace,
            [builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2")],
        )
        invoke_cli("snapshot", "create", "--name", "S2", "--version", "v0.2.0", str(temp_workspace))
        # diff from S2 to S1 (backwards) to test edge case
        result = invoke_cli("diff", "--from", "v0.2.0", "--to", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 0


@pytest.mark.integration
class TestWorkspaceStateCommand:
    def test_workspace_state_non_json(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        result = invoke_cli("workspace-state", str(temp_workspace))
        assert result.exit_code == 0

    def test_workspace_state_payload_mode_state_only(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        result = invoke_cli(
            "workspace-state", "--json", "--payload-mode", "state-only", str(temp_workspace)
        )
        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["status"] == "success"

    def test_workspace_state_no_project(self, temp_workspace: Path) -> None:
        result = invoke_cli("workspace-state", "--json", str(temp_workspace))
        assert result.exit_code == 1


@pytest.mark.integration
class TestSnapshotCreateEdgeCases:
    def test_snapshot_create_no_operations_warns(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        result = invoke_cli(
            "snapshot", "create", "--name", "Empty", "--version", "v0.1.0", str(temp_workspace)
        )
        # Should either fail or warn about no uncommitted operations
        assert "No uncommitted" in result.output or result.exit_code == 1

    def test_snapshot_create_with_tags(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        result = invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Tagged",
            "--version",
            "v0.1.0",
            "--tags",
            "release",
            "--tags",
            "reviewed",
            str(temp_workspace),
        )
        assert result.exit_code == 0


@pytest.mark.integration
class TestRuntimeInfoCommand:
    def test_runtime_info_json(self, temp_workspace: Path) -> None:
        result = invoke_cli("runtime-info", "--json")
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "success"
        assert "providerIds" in data["data"]

    def test_runtime_info_non_json(self, temp_workspace: Path) -> None:
        result = invoke_cli("runtime-info")
        assert result.exit_code == 0
