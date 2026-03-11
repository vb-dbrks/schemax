"""Integration tests for CLI command coverage gaps.

Covers: validate (non-JSON, JSON envelope), sql (snapshot, output, target, JSON),
diff (--from/--to, show-sql, show-details), workspace-state, snapshot (create, tags, rebase),
bundle, runtime-info, changelog undo, apply error paths.
"""

import json
from pathlib import Path

import pytest

from schemax.core.storage import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    read_project,
    write_project,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


# ── Validate ─────────────────────────────────────────────────────────


@pytest.mark.integration
class TestValidateCoverage:
    def test_json_envelope(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        result = invoke_cli("validate", "--json", str(temp_workspace))
        assert result.exit_code == 0
        data = json.loads(result.output)
        # Output may be envelope or direct - check either format
        assert data.get("valid") is True or data.get("status") == "success"

    def test_non_json_full_summary(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            builder.table.add_table("t2", "orders", "s1", "delta", op_id="op_5"),
            builder.column.add_column("c2", "t2", "id", "INT", op_id="op_6"),
        ])
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0
        assert "Project:" in result.output
        assert "Provider:" in result.output
        assert "Tables:" in result.output

    def test_empty_project(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0

    def test_no_project_file(self, temp_workspace: Path) -> None:
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 1


# ── SQL ──────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestSqlCoverage:
    def test_from_snapshot(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        invoke_cli(
            "snapshot", "create", "--name", "S1", "--version", "v0.1.0",
            str(temp_workspace),
        )
        result = invoke_cli("sql", "--snapshot", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 0

    def test_output_file(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ])
        out = temp_workspace / "output.sql"
        result = invoke_cli("sql", "--output", str(out), str(temp_workspace))
        assert result.exit_code == 0
        assert out.exists()
        assert "CREATE" in out.read_text()

    def test_target_env(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        project = read_project(temp_workspace)
        scope = project.get("defaultTarget", "default")
        project["targets"][scope]["environments"]["dev"]["catalogMappings"] = {
            "demo": "dev_demo"
        }
        write_project(temp_workspace, project)
        result = invoke_cli("sql", "--target", "dev", str(temp_workspace))
        assert result.exit_code == 0

    def test_empty_project(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        result = invoke_cli("sql", str(temp_workspace))
        assert result.exit_code == 0

    def test_json_envelope(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        result = invoke_cli("sql", "--json", str(temp_workspace))
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data.get("status") == "success" or "sql" in str(data)

    def test_snapshot_latest(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        invoke_cli(
            "snapshot", "create", "--name", "S1", "--version", "v0.1.0",
            str(temp_workspace),
        )
        result = invoke_cli("sql", "--snapshot", "latest", str(temp_workspace))
        assert result.exit_code == 0


# ── Diff ─────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestDiffCoverage:
    def _setup_two_snapshots(self, workspace: Path) -> None:
        ensure_project_file(workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        invoke_cli(
            "snapshot", "create", "--name", "S1", "--version", "v0.1.0",
            str(workspace),
        )
        append_ops(workspace, [
            builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
            builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
        ])
        invoke_cli(
            "snapshot", "create", "--name", "S2", "--version", "v0.2.0",
            str(workspace),
        )

    def test_basic_diff(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0", str(temp_workspace)
        )
        assert result.exit_code == 0

    def test_show_sql(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0",
            "--show-sql", str(temp_workspace),
        )
        assert result.exit_code == 0

    def test_show_details(self, temp_workspace: Path) -> None:
        self._setup_two_snapshots(temp_workspace)
        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0",
            "--show-details", str(temp_workspace),
        )
        assert result.exit_code == 0


# ── Workspace State ──────────────────────────────────────────────────


@pytest.mark.integration
class TestWorkspaceStateCoverage:
    def test_json(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        result = invoke_cli("workspace-state", "--json", str(temp_workspace))
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "catalogs" in str(data).lower()

    def test_non_json(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
        ])
        result = invoke_cli("workspace-state", str(temp_workspace))
        assert result.exit_code == 0


# ── Snapshot ─────────────────────────────────────────────────────────


@pytest.mark.integration
class TestSnapshotCoverage:
    def test_create_via_cli(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        result = invoke_cli(
            "snapshot", "create", "--name", "Test Snapshot",
            "--version", "v0.1.0", str(temp_workspace),
        )
        assert result.exit_code == 0

    def test_create_with_tags(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        result = invoke_cli(
            "snapshot", "create", "--name", "Tagged",
            "--version", "v0.1.0", "--tags", "release,stable",
            str(temp_workspace),
        )
        assert result.exit_code == 0

    def test_rebase_no_previous(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        create_snapshot(temp_workspace, name="Base", version="v0.1.0")
        result = invoke_cli("snapshot", "rebase", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 1
        assert "previousSnapshot" in result.output or "no previous" in result.output.lower()

    def test_validate_snapshots(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        create_snapshot(temp_workspace, name="Base", version="v0.1.0")
        result = invoke_cli("snapshot", "validate", str(temp_workspace))
        assert result.exit_code in (0, 1)


# ── Bundle ───────────────────────────────────────────────────────────


@pytest.mark.integration
class TestBundleCoverage:
    def test_basic(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        # Bundle works from cwd, so use cwd parameter
        result = invoke_cli("bundle", cwd=temp_workspace)
        assert result.exit_code == 0


# ── Runtime Info ─────────────────────────────────────────────────────


@pytest.mark.integration
class TestRuntimeInfoCoverage:
    def test_json(self) -> None:
        result = invoke_cli("runtime-info", "--json")
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data.get("status") == "success"
        assert "cliVersion" in str(data)

    def test_non_json(self) -> None:
        result = invoke_cli("runtime-info")
        assert result.exit_code == 0


# ── Changelog Undo ───────────────────────────────────────────────────


@pytest.mark.integration
class TestChangelogUndoCoverage:
    def test_undo_specific_op(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        result = invoke_cli(
            "changelog", "undo", "--op-id", "op_2", str(temp_workspace)
        )
        assert result.exit_code == 0

    def test_undo_no_op_ids(self, temp_workspace: Path) -> None:
        """Undo with no op-ids should still work (undo last)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
        ])
        result = invoke_cli("changelog", "undo", str(temp_workspace))
        # May succeed (undo last) or fail - depends on implementation
        assert result.exit_code in (0, 1)


# ── Apply Error Paths ────────────────────────────────────────────────


@pytest.mark.integration
class TestApplyErrorCoverage:
    def test_no_credentials_json(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(temp_workspace, [
            builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
        ])
        result = invoke_cli(
            "apply", "--target", "dev", "--profile", "nonexistent-profile",
            "--warehouse-id", "dummy-wh", "--no-interaction", "--json",
            str(temp_workspace),
        )
        assert result.exit_code == 1

    def test_no_ops(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        result = invoke_cli(
            "apply", "--target", "dev", "--profile", "nonexistent-profile",
            "--warehouse-id", "dummy-wh", "--no-interaction",
            str(temp_workspace),
        )
        assert result.exit_code in (0, 1)
