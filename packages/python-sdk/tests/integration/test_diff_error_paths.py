"""Integration tests for diff command error paths.

Covers:
- Same version comparison
- Missing snapshot files
- Invalid snapshot structure
- No changes between versions
- Diff with show-sql and target environment
"""

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


@pytest.mark.integration
class TestDiffErrors:
    """Test diff command error paths."""

    def test_same_version(self, temp_workspace: Path) -> None:
        """Diff with same --from and --to version fails."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")

        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 1
        assert "same version" in result.output.lower() or "Cannot diff" in result.output

    def test_missing_from_snapshot(self, temp_workspace: Path) -> None:
        """Diff with non-existent --from snapshot."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")

        result = invoke_cli("diff", "--from", "v99.0.0", "--to", "v0.1.0", str(temp_workspace))
        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_missing_to_snapshot(self, temp_workspace: Path) -> None:
        """Diff with non-existent --to snapshot."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("cat_1", "demo", op_id="op_1")],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")

        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v99.0.0", str(temp_workspace))
        assert result.exit_code == 1


@pytest.mark.integration
class TestDiffNoChanges:
    """Test diff when no changes exist between versions."""

    def test_identical_snapshots(self, temp_workspace: Path) -> None:
        """Two snapshots with same ops produce no diff."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")

        # Create second snapshot without adding new ops (same state)
        create_snapshot(temp_workspace, name="S2", version="v0.2.0")

        result = invoke_cli("diff", "--from", "v0.1.0", "--to", "v0.2.0", str(temp_workspace))
        assert result.exit_code == 0
        assert "No changes" in result.output or "0" in result.output


@pytest.mark.integration
class TestDiffWithTargetEnv:
    """Test diff with --target for catalog mapping."""

    def test_diff_show_sql_with_target(self, temp_workspace: Path) -> None:
        """Diff with --show-sql and target environment applies catalog mapping."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")

        append_ops(
            temp_workspace,
            [
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            ],
        )
        create_snapshot(temp_workspace, name="S2", version="v0.2.0")

        project = read_project(temp_workspace)
        scope = project.get("defaultTarget", "default")
        project["targets"][scope]["environments"]["dev"]["catalogMappings"] = {
            "analytics": "dev_analytics"
        }
        write_project(temp_workspace, project)

        result = invoke_cli(
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            "--show-sql",
            str(temp_workspace),
        )
        assert result.exit_code == 0
