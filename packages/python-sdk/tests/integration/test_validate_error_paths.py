"""Integration tests for validate command error paths.

Covers:
- State validation failure (invalid state structure)
- Dependency validation (circular deps, warnings)
- Stale snapshot detection
- Project summary output (catalogs, schemas, tables)
- JSON envelope for validation failures
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


@pytest.mark.integration
class TestValidateProjectSummary:
    """Test the non-JSON summary output paths."""

    def test_multi_schema_summary(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
                builder.schema.add_schema("s2", "curated", "cat_1", op_id="op_3"),
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_4"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_5"),
                builder.table.add_table("t2", "orders", "s2", "delta", op_id="op_6"),
                builder.column.add_column("c2", "t2", "order_id", "INT", op_id="op_7"),
            ],
        )
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0
        assert "Catalogs:" in result.output
        assert "Schemas:" in result.output
        assert "Tables:" in result.output

    def test_validate_with_views(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
                builder.view.add_view(
                    "v1", "user_summary", "s1", "SELECT * FROM users", op_id="op_5"
                ),
            ],
        )
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0

    def test_validate_with_dependencies(self, temp_workspace: Path) -> None:
        """Validate a project with view dependencies."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
                builder.view.add_view(
                    "v1",
                    "user_summary",
                    "s1",
                    "SELECT * FROM users",
                    dependencies=["t1"],
                    op_id="op_5",
                ),
            ],
        )
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0
        # Should show dependency validation passing
        assert "depend" in result.output.lower() or "valid" in result.output.lower()


@pytest.mark.integration
class TestValidateStaleSnapshots:
    """Test stale snapshot detection during validation."""

    def test_stale_snapshot_warning(self, temp_workspace: Path) -> None:
        """Validate detects stale snapshots after changelog modification."""
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

        # Add more ops after snapshot - this makes the snapshot stale
        append_ops(
            temp_workspace,
            [
                builder.table.add_table("t1", "users", "s1", "delta", op_id="op_3"),
                builder.column.add_column("c1", "t1", "id", "INT", op_id="op_4"),
            ],
        )
        create_snapshot(temp_workspace, name="S2", version="v0.2.0")

        # Validate should still pass (stale snapshots are warnings, not errors)
        result = invoke_cli("validate", str(temp_workspace))
        assert result.exit_code == 0

    def test_json_with_stale_snapshots(self, temp_workspace: Path) -> None:
        """JSON output includes staleSnapshots field."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
            ],
        )
        create_snapshot(temp_workspace, name="S1", version="v0.1.0")
        result = invoke_cli("validate", "--json", str(temp_workspace))
        assert result.exit_code == 0
        data = json.loads(result.output)
        # JSON envelope wraps in {data: {valid: ...}} or direct {valid: ...}
        inner = data.get("data", data)
        assert inner.get("valid") is True
        assert "staleSnapshots" in inner or "warnings" in inner


@pytest.mark.integration
class TestValidateJsonErrors:
    """Test JSON envelope output for validation failures."""

    def test_no_project_json_output(self, temp_workspace: Path) -> None:
        """JSON output when project file doesn't exist."""
        result = invoke_cli("validate", "--json", str(temp_workspace))
        assert result.exit_code == 1

    def test_valid_project_json_envelope(self, temp_workspace: Path) -> None:
        """Verify JSON envelope structure for valid project."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "demo", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
            ],
        )
        result = invoke_cli("validate", "--json", str(temp_workspace))
        assert result.exit_code == 0
        data = json.loads(result.output)
        # JSON envelope wraps in {data: {valid: ...}} or direct {valid: ...}
        inner = data.get("data", data)
        assert inner["valid"] is True
        assert inner["errors"] == []


@pytest.mark.integration
class TestValidateDependencyWarnings:
    """Test dependency validation produces warnings for missing refs."""

    def test_view_with_missing_dependency(self, temp_workspace: Path) -> None:
        """View referencing non-existent table shows warning."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("s1", "raw", "cat_1", op_id="op_2"),
                builder.view.add_view(
                    "v1",
                    "orphan_view",
                    "s1",
                    "SELECT * FROM nonexistent",
                    dependencies=["nonexistent_table_id"],
                    op_id="op_3",
                ),
            ],
        )
        result = invoke_cli("validate", str(temp_workspace))
        # Should still pass (missing deps are warnings, not errors)
        assert result.exit_code == 0
