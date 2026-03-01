"""
End-to-end and integration tests for all workflows in docs/WORKFLOWS.md.

Maps each documented situation to test classes:
- Situation 1: Greenfield single dev (init → ops → snapshot)
- Situation 2: Greenfield multi-dev (validate, snapshot validate, snapshot rebase)
- Situation 3: Brownfield rollback guard behavior (no DB)
- Situation 5: Diff, validate, SQL-only (no live DB)

Live command integration coverage (apply/import/rollback against Databricks) is
maintained in tests/integration/test_live_*.py and runs behind SCHEMAX_RUN_LIVE_*.
"""

from pathlib import Path

import pytest

from schemax.commands.diff import generate_diff
from schemax.commands.rollback import RollbackError, rollback_complete
from schemax.commands.sql import generate_sql_migration
from schemax.commands.validate import validate_project
from schemax.core.storage import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    read_project,
    write_project,
)
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli

# -----------------------------------------------------------------------------
# Situation 1: Greenfield (New Data Project) — Single Developer
# -----------------------------------------------------------------------------


@pytest.mark.integration
class TestWorkflowS1GreenfieldSingleDev:
    """Situation 1: Init → Ops → Snapshot → Apply (dev). See WORKFLOWS.md."""

    def test_e2e_init_ops_snapshot_validate_sql_diff(
        self, temp_workspace: Path, sample_operations: list
    ) -> None:
        """Full e2e: init → add ops → snapshot create → validate → sql → diff (no DB)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        # Design: add catalog, schema, table, column
        append_ops(temp_workspace, sample_operations)

        # Checkpoint: create snapshot
        project, snapshot = create_snapshot(
            temp_workspace,
            name="Initial schema",
            version="v0.1.0",
            comment="First version",
        )
        assert project["latestSnapshot"] == "v0.1.0"
        assert len(snapshot["operations"]) >= len(sample_operations)

        # Validate (no DB)
        assert validate_project(temp_workspace, json_output=False) is True

        # SQL-only: generate migration from changelog (empty after snapshot)
        # So add more ops and generate SQL (use a new column not already in rich fixture)
        append_ops(
            temp_workspace,
            [
                builder.column.add_column(
                    "col_023",
                    "table_789",
                    "extra_email",
                    "STRING",
                    nullable=True,
                    comment="Extra email",
                    op_id="op_005",
                )
            ],
        )
        sql_path = temp_workspace / "migration.sql"
        sql_str = generate_sql_migration(temp_workspace, output=sql_path)
        assert "ADD COLUMN" in sql_str or "extra_email" in sql_str
        assert sql_path.exists()

        # Second snapshot
        create_snapshot(temp_workspace, name="Add column", version="v0.2.0")

        # Diff between versions (no DB)
        diff_ops = generate_diff(temp_workspace, from_version="v0.1.0", to_version="v0.2.0")
        assert len(diff_ops) >= 1
        assert any(op.op == "unity.add_column" for op in diff_ops)


# -----------------------------------------------------------------------------
# Situation 2: Greenfield — Multi-Developer (validate, snapshot rebase)
# -----------------------------------------------------------------------------


@pytest.mark.integration
class TestWorkflowS2GreenfieldMultiDev:
    """Situation 2: Validate after merge, snapshot validate, snapshot rebase."""

    def test_validate_after_ops_succeeds(self, initialized_workspace, sample_operations) -> None:
        """Validate project after adding ops (simulates post-merge check)."""
        append_ops(initialized_workspace, sample_operations)
        assert validate_project(initialized_workspace, json_output=False) is True

    def test_snapshot_validate_no_stale_via_cli(
        self, initialized_workspace, sample_operations
    ) -> None:
        """Snapshot validate exits 0 when chain is consistent."""
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "First", version="v0.1.0")

        result = invoke_cli("snapshot", "validate", str(initialized_workspace))
        assert result.exit_code == 0

    def test_snapshot_rebase_workflow(self, initialized_workspace, sample_operations) -> None:
        """Snapshot rebase: create v0.1.0, v0.2.0, rebase v0.2.0 onto v0.1.0."""
        from schemax.commands.snapshot_rebase import rebase_snapshot

        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "Base", version="v0.1.0")

        builder = OperationBuilder()
        append_ops(
            initialized_workspace,
            [
                builder.column.add_column(
                    "col_extra",
                    "table_789",
                    "extra",
                    "STRING",
                    nullable=True,
                    comment="Extra",
                    op_id="op_extra",
                )
            ],
        )
        create_snapshot(initialized_workspace, "With extra column", version="v0.2.0")

        # Rebase v0.2.0 onto v0.1.0 (same base; should apply cleanly)
        result = rebase_snapshot(
            workspace=initialized_workspace,
            snapshot_version="v0.2.0",
            new_base_version="v0.1.0",
        )
        assert result.success is True
        assert result.conflict_count == 0


# -----------------------------------------------------------------------------
# Situation 3: Brownfield (import, adopt-baseline, rollback before baseline)
# -----------------------------------------------------------------------------


@pytest.mark.integration
class TestWorkflowS3Brownfield:
    """Situation 3: rollback before baseline blocked unless --force."""

    def test_rollback_before_baseline_blocked_without_force(
        self, initialized_workspace, sample_operations
    ) -> None:
        """Rollback to snapshot before import baseline raises unless --force."""
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "Pre-baseline", version="v0.0.5")
        append_ops(initialized_workspace, [])  # clear changelog
        create_snapshot(initialized_workspace, "Baseline", version="v0.1.0")

        project = read_project(initialized_workspace)
        project["provider"]["environments"]["dev"]["importBaselineSnapshot"] = "v0.1.0"
        write_project(initialized_workspace, project)

        with pytest.raises(RollbackError) as exc_info:
            rollback_complete(
                workspace=initialized_workspace,
                target_env="dev",
                to_snapshot="v0.0.5",
                profile="dev",
                warehouse_id="wh_123",
                force=False,
            )
        assert "v0.0.5" in str(exc_info.value)
        assert "baseline" in str(exc_info.value).lower()
        assert "force" in str(exc_info.value).lower()


# -----------------------------------------------------------------------------
# Situation 5: Diff, Validate, SQL-Only (No Live DB)
# -----------------------------------------------------------------------------


@pytest.mark.integration
class TestWorkflowS5DiffValidateSqlOnly:
    """Situation 5: diff, validate, sql — no Databricks connection required."""

    def test_validate_succeeds_no_db(self, initialized_workspace) -> None:
        """schemax validate succeeds without any DB."""
        result = invoke_cli("validate", str(initialized_workspace))
        assert result.exit_code == 0

    def test_sql_generates_file_no_db(self, initialized_workspace, sample_operations) -> None:
        """schemax sql --output FILE succeeds and writes SQL (no DB)."""
        append_ops(initialized_workspace, sample_operations)
        sql_file = initialized_workspace / "migration.sql"
        result = invoke_cli("sql", "--output", str(sql_file), str(initialized_workspace))
        assert result.exit_code == 0
        assert sql_file.exists()
        content = sql_file.read_text()
        assert (
            "CREATE CATALOG" in content or "CREATE TABLE" in content or "CREATE SCHEMA" in content
        )

    def test_diff_succeeds_no_db(self, initialized_workspace, sample_operations) -> None:
        """schemax diff --from X --to Y succeeds (no DB)."""
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "v1", version="v0.1.0")
        builder = OperationBuilder()
        append_ops(
            initialized_workspace,
            [
                builder.column.add_column(
                    "col_x", "table_789", "x", "STRING", True, "X", op_id="op_x"
                )
            ],
        )
        create_snapshot(initialized_workspace, "v2", version="v0.2.0")

        result = invoke_cli(
            "diff", "--from", "v0.1.0", "--to", "v0.2.0", str(initialized_workspace)
        )
        assert result.exit_code == 0
        assert "Diff generated" in result.output or "v0.1.0" in result.output

    def test_validate_project_api_no_db(self, initialized_workspace) -> None:
        """validate_project() API returns True for valid project (no DB)."""
        assert validate_project(initialized_workspace, json_output=False) is True

    def test_generate_sql_migration_api_no_db(
        self, initialized_workspace, sample_operations
    ) -> None:
        """generate_sql_migration() API returns SQL string (no DB)."""
        append_ops(initialized_workspace, sample_operations)
        sql_str = generate_sql_migration(initialized_workspace)
        assert isinstance(sql_str, str)
        assert "CREATE" in sql_str or sql_str.strip() == ""

    def test_generate_diff_api_no_db(self, initialized_workspace, sample_operations) -> None:
        """generate_diff() API returns list of ops (no DB)."""
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "v1", version="v0.1.0")
        builder = OperationBuilder()
        append_ops(
            initialized_workspace,
            [builder.column.add_column("c2", "table_789", "y", "INT", False, "Y", op_id="op_y")],
        )
        create_snapshot(initialized_workspace, "v2", version="v0.2.0")

        ops = generate_diff(initialized_workspace, from_version="v0.1.0", to_version="v0.2.0")
        assert isinstance(ops, list)
        assert len(ops) >= 1
        assert any(op.op == "unity.add_column" for op in ops)
