"""
End-to-end and integration tests for all workflows in docs/WORKFLOWS.md.

Maps each documented situation to test classes:
- Situation 1: Greenfield single dev (init → ops → snapshot → apply)
- Situation 2: Greenfield multi-dev (validate, snapshot validate, snapshot rebase)
- Situation 3: Brownfield (import, adopt-baseline, rollback before baseline guard)
- Situation 4: Apply failure and rollback (partial/complete rollback)
- Situation 5: Diff, validate, SQL-only (no live DB)
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
        # So add more ops and generate SQL
        append_ops(
            temp_workspace,
            [
                builder.add_column(
                    "col_002",
                    "table_789",
                    "email",
                    "STRING",
                    nullable=True,
                    comment="Email",
                    op_id="op_005",
                )
            ],
        )
        sql_path = temp_workspace / "migration.sql"
        sql_str = generate_sql_migration(temp_workspace, output=sql_path)
        assert "ADD COLUMN" in sql_str or "email" in sql_str
        assert sql_path.exists()

        # Second snapshot
        create_snapshot(temp_workspace, name="Add email", version="v0.2.0")

        # Diff between versions (no DB)
        diff_ops = generate_diff(temp_workspace, from_version="v0.1.0", to_version="v0.2.0")
        assert len(diff_ops) >= 1
        assert any(op.op == "unity.add_column" for op in diff_ops)

    def test_e2e_apply_dry_run_via_cli(self, temp_workspace: Path) -> None:
        """Apply dry-run via CLI (stubbed execution)."""
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

        with patch(
            "schemax.cli.apply_to_environment",
            return_value=SimpleNamespace(status="success"),
        ):
            result = invoke_cli(
                "apply",
                "--target",
                "dev",
                "--profile",
                "dev",
                "--warehouse-id",
                "wh_123",
                "--dry-run",
                "--no-interaction",
                str(temp_workspace),
            )
        assert result.exit_code == 0


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
                builder.add_column(
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
    """Situation 3: Import → adopt-baseline → normal flow; rollback before baseline blocked."""

    def test_import_adopt_baseline_stores_baseline(
        self, monkeypatch, initialized_workspace
    ) -> None:
        """Import with adopt-baseline stores importBaselineSnapshot per env."""
        from schemax.commands.import_assets import import_from_provider
        from schemax.core.storage import load_current_state
        from schemax.providers import ProviderRegistry

        state, _, _, _ = load_current_state(initialized_workspace, validate=False)
        provider = ProviderRegistry.get("unity")
        assert provider is not None

        monkeypatch.setattr(provider, "discover_state", lambda config, scope: state)
        monkeypatch.setattr(
            "schemax.providers.unity.provider.create_databricks_client",
            lambda _: object(),
        )

        class FakeTracker:
            def __init__(self, client, catalog: str, warehouse_id: str):
                pass

            def ensure_tracking_schema(self, auto_create: bool = True) -> None:
                pass

            def get_latest_deployment(self, environment: str):
                return None

            def get_most_recent_deployment_id(self, environment: str):
                return None

            def start_deployment(self, *args, **kwargs) -> None:
                pass

            def complete_deployment(self, *args, **kwargs) -> None:
                pass

        monkeypatch.setattr("schemax.core.deployment.DeploymentTracker", FakeTracker)

        summary = import_from_provider(
            workspace=initialized_workspace,
            target_env="dev",
            profile="",
            warehouse_id="wh_123",
            dry_run=False,
            adopt_baseline=True,
        )
        assert summary["adopt_baseline"] is True
        assert summary["snapshot_version"]

        project = read_project(initialized_workspace)
        dev_env = project["provider"]["environments"].get("dev", {})
        assert dev_env.get("importBaselineSnapshot") == summary["snapshot_version"]

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

        with patch("schemax.commands.rollback.get_environment_config") as mock_cfg:
            mock_cfg.return_value = {
                "topLevelName": "dev_catalog",
                "importBaselineSnapshot": "v0.1.0",
            }
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

    def test_rollback_before_baseline_with_force_and_no_interaction(
        self, initialized_workspace, sample_operations
    ) -> None:
        """Rollback before baseline with --force and --no-interaction proceeds."""
        append_ops(initialized_workspace, sample_operations)
        create_snapshot(initialized_workspace, "Pre", version="v0.0.5")
        create_snapshot(initialized_workspace, "Base", version="v0.1.0")

        project = read_project(initialized_workspace)
        project["provider"]["environments"]["dev"]["importBaselineSnapshot"] = "v0.1.0"
        write_project(initialized_workspace, project)

        mock_differ = Mock()
        mock_differ.generate_diff_operations.return_value = []
        mock_provider = Mock()
        mock_provider.get_state_differ.return_value = mock_differ

        with patch("schemax.commands.rollback.read_project") as mock_read:
            mock_read.return_value = project
        with patch("schemax.commands.rollback.get_environment_config") as mock_cfg:
            mock_cfg.return_value = {
                "topLevelName": "dev_catalog",
                "importBaselineSnapshot": "v0.1.0",
            }
        with patch("schemax.core.storage.read_snapshot") as mock_snap:
            mock_snap.return_value = {
                "version": "v0.0.5",
                "state": {"catalogs": []},
                "operations": [],
            }
        with patch("schemax.providers.unity.auth.create_databricks_client"):
            with patch("schemax.commands.rollback.DeploymentTracker") as mock_tracker_cls:
                tracker = Mock()
                mock_tracker_cls.return_value = tracker
                tracker.get_latest_deployment.return_value = None
            with patch("schemax.commands.rollback.load_current_state") as mock_load:
                mock_load.return_value = (
                    {"catalogs": []},
                    {},
                    mock_provider,
                    None,
                )
            with patch("schemax.commands.diff._build_catalog_mapping") as mock_map:
                mock_map.return_value = {"__implicit__": "dev_catalog"}
                result = rollback_complete(
                    workspace=initialized_workspace,
                    target_env="dev",
                    to_snapshot="v0.0.5",
                    profile="dev",
                    warehouse_id="wh_123",
                    force=True,
                    no_interaction=True,
                    dry_run=True,
                )
        assert result.success is True


# -----------------------------------------------------------------------------
# Situation 4: Apply Failure and Rollback
# -----------------------------------------------------------------------------


@pytest.mark.integration
class TestWorkflowS4ApplyFailureAndRollback:
    """Situation 4: Partial rollback (failed deployment) and complete rollback."""

    def test_apply_dry_run_then_rollback_dry_run_via_cli(self, temp_workspace: Path) -> None:
        """Apply dry-run then rollback dry-run via CLI (stubbed)."""
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
        create_snapshot(temp_workspace, "v1", version="v0.1.0")

        with patch(
            "schemax.cli.apply_to_environment",
            return_value=SimpleNamespace(status="success"),
        ):
            r1 = invoke_cli(
                "apply",
                "--target",
                "dev",
                "--profile",
                "dev",
                "--warehouse-id",
                "wh_123",
                "--dry-run",
                "--no-interaction",
                str(temp_workspace),
            )
        assert r1.exit_code == 0

        with patch("schemax.cli.rollback_complete") as mock_rollback:
            mock_rollback.return_value = SimpleNamespace(
                success=True, operations_rolled_back=0, error_message=None
            )
            r2 = invoke_cli(
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
                "--no-interaction",
                str(temp_workspace),
            )
        assert r2.exit_code == 0
        mock_rollback.assert_called_once()

    def test_partial_rollback_cli_stub(self, temp_workspace: Path) -> None:
        """Partial rollback CLI (stubbed DB and rollback_partial) exits 0."""
        ensure_project_file(temp_workspace, provider_id="unity")
        # Snapshot with only implicit catalog so catalogMappings (__implicit__ only) suffice
        create_snapshot(temp_workspace, "v1", version="v0.1.0")

        fake_deployment = {
            "id": "deploy_abc",
            "status": "failed",
            "version": "v0.1.0",
            "fromVersion": None,
            "opsApplied": ["op_init_catalog"],
            "failedStatementIndex": 1,
            "opsDetails": [
                {
                    "id": "op_init_catalog",
                    "type": "unity.add_catalog",
                    "target": "cat_implicit",
                    "payload": {"catalogId": "cat_implicit", "name": "__implicit__"},
                },
            ],
        }
        mock_tracker = Mock()
        mock_tracker.get_deployment_by_id.return_value = fake_deployment

        with patch(
            "schemax.providers.unity.auth.create_databricks_client",
            return_value=Mock(),
        ):
            with patch(
                "schemax.core.deployment.DeploymentTracker",
                return_value=mock_tracker,
            ):
                with patch("schemax.commands.rollback.rollback_partial") as mock_partial:
                    mock_partial.return_value = SimpleNamespace(
                        success=True, operations_rolled_back=2, error_message=None
                    )
                    result = invoke_cli(
                        "rollback",
                        "--deployment",
                        "deploy_abc",
                        "--partial",
                        "--target",
                        "dev",
                        "--profile",
                        "dev",
                        "--warehouse-id",
                        "wh_123",
                        "--dry-run",
                        "--no-interaction",
                        str(temp_workspace),
                    )
        assert result.exit_code == 0
        mock_partial.assert_called_once()


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
            [builder.add_column("col_x", "table_789", "x", "STRING", True, "X", op_id="op_x")],
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
            [builder.add_column("c2", "table_789", "y", "INT", False, "Y", op_id="op_y")],
        )
        create_snapshot(initialized_workspace, "v2", version="v0.2.0")

        ops = generate_diff(initialized_workspace, from_version="v0.1.0", to_version="v0.2.0")
        assert isinstance(ops, list)
        assert len(ops) >= 1
        assert any(op.op == "unity.add_column" for op in ops)


# -----------------------------------------------------------------------------
# E2E: Dev → Test → Prod apply and rollback (stubbed execution)
# -----------------------------------------------------------------------------


def _ensure_catalog_mappings_for_logical_catalog(
    workspace: Path, logical_catalog: str = "bronze"
) -> None:
    """Add logical catalog name to each env's catalogMappings so apply/diff work."""
    project = read_project(workspace)
    envs = project["provider"]["environments"]
    name = project.get("name", workspace.name)
    for env_key in ("dev", "test", "prod"):
        if env_key not in envs:
            continue
        env = envs[env_key]
        physical = env.get("topLevelName", f"{env_key}_{name}")
        mappings = dict(env.get("catalogMappings") or {})
        mappings[logical_catalog] = physical
        env["catalogMappings"] = mappings
    write_project(workspace, project)


@pytest.mark.integration
class TestWorkflowE2EMultiEnvApplyAndRollback:
    """E2E: Create dev-like catalog/schema/objects, apply to dev/test/prod, then rollback scenarios."""

    def test_e2e_create_dev_schema_then_apply_to_dev_test_prod(self, temp_workspace: Path) -> None:
        """Create catalog, schema, table (like fixtures) → snapshot → apply to dev, test, prod (stubbed)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        # Dev-like objects: one catalog, schema, table, columns (matches sample_operations style)
        ops = [
            builder.add_catalog("cat_123", "bronze", op_id="op_001"),
            builder.add_schema("schema_456", "raw", "cat_123", op_id="op_002"),
            builder.add_table("table_789", "users", "schema_456", "delta", op_id="op_003"),
            builder.add_column(
                "col_001", "table_789", "user_id", "BIGINT", False, "User ID", op_id="op_004"
            ),
            builder.add_column(
                "col_002", "table_789", "email", "STRING", True, "Email", op_id="op_005"
            ),
        ]
        append_ops(temp_workspace, ops)
        create_snapshot(temp_workspace, "Initial dev schema", version="v0.1.0")

        # So apply can resolve catalog "bronze" per environment
        _ensure_catalog_mappings_for_logical_catalog(temp_workspace, "bronze")

        apply_calls: list[str] = []

        def capture_apply(workspace: Path, target_env: str, **kwargs: object) -> object:
            apply_calls.append(target_env)
            return SimpleNamespace(status="success")

        with patch("schemax.cli.apply_to_environment", side_effect=capture_apply):
            for target in ("dev", "test", "prod"):
                result = invoke_cli(
                    "apply",
                    "--target",
                    target,
                    "--profile",
                    "dev",
                    "--warehouse-id",
                    "wh_123",
                    "--dry-run",
                    "--no-interaction",
                    str(temp_workspace),
                )
                assert result.exit_code == 0, f"apply --target {target}: {result.output}"

        assert apply_calls == ["dev", "test", "prod"]

    def test_e2e_apply_then_rollback_to_previous_snapshot(self, temp_workspace: Path) -> None:
        """Snapshot v0.1.0 → add changes → snapshot v0.2.0 → apply to dev (stub) → rollback to v0.1.0 (stub)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()

        append_ops(
            temp_workspace,
            [
                builder.add_catalog("cat_1", "bronze", op_id="op_1"),
                builder.add_schema("sch_1", "raw", "cat_1", op_id="op_2"),
                builder.add_table("tbl_1", "events", "sch_1", "delta", op_id="op_3"),
            ],
        )
        create_snapshot(temp_workspace, "Base", version="v0.1.0")

        append_ops(
            temp_workspace,
            [
                builder.add_table("tbl_2", "orders", "sch_1", "delta", op_id="op_4"),
            ],
        )
        create_snapshot(temp_workspace, "Add orders table", version="v0.2.0")

        _ensure_catalog_mappings_for_logical_catalog(temp_workspace, "bronze")

        with patch(
            "schemax.cli.apply_to_environment",
            return_value=SimpleNamespace(status="success"),
        ):
            r_apply = invoke_cli(
                "apply",
                "--target",
                "dev",
                "--profile",
                "dev",
                "--warehouse-id",
                "wh_123",
                "--dry-run",
                "--no-interaction",
                str(temp_workspace),
            )
        assert r_apply.exit_code == 0

        rollback_kwargs: dict = {}

        def capture_rollback(**kwargs: object) -> object:
            rollback_kwargs.update(kwargs)
            return SimpleNamespace(success=True, operations_rolled_back=0, error_message=None)

        with patch("schemax.cli.rollback_complete", side_effect=capture_rollback):
            r_rollback = invoke_cli(
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
                "--no-interaction",
                str(temp_workspace),
            )
        assert r_rollback.exit_code == 0
        assert rollback_kwargs.get("to_snapshot") == "v0.1.0"
        assert rollback_kwargs.get("target_env") == "dev"

    def test_e2e_failed_apply_then_partial_rollback(self, temp_workspace: Path) -> None:
        """Simulate failed deployment → partial rollback via CLI (stubbed)."""
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [
                builder.add_catalog("cat_1", "bronze", op_id="op_1"),
                builder.add_schema("sch_1", "raw", "cat_1", op_id="op_2"),
                builder.add_table("tbl_1", "users", "sch_1", "delta", op_id="op_3"),
            ],
        )
        create_snapshot(temp_workspace, "v1", version="v0.1.0")
        _ensure_catalog_mappings_for_logical_catalog(temp_workspace, "bronze")

        # Match opsDetails payload to what state differ produces for add_catalog (so CLI matching succeeds)
        fake_deployment = {
            "id": "deploy_fail_001",
            "status": "failed",
            "version": "v0.1.0",
            "fromVersion": None,
            "opsApplied": ["op_1"],
            "failedStatementIndex": 1,
            "opsDetails": [
                {
                    "id": "op_1",
                    "type": "unity.add_catalog",
                    "target": "cat_1",
                    "payload": {"catalogId": "cat_1", "name": "bronze"},
                },
            ],
        }
        mock_tracker = Mock()
        mock_tracker.get_deployment_by_id.return_value = fake_deployment

        with patch("schemax.providers.unity.auth.create_databricks_client", return_value=Mock()):
            with patch(
                "schemax.core.deployment.DeploymentTracker",
                return_value=mock_tracker,
            ):
                with patch("schemax.commands.rollback.rollback_partial") as mock_partial:
                    mock_partial.return_value = SimpleNamespace(
                        success=True, operations_rolled_back=1, error_message=None
                    )
                    result = invoke_cli(
                        "rollback",
                        "--deployment",
                        "deploy_fail_001",
                        "--partial",
                        "--target",
                        "dev",
                        "--profile",
                        "dev",
                        "--warehouse-id",
                        "wh_123",
                        "--dry-run",
                        "--no-interaction",
                        str(temp_workspace),
                    )
                    assert result.exit_code == 0, result.output
                    mock_partial.assert_called_once()
