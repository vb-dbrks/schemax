"""
Tests for rollback command implementation
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from schemax.commands.rollback import RollbackError, rollback_complete, rollback_partial
from schemax.core.storage import append_ops, create_snapshot, ensure_project_file
from schemax.providers.base.executor import ExecutionResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


class TestRollbackPartial:
    """Test partial rollback functionality"""

    @patch("schemax.commands.rollback.load_current_state")
    def test_no_operations_to_rollback(self, mock_load_state):
        """Test rollback with no operations"""
        mock_load_state.return_value = ({}, None, None, None)

        result = rollback_partial(
            workspace=Path("/tmp"),
            deployment_id="test_deploy",
            successful_ops=[],
            target_env="dev",
            profile="DEFAULT",
            warehouse_id="abc123",
            executor=Mock(),
            from_version=None,  # First deployment (empty initial state)
        )

        assert result.success is True
        assert result.operations_rolled_back == 0

    @patch("schemax.commands.rollback.DeploymentTracker")
    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    @patch("schemax.commands.rollback.load_current_state")
    def test_rollback_with_safe_operations(
        self,
        mock_load_state,
        mock_read_project,
        mock_get_env_config,
        mock_tracker_class,
    ):
        """Test rollback with safe operations"""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.info.id = "unity"
        mock_provider.info.version = "1.0.0"
        mock_state_reducer = Mock()
        mock_state_differ = Mock()
        mock_sql_generator = Mock()
        mock_executor = Mock()
        mock_executor.client = Mock()  # Add client attribute for UnitySQLExecutor

        mock_load_state.return_value = (
            {"catalogs": []},  # pre_deployment_state
            None,
            mock_provider,
            None,  # validation_result
        )

        # Mock project and environment config
        mock_read_project.return_value = {
            "name": "test_project",
            "deployments": [
                {
                    "id": "test_deploy",
                    "fromVersion": None,  # First deployment
                    "version": "v0.1.0",
                    "environment": "dev",
                }
            ],
        }
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "autoCreateSchemaxSchema": True,
        }

        # Mock deployment tracker
        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker
        # Mock get_deployment_by_id to return deployment record
        mock_tracker.get_deployment_by_id.return_value = {
            "id": "test_deploy",
            "fromVersion": None,  # First deployment
            "version": "v0.1.0",
            "environment": "dev",
            "status": "failed",
        }

        # Mock provider methods
        mock_provider.get_state_reducer.return_value = mock_state_reducer
        mock_provider.get_state_differ.return_value = mock_state_differ
        mock_provider.get_sql_generator.return_value = mock_sql_generator

        # Mock state reducer
        mock_state_reducer.apply_operations.return_value = {"catalogs": [{"name": "test"}]}

        # Mock state differ - generate rollback ops
        rollback_op = Operation(
            id="rollback_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_catalog",
            target="catalog_1",
            payload={},
        )
        mock_state_differ.generate_diff_operations.return_value = [rollback_op]

        # Mock SQL generator
        # Mock generate_sql_with_mapping to return SQLGenerationResult
        from schemax.providers.base.sql_generator import SQLGenerationResult, StatementInfo

        mock_sql_generator.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG test;",
            statements=[
                StatementInfo(sql="DROP CATALOG test", operation_ids=["op_1"], execution_order=1)
            ],
        )

        # Mock executor - successful execution with statement_results
        from schemax.providers.base.executor import StatementResult

        stmt_result = StatementResult(
            statement_id="stmt_1",
            sql="DROP CATALOG test",
            status="success",
            execution_time_ms=100,
            error_message=None,
        )
        mock_executor.execute_statements.return_value = ExecutionResult(
            deployment_id="test_deploy",
            total_statements=1,
            successful_statements=1,
            failed_statement_index=None,
            statement_results=[stmt_result],
            total_execution_time_ms=100,
            status="success",
            error_message=None,
        )

        # Mock safety validator to return SAFE
        with patch("schemax.commands.rollback.SafetyValidator") as mock_validator_class:
            mock_validator = Mock()
            mock_validator_class.return_value = mock_validator
            mock_validator.validate.return_value = SafetyReport(
                level=SafetyLevel.SAFE, reason="Catalog is empty", data_at_risk=0
            )

            # Create a successful operation to rollback
            successful_op = Operation(
                id="op_1",
                ts="2025-01-01T00:00:00Z",
                provider="unity",
                op="unity.add_catalog",
                target="catalog_1",
                payload={"catalogId": "catalog_1", "name": "test"},
            )

            # Execute rollback
            result = rollback_partial(
                workspace=Path("/tmp"),
                deployment_id="test_deploy",
                successful_ops=[successful_op],
                target_env="dev",
                profile="DEFAULT",
                warehouse_id="abc123",
                executor=mock_executor,
                catalog_mapping={"__implicit__": "dev_catalog"},
                auto_triggered=True,
                from_version=None,  # First deployment (empty initial state)
            )

        # Assertions
        assert result.success is True
        assert result.operations_rolled_back == 1
        assert result.error_message is None

        # Verify deployment tracking was called
        mock_tracker.ensure_tracking_schema.assert_called_once()
        mock_tracker.start_deployment.assert_called_once()
        mock_tracker.record_operation.assert_called_once()
        mock_tracker.complete_deployment.assert_called_once()

    @patch("schemax.commands.rollback.DeploymentTracker")
    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    @patch("schemax.commands.rollback.load_current_state")
    def test_rollback_blocks_on_destructive(
        self, mock_load_state, mock_read_project, mock_get_env_config, mock_tracker_class
    ):
        """Test rollback blocks on destructive operations in auto mode"""
        # Setup mocks
        mock_provider = Mock()
        mock_state_reducer = Mock()
        mock_state_differ = Mock()

        mock_load_state.return_value = ({"catalogs": []}, None, mock_provider, None)

        # Mock project and environment config
        mock_read_project.return_value = {
            "name": "test_project",
            "deployments": [
                {
                    "id": "test_deploy",
                    "fromVersion": None,  # First deployment
                    "version": "v0.1.0",
                    "environment": "prod",
                }
            ],
        }
        mock_get_env_config.return_value = {
            "topLevelName": "prod_catalog",
            "autoCreateSchemaxSchema": True,
        }

        # Mock deployment tracker
        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker
        mock_tracker.get_deployment_by_id.return_value = {
            "id": "test_deploy",
            "fromVersion": None,
            "version": "v0.1.0",
            "environment": "prod",
            "status": "failed",
        }

        mock_provider.get_state_reducer.return_value = mock_state_reducer
        mock_provider.get_state_differ.return_value = mock_state_differ

        mock_state_reducer.apply_operations.return_value = {"catalogs": []}

        rollback_op = Operation(
            id="rollback_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_table",
            target="catalog.schema.table",
            payload={},
        )
        mock_state_differ.generate_diff_operations.return_value = [rollback_op]

        # Mock safety validator to return DESTRUCTIVE
        with patch("schemax.commands.rollback.SafetyValidator") as mock_validator_class:
            mock_validator = Mock()
            mock_validator_class.return_value = mock_validator
            mock_validator.validate.return_value = SafetyReport(
                level=SafetyLevel.DESTRUCTIVE,
                reason="Table has 10,000 rows",
                data_at_risk=10000,
            )

            successful_op = Operation(
                id="op_1",
                ts="2025-01-01T00:00:00Z",
                provider="unity",
                op="unity.add_table",
                target="catalog.schema.table",
                payload={},
            )

            # Execute rollback - should raise RollbackError
            with pytest.raises(RollbackError, match="Auto-rollback blocked"):
                rollback_partial(
                    workspace=Path("/tmp"),
                    deployment_id="test_deploy",
                    successful_ops=[successful_op],
                    target_env="prod",
                    profile="PROD",
                    warehouse_id="abc123",
                    executor=Mock(),
                    auto_triggered=True,  # Auto mode blocks on destructive
                    from_version=None,  # First deployment (empty initial state)
                )

    @patch("schemax.commands.rollback.DeploymentTracker")
    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    @patch("schemax.commands.rollback.load_current_state")
    def test_rollback_execution_failure(
        self,
        mock_load_state,
        mock_read_project,
        mock_get_env_config,
        mock_tracker_class,
    ):
        """Test rollback handles execution failures"""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.info.id = "unity"
        mock_provider.info.version = "1.0.0"
        mock_state_reducer = Mock()
        mock_state_differ = Mock()
        mock_sql_generator = Mock()
        mock_executor = Mock()
        mock_executor.client = Mock()

        mock_load_state.return_value = ({"catalogs": []}, None, mock_provider, None)

        # Mock project and environment config
        mock_read_project.return_value = {
            "name": "test_project",
            "deployments": [
                {
                    "id": "test_deploy",
                    "fromVersion": None,  # First deployment
                    "version": "v0.1.0",
                    "environment": "dev",
                }
            ],
        }
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "autoCreateSchemaxSchema": True,
        }

        # Mock deployment tracker
        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker
        mock_tracker.get_deployment_by_id.return_value = {
            "id": "test_deploy",
            "fromVersion": None,
            "version": "v0.1.0",
            "environment": "dev",
            "status": "failed",
        }

        mock_provider.get_state_reducer.return_value = mock_state_reducer
        mock_provider.get_state_differ.return_value = mock_state_differ
        mock_provider.get_sql_generator.return_value = mock_sql_generator

        mock_state_reducer.apply_operations.return_value = {"catalogs": []}

        rollback_op = Operation(
            id="rollback_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_catalog",
            target="catalog_1",
            payload={},
        )
        mock_state_differ.generate_diff_operations.return_value = [rollback_op]

        # Mock generate_sql_with_mapping
        from schemax.providers.base.sql_generator import SQLGenerationResult, StatementInfo

        mock_sql_generator.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG test;",
            statements=[
                StatementInfo(
                    sql="DROP CATALOG test", operation_ids=["rollback_1"], execution_order=1
                )
            ],
        )

        # Mock executor - failed execution
        from schemax.providers.base.executor import StatementResult

        stmt_result = StatementResult(
            statement_id="stmt_1",
            sql="DROP CATALOG test",
            status="failed",
            execution_time_ms=50,
            error_message="Catalog does not exist",
        )
        mock_executor.execute_statements.return_value = ExecutionResult(
            deployment_id="test_deploy",
            total_statements=1,
            successful_statements=0,
            failed_statement_index=0,
            statement_results=[stmt_result],
            total_execution_time_ms=50,
            status="failed",
            error_message="Catalog does not exist",
        )

        with patch("schemax.commands.rollback.SafetyValidator") as mock_validator_class:
            mock_validator = Mock()
            mock_validator_class.return_value = mock_validator
            mock_validator.validate.return_value = SafetyReport(
                level=SafetyLevel.SAFE, reason="Safe", data_at_risk=0
            )

            successful_op = Operation(
                id="op_1",
                ts="2025-01-01T00:00:00Z",
                provider="unity",
                op="unity.add_catalog",
                target="catalog_1",
                payload={},
            )

            result = rollback_partial(
                workspace=Path("/tmp"),
                deployment_id="test_deploy",
                successful_ops=[successful_op],
                target_env="dev",
                profile="DEFAULT",
                warehouse_id="abc123",
                executor=mock_executor,
                auto_triggered=True,
                from_version=None,  # First deployment (empty initial state)
            )

        # Assertions
        assert result.success is False
        assert result.operations_rolled_back == 0
        assert "Catalog does not exist" in result.error_message

        # Verify deployment tracking was called even for failures
        mock_tracker.ensure_tracking_schema.assert_called_once()
        mock_tracker.start_deployment.assert_called_once()
        mock_tracker.complete_deployment.assert_called_once()

    @patch("schemax.commands.rollback.DeploymentTracker")
    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    @patch("schemax.commands.rollback.load_current_state")
    def test_rollback_skips_recording_when_deployment_catalog_dropped(
        self,
        mock_load_state,
        mock_read_project,
        mock_get_env_config,
        mock_tracker_class,
    ):
        """When partial rollback successfully drops the deployment catalog, skip DB recording."""
        mock_provider = Mock()
        mock_provider.info.id = "unity"
        mock_provider.info.version = "1.0.0"
        mock_state_reducer = Mock()
        mock_state_differ = Mock()
        mock_sql_generator = Mock()
        mock_executor = Mock()
        mock_executor.client = Mock()

        mock_load_state.return_value = ({"catalogs": []}, None, mock_provider, None)
        mock_read_project.return_value = {
            "name": "test_project",
            "deployments": [{"id": "test_deploy", "fromVersion": None, "version": "v0.1.0"}],
        }
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "autoCreateSchemaxSchema": True,
        }

        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker
        mock_tracker.get_deployment_by_id.return_value = {
            "id": "test_deploy",
            "fromVersion": None,
            "version": "v0.1.0",
        }

        mock_provider.get_state_reducer.return_value = mock_state_reducer
        mock_provider.get_state_differ.return_value = mock_state_differ
        mock_provider.get_sql_generator.return_value = mock_sql_generator
        mock_state_reducer.apply_operations.return_value = {"catalogs": []}

        rollback_op = Operation(
            id="rollback_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_catalog",
            target="catalog_1",
            payload={},
        )
        mock_state_differ.generate_diff_operations.return_value = [rollback_op]

        from schemax.providers.base.sql_generator import SQLGenerationResult, StatementInfo

        mock_sql_generator.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG dev_catalog;",
            statements=[
                StatementInfo(
                    sql="DROP CATALOG IF EXISTS `dev_catalog` CASCADE",
                    operation_ids=["rollback_1"],
                    execution_order=1,
                )
            ],
        )

        from schemax.providers.base.executor import StatementResult

        mock_executor.execute_statements.return_value = ExecutionResult(
            deployment_id="test_deploy",
            total_statements=1,
            successful_statements=1,
            failed_statement_index=None,
            statement_results=[
                StatementResult(
                    statement_id="stmt_1",
                    sql="DROP CATALOG IF EXISTS `dev_catalog` CASCADE",
                    status="success",
                    execution_time_ms=100,
                    error_message=None,
                )
            ],
            total_execution_time_ms=100,
            status="success",
            error_message=None,
        )

        with patch("schemax.commands.rollback.SafetyValidator") as mock_validator_class:
            mock_validator_class.return_value.validate.return_value = SafetyReport(
                level=SafetyLevel.SAFE, reason="Safe", data_at_risk=0
            )
            result = rollback_partial(
                workspace=Path("/tmp"),
                deployment_id="test_deploy",
                successful_ops=[
                    Operation(
                        id="op_1",
                        ts="2025-01-01T00:00:00Z",
                        provider="unity",
                        op="unity.add_catalog",
                        target="catalog_1",
                        payload={"name": "dev_catalog"},
                    )
                ],
                target_env="dev",
                profile="DEFAULT",
                warehouse_id="abc123",
                executor=mock_executor,
                catalog_mapping={"catalog_1": "dev_catalog"},
                auto_triggered=True,
                from_version=None,
            )

        assert result.success is True
        assert result.operations_rolled_back == 1
        mock_tracker.ensure_tracking_schema.assert_not_called()
        mock_tracker.start_deployment.assert_not_called()
        mock_tracker.record_operation.assert_not_called()
        mock_tracker.complete_deployment.assert_not_called()


class TestRollbackCompleteBaselineGuard:
    """Test rollback_complete baseline guard (block rollback before import baseline)."""

    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    def test_rollback_before_baseline_raises_without_force(
        self, mock_read_project, mock_get_env_config
    ):
        """When env has importBaselineSnapshot and to_snapshot is before it, raise unless --force."""
        mock_read_project.return_value = {"name": "p", "provider": {"environments": {}}}
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "importBaselineSnapshot": "v0.1.0",
        }
        with pytest.raises(RollbackError) as exc_info:
            rollback_complete(
                workspace=Path("/tmp"),
                target_env="dev",
                to_snapshot="v0.0.5",
                profile="DEFAULT",
                warehouse_id="wh_123",
                force=False,
            )
        assert "v0.0.5" in str(exc_info.value)
        assert "v0.1.0" in str(exc_info.value)
        assert "force" in str(exc_info.value).lower()

    @patch("schemax.core.storage.read_snapshot")
    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    def test_rollback_before_baseline_proceeds_with_force_and_no_interaction(
        self, mock_read_project, mock_get_env_config, mock_read_snapshot
    ):
        """With --force and --no-interaction, rollback before baseline proceeds (no prompt)."""
        mock_read_project.return_value = {"name": "p", "provider": {"environments": {}}}
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "importBaselineSnapshot": "v0.1.0",
        }
        mock_read_snapshot.return_value = {
            "version": "v0.0.5",
            "state": {"catalogs": []},
            "operations": [],
        }
        mock_differ = Mock()
        mock_differ.generate_diff_operations.return_value = []
        mock_provider = Mock()
        mock_provider.get_state_differ.return_value = mock_differ
        with patch("schemax.providers.unity.auth.create_databricks_client"):
            with patch("schemax.commands.rollback.DeploymentTracker") as mock_tracker_class:
                mock_tracker = Mock()
                mock_tracker_class.return_value = mock_tracker
                mock_tracker.get_latest_deployment.return_value = None
                with patch("schemax.commands.rollback.load_current_state") as mock_load:
                    mock_load.return_value = (
                        {"catalogs": []},
                        {},
                        mock_provider,
                        None,
                    )
                    with patch("schemax.commands.diff._build_catalog_mapping") as mock_build_map:
                        mock_build_map.return_value = {"__implicit__": "dev_catalog"}
                        result = rollback_complete(
                            workspace=Path("/tmp"),
                            target_env="dev",
                            to_snapshot="v0.0.5",
                            profile="DEFAULT",
                            warehouse_id="wh_123",
                            force=True,
                            no_interaction=True,
                        )
        assert result.success is True
        assert result.operations_rolled_back == 0
        mock_read_snapshot.assert_called()

    @patch("schemax.commands.rollback.get_environment_config")
    @patch("schemax.commands.rollback.read_project")
    def test_rollback_at_or_after_baseline_allowed(self, mock_read_project, mock_get_env_config):
        """When to_snapshot is at or after baseline, no RollbackError (baseline check passes)."""
        mock_read_project.return_value = {"name": "p", "provider": {"environments": {}}}
        mock_get_env_config.return_value = {
            "topLevelName": "dev_catalog",
            "importBaselineSnapshot": "v0.1.0",
        }
        mock_differ = Mock()
        mock_differ.generate_diff_operations.return_value = []
        mock_provider = Mock()
        mock_provider.get_state_differ.return_value = mock_differ
        with patch("schemax.core.storage.read_snapshot") as mock_read_snapshot:
            mock_read_snapshot.return_value = {
                "version": "v0.1.0",
                "state": {"catalogs": []},
                "operations": [],
            }
            with patch("schemax.providers.unity.auth.create_databricks_client"):
                with patch("schemax.commands.rollback.DeploymentTracker") as mock_tracker_class:
                    mock_tracker = Mock()
                    mock_tracker_class.return_value = mock_tracker
                    mock_tracker.get_latest_deployment.return_value = {
                        "version": "v0.2.0",
                    }
                    with patch("schemax.commands.rollback.load_current_state") as mock_load:
                        mock_load.return_value = (
                            {"catalogs": []},
                            {},
                            mock_provider,
                            None,
                        )
                        with patch(
                            "schemax.commands.diff._build_catalog_mapping"
                        ) as mock_build_map:
                            mock_build_map.return_value = {"__implicit__": "dev_catalog"}
                            result = rollback_complete(
                                workspace=Path("/tmp"),
                                target_env="dev",
                                to_snapshot="v0.1.0",
                                profile="DEFAULT",
                                warehouse_id="wh_123",
                                force=False,
                            )
        mock_read_snapshot.assert_called()
        assert result.success is True
        assert result.operations_rolled_back == 0


class TestPartialRollbackCli:
    """Unit tests for partial rollback CLI path (run_partial_rollback_cli via invoke_cli).

    These use mocks for DB and rollback_partial so we test CLI dispatch and
    run_partial_rollback_cli logic without a live Databricks connection.
    Integration tests must not patch; partial rollback against a real backend
    is covered by live/integration tests when configured.
    """

    def test_partial_rollback_cli_exits_zero_when_tracker_returns_deployment(
        self, tmp_path: Path
    ) -> None:
        """Partial rollback CLI with stubbed tracker and rollback_partial exits 0."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")
        # One add_catalog op so snapshot contains it; _resolve_successful_ops_from_snapshots can match
        builder = OperationBuilder()
        op = builder.catalog.add_catalog("cat_implicit", "__implicit__", op_id="op_init_catalog")
        append_ops(workspace, [op])
        create_snapshot(workspace, "v1", version="v0.1.0")

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
                "schemax.commands.rollback.DeploymentTracker",
                return_value=mock_tracker,
            ):
                with patch("schemax.commands.rollback.rollback_partial") as mock_partial:
                    mock_partial.return_value = SimpleNamespace(
                        success=True,
                        operations_rolled_back=2,
                        error_message=None,
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
                        str(workspace),
                    )
        assert result.exit_code == 0
        mock_partial.assert_called_once()
