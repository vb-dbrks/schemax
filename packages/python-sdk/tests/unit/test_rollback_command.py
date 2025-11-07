"""
Tests for rollback command implementation
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from schematic.commands.rollback import RollbackError, rollback_partial
from schematic.providers.base.executor import ExecutionResult
from schematic.providers.base.operations import Operation
from schematic.providers.base.reverse_generator import SafetyLevel, SafetyReport


class TestRollbackPartial:
    """Test partial rollback functionality"""

    @patch("schematic.commands.rollback.load_current_state")
    def test_no_operations_to_rollback(self, mock_load_state):
        """Test rollback with no operations"""
        mock_load_state.return_value = ({}, None, None)

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

    @patch("schematic.commands.rollback.DeploymentTracker")
    @patch("schematic.commands.rollback.get_environment_config")
    @patch("schematic.commands.rollback.read_project")
    @patch("schematic.commands.rollback.load_current_state")
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
            "autoCreateSchematicSchema": True,
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
        from schematic.providers.base.sql_generator import SQLGenerationResult, StatementInfo

        mock_sql_generator.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG test;",
            statements=[
                StatementInfo(sql="DROP CATALOG test", operation_ids=["op_1"], execution_order=1)
            ],
        )

        # Mock executor - successful execution with statement_results
        from schematic.providers.base.executor import StatementResult

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
        with patch("schematic.commands.rollback.SafetyValidator") as mock_validator_class:
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

    @patch("schematic.commands.rollback.DeploymentTracker")
    @patch("schematic.commands.rollback.get_environment_config")
    @patch("schematic.commands.rollback.read_project")
    @patch("schematic.commands.rollback.load_current_state")
    def test_rollback_blocks_on_destructive(
        self, mock_load_state, mock_read_project, mock_get_env_config, mock_tracker_class
    ):
        """Test rollback blocks on destructive operations in auto mode"""
        # Setup mocks
        mock_provider = Mock()
        mock_state_reducer = Mock()
        mock_state_differ = Mock()

        mock_load_state.return_value = ({"catalogs": []}, None, mock_provider)

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
            "autoCreateSchematicSchema": True,
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
        with patch("schematic.commands.rollback.SafetyValidator") as mock_validator_class:
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

    @patch("schematic.commands.rollback.DeploymentTracker")
    @patch("schematic.commands.rollback.get_environment_config")
    @patch("schematic.commands.rollback.read_project")
    @patch("schematic.commands.rollback.load_current_state")
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

        mock_load_state.return_value = ({"catalogs": []}, None, mock_provider)

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
            "autoCreateSchematicSchema": True,
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
        from schematic.providers.base.sql_generator import SQLGenerationResult, StatementInfo

        mock_sql_generator.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG test;",
            statements=[
                StatementInfo(
                    sql="DROP CATALOG test", operation_ids=["rollback_1"], execution_order=1
                )
            ],
        )

        # Mock executor - failed execution
        from schematic.providers.base.executor import StatementResult

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

        with patch("schematic.commands.rollback.SafetyValidator") as mock_validator_class:
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
