"""
Unit tests for DeploymentTracker (mock-based)
"""

from unittest.mock import Mock, call
import pytest

from schematic.deployment_tracker import DeploymentTracker
from schematic.providers.base.executor import ExecutionResult, StatementResult


class TestDeploymentTracker:
    """Test deployment tracking functionality"""

    @pytest.fixture
    def mock_executor(self):
        """Create mock SQL executor"""
        executor = Mock()
        executor.execute_statement = Mock(
            return_value=StatementResult(
                statement_id="stmt_123",
                sql="",
                status="success",
                execution_time_ms=500,
                error_message=None,
                rows_affected=0,
            )
        )
        return executor

    @pytest.fixture
    def tracker(self, mock_executor):
        """Create deployment tracker with mock executor"""
        return DeploymentTracker(mock_executor, catalog="test_catalog", warehouse_id="test_wh")

    def test_tracker_initialization(self, tracker):
        """Should initialize with executor and catalog"""
        assert tracker.catalog == "test_catalog"
        assert tracker.schema == "`test_catalog`.`schematic`"
        assert tracker.warehouse_id == "test_wh"

    # Note: Full deployment tracker tests require actual Databricks SDK integration.
    # Real integration tests should be run against actual Databricks workspace.
    # These smoke tests just verify basic initialization.


    # Note: Full integration tests should run against actual Databricks workspace

