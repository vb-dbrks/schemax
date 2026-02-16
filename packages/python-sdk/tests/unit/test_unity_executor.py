"""
Unit tests for Unity Catalog SQL executor (mock-based)

These tests use mocks to avoid requiring actual Databricks credentials.
Real integration tests against Databricks should be added separately.
"""

from unittest.mock import Mock, patch

import pytest

from schemax.providers.unity.executor import UnitySQLExecutor


class TestUnitySQLExecutor:
    """Test Unity SQL executor with mocked Databricks client"""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Databricks WorkspaceClient"""
        client = Mock()
        client.statement_execution = Mock()
        return client

    @pytest.fixture
    def executor(self, mock_client):
        """Create executor with mock client"""
        return UnitySQLExecutor(mock_client)

    def test_executor_initialization(self, executor, mock_client):
        """Should initialize with client"""
        assert executor.client == mock_client
        assert hasattr(executor, "execute_statements")

    # Note: Full executor tests would require complex mocking of Databricks SDK.
    # Real integration tests should be run against actual Databricks workspace.


class TestAuthHelpers:
    """Test authentication helper functions"""

    @patch("schemax.providers.unity.auth.WorkspaceClient")
    def test_create_databricks_client_with_profile(self, mock_workspace_client):
        """Should create client with specified profile"""
        from schemax.providers.unity.auth import create_databricks_client

        create_databricks_client(profile="DEV")

        mock_workspace_client.assert_called_once_with(profile="DEV")

    @patch("schemax.providers.unity.auth.WorkspaceClient")
    def test_create_databricks_client_default_profile(self, mock_workspace_client):
        """Should use default profile if not specified"""
        from schemax.providers.unity.auth import create_databricks_client

        create_databricks_client()

        # Should call WorkspaceClient with no profile (uses default)
        mock_workspace_client.assert_called_once_with()

    @patch("os.path.exists")
    @patch("builtins.open", create=True)
    def test_check_profile_exists_success(self, mock_open, mock_exists):
        """Should return True if profile exists in .databrickscfg"""
        from schemax.providers.unity.auth import check_profile_exists

        # Mock config file exists and has profile
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = (
            "[DEV]\nhost = https://test.databricks.com\n"
        )

        result = check_profile_exists("DEV")

        assert result is True

    @patch("os.path.exists")
    @patch("builtins.open", create=True)
    def test_check_profile_exists_not_found(self, mock_open, mock_exists):
        """Should return False if profile doesn't exist"""
        from schemax.providers.unity.auth import check_profile_exists

        # Mock config file exists but no profile
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = (
            "[PROD]\nhost = https://prod.databricks.com\n"
        )

        result = check_profile_exists("DEV")

        assert result is False

    @patch("os.path.exists")
    def test_check_profile_exists_no_config_file(self, mock_exists):
        """Should return False if config file doesn't exist"""
        from schemax.providers.unity.auth import check_profile_exists

        # Mock config file doesn't exist
        mock_exists.return_value = False

        result = check_profile_exists("DEV")

        assert result is False
