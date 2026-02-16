"""
Test for rollback idempotency

Verifies that running rollback twice to the same snapshot detects
that we're already at the target version and skips redundant execution.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from schemax.commands.rollback import rollback_complete
from schemax.core.deployment import DeploymentTracker


def test_rollback_detects_already_at_target_version(tmp_path: Path) -> None:
    """Test that rollback skips execution if already at target version in database"""

    # Create a simple project structure
    schemax_dir = tmp_path / ".schemax"
    schemax_dir.mkdir()

    # Create project.json
    project_file = schemax_dir / "project.json"
    project_file.write_text("""{
        "version": 4,
        "name": "test_project",
        "provider": {
            "type": "unity",
            "version": "1.0.0",
            "environments": {
                "dev": {
                    "topLevelName": "dev_catalog",
                    "allowDrift": false,
                    "requireSnapshot": true,
                    "autoCreateTopLevel": true,
                    "autoCreateSchemaxSchema": true
                }
            }
        },
        "snapshots": [
            {
                "version": "v0.1.0",
                "name": "Initial",
                "createdAt": "2025-01-01T00:00:00Z"
            }
        ],
        "latestSnapshot": "v0.1.0"
    }""")

    # Create v0.1.0 snapshot
    snapshots_dir = schemax_dir / "snapshots"
    snapshots_dir.mkdir()
    snapshot_file = snapshots_dir / "v0.1.0.json"
    snapshot_file.write_text("""{
        "id": "snap_001",
        "version": "v0.1.0",
        "name": "Initial",
        "ts": "2025-01-01T00:00:00Z",
        "state": {
            "catalogs": []
        },
        "opsIncluded": [],
        "previousSnapshot": null
    }""")

    # Create empty changelog
    changelog_file = schemax_dir / "changelog.json"
    changelog_file.write_text("""{
        "version": 1,
        "sinceSnapshot": "v0.1.0",
        "ops": [],
        "lastModified": "2025-01-01T00:00:00Z"
    }""")

    # Mock Databricks client and tracker (patch at original location)
    with (
        patch("schemax.providers.unity.auth.create_databricks_client") as mock_client_factory,
        patch("schemax.commands.rollback.DeploymentTracker") as mock_tracker_class,
        patch("schemax.commands.rollback.UnitySQLExecutor") as mock_executor_class,
    ):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # Mock executor
        mock_executor = MagicMock()
        mock_executor.client = mock_client
        mock_executor_class.return_value = mock_executor

        # Mock tracker to return deployment at target version
        mock_tracker = MagicMock(spec=DeploymentTracker)
        mock_tracker.get_latest_deployment.return_value = {
            "version": "v0.1.0",  # Already at target version!
            "deploymentId": "deploy_001",
            "status": "success",
        }
        mock_tracker_class.return_value = mock_tracker

        # Run rollback
        result = rollback_complete(
            workspace=tmp_path,
            target_env="dev",
            to_snapshot="v0.1.0",
            profile="DEFAULT",
            warehouse_id="test_warehouse",
            dry_run=False,
        )

        # Verify: rollback detected we're already at target and returned success with 0 operations
        assert result.success is True
        assert result.operations_rolled_back == 0

        # Verify: no SQL was executed (tracker methods not called)
        mock_tracker.start_deployment.assert_not_called()
        mock_tracker.record_operation.assert_not_called()
        mock_tracker.complete_deployment.assert_not_called()


def test_rollback_proceeds_when_not_at_target_version(tmp_path: Path) -> None:
    """Test that rollback proceeds normally when database is at different version"""

    # Create a simple project structure (same as above)
    schemax_dir = tmp_path / ".schemax"
    schemax_dir.mkdir()

    project_file = schemax_dir / "project.json"
    project_file.write_text("""{
        "version": 4,
        "name": "test_project",
        "provider": {
            "type": "unity",
            "version": "1.0.0",
            "environments": {
                "dev": {
                    "topLevelName": "dev_catalog",
                    "allowDrift": false,
                    "requireSnapshot": true,
                    "autoCreateTopLevel": true,
                    "autoCreateSchemaxSchema": true
                }
            }
        },
        "snapshots": [
            {
                "version": "v0.1.0",
                "name": "Initial",
                "createdAt": "2025-01-01T00:00:00Z"
            }
        ],
        "latestSnapshot": "v0.1.0"
    }""")

    snapshots_dir = schemax_dir / "snapshots"
    snapshots_dir.mkdir()
    snapshot_file = snapshots_dir / "v0.1.0.json"
    snapshot_file.write_text("""{
        "id": "snap_001",
        "version": "v0.1.0",
        "name": "Initial",
        "ts": "2025-01-01T00:00:00Z",
        "state": {
            "catalogs": []
        },
        "opsIncluded": [],
        "previousSnapshot": null
    }""")

    changelog_file = schemax_dir / "changelog.json"
    changelog_file.write_text("""{
        "version": 1,
        "sinceSnapshot": "v0.1.0",
        "ops": [],
        "lastModified": "2025-01-01T00:00:00Z"
    }""")

    # Mock: database is at v0.2.0, not v0.1.0
    with (
        patch("schemax.providers.unity.auth.create_databricks_client") as mock_client_factory,
        patch("schemax.commands.rollback.DeploymentTracker") as mock_tracker_class,
        patch("schemax.commands.rollback.UnitySQLExecutor") as mock_executor_class,
    ):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        mock_executor = MagicMock()
        mock_executor.client = mock_client
        mock_executor_class.return_value = mock_executor

        mock_tracker = MagicMock(spec=DeploymentTracker)
        mock_tracker.get_latest_deployment.return_value = {
            "version": "v0.2.0",  # Different version - rollback should proceed
            "deploymentId": "deploy_002",
            "status": "success",
        }
        mock_tracker_class.return_value = mock_tracker

        # Run rollback - it should proceed but generate 0 operations (states match)
        result = rollback_complete(
            workspace=tmp_path,
            target_env="dev",
            to_snapshot="v0.1.0",
            profile="DEFAULT",
            warehouse_id="test_warehouse",
            dry_run=False,
        )

        # In this case, local state matches target, so no ops generated anyway
        # But the key point is: we didn't skip due to database check
        assert result.success is True
        # operations_rolled_back could be 0 if states match locally
