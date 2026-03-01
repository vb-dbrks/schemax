"""Unit tests for apply command using repository injection (no storage patch seams)."""

from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from schemax.commands.apply import apply_to_environment
from schemax.providers.base.executor import ExecutionResult


class _RepoStub:
    def __init__(self) -> None:
        self.project: dict[str, Any] = {
            "version": 4,
            "name": "test_project",
            "provider": {"type": "unity", "version": "1.0.0", "environments": {"dev": {}}},
            "settings": {"versionPrefix": "v"},
            "latestSnapshot": "v0.1.0",
            "managedLocations": {},
            "externalLocations": {},
        }
        self.env_config: dict[str, Any] = {
            "topLevelName": "dev_catalog",
            "allowDrift": True,
            "requireSnapshot": False,
            "autoCreateTopLevel": True,
            "autoCreateSchemaxSchema": True,
            "catalogMappings": {"main": "dev_catalog"},
        }
        self.changelog: dict[str, Any] = {"ops": []}
        self.state_result: tuple[Any, ...] = ({"catalogs": []}, {"ops": []}, None, None)
        self.snapshots: dict[str, dict[str, Any]] = {
            "v0.1.0": {"state": {"catalogs": []}, "operations": []}
        }
        self.snapshot_calls: list[dict[str, Any]] = []

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        del workspace
        return self.project

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]:
        del workspace
        return self.changelog

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        del project, environment
        return self.env_config

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        del workspace, validate
        return self.state_result

    def create_snapshot(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        del workspace, tags
        self.snapshot_calls.append({"name": name, "version": version, "comment": comment})
        snapshot_version: str = version or "v0.2.0"
        snapshot: dict[str, Any] = {
            "version": snapshot_version,
            "state": {"catalogs": []},
            "operations": [],
        }
        self.project["latestSnapshot"] = snapshot_version
        self.snapshots[snapshot_version] = snapshot
        return self.project, snapshot

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        del workspace
        try:
            return self.snapshots[version]
        except KeyError as err:
            raise FileNotFoundError(version) from err


def _provider_with_no_diff() -> Mock:
    provider = Mock()
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"
    provider.info.id = "unity"
    provider.create_initial_state.return_value = {"catalogs": []}
    provider.validate_execution_config.return_value = Mock(valid=True, errors=[])

    differ = Mock()
    differ.generate_diff_operations.return_value = []
    provider.get_state_differ.return_value = differ
    return provider


def test_noninteractive_creates_snapshot_when_uncommitted_ops() -> None:
    repo = _RepoStub()
    provider = _provider_with_no_diff()
    repo.changelog = {"ops": [{"id": "op_1"}]}
    repo.state_result = ({"catalogs": []}, repo.changelog, provider, None)

    with patch("schemax.commands.apply.Prompt.ask") as prompt:
        with patch("schemax.commands.apply.create_databricks_client"):
            with patch("schemax.commands.apply.DeploymentTracker") as tracker_cls:
                tracker_cls.return_value.get_latest_deployment.return_value = None
                result = apply_to_environment(
                    workspace=Path("."),
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_1",
                    dry_run=True,
                    no_interaction=True,
                    workspace_repo=repo,
                )

    assert result.status == "success"
    assert len(repo.snapshot_calls) == 1
    assert repo.snapshot_calls[0]["version"] == "v0.2.0"
    prompt.assert_not_called()


def test_interactive_abort_exits_cleanly() -> None:
    repo = _RepoStub()
    provider = _provider_with_no_diff()
    repo.changelog = {"ops": [{"id": "op_1"}]}
    repo.state_result = ({"catalogs": []}, repo.changelog, provider, None)

    with patch("schemax.commands.apply.Prompt.ask", return_value="abort"):
        with patch("schemax.commands.apply.create_databricks_client"):
            with patch("schemax.commands.apply.DeploymentTracker") as tracker_cls:
                tracker_cls.return_value.get_latest_deployment.return_value = None
                try:
                    apply_to_environment(
                        workspace=Path("."),
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_1",
                        dry_run=True,
                        no_interaction=False,
                        workspace_repo=repo,
                    )
                except BaseException as err:
                    assert isinstance(err, SystemExit)
                    assert err.code == 0


def test_no_uncommitted_ops_no_prompt_and_success() -> None:
    repo = _RepoStub()
    provider = _provider_with_no_diff()
    repo.changelog = {"ops": []}
    repo.state_result = ({"catalogs": []}, repo.changelog, provider, None)

    with patch("schemax.commands.apply.Prompt.ask") as prompt:
        with patch("schemax.commands.apply.create_databricks_client"):
            with patch("schemax.commands.apply.DeploymentTracker") as tracker_cls:
                tracker_cls.return_value.get_latest_deployment.return_value = None
                result = apply_to_environment(
                    workspace=Path("."),
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_1",
                    dry_run=True,
                    no_interaction=False,
                    workspace_repo=repo,
                )

    assert result.status == "success"
    prompt.assert_not_called()


def test_deployed_snapshot_missing_falls_back_to_empty_diff() -> None:
    repo = _RepoStub()
    provider = _provider_with_no_diff()
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)
    repo.project["latestSnapshot"] = "v0.1.0"

    with patch("schemax.commands.apply.create_databricks_client"):
        with patch("schemax.commands.apply.DeploymentTracker") as tracker_cls:
            tracker_cls.return_value.get_latest_deployment.return_value = {"version": "v9.9.9"}
            result = apply_to_environment(
                workspace=Path("."),
                target_env="dev",
                profile="DEFAULT",
                warehouse_id="wh_1",
                dry_run=True,
                no_interaction=True,
                workspace_repo=repo,
            )

    assert result.status == "success"


def test_auto_rollback_called_on_partial_execution() -> None:
    repo = _RepoStub()
    provider = Mock()
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"
    provider.info.id = "unity"
    provider.create_initial_state.return_value = {"catalogs": []}
    provider.validate_execution_config.return_value = Mock(valid=True, errors=[])

    diff_op = Mock(id="op_1")
    differ = Mock()
    differ.generate_diff_operations.return_value = [diff_op]
    provider.get_state_differ.return_value = differ

    sql_result = Mock()
    sql_result.sql = "CREATE SCHEMA x;"
    sql_result.statements = [Mock(sql="CREATE SCHEMA x", operation_ids=["op_1"])]
    generator = Mock()
    generator.generate_sql_with_mapping.return_value = sql_result
    provider.get_sql_generator.return_value = generator

    executor = Mock()
    executor.execute_statements.return_value = ExecutionResult(
        deployment_id="d1",
        total_statements=1,
        successful_statements=0,
        failed_statement_index=0,
        statement_results=[],
        total_execution_time_ms=1,
        status="partial",
        error_message="failed",
    )
    provider.get_sql_executor.return_value = executor

    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    with patch("schemax.commands.apply.create_databricks_client"):
        with patch("schemax.commands.apply.DeploymentTracker") as tracker_cls:
            tracker_cls.return_value.get_latest_deployment.return_value = None
            with patch("schemax.commands.apply.rollback_partial") as rollback_partial:
                rollback_partial.return_value = Mock(success=True, operations_rolled_back=1)
                with pytest.raises(SystemExit, match="1"):
                    apply_to_environment(
                        workspace=Path("."),
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_1",
                        dry_run=False,
                        no_interaction=True,
                        auto_rollback=True,
                        workspace_repo=repo,
                    )

    rollback_partial.assert_called_once()
