"""Unit tests for rollback command with repository injection (no legacy storage patch seams)."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from schemax.commands.rollback import (
    RollbackError,
    rollback_complete,
    rollback_partial,
    run_partial_rollback_cli,
)
from schemax.providers.base.executor import ExecutionResult, StatementResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport
from schemax.providers.base.sql_generator import SQLGenerationResult, StatementInfo


class _RepoStub:
    def __init__(
        self,
        *,
        project: dict | None = None,
        env_config: dict | None = None,
        state_result: tuple | None = None,
        snapshots: dict[str, dict] | None = None,
        changelog: dict | None = None,
    ) -> None:
        self._project = project or {"name": "test", "provider": {"environments": {}}}
        self._env_config = env_config or {"topLevelName": "dev_catalog"}
        self._state_result = state_result
        self._snapshots = snapshots or {}
        self._changelog = changelog or {"ops": []}

    def read_project(self, *, workspace: Path) -> dict:
        del workspace
        return self._project

    def get_environment_config(self, *, project: dict, environment: str) -> dict:
        del project, environment
        return self._env_config

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple:
        del workspace, validate
        if self._state_result is None:
            raise AssertionError("state_result not configured")
        return self._state_result

    def read_snapshot(self, *, workspace: Path, version: str) -> dict:
        del workspace
        return self._snapshots[version]

    def read_changelog(self, *, workspace: Path) -> dict:
        del workspace
        return self._changelog


class TestRollbackPartial:
    def test_no_operations_to_rollback(self) -> None:
        result = rollback_partial(
            workspace=Path("/tmp"),
            deployment_id="d1",
            successful_ops=[],
            target_env="dev",
            profile="DEFAULT",
            warehouse_id="wh_1",
            executor=Mock(),
        )
        assert result.success is True
        assert result.operations_rolled_back == 0

    def test_rollback_with_safe_operations(self) -> None:
        provider = Mock()
        provider.info.id = "unity"
        provider.info.version = "1.0.0"
        provider.apply_operations.side_effect = [{"catalogs": []}, {"catalogs": [{"id": "c1"}]}]

        differ = Mock()
        rollback_op = Operation(
            id="rb_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op="unity.drop_catalog",
            target="c1",
            payload={},
        )
        differ.generate_diff_operations.return_value = [rollback_op]
        provider.get_state_differ.return_value = differ

        sql_gen = Mock()
        sql_gen.generate_sql_with_mapping.return_value = SQLGenerationResult(
            sql="DROP CATALOG dev;",
            statements=[
                StatementInfo(sql="DROP CATALOG dev", operation_ids=["rb_1"], execution_order=1)
            ],
        )
        provider.get_sql_generator.return_value = sql_gen

        repo = _RepoStub(
            project={"name": "proj", "provider": {"environments": {"dev": {}}}},
            env_config={"topLevelName": "dev_catalog", "autoCreateSchemaxSchema": True},
            state_result=({"catalogs": []}, {}, provider, None),
        )

        deployment = {
            "id": "d1",
            "fromVersion": None,
            "version": "v0.1.0",
            "status": "failed",
        }
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = deployment

        executor = Mock()
        executor.client = Mock()
        executor.execute_statements.return_value = ExecutionResult(
            deployment_id="d1",
            total_statements=1,
            successful_statements=1,
            failed_statement_index=None,
            statement_results=[
                StatementResult(
                    statement_id="s1",
                    sql="DROP CATALOG dev",
                    status="success",
                    execution_time_ms=10,
                    error_message=None,
                )
            ],
            total_execution_time_ms=10,
            status="success",
            error_message=None,
        )

        with patch("schemax.commands.rollback.DeploymentTracker", return_value=tracker):
            with patch("schemax.commands.rollback.SafetyValidator") as validator_cls:
                validator_cls.return_value.validate.return_value = SafetyReport(
                    level=SafetyLevel.SAFE,
                    reason="safe",
                    data_at_risk=0,
                )
                result = rollback_partial(
                    workspace=Path("/tmp"),
                    deployment_id="d1",
                    successful_ops=[
                        Operation(
                            id="op_1",
                            ts="2025-01-01T00:00:00Z",
                            provider="unity",
                            op="unity.add_catalog",
                            target="c1",
                            payload={"name": "dev"},
                        )
                    ],
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_1",
                    executor=executor,
                    catalog_mapping={"__implicit__": "dev_catalog"},
                    auto_triggered=True,
                    workspace_repo=repo,
                )

        assert result.success is True
        assert result.operations_rolled_back == 1

    def test_rollback_blocks_on_destructive_in_auto_mode(self) -> None:
        provider = Mock()
        provider.apply_operations.side_effect = [{"catalogs": []}, {"catalogs": []}]
        differ = Mock()
        differ.generate_diff_operations.return_value = [
            Operation(
                id="rb_1",
                ts="2025-01-01T00:00:00Z",
                provider="unity",
                op="unity.drop_table",
                target="c.s.t",
                payload={},
            )
        ]
        provider.get_state_differ.return_value = differ
        provider.get_sql_generator.return_value = Mock(
            generate_sql_with_mapping=Mock(
                return_value=SQLGenerationResult(
                    sql="DROP TABLE t;",
                    statements=[
                        StatementInfo(sql="DROP TABLE t", operation_ids=["rb_1"], execution_order=1)
                    ],
                )
            )
        )

        repo = _RepoStub(
            env_config={"topLevelName": "prod_catalog"},
            state_result=({"catalogs": []}, {}, provider, None),
        )
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = {
            "id": "d1",
            "fromVersion": None,
            "version": "v0.1.0",
            "status": "failed",
        }

        with patch("schemax.commands.rollback.DeploymentTracker", return_value=tracker):
            with patch("schemax.commands.rollback.SafetyValidator") as validator_cls:
                validator_cls.return_value.validate.return_value = SafetyReport(
                    level=SafetyLevel.DESTRUCTIVE,
                    reason="destructive",
                    data_at_risk=1,
                )
                with pytest.raises(RollbackError, match="Auto-rollback blocked"):
                    rollback_partial(
                        workspace=Path("/tmp"),
                        deployment_id="d1",
                        successful_ops=[
                            Operation(
                                id="op_1",
                                ts="2025-01-01T00:00:00Z",
                                provider="unity",
                                op="unity.add_table",
                                target="c.s.t",
                                payload={},
                            )
                        ],
                        target_env="prod",
                        profile="PROD",
                        warehouse_id="wh_1",
                        executor=Mock(client=Mock()),
                        auto_triggered=True,
                        workspace_repo=repo,
                    )


class TestRollbackCompleteBaselineGuard:
    def test_rollback_before_baseline_raises_without_force(self) -> None:
        repo = _RepoStub(
            env_config={"topLevelName": "dev_catalog", "importBaselineSnapshot": "v0.1.0"},
            snapshots={"v0.0.5": {"state": {"catalogs": []}, "operations": []}},
        )
        with pytest.raises(RollbackError, match="before the import baseline"):
            rollback_complete(
                workspace=Path("/tmp"),
                target_env="dev",
                to_snapshot="v0.0.5",
                profile="DEFAULT",
                warehouse_id="wh_1",
                force=False,
                workspace_repo=repo,
            )


class TestPartialRollbackCli:
    def test_partial_cli_uses_injected_repo(self) -> None:
        deployment = {
            "id": "d1",
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
                    "payload": {"catalogId": "cat_1", "name": "demo"},
                }
            ],
        }
        provider = Mock()
        differ = Mock()
        differ.generate_diff_operations.return_value = [
            Operation(
                id="op_1",
                ts="2025-01-01T00:00:00Z",
                provider="unity",
                op="unity.add_catalog",
                target="cat_1",
                payload={"catalogId": "cat_1", "name": "demo"},
            )
        ]
        provider.get_state_differ.return_value = differ

        repo = _RepoStub(
            env_config={"topLevelName": "dev_catalog", "catalogMappings": {"demo": "dev_catalog"}},
            changelog={"ops": []},
            snapshots={
                "v0.1.0": {
                    "state": {"catalogs": [{"name": "demo", "id": "cat_1", "schemas": []}]},
                    "operations": [
                        {
                            "id": "op_1",
                            "ts": "2025-01-01T00:00:00Z",
                            "provider": "unity",
                            "op": "unity.add_catalog",
                            "target": "cat_1",
                            "payload": {"catalogId": "cat_1", "name": "demo"},
                        }
                    ],
                }
            },
            state_result=({"catalogs": [{"name": "demo"}]}, {}, provider, None),
        )

        with patch("schemax.providers.unity.auth.create_databricks_client", return_value=Mock()):
            with patch("schemax.commands.rollback.DeploymentTracker") as tracker_cls:
                tracker_cls.return_value.get_deployment_by_id.return_value = deployment
                with patch("schemax.commands.rollback.rollback_partial") as partial_fn:
                    partial_fn.return_value = SimpleNamespace(
                        success=True,
                        operations_rolled_back=1,
                        error_message=None,
                    )
                    result = run_partial_rollback_cli(
                        workspace_path=Path("/tmp"),
                        deployment_id="d1",
                        target="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_1",
                        no_interaction=True,
                        dry_run=True,
                        workspace_repo=repo,
                    )

        assert result.success is True
        partial_fn.assert_called_once()
        assert partial_fn.call_args.kwargs["auto_triggered"] is True
