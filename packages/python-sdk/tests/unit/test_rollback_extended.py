"""
Extended tests for rollback.py — covers internal functions for coverage.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from schemax.commands.rollback import (
    RollbackError,
    RollbackResult,
    _build_pre_deployment_state,
    _check_already_at_target_version,
    _classify_complete_rollback_safety,
    _enforce_import_baseline_guard,
    rollback_complete,
)
from schemax.providers.base.executor import ExecutionResult, StatementResult
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport
from schemax.providers.base.sql_generator import SQLGenerationResult, StatementInfo


# ── _enforce_import_baseline_guard ─────────────────────────────────────


class TestEnforceImportBaselineGuard:
    def test_no_baseline_noop(self):
        _enforce_import_baseline_guard({}, "v0.1.0", force=False, no_interaction=False)

    def test_target_after_baseline_noop(self):
        env_config = {"importBaselineSnapshot": "v0.1.0"}
        _enforce_import_baseline_guard(env_config, "v0.2.0", force=False, no_interaction=False)

    def test_target_equal_baseline_noop(self):
        env_config = {"importBaselineSnapshot": "v0.1.0"}
        _enforce_import_baseline_guard(env_config, "v0.1.0", force=False, no_interaction=False)

    def test_target_before_baseline_raises_without_force(self):
        env_config = {"importBaselineSnapshot": "v0.2.0"}
        with pytest.raises(RollbackError, match="before the import baseline"):
            _enforce_import_baseline_guard(env_config, "v0.1.0", force=False, no_interaction=False)

    def test_target_before_baseline_force_no_interaction(self):
        env_config = {"importBaselineSnapshot": "v0.2.0"}
        # force=True + no_interaction=True → should pass
        _enforce_import_baseline_guard(env_config, "v0.1.0", force=True, no_interaction=True)

    def test_invalid_version_strings_noop(self):
        env_config = {"importBaselineSnapshot": "not-a-version"}
        # Invalid versions → ValueError caught → noop
        _enforce_import_baseline_guard(env_config, "also-invalid", force=False, no_interaction=False)

    @patch("schemax.commands.rollback.Confirm.ask", return_value=False)
    def test_force_user_cancels(self, mock_ask):
        env_config = {"importBaselineSnapshot": "v0.2.0"}
        with pytest.raises(RollbackError, match="cancelled by user"):
            _enforce_import_baseline_guard(
                env_config, "v0.1.0", force=True, no_interaction=False
            )


# ── _check_already_at_target_version ───────────────────────────────────


class TestCheckAlreadyAtTargetVersion:
    def test_returns_none_when_db_query_fails(self):
        tracker = Mock()
        tracker.get_latest_deployment.side_effect = RuntimeError("no connection")
        result = _check_already_at_target_version(tracker, "dev", "v0.1.0")
        assert result is None

    def test_returns_none_when_no_deployment(self):
        tracker = Mock()
        tracker.get_latest_deployment.return_value = None
        result = _check_already_at_target_version(tracker, "dev", "v0.1.0")
        assert result is None

    def test_returns_none_when_version_differs(self):
        tracker = Mock()
        tracker.get_latest_deployment.return_value = {"version": "v0.2.0"}
        result = _check_already_at_target_version(tracker, "dev", "v0.1.0")
        assert result is None

    def test_returns_success_when_already_at_target(self):
        tracker = Mock()
        tracker.get_latest_deployment.return_value = {"version": "v0.1.0"}
        result = _check_already_at_target_version(tracker, "dev", "v0.1.0")
        assert result is not None
        assert result.success is True
        assert result.operations_rolled_back == 0


# ── _classify_complete_rollback_safety ─────────────────────────────────


class TestClassifyCompleteRollbackSafety:
    def _make_op(self, op_type: str = "unity.drop_table") -> Operation:
        return Operation(
            id="op_1",
            ts="2025-01-01T00:00:00Z",
            provider="unity",
            op=op_type,
            target="t1",
            payload={},
        )

    def test_safe_operations(self):
        validator = Mock()
        validator.validate.return_value = SafetyReport(
            level=SafetyLevel.SAFE, reason="safe", data_at_risk=0
        )
        safe, risky, destructive = _classify_complete_rollback_safety(
            [self._make_op()], validator, None
        )
        assert len(safe) == 1
        assert len(risky) == 0
        assert len(destructive) == 0

    def test_risky_operations(self):
        validator = Mock()
        validator.validate.return_value = SafetyReport(
            level=SafetyLevel.RISKY, reason="risky", data_at_risk=0
        )
        safe, risky, destructive = _classify_complete_rollback_safety(
            [self._make_op()], validator, None
        )
        assert len(safe) == 0
        assert len(risky) == 1

    def test_destructive_operations(self):
        validator = Mock()
        validator.validate.return_value = SafetyReport(
            level=SafetyLevel.DESTRUCTIVE, reason="destructive", data_at_risk=100
        )
        safe, risky, destructive = _classify_complete_rollback_safety(
            [self._make_op()], validator, None
        )
        assert len(destructive) == 1

    def test_validation_error_classified_as_risky(self):
        validator = Mock()
        validator.validate.side_effect = RuntimeError("unexpected")
        safe, risky, destructive = _classify_complete_rollback_safety(
            [self._make_op()], validator, None
        )
        assert len(risky) == 1
        assert "Validation failed" in risky[0][1].reason

    def test_mixed_operations(self):
        validator = Mock()
        reports = [
            SafetyReport(level=SafetyLevel.SAFE, reason="safe", data_at_risk=0),
            SafetyReport(level=SafetyLevel.RISKY, reason="risky", data_at_risk=0),
            SafetyReport(level=SafetyLevel.DESTRUCTIVE, reason="destructive", data_at_risk=10),
        ]
        validator.validate.side_effect = reports
        ops = [self._make_op() for _ in range(3)]
        # Fix unique IDs
        ops[1] = Operation(id="op_2", ts="2025-01-01T00:00:00Z", provider="unity", op="unity.drop_schema", target="s1", payload={})
        ops[2] = Operation(id="op_3", ts="2025-01-01T00:00:00Z", provider="unity", op="unity.drop_catalog", target="c1", payload={})
        safe, risky, destructive = _classify_complete_rollback_safety(ops, validator, None)
        assert len(safe) == 1
        assert len(risky) == 1
        assert len(destructive) == 1


# ── _build_pre_deployment_state ────────────────────────────────────────


class _RepoStub:
    def __init__(self, snapshots: dict | None = None):
        self._snapshots = snapshots or {}

    def read_snapshot(self, *, workspace, version):
        return self._snapshots[version]


class TestBuildPreDeploymentState:
    def test_no_previous_deployment_returns_initial(self):
        provider = Mock()
        provider.create_initial_state.return_value = {"catalogs": []}
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = None
        tracker.get_previous_deployment.return_value = None
        deployment = {"previousDeploymentId": None}

        state = _build_pre_deployment_state(
            Path("/tmp"), _RepoStub(), provider, tracker, "dev", "d1", deployment
        )
        assert state == {"catalogs": []}
        provider.create_initial_state.assert_called()

    def test_previous_deployment_without_ops_details_returns_initial(self):
        provider = Mock()
        provider.create_initial_state.return_value = {"catalogs": []}
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = {"id": "d0"}  # No opsDetails key
        deployment = {"previousDeploymentId": "d0"}

        state = _build_pre_deployment_state(
            Path("/tmp"), _RepoStub(), provider, tracker, "dev", "d1", deployment
        )
        provider.create_initial_state.assert_called()

    def test_previous_deployment_with_ops_details(self):
        provider = Mock()
        provider.create_initial_state.return_value = {"catalogs": []}
        provider.apply_operations.return_value = {"catalogs": [{"id": "c1"}]}

        prev_deployment = {
            "id": "d0",
            "fromVersion": "v0.1.0",
            "opsDetails": [
                {
                    "id": "prev_op_1",
                    "status": "success",
                    "executionOrder": 1,
                    "type": "unity.add_catalog",
                    "target": "c1",
                    "payload": {"name": "cat"},
                }
            ],
        }
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = prev_deployment
        deployment = {"previousDeploymentId": "d0"}

        repo = _RepoStub(snapshots={"v0.1.0": {"state": {"catalogs": []}}})
        state = _build_pre_deployment_state(
            Path("/tmp"), repo, provider, tracker, "dev", "d1", deployment
        )
        provider.apply_operations.assert_called_once()

    def test_uses_get_previous_deployment_fallback(self):
        provider = Mock()
        provider.create_initial_state.return_value = {"catalogs": []}
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = None
        tracker.get_previous_deployment.return_value = {
            "id": "d0",
            "fromVersion": None,
            "opsDetails": [
                {
                    "id": "prev_op_1",
                    "status": "success",
                    "executionOrder": 1,
                    "type": "unity.add_catalog",
                    "target": "c1",
                    "payload": {"name": "cat"},
                }
            ],
        }
        provider.apply_operations.return_value = {"catalogs": [{"id": "c1"}]}
        deployment = {"previousDeploymentId": "d_missing"}

        state = _build_pre_deployment_state(
            Path("/tmp"), _RepoStub(), provider, tracker, "dev", "d1", deployment
        )
        tracker.get_previous_deployment.assert_called_once()
        provider.apply_operations.assert_called_once()

    def test_filters_unsuccessful_ops(self):
        provider = Mock()
        provider.create_initial_state.return_value = {"catalogs": []}
        provider.apply_operations.return_value = {"catalogs": [{"id": "c1"}]}

        prev_deployment = {
            "id": "d0",
            "fromVersion": None,
            "opsDetails": [
                {"id": "op_ok", "status": "success", "executionOrder": 1, "type": "unity.add_catalog", "target": "c1", "payload": {}},
                {"id": "op_fail", "status": "failed", "executionOrder": 2, "type": "unity.add_schema", "target": "s1", "payload": {}},
            ],
        }
        tracker = Mock()
        tracker.get_deployment_by_id.return_value = prev_deployment
        deployment = {"previousDeploymentId": "d0"}

        _build_pre_deployment_state(
            Path("/tmp"), _RepoStub(), provider, tracker, "dev", "d1", deployment
        )
        # Only successful ops should be passed
        call_args = provider.apply_operations.call_args
        ops_passed = call_args[0][1]
        assert len(ops_passed) == 1


# ── RollbackResult dataclass ──────────────────────────────────────────


class TestRollbackResult:
    def test_success_result(self):
        r = RollbackResult(success=True, operations_rolled_back=3)
        assert r.success is True
        assert r.error_message is None

    def test_failure_result(self):
        r = RollbackResult(success=False, operations_rolled_back=0, error_message="fail")
        assert r.error_message == "fail"


# ── rollback_complete (higher-level) ───────────────────────────────────


class _FullRepoStub:
    def __init__(self, *, env_config=None, snapshots=None, project=None, changelog=None, state_result=None):
        self._env_config = env_config or {"topLevelName": "dev_cat"}
        self._snapshots = snapshots or {}
        self._project = project or {"name": "test"}
        self._changelog = changelog or {"ops": []}
        self._state_result = state_result

    def read_project(self, *, workspace):
        return self._project

    def get_environment_config(self, *, project, environment):
        return self._env_config

    def load_current_state(self, *, workspace, validate=False):
        return self._state_result

    def read_snapshot(self, *, workspace, version):
        return self._snapshots[version]

    def read_changelog(self, *, workspace):
        return self._changelog


class TestRollbackCompleteEdgeCases:
    def test_rollback_before_baseline_with_force_raises_on_user_cancel(self):
        repo = _FullRepoStub(
            env_config={"topLevelName": "dev_cat", "importBaselineSnapshot": "v0.2.0"},
            snapshots={"v0.0.1": {"state": {"catalogs": []}, "operations": []}},
        )
        with pytest.raises(RollbackError, match="before the import baseline"):
            rollback_complete(
                workspace=Path("/tmp"),
                target_env="dev",
                to_snapshot="v0.0.1",
                profile="DEFAULT",
                warehouse_id="wh_1",
                force=False,
                workspace_repo=repo,
            )
