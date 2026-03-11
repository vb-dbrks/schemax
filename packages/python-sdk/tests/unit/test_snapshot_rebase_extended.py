"""
Extended tests for snapshot rebase — covers internal functions for coverage.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from schemax.commands.snapshot_rebase import (
    ConflictError,
    RebaseContext,
    RebaseError,
    RebaseResult,
    ReplayResult,
    _clear_changelog,
    _has_conflict,
    _load_rebase_context,
    _persist_conflict_result,
    _persist_success_snapshot,
    _print_conflict_summary,
    _remove_snapshot_from_project,
    _replay_operations_on_base,
    _save_applied_ops_to_changelog,
    _save_conflict_log,
    _save_to_temp_changelog,
    _show_conflict_details,
    rebase_snapshot,
)
from schemax.providers.base.operations import Operation


# ── Helpers ────────────────────────────────────────────────────────────


class _FakeRepo:
    """Minimal repo stub for rebase tests."""

    def __init__(
        self,
        *,
        project: dict | None = None,
        snapshots: dict[str, dict] | None = None,
        changelog: dict | None = None,
        state_result: tuple | None = None,
    ):
        self._project = project or {"name": "t", "snapshots": [], "latestSnapshot": None}
        self._snapshots = snapshots or {}
        self._changelog = changelog or {"ops": []}
        self._state_result = state_result
        self.written_snapshots: list[dict] = []
        self.written_projects: list[dict] = []
        self.written_changelogs: list[dict] = []

    def read_project(self, *, workspace: Path) -> dict:
        del workspace
        return self._project

    def write_project(self, *, workspace: Path, project: dict) -> None:
        del workspace
        self._project = project
        self.written_projects.append(project)

    def read_snapshot(self, *, workspace: Path, version: str) -> dict:
        del workspace
        return self._snapshots[version]

    def write_snapshot(self, *, workspace: Path, snapshot: dict) -> None:
        del workspace
        self.written_snapshots.append(snapshot)

    def read_changelog(self, *, workspace: Path) -> dict:
        del workspace
        return self._changelog

    def write_changelog(self, *, workspace: Path, changelog: dict) -> None:
        del workspace
        self._changelog = changelog
        self.written_changelogs.append(changelog)

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple:
        del workspace, validate
        return self._state_result  # type: ignore[return-value]

    def snapshot_file_path(self, *, workspace: Path, version: str) -> Path:
        return workspace / ".schemax" / "snapshots" / f"{version}.json"


def _op_dict(op: str = "unity.add_column", target: str = "col_1", **extra) -> dict:
    return {
        "id": extra.pop("id", "op_1"),
        "ts": "2025-01-01T00:00:00Z",
        "provider": "unity",
        "op": op,
        "target": target,
        "payload": extra.pop("payload", {}),
        **extra,
    }


def _operation(op: str = "unity.add_column", target: str = "col_1", **extra) -> Operation:
    return Operation(**_op_dict(op, target, **extra))


# ── _has_conflict ──────────────────────────────────────────────────────


class TestHasConflict:
    def test_conflict_when_same_target(self):
        operation = _operation(target="col_x")
        base_ops = [{"target": "col_x", "op": "unity.update_column"}]
        assert _has_conflict(operation, base_ops) is True

    def test_no_conflict_different_targets(self):
        operation = _operation(target="col_x")
        base_ops = [{"target": "col_y", "op": "unity.update_column"}]
        assert _has_conflict(operation, base_ops) is False

    def test_no_conflict_empty_base(self):
        assert _has_conflict(_operation(), []) is False

    def test_conflict_multiple_base_ops(self):
        operation = _operation(target="tbl_1")
        base_ops = [
            {"target": "tbl_2", "op": "unity.add_table"},
            {"target": "tbl_1", "op": "unity.update_table"},
        ]
        assert _has_conflict(operation, base_ops) is True


# ── _load_rebase_context ──────────────────────────────────────────────


class TestLoadRebaseContext:
    def test_raises_when_no_previous_snapshot(self):
        repo = _FakeRepo(snapshots={"v0.2.0": {"state": {}, "operations": []}})
        with pytest.raises(RebaseError, match="no previousSnapshot"):
            _load_rebase_context(Path("/tmp"), "v0.2.0", None, repo)

    def test_raises_when_no_latest_snapshot(self):
        repo = _FakeRepo(
            snapshots={"v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": []}},
            project={"latestSnapshot": None, "snapshots": []},
        )
        with pytest.raises(RebaseError, match="No snapshots available"):
            _load_rebase_context(Path("/tmp"), "v0.2.0", None, repo)

    def test_no_rebase_needed_same_base(self):
        repo = _FakeRepo(
            snapshots={"v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": []}}
        )
        ctx = _load_rebase_context(Path("/tmp"), "v0.2.0", "v0.1.0", repo)
        assert ctx.rebase_needed is False
        assert ctx.old_base == "v0.1.0"
        assert ctx.new_base_version == "v0.1.0"

    def test_rebase_needed(self):
        ops = [_op_dict()]
        repo = _FakeRepo(
            snapshots={
                "v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": ops}
            }
        )
        ctx = _load_rebase_context(Path("/tmp"), "v0.2.0", "v0.1.5", repo)
        assert ctx.rebase_needed is True
        assert ctx.old_base == "v0.1.0"
        assert ctx.new_base_version == "v0.1.5"
        assert ctx.feature_ops == ops

    def test_auto_detects_latest_snapshot(self):
        repo = _FakeRepo(
            snapshots={
                "v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": []}
            },
            project={"latestSnapshot": "v0.1.5", "snapshots": []},
        )
        ctx = _load_rebase_context(Path("/tmp"), "v0.2.0", None, repo)
        assert ctx.rebase_needed is True
        assert ctx.new_base_version == "v0.1.5"


# ── _replay_operations_on_base ─────────────────────────────────────────


class TestReplayOperationsOnBase:
    def test_all_ops_applied_no_conflict(self):
        provider = Mock()
        provider.apply_operations.return_value = {"catalogs": [{"id": "c1"}]}
        ops = [_op_dict(target="col_1"), _op_dict(target="col_2", id="op_2")]
        result = _replay_operations_on_base(ops, {"catalogs": []}, [], provider, "v0.1.0")
        assert result.success is True
        assert len(result.applied_ops) == 2
        assert result.conflicting_ops == []

    def test_conflict_on_same_target(self):
        provider = Mock()
        provider.apply_operations.return_value = {"catalogs": []}
        ops = [_op_dict(target="col_x")]
        base_ops = [{"target": "col_x", "op": "unity.update_column"}]
        result = _replay_operations_on_base(ops, {"catalogs": []}, base_ops, provider, "v0.1.0")
        assert result.success is False
        assert len(result.conflicting_ops) == 1
        assert "changed in v0.1.0" in result.conflicting_ops[0]["reason"]

    def test_subsequent_ops_blocked_after_conflict(self):
        provider = Mock()
        provider.apply_operations.return_value = {"catalogs": []}
        ops = [
            _op_dict(target="col_x", id="op_1"),
            _op_dict(target="col_y", id="op_2"),
            _op_dict(target="col_z", id="op_3"),
        ]
        base_ops = [{"target": "col_x", "op": "unity.update_column"}]
        result = _replay_operations_on_base(ops, {"catalogs": []}, base_ops, provider, "v0.1.0")
        assert result.success is False
        assert len(result.conflicting_ops) == 3
        assert result.conflicting_ops[1]["reason"] == "Blocked by previous conflict"
        assert result.conflicting_ops[2]["reason"] == "Blocked by previous conflict"

    def test_provider_error_treated_as_conflict(self):
        provider = Mock()
        provider.apply_operations.side_effect = RuntimeError("boom")
        ops = [_op_dict()]
        result = _replay_operations_on_base(ops, {"catalogs": []}, [], provider, "v0.1.0")
        assert result.success is False
        assert "Error: boom" in result.conflicting_ops[0]["reason"]


# ── changelog helpers ───────────────────────────────────────────────────


class TestChangelogHelpers:
    def test_clear_changelog(self):
        repo = _FakeRepo(changelog={"ops": [{"id": "x"}], "_rebase_temp": True, "_rebase_message": "msg"})
        _clear_changelog(Path("/tmp"), "v0.1.0", repo)
        cl = repo._changelog
        assert cl["ops"] == []
        assert cl["sinceSnapshot"] == "v0.1.0"
        assert "_rebase_temp" not in cl
        assert "_rebase_message" not in cl

    def test_save_applied_ops(self):
        repo = _FakeRepo(changelog={"ops": [], "_rebase_temp": True, "_rebase_message": "old"})
        ops = [_op_dict()]
        _save_applied_ops_to_changelog(Path("/tmp"), ops, "v0.2.0", repo)
        cl = repo._changelog
        assert cl["ops"] == ops
        assert cl["sinceSnapshot"] == "v0.2.0"
        assert "_rebase_temp" not in cl

    def test_save_to_temp_changelog(self):
        repo = _FakeRepo()
        ops = [_op_dict()]
        _save_to_temp_changelog(Path("/tmp"), ops, "v0.2.0", repo)
        cl = repo._changelog
        assert cl["_rebase_temp"] is True
        assert cl["ops"] == ops


# ── _save_conflict_log ──────────────────────────────────────────────────


class TestSaveConflictLog:
    def test_writes_json_file(self, tmp_path):
        path = _save_conflict_log(
            tmp_path, "v0.4.0", "v0.3.0", "v0.3.1",
            applied_ops=[_op_dict()],
            conflicting_ops=[{"operation": _op_dict(id="op_c"), "index": 1, "reason": "boom"}],
        )
        full = tmp_path / path
        assert full.exists()
        data = json.loads(full.read_text())
        assert data["snapshot_version"] == "v0.4.0"
        assert data["old_base"] == "v0.3.0"
        assert data["new_base"] == "v0.3.1"
        assert len(data["applied_operations"]) == 1
        assert len(data["conflicting_operations"]) == 1


# ── _remove_snapshot_from_project ───────────────────────────────────────


class TestRemoveSnapshotFromProject:
    def test_removes_matching_version(self):
        repo = _FakeRepo(
            project={
                "snapshots": [{"version": "v0.1.0"}, {"version": "v0.2.0"}],
                "latestSnapshot": "v0.2.0",
            }
        )
        _remove_snapshot_from_project(Path("/tmp"), "v0.2.0", repo)
        assert len(repo._project["snapshots"]) == 1
        assert repo._project["latestSnapshot"] == "v0.1.0"

    def test_clears_latest_when_last(self):
        repo = _FakeRepo(
            project={"snapshots": [{"version": "v0.1.0"}], "latestSnapshot": "v0.1.0"}
        )
        _remove_snapshot_from_project(Path("/tmp"), "v0.1.0", repo)
        assert repo._project["snapshots"] == []
        assert repo._project["latestSnapshot"] is None

    def test_noop_when_version_not_found(self):
        repo = _FakeRepo(
            project={"snapshots": [{"version": "v0.1.0"}], "latestSnapshot": "v0.1.0"}
        )
        _remove_snapshot_from_project(Path("/tmp"), "v9.9.9", repo)
        assert len(repo._project["snapshots"]) == 1


# ── _persist_success_snapshot ───────────────────────────────────────────


class TestPersistSuccessSnapshot:
    def test_writes_rebased_snapshot(self):
        repo = _FakeRepo(project={"snapshots": [], "latestSnapshot": "v0.1.0"})
        snapshot = {
            "id": "snap_1",
            "name": "test",
            "ts": "2025-01-01T00:00:00Z",
            "createdBy": "user",
            "previousSnapshot": "v0.1.0",
            "hash": "abc",
            "tags": ["t1"],
            "comment": "hello",
        }
        _persist_success_snapshot(
            Path("/tmp"),
            snapshot,
            "v0.2.0",
            "v0.1.5",
            {"catalogs": [{"id": "c1"}]},
            [_op_dict()],
            repo,
        )
        assert len(repo.written_snapshots) == 1
        written = repo.written_snapshots[0]
        assert written["previousSnapshot"] == "v0.1.5"
        assert written["rebasedFrom"] == "v0.1.0"
        assert "rebasedAt" in written
        assert len(repo._project["snapshots"]) == 1


# ── _persist_conflict_result ────────────────────────────────────────────


class TestPersistConflictResult:
    def test_saves_applied_ops_and_conflict_log(self, tmp_path):
        repo = _FakeRepo()
        applied = [_op_dict()]
        conflicting = [{"operation": _op_dict(id="op_c"), "index": 1, "reason": "boom"}]
        path = _persist_conflict_result(
            tmp_path, applied, conflicting, "v0.1.5", "v0.2.0", "v0.1.0", repo
        )
        assert repo._changelog["ops"] == applied
        assert (tmp_path / path).exists()

    def test_clears_changelog_when_no_applied(self, tmp_path):
        repo = _FakeRepo()
        conflicting = [{"operation": _op_dict(id="op_c"), "index": 0, "reason": "boom"}]
        _persist_conflict_result(tmp_path, [], conflicting, "v0.1.5", "v0.2.0", "v0.1.0", repo)
        assert repo._changelog["ops"] == []


# ── rebase_snapshot (top-level) ─────────────────────────────────────────


class TestRebaseSnapshotTopLevel:
    def test_no_rebase_needed(self):
        repo = _FakeRepo(
            snapshots={"v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": []}}
        )
        result = rebase_snapshot(Path("/tmp"), "v0.2.0", "v0.1.0", workspace_repo=repo)
        assert result.success is True
        assert result.message == "No rebase needed"

    def test_successful_rebase(self, tmp_path):
        # Set up snapshot file so unlink works
        snap_dir = tmp_path / ".schemax" / "snapshots"
        snap_dir.mkdir(parents=True)
        (snap_dir / "v0.2.0.json").write_text("{}")

        provider = Mock()
        provider.apply_operations.return_value = {"catalogs": [{"id": "c1"}]}

        repo = _FakeRepo(
            snapshots={
                "v0.2.0": {
                    "id": "s1",
                    "name": "test",
                    "ts": "2025-01-01T00:00:00Z",
                    "createdBy": "user",
                    "previousSnapshot": "v0.1.0",
                    "state": {"catalogs": []},
                    "operations": [_op_dict(target="col_new")],
                    "hash": "abc",
                    "tags": [],
                    "comment": "",
                },
                "v0.1.5": {
                    "state": {"catalogs": []},
                    "operations": [_op_dict(target="col_other", id="base_op")],
                },
            },
            project={
                "snapshots": [{"version": "v0.2.0"}],
                "latestSnapshot": "v0.2.0",
            },
            state_result=({"catalogs": []}, {}, provider, None),
        )
        result = rebase_snapshot(tmp_path, "v0.2.0", "v0.1.5", workspace_repo=repo)
        assert result.success is True
        assert result.applied_count == 1

    def test_wraps_unexpected_error(self):
        repo = _FakeRepo(
            snapshots={"v0.2.0": {"previousSnapshot": "v0.1.0", "state": {}, "operations": []}}
        )
        # Break the repo so an unexpected exception happens during rebase steps
        repo.read_snapshot = Mock(side_effect=RuntimeError("disk fail"))
        with pytest.raises(RebaseError, match="disk fail"):
            rebase_snapshot(Path("/tmp"), "v0.2.0", "v0.1.5", workspace_repo=repo)

    def test_conflict_returns_failure(self, tmp_path):
        snap_dir = tmp_path / ".schemax" / "snapshots"
        snap_dir.mkdir(parents=True)
        (snap_dir / "v0.2.0.json").write_text("{}")

        provider = Mock()
        provider.apply_operations.return_value = {"catalogs": []}

        repo = _FakeRepo(
            snapshots={
                "v0.2.0": {
                    "id": "s1",
                    "name": "test",
                    "ts": "2025-01-01T00:00:00Z",
                    "createdBy": "user",
                    "previousSnapshot": "v0.1.0",
                    "state": {"catalogs": []},
                    "operations": [_op_dict(target="col_x")],
                    "hash": "abc",
                    "tags": [],
                    "comment": "",
                },
                "v0.1.5": {
                    "state": {"catalogs": []},
                    "operations": [{"target": "col_x", "op": "unity.update_column"}],
                },
            },
            project={
                "snapshots": [{"version": "v0.2.0"}],
                "latestSnapshot": "v0.2.0",
            },
            state_result=({"catalogs": []}, {}, provider, None),
        )
        result = rebase_snapshot(tmp_path, "v0.2.0", "v0.1.5", workspace_repo=repo)
        assert result.success is False
        assert result.conflict_count >= 1
        assert result.conflict_log_path is not None


# ── console output helpers (exercise for coverage) ──────────────────────


class TestConsolePrinting:
    def test_show_conflict_details_runs(self, capsys):
        conflict = {"operation": {"op": "unity.add_column", "target": "col_x"}, "reason": "boom"}
        _show_conflict_details(conflict, "v0.1.5")
        # Just verifies no exceptions; output goes to rich console

    def test_print_conflict_summary_runs(self, capsys):
        conflicts = [{"operation": {"op": "unity.add_column", "target": "col_x"}, "reason": "boom"}]
        _print_conflict_summary(conflicts, [_op_dict()], "/tmp/log.json", "v0.1.5", "v0.2.0")

    def test_print_conflict_summary_no_applied(self, capsys):
        conflicts = [{"operation": {"op": "unity.add_column", "target": "col_x"}, "reason": "boom"}]
        _print_conflict_summary(conflicts, [], "/tmp/log.json", "v0.1.5", "v0.2.0")
