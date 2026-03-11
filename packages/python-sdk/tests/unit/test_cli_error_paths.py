"""CLI error-path and JSON-envelope tests for coverage gaps."""

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from schemax.cli import cli

# ── SQL command error paths ────────────────────────────────────────────


def test_sql_json_error_on_workflow_validation_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.domain.errors import WorkflowValidationError

    def _raise(_self, **kwargs):
        raise WorkflowValidationError(
            code="LEGACY_SINGLE_CATALOG_UNSUPPORTED",
            message="Legacy workspace not supported",
        )

    monkeypatch.setattr("schemax.cli.SqlService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["sql", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "LEGACY_SINGLE_CATALOG_UNSUPPORTED"


def test_sql_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("unexpected boom")

    monkeypatch.setattr("schemax.cli.SqlService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["sql", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "UNEXPECTED_ERROR"


def test_sql_non_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("unexpected boom")

    monkeypatch.setattr("schemax.cli.SqlService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["sql", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


# ── Validate command error paths ───────────────────────────────────────


def test_validate_json_on_command_validation_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.commands import ValidationError as CommandValidationError

    def _raise(_self, **kwargs):
        raise CommandValidationError("bad project")

    monkeypatch.setattr("schemax.cli.ValidateService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "VALIDATION_FAILED"


def test_validate_non_json_command_validation_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.commands import ValidationError as CommandValidationError

    def _raise(_self, **kwargs):
        raise CommandValidationError("bad project")

    monkeypatch.setattr("schemax.cli.ValidateService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Validation failed" in result.output


def test_validate_json_workflow_validation_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.domain.errors import WorkflowValidationError

    def _raise(_self, **kwargs):
        raise WorkflowValidationError(code="LEGACY", message="legacy workspace")

    monkeypatch.setattr("schemax.cli.ValidateService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "LEGACY"


def test_validate_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr("schemax.cli.ValidateService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "UNEXPECTED_ERROR"


def test_validate_non_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr("schemax.cli.ValidateService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


# ── Diff command error paths ───────────────────────────────────────────


def test_diff_json_error_on_diff_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.commands import DiffError

    def _raise(_self, **kwargs):
        raise DiffError("diff failed")

    monkeypatch.setattr("schemax.cli.DiffService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["diff", "--from", "v0.1.0", "--to", "v0.2.0", "--json", str(temp_workspace)]
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "DIFF_FAILED"


def test_diff_non_json_diff_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.commands import DiffError

    def _raise(_self, **kwargs):
        raise DiffError("diff failed")

    monkeypatch.setattr("schemax.cli.DiffService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["diff", "--from", "v0.1.0", "--to", "v0.2.0", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Diff generation failed" in result.output


def test_diff_json_workflow_validation_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.domain.errors import WorkflowValidationError

    def _raise(_self, **kwargs):
        raise WorkflowValidationError(code="LEGACY", message="legacy workspace")

    monkeypatch.setattr("schemax.cli.DiffService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["diff", "--from", "v0.1.0", "--to", "v0.2.0", "--json", str(temp_workspace)]
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "LEGACY"


def test_diff_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("schemax.cli.DiffService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["diff", "--from", "v0.1.0", "--to", "v0.2.0", "--json", str(temp_workspace)]
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "UNEXPECTED_ERROR"


def test_diff_non_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(_self, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("schemax.cli.DiffService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["diff", "--from", "v0.1.0", "--to", "v0.2.0", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


# ── Bundle error paths ─────────────────────────────────────────────────


def test_bundle_json_failure(monkeypatch, tmp_path: Path) -> None:
    from schemax.core.storage import ensure_project_file

    ensure_project_file(tmp_path, provider_id="unity")
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "schemax.cli.BundleService.run",
        lambda _self, **kwargs: SimpleNamespace(
            success=False, message="bundle generation failed", data={}
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["bundle", "--json"])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "BUNDLE_FAILED"


def test_bundle_json_exception(monkeypatch, tmp_path: Path) -> None:
    from schemax.core.storage import ensure_project_file

    ensure_project_file(tmp_path, provider_id="unity")
    monkeypatch.chdir(tmp_path)

    def _raise(_self, **kwargs):
        raise RuntimeError("bundle crash")

    monkeypatch.setattr("schemax.cli.BundleService.run", _raise)
    runner = CliRunner()
    result = runner.invoke(cli, ["bundle", "--json"])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "BUNDLE_ERROR"


def test_bundle_non_json_failure(monkeypatch, tmp_path: Path) -> None:
    from schemax.core.storage import ensure_project_file

    ensure_project_file(tmp_path, provider_id="unity")
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "schemax.cli.BundleService.run",
        lambda _self, **kwargs: SimpleNamespace(
            success=False, message="bundle generation failed", data={}
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["bundle"])
    assert result.exit_code == 1
    assert "bundle generation failed" in result.output


# ── Rollback JSON paths ───────────────────────────────────────────────


def test_rollback_json_partial_success(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=True, operations_rolled_back=3, error_message=None
                )
            }
        ),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "success"
    assert payload["data"]["result"]["operations_rolled_back"] == 3


def test_rollback_json_partial_failure(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=False, operations_rolled_back=0, error_message="auto blocked"
                )
            }
        ),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "ROLLBACK_FAILED"


def test_rollback_json_partial_missing_result(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(data=None),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert "missing rollback result" in payload["errors"][0]["message"]


def test_rollback_json_complete_missing_target(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "ROLLBACK_INVALID_ARGS"
    assert "target" in payload["errors"][0]["message"].lower()


def test_rollback_json_complete_missing_warehouse(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--to-snapshot",
            "v0.1.0",
            "--target",
            "dev",
            "--profile",
            "p",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "ROLLBACK_INVALID_ARGS"
    assert "warehouse" in payload["errors"][0]["message"].lower()


def test_rollback_non_json_no_mode_shows_usage(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "Must specify" in result.output


def test_rollback_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(**kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr("schemax.cli._handle_rollback_dispatch", _raise)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["errors"][0]["code"] == "UNEXPECTED_ERROR"


def test_rollback_non_json_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    def _raise(**kwargs):
        raise RuntimeError("unexpected crash")

    monkeypatch.setattr("schemax.cli._handle_rollback_dispatch", _raise)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "Rollback error" in result.output


# ── Rollback non-JSON dispatch paths ──────────────────────────────────


def test_rollback_non_json_partial_success(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=True, operations_rolled_back=2, error_message=None
                )
            }
        ),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    assert "Rolled back 2 operations" in result.output


def test_rollback_non_json_partial_failure(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=False, operations_rolled_back=0, error_message="blocked"
                )
            }
        ),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "Rollback failed" in result.output


def test_rollback_non_json_partial_missing_result(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_partial",
        lambda _self, **kwargs: SimpleNamespace(data=None),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "d1",
            "--target",
            "dev",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "missing rollback result" in result.output


def test_rollback_non_json_complete_missing_target(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            "p",
            "--warehouse-id",
            "wh",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "--target required" in result.output


def test_rollback_non_json_complete_missing_warehouse(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--to-snapshot",
            "v0.1.0",
            "--target",
            "dev",
            "--profile",
            "p",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 1
    assert "--warehouse-id required" in result.output


# ── Snapshot command error paths ──────────────────────────────────────


def test_snapshot_validate_non_json_all_good(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.validate",
        lambda _self, **kwargs: SimpleNamespace(success=True, data={"stale_snapshots": []}),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "validate", str(temp_workspace)])
    assert result.exit_code == 0
    assert "up to date" in result.output


def test_snapshot_validate_non_json_stale(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.validate",
        lambda _self, **kwargs: SimpleNamespace(
            success=True,
            data={
                "stale_snapshots": [
                    {
                        "version": "v0.4.0",
                        "currentBase": "v0.3.0",
                        "shouldBeBase": "v0.3.1",
                        "missing": ["v0.3.1"],
                    }
                ]
            },
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "validate", str(temp_workspace)])
    assert result.exit_code == 1
    assert "stale" in result.output
    assert "v0.4.0" in result.output
    assert "schemax snapshot rebase" in result.output


def test_snapshot_rebase_conflict(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.rebase",
        lambda _self, **kwargs: SimpleNamespace(
            success=False,
            data={
                "result": SimpleNamespace(
                    success=False, applied_count=1, conflict_count=2, conflict_log_path="/tmp/log"
                )
            },
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "rebase", "v0.4.0", str(temp_workspace)])
    assert result.exit_code == 1
    assert "conflicts" in result.output


def test_snapshot_rebase_rebase_error(monkeypatch, temp_workspace: Path) -> None:
    from schemax.commands.snapshot_rebase import RebaseError

    monkeypatch.setattr(
        "schemax.cli.SnapshotService.rebase",
        lambda _self, **kwargs: (_ for _ in ()).throw(RebaseError("no base")),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "rebase", "v0.4.0", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Rebase failed" in result.output


def test_snapshot_rebase_unexpected_error(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.rebase",
        lambda _self, **kwargs: (_ for _ in ()).throw(RuntimeError("disk error")),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "rebase", "v0.4.0", str(temp_workspace)])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


# ── Snapshot create paths ─────────────────────────────────────────────


def test_snapshot_create_with_ops(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.workspace_repo",
        SimpleNamespace(read_changelog=lambda *, workspace: {"ops": [{"id": "op_1"}]}),
    )
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.create",
        lambda _self, **kwargs: SimpleNamespace(
            success=True,
            data={
                "project": {"snapshots": [{"version": "v0.1.0"}]},
                "snapshot": {
                    "version": "v0.1.0",
                    "name": "test",
                    "comment": "a comment",
                    "tags": ["t1"],
                    "operations": [{"id": "op_1"}],
                },
            },
        ),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "snapshot",
            "create",
            "--name",
            "test",
            "--comment",
            "a comment",
            "--tags",
            "t1",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    assert "Snapshot created" in result.output
    assert "v0.1.0" in result.output
    assert "a comment" in result.output
    assert "t1" in result.output


def test_snapshot_create_file_not_found(monkeypatch, temp_workspace: Path) -> None:
    monkeypatch.setattr(
        "schemax.cli.workspace_repo",
        SimpleNamespace(
            read_changelog=lambda *, workspace: (_ for _ in ()).throw(
                FileNotFoundError("no project")
            )
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["snapshot", "create", "--name", "test", str(temp_workspace)])
    assert result.exit_code == 1
    assert "SchemaX project" in result.output


# ── Workspace state paths ─────────────────────────────────────────────


def test_workspace_state_non_json(monkeypatch, temp_workspace: Path) -> None:
    fake_provider = SimpleNamespace(
        info=SimpleNamespace(id="unity", name="Unity Catalog", version="1.0.0"),
        capabilities=SimpleNamespace(
            model_dump=lambda: {},
        ),
    )
    fake_repo = SimpleNamespace(
        read_project=lambda *, workspace: {"name": "demo", "latestSnapshot": None, "targets": {}},
        load_current_state=lambda *, workspace, validate=False, scope=None: (
            {"catalogs": []},
            {"ops": [], "version": 1},
            fake_provider,
            None,
        ),
    )
    monkeypatch.setattr("schemax.cli.workspace_repo", fake_repo)
    runner = CliRunner()
    result = runner.invoke(cli, ["workspace-state", str(temp_workspace)])
    assert result.exit_code == 0
    assert "Loaded workspace state" in result.output


def test_workspace_state_state_only_mode(monkeypatch, temp_workspace: Path) -> None:
    fake_provider = SimpleNamespace(
        info=SimpleNamespace(id="unity", name="Unity Catalog", version="1.0.0"),
        capabilities=SimpleNamespace(
            model_dump=lambda: {},
            supported_operations=[],
            supported_object_types=[],
            features={},
            hierarchy=None,
        ),
    )
    fake_repo = SimpleNamespace(
        read_project=lambda *, workspace: {"name": "demo", "latestSnapshot": None, "targets": {}},
        load_current_state=lambda *, workspace, validate=False, scope=None: (
            {"catalogs": []},
            {"ops": [{"id": "op_1"}], "version": 1},
            fake_provider,
            None,
        ),
    )
    monkeypatch.setattr("schemax.cli.workspace_repo", fake_repo)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["workspace-state", "--json", "--payload-mode", "state-only", str(temp_workspace)]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["data"]["changelog"]["ops"] == []
    assert payload["data"]["changelog"]["opsCount"] == 1


# ── Serialize helpers ─────────────────────────────────────────────────


def test_serialize_operation_mapping() -> None:
    from schemax.cli import _serialize_operation

    result = _serialize_operation({"id": "op_1", "op": "unity.add_catalog"})
    assert result == {"id": "op_1", "op": "unity.add_catalog"}


def test_serialize_operation_model() -> None:
    from schemax.cli import _serialize_operation
    from schemax.providers.base.operations import Operation

    op = Operation(
        id="op_1",
        ts="2025-01-01T00:00:00Z",
        provider="unity",
        op="unity.add_catalog",
        target="c1",
        payload={},
    )
    result = _serialize_operation(op)
    assert result["id"] == "op_1"


def test_serialize_operation_unsupported() -> None:
    import pytest

    from schemax.cli import _serialize_operation

    with pytest.raises(TypeError, match="Unsupported"):
        _serialize_operation("not a valid operation")


def test_serialize_provider_capabilities_with_hierarchy() -> None:
    from schemax.cli import _serialize_provider_capabilities
    from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel

    levels = [
        HierarchyLevel(
            name="catalog", display_name="Catalog", plural_name="catalogs", is_container=True
        ),
        HierarchyLevel(
            name="table", display_name="Table", plural_name="tables", is_container=False
        ),
    ]
    hierarchy = Hierarchy(levels)
    caps = SimpleNamespace(
        supported_operations=["add_catalog"],
        supported_object_types=["catalog"],
        features={"views": True},
        hierarchy=hierarchy,
    )
    result = _serialize_provider_capabilities(caps)
    assert len(result["hierarchy"]["levels"]) == 2
    assert result["hierarchy"]["levels"][0]["name"] == "catalog"
    assert result["features"] == {"views": True}


def test_serialize_provider_capabilities_no_hierarchy() -> None:
    from schemax.cli import _serialize_provider_capabilities

    caps = SimpleNamespace(
        supported_operations=[],
        supported_object_types=[],
        features={},
        hierarchy=None,
    )
    result = _serialize_provider_capabilities(caps)
    assert result["hierarchy"]["levels"] == []
