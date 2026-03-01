"""CLI routing and command-surface tests."""

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from schemax.cli import cli
from schemax.commands import SQLGenerationError


def _read_contract_fixture(name: str) -> dict[str, object]:
    fixture_path = (
        Path(__file__).resolve().parents[4] / "contracts" / "cli-envelopes" / name
    )
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_cli_help_smoke() -> None:
    """CLI and all subcommands respond to --help (cross-platform smoke)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "SchemaX" in result.output
    for sub in [
        "init",
        "sql",
        "validate",
        "diff",
        "import",
        "apply",
        "rollback",
        "snapshot",
        "runtime-info",
    ]:
        result_sub = runner.invoke(cli, [sub, "--help"])
        assert result_sub.exit_code == 0, f"{sub} --help failed: {result_sub.output}"


def test_runtime_info_json_output() -> None:
    runner = CliRunner()
    contract = _read_contract_fixture("runtime_info.success.json")
    result = runner.invoke(cli, ["runtime-info", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert set(contract.keys()).issubset(set(payload.keys()))
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "runtime-info"
    assert payload["status"] == "success"
    assert isinstance(payload["data"]["supportedCommands"], list)


def test_init_fails_for_unknown_provider(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--provider", "missing", str(temp_workspace)])

    assert result.exit_code == 1
    assert "Provider 'missing' not found" in result.output


def test_init_success_routes_to_storage(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    called: dict[str, object] = {}

    def _run(_self, *, workspace: Path, provider_id: str):
        called["workspace"] = workspace
        called["provider_id"] = provider_id
        return SimpleNamespace(
            success=True,
            data={
                "provider_name": "Unity Catalog",
                "provider_version": "1.0.0",
            },
        )

    monkeypatch.setattr("schemax.cli.InitService.run", _run)

    result = runner.invoke(cli, ["init", "--provider", "unity", str(temp_workspace)])

    assert result.exit_code == 0
    assert called["provider_id"] == "unity"
    assert called["workspace"] == temp_workspace.resolve()


def test_sql_routes_arguments(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _run(_self, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(success=True, data={"sql": "SELECT 1"})

    monkeypatch.setattr("schemax.cli.SqlService.run", _run)

    output = temp_workspace / "out.sql"
    result = runner.invoke(
        cli,
        [
            "sql",
            "--output",
            str(output),
            "--from-version",
            "v0.1.0",
            "--to-version",
            "v0.2.0",
            "--target",
            "dev",
            str(temp_workspace),
        ],
    )

    assert result.exit_code == 0
    assert captured["workspace"] == temp_workspace.resolve()
    assert captured["output"] == output.resolve()
    assert captured["from_version"] == "v0.1.0"
    assert captured["to_version"] == "v0.2.0"
    assert captured["target_env"] == "dev"


def test_sql_returns_error_code_on_command_error(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    def _raise(_self, **kwargs):  # noqa: ARG001
        raise SQLGenerationError("boom")

    monkeypatch.setattr("schemax.cli.SqlService.run", _raise)

    result = runner.invoke(cli, ["sql", str(temp_workspace)])
    assert result.exit_code == 1
    assert "SQL generation failed" in result.output


def test_sql_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.SqlService.run",
        lambda _self, **kwargs: SimpleNamespace(success=True, data={"sql": "SELECT 1"}),
    )

    result = runner.invoke(cli, ["sql", "--json", str(temp_workspace)])
    assert result.exit_code == 0
    contract = _read_contract_fixture("sql.success.json")
    payload = json.loads(result.output)
    assert set(contract.keys()).issubset(set(payload.keys()))
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "sql"
    assert payload["status"] == "success"
    assert payload["data"] == {"sql": "SELECT 1"}


def test_validate_routes_json_option(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _run(_self, *, workspace: Path, json_output: bool):
        captured["workspace"] = workspace
        captured["json_output"] = json_output
        return SimpleNamespace(success=True)

    monkeypatch.setattr("schemax.cli.ValidateService.run", _run)

    result = runner.invoke(cli, ["validate", "--json", str(temp_workspace)])
    assert result.exit_code == 0
    assert captured["workspace"] == temp_workspace.resolve()
    assert captured["json_output"] is True


def test_diff_routes_arguments(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _run(_self, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(success=True, data={"operations": []})

    monkeypatch.setattr("schemax.cli.DiffService.run", _run)

    result = runner.invoke(
        cli,
        [
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            "--show-sql",
            "--show-details",
            "--target",
            "dev",
            str(temp_workspace),
        ],
    )

    assert result.exit_code == 0
    assert captured["from_version"] == "v0.1.0"
    assert captured["to_version"] == "v0.2.0"
    assert captured["show_sql"] is True
    assert captured["show_details"] is True
    assert captured["target_env"] == "dev"


def test_diff_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.cli.DiffService.run",
        lambda _self, **kwargs: SimpleNamespace(
            success=True, data={"operations": [{"op": "unity.add_catalog"}]}
        ),
    )

    result = runner.invoke(
        cli,
        [
            "diff",
            "--from",
            "v0.1.0",
            "--to",
            "v0.2.0",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    contract = _read_contract_fixture("diff.success.json")
    payload = json.loads(result.output)
    assert set(contract.keys()).issubset(set(payload.keys()))
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "diff"
    assert payload["status"] == "success"
    assert payload["data"]["operations"][0]["op"] == "unity.add_catalog"


def test_bundle_contract_not_implemented() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["bundle", "--target", "dev", "--version", "0.1.0"])

    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_apply_uses_status_to_set_exit_code(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.ApplyService.run",
        lambda _self, **kwargs: SimpleNamespace(success=True),
    )
    ok = runner.invoke(
        cli,
        [
            "apply",
            "--target",
            "dev",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh",
            "--dry-run",
            str(temp_workspace),
        ],
    )
    assert ok.exit_code == 0

    monkeypatch.setattr(
        "schemax.cli.ApplyService.run",
        lambda _self, **kwargs: SimpleNamespace(success=False),
    )
    fail = runner.invoke(
        cli,
        [
            "apply",
            "--target",
            "dev",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh",
            "--dry-run",
            str(temp_workspace),
        ],
    )
    assert fail.exit_code == 1


def test_apply_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.cli.ApplyService.run",
        lambda _self, **kwargs: SimpleNamespace(
            success=True, data={"result": {"status": "dry_run"}}
        ),
    )
    result = runner.invoke(
        cli,
        [
            "apply",
            "--target",
            "dev",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh",
            "--dry-run",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    contract = _read_contract_fixture("apply.success.json")
    payload = json.loads(result.output)
    assert set(contract.keys()).issubset(set(payload.keys()))
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "apply"
    assert payload["status"] == "success"
    assert payload["data"]["result"]["status"] == "dry_run"


def test_rollback_requires_minimum_args_for_partial(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["rollback", "--partial", str(temp_workspace)])

    assert result.exit_code == 1
    assert "--deployment required for partial rollback" in result.output


def test_rollback_json_requires_mode(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["rollback", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    contract = _read_contract_fixture("rollback.invalid-args.error.json")
    payload = json.loads(result.output)
    assert set(contract.keys()).issubset(set(payload.keys()))
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "rollback"
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "ROLLBACK_INVALID_ARGS"


def test_import_json_error_for_missing_live_args(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["import", "--json", str(temp_workspace)])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "import"
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "IMPORT_FAILED"


def test_rollback_complete_routes_to_command(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_complete",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=True, operations_rolled_back=0, error_message=None
                )
            }
        ),
    )

    result = runner.invoke(
        cli,
        [
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh",
            "--dry-run",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0


def test_rollback_complete_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.cli.RollbackService.run_complete",
        lambda _self, **kwargs: SimpleNamespace(
            data={
                "result": SimpleNamespace(
                    success=True, operations_rolled_back=2, error_message=None
                )
            }
        ),
    )
    result = runner.invoke(
        cli,
        [
            "rollback",
            "--target",
            "dev",
            "--to-snapshot",
            "v0.1.0",
            "--profile",
            "dev",
            "--warehouse-id",
            "wh",
            "--dry-run",
            "--json",
            str(temp_workspace),
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["schemaVersion"] == "1"
    assert payload["command"] == "rollback"
    assert payload["status"] == "success"
    assert payload["data"]["result"]["operations_rolled_back"] == 2


def test_rollback_error_without_target_does_not_raise_value_error(
    monkeypatch, temp_workspace: Path
) -> None:
    """RollbackError with 'not found' and missing --target must not call get_environment_config.

    Previously, the handler called get_environment_config(project, params['target'] or ''),
    which raised ValueError when target was missing, masking the original RollbackError.
    """
    from schemax.commands.rollback import RollbackError

    def _raise_not_found(*args: object, **kwargs: object) -> None:
        raise RollbackError("Deployment 'deploy_abc' not found in my_catalog.schemax")

    runner = CliRunner()
    monkeypatch.setattr("schemax.cli._handle_rollback_dispatch", _raise_not_found)

    result = runner.invoke(
        cli,
        [
            "rollback",
            "--partial",
            "--deployment",
            "deploy_abc",
            "--profile",
            "p",
            "--warehouse-id",
            "w",
            str(temp_workspace),
        ],
    )

    assert result.exit_code == 1
    assert "not found" in result.output
    assert "Deployment 'deploy_abc' not found" in result.output
    # Bug fix: ValueError from get_environment_config(project, "") must not leak
    assert "Environment '' not found" not in result.output


def test_snapshot_create_no_ops_is_graceful(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.workspace_repo",
        SimpleNamespace(read_changelog=lambda *, workspace: {"ops": []}),
    )

    result = runner.invoke(
        cli,
        ["snapshot", "create", "--name", "No changes", str(temp_workspace)],
    )

    assert result.exit_code == 0
    assert "No uncommitted operations" in result.output


def test_snapshot_rebase_routes_to_command(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.rebase",
        lambda _self, **kwargs: SimpleNamespace(
            success=True,
            data={"result": SimpleNamespace(success=True, applied_count=0, conflict_count=0)},
        ),
    )

    result = runner.invoke(
        cli,
        ["snapshot", "rebase", "v0.2.0", str(temp_workspace)],
    )

    assert result.exit_code == 0
    assert "Successfully rebased" in result.output


def test_snapshot_validate_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.cli.SnapshotService.validate",
        lambda _self, **kwargs: SimpleNamespace(success=True, data={"stale_snapshots": []}),
    )

    result = runner.invoke(
        cli,
        ["snapshot", "validate", "--json", str(temp_workspace)],
    )

    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert parsed["schemaVersion"] == "1"
    assert parsed["command"] == "snapshot.validate"
    assert parsed["status"] == "success"
    assert parsed["data"] == {"stale": [], "count": 0}


def test_workspace_state_json_output(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    fake_provider = SimpleNamespace(
        info=SimpleNamespace(id="unity", name="Unity Catalog", version="1.0.0"),
        capabilities=SimpleNamespace(
            model_dump=lambda: {
                "supported_operations": ["unity.add_catalog"],
                "supported_object_types": ["catalog"],
                "features": {"views": True},
            }
        ),
    )
    fake_repo = SimpleNamespace(
        read_project=lambda *, workspace: {
            "name": "demo",
            "latestSnapshot": None,
            "provider": {"type": "unity", "version": "1.0.0"},
        },
        load_current_state=lambda *, workspace, validate=False: (
            {"catalogs": [{"id": "cat_1", "name": "demo"}]},
            {
                "version": 1,
                "sinceSnapshot": None,
                "ops": [
                    {
                        "id": "op_1",
                        "ts": "2026-01-01T00:00:00Z",
                        "provider": "unity",
                        "op": "unity.add_catalog",
                        "target": "cat_1",
                        "payload": {"name": "demo"},
                    }
                ],
                "lastModified": "2026-01-01T00:00:00Z",
            },
            fake_provider,
            {"errors": [], "warnings": []} if validate else None,
        ),
    )
    monkeypatch.setattr("schemax.cli.workspace_repo", fake_repo)

    result = runner.invoke(
        cli,
        ["workspace-state", "--json", "--validate-dependencies", str(temp_workspace)],
    )

    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "workspace-state"
    assert envelope["status"] == "success"
    payload = envelope["data"]
    assert payload["project"]["name"] == "demo"
    assert payload["provider"]["id"] == "unity"
    assert payload["changelog"]["ops"][0]["op"] == "unity.add_catalog"
    assert payload["validation"] == {"errors": [], "warnings": []}


def test_workspace_state_json_error(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    fake_repo = SimpleNamespace(
        read_project=lambda *, workspace: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    monkeypatch.setattr("schemax.cli.workspace_repo", fake_repo)

    result = runner.invoke(cli, ["workspace-state", "--json", str(temp_workspace)])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["schemaVersion"] == "1"
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "WORKSPACE_STATE_FAILED"
    assert payload["errors"][0]["message"] == "boom"
