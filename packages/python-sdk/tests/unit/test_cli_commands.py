"""CLI routing and command-surface tests."""

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from schemax.cli import cli
from schemax.commands import SQLGenerationError
from schemax.providers.registry import ProviderRegistry


def test_init_fails_for_unknown_provider(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--provider", "missing", str(temp_workspace)])

    assert result.exit_code == 1
    assert "Provider 'missing' not found" in result.output


def test_init_success_routes_to_storage(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    provider = SimpleNamespace(
        info=SimpleNamespace(name="Unity Catalog", version="1.0.0", docs_url=None)
    )

    monkeypatch.setattr(ProviderRegistry, "has", lambda _provider: True)
    monkeypatch.setattr(ProviderRegistry, "get", lambda _provider: provider)

    called: dict[str, Path] = {}

    def _ensure(workspace: Path, provider_id: str) -> None:
        called["workspace"] = workspace
        called["provider_id"] = provider_id

    monkeypatch.setattr("schemax.cli.ensure_project_file", _ensure)

    result = runner.invoke(cli, ["init", "--provider", "unity", str(temp_workspace)])

    assert result.exit_code == 0
    assert called["provider_id"] == "unity"
    assert called["workspace"] == temp_workspace.resolve()


def test_sql_routes_arguments(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _generate(**kwargs):
        captured.update(kwargs)
        return "SELECT 1"

    monkeypatch.setattr("schemax.cli.generate_sql_migration", _generate)

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
    assert captured["_from_version"] == "v0.1.0"
    assert captured["_to_version"] == "v0.2.0"
    assert captured["target_env"] == "dev"


def test_sql_returns_error_code_on_command_error(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    def _raise(**kwargs):  # noqa: ARG001
        raise SQLGenerationError("boom")

    monkeypatch.setattr("schemax.cli.generate_sql_migration", _raise)

    result = runner.invoke(cli, ["sql", str(temp_workspace)])
    assert result.exit_code == 1
    assert "SQL generation failed" in result.output


def test_validate_routes_json_option(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _validate(workspace: Path, json_output: bool) -> bool:
        captured["workspace"] = workspace
        captured["json_output"] = json_output
        return True

    monkeypatch.setattr("schemax.cli.validate_project", _validate)

    result = runner.invoke(cli, ["validate", "--json", str(temp_workspace)])
    assert result.exit_code == 0
    assert captured["workspace"] == temp_workspace.resolve()
    assert captured["json_output"] is True


def test_diff_routes_arguments(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _diff(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("schemax.cli.generate_diff", _diff)

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


def test_bundle_contract_not_implemented() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["bundle", "--target", "dev", "--version", "0.1.0"])

    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_apply_uses_status_to_set_exit_code(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.apply_to_environment",
        lambda **kwargs: SimpleNamespace(status="success"),
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
        "schemax.cli.apply_to_environment",
        lambda **kwargs: SimpleNamespace(status="failed"),
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


def test_rollback_requires_minimum_args_for_partial(temp_workspace: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["rollback", "--partial", str(temp_workspace)])

    assert result.exit_code == 1
    assert "--deployment required for partial rollback" in result.output


def test_rollback_complete_routes_to_command(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "schemax.cli.rollback_complete",
        lambda **kwargs: SimpleNamespace(
            success=True, operations_rolled_back=0, error_message=None
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


def test_snapshot_create_no_ops_is_graceful(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()

    monkeypatch.setattr("schemax.core.storage.read_changelog", lambda _workspace: {"ops": []})

    result = runner.invoke(
        cli,
        ["snapshot", "create", "--name", "No changes", str(temp_workspace)],
    )

    assert result.exit_code == 0
    assert "No uncommitted operations" in result.output


def test_snapshot_rebase_routes_to_command(monkeypatch, temp_workspace: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "schemax.commands.snapshot_rebase.rebase_snapshot",
        lambda **kwargs: SimpleNamespace(success=True, applied_count=0, conflict_count=0),
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
        "schemax.commands.snapshot_rebase.detect_stale_snapshots",
        lambda _workspace, _json_output=False: [],
    )

    result = runner.invoke(
        cli,
        ["snapshot", "validate", "--json", str(temp_workspace)],
    )

    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert parsed == {"stale": [], "count": 0}
