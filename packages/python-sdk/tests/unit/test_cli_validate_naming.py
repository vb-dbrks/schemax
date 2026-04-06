"""CLI tests for `schemax validate --naming` exit codes."""

import json
from pathlib import Path

from click.testing import CliRunner

from schemax.cli import cli


def _workspace_with_table_naming_rule(tmp_path: Path) -> Path:
    """Minimal v5 project with an enabled table naming rule (snake_case)."""
    project_path = tmp_path / ".schemax" / "project.json"
    project_path.parent.mkdir(parents=True, exist_ok=True)
    project = {
        "version": 5,
        "name": "test",
        "targets": {
            "default": {
                "type": "unity",
                "version": "1.0.0",
                "environments": {
                    "dev": {
                        "topLevelName": "dev_catalog",
                        "allowDrift": False,
                        "requireSnapshot": False,
                        "autoCreateTopLevel": True,
                        "autoCreateSchemaxSchema": True,
                    }
                },
            }
        },
        "settings": {
            "namingStandards": {
                "table": {
                    "pattern": "^[a-z][a-z0-9_]*$",
                    "enabled": True,
                    "description": "snake_case",
                }
            }
        },
    }
    project_path.write_text(json.dumps(project, indent=2), encoding="utf-8")
    return tmp_path


def test_validate_naming_invalid_name_exits_1(tmp_path: Path) -> None:
    """Invalid name against configured rule must exit with code 1 (console)."""
    ws = _workspace_with_table_naming_rule(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--naming",
            "--name",
            "BadName",
            "--type",
            "table",
            str(ws),
        ],
    )
    assert result.exit_code == 1
    assert "invalid" in result.output.lower() or "BadName" in result.output


def test_validate_naming_valid_name_exits_0(tmp_path: Path) -> None:
    """Valid name must exit with code 0."""
    ws = _workspace_with_table_naming_rule(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--naming",
            "--name",
            "good_table",
            "--type",
            "table",
            str(ws),
        ],
    )
    assert result.exit_code == 0
    assert "valid" in result.output.lower()


def test_validate_naming_json_invalid_exits_1_with_envelope(tmp_path: Path) -> None:
    """JSON mode: invalid name exits 1; stdout is a success envelope with valid false."""
    ws = _workspace_with_table_naming_rule(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--naming",
            "--name",
            "BadName",
            "--type",
            "table",
            "--json",
            str(ws),
        ],
    )
    assert result.exit_code == 1
    lines = [ln.strip() for ln in result.output.strip().splitlines() if ln.strip()]
    payload = json.loads(lines[-1])
    assert payload.get("schemaVersion") == "1"
    assert payload.get("status") == "success"
    assert payload.get("data", {}).get("valid") is False
    assert payload.get("meta", {}).get("exitCode") == 1


def test_validate_naming_json_valid_exits_0(tmp_path: Path) -> None:
    """JSON mode: valid name exits 0 with valid true in data."""
    ws = _workspace_with_table_naming_rule(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--naming",
            "--name",
            "good_table",
            "--type",
            "table",
            "--json",
            str(ws),
        ],
    )
    assert result.exit_code == 0
    lines = [ln.strip() for ln in result.output.strip().splitlines() if ln.strip()]
    payload = json.loads(lines[-1])
    assert payload.get("data", {}).get("valid") is True
    assert payload.get("meta", {}).get("exitCode") == 0
