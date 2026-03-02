"""Unit tests for validate command module."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from schemax.commands.validate import ValidationError, validate_dependencies, validate_project
from schemax.providers.base.models import ValidationError as ProviderValidationError
from schemax.providers.base.models import ValidationResult
from schemax.providers.base.operations import Operation


def _op(op_id: str = "op_1") -> Operation:
    return Operation(
        id=op_id,
        ts="2026-02-01T00:00:00Z",
        provider="unity",
        op="unity.add_table",
        target="table_1",
        payload={"tableId": "table_1", "name": "users", "schemaId": "schema_1", "format": "delta"},
    )


class _RepoStub:
    def __init__(self, *, project: dict, state_result: tuple) -> None:
        self._project = project
        self._state_result = state_result

    def read_project(self, *, workspace: Path) -> dict:
        del workspace
        return self._project

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple:
        del workspace, validate
        return self._state_result


def test_validate_dependencies_reports_cycle() -> None:
    graph = Mock()
    graph.detect_cycles.return_value = [["table_a", "view_b", "table_a"]]
    graph.validate_dependencies.return_value = []

    generator = Mock()
    generator.build_dependency_graph.return_value = graph
    generator.id_name_map = {"table_a": "catalog.schema.table_a", "view_b": "catalog.schema.view_b"}

    provider = Mock()
    provider.get_sql_generator.return_value = generator

    errors, warnings = validate_dependencies({"catalogs": []}, [_op()], provider)

    assert len(errors) == 1
    assert "Circular dependency" in errors[0]
    assert not warnings


def test_validate_project_json_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    provider = Mock()
    provider.info = SimpleNamespace(name="Unity Catalog", version="1.0.0")
    provider.validate_state.return_value = ValidationResult(valid=True, errors=[])

    repo = _RepoStub(
        project={"version": 4, "name": "demo", "provider": {"type": "unity", "version": "1.0.0"}},
        state_result=({"catalogs": []}, {"ops": []}, provider, None),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.validate_dependencies",
        lambda _state, _ops, _provider: ([], []),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.detect_stale_snapshots",
        lambda _workspace, workspace_repo=None: [],
    )

    result = validate_project(Path("."), json_output=True, workspace_repo=repo)
    out = capsys.readouterr().out

    assert result is True
    assert '"valid": true' in out


def test_validate_project_raises_on_invalid_state(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = Mock()
    provider.validate_state.return_value = ValidationResult(
        valid=False,
        errors=[ProviderValidationError(field="catalogs", message="invalid")],
    )

    repo = _RepoStub(
        project={"version": 4, "name": "demo", "provider": {"type": "unity", "version": "1.0.0"}},
        state_result=({"catalogs": []}, {"ops": []}, provider, None),
    )

    with pytest.raises(ValidationError, match="State validation failed"):
        validate_project(Path("."), json_output=False, workspace_repo=repo)


def test_validate_project_returns_false_when_stale_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = Mock()
    provider.info = SimpleNamespace(name="Unity Catalog", version="1.0.0")
    provider.validate_state.return_value = ValidationResult(valid=True, errors=[])

    repo = _RepoStub(
        project={"version": 4, "name": "demo", "provider": {"type": "unity", "version": "1.0.0"}},
        state_result=({"catalogs": []}, {"ops": []}, provider, None),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.validate_dependencies",
        lambda _state, _ops, _provider: ([], []),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.detect_stale_snapshots",
        lambda _workspace, workspace_repo=None: [
            {
                "version": "v0.2.0",
                "currentBase": "v0.1.0",
                "shouldBeBase": "v0.1.1",
                "missing": ["v0.1.1"],
            }
        ],
    )

    assert validate_project(Path("."), json_output=False, workspace_repo=repo) is False
