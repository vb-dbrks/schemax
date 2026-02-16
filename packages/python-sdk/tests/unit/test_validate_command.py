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


def test_validate_dependencies_reports_cycle() -> None:
    graph = Mock()
    graph.detect_cycles.return_value = [["table_a", "view_b", "table_a"]]
    graph.validate_dependencies.return_value = []

    generator = Mock()
    generator._build_dependency_graph.return_value = graph
    generator.id_name_map = {"table_a": "catalog.schema.table_a", "view_b": "catalog.schema.view_b"}

    provider = Mock()
    provider.get_sql_generator.return_value = generator

    errors, warnings = validate_dependencies({"catalogs": []}, [_op()], provider)

    assert len(errors) == 1
    assert "Circular dependency" in errors[0]
    assert warnings == []


def test_validate_project_json_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    provider = Mock()
    provider.info = SimpleNamespace(name="Unity Catalog", version="1.0.0")
    provider.validate_state.return_value = ValidationResult(valid=True, errors=[])

    monkeypatch.setattr(
        "schemax.commands.validate.read_project",
        lambda _workspace: {
            "version": 4,
            "name": "demo",
            "provider": {"type": "unity", "version": "1.0.0"},
        },
    )
    monkeypatch.setattr(
        "schemax.commands.validate.load_current_state",
        lambda _workspace, validate=False: ({"catalogs": []}, {"ops": []}, provider, None),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.validate_dependencies",
        lambda _state, _ops, _provider: ([], []),
    )
    monkeypatch.setattr(
        "schemax.commands.snapshot_rebase.detect_stale_snapshots",
        lambda _workspace: [],
    )

    result = validate_project(Path("."), json_output=True)
    out = capsys.readouterr().out

    assert result is True
    assert '"valid": true' in out


def test_validate_project_raises_on_invalid_state(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = Mock()
    provider.validate_state.return_value = ValidationResult(
        valid=False,
        errors=[ProviderValidationError(field="catalogs", message="invalid")],
    )

    monkeypatch.setattr(
        "schemax.commands.validate.read_project",
        lambda _workspace: {
            "version": 4,
            "name": "demo",
            "provider": {"type": "unity", "version": "1.0.0"},
        },
    )
    monkeypatch.setattr(
        "schemax.commands.validate.load_current_state",
        lambda _workspace, validate=False: ({"catalogs": []}, {"ops": []}, provider, None),
    )

    with pytest.raises(ValidationError, match="State validation failed"):
        validate_project(Path("."), json_output=False)


def test_validate_project_returns_false_when_stale_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = Mock()
    provider.info = SimpleNamespace(name="Unity Catalog", version="1.0.0")
    provider.validate_state.return_value = ValidationResult(valid=True, errors=[])

    monkeypatch.setattr(
        "schemax.commands.validate.read_project",
        lambda _workspace: {
            "version": 4,
            "name": "demo",
            "provider": {"type": "unity", "version": "1.0.0"},
        },
    )
    monkeypatch.setattr(
        "schemax.commands.validate.load_current_state",
        lambda _workspace, validate=False: ({"catalogs": []}, {"ops": []}, provider, None),
    )
    monkeypatch.setattr(
        "schemax.commands.validate.validate_dependencies",
        lambda _state, _ops, _provider: ([], []),
    )
    monkeypatch.setattr(
        "schemax.commands.snapshot_rebase.detect_stale_snapshots",
        lambda _workspace: [
            {
                "version": "v0.2.0",
                "currentBase": "v0.1.0",
                "shouldBeBase": "v0.1.1",
                "missing": ["v0.1.1"],
            }
        ],
    )

    assert validate_project(Path("."), json_output=False) is False
