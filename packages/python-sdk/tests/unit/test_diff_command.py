"""Unit tests for diff command module."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from schematic.commands.diff import DiffError, generate_diff
from schematic.providers.base.operations import Operation


def _make_op(op_id: str = "op_1") -> Operation:
    return Operation(
        id=op_id,
        ts="2026-02-01T00:00:00Z",
        provider="unity",
        op="unity.add_catalog",
        target="cat_1",
        payload={"catalogId": "cat_1", "name": "demo"},
    )


def test_generate_diff_rejects_same_version() -> None:
    with pytest.raises(DiffError, match="Cannot diff the same version"):
        generate_diff(
            workspace=Path("."),
            from_version="v0.1.0",
            to_version="v0.1.0",
        )


def test_generate_diff_raises_when_provider_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "schematic.commands.diff.read_snapshot",
        lambda _workspace, _version: {"state": {"catalogs": []}},
    )
    monkeypatch.setattr(
        "schematic.commands.diff.read_project",
        lambda _workspace: {"provider": {"type": "missing"}},
    )
    monkeypatch.setattr("schematic.commands.diff.ProviderRegistry.get", lambda _provider_id: None)

    with pytest.raises(DiffError, match="Provider 'missing' not found"):
        generate_diff(workspace=Path("."), from_version="v0.1.0", to_version="v0.2.0")


def test_generate_diff_with_sql_and_target_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    op = _make_op()
    differ = Mock()
    differ.generate_diff_operations.return_value = [op]

    sql_gen = Mock()
    sql_gen.generate_sql.return_value = "CREATE CATALOG dev_demo;"

    provider = Mock()
    provider.get_state_differ.return_value = differ
    provider.get_sql_generator.return_value = sql_gen

    monkeypatch.setattr(
        "schematic.commands.diff.read_snapshot",
        lambda _workspace, _version: {"state": {"catalogs": [{"name": "demo"}]}, "operations": []},
    )
    monkeypatch.setattr(
        "schematic.commands.diff.read_project",
        lambda _workspace: {
            "provider": {"type": "unity"},
            "environments": {"dev": {"topLevelName": "dev_demo"}},
        },
    )
    monkeypatch.setattr(
        "schematic.commands.diff.ProviderRegistry.get", lambda _provider_id: provider
    )
    monkeypatch.setattr(
        "schematic.commands.diff.get_environment_config",
        lambda _project, _env: {
            "topLevelName": "dev_demo",
            "catalogMappings": {"demo": "dev_demo"},
        },
    )

    operations = generate_diff(
        workspace=Path("."),
        from_version="v0.1.0",
        to_version="v0.2.0",
        show_sql=True,
        target_env="dev",
    )

    assert len(operations) == 1
    assert operations[0].op == "unity.add_catalog"
    sql_gen.generate_sql.assert_called_once()


def test_generate_diff_missing_snapshot_error_has_context(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_missing(_workspace: Path, version: str) -> dict:
        raise FileNotFoundError(f"{version}.json")

    monkeypatch.setattr("schematic.commands.diff.read_snapshot", _raise_missing)

    with pytest.raises(DiffError, match="Source snapshot not found"):
        generate_diff(workspace=Path("."), from_version="v0.1.0", to_version="v0.2.0")
