"""Unit tests for diff command module."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from schemax.commands.diff import DiffError, generate_diff
from schemax.providers.base.operations import Operation


def _make_op(op_id: str = "op_1") -> Operation:
    return Operation(
        id=op_id,
        ts="2026-02-01T00:00:00Z",
        provider="unity",
        op="unity.add_catalog",
        target="cat_1",
        payload={"catalogId": "cat_1", "name": "demo"},
    )


class _RepoStub:
    def __init__(
        self,
        *,
        snapshots: dict[str, dict] | None = None,
        project: dict | None = None,
        raise_on_version: str | None = None,
        env_config: dict | None = None,
    ) -> None:
        self._snapshots = snapshots or {}
        self._project = project or {}
        self._raise_on_version = raise_on_version
        self._env_config = env_config or {}

    def read_snapshot(self, *, workspace: Path, version: str) -> dict:
        del workspace
        if version == self._raise_on_version:
            raise FileNotFoundError(f"{version}.json")
        return self._snapshots[version]

    def read_project(self, *, workspace: Path) -> dict:
        del workspace
        return self._project

    def get_environment_config(self, *, project: dict, environment: str) -> dict:
        del project, environment
        return self._env_config


def test_generate_diff_rejects_same_version() -> None:
    with pytest.raises(DiffError, match="Cannot diff the same version"):
        generate_diff(
            workspace=Path("."),
            from_version="v0.1.0",
            to_version="v0.1.0",
        )


def test_generate_diff_raises_when_provider_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("schemax.commands.diff.ProviderRegistry.get", lambda _provider_id: None)
    repo = _RepoStub(
        snapshots={
            "v0.1.0": {"state": {"catalogs": []}},
            "v0.2.0": {"state": {"catalogs": []}},
        },
        project={"provider": {"type": "missing"}},
    )

    with pytest.raises(DiffError, match="Provider 'missing' not found"):
        generate_diff(
            workspace=Path("."),
            from_version="v0.1.0",
            to_version="v0.2.0",
            workspace_repo=repo,
        )


def test_generate_diff_with_sql_and_target_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    operation = _make_op()
    differ = Mock()
    differ.generate_diff_operations.return_value = [operation]

    sql_gen = Mock()
    sql_gen.generate_sql.return_value = "CREATE CATALOG dev_demo;"

    provider = Mock()
    provider.get_state_differ.return_value = differ
    provider.get_sql_generator.return_value = sql_gen

    monkeypatch.setattr("schemax.commands.diff.ProviderRegistry.get", lambda _provider_id: provider)
    repo = _RepoStub(
        snapshots={
            "v0.1.0": {"state": {"catalogs": [{"name": "demo"}]}, "operations": []},
            "v0.2.0": {"state": {"catalogs": [{"name": "demo"}]}, "operations": []},
        },
        project={
            "provider": {"type": "unity"},
            "environments": {"dev": {"topLevelName": "dev_demo"}},
        },
        env_config={"topLevelName": "dev_demo", "catalogMappings": {"demo": "dev_demo"}},
    )

    operations = generate_diff(
        workspace=Path("."),
        from_version="v0.1.0",
        to_version="v0.2.0",
        show_sql=True,
        target_env="dev",
        workspace_repo=repo,
    )

    assert len(operations) == 1
    assert operations[0].op == "unity.add_catalog"
    sql_gen.generate_sql.assert_called_once()


def test_generate_diff_missing_snapshot_error_has_context(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _RepoStub(
        snapshots={"v0.2.0": {"state": {"catalogs": []}, "operations": []}},
        project={"provider": {"type": "unity"}},
        raise_on_version="v0.1.0",
    )

    with pytest.raises(DiffError, match="Source snapshot not found"):
        generate_diff(
            workspace=Path("."),
            from_version="v0.1.0",
            to_version="v0.2.0",
            workspace_repo=repo,
        )
