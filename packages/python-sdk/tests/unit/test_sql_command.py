"""Unit tests for SQL command module."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from schemax.commands.sql import SQLGenerationError, build_catalog_mapping, generate_sql_migration


def _make_op(op_id: str = "op_1") -> dict:
    return {
        "id": op_id,
        "ts": "2026-02-01T00:00:00Z",
        "provider": "unity",
        "op": "unity.add_catalog",
        "target": "cat_1",
        "payload": {"catalogId": "cat_1", "name": "demo"},
    }


class _RepoStub:
    def __init__(
        self,
        *,
        project: dict,
        state_result: tuple | None = None,
        snapshots: dict[str, dict] | None = None,
        env_config: dict | None = None,
    ) -> None:
        self._project = project
        self._state_result = state_result
        self._snapshots = snapshots or {}
        self._env_config = env_config or {}

    def read_project(self, *, workspace: Path) -> dict:
        del workspace
        return self._project

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple:
        del workspace, validate
        if self._state_result is None:
            raise AssertionError("state_result not configured")
        return self._state_result

    def read_snapshot(self, *, workspace: Path, version: str) -> dict:
        del workspace
        return self._snapshots[version]

    def get_environment_config(self, *, project: dict, environment: str) -> dict:
        del project, environment
        return self._env_config


def test_build_catalog_mapping_empty_state_returns_empty_mapping() -> None:
    assert build_catalog_mapping({"catalogs": []}, {"catalogMappings": {}}) == {}


def test_build_catalog_mapping_requires_object_mappings() -> None:
    with pytest.raises(SQLGenerationError, match="must be an object"):
        build_catalog_mapping({"catalogs": [{"name": "demo"}]}, {"catalogMappings": "bad"})


def test_build_catalog_mapping_requires_all_catalogs() -> None:
    state = {"catalogs": [{"name": "demo"}, {"name": "analytics"}]}
    env_config = {"catalogMappings": {"demo": "dev_demo"}}

    with pytest.raises(SQLGenerationError, match=r"Missing catalog mapping\(s\)"):
        build_catalog_mapping(state, env_config)


def test_generate_sql_migration_returns_empty_when_no_ops(monkeypatch: pytest.MonkeyPatch) -> None:
    del monkeypatch
    provider = Mock()
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"

    repo = _RepoStub(
        project={
            "provider": {"environments": {"dev": {"topLevelName": "dev_demo"}}},
            "managedLocations": {},
            "externalLocations": {},
        },
        state_result=({"catalogs": []}, {"ops": []}, provider, None),
    )

    assert generate_sql_migration(workspace=Path("."), workspace_repo=repo) == ""


def test_generate_sql_migration_with_target_and_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    del monkeypatch
    provider = Mock()
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"

    generator = Mock()
    generator.generate_sql.return_value = "CREATE CATALOG `dev_demo`;"
    provider.get_sql_generator.return_value = generator

    state = {"catalogs": [{"name": "demo"}]}
    changelog = {"ops": [_make_op()]}
    project = {
        "provider": {"environments": {"dev": {"topLevelName": "dev_demo"}}},
        "managedLocations": {},
        "externalLocations": {},
    }
    repo = _RepoStub(
        project=project,
        state_result=(state, changelog, provider, None),
        env_config={"topLevelName": "dev_demo", "catalogMappings": {"demo": "dev_demo"}},
    )

    out_file = tmp_path / "migration.sql"
    sql = generate_sql_migration(
        workspace=tmp_path,
        output=out_file,
        target_env="dev",
        workspace_repo=repo,
    )

    assert sql == "CREATE CATALOG `dev_demo`;"
    assert out_file.read_text() == "CREATE CATALOG `dev_demo`;"
    generator.generate_sql.assert_called_once()


def test_generate_sql_migration_snapshot_latest_requires_existing_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    repo = _RepoStub(
        project={
            "latestSnapshot": None,
            "managedLocations": {},
            "externalLocations": {},
        },
    )

    with pytest.raises(SQLGenerationError, match="No snapshots available"):
        generate_sql_migration(workspace=Path("."), snapshot="latest", workspace_repo=repo)
