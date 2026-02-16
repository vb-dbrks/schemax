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
    provider = Mock()
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"

    monkeypatch.setattr(
        "schemax.commands.sql.read_project",
        lambda _workspace: {
            "provider": {"environments": {"dev": {"topLevelName": "dev_demo"}}},
            "managedLocations": {},
            "externalLocations": {},
        },
    )
    monkeypatch.setattr(
        "schemax.commands.sql.load_current_state",
        lambda _workspace, validate=False: ({"catalogs": []}, {"ops": []}, provider, None),
    )

    assert generate_sql_migration(workspace=Path(".")) == ""


def test_generate_sql_migration_with_target_and_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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

    monkeypatch.setattr("schemax.commands.sql.read_project", lambda _workspace: project)
    monkeypatch.setattr(
        "schemax.commands.sql.load_current_state",
        lambda _workspace, validate=False: (state, changelog, provider, None),
    )
    monkeypatch.setattr(
        "schemax.commands.sql.get_environment_config",
        lambda _project, _env: {
            "topLevelName": "dev_demo",
            "catalogMappings": {"demo": "dev_demo"},
        },
    )

    out_file = tmp_path / "migration.sql"
    sql = generate_sql_migration(workspace=tmp_path, output=out_file, target_env="dev")

    assert sql == "CREATE CATALOG `dev_demo`;"
    assert out_file.read_text() == "CREATE CATALOG `dev_demo`;"
    generator.generate_sql.assert_called_once()


def test_generate_sql_migration_snapshot_latest_requires_existing_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "schemax.commands.sql.read_project",
        lambda _workspace: {
            "latestSnapshot": None,
            "managedLocations": {},
            "externalLocations": {},
        },
    )

    with pytest.raises(SQLGenerationError, match="No snapshots available"):
        generate_sql_migration(workspace=Path("."), snapshot="latest")
