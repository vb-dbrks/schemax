"""Unit tests for import command using repository injection (no storage patch seams)."""

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from schemax.commands.import_assets import (
    ImportCommandError,
    import_from_provider,
    import_from_sql_file,
)


class _RepoStub:
    def __init__(self) -> None:
        self.project_exists_flag = True
        self.ensure_initialized_calls = 0
        self.append_calls: list[list[Any]] = []
        self.write_calls = 0
        self.project: dict[str, Any] = {
            "provider": {"environments": {"dev": {}}},
            "latestSnapshot": "v0.1.0",
        }
        self.env_config: dict[str, Any] = {
            "topLevelName": "dev_catalog",
            "catalogMappings": {"main": "dev_catalog"},
        }
        self.state_result: tuple[Any, ...] = ({"catalogs": []}, {"ops": []}, None, None)

    def project_exists(self, *, workspace: Path) -> bool:
        del workspace
        return self.project_exists_flag

    def ensure_initialized(self, *, workspace: Path, provider_id: str) -> None:
        del workspace, provider_id
        self.ensure_initialized_calls += 1

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        del workspace
        return self.project

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        del workspace, validate
        return self.state_result

    def append_operations(self, *, workspace: Path, operations: list[Any]) -> None:
        del workspace
        self.append_calls.append(operations)

    def create_snapshot(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        del workspace, name, version, comment, tags
        snapshot = {"version": "v0.2.0"}
        self.project["latestSnapshot"] = "v0.2.0"
        return self.project, snapshot

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        del project, environment
        return self.env_config

    def open_session(self, *, workspace: Path) -> Mock:
        del workspace
        return Mock()

    def write_project(self, *, workspace: Path, project: dict[str, Any]) -> None:
        del workspace, project
        self.write_calls += 1


def _provider_for_import(*, import_ops: list | None = None) -> Mock:
    provider = Mock()
    provider.info.id = "unity"
    provider.info.name = "Unity Catalog"
    provider.info.version = "1.0.0"

    provider.validate_execution_config.return_value = Mock(valid=True, errors=[])
    provider.validate_import_scope.return_value = Mock(valid=True, errors=[])
    provider.discover_state.return_value = {"catalogs": []}
    provider.collect_import_warnings.return_value = []
    provider.prepare_import_state.return_value = ({"catalogs": []}, {"main": "dev_catalog"}, True)

    differ = Mock()
    differ.generate_diff_operations.return_value = import_ops or []
    provider.get_state_differ.return_value = differ

    provider.capabilities.features = {"baseline_adoption": True}
    provider.adopt_import_baseline.return_value = "dep_1"

    provider.state_from_ddl.return_value = ({"catalogs": []}, {"skipped": 0, "parse_errors": []})
    provider.create_initial_state.return_value = {"catalogs": []}
    return provider


def test_import_from_provider_dry_run_does_not_append() -> None:
    repo = _RepoStub()
    provider = _provider_for_import(import_ops=[Mock(op="unity.add_catalog")])
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    summary = import_from_provider(
        workspace=Path("."),
        target_env="dev",
        profile="DEFAULT",
        warehouse_id="wh_1",
        dry_run=True,
        workspace_repo=repo,
    )

    assert summary["dry_run"] is True
    assert not repo.append_calls


def test_import_from_provider_writes_mappings_and_ops() -> None:
    repo = _RepoStub()
    provider = _provider_for_import(import_ops=[Mock(op="unity.add_catalog")])
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    summary = import_from_provider(
        workspace=Path("."),
        target_env="dev",
        profile="DEFAULT",
        warehouse_id="wh_1",
        dry_run=False,
        workspace_repo=repo,
    )

    assert summary["operations_generated"] == 1
    assert len(repo.append_calls) == 1
    assert repo.write_calls == 1


def test_import_from_provider_invalid_config_raises() -> None:
    repo = _RepoStub()
    provider = _provider_for_import()
    provider.validate_execution_config.return_value = Mock(
        valid=False,
        errors=[Mock(field="warehouse_id", message="missing")],
    )
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    with pytest.raises(ImportCommandError, match="Invalid execution configuration"):
        import_from_provider(
            workspace=Path("."),
            target_env="dev",
            profile="DEFAULT",
            warehouse_id="",
            workspace_repo=repo,
        )


def test_import_from_sql_file_missing_file_raises() -> None:
    repo = _RepoStub()
    with pytest.raises(ImportCommandError, match="SQL file not found"):
        import_from_sql_file(
            workspace=Path("."),
            sql_path=Path("/tmp/does-not-exist.sql"),
            workspace_repo=repo,
        )


def test_import_from_sql_file_initializes_when_project_missing(tmp_path: Path) -> None:
    repo = _RepoStub()
    repo.project_exists_flag = False
    provider = _provider_for_import(import_ops=[])
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("CREATE CATALOG main;")

    summary = import_from_sql_file(
        workspace=tmp_path,
        sql_path=sql_file,
        mode="diff",
        dry_run=True,
        workspace_repo=repo,
    )

    assert summary["source"] == "sql_file"
    assert repo.ensure_initialized_calls == 1


def test_import_from_sql_file_replace_mode_appends_ops(tmp_path: Path) -> None:
    repo = _RepoStub()
    provider = _provider_for_import(import_ops=[Mock(op="unity.add_catalog")])
    repo.state_result = ({"catalogs": []}, {"ops": []}, provider, None)

    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("CREATE CATALOG main;")

    summary = import_from_sql_file(
        workspace=tmp_path,
        sql_path=sql_file,
        mode="replace",
        dry_run=False,
        workspace_repo=repo,
    )

    assert summary["operations_generated"] == 1
    assert len(repo.append_calls) == 1
