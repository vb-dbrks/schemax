"""Tests for workspace repository facade."""

from pathlib import Path

from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.operations import create_operation


def test_workspace_repository_initializes_workspace(tmp_path: Path) -> None:
    """Repository should create project/changelog structure for new workspace."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()

    repo.ensure_initialized(workspace=workspace, provider_id="unity")
    project = repo.read_project(workspace=workspace)

    assert project["version"] == 4
    assert project["provider"]["type"] == "unity"


def test_workspace_repository_appends_operations(tmp_path: Path) -> None:
    """Repository append operation should persist operation in changelog."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")
    operation = create_operation(
        provider="unity",
        op_type="add_schema",
        target="cat_implicit.schema_demo",
        payload={"catalogId": "cat_implicit", "schemaId": "schema_demo", "name": "demo"},
    )

    repo.append_operations(workspace=workspace, operations=[operation])
    changelog = repo.read_changelog(workspace=workspace)

    assert len(changelog["ops"]) >= 1


def test_workspace_repository_creates_snapshot(tmp_path: Path) -> None:
    """Repository should create snapshot and clear changelog through storage layer."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")
    operation = create_operation(
        provider="unity",
        op_type="add_schema",
        target="cat_implicit.schema_demo",
        payload={"catalogId": "cat_implicit", "schemaId": "schema_demo", "name": "demo"},
    )
    repo.append_operations(workspace=workspace, operations=[operation])

    project, snapshot = repo.create_snapshot(
        workspace=workspace,
        name="snapshot_v1",
        version="v0.1.0",
        comment=None,
        tags=[],
    )
    changelog = repo.read_changelog(workspace=workspace)

    assert project["latestSnapshot"] == "v0.1.0"
    assert snapshot["version"] == "v0.1.0"
    assert changelog["ops"] == []


def test_workspace_repository_appends_dict_operations(tmp_path: Path) -> None:
    """Repository should normalize dict operations before append."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")

    repo.append_operations(
        workspace=workspace,
        operations=[
            {
                "id": "op_1",
                "ts": "2026-01-01T00:00:00Z",
                "provider": "unity",
                "op": "unity.add_catalog",
                "target": "cat_1",
                "payload": {"catalogId": "cat_1", "name": "bronze"},
            }
        ],
    )

    changelog = repo.read_changelog(workspace=workspace)
    assert len(changelog["ops"]) >= 2
    assert changelog["ops"][-1]["op"] == "unity.add_catalog"
    assert changelog["ops"][-1]["payload"]["catalogId"] == "cat_1"


def test_workspace_repository_rejects_malformed_operations(tmp_path: Path) -> None:
    """Repository should fail fast on unsupported operation payload shapes."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")

    try:
        repo.append_operations(workspace=workspace, operations=["invalid"])  # type: ignore[list-item]
    except TypeError as err:
        assert "expects Operation or operation dict payloads" in str(err)
    else:
        raise AssertionError("Expected TypeError for malformed operation payload")
