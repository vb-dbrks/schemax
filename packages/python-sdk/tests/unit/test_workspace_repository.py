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
    catalog_op = create_operation(
        provider="unity",
        op_type="add_catalog",
        target="cat_main",
        payload={"catalogId": "cat_main", "name": "main"},
    )
    schema_op = create_operation(
        provider="unity",
        op_type="add_schema",
        target="cat_main.schema_demo",
        payload={"catalogId": "cat_main", "schemaId": "schema_demo", "name": "demo"},
    )

    repo.append_operations(workspace=workspace, operations=[catalog_op, schema_op])
    changelog = repo.read_changelog(workspace=workspace)

    assert len(changelog["ops"]) >= 1


def test_workspace_repository_creates_snapshot(tmp_path: Path) -> None:
    """Repository should create snapshot and clear changelog through storage layer."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")
    catalog_op = create_operation(
        provider="unity",
        op_type="add_catalog",
        target="cat_main",
        payload={"catalogId": "cat_main", "name": "main"},
    )
    schema_op = create_operation(
        provider="unity",
        op_type="add_schema",
        target="cat_main.schema_demo",
        payload={"catalogId": "cat_main", "schemaId": "schema_demo", "name": "demo"},
    )
    repo.append_operations(workspace=workspace, operations=[catalog_op, schema_op])

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
    assert len(changelog["ops"]) == 1
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


def test_workspace_repository_remove_operations_by_id(tmp_path: Path) -> None:
    """Repository should remove requested operation IDs from changelog."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    repo = WorkspaceRepository()
    repo.ensure_initialized(workspace=workspace, provider_id="unity")

    repo.append_operations(
        workspace=workspace,
        operations=[
            {
                "id": "op_keep",
                "ts": "2026-01-01T00:00:00Z",
                "provider": "unity",
                "op": "unity.add_catalog",
                "target": "cat_keep",
                "payload": {"catalogId": "cat_keep", "name": "keep"},
            },
            {
                "id": "op_drop",
                "ts": "2026-01-01T00:00:01Z",
                "provider": "unity",
                "op": "unity.add_schema",
                "target": "sch_drop",
                "payload": {"catalogId": "cat_keep", "schemaId": "sch_drop", "name": "drop"},
            },
        ],
    )

    summary = repo.remove_operations_by_id(
        workspace=workspace,
        op_ids=["op_drop", "missing_id"],
    )

    changelog = repo.read_changelog(workspace=workspace)
    op_ids = [entry.get("id") for entry in changelog["ops"]]
    assert "op_drop" not in op_ids
    assert "op_keep" in op_ids
    assert summary["removedCount"] == 1
    assert summary["missingCount"] == 1
