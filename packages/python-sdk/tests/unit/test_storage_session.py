"""Unit tests for workspace session and mutation locks."""

from pathlib import Path

import pytest

from schemax.core.storage import (
    WorkspaceMutationLock,
    WorkspaceSession,
    ensure_project_file,
    get_changelog_file_path,
    get_lock_file_path,
    read_changelog,
    read_project,
)
from schemax.domain.errors import StorageConflictError


def test_workspace_lock_conflict_raises_error(tmp_path: Path) -> None:
    """Acquiring a lock should fail when lock file already exists."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    ensure_project_file(workspace)
    lock_path = get_lock_file_path(workspace)
    lock_path.write_text("12345\n", encoding="utf-8")

    lock = WorkspaceMutationLock(workspace)
    with pytest.raises(StorageConflictError, match="Workspace is locked"):
        lock.acquire()


def test_workspace_session_commit_updates_project_and_changelog(tmp_path: Path) -> None:
    """Session commit should persist staged project/changelog payloads."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    ensure_project_file(workspace)

    with WorkspaceSession(workspace) as session:
        project = session.read_project()
        changelog = session.read_changelog()
        project["name"] = "workspace_updated"
        changelog["ops"] = []
        changelog["sinceSnapshot"] = "v0.1.0"
        session.write_project(project)
        session.write_changelog(changelog)
        session.commit()

    persisted_project = read_project(workspace)
    persisted_changelog = read_changelog(workspace)
    assert persisted_project["name"] == "workspace_updated"
    assert persisted_changelog["sinceSnapshot"] == "v0.1.0"


def test_write_outputs_json_with_newline(tmp_path: Path) -> None:
    """Session commit should persist JSON files ending with newline."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    ensure_project_file(workspace)

    with WorkspaceSession(workspace) as session:
        changelog = session.read_changelog()
        changelog["ops"] = []
        session.write_changelog(changelog)
        session.commit()

    payload = get_changelog_file_path(workspace).read_text(encoding="utf-8")
    assert payload.endswith("\n")
