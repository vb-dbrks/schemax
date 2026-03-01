"""Workspace repository facade over storage/session operations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .storage import (
    WorkspaceSession,
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_environment_config,
    get_snapshot_file_path,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_changelog,
    write_project,
    write_snapshot,
)


@dataclass(slots=True)
class WorkspaceRepository:
    """Application-facing repository for workspace read/write operations."""

    def ensure_initialized(self, *, workspace: Path, provider_id: str) -> None:
        """Ensure workspace project/changelog files exist."""
        ensure_project_file(workspace, provider_id=provider_id)

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        """Read workspace project configuration."""
        return read_project(workspace)

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]:
        """Read workspace changelog."""
        return read_changelog(workspace)

    def write_changelog(self, *, workspace: Path, changelog: dict[str, Any]) -> None:
        """Persist changelog payload."""
        write_changelog(workspace, changelog)

    def append_operations(self, *, workspace: Path, operations: list[Any]) -> None:
        """Append operations to workspace changelog."""
        append_ops(workspace, operations)

    def create_snapshot(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Create and persist a snapshot."""
        return create_snapshot(
            workspace_path=workspace,
            name=name,
            version=version,
            comment=comment,
            tags=tags,
        )

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        """Resolve one environment config from project configuration."""
        return get_environment_config(project, environment)

    def open_session(self, *, workspace: Path) -> WorkspaceSession:
        """Open a transactional workspace session."""
        return WorkspaceSession(workspace)

    def write_project(self, *, workspace: Path, project: dict[str, Any]) -> None:
        """Persist project payload."""
        write_project(workspace, project)

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        """Read one snapshot payload."""
        return read_snapshot(workspace, version)

    def write_snapshot(self, *, workspace: Path, snapshot: dict[str, Any]) -> None:
        """Persist one snapshot payload."""
        write_snapshot(workspace, snapshot)

    def snapshot_file_path(self, *, workspace: Path, version: str) -> Path:
        """Resolve one snapshot file path."""
        return get_snapshot_file_path(workspace, version)

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        """Load current provider state and changelog."""
        return load_current_state(workspace, validate=validate)
