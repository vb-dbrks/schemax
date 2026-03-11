"""Workspace repository facade over storage/session operations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..providers.base.operations import Operation
from .storage import (
    WorkspaceSession,
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_environment_config,
    get_snapshot_file_path,
    get_target_config,
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

    def append_operations(
        self, *, workspace: Path, operations: list[Operation | dict[str, Any]]
    ) -> None:
        """Append operations to workspace changelog."""
        normalized = [self._normalize_operation(operation) for operation in operations]
        append_ops(workspace, normalized)

    def remove_operations_by_id(self, *, workspace: Path, op_ids: list[str]) -> dict[str, Any]:
        """Remove operations from changelog by ID using best-effort semantics."""
        changelog = self.read_changelog(workspace=workspace)
        raw_ops = changelog.get("ops", [])
        if not isinstance(raw_ops, list):
            raise ValueError("Changelog ops must be a list")

        requested_ids = set(op_ids)
        removed_ids: list[str] = []
        remaining_ops: list[Any] = []

        for operation in raw_ops:
            op_id = operation.get("id") if isinstance(operation, Mapping) else None
            if isinstance(op_id, str) and op_id in requested_ids:
                removed_ids.append(op_id)
                continue
            remaining_ops.append(operation)

        removed_id_set = set(removed_ids)
        missing_ids = [op_id for op_id in op_ids if op_id not in removed_id_set]
        changelog["ops"] = remaining_ops
        self.write_changelog(workspace=workspace, changelog=changelog)

        return {
            "removedOpIds": removed_ids,
            "missingOpIds": missing_ids,
            "removedCount": len(removed_ids),
            "missingCount": len(missing_ids),
            "remainingOpsCount": len(remaining_ops),
        }

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
        self, *, project: dict[str, Any], environment: str, scope: str | None = None
    ) -> dict[str, Any]:
        """Resolve one environment config from project configuration."""
        return get_environment_config(project, environment, scope=scope)

    def get_target_config(
        self, *, project: dict[str, Any], scope: str | None = None
    ) -> dict[str, Any]:
        """Resolve one target config from project configuration."""
        return get_target_config(project, scope)

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

    def load_current_state(
        self, *, workspace: Path, validate: bool = False, scope: str | None = None
    ) -> tuple[Any, ...]:
        """Load current provider state and changelog."""
        return load_current_state(workspace, validate=validate, scope=scope)

    @staticmethod
    def _normalize_operation(operation: Operation | dict[str, Any]) -> Operation:
        if isinstance(operation, Operation):
            return operation
        if isinstance(operation, Mapping):
            return Operation(**dict(operation))
        raise TypeError(
            "WorkspaceRepository.append_operations expects Operation or operation dict payloads"
        )
