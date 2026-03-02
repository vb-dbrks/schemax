"""
Snapshot Rebase Command

Rebase snapshots onto new base versions after git rebase, similar to git rebase for commits.
Unpacks snapshot operations, replays them on new base, and detects conflicts.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from rich.console import Console

from schemax.core.version import get_versions_between
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.operations import Operation

console = Console()


class RebaseError(Exception):
    """Raised when snapshot rebase fails"""


class ConflictError(Exception):
    """Raised when operation conflict is detected during rebase"""


@dataclass
class RebaseResult:
    """Result of snapshot rebase operation"""

    success: bool
    applied_count: int
    conflict_count: int
    conflict_log_path: str | None = None
    message: str | None = None


@dataclass
class RebaseContext:
    """Context for rebase: loaded snapshot and resolved base version."""

    rebase_needed: bool
    snapshot: dict[str, Any] | None
    old_base: str | None
    new_base_version: str | None
    feature_ops: list[dict[str, Any]]


@dataclass
class ReplayResult:
    """Result of replaying operations on a base state."""

    success: bool
    current_state: dict[str, Any]
    applied_ops: list[dict[str, Any]]
    conflicting_ops: list[dict[str, Any]]


class _WorkspaceRepoPort(Protocol):
    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]: ...

    def read_project(self, *, workspace: Path) -> dict[str, Any]: ...

    def write_snapshot(self, *, workspace: Path, snapshot: dict[str, Any]) -> None: ...

    def write_project(self, *, workspace: Path, project: dict[str, Any]) -> None: ...

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]: ...

    def write_changelog(self, *, workspace: Path, changelog: dict[str, Any]) -> None: ...

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]: ...

    def snapshot_file_path(self, *, workspace: Path, version: str) -> Path: ...


class _SnapshotRebaseWorkspaceRepository:
    """Repository adapter for snapshot-rebase workflow."""

    def __init__(self) -> None:
        self._repository = WorkspaceRepository()

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        return self._repository.read_snapshot(workspace=workspace, version=version)

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_project(workspace=workspace)

    def write_snapshot(self, *, workspace: Path, snapshot: dict[str, Any]) -> None:
        self._repository.write_snapshot(workspace=workspace, snapshot=snapshot)

    def write_project(self, *, workspace: Path, project: dict[str, Any]) -> None:
        self._repository.write_project(workspace=workspace, project=project)

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_changelog(workspace=workspace)

    def write_changelog(self, *, workspace: Path, changelog: dict[str, Any]) -> None:
        self._repository.write_changelog(workspace=workspace, changelog=changelog)

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        return self._repository.load_current_state(workspace=workspace, validate=validate)

    def snapshot_file_path(self, *, workspace: Path, version: str) -> Path:
        return self._repository.snapshot_file_path(workspace=workspace, version=version)


def _load_rebase_context(
    workspace: Path,
    snapshot_version: str,
    new_base_version: str | None,
    workspace_repo: _WorkspaceRepoPort,
) -> RebaseContext:
    """Load snapshot and resolve new base. Raises RebaseError if invalid."""
    snapshot = workspace_repo.read_snapshot(workspace=workspace, version=snapshot_version)
    old_base = snapshot.get("previousSnapshot")
    if not old_base:
        raise RebaseError(f"Snapshot {snapshot_version} has no previousSnapshot")

    if not new_base_version:
        project = workspace_repo.read_project(workspace=workspace)
        new_base_version = project.get("latestSnapshot")
        if not new_base_version:
            raise RebaseError("No snapshots available to use as new base")

    if old_base == new_base_version:
        return RebaseContext(
            rebase_needed=False,
            snapshot=None,
            old_base=old_base,
            new_base_version=new_base_version,
            feature_ops=[],
        )

    feature_ops = snapshot.get("operations", [])
    return RebaseContext(
        rebase_needed=True,
        snapshot=snapshot,
        old_base=old_base,
        new_base_version=new_base_version,
        feature_ops=feature_ops,
    )


def _replay_operations_on_base(
    feature_ops: list[dict],
    new_base_state: dict[str, Any],
    new_base_ops: list[dict],
    provider: Any,
    new_base_version: str,
) -> ReplayResult:
    """Replay feature ops on base state using provider; detect conflicts. No I/O."""
    current_state = new_base_state.copy()
    applied_ops: list[dict] = []
    conflicting_ops: list[dict] = []

    for i, op_dict in enumerate(feature_ops, 1):
        operation = Operation(**op_dict)
        try:
            new_state = provider.apply_operations(current_state.copy(), [operation])
            if _has_conflict(operation, new_base_ops):
                raise ConflictError(
                    f"Operation modifies {operation.target} which was changed in {new_base_version}"
                )
            current_state = new_state
            applied_ops.append(op_dict)
        except Exception as e:
            conflict_reason = str(e) if isinstance(e, ConflictError) else f"Error: {e}"
            conflicting_ops.append(
                {"operation": op_dict, "index": i - 1, "reason": conflict_reason}
            )
            for j in range(i, len(feature_ops)):
                conflicting_ops.append(
                    {
                        "operation": feature_ops[j],
                        "index": j,
                        "reason": "Blocked by previous conflict",
                    }
                )
            return ReplayResult(
                success=False,
                current_state=current_state,
                applied_ops=applied_ops,
                conflicting_ops=conflicting_ops,
            )

    return ReplayResult(
        success=True,
        current_state=current_state,
        applied_ops=applied_ops,
        conflicting_ops=[],
    )


def _persist_conflict_result(
    workspace: Path,
    applied_ops: list[dict],
    conflicting_ops: list[dict],
    new_base_version: str,
    snapshot_version: str,
    old_base: str,
    workspace_repo: _WorkspaceRepoPort,
) -> str:
    """Write changelog and conflict log; return conflict log path."""
    if applied_ops:
        _save_applied_ops_to_changelog(
            workspace=workspace,
            operations=applied_ops,
            based_on=new_base_version,
            workspace_repo=workspace_repo,
        )
    else:
        _clear_changelog(
            workspace=workspace, based_on=new_base_version, workspace_repo=workspace_repo
        )
    return _save_conflict_log(
        workspace, snapshot_version, old_base, new_base_version, applied_ops, conflicting_ops
    )


def _persist_success_snapshot(
    workspace: Path,
    snapshot: dict[str, Any],
    snapshot_version: str,
    new_base_version: str,
    current_state: dict[str, Any],
    feature_ops: list[dict],
    workspace_repo: _WorkspaceRepoPort,
) -> None:
    """Write rebased snapshot file and update project.json."""
    new_snapshot = {
        **snapshot,
        "previousSnapshot": new_base_version,
        "state": current_state,
        "operations": feature_ops,
        "rebasedFrom": snapshot.get("previousSnapshot"),
        "rebasedAt": datetime.now(UTC).isoformat(),
    }
    workspace_repo.write_snapshot(workspace=workspace, snapshot=new_snapshot)
    project = workspace_repo.read_project(workspace=workspace)
    project["snapshots"].append(
        {
            "id": snapshot["id"],
            "version": snapshot_version,
            "name": snapshot["name"],
            "ts": snapshot["ts"],
            "createdBy": snapshot["createdBy"],
            "file": f".schemax/snapshots/{snapshot_version}.json",
            "previousSnapshot": new_base_version,
            "opsCount": len(feature_ops),
            "hash": snapshot.get("hash"),
            "tags": snapshot.get("tags", []),
            "comment": snapshot.get("comment", ""),
        }
    )
    workspace_repo.write_project(workspace=workspace, project=project)


def _print_conflict_summary(
    conflicting_ops: list[dict],
    applied_ops: list[dict],
    conflict_log_path: str,
    new_base_version: str,
    snapshot_version: str,
) -> None:
    """Print conflict details and resolution steps to console."""
    console.print()
    _show_conflict_details(conflicting_ops[0], new_base_version)
    console.print()
    console.print("[yellow]Current state:[/yellow]")
    if applied_ops:
        console.print(f"  ✓ Operations 1-{len(applied_ops)} applied successfully")
        console.print(f"  ✗ Operation {len(applied_ops) + 1} blocked (conflict)")
        console.print(f"  📝 {len(applied_ops)} operations saved to changelog")
    else:
        console.print("  ✗ Operation 1 blocked (conflict)")
        console.print("  📝 Changelog cleared (ready for manual resolution)")
    console.print(f"  ⚠️  {len(conflicting_ops)} conflicting operations saved to conflict log")
    console.print()
    console.print("[yellow]Resolution:[/yellow]")
    console.print("  1. Open SchemaX Designer (UI will show conflict indicator)")
    console.print(f"  2. Review the conflict in {conflicting_ops[0]['operation']['target']}")
    console.print("  3. Manually apply your changes in the UI")
    console.print(f"  4. Run: schemax snapshot create --version {snapshot_version}")
    console.print()
    console.print(f"Conflict log saved to: [cyan]{conflict_log_path}[/cyan]")


def _run_rebase_steps(
    workspace: Path,
    snapshot_version: str,
    new_base_version: str | None,
    workspace_repo: _WorkspaceRepoPort,
) -> RebaseResult:
    """Perform rebase: load context, replay ops, persist result. Raises RebaseError on failure."""
    context = _load_rebase_context(workspace, snapshot_version, new_base_version, workspace_repo)
    if not context.rebase_needed:
        console.print(
            f"[yellow]No rebase needed - already based on {context.new_base_version}[/yellow]"
        )
        return RebaseResult(
            success=True, applied_count=0, conflict_count=0, message="No rebase needed"
        )

    snapshot = context.snapshot
    old_base = context.old_base
    new_base_version = context.new_base_version
    feature_ops = context.feature_ops
    assert snapshot is not None
    assert old_base is not None
    assert new_base_version is not None

    console.print(f"  Old base: [yellow]{old_base}[/yellow]")
    console.print(f"  New base: [green]{new_base_version}[/green]")
    console.print()
    console.print(f"Unpacking {snapshot_version}...")
    console.print(f"  ✓ Extracted {len(feature_ops)} operations")

    workspace_repo.snapshot_file_path(workspace=workspace, version=snapshot_version).unlink()
    console.print(f"  ✓ Deleted snapshot file {snapshot_version}.json")
    _remove_snapshot_from_project(
        workspace=workspace,
        snapshot_version=snapshot_version,
        workspace_repo=workspace_repo,
    )
    console.print("  ✓ Removed from project.json")
    console.print("  ✓ Moved operations to temporary changelog")

    console.print()
    console.print(f"Loading new base state ([cyan]{new_base_version}[/cyan])...")
    new_base_snapshot = workspace_repo.read_snapshot(workspace=workspace, version=new_base_version)
    new_base_state = new_base_snapshot["state"]
    new_base_ops = new_base_snapshot.get("operations", [])
    console.print("  ✓ Loaded state")

    _, _, provider, _ = workspace_repo.load_current_state(workspace=workspace, validate=False)
    console.print()
    console.print(f"Replaying operations on [cyan]{new_base_version}[/cyan]...")
    replay_result = _replay_operations_on_base(
        feature_ops, new_base_state, new_base_ops, provider, new_base_version
    )

    for i, op_dict in enumerate(replay_result.applied_ops, 1):
        operation = Operation(**op_dict)
        console.print(f"  [{i}/{len(feature_ops)}] {operation.op} ✓")
    if replay_result.conflicting_ops:
        first_fail = len(replay_result.applied_ops) + 1
        console.print(f"  [{first_fail}/{len(feature_ops)}] [red]✗ CONFLICT![/red]")

    if replay_result.conflicting_ops:
        conflict_log_path = _persist_conflict_result(
            workspace,
            replay_result.applied_ops,
            replay_result.conflicting_ops,
            new_base_version,
            snapshot_version,
            old_base,
            workspace_repo,
        )
        _print_conflict_summary(
            replay_result.conflicting_ops,
            replay_result.applied_ops,
            conflict_log_path,
            new_base_version,
            snapshot_version,
        )
        return RebaseResult(
            success=False,
            applied_count=len(replay_result.applied_ops),
            conflict_count=len(replay_result.conflicting_ops),
            conflict_log_path=conflict_log_path,
        )

    console.print()
    console.print("[green]✓ All operations applied successfully[/green]")
    console.print()
    console.print("Creating rebased snapshot...")
    _persist_success_snapshot(
        workspace,
        snapshot,
        snapshot_version,
        new_base_version,
        replay_result.current_state,
        feature_ops,
        workspace_repo,
    )
    console.print(f"[green]✓ Rebased {snapshot_version} onto {new_base_version}[/green]")
    return RebaseResult(
        success=True,
        applied_count=len(replay_result.applied_ops),
        conflict_count=0,
    )


def rebase_snapshot(
    workspace: Path,
    snapshot_version: str,
    new_base_version: str | None = None,
    workspace_repo: _WorkspaceRepoPort | None = None,
) -> RebaseResult:
    """Rebase snapshot onto new base version

    This unpacks the snapshot, deletes the snapshot file, and replays operations
    one by one on the new base. If a conflict is detected, it stops and saves
    a conflict log for manual resolution.

    Args:
        workspace: Project workspace path
        snapshot_version: Version of snapshot to rebase (e.g., "v0.4.0")
        new_base_version: New base version (auto-detects latest if not provided)
        workspace_repo: Optional repository override for tests/injection

    Returns:
        RebaseResult with success status and details

    Raises:
        RebaseError: If rebase fails

    Example:
        >>> rebase_snapshot(Path.cwd(), "v0.4.0", "v0.3.1")
    """
    console.print(f"[bold cyan]Rebasing snapshot {snapshot_version}[/bold cyan]")
    console.print()
    repository: _WorkspaceRepoPort = workspace_repo or _SnapshotRebaseWorkspaceRepository()

    try:
        return _run_rebase_steps(workspace, snapshot_version, new_base_version, repository)
    except RebaseError:
        raise
    except Exception as e:
        raise RebaseError(f"Snapshot rebase failed: {e}") from e


def _has_conflict(operation: Operation, base_operations: list[dict]) -> bool:
    """Check if operation conflicts with base operations

    A conflict occurs when the same target was modified in both the base and the feature.
    """
    target = operation.target

    for base_op in base_operations:
        if base_op["target"] == target:
            # Same target was modified in base - potential conflict
            return True

    return False


def _remove_snapshot_from_project(
    workspace: Path,
    snapshot_version: str,
    workspace_repo: _WorkspaceRepoPort,
) -> None:
    """Remove snapshot from project.json snapshots array"""
    project = workspace_repo.read_project(workspace=workspace)

    project["snapshots"] = [s for s in project["snapshots"] if s["version"] != snapshot_version]

    # Update latestSnapshot if needed
    if project.get("latestSnapshot") == snapshot_version:
        if project["snapshots"]:
            project["latestSnapshot"] = project["snapshots"][-1]["version"]
        else:
            project["latestSnapshot"] = None

    workspace_repo.write_project(workspace=workspace, project=project)


def _clear_changelog(workspace: Path, based_on: str, workspace_repo: _WorkspaceRepoPort) -> None:
    """Clear changelog after rebase conflict - user will manually resolve in UI"""
    changelog = workspace_repo.read_changelog(workspace=workspace)

    # Clear operations - user will manually apply changes in UI
    changelog["ops"] = []
    changelog["sinceSnapshot"] = based_on
    changelog["lastModified"] = datetime.now(UTC).isoformat()

    # Remove any temp flags if they exist
    changelog.pop("_rebase_temp", None)
    changelog.pop("_rebase_message", None)

    workspace_repo.write_changelog(workspace=workspace, changelog=changelog)


def _save_applied_ops_to_changelog(
    workspace: Path,
    operations: list[dict],
    based_on: str,
    workspace_repo: _WorkspaceRepoPort,
) -> None:
    """Save successfully applied operations to changelog (clean, no temp flags)

    This preserves work that succeeded before a conflict occurred.
    User can continue from here in the UI.
    """
    changelog = workspace_repo.read_changelog(workspace=workspace)

    # Save operations WITHOUT temp flags - this is now a normal changelog
    changelog["ops"] = operations
    changelog["sinceSnapshot"] = based_on
    changelog["lastModified"] = datetime.now(UTC).isoformat()

    # Remove any temp flags if they exist
    changelog.pop("_rebase_temp", None)
    changelog.pop("_rebase_message", None)

    workspace_repo.write_changelog(workspace=workspace, changelog=changelog)


def _save_to_temp_changelog(
    workspace: Path,
    operations: list[dict],
    based_on: str,
    workspace_repo: _WorkspaceRepoPort,
) -> None:
    """Save operations to temporary changelog"""
    changelog = workspace_repo.read_changelog(workspace=workspace)

    # Mark as temporary rebase operations
    changelog["ops"] = operations
    changelog["sinceSnapshot"] = based_on
    changelog["_rebase_temp"] = True
    changelog["_rebase_message"] = "Temporary operations from snapshot rebase"
    changelog["lastModified"] = datetime.now(UTC).isoformat()

    workspace_repo.write_changelog(workspace=workspace, changelog=changelog)


def _save_conflict_log(
    workspace: Path,
    snapshot_version: str,
    old_base: str,
    new_base: str,
    applied_ops: list[dict],
    conflicting_ops: list[dict],
) -> str:
    """Save conflict log for user review"""
    conflicts_dir = workspace / ".schemax" / "conflicts"
    conflicts_dir.mkdir(parents=True, exist_ok=True)

    log = {
        "snapshot_version": snapshot_version,
        "old_base": old_base,
        "new_base": new_base,
        "applied_operations": applied_ops,
        "conflicting_operations": conflicting_ops,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    log_file = conflicts_dir / f"{snapshot_version}_rebase.json"

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)

    return str(log_file.relative_to(workspace))


def _show_conflict_details(conflict: dict, new_base_version: str) -> None:
    """Display conflict details to user"""
    console.print("┌─────────────────────────────────────────────┐")
    console.print("│ [red]❌ Conflict detected![/red]                        │")
    console.print("│                                             │")
    console.print(f"│ Operation: {conflict['operation']['op']:30s} │")
    console.print(f"│ Target: {conflict['operation']['target']:33s} │")
    console.print("│                                             │")
    console.print(f"│ Reason: Column was modified in {new_base_version:9s} │")
    console.print(f"│   {conflict['reason']:41s} │")
    console.print("└─────────────────────────────────────────────┘")


def detect_stale_snapshots(
    workspace: Path,
    _json_output: bool = False,
    workspace_repo: _WorkspaceRepoPort | None = None,
) -> list[dict]:
    """Detect snapshots that need rebasing after git rebase.

    Args:
        workspace: Path to workspace
        workspace_repo: Optional repository override for tests/injection

    Returns:
        List of stale snapshots with details about what's missing
    """
    del _json_output
    repository: _WorkspaceRepoPort = workspace_repo or _SnapshotRebaseWorkspaceRepository()
    project = repository.read_project(workspace=workspace)
    all_versions = [s["version"] for s in project.get("snapshots", [])]

    stale_snapshots = []

    for snapshot in project.get("snapshots", []):
        prev = snapshot.get("previousSnapshot")
        if not prev:
            continue

        version = snapshot["version"]

        # Check if there are versions between prev and current
        versions_between = get_versions_between(prev, version, all_versions)

        if versions_between:
            # Found intermediate versions - snapshot is stale!
            stale_snapshots.append(
                {
                    "version": version,
                    "currentBase": prev,
                    "shouldBeBase": versions_between[-1],
                    "missing": versions_between,
                }
            )

    return stale_snapshots
