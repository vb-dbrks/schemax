"""
Storage layer for reading and writing SchemaX project files.
"""

import json
from pathlib import Path
from typing import Tuple

from .models import (
    ChangelogFile,
    Deployment,
    ProjectFile,
    SnapshotFile,
    State,
)


def read_project(workspace_path: Path) -> ProjectFile:
    """Read project.json file"""
    project_file = workspace_path / ".schemax" / "project.json"

    if not project_file.exists():
        raise FileNotFoundError(f"Project file not found: {project_file}")

    with open(project_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return ProjectFile.model_validate(data)


def read_changelog(workspace_path: Path) -> ChangelogFile:
    """Read changelog.json file"""
    changelog_file = workspace_path / ".schemax" / "changelog.json"

    if not changelog_file.exists():
        raise FileNotFoundError(f"Changelog file not found: {changelog_file}")

    with open(changelog_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return ChangelogFile.model_validate(data)


def read_snapshot(workspace_path: Path, version: str) -> SnapshotFile:
    """Read a specific snapshot file by version"""
    snapshot_file = workspace_path / ".schemax" / "snapshots" / f"{version}.json"

    if not snapshot_file.exists():
        raise FileNotFoundError(f"Snapshot file not found: {snapshot_file}")

    with open(snapshot_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return SnapshotFile.model_validate(data)


def load_current_state(workspace_path: Path) -> Tuple[State, ChangelogFile]:
    """
    Load the current state by:
    1. Loading the latest snapshot (or starting with empty state)
    2. Applying operations from the changelog

    Returns:
        Tuple of (current_state, changelog)
    """
    project = read_project(workspace_path)
    changelog = read_changelog(workspace_path)

    # Start with latest snapshot state or empty state
    if project.latest_snapshot:
        snapshot = read_snapshot(workspace_path, project.latest_snapshot)
        state = snapshot.state
    else:
        state = State(catalogs=[])

    # Apply changelog ops to get current state
    from .state import apply_ops_to_state

    state = apply_ops_to_state(state, changelog.ops)

    return state, changelog


def write_project(workspace_path: Path, project: ProjectFile) -> None:
    """Write project.json file"""
    project_file = workspace_path / ".schemax" / "project.json"
    project_file.parent.mkdir(parents=True, exist_ok=True)

    with open(project_file, "w", encoding="utf-8") as f:
        json.dump(project.model_dump(by_alias=True), f, indent=2)


def write_deployment(workspace_path: Path, deployment: Deployment) -> None:
    """Add a deployment record to project.json"""
    project = read_project(workspace_path)
    project.deployments.append(deployment)
    write_project(workspace_path, project)


def get_last_deployment(project: ProjectFile, environment: str) -> Deployment | None:
    """Get the last deployment for a specific environment"""
    deployments = [d for d in project.deployments if d.environment == environment]
    if not deployments:
        return None
    # Sort by timestamp, return most recent
    return sorted(deployments, key=lambda d: d.ts, reverse=True)[0]
