"""
Storage Layer V3 - Provider-Aware

New storage layer that supports multiple catalog providers through the provider system.
Migrates from v2 storage format to v3 format with provider metadata.
"""

import json
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
from uuid import uuid4

from .providers import ProviderRegistry, Provider, Operation, ProviderState

SCHEMAX_DIR = ".schemax"
PROJECT_FILENAME = "project.json"
CHANGELOG_FILENAME = "changelog.json"
SNAPSHOTS_DIR = "snapshots"


def get_schemax_dir(workspace_path: Path) -> Path:
    """Get the .schemax directory path"""
    return workspace_path / SCHEMAX_DIR


def get_project_file_path(workspace_path: Path) -> Path:
    """Get the project file path"""
    return get_schemax_dir(workspace_path) / PROJECT_FILENAME


def get_changelog_file_path(workspace_path: Path) -> Path:
    """Get the changelog file path"""
    return get_schemax_dir(workspace_path) / CHANGELOG_FILENAME


def get_snapshots_dir(workspace_path: Path) -> Path:
    """Get the snapshots directory path"""
    return get_schemax_dir(workspace_path) / SNAPSHOTS_DIR


def get_snapshot_file_path(workspace_path: Path, version: str) -> Path:
    """Get snapshot file path"""
    return get_snapshots_dir(workspace_path) / f"{version}.json"


def ensure_schemax_dir(workspace_path: Path) -> None:
    """Ensure .schemax/snapshots/ directory exists"""
    schemax_dir = get_schemax_dir(workspace_path)
    snapshots_dir = get_snapshots_dir(workspace_path)

    schemax_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)


def ensure_project_file(workspace_path: Path, provider_id: str = "unity") -> None:
    """
    Initialize a new project with provider selection

    Args:
        workspace_path: Path to workspace
        provider_id: Provider ID (default: 'unity')
    """
    project_path = get_project_file_path(workspace_path)
    changelog_path = get_changelog_file_path(workspace_path)

    if project_path.exists():
        # Project exists - check if migration is needed
        with open(project_path, "r") as f:
            project = json.load(f)

        if project.get("version") == 2:
            # Migrate from v2 to v3
            migrate_v2_to_v3(workspace_path, project, provider_id)
        return

    # Create new v3 project
    workspace_name = workspace_path.name

    # Verify provider exists
    provider = ProviderRegistry.get(provider_id)
    if provider is None:
        available = ", ".join(ProviderRegistry.get_all_ids())
        raise ValueError(
            f"Provider '{provider_id}' not found. Available providers: {available}"
        )

    new_project = {
        "version": 3,
        "name": workspace_name,
        "provider": {
            "type": provider_id,
            "version": provider.info.version,
        },
        "environments": ["dev", "test", "prod"],
        "snapshots": [],
        "deployments": [],
        "settings": {
            "autoIncrementVersion": True,
            "versionPrefix": "v",
            "requireSnapshotForProd": True,
            "allowDrift": False,
            "requireComments": False,
            "warnOnBreakingChanges": True,
        },
        "latestSnapshot": None,
    }

    new_changelog = {
        "version": 1,
        "sinceSnapshot": None,
        "ops": [],
        "lastModified": datetime.utcnow().isoformat() + "Z",
    }

    ensure_schemax_dir(workspace_path)

    with open(project_path, "w") as f:
        json.dump(new_project, f, indent=2)

    with open(changelog_path, "w") as f:
        json.dump(new_changelog, f, indent=2)

    print(
        f"[SchemaX] Initialized new v3 project: {workspace_name} with provider: {provider.info.name}"
    )


def migrate_v2_to_v3(
    workspace_path: Path, v2_project: Dict[str, Any], provider_id: str = "unity"
) -> None:
    """
    Migrate v2 project to v3 format

    Args:
        workspace_path: Path to workspace
        v2_project: V2 project data
        provider_id: Provider ID (default: 'unity')
    """
    print("[SchemaX] Migrating project from v2 to v3...")

    provider = ProviderRegistry.get(provider_id)
    if provider is None:
        raise ValueError(f"Provider '{provider_id}' not found")

    v3_project = {
        "version": 3,
        "name": v2_project.get("name", workspace_path.name),
        "provider": {
            "type": provider_id,
            "version": provider.info.version,
        },
        "environments": v2_project.get("environments", ["dev", "test", "prod"]),
        "snapshots": v2_project.get("snapshots", []),
        "deployments": v2_project.get("deployments", []),
        "settings": v2_project.get(
            "settings",
            {
                "autoIncrementVersion": True,
                "versionPrefix": "v",
                "requireSnapshotForProd": True,
                "allowDrift": False,
                "requireComments": False,
                "warnOnBreakingChanges": True,
            },
        ),
        "latestSnapshot": v2_project.get("latestSnapshot"),
    }

    # Migrate operations in changelog to add provider prefix
    try:
        changelog = read_changelog(workspace_path)
        migrated_ops = []

        for op in changelog["ops"]:
            # Add provider field if missing
            if "provider" not in op:
                op["provider"] = provider_id

            # Add provider prefix to operation type if missing
            if "." not in op["op"]:
                op["op"] = f"{provider_id}.{op['op']}"

            migrated_ops.append(op)

        changelog["ops"] = migrated_ops

        with open(get_changelog_file_path(workspace_path), "w") as f:
            json.dump(changelog, f, indent=2)

    except Exception as e:
        print(f"[SchemaX] Warning: Could not migrate changelog operations: {e}")

    # Write migrated project file
    with open(get_project_file_path(workspace_path), "w") as f:
        json.dump(v3_project, f, indent=2)

    print("[SchemaX] Migration complete: v2 → v3")


def read_project(workspace_path: Path) -> Dict[str, Any]:
    """
    Read project file

    Args:
        workspace_path: Path to workspace

    Returns:
        Project data
    """
    project_path = get_project_file_path(workspace_path)

    with open(project_path, "r") as f:
        project = json.load(f)

    # Auto-migrate if needed
    if project.get("version") == 2:
        migrate_v2_to_v3(workspace_path, project)
        return read_project(workspace_path)  # Re-read after migration

    if project.get("version") != 3:
        raise ValueError(f"Unsupported project version: {project.get('version')}")

    return project


def write_project(workspace_path: Path, project: Dict[str, Any]) -> None:
    """Write project file"""
    project_path = get_project_file_path(workspace_path)

    with open(project_path, "w") as f:
        json.dump(project, f, indent=2)


def read_changelog(workspace_path: Path) -> Dict[str, Any]:
    """
    Read changelog file

    Args:
        workspace_path: Path to workspace

    Returns:
        Changelog data
    """
    changelog_path = get_changelog_file_path(workspace_path)

    try:
        with open(changelog_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        # Changelog doesn't exist, create empty one
        changelog = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": [],
            "lastModified": datetime.utcnow().isoformat() + "Z",
        }

        with open(changelog_path, "w") as f:
            json.dump(changelog, f, indent=2)

        return changelog


def write_changelog(workspace_path: Path, changelog: Dict[str, Any]) -> None:
    """Write changelog file"""
    changelog_path = get_changelog_file_path(workspace_path)
    changelog["lastModified"] = datetime.utcnow().isoformat() + "Z"

    with open(changelog_path, "w") as f:
        json.dump(changelog, f, indent=2)


def read_snapshot(workspace_path: Path, version: str) -> Dict[str, Any]:
    """Read a snapshot file"""
    snapshot_path = get_snapshot_file_path(workspace_path, version)

    with open(snapshot_path, "r") as f:
        return json.load(f)


def write_snapshot(workspace_path: Path, snapshot: Dict[str, Any]) -> None:
    """Write a snapshot file"""
    ensure_schemax_dir(workspace_path)
    snapshot_path = get_snapshot_file_path(workspace_path, snapshot["version"])

    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2)


def load_current_state(
    workspace_path: Path,
) -> tuple[ProviderState, Dict[str, Any], Provider]:
    """
    Load current state using provider

    Args:
        workspace_path: Path to workspace

    Returns:
        Tuple of (state, changelog, provider)
    """
    project = read_project(workspace_path)
    changelog = read_changelog(workspace_path)

    # Get provider
    provider = ProviderRegistry.get(project["provider"]["type"])
    if provider is None:
        raise ValueError(
            f"Provider '{project['provider']['type']}' not found. "
            "Please ensure the provider is installed."
        )

    # Load state
    if project["latestSnapshot"]:
        # Load latest snapshot
        snapshot = read_snapshot(workspace_path, project["latestSnapshot"])
        state = snapshot["state"]
    else:
        # No snapshots yet, start with empty state
        state = provider.create_initial_state()

    # Apply changelog ops using provider's state reducer
    ops = [Operation(**op) for op in changelog["ops"]]
    state = provider.apply_operations(state, ops)

    return state, changelog, provider


def append_ops(workspace_path: Path, ops: List[Operation]) -> None:
    """
    Append operations to changelog

    Args:
        workspace_path: Path to workspace
        ops: Operations to append
    """
    project = read_project(workspace_path)
    changelog = read_changelog(workspace_path)
    provider = ProviderRegistry.get(project["provider"]["type"])

    if provider is None:
        raise ValueError(f"Provider '{project['provider']['type']}' not found")

    # Validate operations
    for op in ops:
        validation = provider.validate_operation(op)
        if not validation.valid:
            errors = ", ".join(f"{e.field}: {e.message}" for e in validation.errors)
            raise ValueError(f"Invalid operation: {errors}")

    # Append ops
    changelog["ops"].extend([op.dict(by_alias=True) for op in ops])

    # Write back
    write_changelog(workspace_path, changelog)


def create_snapshot(
    workspace_path: Path,
    name: str,
    version: Optional[str] = None,
    comment: Optional[str] = None,
    tags: List[str] = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Create a snapshot

    Args:
        workspace_path: Path to workspace
        name: Snapshot name
        version: Optional version string
        comment: Optional comment
        tags: Optional tags

    Returns:
        Tuple of (project, snapshot)
    """
    if tags is None:
        tags = []

    project = read_project(workspace_path)
    changelog = read_changelog(workspace_path)

    # Load current state
    state, _, _ = load_current_state(workspace_path)

    # Determine version
    snapshot_version = version or _get_next_version(
        project.get("latestSnapshot"), project["settings"]
    )

    # Generate IDs for ops that don't have them (backwards compatibility)
    ops_with_ids = []
    for i, op in enumerate(changelog["ops"]):
        if "id" not in op or not op["id"]:
            op["id"] = f"op_{i}_{op['ts']}_{op['target']}"
        ops_with_ids.append(op)

    # Calculate hash
    state_hash = _calculate_state_hash(state, [op["id"] for op in ops_with_ids])

    import os

    # Create snapshot file
    snapshot_file = {
        "id": f"snap_{uuid4()}",
        "version": snapshot_version,
        "name": name,
        "ts": datetime.utcnow().isoformat() + "Z",
        "createdBy": os.environ.get("USER") or os.environ.get("USERNAME") or "unknown",
        "state": state,
        "opsIncluded": [op["id"] for op in ops_with_ids],
        "previousSnapshot": project.get("latestSnapshot"),
        "hash": state_hash,
        "tags": tags,
        "comment": comment,
    }

    # Write snapshot file
    write_snapshot(workspace_path, snapshot_file)

    # Create snapshot metadata
    snapshot_metadata = {
        "id": snapshot_file["id"],
        "version": snapshot_version,
        "name": name,
        "ts": snapshot_file["ts"],
        "createdBy": snapshot_file["createdBy"],
        "file": f".schemax/snapshots/{snapshot_version}.json",
        "previousSnapshot": project.get("latestSnapshot"),
        "opsCount": len(ops_with_ids),
        "hash": state_hash,
        "tags": tags,
        "comment": comment,
    }

    # Update project
    project["snapshots"].append(snapshot_metadata)
    project["latestSnapshot"] = snapshot_version
    write_project(workspace_path, project)

    # Clear changelog
    new_changelog = {
        "version": 1,
        "sinceSnapshot": snapshot_version,
        "ops": [],
        "lastModified": datetime.utcnow().isoformat() + "Z",
    }
    write_changelog(workspace_path, new_changelog)

    print(f"[SchemaX] Created snapshot {snapshot_version}: {name}")
    print(f"[SchemaX] Snapshot file: {snapshot_metadata['file']}")
    print(f"[SchemaX] Ops included: {len(ops_with_ids)}")

    return project, snapshot_file


def get_uncommitted_ops_count(workspace_path: Path) -> int:
    """Get number of uncommitted operations"""
    changelog = read_changelog(workspace_path)
    return len(changelog["ops"])


def _calculate_state_hash(state: Any, ops_included: List[str]) -> str:
    """Calculate hash of state for integrity checking"""
    content = json.dumps({"state": state, "opsIncluded": ops_included}, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()


def _get_next_version(current_version: Optional[str], settings: Dict[str, Any]) -> str:
    """Get next version number"""
    if not current_version:
        return settings["versionPrefix"] + "0.1.0"

    # Parse version (e.g., "v0.1.0" or "0.1.0")
    import re

    match = re.search(r"(\d+)\.(\d+)\.(\d+)", current_version)
    if not match:
        return settings["versionPrefix"] + "0.1.0"

    major, minor, patch = match.groups()
    next_minor = int(minor) + 1

    return f"{settings['versionPrefix']}{major}.{next_minor}.0"

