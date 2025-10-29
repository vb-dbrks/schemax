"""
Storage Layer V4 - Multi-Environment Support

Supports environment-specific catalog configurations with logical → physical name mapping.
Breaking change from v3: environments are now rich objects instead of simple arrays.
"""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from .providers import Operation, Provider, ProviderRegistry, ProviderState

SCHEMATIC_DIR = ".schematic"
PROJECT_FILENAME = "project.json"
CHANGELOG_FILENAME = "changelog.json"
SNAPSHOTS_DIR = "snapshots"


def get_schematic_dir(workspace_path: Path) -> Path:
    """Get the .schematic directory path"""
    return workspace_path / SCHEMATIC_DIR


def get_project_file_path(workspace_path: Path) -> Path:
    """Get the project file path"""
    return get_schematic_dir(workspace_path) / PROJECT_FILENAME


def get_changelog_file_path(workspace_path: Path) -> Path:
    """Get the changelog file path"""
    return get_schematic_dir(workspace_path) / CHANGELOG_FILENAME


def get_snapshots_dir(workspace_path: Path) -> Path:
    """Get the snapshots directory path"""
    return get_schematic_dir(workspace_path) / SNAPSHOTS_DIR


def get_snapshot_file_path(workspace_path: Path, version: str) -> Path:
    """Get snapshot file path"""
    return get_snapshots_dir(workspace_path) / f"{version}.json"


def ensure_schematic_dir(workspace_path: Path) -> None:
    """Ensure .schematic/snapshots/ directory exists"""
    schematic_dir = get_schematic_dir(workspace_path)
    snapshots_dir = get_snapshots_dir(workspace_path)

    schematic_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)


def ensure_project_file(workspace_path: Path, provider_id: str = "unity") -> None:
    """Initialize a new v4 project with environment configuration

    Args:
        workspace_path: Path to workspace
        provider_id: Provider ID (default: 'unity')
    """
    project_path = get_project_file_path(workspace_path)
    changelog_path = get_changelog_file_path(workspace_path)

    if project_path.exists():
        # Project exists - check version
        with open(project_path) as f:
            project = cast(dict[str, Any], json.load(f))

        if project.get("version") == 4:
            return  # Already v4
        else:
            raise ValueError(
                f"Project version {project.get('version')} not supported. "
                "Please create a new project or manually migrate to v4."
            )

    # Create new v4 project
    workspace_name = workspace_path.name

    # Verify provider exists
    provider = ProviderRegistry.get(provider_id)
    if provider is None:
        available = ", ".join(ProviderRegistry.get_all_ids())
        raise ValueError(f"Provider '{provider_id}' not found. Available providers: {available}")

    # Create v4 project with environment configuration
    new_project = {
        "version": 4,
        "name": workspace_name,
        "provider": {
            "type": provider_id,
            "version": provider.info.version,
            "environments": {
                "dev": {
                    "catalog": f"dev_{workspace_name}",
                    "description": "Development environment",
                    "allowDrift": True,
                    "requireSnapshot": False,
                    "autoCreateCatalog": True,
                    "autoCreateSchematicSchema": True,
                },
                "test": {
                    "catalog": f"test_{workspace_name}",
                    "description": "Test/staging environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "autoCreateCatalog": True,
                    "autoCreateSchematicSchema": True,
                },
                "prod": {
                    "catalog": f"prod_{workspace_name}",
                    "description": "Production environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "requireApproval": False,
                    "autoCreateCatalog": False,
                    "autoCreateSchematicSchema": True,
                },
            },
        },
        "snapshots": [],
        "deployments": [],
        "settings": {
            "autoIncrementVersion": True,
            "versionPrefix": "v",
            # "single" = implicit catalog (recommended), "multi" = explicit catalogs
            "catalogMode": "single",
        },
        "latestSnapshot": None,
    }

    # Initialize changelog with implicit catalog for single-catalog mode
    initial_ops = []

    settings = cast(dict[str, Any], new_project.get("settings", {}))
    if settings.get("catalogMode") == "single":
        # Auto-create implicit catalog
        catalog_id = "cat_implicit"
        initial_ops.append(
            {
                "id": "op_init_catalog",
                "ts": datetime.now(UTC).isoformat(),
                "provider": provider_id,
                "op": f"{provider_id}.add_catalog",
                "target": catalog_id,
                "payload": {"catalogId": catalog_id, "name": "__implicit__"},
            }
        )
        print("[Schematic] Auto-created implicit catalog for single-catalog mode")

    new_changelog = {
        "version": 1,
        "sinceSnapshot": None,
        "ops": initial_ops,
        "lastModified": datetime.now(UTC).isoformat(),
    }

    ensure_schematic_dir(workspace_path)

    with open(project_path, "w") as f:
        json.dump(new_project, f, indent=2)

    with open(changelog_path, "w") as f:
        json.dump(new_changelog, f, indent=2)

    provider_name = provider.info.name
    print(
        f"[Schematic] Initialized new v4 project: {workspace_name} with provider: {provider_name}"
    )
    print("[Schematic] Environments: dev, test, prod")


def read_project(workspace_path: Path) -> dict[str, Any]:
    """Read project file (v4 only)

    Args:
        workspace_path: Path to workspace

    Returns:
        Project data

    Raises:
        ValueError: If project version is not v4
    """
    project_path = get_project_file_path(workspace_path)

    if not project_path.exists():
        raise FileNotFoundError(
            f"Project file not found: {project_path}. Run 'schematic init' to create a new project."
        )

    with open(project_path) as f:
        project = cast(dict[str, Any], json.load(f))

    # Enforce v4
    if project.get("version") != 4:
        raise ValueError(
            f"Project version {project.get('version')} not supported. "
            "This version of Schematic requires v4 projects. "
            "Please create a new project or manually migrate to v4."
        )

    return project


def write_project(workspace_path: Path, project: dict[str, Any]) -> None:
    """Write project file"""
    project_path = get_project_file_path(workspace_path)

    with open(project_path, "w") as f:
        json.dump(project, f, indent=2)


def read_changelog(workspace_path: Path) -> dict[str, Any]:
    """Read changelog file

    Args:
        workspace_path: Path to workspace

    Returns:
        Changelog data
    """
    changelog_path = get_changelog_file_path(workspace_path)

    try:
        with open(changelog_path) as f:
            return cast(dict[str, Any], json.load(f))
    except FileNotFoundError:
        # Changelog doesn't exist, create empty one
        changelog: dict[str, Any] = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": [],
            "lastModified": datetime.now(UTC).isoformat(),
        }

        with open(changelog_path, "w") as f:
            json.dump(changelog, f, indent=2)

        return changelog


def write_changelog(workspace_path: Path, changelog: dict[str, Any]) -> None:
    """Write changelog file"""
    changelog_path = get_changelog_file_path(workspace_path)
    changelog["lastModified"] = datetime.now(UTC).isoformat()

    with open(changelog_path, "w") as f:
        json.dump(changelog, f, indent=2)


def read_snapshot(workspace_path: Path, version: str) -> dict[str, Any]:
    """Read a snapshot file"""
    snapshot_path = get_snapshot_file_path(workspace_path, version)

    with open(snapshot_path) as f:
        return cast(dict[str, Any], json.load(f))


def write_snapshot(workspace_path: Path, snapshot: dict[str, Any]) -> None:
    """Write a snapshot file"""
    ensure_schematic_dir(workspace_path)
    snapshot_path = get_snapshot_file_path(workspace_path, snapshot["version"])

    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2)


def load_current_state(
    workspace_path: Path,
) -> tuple[ProviderState, dict[str, Any], Provider]:
    """Load current state using provider

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


def append_ops(workspace_path: Path, ops: list[Operation]) -> None:
    """Append operations to changelog

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
    changelog["ops"].extend([op.model_dump(by_alias=True) for op in ops])

    # Write back
    write_changelog(workspace_path, changelog)


def create_snapshot(
    workspace_path: Path,
    name: str,
    version: str | None = None,
    comment: str | None = None,
    tags: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create a snapshot

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
        "ts": datetime.now(UTC).isoformat(),
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
        "file": f".schematic/snapshots/{snapshot_version}.json",
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
        "lastModified": datetime.now(UTC).isoformat(),
    }
    write_changelog(workspace_path, new_changelog)

    print(f"[Schematic] Created snapshot {snapshot_version}: {name}")
    print(f"[Schematic] Snapshot file: {snapshot_metadata['file']}")
    print(f"[Schematic] Ops included: {len(ops_with_ids)}")

    return project, snapshot_file


def get_uncommitted_ops_count(workspace_path: Path) -> int:
    """Get number of uncommitted operations"""
    changelog = read_changelog(workspace_path)
    return len(changelog["ops"])


def _calculate_state_hash(state: Any, ops_included: list[str]) -> str:
    """Calculate hash of state for integrity checking"""
    content = json.dumps({"state": state, "opsIncluded": ops_included}, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()


def _get_next_version(current_version: str | None, settings: dict[str, Any]) -> str:
    """Get next version number"""
    version_prefix = str(settings.get("versionPrefix", "v"))
    if not current_version:
        return version_prefix + "0.1.0"

    # Parse version (e.g., "v0.1.0" or "0.1.0")
    import re

    match = re.search(r"(\d+)\.(\d+)\.(\d+)", current_version)
    if not match:
        return version_prefix + "0.1.0"

    major, minor, patch = match.groups()
    next_minor = int(minor) + 1

    return f"{version_prefix}{major}.{next_minor}.0"


def write_deployment(workspace_path: Path, deployment: dict[str, Any]) -> None:
    """Add a deployment record to project file

    Args:
        workspace_path: Path to workspace
        deployment: Deployment data
    """
    project = read_project(workspace_path)

    if "deployments" not in project:
        project["deployments"] = []

    project["deployments"].append(deployment)
    write_project(workspace_path, project)


def get_last_deployment(project: dict[str, Any], environment: str) -> dict[str, Any] | None:
    """Get the last deployment for a specific environment

    Args:
        project: Project data
        environment: Environment name

    Returns:
        Last deployment record or None
    """
    deployments = [d for d in project.get("deployments", []) if d.get("environment") == environment]
    if not deployments:
        return None

    # Sort by timestamp, return most recent
    sorted_deployments = sorted(deployments, key=lambda d: d.get("ts", ""), reverse=True)
    return cast(dict[str, Any], sorted_deployments[0])


def get_environment_config(project: dict[str, Any], environment: str) -> dict[str, Any]:
    """Get environment configuration from project

    Args:
        project: Project data (v4)
        environment: Environment name (e.g., "dev", "prod")

    Returns:
        Environment configuration

    Raises:
        ValueError: If environment not found
    """
    environments = project.get("provider", {}).get("environments", {})

    if environment not in environments:
        available = ", ".join(environments.keys())
        raise ValueError(
            f"Environment '{environment}' not found in project. Available environments: {available}"
        )

    return cast(dict[str, Any], environments[environment])
