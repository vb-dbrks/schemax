"""
Storage Layer V4 - Multi-Environment Support

Supports environment-specific catalog configurations with logical → physical name mapping.
Breaking change from v3: environments are now rich objects instead of simple arrays.
"""

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from schemax.domain.errors import StorageConflictError
from schemax.providers import Operation, Provider, ProviderRegistry, ProviderState

SCHEMAX_DIR = ".schemax"
PROJECT_FILENAME = "project.json"
CHANGELOG_FILENAME = "changelog.json"
SNAPSHOTS_DIR = "snapshots"
LOCK_FILENAME = ".workspace.lock"

# Default project settings (single source of truth for new/empty projects and tests)
DEFAULT_PROJECT_SETTINGS: dict[str, Any] = {
    "autoIncrementVersion": True,
    "versionPrefix": "v",
    # "single" = implicit catalog (recommended), "multi" = explicit catalogs
    "catalogMode": "single",
}


def default_project_skeleton_tail() -> dict[str, Any]:
    """Return snapshots, deployments, settings, and latestSnapshot for a new/empty project.

    Shared by ensure_project_file and test fixtures to avoid duplicate structure.
    """
    return {
        "snapshots": [],
        "deployments": [],
        "settings": dict(DEFAULT_PROJECT_SETTINGS),
        "latestSnapshot": None,
    }


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


def get_lock_file_path(workspace_path: Path) -> Path:
    """Get workspace mutation lock path."""
    return get_schemax_dir(workspace_path) / LOCK_FILENAME


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write JSON payload to file with deterministic formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temp_path = tempfile.mkstemp(
        prefix=f"{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


class WorkspaceMutationLock:
    """Single-process lock for write operations against one workspace."""

    def __init__(self, workspace_path: Path) -> None:
        self._workspace_path = workspace_path
        self._lock_path = get_lock_file_path(workspace_path)
        self._locked = False

    def acquire(self) -> None:
        """Acquire lock or raise StorageConflictError if already held."""
        ensure_schemax_dir(self._workspace_path)
        try:
            lock_fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise StorageConflictError(
                message=f"Workspace is locked: {self._lock_path}",
                code="workspace_locked",
            ) from exc
        with os.fdopen(lock_fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(f"{os.getpid()}\n")
        self._locked = True

    def release(self) -> None:
        """Release lock if held."""
        if not self._locked:
            return
        try:
            os.unlink(self._lock_path)
        except FileNotFoundError:
            pass
        self._locked = False

    def __enter__(self) -> "WorkspaceMutationLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback_obj: Any) -> None:
        del exc_type, exc, traceback_obj
        self.release()


class WorkspaceSession:
    """Transactional session for workspace project/changelog/snapshot writes."""

    def __init__(self, workspace_path: Path) -> None:
        self._workspace_path = workspace_path
        self._lock = WorkspaceMutationLock(workspace_path)
        self._project: dict[str, Any] | None = None
        self._changelog: dict[str, Any] | None = None
        self._snapshots: dict[str, dict[str, Any]] = {}

    def __enter__(self) -> "WorkspaceSession":
        self._lock.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback_obj: Any) -> None:
        del exc_type, exc, traceback_obj
        self._lock.release()

    def read_project(self) -> dict[str, Any]:
        """Read and cache project in this session."""
        if self._project is None:
            self._project = read_project(self._workspace_path)
        return self._project

    def write_project(self, project: dict[str, Any]) -> None:
        """Stage project payload for commit."""
        self._project = project

    def read_changelog(self) -> dict[str, Any]:
        """Read and cache changelog in this session."""
        if self._changelog is None:
            self._changelog = read_changelog(self._workspace_path)
        return self._changelog

    def write_changelog(self, changelog: dict[str, Any]) -> None:
        """Stage changelog payload for commit."""
        self._changelog = changelog

    def write_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Stage snapshot payload for commit."""
        snapshot_version = str(snapshot["version"])
        self._snapshots[snapshot_version] = snapshot

    def commit(self) -> None:
        """Atomically persist staged changes."""
        for snapshot in self._snapshots.values():
            snapshot_path = get_snapshot_file_path(self._workspace_path, str(snapshot["version"]))
            _write_json_atomic(snapshot_path, snapshot)
        if self._project is not None:
            _write_json_atomic(get_project_file_path(self._workspace_path), self._project)
        if self._changelog is not None:
            _write_json_atomic(get_changelog_file_path(self._workspace_path), self._changelog)


def ensure_schemax_dir(workspace_path: Path) -> None:
    """Ensure .schemax/snapshots/ directory exists"""
    schemax_dir = get_schemax_dir(workspace_path)
    snapshots_dir = get_snapshots_dir(workspace_path)

    schemax_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)


def ensure_project_file(workspace_path: Path, provider_id: str = "unity") -> None:
    """Initialize a new v4 project with environment configuration

    Args:
        workspace_path: Path to workspace
        provider_id: Provider ID (default: 'unity')
    """
    project_path = get_project_file_path(workspace_path)
    with WorkspaceMutationLock(workspace_path):
        if project_path.exists():
            with open(project_path, encoding="utf-8") as f:
                project = cast(dict[str, Any], json.load(f))
            if project.get("version") == 4:
                return
            raise ValueError(
                f"Project version {project.get('version')} not supported. "
                "Please create a new project or manually migrate to v4."
            )

        workspace_name = workspace_path.name
        provider = ProviderRegistry.get(provider_id)
        if provider is None:
            available = ", ".join(ProviderRegistry.get_all_ids())
            raise ValueError(
                f"Provider '{provider_id}' not found. Available providers: {available}"
            )

        new_project = {
            "version": 4,
            "name": workspace_name,
            "provider": {
                "type": provider_id,
                "version": provider.info.version,
                "environments": {
                    "dev": {
                        "topLevelName": f"dev_{workspace_name}",
                        "description": "Development environment",
                        "allowDrift": True,
                        "requireSnapshot": False,
                        "autoCreateTopLevel": True,
                        "autoCreateSchemaxSchema": True,
                        "catalogMappings": {"__implicit__": f"dev_{workspace_name}"},
                    },
                    "test": {
                        "topLevelName": f"test_{workspace_name}",
                        "description": "Test/staging environment",
                        "allowDrift": False,
                        "requireSnapshot": True,
                        "autoCreateTopLevel": True,
                        "autoCreateSchemaxSchema": True,
                        "catalogMappings": {"__implicit__": f"test_{workspace_name}"},
                    },
                    "prod": {
                        "topLevelName": f"prod_{workspace_name}",
                        "description": "Production environment",
                        "allowDrift": False,
                        "requireSnapshot": True,
                        "requireApproval": False,
                        "autoCreateTopLevel": False,
                        "autoCreateSchemaxSchema": True,
                        "catalogMappings": {"__implicit__": f"prod_{workspace_name}"},
                    },
                },
            },
            "managedLocations": {},
            "externalLocations": {},
            **default_project_skeleton_tail(),
        }

        initial_ops = []
        settings = cast(dict[str, Any], new_project.get("settings", {}))
        if settings.get("catalogMode") == "single":
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
            print("[SchemaX] Auto-created implicit catalog for single-catalog mode")

        new_changelog = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": initial_ops,
            "lastModified": datetime.now(UTC).isoformat(),
        }

        _write_json_atomic(get_project_file_path(workspace_path), new_project)
        _write_json_atomic(get_changelog_file_path(workspace_path), new_changelog)

    provider_name = provider.info.name
    print(f"[SchemaX] Initialized new v4 project: {workspace_name} with provider: {provider_name}")
    print("[SchemaX] Environments: dev, test, prod")


def read_project(workspace_path: Path) -> dict[str, Any]:
    """Read project file (v4 only)

    Args:
        workspace_path: Path to workspace

    Returns:
        Project data

    Raises:
        FileNotFoundError: If project file does not exist
        ValueError: If project version is not v4
    """
    project_path = get_project_file_path(workspace_path)

    if not project_path.exists():
        raise FileNotFoundError(
            f"Project file not found: {project_path}. Run 'schemax init' to create a new project."
        )

    with open(project_path, encoding="utf-8") as f:
        project = cast(dict[str, Any], json.load(f))

    # Enforce v4
    if project.get("version") != 4:
        raise ValueError(
            f"Project version {project.get('version')} not supported. "
            "This version of SchemaX requires v4 projects. "
            "Please create a new project or manually migrate to v4."
        )

    return project


def write_project(workspace_path: Path, project: dict[str, Any]) -> None:
    """Write project file"""
    project_path = get_project_file_path(workspace_path)
    _write_json_atomic(project_path, project)


def read_changelog(workspace_path: Path) -> dict[str, Any]:
    """Read changelog file

    Args:
        workspace_path: Path to workspace

    Returns:
        Changelog data
    """
    changelog_path = get_changelog_file_path(workspace_path)

    try:
        with open(changelog_path, encoding="utf-8") as f:
            return cast(dict[str, Any], json.load(f))
    except FileNotFoundError:
        # Changelog doesn't exist, create empty one
        changelog: dict[str, Any] = {
            "version": 1,
            "sinceSnapshot": None,
            "ops": [],
            "lastModified": datetime.now(UTC).isoformat(),
        }

        with open(changelog_path, "w", encoding="utf-8") as f:
            json.dump(changelog, f, indent=2)

        return changelog


def write_changelog(workspace_path: Path, changelog: dict[str, Any]) -> None:
    """Write changelog file

    Note: Serializes Operation objects to dicts for JSON storage.
    """
    changelog_path = get_changelog_file_path(workspace_path)
    changelog["lastModified"] = datetime.now(UTC).isoformat()

    # Convert Operation objects to dicts for JSON serialization
    serializable_changelog = changelog.copy()
    if "ops" in serializable_changelog and serializable_changelog["ops"]:
        # Check if ops are Operation objects (not dicts)
        if isinstance(serializable_changelog["ops"][0], Operation):
            serializable_changelog["ops"] = [
                operation.model_dump(by_alias=True) for operation in serializable_changelog["ops"]
            ]

    _write_json_atomic(changelog_path, serializable_changelog)


def read_snapshot(workspace_path: Path, version: str) -> dict[str, Any]:
    """Read a snapshot file"""
    snapshot_path = get_snapshot_file_path(workspace_path, version)

    with open(snapshot_path, encoding="utf-8") as f:
        return cast(dict[str, Any], json.load(f))


def write_snapshot(workspace_path: Path, snapshot: dict[str, Any]) -> None:
    """Write a snapshot file"""
    ensure_schemax_dir(workspace_path)
    snapshot_path = get_snapshot_file_path(workspace_path, snapshot["version"])
    _write_json_atomic(snapshot_path, snapshot)


def load_current_state(
    workspace_path: Path, validate: bool = False
) -> tuple[ProviderState, dict[str, Any], Provider, dict[str, Any] | None]:
    """Load current state using provider

    Args:
        workspace_path: Path to workspace
        validate: Whether to validate dependencies (default False for performance)

    Returns:
        Tuple of (state, changelog, provider, validation_result)
        validation_result is None if validate=False, otherwise dict with:
            - errors: list of error messages
            - warnings: list of warning messages
    """
    project = read_project(workspace_path)
    changelog = read_changelog(workspace_path)

    return _load_current_state_from_data(workspace_path, project, changelog, validate=validate)


def _load_current_state_from_data(
    workspace_path: Path,
    project: dict[str, Any],
    changelog: dict[str, Any],
    *,
    validate: bool,
) -> tuple[ProviderState, dict[str, Any], Provider, dict[str, Any] | None]:
    """Load state using preloaded project/changelog data to avoid duplicate disk reads."""
    provider = ProviderRegistry.get(project["provider"]["type"])
    if provider is None:
        raise ValueError(
            f"Provider '{project['provider']['type']}' not found. "
            "Please ensure the provider is installed."
        )

    if project["latestSnapshot"]:
        snapshot = read_snapshot(workspace_path, project["latestSnapshot"])
        state = snapshot["state"]
    else:
        state = provider.create_initial_state()

    ops = [Operation(**op) for op in changelog["ops"]]
    state = provider.apply_operations(state, ops)
    changelog["ops"] = ops

    validation_result = None
    if validate:
        validation_result = validate_dependencies_internal(state, ops, provider)

    return state, changelog, provider, validation_result


def _format_cycle_errors(generator: Any, cycles: list[list[str]]) -> list[dict[str, Any]]:
    """Build list of circular_dependency error dicts from cycle node IDs."""
    errors: list[dict[str, Any]] = []
    for cycle in cycles:
        cycle_names = [generator.id_name_map.get(node_id, node_id) for node_id in cycle]
        cycle_str = " → ".join(cycle_names)
        errors.append(
            {
                "type": "circular_dependency",
                "message": f"Circular dependency: {cycle_str}",
                "cycle": cycle_names,
            }
        )
    return errors


def _collect_graph_hierarchy_warnings(graph: Any) -> list[dict[str, Any]]:
    """Collect hierarchy warning dicts from graph.validate_dependencies()."""
    return [{"type": "hierarchy_warning", "message": w} for w in graph.validate_dependencies()]


def _handle_validation_failure(err: Exception) -> list[dict[str, Any]]:
    """Build warnings payload for dependency validation errors."""
    return [{"type": "validation_error", "message": f"Could not validate dependencies: {err}"}]


def validate_dependencies_internal(
    state: Any, ops: list[Operation], provider: Any
) -> dict[str, Any]:
    """Internal helper to validate dependencies and return structured result"""
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    try:
        generator = provider.get_sql_generator(state=state)
        graph = generator.build_dependency_graph(ops)
        cycles = graph.detect_cycles()
        errors.extend(_format_cycle_errors(generator, cycles))
        warnings.extend(_collect_graph_hierarchy_warnings(graph))
    except (AttributeError, KeyError, TypeError, ValueError, RuntimeError) as err:
        warnings.extend(_handle_validation_failure(err))

    return {"errors": errors, "warnings": warnings}


def append_ops(workspace_path: Path, ops: list[Operation]) -> None:
    """Append operations to changelog

    Args:
        workspace_path: Path to workspace
        ops: Operations to append
    """
    with WorkspaceSession(workspace_path) as session:
        project = session.read_project()
        changelog = session.read_changelog()
        provider = ProviderRegistry.get(project["provider"]["type"])

        if provider is None:
            raise ValueError(f"Provider '{project['provider']['type']}' not found")

        for operation in ops:
            validation = provider.validate_operation(operation)
            if not validation.valid:
                errors = ", ".join(f"{e.field}: {e.message}" for e in validation.errors)
                raise ValueError(f"Invalid operation: {errors}")

        changelog["ops"].extend([operation.model_dump(by_alias=True) for operation in ops])
        session.write_changelog(changelog)
        session.commit()


def _normalize_ops_with_ids(changelog_ops: list[Any]) -> list[dict[str, Any]]:
    """Normalize changelog operations into dict form with stable IDs."""
    ops_with_ids: list[dict[str, Any]] = []
    for index, operation_item in enumerate(changelog_ops):
        if isinstance(operation_item, Operation):
            op_dict = operation_item.model_dump(by_alias=True)
            if not operation_item.id:
                op_dict["id"] = f"op_{index}_{operation_item.ts}_{operation_item.target}"
            ops_with_ids.append(op_dict)
            continue
        if "id" not in operation_item or not operation_item["id"]:
            operation_item["id"] = f"op_{index}_{operation_item['ts']}_{operation_item['target']}"
        ops_with_ids.append(operation_item)
    return ops_with_ids


@dataclass(frozen=True, slots=True)
class _SnapshotRequest:
    """Input request for snapshot creation transaction."""

    name: str
    version: str | None
    comment: str | None
    tags: list[str]


def _build_snapshot_file(
    snapshot_context: dict[str, Any],
) -> dict[str, Any]:
    """Build snapshot file payload."""
    return {
        "id": f"snap_{uuid4()}",
        "version": snapshot_context["snapshot_version"],
        "name": snapshot_context["name"],
        "ts": datetime.now(UTC).isoformat(),
        "createdBy": os.environ.get("USER") or os.environ.get("USERNAME") or "unknown",
        "state": snapshot_context["state"],
        "operations": snapshot_context["ops_with_ids"],
        "previousSnapshot": snapshot_context["previous_snapshot"],
        "hash": snapshot_context["state_hash"],
        "tags": snapshot_context["tags"],
        "comment": snapshot_context["comment"],
    }


def _build_snapshot_metadata(
    snapshot_file: dict[str, Any],
    snapshot_context: dict[str, Any],
) -> dict[str, Any]:
    """Build project snapshot metadata payload."""
    return {
        "id": snapshot_file["id"],
        "version": snapshot_context["snapshot_version"],
        "name": snapshot_context["name"],
        "ts": snapshot_file["ts"],
        "createdBy": snapshot_file["createdBy"],
        "file": f".schemax/snapshots/{snapshot_context['snapshot_version']}.json",
        "previousSnapshot": snapshot_context["previous_snapshot"],
        "opsCount": len(snapshot_context["ops_with_ids"]),
        "hash": snapshot_context["state_hash"],
        "tags": snapshot_context["tags"],
        "comment": snapshot_context["comment"],
    }


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

    request = _SnapshotRequest(name=name, version=version, comment=comment, tags=tags)
    project, snapshot_file, snapshot_metadata, ops_count = _create_snapshot_transaction(
        workspace_path=workspace_path,
        request=request,
    )

    print(f"[SchemaX] Created snapshot {snapshot_file['version']}: {name}")
    print(f"[SchemaX] Snapshot file: {snapshot_metadata['file']}")
    print(f"[SchemaX] Ops included: {ops_count}")

    return project, snapshot_file


def _create_snapshot_transaction(
    *,
    workspace_path: Path,
    request: _SnapshotRequest,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], int]:
    """Execute snapshot creation transaction and return persisted payloads."""
    with WorkspaceSession(workspace_path) as session:
        project = session.read_project()
        changelog = session.read_changelog()
        snapshot_file, snapshot_metadata = _build_snapshot_artifacts(
            workspace_path=workspace_path,
            project=project,
            changelog=changelog,
            request=request,
        )
        snapshot_version = str(snapshot_file["version"])
        project["snapshots"].append(snapshot_metadata)
        project["latestSnapshot"] = snapshot_version
        session.write_snapshot(snapshot_file)
        session.write_project(project)
        session.write_changelog(
            {
                "version": 1,
                "sinceSnapshot": snapshot_version,
                "ops": [],
                "lastModified": datetime.now(UTC).isoformat(),
            }
        )
        session.commit()
    return project, snapshot_file, snapshot_metadata, len(snapshot_file["operations"])


def _build_snapshot_artifacts(
    *,
    workspace_path: Path,
    project: dict[str, Any],
    changelog: dict[str, Any],
    request: _SnapshotRequest,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build snapshot file and metadata from current state/changelog."""
    state, _, _, _ = _load_current_state_from_data(
        workspace_path, project, changelog, validate=False
    )
    previous_snapshot = cast(str | None, project.get("latestSnapshot"))
    snapshot_version = request.version or _get_next_version(previous_snapshot, project["settings"])
    ops_with_ids = _normalize_ops_with_ids(cast(list[Any], changelog["ops"]))
    snapshot_context = {
        "snapshot_version": snapshot_version,
        "name": request.name,
        "comment": request.comment,
        "tags": request.tags,
        "state": state,
        "ops_with_ids": ops_with_ids,
        "previous_snapshot": previous_snapshot,
        "state_hash": _calculate_state_hash(state, ops_with_ids),
    }
    snapshot_file = _build_snapshot_file(snapshot_context)
    snapshot_metadata = _build_snapshot_metadata(snapshot_file, snapshot_context)
    return snapshot_file, snapshot_metadata


def get_uncommitted_ops_count(workspace_path: Path) -> int:
    """Get number of uncommitted operations"""
    changelog = read_changelog(workspace_path)
    return len(changelog["ops"])


def _calculate_state_hash(state: Any, operations: list[dict[str, Any]]) -> str:
    """Calculate hash of state and operations for integrity checking"""
    content = json.dumps({"state": state, "operations": operations}, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()


def _get_next_version(current_version: str | None, settings: dict[str, Any]) -> str:
    """Get next version number"""
    version_prefix = str(settings.get("versionPrefix", "v"))
    if not current_version:
        return version_prefix + "0.1.0"

    # Parse version (e.g., "v0.1.0" or "0.1.0")
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", current_version)
    if not match:
        return version_prefix + "0.1.0"

    major, minor, _patch = match.groups()
    next_minor = int(minor) + 1

    return f"{version_prefix}{major}.{next_minor}.0"


def get_environment_config(project: dict[str, Any], environment: str) -> dict[str, Any]:
    """Get environment configuration from project.

    Each entry under provider.environments.<env> may include optional:
    - importBaselineSnapshot (string, snapshot version) set when
      import --adopt-baseline runs; used to block rollback before baseline
      unless --force is used.
    - managedCategories (list[str]): deployment scope; only ops in these
      categories are emitted (e.g. ["governance"] for governance-only mode).
      Values: catalog_structure, schema_structure, table_structure,
      view_structure, governance. If missing, all categories are included.
    - existingObjects (dict): objects that already exist; skip CREATE for
      them. e.g. {"catalog": ["analytics"]} to skip CREATE CATALOG for
      logical catalog "analytics".

    Args:
        project: Project data (v4)
        environment: Environment name (e.g., "dev", "prod")

    Returns:
        Environment configuration (reference into project dict)

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
