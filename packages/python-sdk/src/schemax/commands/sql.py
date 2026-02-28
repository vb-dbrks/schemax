"""
SQL Generation Command

Generates SQL migration scripts from schema changes in the changelog.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from rich.console import Console
from rich.syntax import Syntax

from schemax.core.storage import (
    get_environment_config,
    load_current_state,
    read_project,
    read_snapshot,
)
from schemax.providers.base.operations import Operation
from schemax.providers.base.scope_filter import filter_operations_by_managed_scope

console = Console()


class SQLGenerationError(Exception):
    """Raised when SQL generation fails"""


# TODO: Snapshot-based SQL generation requires issue #56 to be implemented first
# https://github.com/users/vb-dbrks/projects/1/views/1?pane=issue&itemId=136096732
#
# Once #56 is complete, snapshots will preserve full operation objects.
# This will enable:
# - Simpler implementation (no state reconstruction needed)
# - Accurate operation metadata (timestamps, original payloads)
# - Proper diff between non-adjacent snapshots
# - Better maintainability (DRY principle)


@dataclass
class _OpsSource:
    """Result of resolving where to get operations from (snapshot or changelog)."""

    state: Any
    ops: list[Any]
    provider: Any


@dataclass
class _EnvContext:
    """Catalog mapping and env config for target environment."""

    catalog_mapping: dict[str, str]
    env_config: dict[str, Any] | None


def build_catalog_mapping(state: dict, env_config: dict) -> dict[str, str]:
    """
    Build catalog name mapping (logical → physical) for environment-specific SQL generation.

    Uses environment `catalogMappings` (logical -> physical).
    Requires an explicit mapping for each logical catalog present in state.
    """
    catalogs = state.get("catalogs", [])

    if len(catalogs) == 0:
        # No catalogs yet - no mapping needed
        return {}

    raw_mappings = env_config.get("catalogMappings") or {}
    if not isinstance(raw_mappings, dict):
        raise SQLGenerationError(
            "Environment catalogMappings must be an object mapping logical->physical"
        )

    mappings = {str(logical): str(physical) for logical, physical in raw_mappings.items()}
    catalog_names = [str(c.get("name")) for c in catalogs if c.get("name")]
    missing = [name for name in catalog_names if name not in mappings]
    if missing:
        raise SQLGenerationError(
            "Missing catalog mapping(s) for logical catalog(s): "
            f"{', '.join(sorted(missing))}. "
            "Add them under provider.environments.<env>.catalogMappings."
        )

    resolved = {logical: mappings[logical] for logical in catalog_names}
    rendered = ", ".join(
        f"{logical} → {physical}" for logical, physical in sorted(resolved.items())
    )
    console.print(f"[dim]  Catalog mappings: {rendered}[/dim]")
    return resolved


def generate_sql_migration(
    workspace: Path,
    output: Path | None = None,
    snapshot: str | None = None,
    _from_version: str | None = None,
    _to_version: str | None = None,
    target_env: str | None = None,
) -> str:
    """Generate SQL migration script from schema changes

    Generates SQL DDL statements from operations in the changelog or from a snapshot.
    Can optionally output to a file or print to stdout with syntax highlighting.

    Args:
        workspace: Path to SchemaX workspace
        output: Optional output file path
        snapshot: Optional snapshot version ('latest' or specific version like 'v0.1.0')
        target_env: Optional target environment (for catalog name mapping)

    Returns:
        Generated SQL string

    Raises:
        SQLGenerationError: If SQL generation fails
    """
    try:
        return _generate_sql_impl(workspace, output, snapshot, target_env)
    except FileNotFoundError as err:
        raise SQLGenerationError(f"Project files not found: {err}") from err
    except SQLGenerationError:
        raise
    except Exception as err:
        raise SQLGenerationError(f"Failed to generate SQL: {err}") from err


def _generate_sql_impl(
    workspace: Path,
    output: Path | None,
    snapshot: str | None,
    target_env: str | None,
) -> str:
    """Inner implementation of SQL generation (called from generate_sql_migration)."""
    project = read_project(workspace)
    source = _resolve_ops_source(workspace, project, snapshot)
    if not source.ops:
        console.print("[yellow]No operations to generate SQL for[/yellow]")
        return ""
    env_ctx = _build_env_context(project, target_env, source.state)
    operations = _prepare_operations(source.ops, env_ctx.env_config, source.provider)
    if env_ctx.env_config and source.provider:
        console.print(f"[dim]After deployment scope filter: {len(operations)} ops[/dim]")
    if target_env:
        _log_external_table_ops(operations, project, target_env)
    generator = source.provider.get_sql_generator(
        state=source.state,
        name_mapping=env_ctx.catalog_mapping,
        managed_locations=project.get("managedLocations"),
        external_locations=project.get("externalLocations"),
        environment_name=target_env,
    )
    sql_output = generator.generate_sql(operations)
    _output_sql(sql_output, output)
    return cast(str, sql_output)


def _resolve_ops_source(workspace: Path, project: dict, snapshot: str | None) -> _OpsSource:
    """Determine snapshot vs changelog source; load state, ops, and provider."""
    if snapshot:
        return _resolve_ops_source_from_snapshot(workspace, project, snapshot)
    return _resolve_ops_source_from_changelog(workspace)


def _resolve_ops_source_from_snapshot(workspace: Path, project: dict, snapshot: str) -> _OpsSource:
    """Load state and ops from a snapshot file."""
    if snapshot == "latest":
        if not project.get("latestSnapshot"):
            raise SQLGenerationError(
                "No snapshots available. Use 'schemax sql' to generate from changelog."
            )
        snapshot_version = project["latestSnapshot"]
        console.print(f"[blue]Snapshot:[/blue] {snapshot_version} (latest)")
    else:
        snapshot_version = snapshot
        console.print(f"[blue]Snapshot:[/blue] {snapshot_version}")
    snapshot_data = read_snapshot(workspace, snapshot_version)
    if not ("operations" in snapshot_data and snapshot_data["operations"]):
        raise SQLGenerationError(
            "Snapshot-based SQL generation is not yet supported.\n\n"
            "This feature requires snapshots to preserve full operation objects (issue #56).\n"
            "See: https://github.com/users/vb-dbrks/projects/1/views/1?pane=issue&itemId=136096732\n\n"
            "For now, use 'schemax sql' (without --snapshot) to generate from changelog."
        )
    ops_to_process = snapshot_data["operations"]
    state = snapshot_data["state"]
    _, _, provider, _ = load_current_state(workspace, validate=False)
    console.print(f"[blue]Provider:[/blue] {provider.info.name}")
    console.print("[blue]Source:[/blue] Snapshot (operations preserved)")
    console.print(f"[blue]Operations:[/blue] {len(ops_to_process)}")
    return _OpsSource(state=state, ops=ops_to_process, provider=provider)


def _resolve_ops_source_from_changelog(workspace: Path) -> _OpsSource:
    """Load state and ops from changelog."""
    state, changelog, provider, _ = load_current_state(workspace, validate=False)
    ops_to_process = changelog["ops"]
    console.print(f"[blue]Provider:[/blue] {provider.info.name}")
    console.print(f"[blue]Operations:[/blue] {len(ops_to_process)} (from changelog)")
    return _OpsSource(state=state, ops=ops_to_process, provider=provider)


def _build_env_context(project: dict, target_env: str | None, state: Any) -> _EnvContext:
    """Build catalog mapping and log managed/external locations for target env."""
    catalog_mapping: dict[str, str] = {}
    env_config = None
    if target_env:
        env_config = get_environment_config(project, target_env)
        console.print(f"[blue]Target Environment:[/blue] {target_env}")
        console.print(f"[blue]Physical Catalog:[/blue] {env_config['topLevelName']}")
        catalog_mapping = build_catalog_mapping(state, env_config)
        _log_managed_and_external_locations(project, target_env)
    return _EnvContext(catalog_mapping=catalog_mapping, env_config=env_config)


def _log_managed_and_external_locations(project: dict, target_env: str) -> None:
    """Log project-level managed and external location paths for target env."""
    managed_locs = project.get("managedLocations", {})
    if managed_locs:
        console.print(f"[blue]Managed Locations:[/blue] {len(managed_locs)} configured")
        for name, loc_def in managed_locs.items():
            env_path = loc_def.get("paths", {}).get(target_env)
            if env_path:
                console.print(f"  [dim]• {name}: {env_path}[/dim]")
    external_locs = project.get("externalLocations", {})
    if external_locs:
        console.print(f"[blue]External Locations:[/blue] {len(external_locs)} configured")
        for name, loc_def in external_locs.items():
            env_path = loc_def.get("paths", {}).get(target_env)
            if env_path:
                console.print(f"  [dim]• {name}: {env_path}[/dim]")


def _prepare_operations(
    ops_to_process: list[Any],
    env_config: dict[str, Any] | None,
    provider: Any,
) -> list[Operation]:
    """Convert to Operation objects and apply deployment scope filter."""
    if ops_to_process and isinstance(ops_to_process[0], dict):
        operations = [Operation(**item) for item in ops_to_process]
    else:
        operations = ops_to_process
    if env_config and provider:
        operations = filter_operations_by_managed_scope(operations, env_config, provider)
    return operations


def _extract_add_table_payload(
    operation: Operation | dict[str, Any],
) -> tuple[str, str | None, str] | None:
    """Get (table_name, external_location_name, path) from add_table op or None if not external."""
    if isinstance(operation, Operation):
        if operation.op != "unity.add_table" or not operation.payload.get("external"):
            return None
        payload = operation.payload
    else:
        if operation.get("op") != "unity.add_table" or not operation.get("payload", {}).get(
            "external"
        ):
            return None
        payload = operation["payload"]
    name = payload.get("name", "")
    loc_name = payload.get("externalLocationName")
    path = payload.get("path", "") or ""
    return (name, loc_name, path)


def _log_external_table_ops(operations: list[Operation], project: dict, target_env: str) -> None:
    """Extract and display external table operation details."""
    external_details = []
    for operation in operations:
        details = _extract_add_table_payload(operation)
        if details is not None:
            external_details.append(details)

    if not external_details:
        return

    console.print(f"\n[cyan]External Tables ({len(external_details)}):[/cyan]")
    ext_locs = project.get("externalLocations", {})
    for table_name, loc_name, path in external_details:
        if loc_name and loc_name in ext_locs:
            loc_def = ext_locs[loc_name]
            base = loc_def.get("paths", {}).get(target_env)
            if base:
                resolved = f"{base}/{path}" if path else base
                console.print(f"  • {table_name}: {loc_name}/{path or '(base)'} → {resolved}")
            else:
                console.print(
                    f"  • {table_name}: [yellow]Location '{loc_name}' not configured for "
                    f"{target_env}[/yellow]"
                )
        else:
            console.print(f"  • {table_name}: [red]Location '{loc_name}' not found[/red]")


def _output_sql(sql_output: str, output_path: Path | None) -> None:
    """Write SQL to file or print to stdout with syntax highlighting."""
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sql_output)
        console.print(f"[green]✓[/green] SQL written to {output_path}")
    else:
        syntax = Syntax(sql_output, "sql", theme="monokai", line_numbers=False)
        console.print(syntax)
