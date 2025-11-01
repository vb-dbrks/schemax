"""
SQL Generation Command

Generates SQL migration scripts from schema changes in the changelog.
"""

from pathlib import Path

from rich.console import Console
from rich.syntax import Syntax

from ..providers.base.operations import Operation
from ..storage_v4 import get_environment_config, load_current_state, read_project, read_snapshot

console = Console()


class SQLGenerationError(Exception):
    """Raised when SQL generation fails"""

    pass


# TODO: Snapshot-based SQL generation requires issue #56 to be implemented first
# https://github.com/users/vb-dbrks/projects/1/views/1?pane=issue&itemId=136096732
#
# Once #56 is complete, snapshots will preserve full operation objects:
#   snapshot.operations = [Operation(...), ...]  # Full objects with metadata
#
# This will enable:
# - Simpler implementation (no state reconstruction needed)
# - Accurate operation metadata (timestamps, original payloads)
# - Proper diff between non-adjacent snapshots
# - Better maintainability (DRY principle)


def build_catalog_mapping(state: dict, env_config: dict) -> dict[str, str]:
    """
    Build catalog name mapping (logical → physical) for environment-specific SQL generation.

    Supports two modes:
    1. Single-catalog (implicit): Catalog stored as __implicit__ in state, mapped to env catalog
    2. Single-catalog (explicit): One named catalog, mapped to env catalog
    3. Multi-catalog: Not yet supported
    """
    catalogs = state.get("catalogs", [])

    if len(catalogs) == 0:
        # No catalogs yet - no mapping needed
        return {}

    if len(catalogs) == 1:
        logical_name = catalogs[0]["name"]
        physical_name = env_config["topLevelName"]

        console.print(f"[dim]  Catalog mapping: {logical_name} → {physical_name}[/dim]")

        return {logical_name: physical_name}

    # Multiple catalogs - not supported yet
    raise SQLGenerationError(
        f"Multi-catalog projects are not yet supported. "
        f"Found {len(catalogs)} catalogs: {', '.join(c['name'] for c in catalogs)}. "
        "For now, please use a single catalog per project."
    )


def generate_sql_migration(
    workspace: Path,
    output: Path | None = None,
    snapshot: str | None = None,
    from_version: str | None = None,
    to_version: str | None = None,
    target_env: str | None = None,
) -> str:
    """Generate SQL migration script from schema changes

    Generates SQL DDL statements from operations in the changelog or from a snapshot.
    Can optionally output to a file or print to stdout with syntax highlighting.

    Args:
        workspace: Path to Schematic workspace
        output: Optional output file path
        snapshot: Optional snapshot version ('latest' or specific version like 'v0.1.0')
        from_version: Optional starting version for SQL generation
        to_version: Optional ending version for SQL generation
        target_env: Optional target environment (for catalog name mapping)

    Returns:
        Generated SQL string

    Raises:
        SQLGenerationError: If SQL generation fails
    """
    try:
        # Load project
        project = read_project(workspace)

        # Determine source (snapshot or changelog)
        if snapshot:
            # Resolve 'latest' to actual version
            if snapshot == "latest":
                if not project.get("latestSnapshot"):
                    raise SQLGenerationError(
                        "No snapshots available. Use 'schematic sql' to generate from changelog."
                    )
                snapshot_version = project["latestSnapshot"]
                console.print(f"[blue]Snapshot:[/blue] {snapshot_version} (latest)")
            else:
                snapshot_version = snapshot
                console.print(f"[blue]Snapshot:[/blue] {snapshot_version}")

            # Read snapshot file
            snapshot_data = read_snapshot(workspace, snapshot_version)

            # TODO: Once issue #56 is implemented, snapshots will include full operations
            # For now, we'll check if the snapshot has operations field (post-#56)
            if "operations" in snapshot_data and snapshot_data["operations"]:
                # Post-#56: Use preserved operations directly
                ops_to_process = snapshot_data["operations"]
                state = snapshot_data["state"]

                # Load provider from current state
                _, _, provider = load_current_state(workspace)

                console.print(f"[blue]Provider:[/blue] {provider.info.name}")
                console.print("[blue]Source:[/blue] Snapshot (operations preserved)")
                console.print(f"[blue]Operations:[/blue] {len(ops_to_process)}")
            else:
                # Pre-#56: Snapshots don't preserve operations yet
                raise SQLGenerationError(
                    "Snapshot-based SQL generation is not yet supported.\n\n"
                    "This feature requires snapshots to preserve full operation objects (issue #56).\n"
                    "See: https://github.com/users/vb-dbrks/projects/1/views/1?pane=issue&itemId=136096732\n\n"
                    "For now, use 'schematic sql' (without --snapshot) to generate from changelog."
                )
        else:
            # Load current state from changelog
            state, changelog, provider = load_current_state(workspace)
            ops_to_process = changelog["ops"]

            console.print(f"[blue]Provider:[/blue] {provider.info.name}")
            console.print(f"[blue]Operations:[/blue] {len(ops_to_process)} (from changelog)")

        # Build catalog name mapping and get env config if target environment specified
        catalog_mapping = {}
        env_config = None
        if target_env:
            env_config = get_environment_config(project, target_env)
            console.print(f"[blue]Target Environment:[/blue] {target_env}")
            console.print(f"[blue]Physical Catalog:[/blue] {env_config['topLevelName']}")
            catalog_mapping = build_catalog_mapping(state, env_config)

            # Log managed locations if present (project-level)
            managed_locs = project.get("managedLocations", {})
            if managed_locs:
                console.print(f"[blue]Managed Locations:[/blue] {len(managed_locs)} configured")
                for name, loc_def in managed_locs.items():
                    env_path = loc_def.get("paths", {}).get(target_env)
                    if env_path:
                        console.print(f"  [dim]• {name}: {env_path}[/dim]")

            # Log external locations if present (project-level)
            external_locs = project.get("externalLocations", {})
            if external_locs:
                console.print(f"[blue]External Locations:[/blue] {len(external_locs)} configured")
                for name, loc_def in external_locs.items():
                    env_path = loc_def.get("paths", {}).get(target_env)
                    if env_path:
                        console.print(f"  [dim]• {name}: {env_path}[/dim]")

        if not ops_to_process:
            console.print("[yellow]No operations to generate SQL for[/yellow]")
            return ""

        # Convert to Operation objects
        operations = [Operation(**op) for op in ops_to_process]

        # Log external table operations (if target environment specified)
        external_table_ops = [
            op
            for op in ops_to_process
            if op.get("op") == "unity.add_table" and op["payload"].get("external")
        ]
        if external_table_ops and target_env:
            console.print(f"\n[cyan]External Tables ({len(external_table_ops)}):[/cyan]")
            for op in external_table_ops:
                table_name = op["payload"]["name"]
                loc_name = op["payload"].get("externalLocationName")
                path = op["payload"].get("path", "")

                # Resolve location from project-level externalLocations
                ext_locs = project.get("externalLocations", {})
                if loc_name and loc_name in ext_locs:
                    loc_def = ext_locs[loc_name]
                    base = loc_def.get("paths", {}).get(target_env)
                    if base:
                        resolved = f"{base}/{path}" if path else base
                        console.print(
                            f"  • {table_name}: {loc_name}/{path or '(base)'} → {resolved}"
                        )
                    else:
                        console.print(
                            f"  • {table_name}: [yellow]Location '{loc_name}' not configured for {target_env}[/yellow]"
                        )
                else:
                    console.print(f"  • {table_name}: [red]Location '{loc_name}' not found[/red]")

        # Generate SQL using provider's SQL generator with catalog mapping and locations
        # Use clean provider API (no type casting, proper dependency injection)
        generator = provider.get_sql_generator(
            state=state,
            name_mapping=catalog_mapping,
            managed_locations=project.get("managedLocations"),
            external_locations=project.get("externalLocations"),
            environment_name=target_env,
        )
        sql_output = generator.generate_sql(operations)

        if output:
            # Write to file
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(sql_output)
            console.print(f"[green]✓[/green] SQL written to {output}")
        else:
            # Print to stdout with syntax highlighting
            syntax = Syntax(sql_output, "sql", theme="monokai", line_numbers=False)
            console.print(syntax)

        return sql_output

    except FileNotFoundError as e:
        raise SQLGenerationError(f"Project files not found: {e}") from e
    except Exception as e:
        raise SQLGenerationError(f"Failed to generate SQL: {e}") from e
