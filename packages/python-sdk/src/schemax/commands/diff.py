"""
Diff Command - Generate diff operations between snapshots

Compares two snapshot versions and shows the operations needed to transform
one into the other. Useful for understanding changes between versions and
planning environment deployments.
"""

from pathlib import Path

from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from schemax.commands.sql import SQLGenerationError, build_catalog_mapping
from schemax.core.storage import get_environment_config, read_project, read_snapshot
from schemax.providers.base.operations import Operation
from schemax.providers.registry import ProviderRegistry

console = Console()


class DiffError(Exception):
    """Raised when diff command fails"""


def generate_diff(
    workspace: Path,
    from_version: str,
    to_version: str,
    show_sql: bool = False,
    show_details: bool = False,
    target_env: str | None = None,
) -> list[Operation]:
    """Generate diff operations between two snapshot versions

    Args:
        workspace: Workspace directory
        from_version: Source snapshot version
        to_version: Target snapshot version
        show_sql: Whether to display generated SQL
        show_details: Whether to display detailed operation payloads
        target_env: Target environment for catalog name mapping (optional)

    Returns:
        List of operations representing the diff

    Raises:
        DiffError: If diff generation fails
    """
    try:
        # Validate input versions
        if from_version == to_version:
            raise DiffError(
                f"Cannot diff the same version with itself: {from_version}\n"
                "Please provide different snapshot versions."
            )

        # Load snapshots
        console.print("[bold]Loading snapshots...[/bold]")
        old_snap = read_snapshot(workspace, from_version)
        new_snap = read_snapshot(workspace, to_version)

        # Validate snapshot structure
        if "state" not in old_snap:
            raise DiffError(f"Invalid snapshot structure: {from_version} is missing 'state' field")
        if "state" not in new_snap:
            raise DiffError(f"Invalid snapshot structure: {to_version} is missing 'state' field")

        console.print(f"  [green]✓[/green] {from_version}")
        console.print(f"  [green]✓[/green] {to_version}")

        # Get provider
        project = read_project(workspace)
        provider_id = project["provider"]["type"]
        provider = ProviderRegistry.get(provider_id)

        if not provider:
            raise DiffError(f"Provider '{provider_id}' not found in registry")

        # Build catalog mapping if target environment specified
        catalog_mapping = None
        if target_env:
            env_config = get_environment_config(project, target_env)
            try:
                catalog_mapping = _build_catalog_mapping(new_snap["state"], env_config)
            except SQLGenerationError as e:
                raise DiffError(str(e)) from e
            console.print(f"  [blue]Environment:[/blue] {target_env}")
            console.print(f"  [blue]Catalog mapping:[/blue] {catalog_mapping}")

        # Generate diff
        console.print(f"[bold]Generating diff: {from_version} → {to_version}[/bold]")
        differ = provider.get_state_differ(
            old_snap["state"],
            new_snap["state"],
            old_snap.get("operations", []),
            new_snap.get("operations", []),
        )

        operations = differ.generate_diff_operations()

        # Display results
        console.print()
        console.print("[bold green]✓[/bold green] Diff generated successfully")
        console.print(f"  Operations: {len(operations)}")
        console.print()

        if not operations:
            console.print("[yellow]No changes detected between versions[/yellow]")
            return operations

        # Create operations table
        table = Table(title="Diff Operations", show_header=True, header_style="bold magenta")
        table.add_column("#", style="dim", width=4)
        table.add_column("Operation", style="cyan")
        table.add_column("Target", style="green")
        if show_details:
            table.add_column("Details", style="yellow")

        for i, op in enumerate(operations, 1):
            op_dict = op if isinstance(op, dict) else op.model_dump()
            row = [
                str(i),
                op_dict["op"],
                op_dict["target"],
            ]
            if show_details:
                payload = op_dict.get("payload", {})
                details = ", ".join(f"{k}={v}" for k, v in payload.items() if v is not None)
                row.append(details[:50] + "..." if len(details) > 50 else details)
            table.add_row(*row)

        console.print(table)

        # Generate and display SQL if requested
        if show_sql:
            console.print()
            console.print("[bold]Generated SQL:[/bold]")
            console.print()

            sql_gen = provider.get_sql_generator(new_snap["state"], catalog_mapping)
            sql = sql_gen.generate_sql(operations)

            # Display SQL with syntax highlighting
            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
            console.print(syntax)

        return operations

    except DiffError:
        # Re-raise DiffError as-is (already has good error message)
        raise
    except FileNotFoundError as e:
        # Provide helpful message about missing snapshot files
        error_msg = str(e)
        if from_version in error_msg:
            raise DiffError(
                f"Source snapshot not found: {from_version}\n"
                f"Check that the snapshot exists in .schemax/snapshots/{from_version}.json"
            ) from e
        if to_version in error_msg:
            raise DiffError(
                f"Target snapshot not found: {to_version}\n"
                f"Check that the snapshot exists in .schemax/snapshots/{to_version}.json"
            ) from e
        raise DiffError(f"Snapshot file not found: {e}") from e
    except Exception as e:
        raise DiffError(f"Failed to generate diff: {e}") from e


def _build_catalog_mapping(state: dict, env_config: dict) -> dict[str, str]:
    """Delegate to shared SQL mapping builder."""
    return build_catalog_mapping(state, env_config)
