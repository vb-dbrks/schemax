"""
Diff Command - Generate diff operations between snapshots

Compares two snapshot versions and shows the operations needed to transform
one into the other. Useful for understanding changes between versions and
planning environment deployments.
"""

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from ..providers.registry import ProviderRegistry
from ..storage_v4 import read_project, read_snapshot

console = Console()


@click.command()
@click.argument("from_version")
@click.argument("to_version")
@click.option(
    "--workspace",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path.cwd(),
    help="Workspace directory (default: current directory)",
)
@click.option(
    "--show-sql",
    is_flag=True,
    help="Show generated SQL for the diff",
)
@click.option(
    "--show-details",
    is_flag=True,
    help="Show detailed operation payloads",
)
def diff_command(
    from_version: str, to_version: str, workspace: Path, show_sql: bool, show_details: bool
) -> None:
    """Generate diff operations between two snapshot versions

    \b
    Examples:
        schematic diff v0.1.0 v0.10.0
        schematic diff v0.1.0 v0.10.0 --show-sql
        schematic diff v0.1.0 v0.10.0 --show-details
    """
    try:
        # Load snapshots
        console.print("[bold]Loading snapshots...[/bold]")
        old_snap = read_snapshot(workspace, from_version)
        new_snap = read_snapshot(workspace, to_version)

        # Get provider
        project = read_project(workspace)
        provider_id = project["provider"]["type"]
        provider = ProviderRegistry.get(provider_id)

        if not provider:
            console.print(
                f"[bold red]Error:[/bold red] Provider '{provider_id}' not found",
                style="red",
            )
            raise click.Abort()

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
            return

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

            sql_gen = provider.get_sql_generator(new_snap["state"])
            sql = sql_gen.generate_sql(operations)

            # Display SQL with syntax highlighting
            from rich.syntax import Syntax

            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
            console.print(syntax)

    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}", style="red")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}", style="red")
        raise click.Abort()
