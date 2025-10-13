"""
Click-based CLI for SchemaX.
"""

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.syntax import Syntax

from .sql_generator import SQLGenerator
from .storage import load_current_state, read_project

console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="schemax")
def cli() -> None:
    """SchemaX CLI for Unity Catalog schema management"""
    pass


@cli.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output file path (default: stdout)",
)
@click.option(
    "--from-version",
    help="Generate SQL from this version",
)
@click.option(
    "--to-version",
    help="Generate SQL to this version",
)
@click.option(
    "--environment",
    "-e",
    help="Target environment (generates incremental SQL based on last deployment)",
)
def sql(
    output: Optional[str],
    from_version: Optional[str],
    to_version: Optional[str],
    environment: Optional[str],
) -> None:
    """Generate SQL migration script from schema changes"""

    try:
        workspace = Path.cwd()

        # Load current state and changelog
        state, changelog = load_current_state(workspace)

        # TODO: Handle version ranges and environment-based incremental SQL
        # For now, generate SQL for all changelog ops
        ops_to_process = changelog.ops

        if not ops_to_process:
            console.print("[yellow]No operations to generate SQL for[/yellow]")
            return

        # Generate SQL
        generator = SQLGenerator(state)
        sql_output = generator.generate_sql(ops_to_process)

        if output:
            # Write to file
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(sql_output)
            console.print(f"[green]✓[/green] SQL written to {output}")
        else:
            # Print to stdout with syntax highlighting
            syntax = Syntax(sql_output, "sql", theme="monokai", line_numbers=False)
            console.print(syntax)

    except FileNotFoundError as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)


@cli.command()
def validate() -> None:
    """Validate .schemax/ project files"""

    try:
        workspace = Path.cwd()

        # Try to load project and changelog
        console.print("Validating project files...")
        project = read_project(workspace)
        console.print(f"  [green]✓[/green] project.json (version {project.version})")

        state, changelog = load_current_state(workspace)
        console.print(f"  [green]✓[/green] changelog.json ({len(changelog.ops)} operations)")

        # Validate structure
        console.print(f"\nProject: {project.name}")
        console.print(f"  Catalogs: {len(state.catalogs)}")
        total_schemas = sum(len(c.schemas) for c in state.catalogs)
        console.print(f"  Schemas: {total_schemas}")
        total_tables = sum(len(s.tables) for c in state.catalogs for s in c.schemas)
        console.print(f"  Tables: {total_tables}")

        console.print("\n[green]✓ Schema files are valid[/green]")

    except FileNotFoundError as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Validation failed:[/red] {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("version1")
@click.argument("version2")
def diff(version1: str, version2: str) -> None:
    """Show differences between two schema versions"""

    console.print(f"Comparing {version1} to {version2}...")
    console.print("[yellow]Diff functionality not yet implemented[/yellow]")
    # TODO: Implement version comparison


@cli.command()
@click.option(
    "--environment",
    "-e",
    required=True,
    help="Environment name (dev/test/prod)",
)
@click.option(
    "--version",
    "-v",
    help="Version to deploy (default: latest snapshot or 'changelog')",
)
@click.option(
    "--mark-deployed",
    is_flag=True,
    help="Mark the deployment as successful",
)
def deploy(environment: str, version: Optional[str], mark_deployed: bool) -> None:
    """Track deployment to an environment"""

    try:
        workspace = Path.cwd()

        from datetime import datetime
        from uuid import uuid4

        from .models import Deployment
        from .storage import read_changelog, read_project, write_deployment

        console.print(f"Recording deployment to [cyan]{environment}[/cyan]...")

        # Load project
        project = read_project(workspace)
        changelog = read_changelog(workspace)

        # Determine version to deploy
        if not version:
            if project.latest_snapshot:
                version = project.latest_snapshot
                console.print(f"Using latest snapshot: [cyan]{version}[/cyan]")
            else:
                version = "changelog"
                console.print("Using [cyan]changelog[/cyan] (no snapshots yet)")

        # Get operations that were applied
        if version == "changelog":
            ops_applied = [op.id or f"op_{i}" for i, op in enumerate(changelog.ops)]
            snapshot_id = None
        else:
            # For snapshot deployments, we'd need to track ops since last deployment
            # For now, mark as snapshot-based deployment
            ops_applied = []
            snapshot_id = version

        # Create deployment record
        deployment = Deployment(
            id=f"deploy_{uuid4().hex[:8]}",
            environment=environment,
            ts=datetime.utcnow().isoformat() + "Z",
            deployed_by="cli",
            snapshot_id=snapshot_id,
            ops_applied=ops_applied,
            schema_version=version,
            status="success" if mark_deployed else "pending",
        )

        # Write deployment
        write_deployment(workspace, deployment)

        console.print("[green]✓[/green] Deployment recorded")
        console.print(f"  Deployment ID: {deployment.id}")
        console.print(f"  Environment: {deployment.environment}")
        console.print(f"  Version: {deployment.schema_version}")
        console.print(f"  Operations: {len(deployment.ops_applied)}")
        console.print(f"  Status: {deployment.status}")

    except FileNotFoundError as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--environment",
    "-e",
    required=True,
    help="Environment name",
)
@click.option(
    "--version",
    "-v",
    required=True,
    help="Version to bundle",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=".schemax/dab",
    help="Output directory (default: .schemax/dab)",
)
def bundle(environment: str, version: str, output: str) -> None:
    """Generate Databricks Asset Bundle for deployment"""

    try:
        _workspace = Path.cwd()  # noqa: F841 - Reserved for future DAB implementation
        _output_dir = Path(output)  # noqa: F841 - Reserved for future DAB implementation

        console.print(f"Generating DAB for [cyan]{environment}[/cyan] v{version}...")

        # TODO: Implement DAB generation
        # from .dab import generate_dab

        console.print("[yellow]DAB generation not yet implemented[/yellow]")

    except Exception as e:
        console.print(f"[red]✗ Error:[/red] {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
