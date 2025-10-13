"""
Click-based CLI for SchemaX.
"""

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

# Import providers to initialize them
import schemax.providers  # noqa: F401
from .storage_v3 import (
    load_current_state,
    read_project,
    ensure_project_file,
    get_uncommitted_ops_count,
)
from .providers import ProviderRegistry

console = Console()


@click.group()
@click.version_option(version="0.2.0", prog_name="schemax")
def cli() -> None:
    """SchemaX CLI for catalog schema management (Multi-Provider)"""
    pass


@cli.command()
@click.option(
    "--provider",
    "-p",
    default="unity",
    help="Catalog provider (unity, hive, postgres)",
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def init(provider: str, workspace: str) -> None:
    """Initialize a new SchemaX project"""
    try:
        workspace_path = Path(workspace).resolve()

        # Check if provider exists
        if not ProviderRegistry.has(provider):
            available = ", ".join(ProviderRegistry.get_all_ids())
            console.print(
                f"[red]✗[/red] Provider '{provider}' not found. "
                f"Available providers: {available}"
            )
            sys.exit(1)

        provider_obj = ProviderRegistry.get(provider)

        # Initialize project
        ensure_project_file(workspace_path, provider_id=provider)

        console.print(f"[green]✓[/green] Initialized SchemaX project in {workspace_path}")
        console.print(f"[blue]Provider:[/blue] {provider_obj.info.name}")
        console.print(f"[blue]Version:[/blue] {provider_obj.info.version}")
        console.print("\nNext steps:")
        console.print("  1. Run 'schemax sql' to generate SQL")
        console.print("  2. Use SchemaX VSCode extension to design schemas")
        console.print("  3. Check provider docs: " + (provider_obj.info.docs_url or "N/A"))

    except Exception as e:
        console.print(f"[red]✗[/red] Error initializing project: {e}")
        sys.exit(1)


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
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def sql(
    output: Optional[str],
    from_version: Optional[str],
    to_version: Optional[str],
    environment: Optional[str],
    workspace: str,
) -> None:
    """Generate SQL migration script from schema changes"""

    try:
        workspace_path = Path(workspace).resolve()

        # Load current state, changelog, and provider
        state, changelog, provider = load_current_state(workspace_path)

        console.print(f"[blue]Provider:[/blue] {provider.info.name}")
        console.print(f"[blue]Operations:[/blue] {len(changelog['ops'])}")

        # TODO: Handle version ranges and environment-based incremental SQL
        # For now, generate SQL for all changelog ops
        ops_to_process = changelog["ops"]

        if not ops_to_process:
            console.print("[yellow]No operations to generate SQL for[/yellow]")
            return

        # Convert to Operation objects
        from .providers.base.operations import Operation

        operations = [Operation(**op) for op in ops_to_process]

        # Generate SQL using provider's SQL generator
        generator = provider.get_sql_generator(state)
        sql_output = generator.generate_sql(operations)

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
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def validate(workspace: str) -> None:
    """Validate .schemax/ project files"""

    try:
        workspace_path = Path(workspace).resolve()

        # Try to load project and changelog
        console.print("Validating project files...")
        project = read_project(workspace_path)
        console.print(f"  [green]✓[/green] project.json (version {project['version']})")

        state, changelog, provider = load_current_state(workspace_path)
        console.print(
            f"  [green]✓[/green] changelog.json ({len(changelog['ops'])} operations)"
        )

        # Validate state using provider
        validation = provider.validate_state(state)
        if not validation.valid:
            console.print("[red]✗ State validation failed:[/red]")
            for error in validation.errors:
                console.print(f"  - {error.field}: {error.message}")
            sys.exit(1)

        console.print(f"  [green]✓[/green] State structure valid")

        # Display summary
        console.print(f"\n[bold]Project:[/bold] {project['name']}")
        console.print(f"[bold]Provider:[/bold] {provider.info.name} v{provider.info.version}")
        console.print(f"[bold]Uncommitted Ops:[/bold] {len(changelog['ops'])}")

        # Provider-specific stats (works for Unity Catalog)
        if "catalogs" in state:
            console.print(f"[bold]Catalogs:[/bold] {len(state['catalogs'])}")
            total_schemas = sum(len(c.get("schemas", [])) for c in state["catalogs"])
            console.print(f"[bold]Schemas:[/bold] {total_schemas}")
            total_tables = sum(
                len(s.get("tables", []))
                for c in state["catalogs"]
                for s in c.get("schemas", [])
            )
            console.print(f"[bold]Tables:[/bold] {total_tables}")

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
