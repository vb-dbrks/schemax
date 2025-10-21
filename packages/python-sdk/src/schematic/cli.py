"""
Click-based CLI for Schematic.
"""

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

# Import providers to initialize them
import schematic.providers  # noqa: F401

from .commands import (
    ApplyError,
    DeploymentRecordingError,
    SQLGenerationError,
    apply_to_environment,
    generate_sql_migration,
    record_deployment_to_environment,
    validate_project,
)
from .commands import (
    ValidationError as CommandValidationError,
)
from .providers import ProviderRegistry
from .storage_v4 import ensure_project_file

console = Console()


@click.group()
@click.version_option(version="0.2.0", prog_name="schematic")
def cli() -> None:
    """Schematic CLI for catalog schema management (Multi-Provider)"""
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
    """Initialize a new Schematic project"""
    try:
        workspace_path = Path(workspace).resolve()

        # Check if provider exists
        if not ProviderRegistry.has(provider):
            available = ", ".join(ProviderRegistry.get_all_ids())
            console.print(
                f"[red]✗[/red] Provider '{provider}' not found. Available providers: {available}"
            )
            sys.exit(1)

        provider_obj = ProviderRegistry.get(provider)

        # Initialize project
        ensure_project_file(workspace_path, provider_id=provider)

        console.print(f"[green]✓[/green] Initialized Schematic project in {workspace_path}")
        console.print(f"[blue]Provider:[/blue] {provider_obj.info.name}")
        console.print(f"[blue]Version:[/blue] {provider_obj.info.version}")
        console.print("\nNext steps:")
        console.print("  1. Run 'schematic sql' to generate SQL")
        console.print("  2. Use Schematic VSCode extension to design schemas")
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
    "--target",
    "-t",
    help="Target environment (maps logical catalog names to physical catalog names)",
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def sql(
    output: Optional[str],
    from_version: Optional[str],
    to_version: Optional[str],
    target: Optional[str],
    workspace: str,
) -> None:
    """Generate SQL migration script from schema changes"""
    try:
        workspace_path = Path(workspace).resolve()
        output_path = Path(output).resolve() if output else None

        generate_sql_migration(
            workspace=workspace_path,
            output=output_path,
            from_version=from_version,
            to_version=to_version,
            target_env=target,
        )

    except SQLGenerationError as e:
        console.print(f"[red]✗ SQL generation failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def validate(workspace: str) -> None:
    """Validate .schematic/ project files"""
    try:
        workspace_path = Path(workspace).resolve()
        validate_project(workspace_path)

    except CommandValidationError as e:
        console.print(f"[red]✗ Validation failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.argument("version1")
@click.argument("version2")
def diff(version1: str, version2: str) -> None:
    """Show differences between two schema versions"""

    console.print(f"Comparing {version1} to {version2}...")
    console.print("[yellow]Diff functionality not yet implemented[/yellow]")
    # TODO: Implement version comparison


@cli.command(name="record-deployment")
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
def record_deployment(environment: str, version: Optional[str], mark_deployed: bool) -> None:
    """Record a deployment to an environment (manual tracking)

    This command manually tracks a deployment record in project.json.
    For automated deployment with execution, use 'schematic apply' instead.
    """
    try:
        workspace_path = Path.cwd()

        record_deployment_to_environment(
            workspace=workspace_path,
            environment=environment,
            version=version,
            mark_deployed=mark_deployed,
        )

    except DeploymentRecordingError as e:
        console.print(f"[red]✗ Deployment recording failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


@cli.command(name="deploy", hidden=True)
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
def deploy_alias(environment: str, version: Optional[str], mark_deployed: bool) -> None:
    """[DEPRECATED] Use 'record-deployment' instead

    This command is deprecated. Use 'schematic record-deployment' for manual
    deployment tracking, or 'schematic apply' for automated deployment with execution.
    """
    console.print(
        "[yellow]⚠️  'deploy' is deprecated. Use 'record-deployment' for manual tracking "
        "or 'apply' for automated deployment.[/yellow]\n"
    )
    record_deployment(environment, version, mark_deployed)


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
    default=".schematic/dab",
    help="Output directory (default: .schematic/dab)",
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
        console.print(f"[red]✗ Error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.option("--target", "-t", required=True, help="Target environment (dev/test/prod)")
@click.option("--profile", "-p", required=True, help="Databricks profile name")
@click.option("--warehouse-id", "-w", required=True, help="SQL warehouse ID")
@click.option("--sql", type=click.Path(exists=True), help="SQL file to execute (optional)")
@click.option("--dry-run", is_flag=True, help="Preview changes without executing")
@click.option("--no-interaction", is_flag=True, help="Skip confirmation prompt (for CI/CD)")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def apply(
    target: str,
    profile: str,
    warehouse_id: str,
    sql: Optional[str],
    dry_run: bool,
    no_interaction: bool,
    workspace: str,
) -> None:
    """Execute SQL against target environment

    Applies schema changes to the target environment by executing SQL statements.
    Shows a Terraform-like preview before execution and tracks deployment in the
    target catalog's schematic schema.

    Examples:

        # Preview changes (dry-run)
        schematic apply --target dev --profile DEV --warehouse-id abc123 --dry-run

        # Apply to dev environment
        schematic apply --target dev --profile DEV --warehouse-id abc123

        # Apply specific SQL file
        schematic apply --target prod --profile PROD --warehouse-id xyz789 --sql migration.sql

        # CI/CD mode (non-interactive)
        schematic apply --target dev --profile DEV --warehouse-id $WAREHOUSE_ID --no-interaction
    """
    try:
        workspace_path = Path(workspace).resolve()
        sql_file = Path(sql).resolve() if sql else None

        result = apply_to_environment(
            workspace=workspace_path,
            target_env=target,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            sql_file=sql_file,
        )

        # Exit with appropriate code
        if result.status == "success":
            sys.exit(0)
        else:
            sys.exit(1)

    except ApplyError as e:
        console.print(f"[red]✗ Apply failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
