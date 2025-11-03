"""
Click-based CLI for Schematic.
"""

import sys
from pathlib import Path

import click
from rich.console import Console

# Import providers to initialize them
import schematic.providers  # noqa: F401

from .commands import (
    ApplyError,
    DeploymentRecordingError,
    DiffError,
    SQLGenerationError,
    apply_to_environment,
    generate_diff,
    generate_sql_migration,
    record_deployment_to_environment,
    rollback_complete,
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

        if not provider_obj:
            console.print(f"[red]✗[/red] Provider '{provider}' not found")
            sys.exit(1)

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
    "--snapshot",
    "-s",
    help="Generate SQL from a specific snapshot (version or 'latest')",
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
    output: str | None,
    snapshot: str | None,
    from_version: str | None,
    to_version: str | None,
    target: str | None,
    workspace: str,
) -> None:
    """Generate SQL migration script from schema changes

    Examples:
        schematic sql                    # Generate from changelog
        schematic sql --snapshot latest  # Generate from latest snapshot
        schematic sql --snapshot v0.1.0  # Generate from specific snapshot
        schematic sql --target prod      # Environment-specific catalog mapping
    """
    try:
        workspace_path = Path(workspace).resolve()
        output_path = Path(output).resolve() if output else None

        generate_sql_migration(
            workspace=workspace_path,
            output=output_path,
            snapshot=snapshot,
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
@click.option(
    "--from",
    "from_version",
    required=True,
    help="Source snapshot version (e.g., v0.1.0)",
)
@click.option(
    "--to",
    "to_version",
    required=True,
    help="Target snapshot version (e.g., v0.10.0)",
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
@click.option(
    "--target",
    "-t",
    help="Target environment (for catalog name mapping)",
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def diff(
    from_version: str,
    to_version: str,
    show_sql: bool,
    show_details: bool,
    target: str | None,
    workspace: str,
) -> None:
    """Generate diff operations between two snapshot versions

    Examples:

        # Basic diff
        schematic diff --from v0.1.0 --to v0.10.0

        # Show SQL with logical catalog names
        schematic diff --from v0.1.0 --to v0.10.0 --show-sql

        # Show SQL with environment-specific catalog names
        schematic diff --from v0.1.0 --to v0.10.0 --show-sql --target dev

        # Show detailed operation payloads
        schematic diff --from v0.1.0 --to v0.10.0 --show-details
    """
    try:
        workspace_path = Path(workspace).resolve()

        generate_diff(
            workspace=workspace_path,
            from_version=from_version,
            to_version=to_version,
            show_sql=show_sql,
            show_details=show_details,
            target_env=target,
        )

    except DiffError as e:
        console.print(f"[red]✗ Diff generation failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


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
def record_deployment(environment: str, version: str | None, mark_deployed: bool) -> None:
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
def deploy_alias(environment: str, version: str | None, mark_deployed: bool) -> None:
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
@click.option("--dry-run", is_flag=True, help="Preview changes without executing")
@click.option("--no-interaction", is_flag=True, help="Skip confirmation prompt (for CI/CD)")
@click.option(
    "--auto-rollback", is_flag=True, help="Automatically rollback on failure (MVP feature!)"
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def apply(
    target: str,
    profile: str,
    warehouse_id: str,
    dry_run: bool,
    no_interaction: bool,
    auto_rollback: bool,
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

        # Apply with automatic rollback on failure (MVP feature!)
        schematic apply --target dev --profile DEV --warehouse-id abc123 --auto-rollback

        # CI/CD mode (non-interactive with auto-rollback)
        schematic apply --target prod --profile PROD --warehouse-id $WAREHOUSE_ID \\
            --no-interaction --auto-rollback
    """
    try:
        workspace_path = Path(workspace).resolve()

        result = apply_to_environment(
            workspace=workspace_path,
            target_env=target,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            auto_rollback=auto_rollback,
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


@cli.command()
@click.option("--deployment", "-d", help="Deployment ID to rollback (for partial rollback)")
@click.option("--partial", is_flag=True, help="Partial rollback of failed deployment")
@click.option("--environment", "-e", help="Environment name (for complete rollback)")
@click.option("--to-snapshot", help="Target snapshot version (for complete rollback)")
@click.option("--profile", "-p", help="Databricks CLI profile")
@click.option("--warehouse-id", "-w", help="SQL Warehouse ID")
@click.option("--create-clone", help="Create backup SHALLOW CLONE before rollback")
@click.option("--safe-only", is_flag=True, help="Only execute safe operations (skip destructive)")
@click.option("--dry-run", is_flag=True, help="Preview impact without executing")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def rollback(
    deployment: str | None,
    partial: bool,
    environment: str | None,
    to_snapshot: str | None,
    profile: str | None,
    warehouse_id: str | None,
    create_clone: str | None,
    safe_only: bool,
    dry_run: bool,
    workspace: str,
) -> None:
    """Rollback deployments (partial or complete)

    Two rollback modes:

    1. Partial rollback: Revert a failed deployment by reversing successful operations
       Usage: schematic rollback --deployment deploy_abc123 --partial

    2. Complete rollback: Revert to a previous snapshot version
       Usage: schematic rollback --environment prod --to-snapshot v0.5.0

    Examples:

        # Partial rollback of failed deployment
        schematic rollback --deployment deploy_abc123 --partial \\
            --profile PROD --warehouse-id abc123

        # Complete rollback to previous version
        schematic rollback --environment prod --to-snapshot v0.5.0 \\
            --profile PROD --warehouse-id abc123

        # Complete rollback with backup clone
        schematic rollback --environment prod --to-snapshot v0.5.0 \\
            --create-clone prod_backup --profile PROD --warehouse-id abc123

        # Preview rollback impact (dry-run)
        schematic rollback --environment prod --to-snapshot v0.5.0 --dry-run
    """
    try:
        workspace_path = Path(workspace).resolve()

        if partial:
            # Partial rollback mode
            if not deployment:
                console.print("[red]✗[/red] --deployment required for partial rollback")
                sys.exit(1)
            if not profile or not warehouse_id:
                console.print(
                    "[red]✗[/red] --profile and --warehouse-id required for partial rollback"
                )
                sys.exit(1)

            console.print("[yellow]Partial rollback not yet fully implemented[/yellow]")
            console.print("Use --auto-rollback flag with apply command instead")
            sys.exit(1)

        elif to_snapshot:
            # Complete rollback mode
            if not environment:
                console.print("[red]✗[/red] --environment required for complete rollback")
                sys.exit(1)
            if not profile or not warehouse_id:
                console.print("[red]✗[/red] --profile and --warehouse-id required")
                sys.exit(1)

            result = rollback_complete(
                workspace=workspace_path,
                target_env=environment,
                to_snapshot=to_snapshot,
                profile=profile,
                warehouse_id=warehouse_id,
                create_clone=create_clone,
                safe_only=safe_only,
                dry_run=dry_run,
            )

            if result.success:
                console.print(
                    f"[green]✓[/green] Rolled back {result.operations_rolled_back} operations"
                )
                sys.exit(0)
            else:
                console.print(f"[red]✗[/red] Rollback failed: {result.error_message}")
                sys.exit(1)
        else:
            console.print("[red]✗[/red] Must specify either --partial or --to-snapshot")
            console.print("\nExamples:")
            console.print("  Partial:  schematic rollback --deployment deploy_abc123 --partial")
            console.print("  Complete: schematic rollback --environment prod --to-snapshot v0.5.0")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]✗ Rollback error:[/red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
