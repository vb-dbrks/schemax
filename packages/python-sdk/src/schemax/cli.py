"""
Click-based CLI for SchemaX.
"""

import json
import sys
import traceback
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from ._provider_registration import ensure_providers_loaded
from .commands import (
    ApplyError,
    DiffError,
    ImportCommandError,
    SQLGenerationError,
    apply_to_environment,
    generate_diff,
    generate_sql_migration,
    import_from_provider,
    import_from_sql_file,
    rollback_complete,
    validate_project,
)
from .commands import (
    ValidationError as CommandValidationError,
)
from .commands.rollback import RollbackError, run_partial_rollback_cli
from .commands.snapshot_rebase import (
    RebaseError,
    detect_stale_snapshots,
    rebase_snapshot,
)
from .core.storage import (
    create_snapshot,
    ensure_project_file,
    get_environment_config,
    read_changelog,
    read_project,
)
from .providers import ProviderRegistry

console = Console()


@click.group()
@click.version_option(version="0.2.0", prog_name="schemax")
def cli() -> None:
    """SchemaX CLI for catalog schema management (Multi-Provider)"""
    ensure_providers_loaded()


def _run_init(provider: str, workspace_path: Path) -> None:
    """Initialize a new SchemaX project: validate provider, ensure project file, print next steps."""
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
    ensure_project_file(workspace_path, provider_id=provider)
    console.print(f"[green]✓[/green] Initialized SchemaX project in {workspace_path}")
    console.print(f"[blue]Provider:[/blue] {provider_obj.info.name}")
    console.print(f"[blue]Version:[/blue] {provider_obj.info.version}")
    console.print("\nNext steps:")
    console.print("  1. Run 'schemax sql' to generate SQL")
    console.print("  2. Use SchemaX VSCode extension to design schemas")
    console.print("  3. Check provider docs: " + (provider_obj.info.docs_url or "N/A"))


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
        _run_init(provider, workspace_path)
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
    "snapshot_version",
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
    snapshot_version: str | None,
    from_version: str | None,
    to_version: str | None,
    target: str | None,
    workspace: str,
) -> None:
    """Generate SQL migration script from schema changes

    Examples:
        schemax sql                    # Generate from changelog
        schemax sql --snapshot latest  # Generate from latest snapshot
        schemax sql --snapshot v0.1.0  # Generate from specific snapshot
        schemax sql --target prod      # Environment-specific catalog mapping
    """
    try:
        workspace_path = Path(workspace).resolve()
        output_path = Path(output).resolve() if output else None

        generate_sql_migration(
            workspace=workspace_path,
            output=output_path,
            snapshot=snapshot_version,
            _from_version=from_version,
            _to_version=to_version,
            target_env=target,
        )

    except SQLGenerationError as e:
        console.print(f"[red]✗ SQL generation failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.option("--json", "json_output", is_flag=True, help="Output validation results as JSON")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def validate(workspace: str, json_output: bool) -> None:
    """Validate .schemax/ project files"""
    try:
        workspace_path = Path(workspace).resolve()
        validate_project(workspace_path, json_output=json_output)

    except CommandValidationError as e:
        if not json_output:
            console.print(f"[red]✗ Validation failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        if not json_output:
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
        schemax diff --from v0.1.0 --to v0.10.0

        # Show SQL with logical catalog names
        schemax diff --from v0.1.0 --to v0.10.0 --show-sql

        # Show SQL with environment-specific catalog names
        schemax diff --from v0.1.0 --to v0.10.0 --show-sql --target dev

        # Show detailed operation payloads
        schemax diff --from v0.1.0 --to v0.10.0 --show-details
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


@cli.command()
@click.option(
    "--target",
    "-t",
    required=True,
    help="Target environment (dev/test/prod)",
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
def bundle(target: str, version: str, output: str) -> None:
    """Generate Databricks Asset Bundle for deployment"""

    try:
        _workspace = Path.cwd()  # noqa: F841 - Reserved for future DAB implementation
        _output_dir = Path(output)  # noqa: F841 - Reserved for future DAB implementation

        console.print(f"Generating DAB for [cyan]{target}[/cyan] v{version}...")

        # TODO: Implement DAB generation
        console.print("[yellow]DAB generation not yet implemented[/yellow]")

    except Exception as e:
        console.print(f"[red]✗ Error:[/red] {e}")
        sys.exit(1)


@cli.command(name="import")
@click.option(
    "--from-sql",
    "from_sql_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Import from a SQL DDL file instead of live Databricks (optional --target for catalog mapping)",
)
@click.option(
    "--mode",
    type=click.Choice(["diff", "replace"]),
    default="diff",
    help="For --from-sql: diff = append ops to changelog; replace = treat SQL state as new baseline",
)
@click.option(
    "--target", "-t", help="Target environment (required for live import; optional for --from-sql)"
)
@click.option("--profile", "-p", help="Databricks profile name (required for live import)")
@click.option("--warehouse-id", "-w", help="SQL warehouse ID (required for live import)")
@click.option("--catalog", help="Catalog name to import (live import only)")
@click.option("--schema", help="Schema name to import (requires --catalog)")
@click.option("--table", help="Table name to import (requires --catalog and --schema)")
@click.option(
    "--catalog-map",
    "catalog_map",
    multiple=True,
    help="Catalog mapping override in logical=physical format (repeatable)",
)
@click.option("--dry-run", is_flag=True, help="Preview import operations without writing changelog")
@click.option(
    "--adopt-baseline",
    is_flag=True,
    help="Mark imported snapshot as deployment baseline (planned, scaffold only)",
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
@click.pass_context
def import_command(ctx: click.Context, **_kwargs: Any) -> None:
    """Import existing provider assets into SchemaX changelog.

    Two sources:

    \b
    • Live Databricks: use --target, --profile, --warehouse-id (and optional scope).
    • SQL DDL file: use --from-sql path [--mode diff|replace] [--dry-run] [--target ENV].
    """
    try:
        workspace_path = Path(ctx.params["workspace"]).resolve()
        _run_import_command(workspace_path, ctx.params)
    except (ImportCommandError, ImportError) as e:
        console.print(f"[red]✗ Import failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


def _run_import_command(workspace_path: Path, params: dict[str, Any]) -> None:
    """Execute import from SQL file or live provider based on params."""
    from_sql_path = params.get("from_sql_path")
    if from_sql_path is not None:
        summary = import_from_sql_file(
            workspace=workspace_path,
            sql_path=from_sql_path,
            mode=params.get("mode", "diff"),
            dry_run=params.get("dry_run", False),
            target_env=params.get("target"),
        )
        _print_import_summary(summary)
        return
    target = params.get("target")
    profile = params.get("profile")
    warehouse_id = params.get("warehouse_id")
    if not target or not profile or not warehouse_id:
        console.print(
            "[red]✗[/red] Live import requires --target, --profile, and --warehouse-id. "
            "Use --from-sql for SQL file import."
        )
        sys.exit(1)
    schema = params.get("schema")
    catalog = params.get("catalog")
    table = params.get("table")
    if schema and not catalog:
        console.print("[red]✗[/red] --schema requires --catalog")
        sys.exit(1)
    if table and (not catalog or not schema):
        console.print("[red]✗[/red] --table requires --catalog and --schema")
        sys.exit(1)
    binding_overrides = _parse_catalog_mappings(params.get("catalog_map", ()))
    summary = import_from_provider(
        workspace=workspace_path,
        target_env=target,
        profile=profile,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        table=table,
        dry_run=params.get("dry_run", False),
        adopt_baseline=params.get("adopt_baseline", False),
        catalog_mappings_override=binding_overrides,
    )
    _print_import_summary(summary)


def _parse_catalog_mappings(catalog_map: tuple[str, ...]) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for binding in catalog_map:
        if "=" not in binding:
            raise ImportError(f"Invalid --catalog-map '{binding}'. Expected logical=physical")
        logical, physical = binding.split("=", 1)
        logical = logical.strip()
        physical = physical.strip()
        if not logical or not physical:
            raise ImportError(f"Invalid --catalog-map '{binding}'. Expected logical=physical")
        if logical in bindings and bindings[logical] != physical:
            raise ImportError(
                f"Conflicting --catalog-map for '{logical}': '{bindings[logical]}' vs '{physical}'"
            )
        bindings[logical] = physical
    return bindings


def _print_import_summary(summary: dict[str, Any]) -> None:
    """Render a concise completion summary for the import command."""
    operations = int(summary.get("operations_generated", 0))
    dry_run = bool(summary.get("dry_run", False))
    mode = "previewed" if dry_run else "prepared"
    console.print(f"[green]✓[/green] Import summary: {operations} operation(s) {mode}.")

    catalog_mappings = summary.get("catalog_mappings", {})
    if isinstance(catalog_mappings, dict) and catalog_mappings:
        console.print(f"[blue]Catalog mappings:[/blue] {len(catalog_mappings)}")

    warnings = summary.get("warnings", [])
    if isinstance(warnings, list) and warnings:
        console.print(f"[yellow]Warnings:[/yellow] {len(warnings)}")

    snapshot_version = summary.get("snapshot_version")
    if snapshot_version:
        console.print(f"[blue]Baseline snapshot:[/blue] {snapshot_version}")

    deployment_id = summary.get("deployment_id")
    if deployment_id:
        console.print(f"[blue]Baseline deployment:[/blue] {deployment_id}")


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
    target catalog's tracking schema (.schemax).

    Examples:

        # Preview changes (dry-run)
        schemax apply --target dev --profile DEV --warehouse-id abc123 --dry-run

        # Apply to dev environment
        schemax apply --target dev --profile DEV --warehouse-id abc123

        # Apply with automatic rollback on failure (MVP feature!)
        schemax apply --target dev --profile DEV --warehouse-id abc123 --auto-rollback

        # CI/CD mode (non-interactive with auto-rollback)
        schemax apply --target prod --profile PROD --warehouse-id $WAREHOUSE_ID \\
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


def _print_rollback_usage_and_exit() -> None:
    """Print rollback usage and exit with code 1."""
    console.print("[red]✗[/red] Must specify either --partial or --to-snapshot")
    console.print("\nExamples:")
    console.print("  Partial:  schemax rollback --deployment deploy_abc123 --partial")
    console.print("  Complete: schemax rollback --target prod --to-snapshot v0.5.0")
    sys.exit(1)


def _handle_rollback_dispatch(workspace_path: Path, params: dict[str, Any]) -> None:
    """Run partial or complete rollback from params; prints and exits."""
    if params["partial"]:
        result = run_partial_rollback_cli(
            workspace_path=workspace_path,
            deployment_id=params["deployment"] or "",
            target=params["target"] or "",
            profile=params["profile"] or "",
            warehouse_id=params["warehouse_id"] or "",
            no_interaction=params["no_interaction"],
            dry_run=params["dry_run"],
        )
        if result.success:
            console.print(
                f"[green]✓[/green] Rolled back {result.operations_rolled_back} operations"
            )
            sys.exit(0)
        console.print(f"[red]✗[/red] Rollback failed: {result.error_message}")
        sys.exit(1)
    if params["to_snapshot"]:
        if not params["target"]:
            console.print("[red]✗[/red] --target required for complete rollback")
            sys.exit(1)
        if not params["profile"] or not params["warehouse_id"]:
            console.print("[red]✗[/red] --profile and --warehouse-id required")
            sys.exit(1)
        result = rollback_complete(
            workspace=workspace_path,
            target_env=params["target"],
            to_snapshot=params["to_snapshot"],
            profile=params["profile"],
            warehouse_id=params["warehouse_id"],
            _create_clone=params["create_clone"],
            safe_only=params["safe_only"],
            dry_run=params["dry_run"],
            no_interaction=params["no_interaction"],
            force=params["force"],
        )
        if result.success:
            console.print(
                f"[green]✓[/green] Rolled back {result.operations_rolled_back} operations"
            )
            sys.exit(0)
        console.print(f"[red]✗[/red] Rollback failed: {result.error_message}")
        sys.exit(1)
    _print_rollback_usage_and_exit()


def _print_rollback_deployment_not_found_help(
    deployment_id: str, deployment_catalog: str, target: str
) -> None:
    """Print troubleshooting steps when deployment is not found."""
    console.print("\n[yellow]Troubleshooting steps:[/yellow]")
    console.print(
        f"  1. Verify catalog exists:\n"
        f"     [dim]SELECT * FROM {deployment_catalog}.information_schema.schemata[/dim]\n"
    )
    console.print(
        f"  2. Check if deployment was recorded:\n"
        f"     [dim]SELECT * FROM {deployment_catalog}.schemax.deployments WHERE id = '{deployment_id}'[/dim]\n"
    )
    console.print(
        f"  3. List recent deployments:\n"
        f"     [dim]SELECT id, environment, snapshot_version, status, deployed_at\n"
        f"     FROM {deployment_catalog}.schemax.deployments\n"
        f"     WHERE environment = '{target}'\n"
        f"     ORDER BY deployed_at DESC LIMIT 5[/dim]"
    )


@cli.command()
@click.option("--deployment", "-d", help="Deployment ID to rollback (for partial rollback)")
@click.option("--partial", is_flag=True, help="Partial rollback of failed deployment")
@click.option("--target", "-t", help="Target environment (dev/test/prod) for complete rollback")
@click.option("--to-snapshot", help="Target snapshot version (for complete rollback)")
@click.option("--profile", "-p", help="Databricks CLI profile")
@click.option("--warehouse-id", "-w", help="SQL Warehouse ID")
@click.option("--create-clone", help="Create backup SHALLOW CLONE before rollback")
@click.option("--safe-only", is_flag=True, help="Only execute safe operations (skip destructive)")
@click.option("--dry-run", is_flag=True, help="Preview impact without executing")
@click.option("--no-interaction", is_flag=True, help="Skip confirmation prompts (for CI/CD)")
@click.option(
    "--force",
    is_flag=True,
    help="Override baseline guard for complete rollback (use with caution)",
)
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
@click.pass_context
def rollback(ctx: click.Context, **_kwargs: Any) -> None:
    """Rollback deployments (partial or complete)

    Two rollback modes:

    1. Partial rollback: Revert a failed deployment by reversing successful operations
       Usage: schemax rollback --deployment deploy_abc123 --partial

    2. Complete rollback: Revert to a previous snapshot version
       Usage: schemax rollback --target prod --to-snapshot v0.5.0

    Examples:

        # Partial rollback of failed deployment
        schemax rollback --deployment deploy_abc123 --partial \\
            --profile PROD --warehouse-id abc123

        # Complete rollback to previous version
        schemax rollback --target prod --to-snapshot v0.5.0 \\
            --profile PROD --warehouse-id abc123

        # Complete rollback with backup clone
        schemax rollback --target prod --to-snapshot v0.5.0 \\
            --create-clone prod_backup --profile PROD --warehouse-id abc123

        # Preview rollback impact (dry-run)
        schemax rollback --target prod --to-snapshot v0.5.0 --dry-run

        # Non-interactive (skip confirmation prompts, for CI/CD)
        schemax rollback --partial --deployment deploy_abc123 -p PROD -w abc123 -t prod --no-interaction
    """
    params = None
    workspace_path = None
    try:
        params = ctx.params
        workspace_path = Path(params["workspace"]).resolve()
        _handle_rollback_dispatch(workspace_path, params)
    except RollbackError as e:
        console.print(f"[red]✗[/red] {e}")
        if (
            "not found" in str(e).lower()
            and params is not None
            and params.get("target")
            and workspace_path is not None
        ):
            try:
                project = read_project(workspace_path)
                env_config = get_environment_config(project, params["target"])
                _print_rollback_deployment_not_found_help(
                    params.get("deployment") or "",
                    env_config.get("topLevelName", ""),
                    params["target"],
                )
            except (FileNotFoundError, ValueError):
                pass
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Rollback error:[/red] {e}")
        sys.exit(1)


@cli.group()
def snapshot() -> None:
    """Snapshot management commands"""


def _run_snapshot_create(
    workspace_path: Path,
    name: str,
    version: str | None,
    comment: str | None,
    tags: tuple[str, ...] | None,
) -> None:
    """Create snapshot from changelog and print success message."""
    changelog = read_changelog(workspace_path)
    if not changelog["ops"]:
        console.print("[yellow]⚠️  No uncommitted operations in changelog[/yellow]")
        console.print("Create operations in the SchemaX Designer before creating a snapshot.")
        return
    console.print(f"📸 Creating snapshot: [bold]{name}[/bold]")
    console.print(f"   Operations to snapshot: {len(changelog['ops'])}")
    project, snap_meta = create_snapshot(
        workspace_path,
        name=name,
        version=version,
        comment=comment,
        tags=list(tags) if tags else None,
    )
    console.print()
    console.print("[green]✓ Snapshot created successfully![/green]")
    console.print(f"   Version: [bold]{snap_meta['version']}[/bold]")
    console.print(f"   Name: {snap_meta['name']}")
    if snap_meta.get("comment"):
        console.print(f"   Comment: {snap_meta['comment']}")
    if snap_meta.get("tags"):
        console.print(f"   Tags: {', '.join(snap_meta['tags'])}")
    console.print(f"   Operations: {len(snap_meta['operations'])}")
    console.print(f"   File: [dim].schemax/snapshots/{snap_meta['version']}.json[/dim]")
    console.print()
    console.print(
        f"[green]✓ Changelog cleared ({len(changelog['ops'])} ops moved to snapshot)[/green]"
    )
    console.print(f"[green]✓ Total snapshots: {len(project['snapshots'])}[/green]")


@snapshot.command(name="create")
@click.option("--name", "-n", required=True, help="Snapshot name")
@click.option(
    "--version", "-v", help="Snapshot version (e.g., v0.2.0, auto-generated if not provided)"
)
@click.option("--comment", "-c", help="Optional comment describing the snapshot")
@click.option("--tags", "-t", multiple=True, help="Optional tags (can be specified multiple times)")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def snapshot_create_cmd(
    name: str, version: str | None, comment: str | None, tags: tuple[str, ...], workspace: str
) -> None:
    """Create a new snapshot from current changelog

    Creates a snapshot of the current schema state, capturing all uncommitted
    operations from the changelog. If version is not specified, it will be
    auto-generated based on the latest snapshot and version bump strategy.

    Examples:
        schemax snapshot create --name "Initial schema"
        schemax snapshot create --name "Add users table" --version v0.2.0
        schemax snapshot create --name "Production release" --comment "First prod deployment" --tags prod
    """
    try:
        workspace_path = Path(workspace).resolve()
        _run_snapshot_create(workspace_path, name, version, comment, tags)
    except FileNotFoundError as e:
        console.print(f"[red]✗ Error: {e}[/red]")
        console.print("Make sure you're in a SchemaX project directory.")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Snapshot creation failed: {e}[/red]")
        console.print(f"[dim]{traceback.format_exc()}[/dim]")
        sys.exit(1)


def _run_snapshot_rebase(workspace_path: Path, snapshot_version: str, base: str | None) -> None:
    """Run snapshot rebase and exit with appropriate code."""
    result = rebase_snapshot(
        workspace=workspace_path,
        snapshot_version=snapshot_version,
        new_base_version=base,
    )
    if result.success:
        console.print()
        console.print(f"[green]✓ Successfully rebased {snapshot_version}[/green]")
        sys.exit(0)
    console.print()
    console.print("[red]✗ Rebase stopped due to conflicts[/red]")
    console.print(f"[yellow]Resolved {result.applied_count} operations[/yellow]")
    console.print(f"[yellow]{result.conflict_count} operations need manual resolution[/yellow]")
    sys.exit(1)


@snapshot.command(name="rebase")
@click.argument("snapshot_version", required=True)
@click.option("--base", "-b", help="New base version (auto-detects latest if not provided)")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def snapshot_rebase_cmd(snapshot_version: str, base: str | None, workspace: str) -> None:
    """Rebase snapshot onto new base version after git rebase

    After rebasing your git branch, use this command to rebase your snapshot
    onto the new base version. This unpacks the snapshot, replays operations
    on the new base, and detects conflicts.

    Examples:

        # Rebase v0.4.0 onto latest snapshot
        schemax snapshot rebase v0.4.0

        # Rebase v0.4.0 onto specific version
        schemax snapshot rebase v0.4.0 --base v0.3.1
    """
    try:
        workspace_path = Path(workspace).resolve()
        _run_snapshot_rebase(workspace_path, snapshot_version, base)
    except RebaseError as e:
        console.print(f"[red]✗ Rebase failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


def _run_snapshot_validate(workspace_path: Path, json_output: bool) -> None:
    """Run snapshot validate and exit with appropriate code."""
    stale = detect_stale_snapshots(workspace_path, _json_output=json_output)
    if json_output:
        output = {"stale": stale, "count": len(stale)}
        print(json.dumps(output))
        sys.exit(1 if stale else 0)
    if not stale:
        console.print("[green]✓ All snapshots are up to date[/green]")
        sys.exit(0)
    console.print(f"[yellow]⚠️  Found {len(stale)} stale snapshot(s):[/yellow]")
    console.print()
    for snap_item in stale:
        console.print(f"  [yellow]{snap_item['version']}[/yellow]")
        console.print(f"    Current base: {snap_item['currentBase']}")
        console.print(f"    Should be: {snap_item['shouldBeBase']}")
        console.print(f"    Missing: {', '.join(snap_item['missing'])}")
        console.print()
    console.print("[cyan]Run the following commands to fix:[/cyan]")
    for snap_item in stale:
        console.print(f"  schemax snapshot rebase {snap_item['version']}")
    sys.exit(1)


@snapshot.command(name="validate")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def snapshot_validate_cmd(workspace: str, json_output: bool) -> None:
    """Validate snapshot chain and detect stale snapshots

    Checks for snapshots that need rebasing after git rebase/merge.

    Example:

        schemax snapshot validate
        schemax snapshot validate --json
    """
    try:
        workspace_path = Path(workspace).resolve()
        _run_snapshot_validate(workspace_path, json_output)
    except Exception as e:
        if json_output:
            print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]✗ Validation failed:[/red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
