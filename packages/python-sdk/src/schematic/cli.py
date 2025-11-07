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
    DiffError,
    SQLGenerationError,
    apply_to_environment,
    generate_diff,
    generate_sql_migration,
    rollback_complete,
    validate_project,
)
from .commands import (
    ValidationError as CommandValidationError,
)
from .core.storage import ensure_project_file
from .providers import ProviderRegistry

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
    default=".schematic/dab",
    help="Output directory (default: .schematic/dab)",
)
def bundle(target: str, version: str, output: str) -> None:
    """Generate Databricks Asset Bundle for deployment"""

    try:
        _workspace = Path.cwd()  # noqa: F841 - Reserved for future DAB implementation
        _output_dir = Path(output)  # noqa: F841 - Reserved for future DAB implementation

        console.print(f"Generating DAB for [cyan]{target}[/cyan] v{version}...")

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
@click.option("--target", "-t", help="Target environment (dev/test/prod) for complete rollback")
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
    target: str | None,
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
       Usage: schematic rollback --target prod --to-snapshot v0.5.0

    Examples:

        # Partial rollback of failed deployment
        schematic rollback --deployment deploy_abc123 --partial \\
            --profile PROD --warehouse-id abc123

        # Complete rollback to previous version
        schematic rollback --target prod --to-snapshot v0.5.0 \\
            --profile PROD --warehouse-id abc123

        # Complete rollback with backup clone
        schematic rollback --target prod --to-snapshot v0.5.0 \\
            --create-clone prod_backup --profile PROD --warehouse-id abc123

        # Preview rollback impact (dry-run)
        schematic rollback --target prod --to-snapshot v0.5.0 --dry-run
    """
    try:
        workspace_path = Path(workspace).resolve()

        if partial:
            # Partial rollback mode - manual rollback of a recorded deployment
            if not deployment:
                console.print("[red]✗[/red] --deployment required for partial rollback")
                sys.exit(1)
            if not profile or not warehouse_id:
                console.print(
                    "[red]✗[/red] --profile and --warehouse-id required for partial rollback"
                )
                sys.exit(1)
            if not target:
                console.print("[red]✗[/red] --target required for partial rollback")
                sys.exit(1)

            # Load deployment record from database (source of truth)
            from schematic.core.deployment import DeploymentTracker
            from schematic.core.storage import get_environment_config, read_project
            from schematic.providers.unity.auth import create_databricks_client

            project = read_project(workspace_path)
            env_config = get_environment_config(project, target)
            deployment_catalog = env_config["topLevelName"]

            # Query database for deployment
            client = create_databricks_client(profile)
            tracker = DeploymentTracker(client, deployment_catalog, warehouse_id)
            target_deployment = tracker.get_deployment_by_id(deployment)

            if not target_deployment:
                console.print(
                    f"[red]✗[/red] Deployment '{deployment}' not found in {deployment_catalog}.schematic"
                )
                console.print(
                    f"\n[yellow]Troubleshooting steps:[/yellow]"
                )
                console.print(
                    f"  1. Verify catalog exists:\n"
                    f"     [dim]SELECT * FROM {deployment_catalog}.information_schema.schemata[/dim]\n"
                )
                console.print(
                    f"  2. Check if deployment was recorded:\n"
                    f"     [dim]SELECT * FROM {deployment_catalog}.schematic.deployments WHERE id = '{deployment}'[/dim]\n"
                )
                console.print(
                    f"  3. List recent deployments:\n"
                    f"     [dim]SELECT id, environment, snapshot_version, status, deployed_at\n"
                    f"     FROM {deployment_catalog}.schematic.deployments\n"
                    f"     WHERE environment = '{target}'\n"
                    f"     ORDER BY deployed_at DESC LIMIT 5[/dim]"
                )
                sys.exit(1)

            # Check if it's a failed deployment
            if target_deployment.get("status") != "failed":
                console.print(
                    f"[yellow]⚠️  Deployment '{deployment}' has status: "
                    f"{target_deployment.get('status')}[/yellow]"
                )
                console.print("Partial rollback is typically used for failed deployments.")
                from rich.prompt import Confirm

                if not Confirm.ask("Continue anyway?", default=False):
                    sys.exit(1)

            # Get operations that were applied (successful before failure)
            ops_applied = target_deployment.get("opsApplied", [])
            failed_idx = target_deployment.get("failedStatementIndex", len(ops_applied))

            # Successful ops are those before the failure point
            successful_op_ids = ops_applied[:failed_idx]

            if not successful_op_ids:
                console.print(
                    "[yellow]No operations to rollback (deployment had no successful operations)[/yellow]"
                )
                sys.exit(0)

            # Load operations - try changelog first, then regenerate from snapshots
            from schematic.providers.base.operations import Operation

            from .core.storage import read_changelog

            changelog = read_changelog(workspace_path)
            all_ops = [Operation(**op) for op in changelog.get("ops", [])]

            # Filter to get the successful operations
            successful_ops = [op for op in all_ops if op.id in successful_op_ids]

            # If operations not found in changelog, they might be diff operations from snapshot deployment
            if not successful_ops and target_deployment.get("version"):
                console.print(
                    "[cyan]Operations not in changelog - regenerating from snapshots...[/cyan]"
                )
                from .core.storage import load_current_state, read_snapshot

                from_version = target_deployment.get("fromVersion")
                to_version = target_deployment.get("version")

                # Load states
                _, _, provider = load_current_state(workspace_path)

                if from_version:
                    from_snap = read_snapshot(workspace_path, from_version)
                    from_state = from_snap["state"]
                    from_ops = from_snap.get("operations", [])
                else:
                    # First deployment - diff from empty state
                    from_state = provider.create_initial_state()
                    from_ops = []

                to_snap = read_snapshot(workspace_path, to_version)
                to_state = to_snap["state"]
                to_ops = to_snap.get("operations", [])

                # Regenerate diff operations using provider's state differ
                differ = provider.get_state_differ(from_state, to_state, from_ops, to_ops)
                all_diff_ops = differ.generate_diff_operations()

                # Match by operation type + target + payload (exact semantic match)
                # Use opsDetails from database to find matching operations
                ops_details = target_deployment.get("opsDetails", [])
                successful_ops_details = [
                    op_detail
                    for op_detail in ops_details
                    if op_detail["id"] in successful_op_ids
                ]

                # Match regenerated operations to successful operations by type+target+payload
                successful_ops = []
                for op_detail in successful_ops_details:
                    # Find matching operation in regenerated diff
                    # Match by type, target, AND payload for 100% accuracy
                    matching_op = next(
                        (
                            op
                            for op in all_diff_ops
                            if op.op == op_detail["type"]
                            and op.target == op_detail["target"]
                            and op.payload == op_detail["payload"]  # Exact payload match
                        ),
                        None,
                    )
                    if matching_op:
                        successful_ops.append(matching_op)

                if len(successful_ops) != len(successful_ops_details):
                    console.print(
                        f"[yellow]⚠️  Warning: Matched {len(successful_ops)}/{len(successful_ops_details)} operations[/yellow]"
                    )
                else:
                    console.print(
                        f"[dim]Matched {len(successful_ops)} operations by type+target+payload[/dim]"
                    )

            if not successful_ops:
                console.print("[red]✗[/red] Could not find operation details")
                console.print("This may indicate the deployment is too old or data is corrupted")
                sys.exit(1)

            console.print(f"[cyan]Found {len(successful_ops)} operations to rollback[/cyan]")

            # Get environment config and catalog mapping
            env_config = get_environment_config(project, target)

            # Build catalog mapping
            from .core.storage import load_current_state

            state, _, provider = load_current_state(workspace_path)

            # Build simple catalog mapping (single catalog mode)
            catalogs = state.get("catalogs", [])
            catalog_mapping = {}
            if catalogs:
                logical_name = catalogs[0].get("name", "__implicit__")
                catalog_mapping = {logical_name: env_config["topLevelName"]}

            # Initialize executor
            from schematic.providers.unity.auth import create_databricks_client
            from schematic.providers.unity.executor import UnitySQLExecutor

            client = create_databricks_client(profile)
            executor = UnitySQLExecutor(client)

            # Execute partial rollback
            from .commands.rollback import rollback_partial

            # Get the fromVersion from the deployment record
            from_version = target_deployment.get("fromVersion")

            result = rollback_partial(
                workspace=workspace_path,
                deployment_id=deployment,
                successful_ops=successful_ops,
                target_env=target,
                profile=profile,
                warehouse_id=warehouse_id,
                executor=executor,
                catalog_mapping=catalog_mapping,
                auto_triggered=False,  # Manual mode - allow confirmation
                from_version=from_version,
            )

            if result.success:
                console.print(
                    f"[green]✓[/green] Rolled back {result.operations_rolled_back} operations"
                )
                sys.exit(0)
            else:
                console.print(f"[red]✗[/red] Rollback failed: {result.error_message}")
                sys.exit(1)

        elif to_snapshot:
            # Complete rollback mode
            if not target:
                console.print("[red]✗[/red] --target required for complete rollback")
                sys.exit(1)
            if not profile or not warehouse_id:
                console.print("[red]✗[/red] --profile and --warehouse-id required")
                sys.exit(1)

            result = rollback_complete(
                workspace=workspace_path,
                target_env=target,
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
            console.print("  Complete: schematic rollback --target prod --to-snapshot v0.5.0")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]✗ Rollback error:[/red] {e}")
        sys.exit(1)


@cli.group()
def snapshot() -> None:
    """Snapshot management commands"""
    pass


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
        schematic snapshot create --name "Initial schema"
        schematic snapshot create --name "Add users table" --version v0.2.0
        schematic snapshot create --name "Production release" --comment "First prod deployment" --tags prod
    """
    from pathlib import Path

    from schematic.core.storage import create_snapshot, read_changelog

    workspace_path = Path(workspace).resolve()

    try:
        # Check if there are uncommitted operations
        changelog = read_changelog(workspace_path)
        if not changelog["ops"]:
            console.print("[yellow]⚠️  No uncommitted operations in changelog[/yellow]")
            console.print("Create operations in the Schematic Designer before creating a snapshot.")
            return

        console.print(f"📸 Creating snapshot: [bold]{name}[/bold]")
        console.print(f"   Operations to snapshot: {len(changelog['ops'])}")

        # Create snapshot
        project, snapshot = create_snapshot(
            workspace_path,
            name=name,
            version=version,
            comment=comment,
            tags=list(tags) if tags else None,
        )

        # Success message
        console.print()
        console.print("[green]✓ Snapshot created successfully![/green]")
        console.print(f"   Version: [bold]{snapshot['version']}[/bold]")
        console.print(f"   Name: {snapshot['name']}")
        if snapshot.get("comment"):
            console.print(f"   Comment: {snapshot['comment']}")
        if snapshot.get("tags"):
            console.print(f"   Tags: {', '.join(snapshot['tags'])}")
        console.print(f"   Operations: {len(snapshot['operations'])}")
        console.print(f"   File: [dim].schematic/snapshots/{snapshot['version']}.json[/dim]")
        console.print()
        console.print(
            f"[green]✓ Changelog cleared ({len(changelog['ops'])} ops moved to snapshot)[/green]"
        )
        console.print(f"[green]✓ Total snapshots: {len(project['snapshots'])}[/green]")

    except FileNotFoundError as e:
        console.print(f"[red]✗ Error: {e}[/red]")
        console.print("Make sure you're in a Schematic project directory.")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Snapshot creation failed: {e}[/red]")
        import traceback

        console.print(f"[dim]{traceback.format_exc()}[/dim]")
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
        schematic snapshot rebase v0.4.0

        # Rebase v0.4.0 onto specific version
        schematic snapshot rebase v0.4.0 --base v0.3.1
    """
    try:
        workspace_path = Path(workspace).resolve()

        from .commands.snapshot_rebase import RebaseError, rebase_snapshot

        result = rebase_snapshot(
            workspace=workspace_path,
            snapshot_version=snapshot_version,
            new_base_version=base,
        )

        if result.success:
            console.print()
            console.print(f"[green]✓ Successfully rebased {snapshot_version}[/green]")
            sys.exit(0)
        else:
            console.print()
            console.print("[red]✗ Rebase stopped due to conflicts[/red]")
            console.print(f"[yellow]Resolved {result.applied_count} operations[/yellow]")
            console.print(
                f"[yellow]{result.conflict_count} operations need manual resolution[/yellow]"
            )
            sys.exit(1)

    except RebaseError as e:
        console.print(f"[red]✗ Rebase failed:[/red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗ Unexpected error:[/red] {e}")
        sys.exit(1)


@snapshot.command(name="validate")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.argument("workspace", type=click.Path(exists=True), required=False, default=".")
def snapshot_validate_cmd(workspace: str, json_output: bool) -> None:
    """Validate snapshot chain and detect stale snapshots

    Checks for snapshots that need rebasing after git rebase/merge.

    Example:

        schematic snapshot validate
        schematic snapshot validate --json
    """
    try:
        workspace_path = Path(workspace).resolve()

        from .commands.snapshot_rebase import detect_stale_snapshots

        stale = detect_stale_snapshots(workspace_path, json_output=json_output)

        if json_output:
            # Output JSON for programmatic use (e.g., VS Code extension)
            import json

            output = {"stale": stale, "count": len(stale)}
            print(json.dumps(output))
            sys.exit(1 if stale else 0)

        if not stale:
            console.print("[green]✓ All snapshots are up to date[/green]")
            sys.exit(0)

        console.print(f"[yellow]⚠️  Found {len(stale)} stale snapshot(s):[/yellow]")
        console.print()

        for snap in stale:
            console.print(f"  [yellow]{snap['version']}[/yellow]")
            console.print(f"    Current base: {snap['currentBase']}")
            console.print(f"    Should be: {snap['shouldBeBase']}")
            console.print(f"    Missing: {', '.join(snap['missing'])}")
            console.print()

        console.print("[cyan]Run the following commands to fix:[/cyan]")
        for snap in stale:
            console.print(f"  schematic snapshot rebase {snap['version']}")

        sys.exit(1)

    except Exception as e:
        if json_output:
            import json

            print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]✗ Validation failed:[/red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
