"""
Apply Command Implementation

Executes SQL statements against target environment with preview,
confirmation, and deployment tracking.
"""

import re
import sys
from pathlib import Path
from typing import cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm, Prompt

from schemax.commands.sql import SQLGenerationError, build_catalog_mapping
from schemax.core.deployment import DeploymentTracker
from schemax.core.storage import (
    create_snapshot,
    get_environment_config,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
)
from schemax.providers.base.executor import ExecutionConfig, ExecutionResult
from schemax.providers.unity.executor import UnitySQLExecutor

console = Console()


class ApplyError(Exception):
    """Raised when apply command fails"""

    pass


def apply_to_environment(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    dry_run: bool = False,
    no_interaction: bool = False,
    auto_rollback: bool = False,
) -> ExecutionResult:
    """Apply changes to target environment

    Main entry point for the apply command. Auto-creates snapshot if needed,
    diffs from deployed version to latest snapshot, generates SQL, shows preview,
    confirms with user, and executes statements with deployment tracking.

    Args:
        workspace: Path to SchemaX workspace
        target_env: Target environment name (e.g., "dev", "prod")
        profile: Databricks profile name
        warehouse_id: SQL warehouse ID
        dry_run: If True, preview without executing
        no_interaction: If True, skip confirmation prompt

    Returns:
        ExecutionResult with deployment details

    Raises:
        ApplyError: If apply fails
    """
    try:
        # 1. Check for uncommitted changes and prompt user
        changelog = read_changelog(workspace)
        if changelog["ops"]:
            console.print(
                f"[yellow]⚠ {len(changelog['ops'])} uncommitted operations found[/yellow]"
            )

            # In non-interactive mode, auto-create snapshot
            if no_interaction:
                console.print("[blue]Non-interactive mode: Auto-creating snapshot[/blue]")
                choice = "create"
            else:
                console.print()
                # Prompt user for action
                choice = Prompt.ask(
                    "[bold]What would you like to do?[/bold]",
                    choices=["create", "continue", "abort"],
                    default="create",
                )

            if choice == "abort":
                console.print("[yellow]Apply cancelled[/yellow]")
                sys.exit(0)
            elif choice == "create":
                # Generate next version
                project = read_project(workspace)
                settings = project.get("settings", {})
                version_prefix = str(settings.get("versionPrefix", "v"))
                current = project.get("latestSnapshot")

                # Simple version increment
                if current:
                    match = re.search(r"(\d+)\.(\d+)\.(\d+)", current)
                    if match:
                        major, minor, patch = match.groups()
                        next_version = f"{version_prefix}{major}.{int(minor) + 1}.0"
                    else:
                        next_version = f"{version_prefix}0.1.0"
                else:
                    next_version = f"{version_prefix}0.1.0"

                console.print(f"[blue]Creating snapshot:[/blue] {next_version}")
                create_snapshot(
                    workspace,
                    name=f"Auto-snapshot for {target_env}",
                    version=next_version,
                    comment=f"Automatic snapshot created before deploying to {target_env}",
                )
                console.print("[green]✓[/green] Snapshot created")
            # else: "continue" - proceed without creating snapshot

        # 2. Load project (v4 with environment config)
        project = read_project(workspace)
        project_name = project.get("name", "unknown")

        # 3. Get environment configuration
        env_config = get_environment_config(project, target_env)

        console.print()
        console.print("[bold]SchemaX Apply[/bold]")
        console.print("─" * 60)

        # 4. Load current state and provider
        state, changelog, provider, _ = load_current_state(workspace, validate=False)

        console.print(f"[blue]Provider:[/blue] {provider.info.name} v{provider.info.version}")
        console.print(f"[blue]Environment:[/blue] {target_env}")
        console.print(f"[blue]Physical Catalog:[/blue] {env_config['topLevelName']}")
        console.print(f"[blue]Warehouse:[/blue] {warehouse_id}")
        console.print(f"[blue]Profile:[/blue] {profile}")

        # 5. Get latest snapshot version
        latest_snapshot_version = project.get("latestSnapshot")

        if not latest_snapshot_version:
            raise ApplyError("No snapshots found. Please create a snapshot first.")

        console.print(f"[blue]Latest snapshot:[/blue] {latest_snapshot_version}")

        # 6. Get last deployment from DATABASE (source of truth!)
        # Query the database to see what's actually deployed
        from schemax.providers.unity.auth import create_databricks_client

        try:
            client = create_databricks_client(profile)
            tracker = DeploymentTracker(client, env_config["topLevelName"], warehouse_id)
            db_deployment = tracker.get_latest_deployment(target_env)

            if db_deployment:
                deployed_version = db_deployment.get("version")
                console.print(f"[blue]Deployed to {target_env}:[/blue] {deployed_version}")
                console.print("[dim](Source: Database tracking table)[/dim]")
            else:
                deployed_version = None
                console.print(f"[blue]First deployment to {target_env}[/blue]")
                console.print("[dim](No successful deployments in database)[/dim]")
        except Exception as e:
            # Database connection or query error - fail fast
            console.print("\n[red]✗ Failed to query deployment database[/red]")
            console.print(f"[red]Error: {e}[/red]")
            console.print("\n[yellow]Cannot proceed without database access.[/yellow]")
            console.print("[yellow]Please check:[/yellow]")
            console.print("  • Databricks credentials are valid")
            console.print(f"  • Warehouse {warehouse_id} is running")
            console.print("  • Network connectivity to Databricks")
            console.print(
                f"\n[blue]Retry with:[/blue] schemax apply --target {target_env} "
                f"--profile {profile} --warehouse-id {warehouse_id}"
            )
            raise ApplyError(f"Database query failed: {e}")

        # 7. Load snapshot states
        latest_snap = read_snapshot(workspace, latest_snapshot_version)
        latest_state = latest_snap["state"]
        latest_ops = latest_snap.get("operations", [])

        if deployed_version:
            # Incremental deployment
            deployed_snap = read_snapshot(workspace, deployed_version)
            deployed_state = deployed_snap["state"]
            deployed_ops = deployed_snap.get("operations", [])
            console.print(f"[blue]Diff:[/blue] {deployed_version} → {latest_snapshot_version}")
        else:
            # First deployment - diff from empty
            deployed_state = provider.create_initial_state()
            deployed_ops = []
            console.print(f"[blue]Diff:[/blue] empty → {latest_snapshot_version}")

        # 8. Generate diff operations
        differ = provider.get_state_differ(deployed_state, latest_state, deployed_ops, latest_ops)
        diff_operations = differ.generate_diff_operations()

        console.print(f"[blue]Changes:[/blue] {len(diff_operations)} operations")

        if not diff_operations:
            console.print("[green]✓[/green] No changes to deploy")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 9. Generate SQL with explicit operation mapping
        console.print("[blue]Generating SQL...[/blue]")

        try:
            catalog_mapping = build_catalog_mapping(latest_state, env_config)
        except SQLGenerationError as e:
            raise ApplyError(str(e)) from e
        generator = provider.get_sql_generator(
            latest_state,
            catalog_mapping,
            managed_locations=project.get("managedLocations"),
            external_locations=project.get("externalLocations"),
            environment_name=target_env,
        )

        # Generate SQL with structured mapping (no comment parsing needed!)
        sql_result = generator.generate_sql_with_mapping(diff_operations)

        if not sql_result.sql or not sql_result.sql.strip():
            console.print("[green]✓[/green] No SQL to execute")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 10. Extract statements from structured result
        statements = [stmt.sql for stmt in sql_result.statements]

        if not statements:
            console.print("\n[yellow]No SQL statements to execute.[/yellow]")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 11. Show preview
        console.print("\n[bold]SQL Preview:[/bold]")
        console.print("─" * 60)

        # Show each statement with per-statement truncation
        for i, stmt in enumerate(statements, 1):
            console.print(f"\n[cyan]Statement {i}/{len(statements)}:[/cyan]")
            stmt_lines = stmt.strip().split("\n")

            if len(stmt_lines) <= 5:
                # Short statement - show in full
                for line in stmt_lines:
                    console.print(f"  {line}")
            else:
                # Long statement - show first 3 and last 1 line
                for line in stmt_lines[:3]:
                    console.print(f"  {line}")
                console.print(f"  ... ({len(stmt_lines) - 4} more lines)")
                console.print(f"  {stmt_lines[-1]}")

        console.print()
        console.print(f"[bold]Execute {len(statements)} statements?[/bold]")

        # 12. Dry-run mode - stop here
        if dry_run:
            console.print("\n[yellow]✓ Dry-run complete (no changes made)[/yellow]")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 13. Confirm with user (unless --no-interaction)
        if not no_interaction:
            console.print()
            confirm = Confirm.ask("[bold]Proceed?[/bold]", default=False)
            if not confirm:
                console.print("[yellow]Apply cancelled[/yellow]")
                return _create_empty_result(target_env, latest_snapshot_version)

        # 14. Create execution config
        config = ExecutionConfig(
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=False,
            no_interaction=no_interaction,
        )

        # 15. Validate execution config
        validation = provider.validate_execution_config(config)
        if not validation.valid:
            errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
            raise ApplyError(f"Invalid execution configuration:\n{errors}")

        # 16. Get executor and authenticate
        console.print("\n[cyan]Authenticating with Databricks...[/cyan]")
        executor = provider.get_sql_executor(config)
        console.print("[green]✓[/green] Authenticated successfully")

        # 17. Generate deployment ID early (needed for auto-rollback tracking)
        deployment_id = f"deploy_{uuid4().hex[:8]}"

        # 18. Execute statements FIRST (this creates catalog if autoCreateCatalog: true)
        console.print("\n[cyan]Executing SQL statements...[/cyan]")
        result = executor.execute_statements(statements, config)

        # 19. Track deployment IMMEDIATELY after execution (before auto-rollback)
        deployment_catalog = env_config["topLevelName"]
        unity_executor = cast(UnitySQLExecutor, executor)
        tracker = DeploymentTracker(unity_executor.client, deployment_catalog, warehouse_id)

        console.print(
            f"\n[cyan]Setting up deployment tracking in {deployment_catalog}.schemax...[/cyan]"
        )
        tracker.ensure_tracking_schema(auto_create=env_config.get("autoCreateSchemaxSchema", True))
        console.print("[green]✓[/green] Tracking schema ready")

        # Start and complete deployment tracking (reference previous deployment for partial rollback)
        # Use most recent deployment by time (any status) so partial deployments are linked
        previous_deployment_id = tracker.get_most_recent_deployment_id(target_env)
        tracker.start_deployment(
            deployment_id=deployment_id,
            environment=target_env,
            snapshot_version=latest_snapshot_version,
            project_name=project_name,
            provider_type=provider.info.id,
            provider_version=provider.info.version,
            schemax_version="0.2.0",
            from_snapshot_version=deployed_version,
            previous_deployment_id=previous_deployment_id,
        )

        # Track individual operations using explicit mapping
        # No comment parsing needed - we have structured data!

        op_id_to_op = {op.id: op for op in diff_operations}

        for i, stmt_result in enumerate(result.statement_results):
            # Find the corresponding statement info from sql_result
            stmt_info = sql_result.statements[i] if i < len(sql_result.statements) else None

            if stmt_info:
                # Record each operation that contributed to this statement
                for op_id in stmt_info.operation_ids:
                    op = op_id_to_op.get(op_id)
                    if op:
                        tracker.record_operation(
                            deployment_id=deployment_id,
                            op=op,
                            sql_stmt=stmt_result.sql,
                            result=stmt_result,
                            execution_order=i + 1,
                        )
                    else:
                        console.print(
                            f"[yellow]⚠️  Warning: Operation {op_id} not found in diff[/yellow]"
                        )
            else:
                # This shouldn't happen - mismatch between generated and executed statements
                console.print(f"[yellow]⚠️  Warning: Statement {i + 1} has no mapping info[/yellow]")

        # Complete deployment tracking
        tracker.complete_deployment(deployment_id, result, result.error_message)

        # 20. Auto-rollback on failure (MVP feature!)
        # Only trigger for "partial" (some statements succeeded, some failed)
        # If status is "failed" (0 successful), there's nothing to roll back
        if result.status == "partial" and auto_rollback:
            console.print()
            console.print("[yellow]⚠️  Deployment failed! Auto-rollback triggered...[/yellow]")
            console.print()

            try:
                # Import rollback function and error here to avoid circular imports
                from .rollback import RollbackError, rollback_partial

                # Get successful operations from statement-to-operation mapping.
                # Statement index != operation index when one op generates multiple statements
                # (e.g. add_column with NOT NULL → ADD COLUMN + SET NOT NULL).
                failed_idx = result.failed_statement_index or 0
                successful_op_ids = set()
                for i in range(failed_idx):
                    if i < len(sql_result.statements):
                        for op_id in sql_result.statements[i].operation_ids:
                            successful_op_ids.add(op_id)
                if failed_idx < len(sql_result.statements):
                    for op_id in sql_result.statements[failed_idx].operation_ids:
                        successful_op_ids.discard(op_id)
                successful_ops = [op for op in diff_operations if op.id in successful_op_ids]

                # Trigger partial rollback automatically
                # Pass the failed deployment_id so rollback can query the database
                rollback_result = rollback_partial(
                    workspace=workspace,
                    deployment_id=deployment_id,  # The failed deployment ID
                    successful_ops=successful_ops,
                    target_env=target_env,
                    profile=profile,
                    warehouse_id=warehouse_id,
                    executor=executor,  # Reuse existing connection
                    catalog_mapping=catalog_mapping,
                    auto_triggered=True,  # Skip confirmation prompts
                    from_version=deployed_version,  # For accurate rollback tracking
                )

                if rollback_result.success:
                    console.print()
                    console.print("[green]✅ Environment restored to pre-deployment state[/green]")
                    console.print(
                        f"   Rolled back {rollback_result.operations_rolled_back} operations"
                    )
                    console.print("   Status: FAILED + ROLLED BACK")
                    console.print()
                    console.print("Fix the issue and redeploy.")

                    # Auto-rollback succeeded, exit with failure (no further tracking needed)
                    sys.exit(1)

                console.print()
                console.print("[red]❌ Auto-rollback failed[/red]")
                if rollback_result.error_message:
                    console.print(f"   {rollback_result.error_message}")
                console.print("   Manual rollback may be required")

            except RollbackError as e:
                console.print()
                console.print("[red]❌ Auto-rollback blocked[/red]")
                console.print(f"   {e}")
                console.print()
                console.print(
                    "[yellow]Manual intervention required - deployment partially applied[/yellow]"
                )
            except Exception as e:
                console.print()
                console.print(f"[red]❌ Auto-rollback failed unexpectedly: {e}[/red]")
                console.print("[yellow]Manual rollback may be required[/yellow]")

        # 21. Show results
        console.print()
        console.print("─" * 60)

        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Deployed {latest_snapshot_version} to {target_env} "
                f"({result.successful_statements} statements, {exec_time:.2f}s)[/green]"
            )
            schema_name = f"{env_config['topLevelName']}.schemax"
            console.print(f"[green]✓ Deployment tracked in {schema_name}[/green]")
            console.print(f"[dim]  Deployment ID: {deployment_id}[/dim]")
        else:
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Deployment failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]✓ {result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            console.print()
            console.print(f"[blue]Deployment ID:[/blue] {deployment_id}")
            console.print(f"[blue]Environment:[/blue] {target_env}")
            console.print(f"[blue]Version:[/blue] {latest_snapshot_version}")
            console.print(f"[blue]Status:[/blue] {result.status}")
            schema_loc = f"{env_config['topLevelName']}.schemax"
            console.print(f"[dim]  Tracked in {schema_loc} (ID: {deployment_id})[/dim]")

        return result

    except Exception as e:
        console.print(f"\n[red]✗ Apply failed: {e}[/red]")
        raise ApplyError(str(e)) from e


def _create_empty_result(environment: str, version: str) -> ExecutionResult:
    """Create empty execution result when no changes to deploy

    Args:
        environment: Target environment name
        version: Snapshot version

    Returns:
        Empty ExecutionResult
    """
    return ExecutionResult(
        deployment_id="none",
        total_statements=0,
        successful_statements=0,
        failed_statement_index=None,
        statement_results=[],
        total_execution_time_ms=0,
        status="success",
        error_message=None,
    )
