"""
Rollback Command Implementation

Provides partial and complete rollback functionality for failed deployments
with automatic safety validation and data loss prevention.

Uses state_differ (like diff command) to generate rollback operations - much
simpler and more robust than manual reverse operation generation.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm

from schematic.core.deployment import DeploymentTracker
from schematic.core.storage import (
    get_environment_config,
    load_current_state,
    read_project,
)
from schematic.providers.base.executor import ExecutionConfig, SQLExecutor
from schematic.providers.base.operations import Operation
from schematic.providers.base.reverse_generator import SafetyLevel, SafetyReport
from schematic.providers.unity.executor import UnitySQLExecutor
from schematic.providers.unity.safety_validator import SafetyValidator

console = Console()


class RollbackError(Exception):
    """Raised when rollback cannot proceed safely"""

    pass


@dataclass
class RollbackResult:
    """Result of rollback operation"""

    success: bool
    operations_rolled_back: int
    error_message: str | None = None


def rollback_partial(
    workspace: Path,
    deployment_id: str,
    successful_ops: list[Operation],
    target_env: str,
    profile: str,
    warehouse_id: str,
    executor: SQLExecutor,
    catalog_mapping: dict[str, str] | None = None,
    auto_triggered: bool = False,
    from_version: str | None = None,
    dry_run: bool = False,
) -> RollbackResult:
    """Rollback failed deployment by reversing successful operations

    This is called either:
    1. Automatically by apply command with --auto-rollback flag
    2. Manually via schematic rollback --partial command

    Args:
        workspace: Project workspace path
        deployment_id: Deployment ID being rolled back
        successful_ops: List of operations that succeeded before failure
        target_env: Target environment name
        profile: Databricks CLI profile
        warehouse_id: SQL Warehouse ID
        executor: SQL executor (reused from apply)
        catalog_mapping: Logical to physical catalog name mapping
        auto_triggered: If True, skips confirmation and blocks on risky operations
        from_version: Pre-deployment snapshot version (optional, auto-detected if not provided)
        dry_run: If True, show SQL preview and exit without executing

    Returns:
        RollbackResult with success status and details

    Raises:
        RollbackError: If rollback cannot proceed safely (auto mode only)
    """
    console.print(f"🔄 Rolling back deployment: {deployment_id}")
    console.print(f"   Rolling back {len(successful_ops)} successful operations...")

    if not successful_ops:
        console.print("   No operations to rollback")
        return RollbackResult(success=True, operations_rolled_back=0)

    # Load project metadata for tracking
    project = read_project(workspace)
    project_name = project.get("name", "unknown")
    env_config = get_environment_config(project, target_env)
    deployment_catalog = env_config["topLevelName"]

    # 1. Determine pre-deployment version
    from schematic.core.storage import read_snapshot

    # If from_version not provided, look it up from deployment record in database
    if from_version is None:
        # Query database for deployment (source of truth)
        tracker = DeploymentTracker(
            cast(UnitySQLExecutor, executor).client, deployment_catalog, warehouse_id
        )
        deployment = tracker.get_deployment_by_id(deployment_id)

        if not deployment:
            raise RollbackError(
                f"Deployment '{deployment_id}' not found in {deployment_catalog}.schematic"
            )

        from_version = deployment.get("fromVersion")

    # 2. Load pre-deployment state (from the fromVersion snapshot, not current workspace state!)
    _, _, provider = load_current_state(workspace)

    if from_version:
        from_snap = read_snapshot(workspace, from_version)
        pre_deployment_state = from_snap["state"]
    else:
        # First deployment - pre-deployment state is empty
        pre_deployment_state = provider.create_initial_state()

    # 3. Calculate post-deployment state (after successful operations)
    # Apply successful operations to pre-deployment state to get current state
    post_deployment_state = provider.apply_operations(pre_deployment_state.copy(), successful_ops)

    # 4. Generate rollback operations using state_differ
    # This is the same approach as diff.py - battle-tested and robust!
    console.print("   Generating rollback operations...")
    differ = provider.get_state_differ(
        post_deployment_state,  # Current state (after successful ops)
        pre_deployment_state,  # Target state (before deployment)
        successful_ops,  # Operations that were applied
        [],  # Target has no operations
    )

    try:
        rollback_ops = differ.generate_diff_operations()
    except Exception as e:
        error = f"Failed to generate rollback operations: {e}"
        console.print(f"[red]✗ {error}[/red]")
        return RollbackResult(success=False, operations_rolled_back=0, error_message=error)

    if not rollback_ops:
        console.print("   No rollback operations needed (state unchanged)")
        return RollbackResult(success=True, operations_rolled_back=0)

    # 5. Validate safety of rollback operations
    validation_config = ExecutionConfig(
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        dry_run=False,
        no_interaction=False,
    )
    safety_validator = SafetyValidator(executor, validation_config)

    console.print("   Analyzing safety...", end=" ")

    all_safe = True
    blocking_reason = None

    for rollback_op in rollback_ops:
        try:
            safety = safety_validator.validate(rollback_op, catalog_mapping)

            if safety.level == SafetyLevel.RISKY or safety.level == SafetyLevel.DESTRUCTIVE:
                all_safe = False

                if auto_triggered:
                    # Auto-rollback BLOCKS on risky/destructive operations
                    blocking_reason = (
                        f"{safety.level.value} operation detected: {rollback_op.op}\n"
                        f"   {safety.reason}"
                    )
                    break
                else:
                    # Manual rollback shows warning but can proceed with confirmation
                    console.print()
                    console.print(f"   [yellow]⚠️  {safety.level.value}: {safety.reason}[/yellow]")
                    if safety.sample_data:
                        console.print("   Sample data at risk:")
                        for row in safety.sample_data[:3]:
                            console.print(f"      {row}")
        except Exception as e:
            console.print()
            console.print(
                f"   [yellow]⚠️  Could not validate safety for {rollback_op.op}: {e}[/yellow]"
            )

    if blocking_reason and auto_triggered:
        # Auto-rollback cannot proceed
        console.print("[red]✗ BLOCKED[/red]")
        console.print()
        raise RollbackError(
            f"Auto-rollback blocked - manual intervention required\n{blocking_reason}"
        )

    if all_safe:
        console.print("✓ All operations SAFE (no data loss)")
    else:
        console.print()

    # 6. Show operations and confirm (if not auto-triggered)
    if not auto_triggered:
        console.print()
        console.print("[bold]Rollback operations to execute:[/bold]")
        for i, op in enumerate(rollback_ops, 1):
            console.print(f"  [{i}/{len(rollback_ops)}] {op.op} {op.target}")
        console.print()

        if not Confirm.ask("Execute rollback?", default=False):
            console.print("[yellow]Rollback cancelled[/yellow]")
            return RollbackResult(
                success=False, operations_rolled_back=0, error_message="Cancelled by user"
            )

    # 7. Generate SQL for rollback operations
    # Use post_deployment_state (current) so objects to be dropped are in id_name_map
    # This makes SQL generation deterministic - no need for payload fallback
    sql_generator = provider.get_sql_generator(post_deployment_state, catalog_mapping)
    # 8. Generate SQL with structured mapping
    sql_result = sql_generator.generate_sql_with_mapping(rollback_ops)

    # Extract statements
    statements = [stmt.sql for stmt in sql_result.statements]

    if not statements:
        console.print("[yellow]No SQL statements generated[/yellow]")
        return RollbackResult(
            success=False, operations_rolled_back=0, error_message="No SQL generated"
        )

    # 9. Show SQL preview (match apply command behavior)
    console.print()
    console.print("[bold]SQL Preview:[/bold]")
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
    console.print(f"[bold]Execute {len(statements)} rollback statements?[/bold]")

    # 10. Dry run - stop here
    if dry_run:
        console.print()
        console.print("[yellow]✓ Dry-run complete (no changes made)[/yellow]")
        return RollbackResult(success=True, operations_rolled_back=0)

    # 11. Execute rollback SQL statements
    console.print()
    console.print(f"[cyan]Executing {len(statements)} rollback statements...[/cyan]")

    config = ExecutionConfig(
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        catalog=catalog_mapping.get("__implicit__") if catalog_mapping else None,
    )

    try:
        result = executor.execute_statements(statements, config)

        # 12. Track rollback deployment in database and locally
        console.print()
        console.print("[cyan]Recording rollback...[/cyan]")

        # Initialize deployment tracker
        unity_executor = cast(UnitySQLExecutor, executor)
        tracker = DeploymentTracker(unity_executor.client, deployment_catalog, warehouse_id)

        # Ensure tracking schema exists
        tracker.ensure_tracking_schema(
            auto_create=env_config.get("autoCreateSchematicSchema", True)
        )

        # Generate unique rollback deployment ID
        rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"

        # Determine the snapshot version after rollback
        # For partial rollback, we revert to the deployment's fromVersion
        # Need to query deployment to get version info
        target_deployment = tracker.get_deployment_by_id(deployment_id)
        if not target_deployment:
            raise RollbackError(f"Deployment '{deployment_id}' not found")

        reverted_to_version = target_deployment.get("fromVersion")
        if not reverted_to_version:
            # Edge case: deployment has no fromVersion (created before tracking was added)
            # Fall back to marking as rollback
            reverted_to_version = f"rollback_of_{deployment_id}"

        # Start rollback deployment tracking
        tracker.start_deployment(
            deployment_id=rollback_deployment_id,
            environment=target_env,
            snapshot_version=reverted_to_version,  # State after rollback
            project_name=project_name,
            provider_type=provider.info.id,
            provider_version=provider.info.version,
            schematic_version="0.2.0",
            from_snapshot_version=target_deployment.get("version"),  # Failed deployment version
        )

        # Track individual rollback operations
        for i, (rollback_op, stmt_result) in enumerate(zip(rollback_ops, result.statement_results)):
            tracker.record_operation(
                deployment_id=rollback_deployment_id,
                op=rollback_op,
                sql_stmt=stmt_result.sql,
                result=stmt_result,
                execution_order=i + 1,
            )

        # Complete rollback deployment tracking in database
        tracker.complete_deployment(rollback_deployment_id, result, result.error_message)

        # 10. Report execution results
        console.print()
        if result.status == "success":
            console.print(
                f"[green]✓ Successfully rolled back {len(rollback_ops)} operations[/green]"
            )
            for i, rollback_op in enumerate(rollback_ops, 1):
                console.print(f"  [{i}/{len(rollback_ops)}] {rollback_op.op} {rollback_op.target}")
            console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schematic[/green]")
            console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
            return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))
        else:
            # Rollback execution failed
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Rollback failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]{result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            schema_loc = f"{deployment_catalog}.schematic"
            console.print(f"[dim]  Tracked in {schema_loc} (ID: {rollback_deployment_id})[/dim]")

            return RollbackResult(
                success=False,
                operations_rolled_back=result.successful_statements,
                error_message=result.error_message,
            )

    except Exception as e:
        console.print(f"[red]✗ Rollback execution failed: {e}[/red]")
        return RollbackResult(
            success=False,
            operations_rolled_back=0,
            error_message=str(e),
        )


def rollback_complete(
    workspace: Path,
    target_env: str,
    to_snapshot: str,
    profile: str,
    warehouse_id: str,
    create_clone: str | None = None,
    safe_only: bool = False,
    dry_run: bool = False,
) -> RollbackResult:
    """Complete rollback to a previous snapshot

    This uses the same pattern as diff.py - compare current state with
    target snapshot and generate operations to transform current → target.

    Args:
        workspace: Project workspace path
        target_env: Target environment name
        to_snapshot: Target snapshot version to roll back to
        profile: Databricks CLI profile
        warehouse_id: SQL Warehouse ID
        create_clone: Optional name for backup SHALLOW CLONE (not yet implemented)
        safe_only: Only execute safe operations (skip destructive)
        dry_run: Preview impact without executing

    Returns:
        RollbackResult with success status
    """
    console.print(f"[bold]🔄 Complete Rollback to {to_snapshot}[/bold]")
    console.print()

    try:
        # 1. Load project and environment config
        project = read_project(workspace)
        project_name = project.get("name", "unknown")
        env_config = get_environment_config(project, target_env)
        deployment_catalog = env_config["topLevelName"]

        # 2. Build catalog mapping
        from schematic.commands.diff import _build_catalog_mapping
        from schematic.core.storage import read_snapshot

        target_snapshot = read_snapshot(workspace, to_snapshot)
        catalog_mapping = _build_catalog_mapping(target_snapshot["state"], env_config)

        console.print(f"[cyan]Environment:[/cyan] {target_env}")
        console.print(f"[cyan]Target catalog:[/cyan] {deployment_catalog}")
        console.print(f"[cyan]Target snapshot:[/cyan] {to_snapshot}")
        console.print()

        # 3. Check if already at target version in database (prevent redundant rollbacks)
        from schematic.providers.unity.auth import create_databricks_client

        console.print("[cyan]Checking database state...[/cyan]")
        client = create_databricks_client(profile)
        executor = UnitySQLExecutor(client)
        tracker = DeploymentTracker(executor.client, deployment_catalog, warehouse_id)

        try:
            # Query database for latest deployment (source of truth)
            current_deployment = tracker.get_latest_deployment(target_env)
            if current_deployment:
                deployed_version = current_deployment.get("version")
                console.print(f"  [blue]Currently deployed:[/blue] {deployed_version}")

                # Check if already at target version
                if deployed_version == to_snapshot:
                    console.print()
                    console.print(f"[yellow]✓ Already at {to_snapshot} in database[/yellow]")
                    console.print(
                        "[dim]  No rollback needed - database matches target snapshot[/dim]"
                    )
                    console.print()
                    console.print("[yellow]Hint:[/yellow] Your local workspace may be out of sync.")
                    console.print("  Consider running: [cyan]schematic snapshot create[/cyan]")
                    return RollbackResult(success=True, operations_rolled_back=0)
            else:
                console.print("  [dim]No deployment history found[/dim]")
        except Exception as e:
            # If we can't query database, continue with local state comparison
            console.print(f"  [dim]Cannot query database: {e}[/dim]")
            console.print("  [dim]Will compare local state with target snapshot[/dim]")

        console.print()

        # 4. Load current state and target snapshot state
        console.print("[cyan]Loading states...[/cyan]")
        current_state, _, provider = load_current_state(workspace)
        target_state = target_snapshot["state"]

        console.print("  [green]✓[/green] Current state loaded")
        console.print(f"  [green]✓[/green] Target snapshot loaded: {to_snapshot}")

        # 5. Generate rollback operations using state_differ (same as diff.py)
        console.print()
        console.print("[cyan]Generating rollback operations...[/cyan]")

        differ = provider.get_state_differ(
            current_state,  # Source: current state
            target_state,  # Target: previous snapshot state
            [],  # Current state has no operations (it's merged)
            target_snapshot.get("operations", []),  # Target operations from snapshot
        )

        rollback_ops = differ.generate_diff_operations()

        if not rollback_ops:
            console.print("[yellow]✓ No changes needed - already at target state[/yellow]")
            return RollbackResult(success=True, operations_rolled_back=0)

        console.print(f"  [green]✓[/green] Generated {len(rollback_ops)} rollback operations")

        # 6. Validate safety of rollback operations
        console.print()
        console.print("[cyan]Analyzing safety...[/cyan]")

        # Reuse executor from database check above (already initialized)

        validation_config = ExecutionConfig(
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=False,
            no_interaction=False,
        )
        safety_validator = SafetyValidator(executor, validation_config)

        safe_ops = []
        risky_ops = []
        destructive_ops = []

        for rollback_op in rollback_ops:
            try:
                safety = safety_validator.validate(rollback_op, catalog_mapping)

                if safety.level == SafetyLevel.SAFE:
                    safe_ops.append((rollback_op, safety))
                elif safety.level == SafetyLevel.RISKY:
                    risky_ops.append((rollback_op, safety))
                else:  # DESTRUCTIVE
                    destructive_ops.append((rollback_op, safety))
            except Exception as e:
                console.print(f"  [yellow]⚠️  Could not validate {rollback_op.op}: {e}[/yellow]")
                # Create a default RISKY safety report for validation failures
                default_safety = SafetyReport(
                    level=SafetyLevel.RISKY,
                    reason=f"Validation failed: {e}",
                    data_at_risk=0,
                )
                risky_ops.append((rollback_op, default_safety))

        console.print(f"  [green]SAFE:[/green] {len(safe_ops)} operations")
        console.print(f"  [yellow]RISKY:[/yellow] {len(risky_ops)} operations")
        console.print(f"  [red]DESTRUCTIVE:[/red] {len(destructive_ops)} operations")

        # 7. Show operations
        console.print()
        console.print("[bold]Rollback Operations:[/bold]")
        for i, op in enumerate(rollback_ops, 1):
            console.print(f"  [{i}/{len(rollback_ops)}] {op.op} {op.target}")

        # 8. Apply safe_only filter if requested
        if safe_only and (risky_ops or destructive_ops):
            console.print()
            msg = "⚠️  --safe-only flag enabled, skipping risky/destructive operations"
            console.print(f"[yellow]{msg}[/yellow]")
            rollback_ops = [op for op, _ in safe_ops]

            if not rollback_ops:
                console.print("[yellow]No safe operations to execute[/yellow]")
                return RollbackResult(
                    success=False, operations_rolled_back=0, error_message="No safe operations"
                )

        # 9. Generate SQL
        console.print()
        console.print("[cyan]Generating SQL...[/cyan]")
        # Use current_state (not target_state) so objects to be dropped are in id_name_map
        # This makes SQL generation deterministic - no need for payload fallback
        sql_generator = provider.get_sql_generator(current_state, catalog_mapping)
        sql_result = sql_generator.generate_sql_with_mapping(rollback_ops)

        statements = [stmt.sql for stmt in sql_result.statements]
        console.print(f"  [green]✓[/green] Generated {len(statements)} SQL statements")

        # 10. Show SQL preview (match apply command behavior)
        console.print()
        console.print("[bold]SQL Preview:[/bold]")
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
        console.print(f"[bold]Execute {len(statements)} rollback statements?[/bold]")

        # 11. Dry run - stop here
        if dry_run:
            console.print()
            console.print("[yellow]✓ Dry-run complete (no changes made)[/yellow]")
            return RollbackResult(success=True, operations_rolled_back=0)

        # 12. Confirm rollback (unless safe_only with all safe ops)
        if not safe_only or risky_ops or destructive_ops:
            console.print()
            if destructive_ops:
                console.print(
                    "[red]⚠️  WARNING: This rollback includes DESTRUCTIVE operations![/red]"
                )
                for op, safety in destructive_ops[:3]:  # Show first 3
                    console.print(f"  • {op.op}: {safety.reason if safety else 'Unknown risk'}")
                if len(destructive_ops) > 3:
                    console.print(f"  ... and {len(destructive_ops) - 3} more")
                console.print()

            if not Confirm.ask(f"Execute complete rollback to {to_snapshot}?", default=False):
                console.print("[yellow]Rollback cancelled[/yellow]")
                return RollbackResult(
                    success=False, operations_rolled_back=0, error_message="Cancelled by user"
                )

        # 13. Execute rollback SQL
        console.print()
        console.print(f"[cyan]Executing {len(statements)} rollback statements...[/cyan]")

        config = ExecutionConfig(
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            catalog=catalog_mapping.get("__implicit__") if catalog_mapping else None,
        )

        result = executor.execute_statements(statements, config)

        # 14. Track rollback deployment (reuse tracker from above)
        console.print()
        console.print("[cyan]Recording rollback...[/cyan]")

        # Reuse tracker from above, ensure schema exists
        tracker.ensure_tracking_schema(
            auto_create=env_config.get("autoCreateSchematicSchema", True)
        )

        # Get current deployed version from database for accurate tracking
        current_deployment = tracker.get_latest_deployment(target_env)
        current_deployed_version = current_deployment.get("version") if current_deployment else None

        rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"

        tracker.start_deployment(
            deployment_id=rollback_deployment_id,
            environment=target_env,
            snapshot_version=to_snapshot,  # State after rollback
            project_name=project_name,
            provider_type=provider.info.id,
            provider_version=provider.info.version,
            schematic_version="0.2.0",
            from_snapshot_version=current_deployed_version,  # State before rollback
        )

        # Track individual operations
        for i, (rollback_op, stmt_result) in enumerate(zip(rollback_ops, result.statement_results)):
            tracker.record_operation(
                deployment_id=rollback_deployment_id,
                op=rollback_op,
                sql_stmt=stmt_result.sql,
                result=stmt_result,
                execution_order=i + 1,
            )

        tracker.complete_deployment(rollback_deployment_id, result, result.error_message)

        # 15. Report results
        console.print()
        console.print("─" * 60)

        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Rolled back to {to_snapshot} ({result.successful_statements} "
                f"statements, {exec_time:.2f}s)[/green]"
            )
            console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schematic[/green]")
            console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
            return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))
        else:
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Rollback failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]{result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            console.print(f"[yellow]Partial rollback recorded: {rollback_deployment_id}[/yellow]")

            return RollbackResult(
                success=False,
                operations_rolled_back=result.successful_statements,
                error_message=result.error_message,
            )

    except Exception as e:
        console.print(f"[red]✗ Complete rollback failed: {e}[/red]")
        return RollbackResult(
            success=False,
            operations_rolled_back=0,
            error_message=str(e),
        )
