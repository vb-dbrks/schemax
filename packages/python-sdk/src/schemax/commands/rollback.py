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

from schemax.commands.sql import SQLGenerationError, build_catalog_mapping
from schemax.core.deployment import DeploymentTracker
from schemax.core.storage import (
    get_environment_config,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
)
from schemax.core.version import parse_semantic_version
from schemax.providers.base.executor import ExecutionConfig, SQLExecutor
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport
from schemax.providers.unity.auth import create_databricks_client
from schemax.providers.unity.executor import UnitySQLExecutor
from schemax.providers.unity.safety_validator import SafetyValidator

console = Console()


class RollbackError(Exception):
    """Raised when rollback cannot proceed safely"""


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
    2. Manually via schemax rollback --partial command

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

    # 1. Get deployment record and optionally previous deployment (for correct pre-deployment state)
    tracker = DeploymentTracker(
        cast(UnitySQLExecutor, executor).client, deployment_catalog, warehouse_id
    )
    deployment = tracker.get_deployment_by_id(deployment_id)
    if not deployment:
        raise RollbackError(
            f"Deployment '{deployment_id}' not found in {deployment_catalog}.schemax"
        )
    if from_version is None:
        from_version = deployment.get("fromVersion")

    _, _, provider, _ = load_current_state(workspace, validate=False)

    # 2. Pre-deployment state = state that existed before *this* deployment ran.
    # Use previous_deployment_id from the deployment record when set (recorded at apply time);
    # otherwise fall back to timestamp-based lookup.
    previous_deployment = None
    prev_id = deployment.get("previousDeploymentId")
    if prev_id:
        previous_deployment = tracker.get_deployment_by_id(prev_id)
    if not previous_deployment:
        previous_deployment = tracker.get_previous_deployment(target_env, deployment_id)
    if (
        previous_deployment
        and isinstance(previous_deployment, dict)
        and "opsDetails" in previous_deployment
    ):
        # State after previous deployment = previous's from_version state + previous's successful ops
        prev_from = previous_deployment.get("fromVersion")
        prev_from_state = (
            read_snapshot(workspace, prev_from)["state"]
            if isinstance(prev_from, str) and prev_from
            else provider.create_initial_state()
        )
        ops_details = previous_deployment.get("opsDetails", [])
        successful_details = [d for d in ops_details if d.get("status") == "success"]
        successful_details.sort(key=lambda d: d.get("executionOrder", 0))
        prev_ops: list[Operation] = []
        for i, d in enumerate(successful_details):
            # Valid ISO 8601: use HH:MM:SS so i>=60 does not produce invalid :60
            ts = f"1970-01-01T{i // 3600:02d}:{(i // 60) % 60:02d}:{i % 60:02d}.000Z"
            prev_ops.append(
                Operation(
                    id=d.get("id", f"op_prev_{i}"),
                    ts=ts,
                    provider="unity",
                    op=d.get("type", ""),
                    target=d.get("target", ""),
                    payload=d.get("payload") or {},
                )
            )
        pre_deployment_state = provider.apply_operations(prev_from_state.copy(), prev_ops)
    else:
        # No previous deployment (or query failed): use empty so we get rollback ops (undo everything)
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

    # 4.5. Check for CASCADE drops in partial rollback (common when reverting from empty state)
    cascade_ops = [
        op for op in rollback_ops if op.op in {"unity.drop_catalog", "unity.drop_schema"}
    ]
    if cascade_ops:
        console.print()
        console.print("[bold yellow]⚠️  WARNING: CASCADE drops detected[/bold yellow]")
        console.print()
        console.print(
            "   [dim]Partial rollback undoes only deployment "
            f"[cyan]{deployment_id}[/cyan] — but that deployment created the whole catalog.[/dim]"
        )
        console.print()
        console.print("   Partial rollback will drop entire containers:")
        for op in cascade_ops:
            obj_name = "unknown"
            try:
                # Handle both dict and Pydantic models
                catalogs = []
                if isinstance(post_deployment_state, dict):
                    catalogs = post_deployment_state.get("catalogs", [])
                elif hasattr(post_deployment_state, "catalogs"):
                    catalogs = post_deployment_state.catalogs or []

                if op.op == "unity.drop_catalog":
                    # Try to get catalog name from post-deployment state
                    for cat in catalogs:
                        cat_dict = cat if isinstance(cat, dict) else cat.model_dump()
                        if cat_dict.get("id") == op.target:
                            obj_name = cat_dict.get("name", op.target)
                            break
                    console.print(f"   • DROP CATALOG `{obj_name}` CASCADE")
                    console.print(
                        "     (This will delete ALL schemas, tables, views, and data inside)"
                    )
                elif op.op == "unity.drop_schema":
                    # Try to get schema name
                    for cat in catalogs:
                        cat_dict = cat if isinstance(cat, dict) else cat.model_dump()
                        for sch in cat_dict.get("schemas", []):
                            sch_dict = sch if isinstance(sch, dict) else sch.model_dump()
                            if sch_dict.get("id") == op.target:
                                obj_name = f"{cat_dict.get('name', 'unknown')}.{sch_dict.get('name', op.target)}"
                                break
                    console.print(f"   • DROP SCHEMA `{obj_name}` CASCADE")
                    console.print("     (This will delete ALL tables and views inside)")
            except Exception:
                # If we can't get the name, just show the operation
                console.print(f"   • {op.op} {op.target}")

        console.print()
        console.print(
            "[dim]Partial rollback undoes only this deployment (no other deployments are touched).[/dim]"
        )
        console.print(
            "[dim]This deployment's pre-deployment state was empty (first deployment), so undoing "
            "it removes everything it created — hence DROP CATALOG/SCHEMA CASCADE.[/dim]"
        )
        console.print(
            "[dim]To roll back to a specific snapshot version instead, use "
            "[cyan]schemax rollback --to-snapshot VERSION[/cyan].[/dim]"
        )
        console.print()

        if not auto_triggered and not Confirm.ask(
            "[bold red]Continue with CASCADE drops?[/bold red]", default=False
        ):
            console.print("[yellow]Rollback cancelled[/yellow]")
            return RollbackResult(
                success=False, operations_rolled_back=0, error_message="Cancelled by user"
            )

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

    rollback_deployment_id: str | None = None
    try:
        result = executor.execute_statements(statements, config)

        # If we dropped the deployment catalog itself, we cannot record rollback in the DB
        mapping = catalog_mapping or {}
        dropped_tracking_catalog = any(
            op.op == "unity.drop_catalog" and mapping.get(op.target) == deployment_catalog
            for op in rollback_ops
        )

        if not dropped_tracking_catalog:
            # 12. Track rollback deployment in database and locally (even on execution failure)
            console.print()
            console.print("[cyan]Recording rollback...[/cyan]")

            rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"

            # Initialize deployment tracker
            unity_executor = cast(UnitySQLExecutor, executor)
            tracker = DeploymentTracker(unity_executor.client, deployment_catalog, warehouse_id)

            # Ensure tracking schema exists
            tracker.ensure_tracking_schema(
                auto_create=env_config.get("autoCreateSchemaxSchema", True)
            )

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
                schemax_version="0.2.0",
                from_snapshot_version=target_deployment.get("version"),  # Failed deployment version
            )

            # Track individual rollback operations
            for i, (rollback_op, stmt_result) in enumerate(
                zip(rollback_ops, result.statement_results)
            ):
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
            # Show the actual SQL that was executed for each operation
            for i, stmt_result in enumerate(result.statement_results, 1):
                # Extract first line of SQL for compact display (remove leading dashes/comments)
                sql_lines = stmt_result.sql.strip().split("\n")
                first_sql_line = next(
                    (
                        line
                        for line in sql_lines
                        if line.strip() and not line.strip().startswith("--")
                    ),
                    sql_lines[0] if sql_lines else "",
                )
                console.print(f"  [{i}/{len(result.statement_results)}] {first_sql_line[:80]}...")
            if not dropped_tracking_catalog and rollback_deployment_id:
                console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schemax[/green]")
                console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
            else:
                console.print(
                    "[dim]  (Catalog was dropped; rollback not recorded in database)[/dim]"
                )
            return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))
        # Rollback execution failed
        failed_idx = result.failed_statement_index or 0
        console.print(f"[red]✗ Rollback failed at statement {failed_idx + 1}[/red]")
        console.print(f"[yellow]{result.successful_statements} statements succeeded[/yellow]")
        if result.error_message:
            console.print(f"[red]Error: {result.error_message}[/red]")
        schema_loc = f"{deployment_catalog}.schemax"
        if rollback_deployment_id:
            console.print(f"[dim]  Tracked in {schema_loc} (ID: {rollback_deployment_id})[/dim]")
        else:
            console.print(f"[dim]  Tracking schema: {schema_loc}[/dim]")

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


def _resolve_successful_ops_from_snapshots(
    workspace_path: Path,
    target_deployment: dict,
    successful_op_ids: list[str],
) -> list[Operation]:
    """Regenerate diff ops from snapshots and match by opsDetails."""
    from_version = target_deployment.get("fromVersion")
    to_version = target_deployment.get("version")
    if not to_version:
        raise RollbackError("Deployment missing version information")
    _, _, prov, _ = load_current_state(workspace_path, validate=False)
    if from_version:
        from_snap = read_snapshot(workspace_path, from_version)
        from_state = from_snap["state"]
        from_ops = from_snap.get("operations", [])
    else:
        from_state = prov.create_initial_state()
        from_ops = []
    to_snap = read_snapshot(workspace_path, to_version)
    to_state = to_snap["state"]
    to_ops = to_snap.get("operations", [])
    differ = prov.get_state_differ(from_state, to_state, from_ops, to_ops)
    all_diff_ops = differ.generate_diff_operations()
    ops_details = target_deployment.get("opsDetails", [])
    successful_ops_details = [od for od in ops_details if od.get("id") in successful_op_ids]
    successful_ops: list[Operation] = []
    for op_detail in successful_ops_details:
        matching = next(
            (
                op
                for op in all_diff_ops
                if op.op == op_detail.get("type")
                and op.target == op_detail.get("target")
                and op.payload == op_detail.get("payload", {})
            ),
            None,
        )
        if matching:
            successful_ops.append(matching)
    return successful_ops


def run_partial_rollback_cli(
    workspace_path: Path,
    deployment_id: str,
    target: str,
    profile: str,
    warehouse_id: str,
    no_interaction: bool = False,
    dry_run: bool = False,
) -> RollbackResult:
    """Run partial rollback from CLI: load deployment, resolve successful ops, execute rollback.

    Raises RollbackError on validation errors, deployment not found, user cancel, or
    SQLGenerationError when building catalog mapping.
    """
    if not deployment_id:
        raise RollbackError("--deployment required for partial rollback")
    if not profile or not warehouse_id:
        raise RollbackError("--profile and --warehouse-id required for partial rollback")
    if not target:
        raise RollbackError("--target required for partial rollback")

    project = read_project(workspace_path)
    env_config = get_environment_config(project, target)
    deployment_catalog = env_config["topLevelName"]
    client = create_databricks_client(profile)
    tracker = DeploymentTracker(client, deployment_catalog, warehouse_id)
    target_deployment = tracker.get_deployment_by_id(deployment_id)

    if not target_deployment:
        raise RollbackError(
            f"Deployment '{deployment_id}' not found in {deployment_catalog}.schemax"
        )

    if target_deployment.get("status") != "failed":
        console.print(
            f"[yellow]⚠️  Deployment '{deployment_id}' has status: "
            f"{target_deployment.get('status')}[/yellow]"
        )
        console.print("Partial rollback is typically used for failed deployments.")
        if not no_interaction and not Confirm.ask("Continue anyway?", default=False):
            raise RollbackError("Cancelled by user")

    ops_applied = target_deployment.get("opsApplied", [])
    failed_idx = target_deployment.get("failedStatementIndex", len(ops_applied))
    successful_op_ids = ops_applied[:failed_idx]

    if not successful_op_ids:
        console.print(
            "[yellow]No operations to rollback (deployment had no successful operations)[/yellow]"
        )
        return RollbackResult(success=True, operations_rolled_back=0)

    changelog = read_changelog(workspace_path)
    all_ops = [Operation(**op) for op in changelog.get("ops", [])]
    successful_ops = [op for op in all_ops if op.id in successful_op_ids]

    if not successful_ops and target_deployment.get("version"):
        console.print("[cyan]Operations not in changelog - regenerating from snapshots...[/cyan]")
        successful_ops = _resolve_successful_ops_from_snapshots(
            workspace_path, target_deployment, successful_op_ids
        )
        if successful_ops:
            console.print("[dim]Matched operations by type+target+payload[/dim]")

    if not successful_ops:
        raise RollbackError(
            "Could not find operation details (deployment may be too old or data corrupted)"
        )

    console.print(f"[cyan]Found {len(successful_ops)} operations to rollback[/cyan]")

    state, _, provider, _ = load_current_state(workspace_path, validate=False)
    state_dict = state.model_dump(by_alias=True) if hasattr(state, "model_dump") else state
    try:
        catalog_mapping = build_catalog_mapping(state_dict, env_config)
    except SQLGenerationError as e:
        raise RollbackError(str(e)) from e

    executor = UnitySQLExecutor(client)
    from_version = target_deployment.get("fromVersion")

    return rollback_partial(
        workspace=workspace_path,
        deployment_id=deployment_id,
        successful_ops=successful_ops,
        target_env=target,
        profile=profile,
        warehouse_id=warehouse_id,
        executor=executor,
        catalog_mapping=catalog_mapping,
        auto_triggered=False,
        from_version=from_version,
        dry_run=dry_run,
    )


def rollback_complete(
    workspace: Path,
    target_env: str,
    to_snapshot: str,
    profile: str,
    warehouse_id: str,
    _create_clone: str | None = None,
    safe_only: bool = False,
    dry_run: bool = False,
    no_interaction: bool = False,
    force: bool = False,
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
        _create_clone: Optional name for backup SHALLOW CLONE (not yet implemented)
        safe_only: Only execute safe operations (skip destructive)
        dry_run: Preview impact without executing
        no_interaction: If True, skip confirmation prompt
        force: If True, allow rollback to snapshot before import baseline (with confirmation)

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

        # 1b. Baseline guard: block rollback before import baseline unless --force
        baseline_version = env_config.get("importBaselineSnapshot")
        if baseline_version:
            try:
                to_parsed = parse_semantic_version(to_snapshot)
                baseline_parsed = parse_semantic_version(baseline_version)
                if to_parsed < baseline_parsed:
                    if not force:
                        raise RollbackError(
                            f"Rollback to {to_snapshot} is before the import baseline for this "
                            f"environment ({baseline_version}). Rolling back past the baseline would "
                            "undo the imported state and is not allowed. Use --force to override (see docs)."
                        )
                    console.print(
                        "[yellow]⚠ Rollback target is before the import baseline for this environment.[/yellow]"
                    )
                    console.print(
                        "[yellow]  This undoes the imported state and may leave project and DB out of sync.[/yellow]"
                    )
                    if not no_interaction and not Confirm.ask(
                        "Proceed with rollback before baseline?", default=False
                    ):
                        raise RollbackError("Rollback cancelled by user.")
            except ValueError:
                # Non-semver snapshot labels: skip baseline check (or allow only with force)
                pass

        # 2. Build catalog mapping
        from schemax.commands.diff import _build_catalog_mapping
        from schemax.core.storage import read_snapshot

        target_snapshot = read_snapshot(workspace, to_snapshot)
        catalog_mapping = _build_catalog_mapping(target_snapshot["state"], env_config)

        console.print(f"[cyan]Environment:[/cyan] {target_env}")
        console.print(f"[cyan]Target catalog:[/cyan] {deployment_catalog}")
        console.print(f"[cyan]Target snapshot:[/cyan] {to_snapshot}")
        console.print()

        # 3. Check if already at target version in database (prevent redundant rollbacks)
        from schemax.providers.unity.auth import create_databricks_client

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
                    console.print("  Consider running: [cyan]schemax snapshot create[/cyan]")
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
        current_state, _, provider, _ = load_current_state(workspace, validate=False)
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

            if not no_interaction and not Confirm.ask(
                f"Execute complete rollback to {to_snapshot}?", default=False
            ):
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

        # If we dropped the deployment catalog itself, we cannot record rollback in the DB
        mapping = catalog_mapping or {}
        dropped_tracking_catalog = any(
            op.op == "unity.drop_catalog" and mapping.get(op.target) == deployment_catalog
            for op in rollback_ops
        )

        rollback_deployment_id: str | None = None
        if not dropped_tracking_catalog:
            # 14. Track rollback deployment (reuse tracker from above)
            console.print()
            console.print("[cyan]Recording rollback...[/cyan]")

            # Reuse tracker from above, ensure schema exists
            tracker.ensure_tracking_schema(
                auto_create=env_config.get("autoCreateSchemaxSchema", True)
            )

            # Get current deployed version from database for accurate tracking
            current_deployment = tracker.get_latest_deployment(target_env)
            current_deployed_version = (
                current_deployment.get("version") if current_deployment else None
            )

            rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"

            tracker.start_deployment(
                deployment_id=rollback_deployment_id,
                environment=target_env,
                snapshot_version=to_snapshot,  # State after rollback
                project_name=project_name,
                provider_type=provider.info.id,
                provider_version=provider.info.version,
                schemax_version="0.2.0",
                from_snapshot_version=current_deployed_version,  # State before rollback
            )

            # Track individual operations
            for i, (rollback_op, stmt_result) in enumerate(
                zip(rollback_ops, result.statement_results)
            ):
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
            # Show the actual SQL that was executed for each operation
            for i, stmt_result in enumerate(result.statement_results, 1):
                # Extract first line of SQL for compact display (remove leading dashes/comments)
                sql_lines = stmt_result.sql.strip().split("\n")
                first_sql_line = next(
                    (
                        line
                        for line in sql_lines
                        if line.strip() and not line.strip().startswith("--")
                    ),
                    sql_lines[0] if sql_lines else "",
                )
                console.print(f"  [{i}/{len(result.statement_results)}] {first_sql_line[:80]}...")
            if not dropped_tracking_catalog and rollback_deployment_id:
                console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schemax[/green]")
                console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
            else:
                console.print(
                    "[dim]  (Catalog was dropped; rollback not recorded in database)[/dim]"
                )
            return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))
        failed_idx = result.failed_statement_index or 0
        console.print(f"[red]✗ Rollback failed at statement {failed_idx + 1}[/red]")
        console.print(f"[yellow]{result.successful_statements} statements succeeded[/yellow]")
        if result.error_message:
            console.print(f"[red]Error: {result.error_message}[/red]")
        if rollback_deployment_id:
            console.print(f"[yellow]Partial rollback recorded: {rollback_deployment_id}[/yellow]")

        return RollbackResult(
            success=False,
            operations_rolled_back=result.successful_statements,
            error_message=result.error_message,
        )

    except RollbackError:
        raise
    except Exception as e:
        console.print(f"[red]✗ Complete rollback failed: {e}[/red]")
        return RollbackResult(
            success=False,
            operations_rolled_back=0,
            error_message=str(e),
        )
