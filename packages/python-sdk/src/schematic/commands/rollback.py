"""
Rollback Command Implementation

Provides partial and complete rollback functionality for failed deployments
with automatic safety validation and data loss prevention.

Uses state_differ (like diff command) to generate rollback operations - much
simpler and more robust than manual reverse operation generation.
"""

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.prompt import Confirm

from ..providers.base.executor import ExecutionConfig, SQLExecutor
from ..providers.base.operations import Operation
from ..providers.base.reverse_generator import SafetyLevel
from ..providers.unity.safety_validator import SafetyValidator
from ..storage_v4 import load_current_state
from .apply import parse_sql_statements

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

    # 1. Load project and provider
    pre_deployment_state, _, provider = load_current_state(workspace)

    # 2. Calculate post-deployment state (after successful operations)
    # Apply successful operations to pre-deployment state to get current state
    state_reducer = provider.get_state_reducer()
    post_deployment_state = state_reducer.apply_operations(
        pre_deployment_state.copy(), successful_ops
    )

    # 3. Generate rollback operations using state_differ
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

    # 4. Validate safety of rollback operations
    safety_validator = SafetyValidator(executor)

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

    # 5. Show operations and confirm (if not auto-triggered)
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

    # 6. Generate SQL for rollback operations
    sql_generator = provider.get_sql_generator(pre_deployment_state, catalog_mapping)
    sql = sql_generator.generate_sql(rollback_ops)

    # 7. Parse SQL into individual statements
    statements = parse_sql_statements(sql)

    if not statements:
        console.print("[yellow]No SQL statements generated[/yellow]")
        return RollbackResult(
            success=False, operations_rolled_back=0, error_message="No SQL generated"
        )

    # 8. Execute rollback SQL statements
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

        # 9. Report execution results
        console.print()
        if result.status == "success":
            console.print(
                f"[green]✓ Successfully rolled back {len(rollback_ops)} operations[/green]"
            )
            for i, rollback_op in enumerate(rollback_ops, 1):
                console.print(f"  [{i}/{len(rollback_ops)}] {rollback_op.op} {rollback_op.target}")
            return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))
        else:
            # Rollback execution failed
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Rollback failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]{result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")

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

    Args:
        workspace: Project workspace path
        target_env: Target environment name
        to_snapshot: Target snapshot version to roll back to
        profile: Databricks CLI profile
        warehouse_id: SQL Warehouse ID
        create_clone: Optional name for backup SHALLOW CLONE
        safe_only: Only execute safe operations (skip destructive)
        dry_run: Preview impact without executing

    Returns:
        RollbackResult with success status
    """
    # TODO: Implement complete rollback
    console.print("[yellow]Complete rollback not yet implemented[/yellow]")
    return RollbackResult(success=False, operations_rolled_back=0, error_message="Not implemented")
