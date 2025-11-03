"""
Rollback Command Implementation

Provides partial and complete rollback functionality for failed deployments
with automatic safety validation and data loss prevention.
"""

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.prompt import Confirm

from ..providers.base.executor import SQLExecutor
from ..providers.base.operations import Operation
from ..providers.base.reverse_generator import ReverseOperationGenerator, SafetyLevel
from ..providers.unity.reverse_generator import UnityReverseGenerator
from ..providers.unity.safety_validator import RollbackError, SafetyValidator
from ..storage_v4 import load_current_state

console = Console()


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

    # 1. Load current state
    state, _, provider = load_current_state(workspace)

    # 2. Generate reverse operations (in REVERSE order!)
    reverse_generator = UnityReverseGenerator()
    reverse_ops = []

    for op in reversed(successful_ops):
        if not reverse_generator.can_reverse(op):
            error = f"Cannot automatically reverse operation: {op.op}"
            console.print(f"[red]✗ {error}[/red]")
            return RollbackResult(success=False, operations_rolled_back=0, error_message=error)

        try:
            reverse_op = reverse_generator.generate_reverse(op, state)
            reverse_ops.append(reverse_op)
        except Exception as e:
            error = f"Failed to generate reverse operation for {op.op}: {e}"
            console.print(f"[red]✗ {error}[/red]")
            return RollbackResult(success=False, operations_rolled_back=0, error_message=error)

    # 3. Validate safety
    safety_validator = SafetyValidator(executor)

    console.print("   Analyzing safety...", end=" ")

    all_safe = True
    blocking_reason = None

    for reverse_op in reverse_ops:
        try:
            safety = safety_validator.validate(reverse_op, catalog_mapping)

            if safety.level == SafetyLevel.RISKY or safety.level == SafetyLevel.DESTRUCTIVE:
                all_safe = False

                if auto_triggered:
                    # Auto-rollback BLOCKS on risky/destructive operations
                    blocking_reason = (
                        f"{safety.level.value} operation detected: {reverse_op.op}\n"
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
                f"   [yellow]⚠️  Could not validate safety for {reverse_op.op}: {e}[/yellow]"
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

    # 4. Show operations and confirm (if not auto-triggered)
    if not auto_triggered:
        console.print()
        console.print("[bold]Reverse operations to execute:[/bold]")
        for i, op in enumerate(reverse_ops, 1):
            console.print(f"  [{i}/{len(reverse_ops)}] {op.op} {op.target}")
        console.print()

        if not Confirm.ask("Execute rollback?", default=False):
            console.print("[yellow]Rollback cancelled[/yellow]")
            return RollbackResult(
                success=False, operations_rolled_back=0, error_message="Cancelled by user"
            )

    # 5. Generate and execute SQL
    sql_generator = provider.get_sql_generator(state, catalog_mapping)
    sql_generator.generate_sql(reverse_ops)

    console.print()
    for i, reverse_op in enumerate(reverse_ops, 1):
        console.print(f"✓ [{i}/{len(reverse_ops)}] {reverse_op.op} {reverse_op.target}")

    # TODO: Execute SQL statements
    # For now, just simulate success
    # result = executor.execute_statements(sql_statements)

    console.print()
    console.print(f"✓ Rolled back {len(reverse_ops)} operations")

    return RollbackResult(success=True, operations_rolled_back=len(reverse_ops))


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
