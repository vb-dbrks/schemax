"""
Rollback Command Implementation

Provides partial and complete rollback functionality for failed deployments
with automatic safety validation and data loss prevention.

Uses state_differ (like diff command) to generate rollback operations - much
simpler and more robust than manual reverse operation generation.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm

import schemax.providers.unity.auth as unity_auth
from schemax.commands._preview import print_sql_statements_preview
from schemax.commands.sql import SQLGenerationError, build_catalog_mapping
from schemax.core.deployment import DeploymentRecord, DeploymentTracker
from schemax.core.version import parse_semantic_version
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.executor import ExecutionConfig, SQLExecutor
from schemax.providers.base.operations import Operation
from schemax.providers.base.reverse_generator import SafetyLevel, SafetyReport
from schemax.providers.unity.executor import UnitySQLExecutor
from schemax.providers.unity.safety_validator import SafetyValidator

console = Console()


class RollbackError(Exception):
    """Raised when rollback cannot proceed safely"""


class _WorkspaceRepoPort(Protocol):
    def read_project(self, *, workspace: Path) -> dict[str, Any]: ...

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]: ...

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]: ...

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]: ...

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]: ...


class _RollbackWorkspaceRepository:
    """Repository adapter for rollback workflows."""

    def __init__(self) -> None:
        self._repository = WorkspaceRepository()

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_project(workspace=workspace)

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        return self._repository.get_environment_config(project=project, environment=environment)

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        return self._repository.load_current_state(workspace=workspace, validate=validate)

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        return self._repository.read_snapshot(workspace=workspace, version=version)

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_changelog(workspace=workspace)


@dataclass
class RollbackResult:
    """Result of rollback operation"""

    success: bool
    operations_rolled_back: int
    error_message: str | None = None


def _enforce_import_baseline_guard(
    env_config: dict[str, Any],
    to_snapshot: str,
    force: bool,
    no_interaction: bool,
) -> None:
    """Block rollback before baseline unless explicitly forced."""
    baseline_version = env_config.get("importBaselineSnapshot")
    if not baseline_version:
        return
    try:
        to_parsed = parse_semantic_version(to_snapshot)
        baseline_parsed = parse_semantic_version(baseline_version)
    except ValueError:
        return
    if to_parsed >= baseline_parsed:
        return
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


def _check_already_at_target_version(
    tracker: DeploymentTracker, target_env: str, to_snapshot: str
) -> RollbackResult | None:
    """Return a successful short-circuit result when DB already matches target snapshot."""
    try:
        current_deployment = tracker.get_latest_deployment(target_env)
    except Exception as err:
        console.print(f"  [dim]Cannot query database: {err}[/dim]")
        console.print("  [dim]Will compare local state with target snapshot[/dim]")
        return None

    if not current_deployment:
        console.print("  [dim]No deployment history found[/dim]")
        return None

    deployed_version = current_deployment.get("version")
    console.print(f"  [blue]Currently deployed:[/blue] {deployed_version}")
    if deployed_version != to_snapshot:
        return None
    console.print()
    console.print(f"[yellow]✓ Already at {to_snapshot} in database[/yellow]")
    console.print("[dim]  No rollback needed - database matches target snapshot[/dim]")
    console.print()
    console.print("[yellow]Hint:[/yellow] Your local workspace may be out of sync.")
    console.print("  Consider running: [cyan]schemax snapshot create[/cyan]")
    return RollbackResult(success=True, operations_rolled_back=0)


def _classify_complete_rollback_safety(
    rollback_ops: list[Operation],
    safety_validator: SafetyValidator,
    catalog_mapping: dict[str, str] | None,
) -> tuple[
    list[tuple[Operation, SafetyReport]],
    list[tuple[Operation, SafetyReport]],
    list[tuple[Operation, SafetyReport]],
]:
    """Classify rollback operations into SAFE/RISKY/DESTRUCTIVE buckets."""
    safe_ops: list[tuple[Operation, SafetyReport]] = []
    risky_ops: list[tuple[Operation, SafetyReport]] = []
    destructive_ops: list[tuple[Operation, SafetyReport]] = []
    for rollback_op in rollback_ops:
        try:
            safety = safety_validator.validate(rollback_op, catalog_mapping)
            if safety.level == SafetyLevel.SAFE:
                safe_ops.append((rollback_op, safety))
            elif safety.level == SafetyLevel.RISKY:
                risky_ops.append((rollback_op, safety))
            else:
                destructive_ops.append((rollback_op, safety))
        except Exception as err:
            console.print(f"  [yellow]⚠️  Could not validate {rollback_op.op}: {err}[/yellow]")
            risky_ops.append(
                (
                    rollback_op,
                    SafetyReport(
                        level=SafetyLevel.RISKY,
                        reason=f"Validation failed: {err}",
                        data_at_risk=0,
                    ),
                )
            )
    return safe_ops, risky_ops, destructive_ops


def _build_pre_deployment_state(
    workspace: Path,
    workspace_repo: _WorkspaceRepoPort,
    provider: Any,
    tracker: DeploymentTracker,
    target_env: str,
    deployment_id: str,
    deployment: DeploymentRecord,
) -> Any:
    """Build state that existed before the deployment under rollback."""
    previous_deployment = None
    prev_id = deployment.get("previousDeploymentId")
    if prev_id:
        previous_deployment = tracker.get_deployment_by_id(prev_id)
    if not previous_deployment:
        previous_deployment = tracker.get_previous_deployment(target_env, deployment_id)
    if not (
        previous_deployment
        and isinstance(previous_deployment, dict)
        and "opsDetails" in previous_deployment
    ):
        return provider.create_initial_state()

    prev_from = previous_deployment.get("fromVersion")
    prev_from_state = (
        workspace_repo.read_snapshot(workspace=workspace, version=prev_from)["state"]
        if isinstance(prev_from, str) and prev_from
        else provider.create_initial_state()
    )
    ops_details = previous_deployment.get("opsDetails", [])
    successful_details = [
        op_detail for op_detail in ops_details if op_detail.get("status") == "success"
    ]
    successful_details.sort(key=lambda op_detail: op_detail.get("executionOrder", 0))
    prev_ops: list[Operation] = []
    for index, op_detail in enumerate(successful_details):
        timestamp = f"1970-01-01T{index // 3600:02d}:{(index // 60) % 60:02d}:{index % 60:02d}.000Z"
        prev_ops.append(
            Operation(
                id=op_detail.get("id", f"op_prev_{index}"),
                ts=timestamp,
                provider="unity",
                op=op_detail.get("type", ""),
                target=op_detail.get("target", ""),
                payload=op_detail.get("payload") or {},
            )
        )
    return provider.apply_operations(prev_from_state.copy(), prev_ops)


def _collect_catalogs_from_state(state: Any) -> list[Any]:
    """Get catalogs list from dict/model state."""
    if isinstance(state, dict):
        catalogs = state.get("catalogs", [])
        return catalogs if isinstance(catalogs, list) else []
    if hasattr(state, "catalogs"):
        catalogs = state.catalogs or []
        return catalogs if isinstance(catalogs, list) else []
    return []


def _resolve_cascade_target_name(
    operation: Operation,
    catalogs: list[Any],
) -> tuple[str, str]:
    """Resolve user-facing object name and impact note for cascade drops."""
    if operation.op == "unity.drop_catalog":
        for catalog in catalogs:
            cat_dict = catalog if isinstance(catalog, dict) else catalog.model_dump()
            if cat_dict.get("id") == operation.target:
                return (
                    str(cat_dict.get("name", operation.target)),
                    "(This will delete ALL schemas, tables, views, and data inside)",
                )
        return operation.target, "(This will delete ALL schemas, tables, views, and data inside)"

    if operation.op == "unity.drop_schema":
        for catalog_name, schema_name, schema_id in _iter_schema_refs(catalogs):
            if schema_id == operation.target:
                return (
                    f"{catalog_name}.{schema_name}",
                    "(This will delete ALL tables and views inside)",
                )
        return operation.target, "(This will delete ALL tables and views inside)"

    return operation.target, ""


def _iter_schema_refs(catalogs: list[Any]) -> list[tuple[str, str, str]]:
    """Return (catalog_name, schema_name, schema_id) tuples from mixed state model/dict."""
    refs: list[tuple[str, str, str]] = []
    for catalog in catalogs:
        cat_dict = catalog if isinstance(catalog, dict) else catalog.model_dump()
        catalog_name = str(cat_dict.get("name", "unknown"))
        for schema in cat_dict.get("schemas", []):
            sch_dict = schema if isinstance(schema, dict) else schema.model_dump()
            schema_name = str(sch_dict.get("name", "unknown"))
            schema_id = str(sch_dict.get("id", ""))
            refs.append((catalog_name, schema_name, schema_id))
    return refs


def _print_cascade_drop_details(operations: list[Operation], catalogs: list[Any]) -> None:
    """Print affected objects for partial rollback cascade operations."""
    for operation in operations:
        try:
            object_name, impact = _resolve_cascade_target_name(operation, catalogs)
            entity = "CATALOG" if operation.op == "unity.drop_catalog" else "SCHEMA"
            console.print(f"   • DROP {entity} `{object_name}` CASCADE")
            if impact:
                console.print(f"     {impact}")
        except Exception:
            console.print(f"   • {operation.op} {operation.target}")


def _maybe_confirm_partial_cascade_drops(
    rollback_ops: list[Operation],
    post_deployment_state: Any,
    deployment_id: str,
    auto_triggered: bool,
) -> RollbackResult | None:
    """Warn and optionally confirm CASCADE drops for partial rollback."""
    cascade_ops = [
        operation
        for operation in rollback_ops
        if operation.op in {"unity.drop_catalog", "unity.drop_schema"}
    ]
    if not cascade_ops:
        return None

    catalogs = _collect_catalogs_from_state(post_deployment_state)
    console.print()
    console.print("[bold yellow]⚠️  WARNING: CASCADE drops detected[/bold yellow]")
    console.print()
    console.print(
        "   [dim]Partial rollback undoes only deployment "
        f"[cyan]{deployment_id}[/cyan] — but that deployment created the whole catalog.[/dim]"
    )
    console.print()
    console.print("   Partial rollback will drop entire containers:")
    _print_cascade_drop_details(cascade_ops, catalogs)
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
    if auto_triggered:
        return None
    if Confirm.ask("[bold red]Continue with CASCADE drops?[/bold red]", default=False):
        return None
    console.print("[yellow]Rollback cancelled[/yellow]")
    return RollbackResult(
        success=False, operations_rolled_back=0, error_message="Cancelled by user"
    )


@dataclass
class _PartialRollbackContext:
    workspace: Path
    deployment_id: str
    target_env: str
    profile: str
    warehouse_id: str
    executor: SQLExecutor
    catalog_mapping: dict[str, str] | None
    auto_triggered: bool
    dry_run: bool
    project_name: str
    env_config: dict[str, Any]
    deployment_catalog: str
    provider: Any
    post_deployment_state: Any
    rollback_ops: list[Operation]


def _load_partial_rollback_context(
    workspace: Path,
    deployment_id: str,
    successful_ops: list[Operation],
    target_env: str,
    profile: str,
    warehouse_id: str,
    executor: SQLExecutor,
    catalog_mapping: dict[str, str] | None,
    auto_triggered: bool,
    dry_run: bool,
    workspace_repo: _WorkspaceRepoPort,
) -> _PartialRollbackContext:
    """Load all state needed by partial rollback and generate rollback operations."""
    project = workspace_repo.read_project(workspace=workspace)
    project_name = project.get("name", "unknown")
    env_config = workspace_repo.get_environment_config(project=project, environment=target_env)
    deployment_catalog = env_config["topLevelName"]

    tracker = DeploymentTracker(
        cast(UnitySQLExecutor, executor).client, deployment_catalog, warehouse_id
    )
    deployment = tracker.get_deployment_by_id(deployment_id)
    if not deployment:
        raise RollbackError(
            f"Deployment '{deployment_id}' not found in {deployment_catalog}.schemax"
        )
    _, _, provider, _ = workspace_repo.load_current_state(workspace=workspace, validate=False)
    pre_deployment_state = _build_pre_deployment_state(
        workspace=workspace,
        workspace_repo=workspace_repo,
        provider=provider,
        tracker=tracker,
        target_env=target_env,
        deployment_id=deployment_id,
        deployment=deployment,
    )
    post_deployment_state = provider.apply_operations(pre_deployment_state.copy(), successful_ops)

    console.print("   Generating rollback operations...")
    differ = provider.get_state_differ(
        post_deployment_state,
        pre_deployment_state,
        successful_ops,
        [],
    )
    try:
        rollback_ops = differ.generate_diff_operations()
    except Exception as err:
        raise RollbackError(f"Failed to generate rollback operations: {err}") from err

    return _PartialRollbackContext(
        workspace=workspace,
        deployment_id=deployment_id,
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        executor=executor,
        catalog_mapping=catalog_mapping,
        auto_triggered=auto_triggered,
        dry_run=dry_run,
        project_name=project_name,
        env_config=env_config,
        deployment_catalog=deployment_catalog,
        provider=provider,
        post_deployment_state=post_deployment_state,
        rollback_ops=rollback_ops,
    )


def _analyze_partial_rollback_safety(
    rollback_ops: list[Operation],
    target_env: str,
    profile: str,
    warehouse_id: str,
    executor: SQLExecutor,
    catalog_mapping: dict[str, str] | None,
    auto_triggered: bool,
) -> tuple[bool, str | None]:
    """Validate rollback safety and return (all_safe, blocking_reason)."""
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
    blocking_reason: str | None = None

    for rollback_op in rollback_ops:
        try:
            safety = safety_validator.validate(rollback_op, catalog_mapping)
        except Exception as err:
            console.print()
            console.print(
                f"   [yellow]⚠️  Could not validate safety for {rollback_op.op}: {err}[/yellow]"
            )
            continue

        if safety.level not in (SafetyLevel.RISKY, SafetyLevel.DESTRUCTIVE):
            continue

        all_safe = False
        if auto_triggered:
            blocking_reason = (
                f"{safety.level.value} operation detected: {rollback_op.op}\n   {safety.reason}"
            )
            break

        console.print()
        console.print(f"   [yellow]⚠️  {safety.level.value}: {safety.reason}[/yellow]")
        if safety.sample_data:
            console.print("   Sample data at risk:")
            for row in safety.sample_data[:3]:
                console.print(f"      {row}")

    return all_safe, blocking_reason


def _confirm_partial_rollback_execution(
    rollback_ops: list[Operation], auto_triggered: bool
) -> RollbackResult | None:
    """Prompt for partial rollback execution in manual mode."""
    if auto_triggered:
        return None

    console.print()
    console.print("[bold]Rollback operations to execute:[/bold]")
    for index, operation in enumerate(rollback_ops, 1):
        console.print(f"  [{index}/{len(rollback_ops)}] {operation.op} {operation.target}")
    console.print()
    if Confirm.ask("Execute rollback?", default=False):
        return None
    console.print("[yellow]Rollback cancelled[/yellow]")
    return RollbackResult(
        success=False, operations_rolled_back=0, error_message="Cancelled by user"
    )


def _build_partial_rollback_statements(
    provider: Any,
    post_deployment_state: Any,
    rollback_ops: list[Operation],
    catalog_mapping: dict[str, str] | None,
) -> list[str]:
    """Generate rollback SQL statements from operations."""
    sql_generator = provider.get_sql_generator(post_deployment_state, catalog_mapping)
    sql_result = sql_generator.generate_sql_with_mapping(rollback_ops)
    return [stmt.sql for stmt in sql_result.statements]


def _format_sql_preview(sql_text: str) -> str:
    """Return first non-comment SQL line for compact execution summary."""
    sql_lines = sql_text.strip().split("\n")
    first_sql_line = next(
        (line for line in sql_lines if line.strip() and not line.strip().startswith("--")),
        sql_lines[0] if sql_lines else "",
    )
    return f"{first_sql_line[:80]}..."


def _track_partial_rollback_deployment(
    context: _PartialRollbackContext,
    result: Any,
) -> str:
    """Track partial rollback execution in deployment history."""
    rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"
    unity_executor = cast(UnitySQLExecutor, context.executor)
    tracker = DeploymentTracker(
        unity_executor.client, context.deployment_catalog, context.warehouse_id
    )
    tracker.ensure_tracking_schema(
        auto_create=context.env_config.get("autoCreateSchemaxSchema", True)
    )

    target_deployment = tracker.get_deployment_by_id(context.deployment_id)
    if not target_deployment:
        raise RollbackError(f"Deployment '{context.deployment_id}' not found")
    reverted_to_version = (
        target_deployment.get("fromVersion") or f"rollback_of_{context.deployment_id}"
    )

    tracker.start_deployment(
        deployment_id=rollback_deployment_id,
        environment=context.target_env,
        snapshot_version=reverted_to_version,
        project_name=context.project_name,
        provider_type=context.provider.info.id,
        provider_version=context.provider.info.version,
        schemax_version="0.2.0",
        from_snapshot_version=target_deployment.get("version"),
    )
    for index, (rollback_op, stmt_result) in enumerate(
        zip(context.rollback_ops, result.statement_results), start=1
    ):
        tracker.record_operation(
            deployment_id=rollback_deployment_id,
            operation=rollback_op,
            sql_stmt=stmt_result.sql,
            result=stmt_result,
            execution_order=index,
        )
    tracker.complete_deployment(rollback_deployment_id, result, result.error_message)
    return rollback_deployment_id


def _report_partial_rollback_result(
    result: Any,
    rollback_ops: list[Operation],
    rollback_deployment_id: str | None,
    deployment_catalog: str,
    dropped_tracking_catalog: bool,
) -> RollbackResult:
    """Render and return final partial rollback result."""
    console.print()
    if result.status == "success":
        console.print(f"[green]✓ Successfully rolled back {len(rollback_ops)} operations[/green]")
        for index, stmt_result in enumerate(result.statement_results, 1):
            console.print(
                f"  [{index}/{len(result.statement_results)}] {_format_sql_preview(stmt_result.sql)}"
            )
        if not dropped_tracking_catalog and rollback_deployment_id:
            console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schemax[/green]")
            console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
        else:
            console.print("[dim]  (Catalog was dropped; rollback not recorded in database)[/dim]")
        return RollbackResult(success=True, operations_rolled_back=len(rollback_ops))

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


def _execute_partial_rollback(
    context: _PartialRollbackContext, statements: list[str]
) -> RollbackResult:
    """Execute rollback SQL and record tracking metadata when possible."""
    console.print()
    console.print(f"[cyan]Executing {len(statements)} rollback statements...[/cyan]")

    config = ExecutionConfig(
        target_env=context.target_env,
        profile=context.profile,
        warehouse_id=context.warehouse_id,
        catalog=context.catalog_mapping.get("__implicit__") if context.catalog_mapping else None,
    )

    result = context.executor.execute_statements(statements, config)
    mapping = context.catalog_mapping or {}
    dropped_tracking_catalog = any(
        op.op == "unity.drop_catalog" and mapping.get(op.target) == context.deployment_catalog
        for op in context.rollback_ops
    )

    rollback_deployment_id: str | None = None
    if not dropped_tracking_catalog:
        console.print()
        console.print("[cyan]Recording rollback...[/cyan]")
        rollback_deployment_id = _track_partial_rollback_deployment(context, result)

    return _report_partial_rollback_result(
        result=result,
        rollback_ops=context.rollback_ops,
        rollback_deployment_id=rollback_deployment_id,
        deployment_catalog=context.deployment_catalog,
        dropped_tracking_catalog=dropped_tracking_catalog,
    )


def _run_partial_rollback_flow(context: _PartialRollbackContext) -> RollbackResult:
    """Run partial rollback workflow once context is loaded."""
    cascade_cancelled = _maybe_confirm_partial_cascade_drops(
        rollback_ops=context.rollback_ops,
        post_deployment_state=context.post_deployment_state,
        deployment_id=context.deployment_id,
        auto_triggered=context.auto_triggered,
    )
    if cascade_cancelled is not None:
        return cascade_cancelled

    all_safe, blocking_reason = _analyze_partial_rollback_safety(
        rollback_ops=context.rollback_ops,
        target_env=context.target_env,
        profile=context.profile,
        warehouse_id=context.warehouse_id,
        executor=context.executor,
        catalog_mapping=context.catalog_mapping,
        auto_triggered=context.auto_triggered,
    )
    if blocking_reason and context.auto_triggered:
        console.print("[red]✗ BLOCKED[/red]")
        console.print()
        raise RollbackError(
            f"Auto-rollback blocked - manual intervention required\n{blocking_reason}"
        )
    if all_safe:
        console.print("✓ All operations SAFE (no data loss)")
    else:
        console.print()

    confirmation = _confirm_partial_rollback_execution(context.rollback_ops, context.auto_triggered)
    if confirmation is not None:
        return confirmation

    statements = _build_partial_rollback_statements(
        provider=context.provider,
        post_deployment_state=context.post_deployment_state,
        rollback_ops=context.rollback_ops,
        catalog_mapping=context.catalog_mapping,
    )
    if not statements:
        console.print("[yellow]No SQL statements generated[/yellow]")
        return RollbackResult(
            success=False, operations_rolled_back=0, error_message="No SQL generated"
        )

    print_sql_statements_preview(
        statements,
        title="SQL Preview",
        action_prompt=f"Execute {len(statements)} rollback statements?",
    )
    if context.dry_run:
        console.print()
        console.print("[yellow]✓ Dry-run complete (no changes made)[/yellow]")
        return RollbackResult(success=True, operations_rolled_back=0)

    try:
        return _execute_partial_rollback(context, statements)
    except Exception as err:
        console.print(f"[red]✗ Rollback execution failed: {err}[/red]")
        return RollbackResult(success=False, operations_rolled_back=0, error_message=str(err))


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
    dry_run: bool = False,
    workspace_repo: _WorkspaceRepoPort | None = None,
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
        dry_run: If True, show SQL preview and exit without executing
        workspace_repo: Optional repository override for tests/injection

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
    repository: _WorkspaceRepoPort = workspace_repo or _RollbackWorkspaceRepository()

    try:
        context = _load_partial_rollback_context(
            workspace=workspace,
            deployment_id=deployment_id,
            successful_ops=successful_ops,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            executor=executor,
            catalog_mapping=catalog_mapping,
            auto_triggered=auto_triggered,
            dry_run=dry_run,
            workspace_repo=repository,
        )
    except RollbackError as err:
        console.print(f"[red]✗ {err}[/red]")
        return RollbackResult(success=False, operations_rolled_back=0, error_message=str(err))

    if not context.rollback_ops:
        console.print("   No rollback operations needed (state unchanged)")
        return RollbackResult(success=True, operations_rolled_back=0)
    return _run_partial_rollback_flow(context)


def _resolve_successful_ops_from_snapshots(
    workspace_path: Path,
    target_deployment: DeploymentRecord,
    successful_op_ids: list[str],
    workspace_repo: _WorkspaceRepoPort,
) -> list[Operation]:
    """Regenerate diff ops from snapshots and match by opsDetails."""
    from_version = target_deployment.get("fromVersion")
    to_version = target_deployment.get("version")
    if not to_version:
        raise RollbackError("Deployment missing version information")
    _, _, prov, _ = workspace_repo.load_current_state(workspace=workspace_path, validate=False)
    if from_version:
        from_snap = workspace_repo.read_snapshot(workspace=workspace_path, version=from_version)
        from_state = from_snap["state"]
        from_ops = from_snap.get("operations", [])
    else:
        from_state = prov.create_initial_state()
        from_ops = []
    to_snap = workspace_repo.read_snapshot(workspace=workspace_path, version=to_version)
    to_state = to_snap["state"]
    to_ops = to_snap.get("operations", [])
    differ = prov.get_state_differ(from_state, to_state, from_ops, to_ops)
    all_diff_ops = differ.generate_diff_operations()
    ops_details = target_deployment.get("opsDetails", [])
    successful_ops_details = [od for od in ops_details if od.get("id") in successful_op_ids]

    operation_index: dict[tuple[str, str | None, str], list[Operation]] = {}
    for operation in all_diff_ops:
        signature = _operation_signature(operation.op, operation.target, operation.payload)
        operation_index.setdefault(signature, []).append(operation)

    successful_ops: list[Operation] = []
    for op_detail in successful_ops_details:
        signature = _operation_signature(
            str(op_detail.get("type", "")),
            op_detail.get("target"),
            op_detail.get("payload", {}),
        )
        matching_ops = operation_index.get(signature, [])
        if matching_ops:
            successful_ops.append(matching_ops.pop(0))
    return successful_ops


def _operation_signature(
    op_type: str, target: str | None, payload: Any
) -> tuple[str, str | None, str]:
    """Build a stable operation signature for matching deployment op details."""
    payload_json = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"), default=str)
    return op_type, target, payload_json


def _validate_partial_cli_inputs(
    deployment_id: str, target: str, profile: str, warehouse_id: str
) -> None:
    """Validate required CLI inputs for partial rollback."""
    if not deployment_id:
        raise RollbackError("--deployment required for partial rollback")
    if not profile or not warehouse_id:
        raise RollbackError("--profile and --warehouse-id required for partial rollback")
    if not target:
        raise RollbackError("--target required for partial rollback")


def _resolve_successful_partial_ops(
    workspace_path: Path,
    target_deployment: DeploymentRecord,
    successful_op_ids: list[str],
    workspace_repo: _WorkspaceRepoPort,
) -> list[Operation]:
    """Resolve successful operations for partial rollback from changelog/snapshots."""
    changelog = workspace_repo.read_changelog(workspace=workspace_path)
    all_ops = [Operation(**operation) for operation in changelog.get("ops", [])]
    successful_ops = [operation for operation in all_ops if operation.id in successful_op_ids]
    if successful_ops or not target_deployment.get("version"):
        return successful_ops
    console.print("[cyan]Operations not in changelog - regenerating from snapshots...[/cyan]")
    successful_ops = _resolve_successful_ops_from_snapshots(
        workspace_path, target_deployment, successful_op_ids, workspace_repo
    )
    if successful_ops:
        console.print("[dim]Matched operations by type+target+payload[/dim]")
    return successful_ops


def _confirm_complete_rollback_execution(
    to_snapshot: str,
    no_interaction: bool,
    destructive_ops: list[tuple[Operation, SafetyReport]],
) -> bool:
    """Display destructive warnings and collect execution confirmation."""
    console.print()
    if destructive_ops:
        console.print("[red]⚠️  WARNING: This rollback includes DESTRUCTIVE operations![/red]")
        for operation, safety in destructive_ops[:3]:
            console.print(f"  • {operation.op}: {safety.reason if safety else 'Unknown risk'}")
        if len(destructive_ops) > 3:
            console.print(f"  ... and {len(destructive_ops) - 3} more")
        console.print()
    if no_interaction:
        return True
    return Confirm.ask(f"Execute complete rollback to {to_snapshot}?", default=False)


@dataclass
class _CompleteRollbackContext:
    target_env: str
    to_snapshot: str
    profile: str
    warehouse_id: str
    project_name: str
    env_config: dict[str, Any]
    deployment_catalog: str
    catalog_mapping: dict[str, str] | None
    executor: UnitySQLExecutor
    tracker: DeploymentTracker
    provider: Any
    current_state: Any
    rollback_ops: list[Operation]
    safe_ops: list[tuple[Operation, SafetyReport]]
    risky_ops: list[tuple[Operation, SafetyReport]]
    destructive_ops: list[tuple[Operation, SafetyReport]]


@dataclass
class _PartialCliPlan:
    env_config: dict[str, Any]
    client: Any
    successful_ops: list[Operation]
    catalog_mapping: dict[str, str]


def _prepare_partial_cli_plan(
    workspace_path: Path,
    deployment_id: str,
    target: str,
    profile: str,
    warehouse_id: str,
    no_interaction: bool,
    workspace_repo: _WorkspaceRepoPort,
) -> _PartialCliPlan:
    """Load deployment and rollback operation inputs for partial CLI path."""
    project = workspace_repo.read_project(workspace=workspace_path)
    env_config = workspace_repo.get_environment_config(project=project, environment=target)
    deployment_catalog = env_config["topLevelName"]
    client = unity_auth.create_databricks_client(profile)
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
        return _PartialCliPlan(
            env_config=env_config,
            client=client,
            successful_ops=[],
            catalog_mapping={},
        )

    successful_ops = _resolve_successful_partial_ops(
        workspace_path, target_deployment, successful_op_ids, workspace_repo
    )
    if not successful_ops:
        raise RollbackError(
            "Could not find operation details (deployment may be too old or data corrupted)"
        )

    console.print(f"[cyan]Found {len(successful_ops)} operations to rollback[/cyan]")
    state, _, _provider, _ = workspace_repo.load_current_state(
        workspace=workspace_path, validate=False
    )
    state_dict = state.model_dump(by_alias=True) if hasattr(state, "model_dump") else state
    try:
        catalog_mapping = build_catalog_mapping(state_dict, env_config)
    except SQLGenerationError as err:
        raise RollbackError(str(err)) from err

    return _PartialCliPlan(
        env_config=env_config,
        client=client,
        successful_ops=successful_ops,
        catalog_mapping=catalog_mapping,
    )


def _load_complete_rollback_setup(
    workspace: Path,
    target_env: str,
    to_snapshot: str,
    profile: str,
    warehouse_id: str,
    no_interaction: bool,
    force: bool,
    workspace_repo: _WorkspaceRepoPort,
) -> (
    tuple[
        str,
        dict[str, Any],
        str,
        dict[str, Any],
        dict[str, str] | None,
        UnitySQLExecutor,
        DeploymentTracker,
    ]
    | RollbackResult
):
    """Load project/env setup and establish tracking/runtime clients."""
    project = workspace_repo.read_project(workspace=workspace)
    project_name = project.get("name", "unknown")
    env_config = workspace_repo.get_environment_config(project=project, environment=target_env)
    deployment_catalog = env_config["topLevelName"]
    _enforce_import_baseline_guard(env_config, to_snapshot, force, no_interaction)

    target_snapshot = workspace_repo.read_snapshot(workspace=workspace, version=to_snapshot)
    catalog_mapping = build_catalog_mapping(target_snapshot["state"], env_config)

    console.print(f"[cyan]Environment:[/cyan] {target_env}")
    console.print(f"[cyan]Target catalog:[/cyan] {deployment_catalog}")
    console.print(f"[cyan]Target snapshot:[/cyan] {to_snapshot}")
    console.print()
    console.print("[cyan]Checking database state...[/cyan]")

    client = unity_auth.create_databricks_client(profile)
    executor = UnitySQLExecutor(client)
    tracker = DeploymentTracker(executor.client, deployment_catalog, warehouse_id)
    short_circuit = _check_already_at_target_version(tracker, target_env, to_snapshot)
    if short_circuit is not None:
        return short_circuit

    return (
        project_name,
        env_config,
        deployment_catalog,
        target_snapshot,
        catalog_mapping,
        executor,
        tracker,
    )


def _build_complete_rollback_plan(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    to_snapshot: str,
    target_snapshot: dict[str, Any],
    catalog_mapping: dict[str, str] | None,
    executor: UnitySQLExecutor,
    workspace_repo: _WorkspaceRepoPort,
) -> (
    tuple[
        Any,
        Any,
        list[Operation],
        list[tuple[Operation, SafetyReport]],
        list[tuple[Operation, SafetyReport]],
        list[tuple[Operation, SafetyReport]],
    ]
    | RollbackResult
):
    """Load states, generate rollback operations, and classify safety."""
    console.print()
    console.print("[cyan]Loading states...[/cyan]")
    current_state, _, provider, _ = workspace_repo.load_current_state(
        workspace=workspace, validate=False
    )
    console.print("  [green]✓[/green] Current state loaded")
    console.print(f"  [green]✓[/green] Target snapshot loaded: {to_snapshot}")

    console.print()
    console.print("[cyan]Generating rollback operations...[/cyan]")
    differ = provider.get_state_differ(
        current_state,
        target_snapshot["state"],
        [],
        target_snapshot.get("operations", []),
    )
    rollback_ops = differ.generate_diff_operations()
    if not rollback_ops:
        console.print("[yellow]✓ No changes needed - already at target state[/yellow]")
        return RollbackResult(success=True, operations_rolled_back=0)
    console.print(f"  [green]✓[/green] Generated {len(rollback_ops)} rollback operations")

    validation_config = ExecutionConfig(
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        dry_run=False,
        no_interaction=False,
    )
    safety_validator = SafetyValidator(executor, validation_config)
    safe_ops, risky_ops, destructive_ops = _classify_complete_rollback_safety(
        rollback_ops, safety_validator, catalog_mapping
    )
    return current_state, provider, rollback_ops, safe_ops, risky_ops, destructive_ops


def _initialize_complete_rollback(
    workspace: Path,
    target_env: str,
    to_snapshot: str,
    profile: str,
    warehouse_id: str,
    no_interaction: bool,
    force: bool,
    workspace_repo: _WorkspaceRepoPort,
) -> _CompleteRollbackContext | RollbackResult:
    """Load config, validate baseline, and build diff/safety context."""
    setup = _load_complete_rollback_setup(
        workspace=workspace,
        target_env=target_env,
        to_snapshot=to_snapshot,
        profile=profile,
        warehouse_id=warehouse_id,
        no_interaction=no_interaction,
        force=force,
        workspace_repo=workspace_repo,
    )
    if isinstance(setup, RollbackResult):
        return setup
    (
        project_name,
        env_config,
        deployment_catalog,
        target_snapshot,
        catalog_mapping,
        executor,
        tracker,
    ) = setup
    plan = _build_complete_rollback_plan(
        workspace=workspace,
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        to_snapshot=to_snapshot,
        target_snapshot=target_snapshot,
        catalog_mapping=catalog_mapping,
        executor=executor,
        workspace_repo=workspace_repo,
    )
    if isinstance(plan, RollbackResult):
        return plan
    current_state, provider, rollback_ops, safe_ops, risky_ops, destructive_ops = plan

    return _CompleteRollbackContext(
        target_env=target_env,
        to_snapshot=to_snapshot,
        profile=profile,
        warehouse_id=warehouse_id,
        project_name=project_name,
        env_config=env_config,
        deployment_catalog=deployment_catalog,
        catalog_mapping=catalog_mapping,
        executor=executor,
        tracker=tracker,
        provider=provider,
        current_state=current_state,
        rollback_ops=rollback_ops,
        safe_ops=safe_ops,
        risky_ops=risky_ops,
        destructive_ops=destructive_ops,
    )


def _apply_safe_only_filter(
    rollback_ops: list[Operation],
    safe_ops: list[tuple[Operation, SafetyReport]],
    risky_ops: list[tuple[Operation, SafetyReport]],
    destructive_ops: list[tuple[Operation, SafetyReport]],
    safe_only: bool,
) -> tuple[list[Operation], RollbackResult | None]:
    """Apply --safe-only policy to complete rollback operations."""
    if not safe_only or (not risky_ops and not destructive_ops):
        return rollback_ops, None

    console.print()
    console.print(
        "[yellow]⚠️  --safe-only flag enabled, skipping risky/destructive operations[/yellow]"
    )
    filtered_ops = [op for op, _ in safe_ops]
    if filtered_ops:
        return filtered_ops, None
    console.print("[yellow]No safe operations to execute[/yellow]")
    return filtered_ops, RollbackResult(
        success=False,
        operations_rolled_back=0,
        error_message="No safe operations",
    )


def _build_complete_rollback_statements(
    provider: Any,
    current_state: Any,
    rollback_ops: list[Operation],
    catalog_mapping: dict[str, str] | None,
) -> list[str]:
    """Generate SQL statements for complete rollback operations."""
    sql_generator = provider.get_sql_generator(current_state, catalog_mapping)
    sql_result = sql_generator.generate_sql_with_mapping(rollback_ops)
    return [stmt.sql for stmt in sql_result.statements]


def _track_complete_rollback_deployment(
    context: _CompleteRollbackContext,
    rollback_ops: list[Operation],
    result: Any,
) -> str:
    """Persist complete rollback deployment metadata."""
    context.tracker.ensure_tracking_schema(
        auto_create=context.env_config.get("autoCreateSchemaxSchema", True)
    )
    current_deployment = context.tracker.get_latest_deployment(context.target_env)
    current_deployed_version = current_deployment.get("version") if current_deployment else None
    rollback_deployment_id = f"rollback_{uuid4().hex[:8]}"
    context.tracker.start_deployment(
        deployment_id=rollback_deployment_id,
        environment=context.target_env,
        snapshot_version=context.to_snapshot,
        project_name=context.project_name,
        provider_type=context.provider.info.id,
        provider_version=context.provider.info.version,
        schemax_version="0.2.0",
        from_snapshot_version=current_deployed_version,
    )
    for index, (rollback_op, stmt_result) in enumerate(
        zip(rollback_ops, result.statement_results), start=1
    ):
        context.tracker.record_operation(
            deployment_id=rollback_deployment_id,
            operation=rollback_op,
            sql_stmt=stmt_result.sql,
            result=stmt_result,
            execution_order=index,
        )
    context.tracker.complete_deployment(rollback_deployment_id, result, result.error_message)
    return rollback_deployment_id


def _report_complete_rollback_result(
    result: Any,
    rollback_ops: list[Operation],
    to_snapshot: str,
    rollback_deployment_id: str | None,
    deployment_catalog: str,
    dropped_tracking_catalog: bool,
) -> RollbackResult:
    """Render final complete rollback outcome."""
    console.print()
    console.print("─" * 60)
    if result.status == "success":
        exec_time = result.total_execution_time_ms / 1000
        console.print(
            f"[green]✓ Rolled back to {to_snapshot} ({result.successful_statements} "
            f"statements, {exec_time:.2f}s)[/green]"
        )
        for index, stmt_result in enumerate(result.statement_results, 1):
            console.print(
                f"  [{index}/{len(result.statement_results)}] {_format_sql_preview(stmt_result.sql)}"
            )
        if not dropped_tracking_catalog and rollback_deployment_id:
            console.print(f"[green]✓ Rollback tracked in {deployment_catalog}.schemax[/green]")
            console.print(f"[dim]  Rollback ID: {rollback_deployment_id}[/dim]")
        else:
            console.print("[dim]  (Catalog was dropped; rollback not recorded in database)[/dim]")
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


def _execute_complete_rollback(
    context: _CompleteRollbackContext,
    rollback_ops: list[Operation],
    statements: list[str],
) -> RollbackResult:
    """Execute complete rollback SQL and track deployment history."""
    console.print()
    console.print(f"[cyan]Executing {len(statements)} rollback statements...[/cyan]")
    config = ExecutionConfig(
        target_env=context.target_env,
        profile=context.profile,
        warehouse_id=context.warehouse_id,
        catalog=context.catalog_mapping.get("__implicit__") if context.catalog_mapping else None,
    )
    result = context.executor.execute_statements(statements, config)

    mapping = context.catalog_mapping or {}
    dropped_tracking_catalog = any(
        operation.op == "unity.drop_catalog"
        and mapping.get(operation.target) == context.deployment_catalog
        for operation in rollback_ops
    )

    rollback_deployment_id: str | None = None
    if not dropped_tracking_catalog:
        console.print()
        console.print("[cyan]Recording rollback...[/cyan]")
        rollback_deployment_id = _track_complete_rollback_deployment(context, rollback_ops, result)

    return _report_complete_rollback_result(
        result=result,
        rollback_ops=rollback_ops,
        to_snapshot=context.to_snapshot,
        rollback_deployment_id=rollback_deployment_id,
        deployment_catalog=context.deployment_catalog,
        dropped_tracking_catalog=dropped_tracking_catalog,
    )


def _run_complete_rollback_workflow(
    workspace: Path,
    target_env: str,
    to_snapshot: str,
    profile: str,
    warehouse_id: str,
    safe_only: bool,
    dry_run: bool,
    no_interaction: bool,
    force: bool,
    workspace_repo: _WorkspaceRepoPort,
) -> RollbackResult:
    """Orchestrate complete rollback flow using focused helper steps."""
    initialized = _initialize_complete_rollback(
        workspace=workspace,
        target_env=target_env,
        to_snapshot=to_snapshot,
        profile=profile,
        warehouse_id=warehouse_id,
        no_interaction=no_interaction,
        force=force,
        workspace_repo=workspace_repo,
    )
    if isinstance(initialized, RollbackResult):
        return initialized
    context = initialized

    console.print()
    console.print("[cyan]Analyzing safety...[/cyan]")
    console.print(f"  [green]SAFE:[/green] {len(context.safe_ops)} operations")
    console.print(f"  [yellow]RISKY:[/yellow] {len(context.risky_ops)} operations")
    console.print(f"  [red]DESTRUCTIVE:[/red] {len(context.destructive_ops)} operations")

    rollback_ops, safe_only_result = _apply_safe_only_filter(
        rollback_ops=context.rollback_ops,
        safe_ops=context.safe_ops,
        risky_ops=context.risky_ops,
        destructive_ops=context.destructive_ops,
        safe_only=safe_only,
    )
    if safe_only_result is not None:
        return safe_only_result

    console.print()
    console.print("[bold]Rollback Operations:[/bold]")
    for index, operation in enumerate(rollback_ops, 1):
        console.print(f"  [{index}/{len(rollback_ops)}] {operation.op} {operation.target}")

    statements = _build_complete_rollback_statements(
        provider=context.provider,
        current_state=context.current_state,
        rollback_ops=rollback_ops,
        catalog_mapping=context.catalog_mapping,
    )
    console.print()
    console.print("[cyan]Generating SQL...[/cyan]")
    console.print(f"  [green]✓[/green] Generated {len(statements)} SQL statements")
    print_sql_statements_preview(
        statements,
        title="SQL Preview",
        action_prompt=f"Execute {len(statements)} rollback statements?",
    )
    if dry_run:
        console.print()
        console.print("[yellow]✓ Dry-run complete (no changes made)[/yellow]")
        return RollbackResult(success=True, operations_rolled_back=0)

    if not safe_only or context.risky_ops or context.destructive_ops:
        confirmed = _confirm_complete_rollback_execution(
            to_snapshot=to_snapshot,
            no_interaction=no_interaction,
            destructive_ops=context.destructive_ops,
        )
        if not confirmed:
            console.print("[yellow]Rollback cancelled[/yellow]")
            return RollbackResult(
                success=False, operations_rolled_back=0, error_message="Cancelled by user"
            )

    return _execute_complete_rollback(context, rollback_ops, statements)


def run_partial_rollback_cli(
    workspace_path: Path,
    deployment_id: str,
    target: str,
    profile: str,
    warehouse_id: str,
    no_interaction: bool = False,
    dry_run: bool = False,
    workspace_repo: _WorkspaceRepoPort | None = None,
) -> RollbackResult:
    """Run partial rollback from CLI: load deployment, resolve successful ops, execute rollback.

    Raises RollbackError on validation errors, deployment not found, user cancel, or
    SQLGenerationError when building catalog mapping.
    """
    repository: _WorkspaceRepoPort = workspace_repo or _RollbackWorkspaceRepository()
    _validate_partial_cli_inputs(deployment_id, target, profile, warehouse_id)
    plan = _prepare_partial_cli_plan(
        workspace_path=workspace_path,
        deployment_id=deployment_id,
        target=target,
        profile=profile,
        warehouse_id=warehouse_id,
        no_interaction=no_interaction,
        workspace_repo=repository,
    )
    if not plan.successful_ops:
        return RollbackResult(success=True, operations_rolled_back=0)

    executor = UnitySQLExecutor(plan.client)

    return rollback_partial(
        workspace=workspace_path,
        deployment_id=deployment_id,
        successful_ops=plan.successful_ops,
        target_env=target,
        profile=profile,
        warehouse_id=warehouse_id,
        executor=executor,
        catalog_mapping=plan.catalog_mapping,
        auto_triggered=no_interaction,
        dry_run=dry_run,
        workspace_repo=repository,
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
    workspace_repo: _WorkspaceRepoPort | None = None,
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
        safe_only: Only execute safe operations (skip destructive)
        dry_run: Preview impact without executing
        no_interaction: If True, skip confirmation prompt
        force: If True, allow rollback to snapshot before import baseline (with confirmation)
        workspace_repo: Optional repository override for tests/injection

    Returns:
        RollbackResult with success status
    """
    console.print(f"[bold]🔄 Complete Rollback to {to_snapshot}[/bold]")
    console.print()
    del _create_clone
    repository: _WorkspaceRepoPort = workspace_repo or _RollbackWorkspaceRepository()

    try:
        return _run_complete_rollback_workflow(
            workspace=workspace,
            target_env=target_env,
            to_snapshot=to_snapshot,
            profile=profile,
            warehouse_id=warehouse_id,
            safe_only=safe_only,
            dry_run=dry_run,
            no_interaction=no_interaction,
            force=force,
            workspace_repo=repository,
        )

    except RollbackError:
        raise
    except Exception as err:
        console.print(f"[red]✗ Complete rollback failed: {err}[/red]")
        return RollbackResult(
            success=False,
            operations_rolled_back=0,
            error_message=str(err),
        )
