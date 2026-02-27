"""
Apply Command Implementation

Executes SQL statements against target environment with preview,
confirmation, and deployment tracking.
"""

import re
import sys
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm, Prompt

from schemax.commands.rollback import RollbackError, rollback_partial
from schemax.commands.sql import build_catalog_mapping
from schemax.core.deployment import DeploymentTracker
from schemax.core.storage import (
    create_snapshot,
    get_environment_config,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
)
from schemax.providers.base.exceptions import SchemaXProviderError
from schemax.providers.base.executor import ExecutionConfig, ExecutionResult
from schemax.providers.base.scope_filter import filter_operations_by_managed_scope
from schemax.providers.unity.auth import create_databricks_client
from schemax.providers.unity.executor import UnitySQLExecutor

console = Console()


class ApplyError(Exception):
    """Raised when apply command fails"""


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
        auto_rollback: If True, automatically roll back on failed deployment

    Returns:
        ExecutionResult with deployment details

    Raises:
        ApplyError: If apply fails
    """
    try:
        return _ApplyCommand(
            workspace=workspace,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            auto_rollback=auto_rollback,
        ).execute()
    except Exception as err:
        console.print(f"\n[red]✗ Apply failed: {err}[/red]")
        raise ApplyError(str(err)) from err


def _record_statement_operations(
    tracker: DeploymentTracker,
    deployment_id: str,
    sql_result: Any,
    index: int,
    stmt_result: Any,
    op_id_to_op: dict[str, Any],
) -> None:
    """Record operations that contributed to one executed statement."""
    stmt_info = sql_result.statements[index] if index < len(sql_result.statements) else None
    if not stmt_info:
        console.print(f"[yellow]⚠️  Warning: Statement {index + 1} has no mapping info[/yellow]")
        return
    for op_id in stmt_info.operation_ids:
        operation = op_id_to_op.get(op_id)
        if operation:
            tracker.record_operation(
                deployment_id=deployment_id,
                operation=operation,
                sql_stmt=stmt_result.sql,
                result=stmt_result,
                execution_order=index + 1,
            )
        else:
            console.print(f"[yellow]⚠️  Warning: Operation {op_id} not found in diff[/yellow]")


def _compute_successful_ops_for_rollback(
    result: ExecutionResult, sql_result: Any, diff_ops: list[Any]
) -> list[Any]:
    """Compute which operations succeeded before the failure (for partial rollback)."""
    failed_idx = result.failed_statement_index or 0
    successful_op_ids = set()
    for i in range(failed_idx):
        if i < len(sql_result.statements):
            for op_id in sql_result.statements[i].operation_ids:
                successful_op_ids.add(op_id)
    if failed_idx < len(sql_result.statements):
        for op_id in sql_result.statements[failed_idx].operation_ids:
            successful_op_ids.discard(op_id)
    return [operation for operation in diff_ops if operation.id in successful_op_ids]


def _print_rollback_result(rollback_result: Any) -> None:
    """Print rollback outcome; exit(1) if rollback succeeded."""
    if rollback_result.success:
        console.print()
        console.print("[green]✅ Environment restored to pre-deployment state[/green]")
        console.print(f"   Rolled back {rollback_result.operations_rolled_back} operations")
        console.print("   Status: FAILED + ROLLED BACK")
        console.print()
        console.print("Fix the issue and redeploy.")
        sys.exit(1)
    console.print()
    console.print("[red]❌ Auto-rollback failed[/red]")
    if rollback_result.error_message:
        console.print(f"   {rollback_result.error_message}")
    console.print("   Manual rollback may be required")


def _print_rollback_blocked(err: RollbackError) -> None:
    """Print message when rollback is blocked."""
    console.print()
    console.print("[red]❌ Auto-rollback blocked[/red]")
    console.print(f"   {err}")
    console.print()
    console.print("[yellow]Manual intervention required - deployment partially applied[/yellow]")


def _print_rollback_failed(err: Exception) -> None:
    """Print message when rollback fails unexpectedly."""
    console.print()
    console.print(f"[red]❌ Auto-rollback failed unexpectedly: {err}[/red]")
    console.print("[yellow]Manual rollback may be required[/yellow]")


def _create_empty_result(environment: str, version: str) -> ExecutionResult:
    """Create empty execution result when no changes to deploy."""
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


class _ApplyCommand:
    """Orchestrates the apply workflow; state is stored on the instance."""

    def __init__(
        self,
        workspace: Path,
        target_env: str,
        profile: str,
        warehouse_id: str,
        dry_run: bool,
        no_interaction: bool,
        auto_rollback: bool,
    ) -> None:
        self.workspace = workspace
        self.target_env = target_env
        self.profile = profile
        self.warehouse_id = warehouse_id
        self.dry_run = dry_run
        self.no_interaction = no_interaction
        self.auto_rollback = auto_rollback
        self.project: dict[str, Any] = {}
        self.project_name = "unknown"
        self.env_config: dict[str, Any] = {}
        self.state: Any = None
        self.changelog: dict[str, Any] = {}
        self.provider: Any = None
        self.latest_snapshot_version: str | None = None
        self.catalog_mapping: dict[str, str] = {}
        self.deployment_catalog = ""
        self.deployed_version: str | None = None
        self.diff_operations: list[Any] = []
        self.sql_result: Any = None
        self.statements: list[str] = []
        self.deployment_id = ""
        self.executor: Any = None
        self.result: ExecutionResult | None = None

    def execute(self) -> ExecutionResult:
        """Run the full apply workflow."""
        self._handle_uncommitted_changes()
        self._load_project_and_environment()
        self._query_deployment_state()
        diff_ops = self._compute_diff()
        if not diff_ops:
            return self._empty_result()
        sql_result, statements = self._generate_sql(diff_ops)
        if not statements:
            return self._empty_result()
        self._show_preview(statements)
        if self.dry_run:
            return self._empty_result()
        if not self._confirm_execution():
            return self._empty_result()
        self.result = self._execute(statements)
        self._track_deployment(self.result, diff_ops, sql_result)
        if self.result.status == "partial" and self.auto_rollback:
            self._attempt_auto_rollback(self.result, diff_ops, sql_result)
        self._show_results(self.result)
        return self.result

    def _handle_uncommitted_changes(self) -> None:
        """Prompt for snapshot creation when there are uncommitted ops."""
        self.changelog = read_changelog(self.workspace)
        if not self.changelog["ops"]:
            return
        console.print(
            f"[yellow]⚠ {len(self.changelog['ops'])} uncommitted operations found[/yellow]"
        )
        # if no_interaction: auto-create snapshot (CI/CD)
        if self.no_interaction:
            console.print("[blue]Non-interactive mode: Auto-creating snapshot[/blue]")
            choice = "create"
        else:
            console.print()
            choice = Prompt.ask(
                "[bold]What would you like to do?[/bold]",
                choices=["create", "continue", "abort"],
                default="create",
            )
        if choice == "abort":
            console.print("[yellow]Apply cancelled[/yellow]")
            sys.exit(0)
        if choice != "create":
            return
        project = read_project(self.workspace)
        settings = project.get("settings", {})
        version_prefix = str(settings.get("versionPrefix", "v"))
        current = project.get("latestSnapshot")
        if current:
            match = re.search(r"(\d+)\.(\d+)\.(\d+)", current)
            if match:
                major, minor, _patch = match.groups()
                next_version = f"{version_prefix}{major}.{int(minor) + 1}.0"
            else:
                next_version = f"{version_prefix}0.1.0"
        else:
            next_version = f"{version_prefix}0.1.0"
        console.print(f"[blue]Creating snapshot:[/blue] {next_version}")
        create_snapshot(
            self.workspace,
            name=f"Auto-snapshot for {self.target_env}",
            version=next_version,
            comment=f"Automatic snapshot created before deploying to {self.target_env}",
        )
        console.print("[green]✓[/green] Snapshot created")

    def _load_project_and_environment(self) -> None:
        """Load project, env config, state, provider; build catalog mapping."""
        self.project = read_project(self.workspace)
        self.project_name = self.project.get("name", "unknown")
        self.env_config = get_environment_config(self.project, self.target_env)
        console.print()
        console.print("[bold]SchemaX Apply[/bold]")
        console.print("─" * 60)
        self.state, self.changelog, self.provider, _ = load_current_state(
            self.workspace, validate=False
        )
        console.print(
            f"[blue]Provider:[/blue] {self.provider.info.name} v{self.provider.info.version}"
        )
        console.print(f"[blue]Environment:[/blue] {self.target_env}")
        console.print(f"[blue]Warehouse:[/blue] {self.warehouse_id}")
        console.print(f"[blue]Profile:[/blue] {self.profile}")
        self.latest_snapshot_version = self.project.get("latestSnapshot")
        if not self.latest_snapshot_version:
            raise ApplyError("No snapshots found. Please create a snapshot first.")
        console.print(f"[blue]Latest snapshot:[/blue] {self.latest_snapshot_version}")
        desired_state_dict = (
            self.state.model_dump(by_alias=True)
            if hasattr(self.state, "model_dump")
            else self.state
        )
        self.catalog_mapping = build_catalog_mapping(desired_state_dict, self.env_config)
        self.deployment_catalog = self.env_config["topLevelName"]
        console.print(f"[blue]Deployment tracking catalog:[/blue] {self.deployment_catalog}")

    def _query_deployment_state(self) -> None:
        """Query database for last deployment; set self.deployed_version."""
        try:
            client = create_databricks_client(self.profile)
            tracker = DeploymentTracker(client, self.deployment_catalog, self.warehouse_id)
            db_deployment = tracker.get_latest_deployment(self.target_env)
            if db_deployment:
                self.deployed_version = db_deployment.get("version")
                console.print(
                    f"[blue]Deployed to {self.target_env}:[/blue] {self.deployed_version}"
                )
                console.print("[dim](Source: Database tracking table)[/dim]")
            else:
                self.deployed_version = None
                console.print(f"[blue]First deployment to {self.target_env}[/blue]")
                console.print("[dim](No successful deployments in database)[/dim]")
        except Exception as err:
            console.print("\n[red]✗ Failed to query deployment database[/red]")
            console.print(f"[red]Error: {err}[/red]")
            console.print("\n[yellow]Cannot proceed without database access.[/yellow]")
            console.print("[yellow]Please check:[/yellow]")
            console.print("  • Databricks credentials are valid")
            console.print(f"  • Warehouse {self.warehouse_id} is running")
            console.print("  • Network connectivity to Databricks")
            console.print(
                f"\n[blue]Retry with:[/blue] schemax apply --target {self.target_env} "
                f"--profile {self.profile} --warehouse-id {self.warehouse_id}"
            )
            raise ApplyError(f"Database query failed: {err}") from err

    def _compute_diff(self) -> list[Any]:
        """Compute diff operations (deployed vs desired); filter by scope."""
        latest_snap = read_snapshot(self.workspace, self.latest_snapshot_version or "")
        desired_ops = list(latest_snap.get("operations", [])) + list(self.changelog["ops"])
        if self.deployed_version:
            try:
                deployed_snap = read_snapshot(self.workspace, self.deployed_version)
                deployed_state = deployed_snap["state"]
                deployed_ops = deployed_snap.get("operations", [])
                console.print(
                    f"[blue]Diff:[/blue] {self.deployed_version} → {self.latest_snapshot_version}"
                )
            except FileNotFoundError:
                console.print(
                    f"[yellow]⚠ Deployed version {self.deployed_version} is recorded in the database, "
                    "but the snapshot file is missing locally.[/yellow]"
                )
                console.print(
                    "[dim]Diffing from empty state. To sync, use record-deployment or create the snapshot file.[/dim]"
                )
                deployed_state = self.provider.create_initial_state()
                deployed_ops = []
                console.print(f"[blue]Diff:[/blue] empty → {self.latest_snapshot_version}")
        else:
            deployed_state = self.provider.create_initial_state()
            deployed_ops = []
            console.print(f"[blue]Diff:[/blue] empty → {self.latest_snapshot_version}")
        if self.changelog["ops"]:
            console.print(
                f"[dim](Including {len(self.changelog['ops'])} uncommitted op(s) in desired state)[/dim]"
            )
        differ = self.provider.get_state_differ(
            deployed_state, self.state, deployed_ops, desired_ops
        )
        diff_operations = differ.generate_diff_operations()
        diff_operations = filter_operations_by_managed_scope(
            diff_operations, self.env_config, self.provider
        )
        console.print(f"[blue]Changes:[/blue] {len(diff_operations)} operations")
        return diff_operations

    def _generate_sql(self, diff_ops: list[Any]) -> tuple[Any, list[str]]:
        """Generate SQL from diff ops; return (sql_result, statements) or (None, [])."""
        console.print("[blue]Generating SQL...[/blue]")
        console.print(
            "[dim]Catalog names in the UI are logical; SQL uses physical names per environment (see mapping below).[/dim]"
        )
        generator = self.provider.get_sql_generator(
            self.state,
            self.catalog_mapping,
            managed_locations=self.project.get("managedLocations"),
            external_locations=self.project.get("externalLocations"),
            environment_name=self.target_env,
        )
        try:
            sql_result = generator.generate_sql_with_mapping(diff_ops)
        except SchemaXProviderError as err:
            raise ApplyError(str(err)) from err
        if not sql_result.sql or not sql_result.sql.strip():
            console.print("[green]✓[/green] No SQL to execute")
            return None, []
        statements = [stmt.sql for stmt in sql_result.statements]
        if not statements:
            console.print("\n[yellow]No SQL statements to execute.[/yellow]")
            return None, []
        return sql_result, statements

    def _show_preview(self, statements: list[str]) -> None:
        """Print SQL preview."""
        console.print("\n[bold]SQL Preview:[/bold]")
        console.print("─" * 60)
        for i, stmt in enumerate(statements, 1):
            console.print(f"\n[cyan]Statement {i}/{len(statements)}:[/cyan]")
            stmt_lines = stmt.strip().split("\n")
            if len(stmt_lines) <= 5:
                for line in stmt_lines:
                    console.print(f"  {line}")
            else:
                for line in stmt_lines[:3]:
                    console.print(f"  {line}")
                console.print(f"  ... ({len(stmt_lines) - 4} more lines)")
                console.print(f"  {stmt_lines[-1]}")
        console.print()
        console.print(f"[bold]Execute {len(statements)} statements?[/bold]")

    def _confirm_execution(self) -> bool:
        """Ask user to confirm; return False if cancelled."""
        # if not no_interaction: prompt for SQL execution confirmation
        if self.no_interaction:
            return True
        console.print()
        confirm = Confirm.ask("[bold]Proceed?[/bold]", default=False)
        if not confirm:
            console.print("[yellow]Apply cancelled[/yellow]")
            return False
        return True

    def _execute(self, statements: list[str]) -> ExecutionResult:
        """Validate config, get executor, run statements."""
        config = ExecutionConfig(
            target_env=self.target_env,
            profile=self.profile,
            warehouse_id=self.warehouse_id,
            dry_run=False,
            no_interaction=self.no_interaction,
        )
        validation = self.provider.validate_execution_config(config)
        if not validation.valid:
            errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
            raise ApplyError(f"Invalid execution configuration:\n{errors}")
        console.print("\n[cyan]Authenticating with Databricks...[/cyan]")
        self.executor = self.provider.get_sql_executor(config)
        console.print("[green]✓[/green] Authenticated successfully")
        self.deployment_id = f"deploy_{uuid4().hex[:8]}"
        console.print("\n[cyan]Executing SQL statements...[/cyan]")
        return cast(ExecutionResult, self.executor.execute_statements(statements, config))

    def _track_deployment(
        self,
        result: ExecutionResult,
        diff_ops: list[Any],
        sql_result: Any,
    ) -> None:
        """Set up tracking schema and record deployment and operations."""
        if not sql_result:
            return
        unity_executor = cast(UnitySQLExecutor, self.executor)
        tracker = DeploymentTracker(
            unity_executor.client,
            self.deployment_catalog,
            self.warehouse_id,
        )
        console.print(
            f"\n[cyan]Setting up deployment tracking in {self.deployment_catalog}.schemax...[/cyan]"
        )
        tracker.ensure_tracking_schema(
            auto_create=self.env_config.get("autoCreateSchemaxSchema", True)
        )
        console.print("[green]✓[/green] Tracking schema ready")
        previous_deployment_id = tracker.get_most_recent_deployment_id(self.target_env)
        tracker.start_deployment(
            deployment_id=self.deployment_id,
            environment=self.target_env,
            snapshot_version=self.latest_snapshot_version or "",
            project_name=self.project_name,
            provider_type=self.provider.info.id,
            provider_version=self.provider.info.version,
            schemax_version="0.2.0",
            from_snapshot_version=self.deployed_version,
            previous_deployment_id=previous_deployment_id,
        )
        op_id_to_op = {operation.id: operation for operation in diff_ops}
        for i, stmt_result in enumerate(result.statement_results):
            _record_statement_operations(
                tracker,
                self.deployment_id,
                sql_result,
                i,
                stmt_result,
                op_id_to_op,
            )
        tracker.complete_deployment(self.deployment_id, result, result.error_message)

    def _attempt_auto_rollback(
        self,
        result: ExecutionResult,
        diff_ops: list[Any],
        sql_result: Any,
    ) -> None:
        """Run partial rollback when deployment failed partially and auto_rollback is on."""
        console.print()
        console.print("[yellow]⚠️  Deployment failed! Auto-rollback triggered...[/yellow]")
        console.print()
        try:
            successful_ops = _compute_successful_ops_for_rollback(result, sql_result, diff_ops)
            rollback_result = rollback_partial(
                workspace=self.workspace,
                deployment_id=self.deployment_id,
                successful_ops=successful_ops,
                target_env=self.target_env,
                profile=self.profile,
                warehouse_id=self.warehouse_id,
                executor=self.executor,
                catalog_mapping=self.catalog_mapping,
                auto_triggered=True,
                from_version=self.deployed_version,
            )
            _print_rollback_result(rollback_result)
        except RollbackError as err:
            _print_rollback_blocked(err)
        except Exception as err:
            _print_rollback_failed(err)

    def _show_results(self, result: ExecutionResult) -> None:
        """Print final success or failure summary."""
        console.print()
        console.print("─" * 60)
        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Deployed {self.latest_snapshot_version} to {self.target_env} "
                f"({result.successful_statements} statements, {exec_time:.2f}s)[/green]"
            )
            schema_name = f"{self.deployment_catalog}.schemax"
            console.print(f"[green]✓ Deployment tracked in {schema_name}[/green]")
            console.print(f"[dim]  Deployment ID: {self.deployment_id}[/dim]")
        else:
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Deployment failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]✓ {result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            console.print()
            console.print(f"[blue]Deployment ID:[/blue] {self.deployment_id}")
            console.print(f"[blue]Environment:[/blue] {self.target_env}")
            console.print(f"[blue]Version:[/blue] {self.latest_snapshot_version}")
            console.print(f"[blue]Status:[/blue] {result.status}")
            schema_loc = f"{self.deployment_catalog}.schemax"
            console.print(f"[dim]  Tracked in {schema_loc} (ID: {self.deployment_id})[/dim]")

    def _empty_result(self) -> ExecutionResult:
        """Return empty success result when there is nothing to deploy."""
        return _create_empty_result(self.target_env, self.latest_snapshot_version or "")
