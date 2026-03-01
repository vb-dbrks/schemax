"""
Apply Command Implementation

Executes SQL statements against target environment with preview,
confirmation, and deployment tracking.
"""

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm, Prompt

from schemax.commands._preview import print_sql_statements_preview
from schemax.commands.rollback import RollbackError, rollback_partial
from schemax.commands.sql import build_catalog_mapping
from schemax.core.deployment import DeploymentTracker
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.exceptions import SchemaXProviderError
from schemax.providers.base.executor import ExecutionConfig, ExecutionResult
from schemax.providers.base.scope_filter import filter_operations_by_managed_scope
from schemax.providers.unity.auth import create_databricks_client
from schemax.providers.unity.executor import UnitySQLExecutor

console = Console()


class ApplyError(Exception):
    """Raised when apply command fails"""


class _WorkspaceRepoPort(Protocol):
    def read_project(self, *, workspace: Path) -> dict[str, Any]: ...

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]: ...

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]: ...

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]: ...

    def create_snapshot(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]: ...


class _ApplyWorkspaceRepository:
    """Repository adapter for apply workflow."""

    def __init__(self) -> None:
        self._repository = WorkspaceRepository()

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_project(workspace=workspace)

    def read_changelog(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_changelog(workspace=workspace)

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        return self._repository.get_environment_config(project=project, environment=environment)

    def load_current_state(self, *, workspace: Path, validate: bool = False) -> tuple[Any, ...]:
        return self._repository.load_current_state(workspace=workspace, validate=validate)

    def create_snapshot(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return self._repository.create_snapshot(
            workspace=workspace,
            name=name,
            version=version,
            comment=comment,
            tags=tags,
        )

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        return self._repository.read_snapshot(workspace=workspace, version=version)


def apply_to_environment(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    dry_run: bool = False,
    no_interaction: bool = False,
    auto_rollback: bool = False,
    workspace_repo: _WorkspaceRepoPort | None = None,
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
        workspace_repo: Optional repository override for tests/injection

    Returns:
        ExecutionResult with deployment details

    Raises:
        ApplyError: If apply fails
    """
    repository: _WorkspaceRepoPort = workspace_repo or _ApplyWorkspaceRepository()
    try:
        return _ApplyCommand(
            workspace=workspace,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            auto_rollback=auto_rollback,
            workspace_repo=repository,
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


def _create_empty_result(_environment: str, _version: str) -> ExecutionResult:
    """Create empty execution result when no changes to deploy."""
    return ExecutionResult.empty()


@dataclass
class _ApplyConfig:
    """Immutable apply command parameters."""

    workspace: Path
    target_env: str
    profile: str
    warehouse_id: str
    dry_run: bool
    no_interaction: bool
    auto_rollback: bool


@dataclass
class _ApplyRuntime:
    """Mutable state built during apply workflow execution."""

    project: dict[str, Any] = field(default_factory=dict)
    project_name: str = "unknown"
    env_config: dict[str, Any] = field(default_factory=dict)
    state: Any = None
    changelog: dict[str, Any] = field(default_factory=dict)
    provider: Any = None
    latest_snapshot_version: str | None = None
    catalog_mapping: dict[str, str] = field(default_factory=dict)
    deployment_catalog: str = ""
    deployed_version: str | None = None
    diff_operations: list[Any] = field(default_factory=list)
    sql_result: Any = None
    statements: list[str] = field(default_factory=list)
    deployment_id: str = ""
    executor: Any = None
    result: ExecutionResult | None = None


class _ApplyCommand:
    """Orchestrates the apply workflow; state is stored in config and runtime."""

    def __init__(
        self,
        workspace: Path,
        target_env: str,
        profile: str,
        warehouse_id: str,
        dry_run: bool,
        no_interaction: bool,
        auto_rollback: bool,
        workspace_repo: _WorkspaceRepoPort,
    ) -> None:
        self._config = _ApplyConfig(
            workspace=workspace,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            auto_rollback=auto_rollback,
        )
        self._workspace_repo = workspace_repo
        self._runtime = _ApplyRuntime()

    def execute(self) -> ExecutionResult:
        """Run the full apply workflow."""
        config = self._config
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
        if config.dry_run:
            return self._empty_result()
        if not self._confirm_execution():
            return self._empty_result()
        result = self._execute(statements)
        self._runtime.result = result
        self._track_deployment(result, diff_ops, sql_result)
        if result.status == "partial" and config.auto_rollback:
            self._attempt_auto_rollback(result, diff_ops, sql_result)
        self._show_results(result)
        return result

    def _handle_uncommitted_changes(self) -> None:
        """Prompt for snapshot creation when there are uncommitted ops."""
        self._runtime.changelog = self._workspace_repo.read_changelog(
            workspace=self._config.workspace
        )
        if not self._runtime.changelog["ops"]:
            return
        console.print(
            f"[yellow]⚠ {len(self._runtime.changelog['ops'])} uncommitted operations found[/yellow]"
        )
        # if no_interaction: auto-create snapshot (CI/CD)
        if self._config.no_interaction:
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
        project = self._workspace_repo.read_project(workspace=self._config.workspace)
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
        self._workspace_repo.create_snapshot(
            workspace=self._config.workspace,
            name=f"Auto-snapshot for {self._config.target_env}",
            version=next_version,
            comment=f"Automatic snapshot created before deploying to {self._config.target_env}",
            tags=[],
        )
        console.print("[green]✓[/green] Snapshot created")

    def _load_project_and_environment(self) -> None:
        """Load project, env config, state, provider; build catalog mapping."""
        runtime = self._runtime
        config = self._config
        runtime.project = self._workspace_repo.read_project(workspace=config.workspace)
        runtime.project_name = runtime.project.get("name", "unknown")
        runtime.env_config = self._workspace_repo.get_environment_config(
            project=runtime.project, environment=config.target_env
        )
        console.print()
        console.print("[bold]SchemaX Apply[/bold]")
        console.print("─" * 60)
        runtime.state, runtime.changelog, runtime.provider, _ = (
            self._workspace_repo.load_current_state(workspace=config.workspace, validate=False)
        )
        console.print(
            f"[blue]Provider:[/blue] {runtime.provider.info.name} v{runtime.provider.info.version}"
        )
        console.print(f"[blue]Environment:[/blue] {config.target_env}")
        console.print(f"[blue]Warehouse:[/blue] {config.warehouse_id}")
        console.print(f"[blue]Profile:[/blue] {config.profile}")
        runtime.latest_snapshot_version = runtime.project.get("latestSnapshot")
        if not runtime.latest_snapshot_version:
            raise ApplyError("No snapshots found. Please create a snapshot first.")
        console.print(f"[blue]Latest snapshot:[/blue] {runtime.latest_snapshot_version}")
        desired_state_dict = (
            runtime.state.model_dump(by_alias=True)
            if hasattr(runtime.state, "model_dump")
            else runtime.state
        )
        runtime.catalog_mapping = build_catalog_mapping(desired_state_dict, runtime.env_config)
        runtime.deployment_catalog = runtime.env_config["topLevelName"]
        console.print(f"[blue]Deployment tracking catalog:[/blue] {runtime.deployment_catalog}")

    def _query_deployment_state(self) -> None:
        """Query database for last deployment; set self.deployed_version."""
        runtime, config = self._runtime, self._config
        try:
            client = create_databricks_client(config.profile)
            tracker = DeploymentTracker(client, runtime.deployment_catalog, config.warehouse_id)
            db_deployment = tracker.get_latest_deployment(config.target_env)
            if db_deployment:
                runtime.deployed_version = db_deployment.get("version")
                console.print(
                    f"[blue]Deployed to {config.target_env}:[/blue] {runtime.deployed_version}"
                )
                console.print("[dim](Source: Database tracking table)[/dim]")
            else:
                runtime.deployed_version = None
                console.print(f"[blue]First deployment to {config.target_env}[/blue]")
                console.print("[dim](No successful deployments in database)[/dim]")
        except Exception as err:
            console.print("\n[red]✗ Failed to query deployment database[/red]")
            console.print(f"[red]Error: {err}[/red]")
            console.print("\n[yellow]Cannot proceed without database access.[/yellow]")
            console.print("[yellow]Please check:[/yellow]")
            console.print("  • Databricks credentials are valid")
            console.print(f"  • Warehouse {config.warehouse_id} is running")
            console.print("  • Network connectivity to Databricks")
            console.print(
                f"\n[blue]Retry with:[/blue] schemax apply --target {config.target_env} "
                f"--profile {config.profile} --warehouse-id {config.warehouse_id}"
            )
            raise ApplyError(f"Database query failed: {err}") from err

    def _compute_diff(self) -> list[Any]:
        """Compute diff operations (deployed vs desired); filter by scope."""
        runtime, config = self._runtime, self._config
        latest_snap = self._workspace_repo.read_snapshot(
            workspace=config.workspace, version=runtime.latest_snapshot_version or ""
        )
        desired_ops = list(latest_snap.get("operations", [])) + list(runtime.changelog["ops"])
        if runtime.deployed_version:
            try:
                deployed_snap = self._workspace_repo.read_snapshot(
                    workspace=config.workspace, version=runtime.deployed_version
                )
                deployed_state = deployed_snap["state"]
                deployed_ops = deployed_snap.get("operations", [])
                console.print(
                    f"[blue]Diff:[/blue] {runtime.deployed_version} → {runtime.latest_snapshot_version}"
                )
            except FileNotFoundError:
                console.print(
                    f"[yellow]⚠ Deployed version {runtime.deployed_version} is recorded in the database, "
                    "but the snapshot file is missing locally.[/yellow]"
                )
                console.print(
                    "[dim]Diffing from empty state. To sync, use record-deployment or create the snapshot file.[/dim]"
                )
                deployed_state = runtime.provider.create_initial_state()
                deployed_ops = []
                console.print(f"[blue]Diff:[/blue] empty → {runtime.latest_snapshot_version}")
        else:
            deployed_state = runtime.provider.create_initial_state()
            deployed_ops = []
            console.print(f"[blue]Diff:[/blue] empty → {runtime.latest_snapshot_version}")
        if runtime.changelog["ops"]:
            console.print(
                f"[dim](Including {len(runtime.changelog['ops'])} uncommitted op(s) in desired state)[/dim]"
            )
        differ = runtime.provider.get_state_differ(
            deployed_state, runtime.state, deployed_ops, desired_ops
        )
        diff_operations = differ.generate_diff_operations()
        diff_operations = filter_operations_by_managed_scope(
            diff_operations, runtime.env_config, runtime.provider
        )
        console.print(f"[blue]Changes:[/blue] {len(diff_operations)} operations")
        return diff_operations

    def _generate_sql(self, diff_ops: list[Any]) -> tuple[Any, list[str]]:
        """Generate SQL from diff ops; return (sql_result, statements) or (None, [])."""
        console.print("[blue]Generating SQL...[/blue]")
        console.print(
            "[dim]Catalog names in the UI are logical; SQL uses physical names per environment (see mapping below).[/dim]"
        )
        runtime = self._runtime
        config = self._config
        generator = runtime.provider.get_sql_generator(
            runtime.state,
            runtime.catalog_mapping,
            managed_locations=runtime.project.get("managedLocations"),
            external_locations=runtime.project.get("externalLocations"),
            environment_name=config.target_env,
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
        print_sql_statements_preview(
            statements,
            title="SQL Preview",
            action_prompt=f"Execute {len(statements)} statements?",
        )

    def _confirm_execution(self) -> bool:
        """Ask user to confirm; return False if cancelled."""
        # if not no_interaction: prompt for SQL execution confirmation
        if self._config.no_interaction:
            return True
        console.print()
        confirm = Confirm.ask("[bold]Proceed?[/bold]", default=False)
        if not confirm:
            console.print("[yellow]Apply cancelled[/yellow]")
            return False
        return True

    def _execute(self, statements: list[str]) -> ExecutionResult:
        """Validate config, get executor, run statements."""
        runtime, config = self._runtime, self._config
        exec_config = ExecutionConfig(
            target_env=config.target_env,
            profile=config.profile,
            warehouse_id=config.warehouse_id,
            dry_run=False,
            no_interaction=config.no_interaction,
        )
        validation = runtime.provider.validate_execution_config(exec_config)
        if not validation.valid:
            errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
            raise ApplyError(f"Invalid execution configuration:\n{errors}")
        console.print("\n[cyan]Authenticating with Databricks...[/cyan]")
        runtime.executor = runtime.provider.get_sql_executor(exec_config)
        console.print("[green]✓[/green] Authenticated successfully")
        runtime.deployment_id = f"deploy_{uuid4().hex[:8]}"
        console.print("\n[cyan]Executing SQL statements...[/cyan]")
        return cast(ExecutionResult, runtime.executor.execute_statements(statements, exec_config))

    def _track_deployment(
        self,
        result: ExecutionResult,
        diff_ops: list[Any],
        sql_result: Any,
    ) -> None:
        """Set up tracking schema and record deployment and operations."""
        if not sql_result:
            return
        runtime, config = self._runtime, self._config
        unity_executor = cast(UnitySQLExecutor, runtime.executor)
        tracker = DeploymentTracker(
            unity_executor.client,
            runtime.deployment_catalog,
            config.warehouse_id,
        )
        console.print(
            f"\n[cyan]Setting up deployment tracking in {runtime.deployment_catalog}.schemax...[/cyan]"
        )
        tracker.ensure_tracking_schema(
            auto_create=runtime.env_config.get("autoCreateSchemaxSchema", True)
        )
        console.print("[green]✓[/green] Tracking schema ready")
        previous_deployment_id = tracker.get_most_recent_deployment_id(config.target_env)
        tracker.start_deployment(
            deployment_id=runtime.deployment_id,
            environment=config.target_env,
            snapshot_version=runtime.latest_snapshot_version or "",
            project_name=runtime.project_name,
            provider_type=runtime.provider.info.id,
            provider_version=runtime.provider.info.version,
            schemax_version="0.2.0",
            from_snapshot_version=runtime.deployed_version,
            previous_deployment_id=previous_deployment_id,
        )
        op_id_to_op = {operation.id: operation for operation in diff_ops}
        for i, stmt_result in enumerate(result.statement_results):
            _record_statement_operations(
                tracker,
                runtime.deployment_id,
                sql_result,
                i,
                stmt_result,
                op_id_to_op,
            )
        tracker.complete_deployment(runtime.deployment_id, result, result.error_message)

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
            runtime, config = self._runtime, self._config
            rollback_result = rollback_partial(
                workspace=config.workspace,
                deployment_id=runtime.deployment_id,
                successful_ops=successful_ops,
                target_env=config.target_env,
                profile=config.profile,
                warehouse_id=config.warehouse_id,
                executor=runtime.executor,
                catalog_mapping=runtime.catalog_mapping,
                auto_triggered=True,
            )
            _print_rollback_result(rollback_result)
        except RollbackError as err:
            _print_rollback_blocked(err)
        except Exception as err:
            _print_rollback_failed(err)

    def _show_results(self, result: ExecutionResult) -> None:
        """Print final success or failure summary."""
        runtime, config = self._runtime, self._config
        console.print()
        console.print("─" * 60)
        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Deployed {runtime.latest_snapshot_version} to {config.target_env} "
                f"({result.successful_statements} statements, {exec_time:.2f}s)[/green]"
            )
            schema_name = f"{runtime.deployment_catalog}.schemax"
            console.print(f"[green]✓ Deployment tracked in {schema_name}[/green]")
            console.print(f"[dim]  Deployment ID: {runtime.deployment_id}[/dim]")
        else:
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Deployment failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]✓ {result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            console.print()
            console.print(f"[blue]Deployment ID:[/blue] {runtime.deployment_id}")
            console.print(f"[blue]Environment:[/blue] {config.target_env}")
            console.print(f"[blue]Version:[/blue] {runtime.latest_snapshot_version}")
            console.print(f"[blue]Status:[/blue] {result.status}")
            schema_loc = f"{runtime.deployment_catalog}.schemax"
            console.print(f"[dim]  Tracked in {schema_loc} (ID: {runtime.deployment_id})[/dim]")

    def _empty_result(self) -> ExecutionResult:
        """Return empty success result when there is nothing to deploy."""
        runtime, config = self._runtime, self._config
        return _create_empty_result(config.target_env, runtime.latest_snapshot_version or "")
