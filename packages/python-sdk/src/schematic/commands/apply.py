"""
Apply Command Implementation

Executes SQL statements against target environment with preview,
confirmation, and deployment tracking.
"""

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from rich.console import Console
from rich.prompt import Confirm

from ..deployment_tracker import DeploymentTracker
from ..providers.base.executor import ExecutionConfig, ExecutionResult
from ..providers.unity.executor import UnitySQLExecutor
from ..storage_v4 import (
    create_snapshot,
    get_environment_config,
    get_last_deployment,
    load_current_state,
    read_changelog,
    read_project,
    read_snapshot,
    write_deployment,
)

console = Console()


class ApplyError(Exception):
    """Raised when apply command fails"""

    pass


def _build_catalog_mapping(state: dict, env_config: dict) -> dict:
    """
    Build catalog name mapping (logical → physical) for environment-specific SQL generation.

    Supports two modes:
    1. Single-catalog (implicit): Catalog stored as __implicit__ in state, mapped to env catalog
    2. Single-catalog (explicit): One named catalog, mapped to env catalog
    3. Multi-catalog: Not yet supported
    """
    catalogs = state.get("catalogs", [])

    if len(catalogs) == 0:
        # No catalogs yet - no mapping needed
        return {}

    if len(catalogs) == 1:
        logical_name = catalogs[0]["name"]
        physical_name = env_config["topLevelName"]

        console.print(f"[dim]  Catalog mapping: {logical_name} → {physical_name}[/dim]")

        return {logical_name: physical_name}

    # Multiple catalogs - not supported yet
    raise ApplyError(
        f"Multi-catalog projects are not yet supported. "
        f"Found {len(catalogs)} catalogs: {', '.join(c['name'] for c in catalogs)}. "
        "For now, please use a single catalog per project."
    )


def apply_to_environment(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    dry_run: bool = False,
    no_interaction: bool = False,
) -> ExecutionResult:
    """Apply changes to target environment

    Main entry point for the apply command. Auto-creates snapshot if needed,
    diffs from deployed version to latest snapshot, generates SQL, shows preview,
    confirms with user, and executes statements with deployment tracking.

    Args:
        workspace: Path to Schematic workspace
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
        # 1. Auto-create snapshot if changelog has uncommitted changes
        changelog = read_changelog(workspace)
        if changelog["ops"]:
            console.print(
                f"[yellow]⚠ {len(changelog['ops'])} uncommitted operations found[/yellow]"
            )

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

        # 2. Load project (v4 with environment config)
        project = read_project(workspace)
        project_name = project.get("name", "unknown")

        # 3. Get environment configuration
        env_config = get_environment_config(project, target_env)

        console.print()
        console.print("[bold]Schematic Apply[/bold]")
        console.print("─" * 60)

        # 4. Load current state and provider
        state, changelog, provider = load_current_state(workspace)

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

        # 6. Get last deployment for target environment
        last_deployment = get_last_deployment(project, target_env)

        if last_deployment:
            deployed_version = last_deployment.get("version")
            console.print(f"[blue]Deployed to {target_env}:[/blue] {deployed_version}")
        else:
            deployed_version = None
            console.print(f"[blue]First deployment to {target_env}[/blue]")

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

        # 9. Generate SQL in-memory from diff operations
        console.print("[blue]Generating SQL...[/blue]")

        catalog_mapping = _build_catalog_mapping(latest_state, env_config)
        generator = provider.get_sql_generator(latest_state, catalog_mapping)
        sql = generator.generate_sql(diff_operations)

        if not sql or not sql.strip():
            console.print("[green]✓[/green] No SQL to execute")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 10. Parse into statements
        statements = parse_sql_statements(sql)

        if not statements:
            console.print("\n[yellow]No SQL statements to execute.[/yellow]")
            return _create_empty_result(target_env, latest_snapshot_version)

        # 11. Show preview
        console.print("\n[bold]SQL Preview:[/bold]")
        console.print("─" * 60)

        # Show SQL with syntax highlighting (first 10 lines)
        sql_lines = sql.strip().split("\n")
        preview_lines = sql_lines[:10]
        for line in preview_lines:
            console.print(f"  {line}")
        if len(sql_lines) > 10:
            remaining = len(sql_lines) - 10
            console.print(f"  ... ({remaining} more lines)")

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

        # 17. Execute statements FIRST (this creates catalog if autoCreateCatalog: true)
        console.print("\n[cyan]Executing SQL statements...[/cyan]")
        result = executor.execute_statements(statements, config)

        # 18. Initialize deployment tracker AFTER catalog exists
        deployment_catalog = env_config["topLevelName"]
        # Cast to UnitySQLExecutor to access client attribute
        unity_executor = cast(UnitySQLExecutor, executor)
        tracker = DeploymentTracker(unity_executor.client, deployment_catalog, warehouse_id)

        console.print(
            f"\n[cyan]Setting up deployment tracking in {deployment_catalog}.schematic...[/cyan]"
        )
        tracker.ensure_tracking_schema(
            auto_create=env_config.get("autoCreateSchematicSchema", True)
        )
        console.print("[green]✓[/green] Tracking schema ready")

        # 19. Start deployment tracking
        deployment_id = f"deploy_{uuid4().hex[:8]}"
        tracker.start_deployment(
            deployment_id=deployment_id,
            environment=target_env,
            snapshot_version=latest_snapshot_version,
            project_name=project_name,
            provider_type=provider.info.id,
            provider_version=provider.info.version,
            schematic_version="0.2.0",
        )

        # 20. Track individual operations from diff
        for i, (op, stmt_result) in enumerate(zip(diff_operations, result.statement_results)):
            tracker.record_operation(
                deployment_id=deployment_id,
                op=op,
                sql_stmt=stmt_result.sql,
                result=stmt_result,
                execution_order=i + 1,
            )

        # 21. Complete deployment tracking in database
        tracker.complete_deployment(deployment_id, result, result.error_message)

        # 22. Write deployment record to local project.json
        deployment_record = {
            "id": deployment_id,
            "environment": target_env,
            "version": latest_snapshot_version,
            "fromVersion": deployed_version,
            "ts": datetime.now(UTC).isoformat(),
            "status": result.status,
            "executionTimeMs": result.total_execution_time_ms,
            "statementCount": result.total_statements,
            "successfulStatements": result.successful_statements,
            "failedStatementIndex": result.failed_statement_index,
            "opsApplied": [op.id for op in diff_operations],
        }

        write_deployment(workspace, deployment_record)

        # 23. Show results
        console.print()
        console.print("─" * 60)

        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Deployed {latest_snapshot_version} to {target_env} "
                f"({result.successful_statements} statements, {exec_time:.2f}s)[/green]"
            )
            console.print(f"[green]✓ Deployment recorded: {deployment_id}[/green]")
            console.print("[green]✓ Local record saved to project.json[/green]")
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
            console.print("[yellow]Local record saved to project.json[/yellow]")

        return result

    except Exception as e:
        console.print(f"\n[red]✗ Apply failed: {e}[/red]")
        raise ApplyError(str(e)) from e


def parse_sql_statements(sql: str) -> list[str]:
    """Parse SQL into individual statements

    Splits SQL by semicolons, handling comments and multi-line statements.

    Args:
        sql: SQL text to parse

    Returns:
        List of individual SQL statements
    """
    statements = []

    # Remove comments
    sql_no_comments = re.sub(r"--[^\n]*", "", sql)

    # Split by semicolon
    raw_statements = sql_no_comments.split(";")

    for stmt in raw_statements:
        # Clean up whitespace
        stmt = stmt.strip()

        # Skip empty statements
        if not stmt:
            continue

        statements.append(stmt)

    return statements


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
