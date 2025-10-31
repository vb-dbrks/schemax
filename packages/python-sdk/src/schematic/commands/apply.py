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
from rich.syntax import Syntax

from ..deployment_tracker import DeploymentTracker
from ..providers.base.executor import ExecutionConfig, ExecutionResult
from ..providers.base.operations import Operation
from ..providers.unity.executor import UnitySQLExecutor
from ..providers.unity.sql_generator import UnitySQLGenerator
from ..storage_v4 import (
    get_environment_config,
    load_current_state,
    read_project,
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
    sql_file: Path | None = None,
) -> ExecutionResult:
    """Apply changes to target environment

    Main entry point for the apply command. Loads project configuration,
    generates or loads SQL, shows preview, confirms with user, and executes
    statements with deployment tracking.

    Args:
        workspace: Path to Schematic workspace
        target_env: Target environment name (e.g., "dev", "prod")
        profile: Databricks profile name
        warehouse_id: SQL warehouse ID
        dry_run: If True, preview without executing
        no_interaction: If True, skip confirmation prompt
        sql_file: Optional SQL file to execute (instead of generating)

    Returns:
        ExecutionResult with deployment details

    Raises:
        ApplyError: If apply fails
    """
    try:
        # 1. Load project (v4 with environment config)
        project = read_project(workspace)
        project_name = project.get("name", "unknown")

        # 2. Get environment configuration
        env_config = get_environment_config(project, target_env)

        console.print("[bold]Schematic Apply[/bold]")
        console.print("─" * 60)

        # 3. Load current state and provider
        state, changelog, provider = load_current_state(workspace)

        console.print(f"[blue]Provider:[/blue] {provider.info.name} v{provider.info.version}")
        console.print(f"[blue]Environment:[/blue] {target_env}")
        console.print(f"[blue]Physical Catalog:[/blue] {env_config['topLevelName']}")
        console.print(f"[blue]Warehouse:[/blue] {warehouse_id}")
        console.print(f"[blue]Profile:[/blue] {profile}")

        # 4. Validate environment-specific policies
        if env_config.get("requireSnapshot", False) and not sql_file:
            if not project.get("latestSnapshot"):
                raise ApplyError(
                    f"Environment '{target_env}' requires a snapshot for deployment, "
                    f"but no snapshots exist. Create a snapshot first with 'schematic snapshot'."
                )
            # Check if there are uncommitted changes
            if changelog["ops"]:
                raise ApplyError(
                    f"Environment '{target_env}' requires a snapshot for deployment, "
                    f"but there are {len(changelog['ops'])} uncommitted operations in changelog. "
                    f"Create a snapshot first or use --sql flag with a specific migration file."
                )

        # 5. Generate or load SQL
        if sql_file:
            console.print(f"[blue]SQL Source:[/blue] {sql_file}")
            sql = sql_file.read_text()
        else:
            console.print(
                f"[blue]SQL Source:[/blue] changelog ({len(changelog['ops'])} operations)"
            )

            if not changelog["ops"]:
                console.print("\n[yellow]No operations in changelog. Nothing to apply.[/yellow]")
                return _create_empty_result()

            # Generate SQL from changelog with catalog name mapping
            operations = [Operation(**op) for op in changelog["ops"]]
            catalog_mapping = _build_catalog_mapping(state, env_config)
            generator = provider.get_sql_generator(state)
            # Cast to UnitySQLGenerator to set catalog_name_mapping
            unity_generator = cast(UnitySQLGenerator, generator)
            unity_generator.catalog_name_mapping = catalog_mapping
            sql = unity_generator.generate_sql(operations)

        # 4. Parse into statements
        statements = parse_sql_statements(sql)

        if not statements:
            console.print("\n[yellow]No SQL statements to execute.[/yellow]")
            return _create_empty_result()

        # 5. Show preview
        console.print("\n[bold]Preview of changes:[/bold]")
        console.print("─" * 60)

        # Show SQL with syntax highlighting
        syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
        console.print(syntax)

        console.print()
        console.print(f"[bold]{len(statements)} statements will be executed.[/bold]")

        # 6. Dry-run mode - stop here
        if dry_run:
            console.print("\n[yellow]✓ Dry-run complete (no changes made)[/yellow]")
            return _create_empty_result()

        # 7. Confirm with user (unless --no-interaction)
        if not no_interaction:
            console.print()
            confirm = Confirm.ask("[bold]Apply changes?[/bold]", default=False)
            if not confirm:
                console.print("[yellow]Apply cancelled[/yellow]")
                return _create_empty_result()

        # 8. Create execution config
        config = ExecutionConfig(
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=False,
            no_interaction=no_interaction,
        )

        # 9. Validate execution config
        validation = provider.validate_execution_config(config)
        if not validation.valid:
            errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
            raise ApplyError(f"Invalid execution configuration:\n{errors}")

        # 10. Get executor and authenticate
        console.print("\n[cyan]Authenticating with Databricks...[/cyan]")
        executor = provider.get_sql_executor(config)
        console.print("[green]✓[/green] Authenticated successfully")

        # 11. Execute statements FIRST (this creates catalog if autoCreateCatalog: true)
        console.print("\n[cyan]Executing SQL statements...[/cyan]")
        result = executor.execute_statements(statements, config)

        # 12. Initialize deployment tracker AFTER catalog exists
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

        # 13. Start deployment tracking
        deployment_id = f"deploy_{uuid4().hex[:8]}"
        tracker.start_deployment(
            deployment_id=deployment_id,
            environment=target_env,
            snapshot_version="changelog",  # TODO: Support snapshots
            project_name=project_name,
            provider_type=provider.info.id,
            provider_version=provider.info.version,
            schematic_version="0.2.0",
        )

        # 14. Track individual operations (if we have them)
        if not sql_file and changelog["ops"]:
            operations = [Operation(**op) for op in changelog["ops"]]
            for i, (op, stmt_result) in enumerate(zip(operations, result.statement_results)):
                tracker.record_operation(
                    deployment_id=deployment_id,
                    op=op,
                    sql_stmt=stmt_result.sql,
                    result=stmt_result,
                    execution_order=i + 1,
                )

        # 15. Complete deployment tracking in database
        tracker.complete_deployment(deployment_id, result, result.error_message)

        # 16. Write deployment record to local project.json
        snapshot_version = project.get("latestSnapshot") or "changelog"
        deployment_record = {
            "id": deployment_id,
            "environment": target_env,
            "version": snapshot_version,
            "ts": datetime.now(UTC).isoformat(),
            "status": result.status,
            "executionTimeMs": result.total_execution_time_ms,
            "statementCount": result.total_statements,
            "successfulStatements": result.successful_statements,
            "failedStatementIndex": result.failed_statement_index,
        }

        if sql_file:
            deployment_record["sqlFile"] = str(sql_file.name)

        write_deployment(workspace, deployment_record)

        # 17. Show results
        console.print()
        console.print("─" * 60)

        if result.status == "success":
            exec_time = result.total_execution_time_ms / 1000
            console.print(
                f"[green]✓ Successfully applied {result.successful_statements} statements "
                f"({exec_time:.2f}s)[/green]"
            )
            console.print(f"[green]✓ Deployment recorded: {deployment_id}[/green]")
            console.print("[green]✓ Local record saved to project.json[/green]")
            console.print()
            console.print("[blue]Deployment Details:[/blue]")
            console.print(f"  Environment: {target_env}")
            console.print(f"  Version: {snapshot_version}")
            console.print("  Status: success")
            console.print(f"  Execution time: {exec_time:.2f}s")
        else:
            failed_idx = result.failed_statement_index or 0
            console.print(f"[red]✗ Deployment failed at statement {failed_idx + 1}[/red]")
            console.print(f"[yellow]✓ {result.successful_statements} statements succeeded[/yellow]")
            if result.error_message:
                console.print(f"[red]Error: {result.error_message}[/red]")
            console.print()
            console.print(f"[blue]Deployment ID:[/blue] {deployment_id}")
            console.print(f"[blue]Environment:[/blue] {target_env}")
            console.print(f"[blue]Version:[/blue] {snapshot_version}")
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


def _create_empty_result() -> ExecutionResult:
    """Create empty execution result for dry-run or cancelled operations

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
