"""
Diff Command - Generate diff operations between snapshots

Compares two snapshot versions and shows the operations needed to transform
one into the other. Useful for understanding changes between versions and
planning environment deployments.
"""

from pathlib import Path
from typing import Any, Protocol

from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from schemax.commands.sql import SQLGenerationError, build_catalog_mapping
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.operations import Operation
from schemax.providers.base.provider import Provider
from schemax.providers.registry import ProviderRegistry

console = Console()


class DiffError(Exception):
    """Raised when diff command fails"""


class _WorkspaceRepoPort(Protocol):
    def read_project(self, *, workspace: Path) -> dict[str, Any]: ...

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]: ...

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]: ...


class _DiffWorkspaceRepository:
    """Repository adapter for diff workflow."""

    def __init__(self) -> None:
        self._repository = WorkspaceRepository()

    def read_project(self, *, workspace: Path) -> dict[str, Any]:
        return self._repository.read_project(workspace=workspace)

    def read_snapshot(self, *, workspace: Path, version: str) -> dict[str, Any]:
        return self._repository.read_snapshot(workspace=workspace, version=version)

    def get_environment_config(
        self, *, project: dict[str, Any], environment: str
    ) -> dict[str, Any]:
        return self._repository.get_environment_config(project=project, environment=environment)


def generate_diff(
    workspace: Path,
    from_version: str,
    to_version: str,
    show_sql: bool = False,
    show_details: bool = False,
    target_env: str | None = None,
    workspace_repo: _WorkspaceRepoPort | None = None,
) -> list[Operation]:
    """Generate diff operations between two snapshot versions

    Args:
        workspace: Workspace directory
        from_version: Source snapshot version
        to_version: Target snapshot version
        show_sql: Whether to display generated SQL
        show_details: Whether to display detailed operation payloads
        target_env: Target environment for catalog name mapping (optional)
        workspace_repo: Optional repository override for tests/injection

    Returns:
        List of operations representing the diff

    Raises:
        DiffError: If diff generation fails
    """
    repository: _WorkspaceRepoPort = workspace_repo or _DiffWorkspaceRepository()
    old_snap, new_snap = _load_snapshots(
        workspace=workspace,
        from_version=from_version,
        to_version=to_version,
        workspace_repo=repository,
    )

    project = repository.read_project(workspace=workspace)
    provider_id = project["provider"]["type"]
    provider = ProviderRegistry.get(provider_id)
    if not provider:
        raise DiffError(f"Provider '{provider_id}' not found in registry")

    catalog_mapping = _resolve_diff_catalog_mapping(
        workspace=workspace,
        target_env=target_env,
        state=new_snap["state"],
        project=project,
        workspace_repo=repository,
    )
    if target_env and catalog_mapping is not None:
        console.print(f"  [blue]Environment:[/blue] {target_env}")
        console.print(f"  [blue]Catalog mapping:[/blue] {catalog_mapping}")

    console.print(f"[bold]Generating diff: {from_version} → {to_version}[/bold]")
    differ = provider.get_state_differ(
        old_snap["state"],
        new_snap["state"],
        old_snap.get("operations", []),
        new_snap.get("operations", []),
    )
    operations = differ.generate_diff_operations()

    console.print()
    console.print("[bold green]✓[/bold green] Diff generated successfully")
    console.print(f"  Operations: {len(operations)}")
    console.print()

    if not operations:
        console.print("[yellow]No changes detected between versions[/yellow]")
        return operations

    _render_operations_table(operations, show_details)

    if show_sql:
        _render_diff_sql(provider, new_snap["state"], operations, catalog_mapping)

    return operations


def _load_snapshots(
    *,
    workspace: Path,
    from_version: str,
    to_version: str,
    workspace_repo: _WorkspaceRepoPort,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load and validate both snapshot files; raise DiffError on failure."""
    if from_version == to_version:
        raise DiffError(
            f"Cannot diff the same version with itself: {from_version}\n"
            "Please provide different snapshot versions."
        )
    try:
        console.print("[bold]Loading snapshots...[/bold]")
        old_snap = workspace_repo.read_snapshot(workspace=workspace, version=from_version)
        new_snap = workspace_repo.read_snapshot(workspace=workspace, version=to_version)
    except FileNotFoundError as err:
        msg = str(err)
        if from_version in msg:
            raise DiffError(
                f"Source snapshot not found: {from_version}\n"
                f"Check that the snapshot exists in .schemax/snapshots/{from_version}.json"
            ) from err
        if to_version in msg:
            raise DiffError(
                f"Target snapshot not found: {to_version}\n"
                f"Check that the snapshot exists in .schemax/snapshots/{to_version}.json"
            ) from err
        raise DiffError(f"Snapshot file not found: {err}") from err

    if "state" not in old_snap:
        raise DiffError(f"Invalid snapshot structure: {from_version} is missing 'state' field")
    if "state" not in new_snap:
        raise DiffError(f"Invalid snapshot structure: {to_version} is missing 'state' field")

    console.print(f"  [green]✓[/green] {from_version}")
    console.print(f"  [green]✓[/green] {to_version}")
    return old_snap, new_snap


def _resolve_diff_catalog_mapping(
    *,
    workspace: Path,
    target_env: str | None,
    state: dict[str, Any],
    project: dict[str, Any],
    workspace_repo: _WorkspaceRepoPort,
) -> dict[str, str] | None:
    """Build catalog mapping for target environment, or None if not specified."""
    del workspace
    if not target_env:
        return None
    env_config = workspace_repo.get_environment_config(project=project, environment=target_env)
    try:
        return build_catalog_mapping(state, env_config)
    except SQLGenerationError as err:
        raise DiffError(str(err)) from err


def _render_operations_table(operations: list[Operation], show_details: bool) -> None:
    """Build and print the Rich table of diff operations."""
    table = Table(title="Diff Operations", show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("Operation", style="cyan")
    table.add_column("Target", style="green")
    if show_details:
        table.add_column("Details", style="yellow")

    for i, operation in enumerate(operations, 1):
        op_dict = operation if isinstance(operation, dict) else operation.model_dump()
        row = [
            str(i),
            op_dict["op"],
            op_dict["target"],
        ]
        if show_details:
            payload = op_dict.get("payload", {})
            details = ", ".join(f"{k}={v}" for k, v in payload.items() if v is not None)
            row.append(details[:50] + "..." if len(details) > 50 else details)
        table.add_row(*row)

    console.print(table)


def _build_catalog_mapping(state: dict, env_config: dict) -> dict[str, str]:
    """Delegate to shared SQL mapping builder (used by rollback and tests)."""
    return build_catalog_mapping(state, env_config)


def _render_diff_sql(
    provider: Provider,
    state: dict,
    operations: list[Operation],
    catalog_mapping: dict[str, str] | None,
) -> None:
    """Generate SQL from diff operations and print with syntax highlighting."""
    console.print()
    console.print("[bold]Generated SQL:[/bold]")
    console.print()
    sql_gen = provider.get_sql_generator(state, catalog_mapping)
    sql = sql_gen.generate_sql(operations)
    syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
    console.print(syntax)
