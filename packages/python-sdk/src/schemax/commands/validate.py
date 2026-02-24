"""
Validation Command

Validates SchemaX project files and state structure.
"""

import json
import traceback
from pathlib import Path
from typing import Any

from rich.console import Console

from schemax.core.storage import load_current_state, read_project

from .snapshot_rebase import detect_stale_snapshots

console = Console()


class ValidationError(Exception):
    """Raised when validation fails"""


def _compute_dependency_errors_and_warnings(
    state: Any, ops: list[Any], provider: Any
) -> tuple[list[str], list[str]]:
    """
    Build dependency graph and return cycle errors plus hierarchy warnings.
    Single responsibility: compute only; no I/O or exception handling.
    """
    errors: list[str] = []
    warnings: list[str] = []
    if not ops:
        return errors, warnings

    generator = provider.get_sql_generator(state=state)
    graph = generator.build_dependency_graph(ops)
    cycles = graph.detect_cycles()

    for cycle in cycles:
        cycle_names = [
            generator.id_name_map.get(node_id, node_id) for node_id in cycle
        ]
        cycle_str = " → ".join(cycle_names)
        errors.append(f"Circular dependency: {cycle_str}")

    hierarchy_warnings = graph.validate_dependencies()
    warnings.extend(hierarchy_warnings)
    return errors, warnings


def validate_dependencies(state: Any, ops: list[Any], provider: Any) -> tuple[list[str], list[str]]:
    """
    Validate dependency graph for circular dependencies and missing references.

    Args:
        state: Current schema state
        ops: List of Operation objects (already Pydantic from storage layer)
        provider: Schema provider

    Returns:
        Tuple of (errors list, warnings list)
    """
    errors: list[str] = []
    warnings: list[str] = []
    try:
        errors, warnings = _compute_dependency_errors_and_warnings(state, ops, provider)
    except Exception as e:
        warnings.append(f"Could not validate dependencies: {e}")
        warnings.append(f"Traceback: {traceback.format_exc()[:200]}")
    return errors, warnings


def _load_project_and_state(workspace: Path) -> tuple[dict, Any, dict, Any]:
    """Load project.json and current state. Raises FileNotFoundError if missing."""
    project = read_project(workspace)
    state, changelog, provider, _ = load_current_state(workspace, validate=False)
    return project, state, changelog, provider


def _emit_state_validation_failure(validation: Any, json_output: bool) -> None:
    """Output state validation failure (JSON or console)."""
    if json_output:
        state_errors = [f"{e.field}: {e.message}" for e in validation.errors]
        print(json.dumps({"valid": False, "errors": state_errors, "warnings": []}))
    else:
        console.print("[red]✗ State validation failed:[/red]")
        for error in validation.errors:
            console.print(f"  - {error.field}: {error.message}")


def _emit_dependency_failure(
    dep_errors: list[str], dep_warnings: list[str], json_output: bool
) -> None:
    """Output dependency validation failure (JSON or console)."""
    if json_output:
        print(json.dumps({"valid": False, "errors": dep_errors, "warnings": dep_warnings}))
    else:
        console.print("[red]✗ Dependency validation failed:[/red]")
        for error_msg in dep_errors:
            console.print(f"  [red]•[/red] {error_msg}")


def _print_dependency_status(dep_warnings: list[str], json_output: bool) -> None:
    """Print dependency warnings or success (console only)."""
    if json_output:
        return
    if dep_warnings:
        console.print("[yellow]⚠  Dependency warnings:[/yellow]")
        for warning in dep_warnings:
            console.print(f"  [yellow]•[/yellow] {warning}")
    else:
        console.print("  [green]✓[/green] No dependency issues detected")


def _print_project_summary(project: dict, provider: Any, changelog: dict, state: Any) -> None:
    """Print project/config summary to console."""
    console.print(f"\n[bold]Project:[/bold] {project['name']}")
    console.print(f"[bold]Provider:[/bold] {provider.info.name} v{provider.info.version}")
    console.print(f"[bold]Uncommitted Ops:[/bold] {len(changelog['ops'])}")
    if "catalogs" in state:
        console.print(f"[bold]Catalogs:[/bold] {len(state['catalogs'])}")
        total_schemas = sum(len(c.get("schemas", [])) for c in state["catalogs"])
        console.print(f"[bold]Schemas:[/bold] {total_schemas}")
        total_tables = sum(
            len(s.get("tables", []))
            for c in state["catalogs"]
            for s in c.get("schemas", [])
        )
        console.print(f"[bold]Tables:[/bold] {total_tables}")


def _print_stale_snapshots_remediation(stale: list[dict]) -> None:
    """Print stale snapshot warnings and rebase commands."""
    console.print()
    console.print(f"[yellow]⚠️  Found {len(stale)} stale snapshot(s):[/yellow]")
    for snap in stale:
        console.print(f"  [yellow]{snap['version']}[/yellow]")
        console.print(f"    Current base: {snap['currentBase']}")
        console.print(f"    Should be: {snap['shouldBeBase']}")
        console.print(f"    Missing: {', '.join(snap['missing'])}")
    console.print()
    console.print("[cyan]Run the following commands to fix:[/cyan]")
    for snap in stale:
        console.print(f"  schemax snapshot rebase {snap['version']}")
    console.print()
    console.print("[yellow]⚠️ Validation passed but snapshots need rebasing[/yellow]")


def _run_validation_steps(
    workspace: Path,
    project: dict,
    state: Any,
    changelog: dict,
    provider: Any,
    json_output: bool,
) -> bool:
    """Run state, dependency, and stale-snapshot checks. Returns True if valid."""
    if not json_output:
        console.print("Validating project files...")
        console.print(f"  [green]✓[/green] project.json (version {project['version']})")
        console.print(f"  [green]✓[/green] changelog.json ({len(changelog['ops'])} operations)")

    validation = provider.validate_state(state)
    if not validation.valid:
        _emit_state_validation_failure(validation, json_output)
        raise ValidationError("State validation failed")

    if not json_output:
        console.print("  [green]✓[/green] State structure valid")
        console.print("\nValidating dependencies...")

    dep_errors, dep_warnings = validate_dependencies(state, changelog["ops"], provider)
    if dep_errors:
        _emit_dependency_failure(dep_errors, dep_warnings, json_output)
        raise ValidationError("Circular dependencies detected")

    _print_dependency_status(dep_warnings, json_output)
    stale = detect_stale_snapshots(workspace)

    if json_output:
        result = {
            "valid": True,
            "errors": [],
            "warnings": dep_warnings,
            "staleSnapshots": stale,
        }
        print(json.dumps(result))
        return len(stale) == 0

    _print_project_summary(project, provider, changelog, state)
    if stale:
        _print_stale_snapshots_remediation(stale)
        return False
    console.print("\n[green]✓ Schema files are valid[/green]")
    return True


def validate_project(workspace: Path, json_output: bool = False) -> bool:
    """Validate SchemaX project files.

    Validates project.json, changelog.json, and state structure.
    Displays summary of project configuration and statistics.

    Args:
        workspace: Path to SchemaX workspace
        json_output: If True, output results as JSON instead of rich console

    Returns:
        True if validation passes

    Raises:
        ValidationError: If validation fails
    """
    try:
        project, state, changelog, provider = _load_project_and_state(workspace)
    except FileNotFoundError as e:
        raise ValidationError(f"Project files not found: {e}") from e

    try:
        return _run_validation_steps(workspace, project, state, changelog, provider, json_output)
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Validation failed: {e}") from e
