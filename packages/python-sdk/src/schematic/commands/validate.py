"""
Validation Command

Validates Schematic project files and state structure.
"""

from pathlib import Path
from typing import Any

from rich.console import Console

from schematic.core.storage import load_current_state, read_project

console = Console()


class ValidationError(Exception):
    """Raised when validation fails"""

    pass


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
        # Ops are already Operation objects from load_current_state()
        # No conversion needed - storage layer handles this
        if not ops:
            return errors, warnings

        # Get SQL generator to build dependency graph
        generator = provider.get_sql_generator(state=state)

        # Try to build dependency graph
        graph = generator._build_dependency_graph(ops)

        # Check for circular dependencies
        cycles = graph.detect_cycles()

        if cycles:
            for cycle in cycles:
                # Get names from generator's id_name_map
                cycle_names = []
                for node_id in cycle:
                    name = generator.id_name_map.get(node_id, node_id)
                    cycle_names.append(name)
                cycle_str = " → ".join(cycle_names)
                errors.append(f"Circular dependency: {cycle_str}")

        # Check for invalid hierarchy (e.g., schema depending on table)
        hierarchy_warnings = graph.validate_dependencies()
        warnings.extend(hierarchy_warnings)

    except Exception as e:
        import traceback

        warnings.append(f"Could not validate dependencies: {e}")
        # Add traceback for debugging
        warnings.append(f"Traceback: {traceback.format_exc()[:200]}")

    return errors, warnings


def validate_project(workspace: Path, json_output: bool = False) -> bool:
    """Validate Schematic project files

    Validates project.json, changelog.json, and state structure.
    Displays summary of project configuration and statistics.

    Args:
        workspace: Path to Schematic workspace
        json_output: If True, output results as JSON instead of rich console

    Returns:
        True if validation passes

    Raises:
        ValidationError: If validation fails
    """
    import json

    try:
        # Try to load project and changelog
        if not json_output:
            console.print("Validating project files...")
        project = read_project(workspace)
        if not json_output:
            console.print(f"  [green]✓[/green] project.json (version {project['version']})")

        state, changelog, provider, _ = load_current_state(workspace, validate=False)
        if not json_output:
            console.print(f"  [green]✓[/green] changelog.json ({len(changelog['ops'])} operations)")

        # Validate state using provider
        validation = provider.validate_state(state)
        if not validation.valid:
            if not json_output:
                console.print("[red]✗ State validation failed:[/red]")
                for error in validation.errors:
                    console.print(f"  - {error.field}: {error.message}")
            raise ValidationError("State validation failed")

        if not json_output:
            console.print("  [green]✓[/green] State structure valid")

        # Validate dependencies (circular dependencies, missing refs, etc.)
        if not json_output:
            console.print("\nValidating dependencies...")

        dep_errors, dep_warnings = validate_dependencies(state, changelog["ops"], provider)

        if dep_errors:
            if not json_output:
                console.print("[red]✗ Dependency validation failed:[/red]")
                for error in dep_errors:
                    console.print(f"  [red]•[/red] {error}")
            raise ValidationError("Circular dependencies detected")

        if not json_output:
            if dep_warnings:
                console.print("[yellow]⚠  Dependency warnings:[/yellow]")
                for warning in dep_warnings:
                    console.print(f"  [yellow]•[/yellow] {warning}")
            else:
                console.print("  [green]✓[/green] No dependency issues detected")

        # Check for stale snapshots
        from .snapshot_rebase import detect_stale_snapshots

        stale = detect_stale_snapshots(workspace)

        # If JSON output requested, output JSON and return
        if json_output:
            result = {
                "valid": True,
                "errors": [],
                "warnings": dep_warnings,
                "staleSnapshots": stale,
            }
            print(json.dumps(result))
            return len(stale) == 0  # Return False if stale snapshots

        # Display summary (console mode only)
        console.print(f"\n[bold]Project:[/bold] {project['name']}")
        console.print(f"[bold]Provider:[/bold] {provider.info.name} v{provider.info.version}")
        console.print(f"[bold]Uncommitted Ops:[/bold] {len(changelog['ops'])}")

        # Provider-specific stats (works for Unity Catalog)
        if "catalogs" in state:
            console.print(f"[bold]Catalogs:[/bold] {len(state['catalogs'])}")
            total_schemas = sum(len(c.get("schemas", [])) for c in state["catalogs"])
            console.print(f"[bold]Schemas:[/bold] {total_schemas}")
            total_tables = sum(
                len(s.get("tables", [])) for c in state["catalogs"] for s in c.get("schemas", [])
            )
            console.print(f"[bold]Tables:[/bold] {total_tables}")

        if stale:
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
                console.print(f"  schematic snapshot rebase {snap['version']}")
            console.print()
            console.print("[yellow]⚠️ Validation passed but snapshots need rebasing[/yellow]")
            return False  # Return False to indicate warning

        console.print("\n[green]✓ Schema files are valid[/green]")

        return True

    except FileNotFoundError as e:
        raise ValidationError(f"Project files not found: {e}") from e
    except Exception as e:
        raise ValidationError(f"Validation failed: {e}") from e
