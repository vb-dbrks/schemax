"""
Validation Command

Validates Schematic project files and state structure.
"""

from pathlib import Path

from rich.console import Console

from ..storage_v4 import load_current_state, read_project

console = Console()


class ValidationError(Exception):
    """Raised when validation fails"""

    pass


def validate_project(workspace: Path) -> bool:
    """Validate Schematic project files

    Validates project.json, changelog.json, and state structure.
    Displays summary of project configuration and statistics.

    Args:
        workspace: Path to Schematic workspace

    Returns:
        True if validation passes

    Raises:
        ValidationError: If validation fails
    """
    try:
        # Try to load project and changelog
        console.print("Validating project files...")
        project = read_project(workspace)
        console.print(f"  [green]✓[/green] project.json (version {project['version']})")

        state, changelog, provider = load_current_state(workspace)
        console.print(f"  [green]✓[/green] changelog.json ({len(changelog['ops'])} operations)")

        # Validate state using provider
        validation = provider.validate_state(state)
        if not validation.valid:
            console.print("[red]✗ State validation failed:[/red]")
            for error in validation.errors:
                console.print(f"  - {error.field}: {error.message}")
            raise ValidationError("State validation failed")

        console.print("  [green]✓[/green] State structure valid")

        # Display summary
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

        console.print("\n[green]✓ Schema files are valid[/green]")

        return True

    except FileNotFoundError as e:
        raise ValidationError(f"Project files not found: {e}") from e
    except Exception as e:
        raise ValidationError(f"Validation failed: {e}") from e
