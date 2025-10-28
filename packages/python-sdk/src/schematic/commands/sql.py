"""
SQL Generation Command

Generates SQL migration scripts from schema changes in the changelog.
"""

from pathlib import Path
from typing import Dict, Optional

from rich.console import Console
from rich.syntax import Syntax

from ..providers.base.operations import Operation
from ..storage_v4 import get_environment_config, load_current_state, read_project

console = Console()


class SQLGenerationError(Exception):
    """Raised when SQL generation fails"""

    pass


def build_catalog_mapping(state: dict, env_config: dict) -> Dict[str, str]:
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
    raise SQLGenerationError(
        f"Multi-catalog projects are not yet supported. "
        f"Found {len(catalogs)} catalogs: {', '.join(c['name'] for c in catalogs)}. "
        "For now, please use a single catalog per project."
    )


def generate_sql_migration(
    workspace: Path,
    output: Optional[Path] = None,
    from_version: Optional[str] = None,
    to_version: Optional[str] = None,
    target_env: Optional[str] = None,
) -> str:
    """Generate SQL migration script from schema changes

    Generates SQL DDL statements from operations in the changelog.
    Can optionally output to a file or print to stdout with syntax highlighting.

    Args:
        workspace: Path to Schematic workspace
        output: Optional output file path
        from_version: Optional starting version for SQL generation
        to_version: Optional ending version for SQL generation
        target_env: Optional target environment (for catalog name mapping)

    Returns:
        Generated SQL string

    Raises:
        SQLGenerationError: If SQL generation fails
    """
    try:
        # Load project and state
        project = read_project(workspace)
        state, changelog, provider = load_current_state(workspace)

        console.print(f"[blue]Provider:[/blue] {provider.info.name}")
        console.print(f"[blue]Operations:[/blue] {len(changelog['ops'])}")

        # Build catalog name mapping if target environment specified
        catalog_mapping = {}
        if target_env:
            env_config = get_environment_config(project, target_env)
            console.print(f"[blue]Target Environment:[/blue] {target_env}")
            console.print(f"[blue]Physical Catalog:[/blue] {env_config['topLevelName']}")
            catalog_mapping = build_catalog_mapping(state, env_config)

        # TODO: Handle version ranges
        # For now, generate SQL for all changelog ops
        ops_to_process = changelog["ops"]

        if not ops_to_process:
            console.print("[yellow]No operations to generate SQL for[/yellow]")
            return ""

        # Convert to Operation objects
        operations = [Operation(**op) for op in ops_to_process]

        # Generate SQL using provider's SQL generator with catalog mapping
        generator = provider.get_sql_generator(state, catalog_mapping)
        sql_output = generator.generate_sql(operations)

        if output:
            # Write to file
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(sql_output)
            console.print(f"[green]✓[/green] SQL written to {output}")
        else:
            # Print to stdout with syntax highlighting
            syntax = Syntax(sql_output, "sql", theme="monokai", line_numbers=False)
            console.print(syntax)

        return sql_output

    except FileNotFoundError as e:
        raise SQLGenerationError(f"Project files not found: {e}") from e
    except Exception as e:
        raise SQLGenerationError(f"Failed to generate SQL: {e}") from e
