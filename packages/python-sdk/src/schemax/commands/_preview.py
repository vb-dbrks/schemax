"""Shared CLI preview helpers (SQL statement listing, etc.)."""

from rich.console import Console

console = Console()


def print_sql_statements_preview(
    statements: list[str], title: str = "SQL Preview", action_prompt: str | None = None
) -> None:
    """Print a truncated listing of SQL statements for user confirmation.

    Args:
        statements: List of SQL statement strings.
        title: Section title (e.g. "SQL Preview").
        action_prompt: Optional prompt line after the list (e.g. "Execute N statements?").
    """
    console.print()
    console.print(f"[bold]{title}:[/bold]")
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
    if action_prompt:
        console.print(f"[bold]{action_prompt}[/bold]")
