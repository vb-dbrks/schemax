"""
SQL utilities - provider-agnostic helpers for SQL script handling.

Single source of truth for splitting SQL text into statements (e.g. for DDL
import or future "run SQL file" features). No provider or dialect dependency.
"""


def _process_line(
    line: str,
    current: list[str],
    in_single_quote: bool,
    in_double_quote: bool,
) -> tuple[list[str], bool, bool, list[str], bool]:
    """Process one line; update quote state and collect any completed statements.

    Returns:
        (updated_current, new_in_single, new_in_double, completed_statements, saw_statement_end)
    """
    completed: list[str] = []
    new_single = in_single_quote
    new_double = in_double_quote
    new_current = list(current)

    for char in line:
        if char == "'" and not new_double:
            new_single = not new_single
        elif char == '"' and not new_single:
            new_double = not new_double

        if char == ";" and not new_single and not new_double:
            statement = "".join(new_current).strip()
            if statement:
                completed.append(statement)
            return ([], new_single, new_double, completed, True)
        new_current.append(char)

    return (new_current, new_single, new_double, completed, False)


def split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL script into statements while preserving quoted semicolons.

    Treats semicolons inside single- or double-quoted strings as part of the
    statement. Skips lines that are entirely comment (start with --).
    Empty statements are not included.

    Args:
        sql_text: Raw SQL script content (e.g. from a file).

    Returns:
        List of non-empty statement strings, in order.
    """
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        new_current, in_single_quote, in_double_quote, completed, saw_end = _process_line(
            line, current, in_single_quote, in_double_quote
        )
        current = new_current
        statements.extend(completed)
        if not saw_end:
            current.append("\n")

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements
