"""
SQL utilities - provider-agnostic helpers for SQL script handling.

Single source of truth for splitting SQL text into statements (e.g. for DDL
import or future "run SQL file" features). No provider or dialect dependency.
"""


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
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue

        for char in line:
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            if char == ";" and not in_single_quote and not in_double_quote:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
        current.append("\n")

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements
