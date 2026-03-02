"""Architecture fitness checks for provider-layer boundaries."""

from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_MODULE_PREFIXES = (
    "schemax.providers.unity",
    "schemax.core.storage",
)


def _iter_target_files(root: Path) -> list[Path]:
    """Return CLI and application Python modules to enforce import rules."""
    files = [root / "src" / "schemax" / "cli.py"]
    app_dir = root / "src" / "schemax" / "application"
    if app_dir.exists():
        files.extend(sorted(app_dir.rglob("*.py")))
    return files


def _forbidden_imports(path: Path) -> list[str]:
    """Collect forbidden import lines in one module."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    hits: list[str] = []

    for node in ast.walk(tree):
        violation = _import_violation(node)
        if violation:
            hits.append(violation)
    return hits


def _import_violation(node: ast.AST) -> str | None:
    """Return import violation string for AST node, or None."""
    if isinstance(node, ast.Import):
        return _import_names_violation(node)
    if isinstance(node, ast.ImportFrom):
        return _from_import_violation(node)
    return None


def _import_names_violation(node: ast.Import) -> str | None:
    """Check `import x` nodes for forbidden imports."""
    for alias in node.names:
        if alias.name.startswith(FORBIDDEN_MODULE_PREFIXES):
            return f"import {alias.name}"
    return None


def _from_import_violation(node: ast.ImportFrom) -> str | None:
    """Check `from x import y` nodes for forbidden imports."""
    module_name = node.module or ""
    if module_name.startswith(FORBIDDEN_MODULE_PREFIXES):
        return f"from {module_name} import ..."
    return None


def test_cli_and_application_import_boundaries() -> None:
    """Non-provider layers must not couple to provider impl or legacy storage modules."""
    repo_root = Path(__file__).resolve().parents[2]
    violations: dict[str, list[str]] = {}

    for file_path in _iter_target_files(repo_root):
        forbidden = _forbidden_imports(file_path)
        if forbidden:
            violations[str(file_path.relative_to(repo_root))] = forbidden

    assert not violations, f"Forbidden Unity imports detected: {violations}"
