"""Helpers for invoking SchemaX CLI in tests."""

from __future__ import annotations

import os
from pathlib import Path

from click.testing import CliRunner, Result

from schemax.cli import cli


def invoke_cli(*args: str, cwd: Path | None = None) -> Result:
    """Invoke the Click CLI, optionally from a specific working directory."""
    runner = CliRunner()
    if cwd is None:
        return runner.invoke(cli, list(args), catch_exceptions=True)

    original = Path.cwd()
    os.chdir(cwd)
    try:
        return runner.invoke(cli, list(args), catch_exceptions=True)
    finally:
        os.chdir(original)
