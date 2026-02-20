"""
Integration tests for import from SQL DDL file.
"""

from pathlib import Path

import pytest

from schemax.commands.import_assets import import_from_sql_file
from schemax.core.storage import ensure_project_file, read_changelog


@pytest.mark.integration
def test_import_from_sql_file_diff_dry_run_then_write(temp_workspace: Path) -> None:
    """Import from a SQL file: dry-run then real run appends ops to changelog."""
    ensure_project_file(temp_workspace, provider_id="unity")
    sql_file = temp_workspace / "schema.sql"
    sql_file.write_text(
        "CREATE CATALOG app;\n"
        "CREATE SCHEMA app.public;\n"
        "CREATE TABLE app.public.config (k STRING, v STRING) USING DELTA;\n"
    )
    changelog_before = read_changelog(temp_workspace)
    ops_before = len(changelog_before["ops"])

    summary_dry = import_from_sql_file(
        workspace=temp_workspace,
        sql_path=sql_file,
        mode="diff",
        dry_run=True,
    )
    assert summary_dry["source"] == "sql_file"
    assert summary_dry["dry_run"] is True
    assert summary_dry["operations_generated"] >= 1
    changelog_after_dry = read_changelog(temp_workspace)
    assert len(changelog_after_dry["ops"]) == ops_before

    summary_write = import_from_sql_file(
        workspace=temp_workspace,
        sql_path=sql_file,
        mode="diff",
        dry_run=False,
    )
    assert summary_write["dry_run"] is False
    changelog_final = read_changelog(temp_workspace)
    assert len(changelog_final["ops"]) >= ops_before + 1


@pytest.mark.integration
def test_import_from_sql_file_init_project_if_missing(tmp_path: Path) -> None:
    """When workspace has no .schemax/, import_from_sql_file inits a Unity project."""
    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("CREATE CATALOG c;\n")
    project_file = tmp_path / ".schemax" / "project.json"
    assert not project_file.exists()

    import_from_sql_file(
        workspace=tmp_path,
        sql_path=sql_file,
        mode="diff",
        dry_run=True,
    )
    assert project_file.exists()
