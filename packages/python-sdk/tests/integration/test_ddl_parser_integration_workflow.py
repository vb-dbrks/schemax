"""True integration tests for Unity DDL parser via CLI SQL-file import workflows."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from schemax.cli import cli
from schemax.core.storage import ensure_project_file


def _invoke_cli(*args: str, cwd: Path | None = None):
    runner = CliRunner()
    if cwd is None:
        return runner.invoke(cli, list(args), catch_exceptions=True)
    original = Path.cwd()
    os.chdir(cwd)
    try:
        return runner.invoke(cli, list(args), catch_exceptions=True)
    finally:
        os.chdir(original)


def _import_sql(
    workspace: Path, sql_text: str, *, dry_run: bool, mode: str = "replace"
) -> dict[str, Any]:
    sql_file = workspace / "ddl_parser_fixture.sql"
    sql_file.write_text(sql_text, encoding="utf-8")
    result = _invoke_cli(
        "import",
        "--json",
        "--from-sql",
        str(sql_file),
        "--mode",
        mode,
        *(["--dry-run"] if dry_run else []),
        str(workspace),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["status"] == "success"
    summary = envelope["data"]["summary"]
    assert isinstance(summary, dict)
    return summary


def _workspace_state(workspace: Path) -> dict[str, Any]:
    result = _invoke_cli("workspace-state", "--json", str(workspace))
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["status"] == "success"
    payload = envelope["data"]
    assert isinstance(payload, dict)
    return payload


def _find_catalog(state: dict[str, Any], catalog_name: str) -> dict[str, Any]:
    catalogs = state.get("catalogs", [])
    for catalog in catalogs:
        if catalog.get("name") == catalog_name:
            return catalog
    raise AssertionError(f"Catalog not found: {catalog_name}")


@pytest.mark.integration
def test_ddl_parser_cli_import_comprehensive_pack(temp_workspace: Path) -> None:
    """Covers command fallback, ALTER lifecycle, comments/tags, and report+continue behavior."""
    ensure_project_file(temp_workspace, provider_id="unity")

    summary = _import_sql(
        temp_workspace,
        """
        CREATE CATALOG IF NOT EXISTS ddl_cov_cat WITH MANAGED LOCATION 'abfss://placeholder/cat' COMMENT 'catalog bootstrap';
        CREATE SCHEMA IF NOT EXISTS ddl_cov_cat.core MANAGED LOCATION 'abfss://placeholder/schema' COMMENT 'schema bootstrap';
        CREATE TABLE ddl_cov_cat.core.orders (
            order_id BIGINT NOT NULL,
            created_at TIMESTAMP,
            amount DECIMAL(18,2)
        ) USING DELTA;
        ALTER TABLE ddl_cov_cat.core.orders ADD COLUMN note STRING, ADD COLUMN priority INT;
        ALTER TABLE ddl_cov_cat.core.orders RENAME COLUMN note TO order_note;
        ALTER TABLE ddl_cov_cat.core.orders ALTER COLUMN order_note SET NOT NULL;
        ALTER TABLE ddl_cov_cat.core.orders ALTER COLUMN priority TYPE BIGINT;
        ALTER TABLE ddl_cov_cat.core.orders SET TBLPROPERTIES ('delta.appendOnly' = 'true', 'quality' = 'silver');
        ALTER TABLE ddl_cov_cat.core.orders SET TAGS ('domain' = 'sales', 'pii' = 'no');
        ALTER TABLE ddl_cov_cat.core.orders DROP COLUMN IF EXISTS order_note;
        ALTER TABLE ddl_cov_cat.core.orders RENAME TO ddl_cov_cat.core.orders_v2;
        CREATE VIEW ddl_cov_cat.core.orders_view AS SELECT order_id, priority FROM ddl_cov_cat.core.orders_v2;
        COMMENT ON TABLE ddl_cov_cat.core.orders_v2 IS 'orders table comment';
        COMMENT ON VIEW ddl_cov_cat.core.orders_view IS 'orders view comment';
        ALTER SCHEMA ddl_cov_cat.core SET TAGS ('tier' = 'core');
        ALTER CATALOG ddl_cov_cat SET TAGS ('owner' = 'platform');
        COMMENT ON CATALOG ddl_cov_cat IS 'catalog comment updated';
        COMMENT ON SCHEMA ddl_cov_cat.core IS 'schema comment updated';
        SELECT 1;
        CREATE TABLE broken (
        """,
        dry_run=False,
    )

    report = summary["report"]
    assert report["created"]["catalogs"] >= 1
    assert report["created"]["schemas"] >= 1
    assert report["created"]["tables"] >= 1
    assert report["created"]["views"] >= 1
    assert report["skipped"] >= 1
    assert len(report["parse_errors"]) >= 1

    state_payload = _workspace_state(temp_workspace)
    state = state_payload["state"]
    catalog = _find_catalog(state, "ddl_cov_cat")
    assert catalog.get("comment") == "catalog comment updated"
    assert (catalog.get("tags") or {}).get("owner") == "platform"

    schemas = catalog.get("schemas", [])
    core_schema = next(schema for schema in schemas if schema.get("name") == "core")
    assert core_schema.get("comment") == "schema comment updated"
    assert (core_schema.get("tags") or {}).get("tier") == "core"

    orders_table = next(
        table for table in core_schema.get("tables", []) if table.get("name") == "orders_v2"
    )
    assert orders_table.get("comment") == "orders table comment"
    assert (orders_table.get("tags") or {}).get("domain") == "sales"
    assert (orders_table.get("properties") or {}).get("delta.appendOnly") == "true"
    column_names = [column.get("name") for column in orders_table.get("columns", [])]
    assert "order_id" in column_names
    assert "priority" in column_names
    assert "order_note" not in column_names

    orders_view = next(
        view for view in core_schema.get("views", []) if view.get("name") == "orders_view"
    )
    assert orders_view.get("comment") == "orders view comment"
    extracted = orders_view.get("extractedDependencies") or {}
    assert extracted.get("tables") or extracted.get("views")


@pytest.mark.integration
def test_ddl_parser_cli_import_ordering_and_multi_action_pack(temp_workspace: Path) -> None:
    """Locks schema-before-catalog behavior and multi-action ALTER parsing."""
    ensure_project_file(temp_workspace, provider_id="unity")

    summary = _import_sql(
        temp_workspace,
        """
        CREATE SCHEMA main.public;
        CREATE TABLE main.public.events (id INT) USING DELTA;
        ALTER TABLE main.public.events ADD COLUMN c1 STRING, ADD COLUMN c2 INT;
        ALTER TABLE main.public.events RENAME TO main.public.events_v2;
        COMMENT ON TABLE main.public.events_v2 IS 'renamed table';
        CREATE CATALOG main COMMENT 'main catalog';
        COMMENT ON CATALOG main IS 'main catalog override';
        """,
        dry_run=False,
    )

    report = summary["report"]
    assert report["skipped"] == 0
    assert len(report["parse_errors"]) == 0

    state_payload = _workspace_state(temp_workspace)
    state = state_payload["state"]
    main_catalog = _find_catalog(state, "main")
    assert main_catalog.get("comment") == "main catalog override"

    public_schema = next(
        schema for schema in main_catalog.get("schemas", []) if schema.get("name") == "public"
    )
    events_table = next(
        table for table in public_schema.get("tables", []) if table.get("name") == "events_v2"
    )
    column_names = [column.get("name") for column in events_table.get("columns", [])]
    assert set(["id", "c1", "c2"]).issubset(set(column_names))
    assert events_table.get("comment") == "renamed table"
