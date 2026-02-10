"""
Import Command Implementation

Imports existing provider assets into Schematic state/changelog so users can
adopt already-deployed objects instead of starting from an empty model.
"""

from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import uuid4

from rich.console import Console

from schematic.core.deployment import DeploymentTracker
from schematic.core.storage import (
    append_ops,
    create_snapshot,
    get_environment_config,
    load_current_state,
    read_project,
    write_project,
)
from schematic.providers.base.executor import ExecutionConfig, ExecutionResult
from schematic.providers.unity.auth import create_databricks_client

console = Console()


class ImportError(Exception):
    """Raised when import command fails"""

    pass


def import_from_provider(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    catalog: str | None = None,
    schema: str | None = None,
    table: str | None = None,
    dry_run: bool = False,
    adopt_baseline: bool = False,
    catalog_mappings_override: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Import existing assets from provider into local changelog.

    This command is intentionally CLI-first and currently scaffolds the flow.
    Provider-specific discovery implementation is added incrementally.
    """
    state, changelog, provider, _ = load_current_state(workspace, validate=False)

    config = ExecutionConfig(
        target_env=target_env,
        profile=profile,
        warehouse_id=warehouse_id,
        dry_run=dry_run,
        no_interaction=True,
    )

    validation = provider.validate_execution_config(config)
    if not validation.valid:
        errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
        raise ImportError(f"Invalid execution configuration:\n{errors}")

    scope = {
        "catalog": catalog,
        "schema": schema,
        "table": table,
    }

    console.print("[bold]Schematic Import[/bold]")
    console.print("─" * 60)
    console.print(f"[blue]Provider:[/blue] {provider.info.name} v{provider.info.version}")
    console.print(f"[blue]Environment:[/blue] {target_env}")
    console.print(f"[blue]Scope:[/blue] {scope}")

    try:
        discovered_state = provider.discover_state(config=config, scope=scope)
    except NotImplementedError as e:
        raise ImportError(str(e)) from e

    project = read_project(workspace)
    normalized_state, catalog_mappings, mappings_updated = _normalize_discovered_state(
        local_state=state,
        discovered_state=discovered_state,
        project=project,
        target_env=target_env,
        catalog_mappings_override=catalog_mappings_override,
    )

    differ = provider.get_state_differ(
        old_state=state,
        new_state=normalized_state,
        old_operations=changelog.get("ops", []),
        new_operations=[],
    )
    import_ops = differ.generate_diff_operations()
    object_counts = _count_objects(discovered_state)
    op_counts = _summarize_operations(import_ops)

    summary = {
        "provider": provider.info.id,
        "target_env": target_env,
        "scope": scope,
        "object_counts": object_counts,
        "operations_generated": len(import_ops),
        "operation_breakdown": op_counts,
        "dry_run": dry_run,
        "adopt_baseline": adopt_baseline,
        "catalog_mappings": catalog_mappings,
    }

    console.print(
        "[blue]Discovered:[/blue] "
        f"{object_counts['catalogs']} catalog(s), "
        f"{object_counts['schemas']} schema(s), "
        f"{object_counts['tables']} table(s), "
        f"{object_counts['views']} view(s), "
        f"{object_counts['columns']} column(s)"
    )
    if import_ops:
        rendered = ", ".join(f"{k}={v}" for k, v in op_counts.items())
        console.print(f"[blue]Planned operations:[/blue] {len(import_ops)} ({rendered})")
    else:
        console.print("[blue]Planned operations:[/blue] 0")
    if catalog_mappings:
        rendered_mappings = ", ".join(
            f"{logical}->{physical}" for logical, physical in sorted(catalog_mappings.items())
        )
        console.print(f"[blue]Catalog mappings:[/blue] {rendered_mappings}")

    if dry_run:
        console.print(
            f"[yellow]Dry-run:[/yellow] generated {len(import_ops)} operation(s); no files modified."
        )
        console.print(
            "[dim]Next:[/dim] Run without --dry-run to write import operations to changelog."
        )
        return summary

    if mappings_updated:
        _write_catalog_mappings(project, target_env, catalog_mappings)
        write_project(workspace, project)
        console.print(
            f"[green]✓[/green] Updated catalog mappings for environment '{target_env}'"
        )

    if import_ops:
        append_ops(workspace, import_ops)
        console.print(f"[green]✓[/green] Imported {len(import_ops)} operation(s) into changelog")
    else:
        console.print("[green]✓[/green] No import operations required")

    if adopt_baseline:
        if provider.info.id != "unity":
            raise ImportError("Baseline adoption is currently supported for Unity provider only")

        snapshot_version = project.get("latestSnapshot")
        if import_ops or not snapshot_version:
            _, snapshot = create_snapshot(
                workspace,
                name=f"Imported baseline for {target_env}",
                comment=(
                    f"Imported existing provider assets for {target_env} and "
                    "adopted as deployment baseline."
                ),
                tags=["import", "baseline"],
            )
            snapshot_version = snapshot["version"]
            console.print(f"[green]✓[/green] Created baseline snapshot: {snapshot_version}")
        else:
            console.print(
                f"[blue]Using existing latest snapshot for baseline:[/blue] {snapshot_version}"
            )

        if not snapshot_version:
            raise ImportError("Could not determine snapshot version for baseline adoption")

        deployment_id = _adopt_baseline_deployment(
            workspace=workspace,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            provider_id=provider.info.id,
            provider_version=provider.info.version,
            snapshot_version=snapshot_version,
        )
        console.print(f"[green]✓[/green] Adopted baseline deployment: {deployment_id}")
        summary["snapshot_version"] = snapshot_version
        summary["deployment_id"] = deployment_id
        console.print(
            f"[dim]Next:[/dim] Run `schematic apply --target {target_env} --profile {profile} "
            f"--warehouse-id {warehouse_id} --dry-run` to verify zero-diff."
        )
    else:
        console.print(
            "[dim]Next:[/dim] Create a snapshot and run apply when you are ready to deploy."
        )

    return summary


def _adopt_baseline_deployment(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    provider_id: str,
    provider_version: str,
    snapshot_version: str,
) -> str:
    """Write a successful deployment baseline entry without executing DDL."""
    project = read_project(workspace)
    env_config = get_environment_config(project, target_env)
    deployment_catalog = env_config["topLevelName"]
    auto_create_schema = env_config.get("autoCreateSchematicSchema", True)

    client = create_databricks_client(profile)
    tracker = DeploymentTracker(client, deployment_catalog, warehouse_id)
    tracker.ensure_tracking_schema(auto_create=auto_create_schema)

    latest_success = tracker.get_latest_deployment(target_env)
    from_snapshot_version = latest_success.get("version") if latest_success else None
    previous_deployment_id = tracker.get_most_recent_deployment_id(target_env)

    deployment_id = f"deploy_import_{uuid4().hex[:8]}"
    tracker.start_deployment(
        deployment_id=deployment_id,
        environment=target_env,
        snapshot_version=snapshot_version,
        project_name=project.get("name", "unknown"),
        provider_type=provider_id,
        provider_version=provider_version,
        schematic_version="0.2.0",
        from_snapshot_version=from_snapshot_version,
        previous_deployment_id=previous_deployment_id,
    )

    tracker.complete_deployment(
        deployment_id=deployment_id,
        result=ExecutionResult(
            deployment_id=deployment_id,
            total_statements=0,
            successful_statements=0,
            failed_statement_index=None,
            statement_results=[],
            total_execution_time_ms=0,
            status="success",
            error_message=None,
        ),
    )
    return deployment_id


def _count_objects(state: dict[str, Any]) -> dict[str, int]:
    catalogs = state.get("catalogs", [])
    catalog_count = len(catalogs)
    schema_count = 0
    table_count = 0
    view_count = 0
    column_count = 0

    for catalog in catalogs:
        schemas = catalog.get("schemas", [])
        schema_count += len(schemas)
        for schema in schemas:
            tables = schema.get("tables", [])
            views = schema.get("views", [])
            table_count += len(tables)
            view_count += len(views)
            for table in tables:
                column_count += len(table.get("columns", []))

    return {
        "catalogs": catalog_count,
        "schemas": schema_count,
        "tables": table_count,
        "views": view_count,
        "columns": column_count,
    }


def _summarize_operations(ops: list[Any]) -> dict[str, int]:
    op_counter = Counter(op.op if hasattr(op, "op") else op.get("op", "unknown") for op in ops)
    return dict(sorted(op_counter.items()))


def _normalize_discovered_state(
    local_state: dict[str, Any],
    discovered_state: dict[str, Any],
    project: dict[str, Any],
    target_env: str,
    catalog_mappings_override: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, str], bool]:
    """Normalize discovered physical names to logical names for stable import diffs.

    Returns:
        - normalized state aligned to local logical naming
        - resulting catalog mappings for target environment (logical -> physical)
        - whether mappings changed
    """
    env_cfg = get_environment_config(project, target_env)
    top_level_name = env_cfg.get("topLevelName")
    existing_mappings = _read_catalog_mappings(env_cfg)
    catalog_mappings = dict(existing_mappings)
    if catalog_mappings_override:
        for logical, physical in catalog_mappings_override.items():
            catalog_mappings[str(logical)] = str(physical)
    reverse_mappings = _build_reverse_mappings(catalog_mappings)

    local_catalogs = (local_state or {}).get("catalogs", []) or []
    local_catalog_by_name = {
        str(cat.get("name")): cat for cat in local_catalogs if isinstance(cat, dict) and cat.get("name")
    }

    normalized = deepcopy(discovered_state or {})
    normalized_catalogs = []
    for discovered_catalog in normalized.get("catalogs", []) or []:
        if not isinstance(discovered_catalog, dict):
            continue

        physical_name = str(discovered_catalog.get("name", ""))
        if not physical_name:
            continue

        logical_name = reverse_mappings.get(physical_name)
        if logical_name is None:
            if (
                top_level_name
                and physical_name == top_level_name
                and len(local_catalog_by_name) == 1
            ):
                logical_name = next(iter(local_catalog_by_name.keys()))
            else:
                logical_name = physical_name

            existing_physical = catalog_mappings.get(logical_name)
            if existing_physical and existing_physical != physical_name:
                    raise ImportError(
                    "Catalog mapping conflict for logical catalog "
                    f"'{logical_name}': '{existing_physical}' vs '{physical_name}'"
                )

            catalog_mappings[logical_name] = physical_name
            reverse_mappings[physical_name] = logical_name

        local_catalog = local_catalog_by_name.get(logical_name)
        normalized_catalog = _normalize_catalog_tree(discovered_catalog, local_catalog)
        normalized_catalog["name"] = logical_name
        normalized_catalogs.append(normalized_catalog)

    normalized["catalogs"] = normalized_catalogs
    return normalized, catalog_mappings, catalog_mappings != existing_mappings


def _normalize_catalog_tree(
    discovered_catalog: dict[str, Any],
    local_catalog: dict[str, Any] | None,
) -> dict[str, Any]:
    """Re-use local IDs by matching object names to avoid synthetic add/drop churn."""
    normalized_catalog = deepcopy(discovered_catalog)
    if local_catalog and local_catalog.get("id"):
        normalized_catalog["id"] = local_catalog["id"]

    local_schemas = (local_catalog or {}).get("schemas", []) or []
    local_schema_by_name = {
        str(schema.get("name")): schema
        for schema in local_schemas
        if isinstance(schema, dict) and schema.get("name")
    }

    normalized_schemas = []
    for schema in normalized_catalog.get("schemas", []) or []:
        if not isinstance(schema, dict):
            continue
        local_schema = local_schema_by_name.get(str(schema.get("name", "")))
        normalized_schema = deepcopy(schema)
        if local_schema and local_schema.get("id"):
            normalized_schema["id"] = local_schema["id"]
        _normalize_schema_tree(normalized_schema, local_schema)
        normalized_schemas.append(normalized_schema)

    normalized_catalog["schemas"] = normalized_schemas
    return normalized_catalog


def _normalize_schema_tree(
    normalized_schema: dict[str, Any],
    local_schema: dict[str, Any] | None,
) -> None:
    local_tables = (local_schema or {}).get("tables", []) or []
    local_table_by_name = {
        str(table.get("name")): table
        for table in local_tables
        if isinstance(table, dict) and table.get("name")
    }
    normalized_tables = []
    for table in normalized_schema.get("tables", []) or []:
        if not isinstance(table, dict):
            continue
        local_table = local_table_by_name.get(str(table.get("name", "")))
        normalized_table = deepcopy(table)
        if local_table and local_table.get("id"):
            normalized_table["id"] = local_table["id"]
        _normalize_table_tree(normalized_table, local_table)
        normalized_tables.append(normalized_table)
    normalized_schema["tables"] = normalized_tables

    local_views = (local_schema or {}).get("views", []) or []
    local_view_by_name = {
        str(view.get("name")): view
        for view in local_views
        if isinstance(view, dict) and view.get("name")
    }
    normalized_views = []
    for view in normalized_schema.get("views", []) or []:
        if not isinstance(view, dict):
            continue
        local_view = local_view_by_name.get(str(view.get("name", "")))
        normalized_view = deepcopy(view)
        if local_view and local_view.get("id"):
            normalized_view["id"] = local_view["id"]
        normalized_views.append(normalized_view)
    normalized_schema["views"] = normalized_views


def _normalize_table_tree(
    normalized_table: dict[str, Any],
    local_table: dict[str, Any] | None,
) -> None:
    local_columns = (local_table or {}).get("columns", []) or []
    local_column_by_name = {
        str(column.get("name")): column
        for column in local_columns
        if isinstance(column, dict) and column.get("name")
    }
    normalized_columns = []
    for column in normalized_table.get("columns", []) or []:
        if not isinstance(column, dict):
            continue
        local_column = local_column_by_name.get(str(column.get("name", "")))
        normalized_column = deepcopy(column)
        if local_column and local_column.get("id"):
            normalized_column["id"] = local_column["id"]
        normalized_columns.append(normalized_column)
    normalized_table["columns"] = normalized_columns


def _read_catalog_mappings(env_cfg: dict[str, Any]) -> dict[str, str]:
    raw = env_cfg.get("catalogMappings") or {}
    if not isinstance(raw, dict):
        raise ImportError("Environment catalogMappings must be an object mapping logical->physical")
    return {str(k): str(v) for k, v in raw.items()}


def _write_catalog_mappings(
    project: dict[str, Any], target_env: str, mappings: dict[str, str]
) -> None:
    environments = project.setdefault("provider", {}).setdefault("environments", {})
    env_cfg = environments.setdefault(target_env, {})
    env_cfg["catalogMappings"] = dict(sorted(mappings.items()))


def _build_reverse_mappings(mappings: dict[str, str]) -> dict[str, str]:
    reverse: dict[str, str] = {}
    for logical, physical in mappings.items():
        existing = reverse.get(physical)
        if existing and existing != logical:
            raise ImportError(
                "Invalid catalogMappings: physical catalog "
                f"'{physical}' is mapped from multiple logical catalogs ('{existing}', '{logical}')"
            )
        reverse[physical] = logical
    return reverse
