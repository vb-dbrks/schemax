"""
Import Command Implementation

Imports existing provider assets into Schematic state/changelog so users can
adopt already-deployed objects instead of starting from an empty model.
"""

from collections import Counter
from pathlib import Path
from typing import Any

from rich.console import Console

from schematic.core.storage import (
    append_ops,
    create_snapshot,
    get_environment_config,
    load_current_state,
    read_project,
    write_project,
)
from schematic.providers.base.executor import ExecutionConfig

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
    scope_validation = provider.validate_import_scope(scope)
    if not scope_validation.valid:
        errors = "\n".join([f"  - {e.field}: {e.message}" for e in scope_validation.errors])
        raise ImportError(f"Invalid import scope:\n{errors}")

    console.print("[bold]Schematic Import[/bold]")
    console.print("─" * 60)
    console.print(f"[blue]Provider:[/blue] {provider.info.name} v{provider.info.version}")
    console.print(f"[blue]Environment:[/blue] {target_env}")
    console.print(f"[blue]Scope:[/blue] {scope}")

    try:
        discovered_state = provider.discover_state(config=config, scope=scope)
    except NotImplementedError as e:
        raise ImportError(str(e)) from e
    import_warnings = provider.collect_import_warnings(
        config=config,
        scope=scope,
        discovered_state=discovered_state,
    )

    project = read_project(workspace)
    env_config = get_environment_config(project, target_env)
    try:
        normalized_state, catalog_mappings, mappings_updated = provider.prepare_import_state(
            local_state=state,
            discovered_state=discovered_state,
            env_config=env_config,
            mapping_overrides=catalog_mappings_override,
        )
    except Exception as e:
        raise ImportError(str(e)) from e

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
        "warnings": import_warnings,
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
    for warning in import_warnings:
        console.print(f"[yellow]Warning:[/yellow] {warning}")

    if dry_run:
        console.print(
            f"[yellow]Dry-run:[/yellow] generated {len(import_ops)} operation(s); no files modified."
        )
        console.print(
            "[dim]Next:[/dim] Run without --dry-run to write import operations to changelog."
        )
        return summary

    if mappings_updated:
        provider.update_env_import_mappings(env_config, catalog_mappings)
        write_project(workspace, project)
        console.print(f"[green]✓[/green] Updated catalog mappings for environment '{target_env}'")

    if import_ops:
        append_ops(workspace, import_ops)
        console.print(f"[green]✓[/green] Imported {len(import_ops)} operation(s) into changelog")
    else:
        console.print("[green]✓[/green] No import operations required")

    if adopt_baseline:
        if not provider.capabilities.features.get("baseline_adoption", False):
            raise ImportError(f"Provider '{provider.info.id}' does not support baseline adoption.")

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

        try:
            deployment_id = provider.adopt_import_baseline(
                project=project,
                env_config=env_config,
                target_env=target_env,
                profile=profile,
                warehouse_id=warehouse_id,
                snapshot_version=snapshot_version,
            )
        except NotImplementedError as e:
            raise ImportError(str(e)) from e
        env_config["importBaselineSnapshot"] = snapshot_version
        write_project(workspace, project)
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
