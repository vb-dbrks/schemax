"""
Import Command Implementation

Imports existing provider assets into SchemaX state/changelog so users can
adopt already-deployed objects instead of starting from an empty model.
"""

from collections import Counter
from pathlib import Path
from typing import Any, NamedTuple

from rich.console import Console

from schemax.core.storage import (
    append_ops,
    create_snapshot,
    ensure_project_file,
    get_environment_config,
    get_project_file_path,
    load_current_state,
    read_project,
    write_project,
)
from schemax.providers.base.executor import ExecutionConfig

console = Console()


class ImportCommandError(Exception):
    """Raised when import command fails"""


class _DiscoverResult(NamedTuple):
    """Result of _discover_and_normalize."""

    normalized_state: Any
    catalog_mappings: dict[str, str]
    mappings_updated: bool
    import_warnings: list[str]
    discovered_state: Any
    project: dict[str, Any]
    env_config: dict[str, Any]


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
    scope = {"catalog": catalog, "schema": schema, "table": table}

    _validate_import_config(provider, config, scope)

    console.print("[bold]SchemaX Import[/bold]")
    console.print("─" * 60)
    console.print(f"[blue]Provider:[/blue] {provider.info.name} v{provider.info.version}")
    console.print(f"[blue]Environment:[/blue] {target_env}")
    console.print(f"[blue]Scope:[/blue] {scope}")

    discover = _discover_and_normalize(
        provider, state, config, scope, workspace, target_env, catalog_mappings_override
    )

    differ = provider.get_state_differ(
        old_state=state,
        new_state=discover.normalized_state,
        old_operations=changelog.get("ops", []),
        new_operations=[],
    )
    import_ops = differ.generate_diff_operations()
    object_counts = _count_objects(discover.discovered_state)
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
        "catalog_mappings": discover.catalog_mappings,
        "warnings": discover.import_warnings,
    }

    _print_import_summary(
        object_counts, op_counts, import_ops, discover.catalog_mappings, discover.import_warnings
    )

    if dry_run:
        console.print(
            f"[yellow]Dry-run:[/yellow] generated {len(import_ops)} operation(s); no files modified."
        )
        console.print(
            "[dim]Next:[/dim] Run without --dry-run to write import operations to changelog."
        )
        return summary

    if discover.mappings_updated:
        provider.update_env_import_mappings(discover.env_config, discover.catalog_mappings)
        write_project(workspace, discover.project)
        console.print(f"[green]✓[/green] Updated catalog mappings for environment '{target_env}'")

    if import_ops:
        append_ops(workspace, import_ops)
        console.print(f"[green]✓[/green] Imported {len(import_ops)} operation(s) into changelog")
    else:
        console.print("[green]✓[/green] No import operations required")

    if adopt_baseline:
        project, baseline_updates = _adopt_baseline(
            provider,
            discover.project,
            discover.env_config,
            target_env,
            profile,
            warehouse_id,
            import_ops,
            workspace,
        )
        summary.update(baseline_updates)
        if baseline_updates:
            console.print(
                f"[dim]Next:[/dim] Run `schemax apply --target {target_env} --profile {profile} "
                f"--warehouse-id {warehouse_id} --dry-run` to verify zero-diff."
            )
    else:
        console.print(
            "[dim]Next:[/dim] Create a snapshot and run apply when you are ready to deploy."
        )

    return summary


def _validate_import_config(
    provider: Any, config: ExecutionConfig, scope: dict[str, str | None]
) -> None:
    """Run execution and scope validation; raise ImportCommandError on failure."""
    validation = provider.validate_execution_config(config)
    if not validation.valid:
        errors = "\n".join([f"  - {e.field}: {e.message}" for e in validation.errors])
        raise ImportCommandError(f"Invalid execution configuration:\n{errors}")
    scope_validation = provider.validate_import_scope(scope)
    if not scope_validation.valid:
        errors = "\n".join([f"  - {e.field}: {e.message}" for e in scope_validation.errors])
        raise ImportCommandError(f"Invalid import scope:\n{errors}")


def _discover_and_normalize(
    provider: Any,
    state: Any,
    config: ExecutionConfig,
    scope: dict[str, str | None],
    workspace: Path,
    target_env: str,
    mapping_overrides: dict[str, str] | None,
) -> _DiscoverResult:
    """Discover state, collect warnings, prepare import state; return normalized state and metadata."""
    try:
        discovered_state = provider.discover_state(config=config, scope=scope)
    except NotImplementedError as err:
        raise ImportCommandError(str(err)) from err

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
            mapping_overrides=mapping_overrides,
        )
    except Exception as err:
        raise ImportCommandError(str(err)) from err

    return _DiscoverResult(
        normalized_state=normalized_state,
        catalog_mappings=catalog_mappings,
        mappings_updated=mappings_updated,
        import_warnings=import_warnings,
        discovered_state=discovered_state,
        project=project,
        env_config=env_config,
    )


def _print_import_summary(
    object_counts: dict[str, int],
    op_counts: dict[str, int],
    import_ops: list[Any],
    catalog_mappings: dict[str, str],
    import_warnings: list[str],
) -> None:
    """Print discovered counts, planned operations, catalog mappings, and warnings."""
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


def _adopt_baseline(
    provider: Any,
    project: dict[str, Any],
    env_config: dict[str, Any],
    target_env: str,
    profile: str,
    warehouse_id: str,
    import_ops: list[Any],
    workspace: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run adopt-baseline flow; return (updated project, summary updates to merge)."""
    if not provider.capabilities.features.get("baseline_adoption", False):
        raise ImportCommandError(
            f"Provider '{provider.info.id}' does not support baseline adoption."
        )

    snapshot_version = project.get("latestSnapshot")
    if import_ops or not snapshot_version:
        project, snapshot = create_snapshot(
            workspace,
            name=f"Imported baseline for {target_env}",
            comment=(
                f"Imported existing provider assets for {target_env} and "
                "adopted as deployment baseline."
            ),
            tags=["import", "baseline"],
        )
        snapshot_version = snapshot["version"]
        env_config = get_environment_config(project, target_env)
        console.print(f"[green]✓[/green] Created baseline snapshot: {snapshot_version}")
    else:
        console.print(
            f"[blue]Using existing latest snapshot for baseline:[/blue] {snapshot_version}"
        )

    if not snapshot_version:
        raise ImportCommandError("Could not determine snapshot version for baseline adoption")

    try:
        deployment_id = provider.adopt_import_baseline(
            project=project,
            env_config=env_config,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            snapshot_version=snapshot_version,
        )
    except NotImplementedError as err:
        raise ImportCommandError(str(err)) from err

    env_config["importBaselineSnapshot"] = snapshot_version
    write_project(workspace, project)
    console.print(f"[green]✓[/green] Adopted baseline deployment: {deployment_id}")
    return project, {
        "snapshot_version": snapshot_version,
        "deployment_id": deployment_id,
    }


def import_from_sql_file(
    workspace: Path,
    sql_path: Path,
    mode: str = "diff",
    dry_run: bool = False,
    target_env: str | None = None,
) -> dict[str, Any]:
    """Import state from a SQL DDL file; diff against current state or replace as new baseline.

    Args:
        workspace: Workspace root (project root).
        sql_path: Path to .sql file.
        mode: "diff" = append ops to changelog; "replace" = treat parsed state as new baseline (ops from empty).
        dry_run: If True, do not write changelog.
        target_env: Optional target environment (for catalog mapping / consistency; v1 may ignore).

    Returns:
        Summary dict with object_counts, operations_generated, warnings, report (created/skipped/parse_errors).
    """
    if not sql_path.exists():
        raise ImportCommandError(f"SQL file not found: {sql_path}")

    if not get_project_file_path(workspace).exists():
        ensure_project_file(workspace, provider_id="unity")
        console.print("[dim]Initialized new SchemaX project (Unity)[/dim]")

    state, changelog, provider, _ = load_current_state(workspace, validate=False)
    parsed_state, report, import_warnings = _parse_sql_and_collect_warnings(provider, sql_path)

    old_state, new_state, old_ops, catalog_mappings, mappings_updated = _normalize_for_diff(
        mode, provider, state, changelog, parsed_state, workspace, target_env
    )

    differ = provider.get_state_differ(
        old_state=old_state,
        new_state=new_state,
        old_operations=old_ops,
        new_operations=[],
    )
    import_ops = differ.generate_diff_operations()
    object_counts = _count_objects(parsed_state)
    op_counts = _summarize_operations(import_ops)

    summary = {
        "provider": provider.info.id,
        "source": "sql_file",
        "sql_path": str(sql_path),
        "mode": mode,
        "object_counts": object_counts,
        "operations_generated": len(import_ops),
        "operation_breakdown": op_counts,
        "dry_run": dry_run,
        "warnings": import_warnings,
        "report": report,
    }

    _print_sql_import_summary(sql_path, mode, object_counts, op_counts, import_ops, import_warnings)

    if dry_run:
        console.print(
            f"[yellow]Dry-run:[/yellow] generated {len(import_ops)} operation(s); no files modified."
        )
        console.print(
            "[dim]Next:[/dim] Run without --dry-run to write import operations to changelog."
        )
        return summary

    if import_ops:
        append_ops(workspace, import_ops)
        console.print(f"[green]✓[/green] Imported {len(import_ops)} operation(s) into changelog")
    else:
        console.print("[green]✓[/green] No import operations required")

    if mappings_updated and target_env:
        project = read_project(workspace)
        env_config = get_environment_config(project, target_env)
        provider.update_env_import_mappings(env_config, catalog_mappings)
        write_project(workspace, project)
        console.print(f"[green]✓[/green] Updated catalog mappings for environment '{target_env}'")

    console.print("[dim]Next:[/dim] Create a snapshot and run apply when you are ready to deploy.")
    return summary


def _parse_sql_and_collect_warnings(
    provider: Any, sql_path: Path
) -> tuple[Any, dict[str, Any], list[str]]:
    """Parse SQL file via provider and build warnings list; return (parsed_state, report, warnings)."""
    try:
        parsed_state, report = provider.state_from_ddl(sql_path=sql_path)
    except NotImplementedError as err:
        raise ImportCommandError(str(err)) from err
    except ValueError as err:
        raise ImportCommandError(str(err)) from err

    import_warnings: list[str] = []
    skipped = report.get("skipped", 0)
    parse_errors = report.get("parse_errors", [])
    if skipped:
        import_warnings.append(f"Skipped {skipped} statement(s) (unsupported or non-DDL).")
    for err in parse_errors[:5]:
        import_warnings.append(
            f"Parse error at statement {err.get('index', '?')}: {err.get('message', '')[:80]}"
        )
    if len(parse_errors) > 5:
        import_warnings.append(f"... and {len(parse_errors) - 5} more parse error(s).")
    return parsed_state, report, import_warnings


def _normalize_for_diff(
    mode: str,
    provider: Any,
    state: Any,
    changelog: dict[str, Any],
    parsed_state: Any,
    workspace: Path,
    target_env: str | None,
) -> tuple[Any, Any, list[Any], dict[str, str], bool]:
    """Compute old/new state and ops for diff; return (old_state, new_state, old_ops, catalog_mappings, mappings_updated)."""
    if mode == "replace":
        return (
            provider.create_initial_state(),
            parsed_state,
            [],
            {},
            False,
        )

    if not target_env:
        return state, parsed_state, changelog.get("ops", []), {}, False

    project = read_project(workspace)
    env_config = get_environment_config(project, target_env)
    try:
        new_state, catalog_mappings, mappings_updated = provider.prepare_import_state(
            local_state=state,
            discovered_state=parsed_state,
            env_config=env_config,
            mapping_overrides=None,
        )
    except Exception as err:
        raise ImportCommandError(str(err)) from err
    return state, new_state, changelog.get("ops", []), catalog_mappings, mappings_updated


def _print_sql_import_summary(
    sql_path: Path,
    mode: str,
    object_counts: dict[str, int],
    op_counts: dict[str, int],
    import_ops: list[Any],
    import_warnings: list[str],
) -> None:
    """Print summary for SQL file import."""
    console.print("[bold]SchemaX Import (from SQL file)[/bold]")
    console.print("─" * 60)
    console.print(f"[blue]File:[/blue] {sql_path}")
    console.print(f"[blue]Mode:[/blue] {mode}")
    console.print(
        "[blue]Parsed:[/blue] "
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
    for warning in import_warnings:
        console.print(f"[yellow]Warning:[/yellow] {warning}")


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
    op_counter = Counter(
        item.op if hasattr(item, "op") else item.get("op", "unknown") for item in ops
    )
    return dict(sorted(op_counter.items()))
