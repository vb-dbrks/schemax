"""Application service layer over command modules.

This module provides a stable orchestration surface for CLI and SDK callers,
while preserving current command behavior during the architecture transition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from schemax.commands.apply import apply_to_environment
from schemax.commands.diff import generate_diff
from schemax.commands.import_assets import import_from_provider, import_from_sql_file
from schemax.commands.rollback import rollback_complete, run_partial_rollback_cli
from schemax.commands.snapshot_rebase import detect_stale_snapshots, rebase_snapshot
from schemax.commands.sql import generate_sql_migration
from schemax.commands.validate import validate_project
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.domain.results import CommandResult
from schemax.providers import ProviderRegistry


@dataclass(slots=True)
class InitService:
    """Initialize workspace project files."""

    workspace_repo: WorkspaceRepository = field(default_factory=WorkspaceRepository)

    def run(self, *, workspace: Path, provider_id: str) -> CommandResult:
        provider = ProviderRegistry.get(provider_id)
        if provider is None:
            available = ", ".join(ProviderRegistry.get_all_ids())
            return CommandResult(
                success=False,
                code="provider_not_found",
                message=f"Provider '{provider_id}' not found. Available providers: {available}",
            )
        self.workspace_repo.ensure_initialized(workspace=workspace, provider_id=provider_id)
        return CommandResult(
            success=True,
            code="initialized",
            message=f"Initialized SchemaX project in {workspace}",
            data={
                "provider": provider.info.id,
                "provider_name": provider.info.name,
                "provider_version": provider.info.version,
            },
        )


@dataclass(slots=True)
class ValidateService:
    """Validate workspace project state."""

    def run(self, *, workspace: Path, json_output: bool = False) -> CommandResult:
        valid = validate_project(workspace, json_output=json_output)
        code = "valid" if valid else "invalid"
        return CommandResult(success=valid, code=code, message="Validation completed")


@dataclass(slots=True)
class SqlService:
    """Generate migration SQL for workspace/snapshot ranges."""

    def run(
        self,
        *,
        workspace: Path,
        output: Path | None,
        snapshot: str | None,
        from_version: str | None,
        to_version: str | None,
        target_env: str | None,
    ) -> CommandResult:
        sql = generate_sql_migration(
            workspace=workspace,
            output=output,
            snapshot=snapshot,
            _from_version=from_version,
            _to_version=to_version,
            target_env=target_env,
        )
        return CommandResult(
            success=True,
            code="sql_generated",
            message="SQL generation completed",
            data={"sql": sql},
        )


@dataclass(slots=True)
class DiffService:
    """Generate operations between two snapshot versions."""

    def run(
        self,
        *,
        workspace: Path,
        from_version: str,
        to_version: str,
        show_sql: bool,
        show_details: bool,
        target_env: str | None,
    ) -> CommandResult:
        operations = generate_diff(
            workspace=workspace,
            from_version=from_version,
            to_version=to_version,
            show_sql=show_sql,
            show_details=show_details,
            target_env=target_env,
        )
        return CommandResult(
            success=True,
            code="diff_generated",
            message="Diff generation completed",
            data={"operations": [op.model_dump(by_alias=True) for op in operations]},
        )


@dataclass(slots=True)
class ImportService:
    """Import assets from provider discovery or SQL files."""

    def run_provider_import(
        self,
        *,
        workspace: Path,
        target_env: str,
        profile: str,
        warehouse_id: str,
        catalog: str | None,
        schema: str | None,
        table: str | None,
        dry_run: bool,
        adopt_baseline: bool,
        catalog_mappings_override: dict[str, str] | None,
    ) -> CommandResult:
        summary = import_from_provider(
            workspace=workspace,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            table=table,
            dry_run=dry_run,
            adopt_baseline=adopt_baseline,
            catalog_mappings_override=catalog_mappings_override,
        )
        return CommandResult(success=True, code="import_completed", data={"summary": summary})

    def run_sql_import(
        self,
        *,
        workspace: Path,
        sql_path: Path,
        mode: str,
        target_env: str | None,
        dry_run: bool,
    ) -> CommandResult:
        summary = import_from_sql_file(
            workspace=workspace,
            sql_path=sql_path,
            mode=mode,
            target_env=target_env,
            dry_run=dry_run,
        )
        return CommandResult(success=True, code="import_completed", data={"summary": summary})


@dataclass(slots=True)
class ApplyService:
    """Apply migration SQL to a target environment."""

    def run(
        self,
        *,
        workspace: Path,
        target_env: str,
        profile: str,
        warehouse_id: str,
        dry_run: bool,
        no_interaction: bool,
        auto_rollback: bool,
    ) -> CommandResult:
        request = _build_apply_request(
            workspace,
            target_env,
            profile,
            warehouse_id,
            dry_run,
            no_interaction,
            auto_rollback,
        )
        result = apply_to_environment(**request)
        return CommandResult(
            success=result.status in {"success", "dry_run"},
            code=f"apply_{result.status}",
            data={"result": _execution_result_to_dict(result)},
        )


@dataclass(slots=True)
class RollbackService:
    """Run partial or complete rollback workflows."""

    def run_partial(
        self,
        *,
        workspace: Path,
        deployment_id: str,
        target_env: str,
        profile: str,
        warehouse_id: str,
        dry_run: bool,
        no_interaction: bool,
    ) -> CommandResult:
        result = run_partial_rollback_cli(
            workspace_path=workspace,
            deployment_id=deployment_id,
            target=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
        )
        return CommandResult(
            success=result.success, code="rollback_partial", data={"result": result}
        )

    def run_complete(
        self,
        *,
        workspace: Path,
        to_snapshot: str,
        target_env: str,
        profile: str,
        warehouse_id: str,
        create_clone: str | None,
        safe_only: bool,
        dry_run: bool,
        no_interaction: bool,
        force: bool,
    ) -> CommandResult:
        result = rollback_complete(
            workspace=workspace,
            to_snapshot=to_snapshot,
            target_env=target_env,
            profile=profile,
            warehouse_id=warehouse_id,
            dry_run=dry_run,
            no_interaction=no_interaction,
            safe_only=safe_only,
            _create_clone=create_clone,
            force=force,
        )
        return CommandResult(
            success=result.success, code="rollback_complete", data={"result": result}
        )


@dataclass(slots=True)
class SnapshotService:
    """Snapshot create/validate/rebase workflow facade."""

    workspace_repo: WorkspaceRepository = field(default_factory=WorkspaceRepository)

    def create(
        self,
        *,
        workspace: Path,
        name: str,
        version: str | None,
        comment: str | None,
        tags: list[str],
    ) -> CommandResult:
        project, snapshot = self.workspace_repo.create_snapshot(
            workspace=workspace,
            name=name,
            version=version,
            comment=comment,
            tags=tags,
        )
        return CommandResult(
            success=True,
            code="snapshot_created",
            data={
                "project": project,
                "snapshot": snapshot,
            },
        )

    def validate(self, *, workspace: Path, json_output: bool) -> CommandResult:
        stale = detect_stale_snapshots(workspace=workspace, _json_output=json_output)
        return CommandResult(
            success=True,
            code="snapshot_validated",
            data={"stale_snapshots": stale},
        )

    def rebase(
        self, *, workspace: Path, version: str, base_version: str | None = None
    ) -> CommandResult:
        result = rebase_snapshot(
            workspace=workspace, snapshot_version=version, new_base_version=base_version
        )
        return CommandResult(success=True, code="snapshot_rebased", data={"result": result})


def _execution_result_to_dict(result: Any) -> dict[str, Any]:
    """Serialize execution result object to a stable dictionary."""
    return {
        "status": result.status,
        "total_statements": result.total_statements,
        "successful_statements": result.successful_statements,
        "failed_statement_index": result.failed_statement_index,
        "total_execution_time_ms": result.total_execution_time_ms,
        "statement_results": [
            {
                "statement_id": statement_result.statement_id,
                "sql": statement_result.sql,
                "status": statement_result.status,
                "execution_time_ms": statement_result.execution_time_ms,
                "rows_affected": statement_result.rows_affected,
                "error_message": statement_result.error_message,
            }
            for statement_result in result.statement_results
        ],
    }


def _build_apply_request(
    workspace: Path,
    target_env: str,
    profile: str,
    warehouse_id: str,
    dry_run: bool,
    no_interaction: bool,
    auto_rollback: bool,
) -> dict[str, Any]:
    """Build kwargs for apply command boundary calls."""
    keys = (
        "workspace",
        "target_env",
        "profile",
        "warehouse_id",
        "dry_run",
        "no_interaction",
        "auto_rollback",
    )
    values = (
        workspace,
        target_env,
        profile,
        warehouse_id,
        dry_run,
        no_interaction,
        auto_rollback,
    )
    return dict(zip(keys, values, strict=True))
