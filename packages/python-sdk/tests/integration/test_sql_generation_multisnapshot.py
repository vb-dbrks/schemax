"""Integration coverage tests for Unity SQL generation across snapshot phases."""

from __future__ import annotations

from pathlib import Path

import pytest

from schemax.commands.diff import generate_diff
from schemax.core.storage import append_ops, ensure_project_file
from schemax.core.workspace_repository import WorkspaceRepository
from schemax.providers.base.operations import Operation, create_operation
from schemax.providers.registry import ProviderRegistry
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


def _state_dict(state: object) -> dict:
    if hasattr(state, "model_dump"):
        return state.model_dump(by_alias=True)
    assert isinstance(state, dict)
    return state


def _create_snapshot(workspace: Path, version: str, name: str) -> None:
    result = invoke_cli(
        "snapshot",
        "create",
        "--name",
        name,
        "--version",
        version,
        str(workspace),
    )
    assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_unity_sql_generation_covers_create_alter_drop_phases(temp_workspace: Path) -> None:
    """Exercise Unity SQL handlers via a 3-snapshot lifecycle: create -> alter -> drop."""
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()

    catalog_id = "cat_cov"
    schema_id = "sch_cov"
    table_id = "tbl_cov"
    col_a = "col_cov_a"
    col_b = "col_cov_b"
    col_c = "col_cov_c"
    view_id = "vw_cov"
    volume_id = "vol_cov"
    function_id = "fn_cov"
    mv_id = "mv_cov"
    constraint_id = "cons_cov_pk"

    create_ops: list[Operation] = [
        builder.catalog.add_catalog(catalog_id, "cov_catalog", op_id="op_c_001"),
        builder.schema.add_schema(schema_id, "cov_schema", catalog_id, op_id="op_c_002"),
        builder.table.add_table(table_id, "cov_table", schema_id, "delta", op_id="op_c_003"),
        builder.column.add_column(col_a, table_id, "id", "STRING", op_id="op_c_004"),
        builder.column.add_column(col_b, table_id, "name", "STRING", op_id="op_c_005"),
        builder.column.add_column(col_c, table_id, "amount", "INT", op_id="op_c_006"),
        builder.view.add_view(
            view_id,
            "cov_view",
            schema_id,
            "SELECT id, name FROM cov_catalog.cov_schema.cov_table",
            op_id="op_c_007",
        ),
        builder.volume.add_volume(
            volume_id,
            "cov_volume",
            schema_id,
            "external",
            location="abfss://cov@storage.dfs.core.windows.net/vol",
            op_id="op_c_008",
        ),
        builder.function.add_function(
            function_id,
            "cov_fn",
            schema_id,
            "SQL",
            "STRING",
            "upper(input)",
            parameters=[{"name": "input", "dataType": "STRING"}],
            op_id="op_c_009",
        ),
        builder.materialized_view.add_materialized_view(
            mv_id,
            "cov_mv",
            schema_id,
            "SELECT id FROM cov_catalog.cov_schema.cov_table",
            op_id="op_c_010",
        ),
    ]
    append_ops(temp_workspace, create_ops)
    _create_snapshot(temp_workspace, "v0.1.0", "Create")

    alter_ops: list[Operation] = [
        builder.table.set_table_comment(table_id, "Coverage table", op_id="op_a_001"),
        builder.table.set_table_property(table_id, "delta.appendOnly", "true", op_id="op_a_002"),
        builder.table.unset_table_property(table_id, "delta.appendOnly", op_id="op_a_003"),
        builder.table.set_table_tag(table_id, "domain", "coverage", op_id="op_a_004"),
        builder.table.unset_table_tag(table_id, "domain", op_id="op_a_005"),
        builder.column.rename_column(col_b, table_id, "full_name", "name", op_id="op_a_006"),
        builder.column.change_column_type(col_c, table_id, "BIGINT", op_id="op_a_007"),
        builder.column.set_nullable(col_c, table_id, False, op_id="op_a_008"),
        builder.column.set_column_comment(col_a, table_id, "Identifier", op_id="op_a_009"),
        builder.column.set_column_tag(col_a, table_id, "pii", "no", op_id="op_a_010"),
        builder.column.unset_column_tag(col_a, table_id, "pii", op_id="op_a_011"),
        builder.column.reorder_columns(table_id, [col_c, col_a, col_b], op_id="op_a_012"),
        builder.constraint.add_constraint(
            constraint_id,
            table_id,
            "primary_key",
            [col_a],
            name="pk_cov",
            op_id="op_a_013",
        ),
        builder.view.update_view(
            view_id,
            definition="SELECT id, full_name FROM cov_catalog.cov_schema.cov_table",
            extracted_dependencies={"tables": ["cov_table"], "views": []},
            op_id="op_a_014",
        ),
        create_operation(
            provider="unity",
            op_type="set_view_comment",
            target=view_id,
            payload={"viewId": view_id, "comment": "Coverage view"},
            op_id="op_a_015",
        ),
        create_operation(
            provider="unity",
            op_type="set_view_property",
            target=view_id,
            payload={"viewId": view_id, "key": "quality", "value": "gold"},
            op_id="op_a_016",
        ),
        create_operation(
            provider="unity",
            op_type="unset_view_property",
            target=view_id,
            payload={"viewId": view_id, "key": "quality"},
            op_id="op_a_017",
        ),
        builder.volume.update_volume(
            volume_id,
            comment="Updated volume",
            location="abfss://cov@storage.dfs.core.windows.net/vol2",
            op_id="op_a_018",
        ),
        builder.function.update_function(
            function_id,
            body="lower(input)",
            comment="updated",
            op_id="op_a_019",
        ),
        builder.function.set_function_comment(function_id, "fn comment", op_id="op_a_020"),
        builder.materialized_view.update_materialized_view(
            mv_id,
            definition="SELECT id, amount FROM cov_catalog.cov_schema.cov_table",
            comment="Updated MV",
            refresh_schedule="EVERY 1 HOURS",
            op_id="op_a_021",
        ),
        builder.materialized_view.set_materialized_view_comment(
            mv_id, "Updated MV comment", op_id="op_a_022"
        ),
        builder.grant.add_grant(
            "table",
            table_id,
            "analysts",
            ["SELECT"],
            op_id="op_a_023",
        ),
        builder.grant.revoke_grant(
            "table",
            table_id,
            "analysts",
            ["SELECT"],
            op_id="op_a_024",
        ),
    ]
    append_ops(temp_workspace, alter_ops)
    _create_snapshot(temp_workspace, "v0.2.0", "Alter")

    drop_ops: list[Operation] = [
        builder.constraint.drop_constraint(
            constraint_id, table_id, op_id="op_d_001", name="pk_cov"
        ),
        builder.column.drop_column(col_c, table_id, op_id="op_d_002"),
        builder.view.drop_view(view_id, op_id="op_d_003"),
        builder.materialized_view.drop_materialized_view(mv_id, op_id="op_d_004"),
        builder.function.drop_function(function_id, op_id="op_d_005"),
        builder.volume.drop_volume(volume_id, op_id="op_d_006"),
        builder.table.drop_table(table_id, op_id="op_d_007"),
        builder.schema.drop_schema(schema_id, op_id="op_d_008"),
        builder.catalog.drop_catalog(catalog_id, op_id="op_d_009"),
    ]
    append_ops(temp_workspace, drop_ops)
    _create_snapshot(temp_workspace, "v0.3.0", "Drop")

    repository = WorkspaceRepository()
    provider = ProviderRegistry.get("unity")
    assert provider is not None
    snap_create = repository.read_snapshot(workspace=temp_workspace, version="v0.1.0")
    snap_alter = repository.read_snapshot(workspace=temp_workspace, version="v0.2.0")
    create_state = _state_dict(snap_create["state"])
    alter_state = _state_dict(snap_alter["state"])

    sql_create = provider.get_sql_generator(state=create_state, name_mapping={}).generate_sql(
        create_ops
    )
    sql_alter = provider.get_sql_generator(state=alter_state, name_mapping={}).generate_sql(
        alter_ops
    )
    sql_drop = provider.get_sql_generator(state=alter_state, name_mapping={}).generate_sql(drop_ops)

    assert "CREATE CATALOG" in sql_create
    assert "CREATE SCHEMA" in sql_create
    assert "CREATE TABLE" in sql_create
    assert "CREATE VIEW" in sql_create
    assert "CREATE EXTERNAL VOLUME" in sql_create or "CREATE VOLUME" in sql_create
    assert "CREATE OR REPLACE FUNCTION" in sql_create
    assert "CREATE MATERIALIZED VIEW" in sql_create

    assert "ALTER TABLE" in sql_alter
    assert "COMMENT ON VIEW" in sql_alter
    assert "ALTER VIEW" in sql_alter
    assert "ALTER VOLUME" in sql_alter
    assert "COMMENT ON FUNCTION" in sql_alter or "CREATE OR REPLACE FUNCTION" in sql_alter
    assert "MATERIALIZED VIEW" in sql_alter
    assert "GRANT" in sql_alter
    assert "REVOKE" in sql_alter

    assert "DROP VIEW" in sql_drop
    assert "DROP MATERIALIZED VIEW" in sql_drop
    assert "DROP FUNCTION" in sql_drop
    assert "DROP VOLUME" in sql_drop
    assert "DROP TABLE" in sql_drop
    assert "DROP SCHEMA" in sql_drop
    assert "DROP CATALOG" in sql_drop


@pytest.mark.integration
def test_unity_sql_generation_from_diff_between_snapshots(temp_workspace: Path) -> None:
    """Validate diff-driven SQL generation across alter and drop phases."""
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()

    add_ops = [
        builder.catalog.add_catalog("cat_d", "diff_catalog", op_id="op_1"),
        builder.schema.add_schema("sch_d", "diff_schema", "cat_d", op_id="op_2"),
        builder.table.add_table("tbl_d", "diff_table", "sch_d", "delta", op_id="op_3"),
        builder.column.add_column("col_d_1", "tbl_d", "id", "INT", op_id="op_4"),
        builder.view.add_view(
            "vw_d",
            "diff_view",
            "sch_d",
            "SELECT id FROM diff_catalog.diff_schema.diff_table",
            op_id="op_5",
        ),
    ]
    append_ops(temp_workspace, add_ops)
    _create_snapshot(temp_workspace, "v0.1.0", "Base")

    change_ops = [
        builder.table.set_table_comment("tbl_d", "table comment", op_id="op_6"),
        builder.column.add_column("col_d_2", "tbl_d", "name", "STRING", op_id="op_7"),
        builder.view.update_view(
            "vw_d",
            definition="SELECT id, name FROM diff_catalog.diff_schema.diff_table",
            extracted_dependencies={"tables": ["diff_table"], "views": []},
            op_id="op_8",
        ),
    ]
    append_ops(temp_workspace, change_ops)
    _create_snapshot(temp_workspace, "v0.2.0", "Change")

    remove_ops = [
        builder.view.drop_view("vw_d", op_id="op_9"),
        builder.table.drop_table("tbl_d", op_id="op_10"),
    ]
    append_ops(temp_workspace, remove_ops)
    _create_snapshot(temp_workspace, "v0.3.0", "Remove")

    provider = ProviderRegistry.get("unity")
    assert provider is not None
    repository = WorkspaceRepository()
    snap_two = repository.read_snapshot(workspace=temp_workspace, version="v0.2.0")
    snap_three = repository.read_snapshot(workspace=temp_workspace, version="v0.3.0")
    snap_two_state = _state_dict(snap_two["state"])
    snap_three_state = _state_dict(snap_three["state"])

    alter_diff_ops = generate_diff(
        workspace=temp_workspace,
        from_version="v0.1.0",
        to_version="v0.2.0",
    )
    alter_sql = provider.get_sql_generator(state=snap_two_state, name_mapping={}).generate_sql(
        alter_diff_ops
    )
    alter_types = {operation.op for operation in alter_diff_ops}
    assert "unity.set_table_comment" in alter_types
    assert "unity.add_column" in alter_types
    assert "unity.update_view" in alter_types
    assert "ALTER TABLE" in alter_sql
    update_view_op = next(
        operation for operation in alter_diff_ops if operation.op == "unity.update_view"
    )
    update_view_sql = (
        provider.get_sql_generator(
            state=snap_two_state,
            name_mapping={},
        )
        .generate_sql_for_operation(update_view_op)
        .sql
    )
    assert "CREATE OR REPLACE VIEW" in update_view_sql

    drop_diff_ops = generate_diff(
        workspace=temp_workspace,
        from_version="v0.2.0",
        to_version="v0.3.0",
    )
    drop_sql = provider.get_sql_generator(state=snap_three_state, name_mapping={}).generate_sql(
        drop_diff_ops
    )
    drop_types = {operation.op for operation in drop_diff_ops}
    assert "unity.drop_view" in drop_types
    assert "unity.drop_table" in drop_types
    assert "DROP VIEW" in drop_sql
    assert "DROP TABLE" in drop_sql
