"""Tests for commands/sql.py — covers build_catalog_mapping and helper functions."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from schemax.commands.sql import (
    SQLGenerationError,
    _extract_add_table_payload,
    _prepare_operations,
    build_catalog_mapping,
    generate_sql_migration,
)
from schemax.providers.base.operations import Operation


# ── build_catalog_mapping ──────────────────────────────────────────────


class TestBuildCatalogMapping:
    def test_no_catalogs(self):
        result = build_catalog_mapping({"catalogs": []}, {})
        assert result == {}

    def test_valid_mapping(self):
        state = {"catalogs": [{"name": "analytics"}]}
        env = {"catalogMappings": {"analytics": "prod_analytics"}}
        result = build_catalog_mapping(state, env)
        assert result == {"analytics": "prod_analytics"}

    def test_missing_mapping_raises(self):
        state = {"catalogs": [{"name": "analytics"}, {"name": "raw"}]}
        env = {"catalogMappings": {"analytics": "prod_analytics"}}
        with pytest.raises(SQLGenerationError, match="Missing catalog mapping"):
            build_catalog_mapping(state, env)

    def test_non_dict_mappings_raises(self):
        state = {"catalogs": [{"name": "analytics"}]}
        env = {"catalogMappings": "not_a_dict"}
        with pytest.raises(SQLGenerationError, match="must be an object"):
            build_catalog_mapping(state, env)

    def test_empty_mappings_dict(self):
        state = {"catalogs": [{"name": "cat1"}]}
        env = {"catalogMappings": {}}
        with pytest.raises(SQLGenerationError, match="Missing catalog mapping"):
            build_catalog_mapping(state, env)


# ── _extract_add_table_payload ─────────────────────────────────────────


class TestExtractAddTablePayload:
    def test_non_add_table_op_returns_none(self):
        op = Operation(
            id="op1", ts="2025-01-01T00:00:00Z", provider="unity",
            op="unity.add_schema", target="s1", payload={},
        )
        assert _extract_add_table_payload(op) is None

    def test_non_external_table_returns_none(self):
        op = Operation(
            id="op1", ts="2025-01-01T00:00:00Z", provider="unity",
            op="unity.add_table", target="t1", payload={"name": "tbl"},
        )
        assert _extract_add_table_payload(op) is None

    def test_external_table_operation(self):
        op = Operation(
            id="op1", ts="2025-01-01T00:00:00Z", provider="unity",
            op="unity.add_table", target="t1",
            payload={"name": "ext_tbl", "external": True, "externalLocationName": "loc1", "path": "data/"},
        )
        result = _extract_add_table_payload(op)
        assert result == ("ext_tbl", "loc1", "data/")

    def test_dict_operation_non_external(self):
        assert _extract_add_table_payload({"op": "unity.add_table", "payload": {"name": "t"}}) is None

    def test_dict_operation_non_add_table(self):
        assert _extract_add_table_payload({"op": "unity.add_schema"}) is None

    def test_dict_external_table(self):
        result = _extract_add_table_payload({
            "op": "unity.add_table",
            "payload": {"name": "ext", "external": True, "externalLocationName": "loc", "path": "p/"},
        })
        assert result == ("ext", "loc", "p/")


# ── _prepare_operations ───────────────────────────────────────────────


class TestPrepareOperations:
    def test_converts_dicts_to_operations(self):
        ops = [
            {
                "id": "op1", "ts": "2025-01-01T00:00:00Z", "provider": "unity",
                "op": "unity.add_table", "target": "t1", "payload": {},
            }
        ]
        result = _prepare_operations(ops, None, None)
        assert isinstance(result[0], Operation)

    def test_passes_through_operation_objects(self):
        op = Operation(
            id="op1", ts="2025-01-01T00:00:00Z", provider="unity",
            op="unity.add_table", target="t1", payload={},
        )
        result = _prepare_operations([op], None, None)
        assert result[0] is op


# ── generate_sql_migration ─────────────────────────────────────────────


class _FakeRepo:
    def __init__(self, *, project=None, state_result=None, snapshots=None, env_config=None):
        self._project = project or {"name": "test"}
        self._state_result = state_result
        self._snapshots = snapshots or {}
        self._env_config = env_config or {}

    def read_project(self, *, workspace):
        return self._project

    def load_current_state(self, *, workspace, validate=False):
        return self._state_result

    def read_snapshot(self, *, workspace, version):
        return self._snapshots[version]

    def get_environment_config(self, *, project, environment):
        return self._env_config


class TestGenerateSqlMigration:
    def test_no_ops_returns_empty(self):
        provider = Mock()
        provider.info = SimpleNamespace(name="Unity Catalog")
        repo = _FakeRepo(
            state_result=({"catalogs": []}, {"ops": []}, provider, None),
        )
        result = generate_sql_migration(Path("/tmp"), workspace_repo=repo)
        assert result == ""

    def test_with_ops_generates_sql(self):
        provider = Mock()
        provider.info = SimpleNamespace(name="Unity Catalog")
        provider.get_sql_generator.return_value.generate_sql.return_value = "CREATE CATALOG demo;"

        ops = [
            {
                "id": "op1", "ts": "2025-01-01T00:00:00Z", "provider": "unity",
                "op": "unity.add_catalog", "target": "c1", "payload": {"name": "demo"},
            }
        ]
        repo = _FakeRepo(
            state_result=({"catalogs": []}, {"ops": ops}, provider, None),
        )
        result = generate_sql_migration(Path("/tmp"), workspace_repo=repo)
        assert "CREATE CATALOG demo" in result

    def test_file_not_found_wraps_error(self):
        repo = _FakeRepo()
        repo.read_project = Mock(side_effect=FileNotFoundError("no project"))
        with pytest.raises(SQLGenerationError, match="Project files not found"):
            generate_sql_migration(Path("/tmp"), workspace_repo=repo)

    def test_unexpected_error_wraps(self):
        repo = _FakeRepo()
        repo.read_project = Mock(side_effect=RuntimeError("boom"))
        with pytest.raises(SQLGenerationError, match="Failed to generate SQL"):
            generate_sql_migration(Path("/tmp"), workspace_repo=repo)

    def test_snapshot_latest_no_snapshots_raises(self):
        repo = _FakeRepo(project={"name": "test", "latestSnapshot": None})
        with pytest.raises(SQLGenerationError, match="No snapshots available"):
            generate_sql_migration(Path("/tmp"), snapshot="latest", workspace_repo=repo)

    def test_snapshot_latest_with_ops(self):
        provider = Mock()
        provider.info = SimpleNamespace(name="Unity Catalog")
        provider.get_sql_generator.return_value.generate_sql.return_value = "ALTER TABLE t1;"
        ops = [
            {
                "id": "op1", "ts": "2025-01-01T00:00:00Z", "provider": "unity",
                "op": "unity.add_column", "target": "col1", "payload": {},
            }
        ]
        repo = _FakeRepo(
            project={"name": "test", "latestSnapshot": "v0.1.0"},
            snapshots={"v0.1.0": {"state": {"catalogs": []}, "operations": ops}},
            state_result=({"catalogs": []}, {"ops": []}, provider, None),
        )
        result = generate_sql_migration(Path("/tmp"), snapshot="latest", workspace_repo=repo)
        assert "ALTER TABLE t1" in result

    def test_snapshot_no_operations_raises(self):
        provider = Mock()
        repo = _FakeRepo(
            project={"name": "test", "latestSnapshot": "v0.1.0"},
            snapshots={"v0.1.0": {"state": {"catalogs": []}}},
            state_result=({"catalogs": []}, {"ops": []}, provider, None),
        )
        with pytest.raises(SQLGenerationError, match="not yet supported"):
            generate_sql_migration(Path("/tmp"), snapshot="latest", workspace_repo=repo)

    def test_output_to_file(self, tmp_path):
        provider = Mock()
        provider.info = SimpleNamespace(name="Unity Catalog")
        provider.get_sql_generator.return_value.generate_sql.return_value = "CREATE TABLE t1;"

        ops = [
            {
                "id": "op1", "ts": "2025-01-01T00:00:00Z", "provider": "unity",
                "op": "unity.add_table", "target": "t1", "payload": {},
            }
        ]
        repo = _FakeRepo(
            state_result=({"catalogs": []}, {"ops": ops}, provider, None),
        )
        output = tmp_path / "out.sql"
        result = generate_sql_migration(Path("/tmp"), output=output, workspace_repo=repo)
        assert output.exists()
        assert output.read_text() == "CREATE TABLE t1;"
