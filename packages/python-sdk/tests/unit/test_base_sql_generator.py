"""
Tests for providers/base/sql_generator.py — covers BaseSQLGenerator internals.
"""

from unittest.mock import Mock, patch

import pytest

from schemax.providers.base.dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)
from schemax.providers.base.operations import Operation
from schemax.providers.base.sql_generator import (
    BaseSQLGenerator,
    SQLGenerationResult,
    SQLGenerator,
    StatementInfo,
)


def _op(
    op: str = "unity.add_table",
    target: str = "t1",
    op_id: str = "op_1",
    ts: str = "2025-01-01T00:00:00Z",
    payload: dict | None = None,
) -> Operation:
    return Operation(
        id=op_id,
        ts=ts,
        provider="unity",
        op=op,
        target=target,
        payload=payload or {},
    )


class ConcreteSQLGenerator(BaseSQLGenerator):
    """Concrete implementation for testing BaseSQLGenerator."""

    def _get_target_object_id(self, operation: Operation) -> str | None:
        if "column" in operation.op:
            return operation.payload.get("tableId", operation.target)
        return operation.target

    def _is_create_operation(self, operation: Operation) -> bool:
        return "add_" in operation.op

    def _is_drop_operation(self, operation: Operation) -> bool:
        return "drop_" in operation.op

    def _get_dependency_level(self, operation: Operation) -> int:
        if "catalog" in operation.op:
            return 0
        if "schema" in operation.op:
            return 1
        if "table" in operation.op:
            return 2
        if "view" in operation.op:
            return 3
        return 4

    def _generate_batched_create_sql(self, object_id, batch_info):
        return f"CREATE {object_id}"

    def _generate_batched_alter_sql(self, object_id, batch_info):
        return f"ALTER {object_id}"

    def generate_sql_for_operation(self, operation: Operation) -> SQLGenerationResult:
        return SQLGenerationResult(
            sql=f"-- {operation.op} {operation.target}",
            statements=[
                StatementInfo(
                    sql=f"-- {operation.op} {operation.target}",
                    operation_ids=[operation.id],
                    execution_order=1,
                )
            ],
        )

    def can_generate_sql(self, operation: Operation) -> bool:
        return "unsupported" not in operation.op


# ── StatementInfo / SQLGenerationResult ────────────────────────────────


class TestModels:
    def test_statement_info(self):
        si = StatementInfo(sql="SELECT 1", operation_ids=["op1"], execution_order=1)
        assert si.sql == "SELECT 1"

    def test_sql_generation_result_defaults(self):
        r = SQLGenerationResult(sql="SELECT 1")
        assert r.statements == []
        assert r.warnings == []
        assert r.is_idempotent is True


# ── BaseSQLGenerator utilities ─────────────────────────────────────────


class TestBaseSQLGeneratorUtilities:
    def test_escape_identifier(self):
        assert BaseSQLGenerator.escape_identifier("my_table") == "`my_table`"

    def test_escape_identifier_with_backticks(self):
        assert BaseSQLGenerator.escape_identifier("my`table") == "`my``table`"

    def test_escape_string(self):
        assert BaseSQLGenerator.escape_string("it's") == "it''s"

    def test_build_fqn(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        fqn = gen._build_fqn("catalog", "schema", "table")
        assert fqn == "`catalog`.`schema`.`table`"

    def test_build_fqn_skips_empty(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        fqn = gen._build_fqn("catalog", "", "table")
        assert fqn == "`catalog`.`table`"

    def test_build_fqn_custom_separator(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        fqn = gen._build_fqn("a", "b", separator="::")
        assert fqn == "`a`::`b`"


# ── _get_object_type_from_operation ────────────────────────────────────


class TestGetObjectType:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_catalog(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_catalog")) == "catalog"

    def test_schema(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_schema")) == "schema"

    def test_table(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_table")) == "table"

    def test_view(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_view")) == "view"

    def test_column(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_column")) == "column"

    def test_constraint(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_constraint")) == "constraint"

    def test_unknown(self):
        assert self.gen._get_object_type_from_operation(_op("unity.add_grant")) == "unknown"


# ── _get_object_display_name_from_op ───────────────────────────────────


class TestGetObjectDisplayName:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_table_with_name_in_payload(self):
        op = _op("unity.drop_table", payload={"name": "my_tbl"})
        assert self.gen._get_object_display_name_from_op(op) == "table 'my_tbl'"

    def test_view(self):
        op = _op("unity.drop_view", payload={"name": "my_view"})
        assert self.gen._get_object_display_name_from_op(op) == "view 'my_view'"

    def test_schema(self):
        op = _op("unity.drop_schema", payload={"name": "my_sch"})
        assert self.gen._get_object_display_name_from_op(op) == "schema 'my_sch'"

    def test_catalog(self):
        op = _op("unity.drop_catalog", payload={"name": "my_cat"})
        assert self.gen._get_object_display_name_from_op(op) == "catalog 'my_cat'"

    def test_fallback_to_target(self):
        op = _op("unity.drop_table", target="tgt_id", payload={})
        assert self.gen._get_object_display_name_from_op(op) == "table 'tgt_id'"

    def test_unknown_op_type(self):
        op = _op("unity.add_grant", payload={"name": "g1"})
        assert self.gen._get_object_display_name_from_op(op) == "object 'g1'"


# ── generate_sql (default) ─────────────────────────────────────────────


class TestGenerateSQL:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_empty_ops(self):
        assert self.gen.generate_sql([]) == ""

    def test_single_op(self):
        sql = self.gen.generate_sql([_op()])
        assert "unity.add_table" in sql
        assert "Operation:" in sql

    def test_unsupported_op_skipped(self):
        sql = self.gen.generate_sql([_op("unity.unsupported_op")])
        assert sql == ""

    def test_multiple_ops(self):
        ops = [_op(op_id="op_1"), _op(op_id="op_2", target="t2")]
        sql = self.gen.generate_sql(ops)
        assert "op_1" in sql
        assert "op_2" in sql


# ── generate_sql_with_mapping (base SQLGenerator default) ──────────────


class TestGenerateSQLWithMapping:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_default_mapping(self):
        result = self.gen.generate_sql_with_mapping([_op()])
        assert isinstance(result, SQLGenerationResult)
        assert len(result.statements) >= 1


# ── _build_dependency_graph ────────────────────────────────────────────


class TestBuildDependencyGraph:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_single_op(self):
        graph = self.gen.build_dependency_graph([_op(target="t1")])
        assert "t1" in graph.nodes

    def test_multiple_ops_same_target(self):
        ops = [
            _op("unity.add_table", target="t1", op_id="op_1"),
            _op("unity.update_table_comment", target="t1", op_id="op_2"),
        ]
        graph = self.gen.build_dependency_graph(ops)
        assert "t1" in graph.nodes
        assert graph.nodes["t1"].metadata["op_count"] == 2

    def test_no_target_id_skipped(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        # Override to return None
        gen._get_target_object_id = lambda op: None
        graph = gen.build_dependency_graph([_op()])
        assert len(graph.nodes) == 0

    def test_stores_ops_by_target_in_metadata(self):
        ops = [_op(target="t1")]
        graph = self.gen.build_dependency_graph(ops)
        assert "t1" in graph.metadata["ops_by_target"]


# ── _sort_operations_by_level ──────────────────────────────────────────


class TestSortOperationsByLevel:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_sorts_catalog_before_schema_before_table(self):
        ops = [
            _op("unity.add_table", op_id="t"),
            _op("unity.add_catalog", op_id="c"),
            _op("unity.add_schema", op_id="s"),
        ]
        sorted_ops = self.gen._sort_operations_by_level(ops)
        assert sorted_ops[0].id == "c"
        assert sorted_ops[1].id == "s"
        assert sorted_ops[2].id == "t"


# ── generate_sql_with_dependencies ─────────────────────────────────────


class TestGenerateSQLWithDependencies:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_empty_ops(self):
        result = self.gen.generate_sql_with_dependencies([])
        assert result.sql == ""
        assert result.statements == []

    def test_single_op(self):
        result = self.gen.generate_sql_with_dependencies([_op()])
        assert len(result.statements) == 1

    def test_unsupported_op_adds_warning(self):
        result = self.gen.generate_sql_with_dependencies([_op("unity.unsupported_op")])
        assert any("Cannot generate" in w for w in result.warnings)
        assert len(result.statements) == 0

    def test_falls_back_on_dependency_error(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        gen._build_dependency_graph = Mock(side_effect=RuntimeError("graph fail"))
        result = gen.generate_sql_with_dependencies([_op()])
        assert any("Dependency analysis failed" in w for w in result.warnings)
        assert len(result.statements) == 1  # Still generates SQL via fallback


# ── _detect_breaking_changes ───────────────────────────────────────────


class TestDetectBreakingChanges:
    def setup_method(self):
        self.gen = ConcreteSQLGenerator(state={"catalogs": []})

    def test_no_warnings_for_create_ops(self):
        warnings = self.gen._detect_breaking_changes([_op("unity.add_table")])
        assert warnings == []

    def test_no_warnings_for_drop_without_dependents(self):
        ops = [_op("unity.drop_table", target="t1")]
        warnings = self.gen._detect_breaking_changes(ops)
        assert warnings == []

    def test_graph_build_failure_returns_empty(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        gen._build_dependency_graph = Mock(side_effect=RuntimeError("fail"))
        warnings = gen._detect_breaking_changes([_op("unity.drop_table")], graph=None)
        assert warnings == []

    def test_skip_non_drop_ops(self):
        ops = [_op("unity.add_table"), _op("unity.update_table_comment")]
        warnings = self.gen._detect_breaking_changes(ops)
        assert warnings == []


# ── _extract_operation_dependencies (default) ──────────────────────────


class TestExtractOperationDependencies:
    def test_default_returns_empty(self):
        gen = ConcreteSQLGenerator(state={"catalogs": []})
        assert gen._extract_operation_dependencies(_op()) == []
