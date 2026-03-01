"""Integration-style coverage tests for base provider utility modules."""

from __future__ import annotations

from typing import Any, cast

import pytest

from schemax.providers.base.batching import BatchInfo, OperationBatcher
from schemax.providers.base.dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)
from schemax.providers.base.exceptions import CircularDependencyError, MissingDependencyError
from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel
from schemax.providers.base.operations import Operation
from schemax.providers.base.optimization import ColumnReorderOptimizer
from schemax.providers.base.sql_generator import BaseSQLGenerator, SQLGenerationResult


def _operation(
    op_id: str,
    op: str,
    target: str,
    *,
    ts: str = "2026-01-01T00:00:00Z",
    payload: dict[str, Any] | None = None,
) -> Operation:
    return Operation(
        id=op_id,
        ts=ts,
        provider="test",
        op=op,
        target=target,
        payload=payload or {},
    )


class _DummySQLGenerator(BaseSQLGenerator):
    """Concrete test double for BaseSQLGenerator abstract contract."""

    def can_generate_sql(self, operation: Operation) -> bool:
        return operation.op != "test.unsupported"

    def generate_sql_for_operation(self, operation: Operation) -> SQLGenerationResult:
        return SQLGenerationResult(sql=f"SELECT '{operation.id}'")

    def _get_target_object_id(self, operation: Operation) -> str | None:
        return str(operation.payload.get("targetId") or operation.target)

    def _is_create_operation(self, operation: Operation) -> bool:
        return operation.op.endswith(".add")

    def _is_drop_operation(self, operation: Operation) -> bool:
        return operation.op.endswith(".drop")

    def _get_dependency_level(self, operation: Operation) -> int:
        level = operation.payload.get("level")
        return int(level) if isinstance(level, int) else 0

    def _extract_operation_dependencies(
        self, operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        deps = operation.payload.get("deps", [])
        if not isinstance(deps, list):
            return []
        return [
            (str(dep_id), DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
            for dep_id in deps
        ]

    def _generate_batched_create_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        return f"-- create {object_id} ({len(batch_info.op_ids)})"

    def _generate_batched_alter_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        return f"-- alter {object_id} ({len(batch_info.modify_ops)})"


@pytest.mark.integration
def test_batcher_batches_and_reports_statistics() -> None:
    batcher = OperationBatcher()
    ops = [
        _operation("op1", "test.add", "t1", payload={"targetId": "table:1"}),
        _operation("op2", "test.modify", "c1", payload={"targetId": "table:1"}),
        _operation("op3", "test.modify", "c2", payload={"targetId": "table:2"}),
    ]

    batches = batcher.batch_operations(
        ops,
        get_target_func=lambda operation: cast(str | None, operation.payload.get("targetId")),
        is_create_func=lambda operation: operation.op.endswith(".add"),
    )
    assert set(batches.keys()) == {"table:1", "table:2"}
    assert batches["table:1"].is_new is True
    assert len(batches["table:1"].modify_ops) == 1

    stats = batcher.get_batch_statistics(batches)
    assert stats["total_batches"] == 2
    assert stats["total_operations"] == 3
    assert stats["new_objects"] == 1
    assert stats["modified_objects"] == 1
    assert stats["max_ops_in_batch"] == 2


@pytest.mark.integration
def test_batcher_batches_by_type() -> None:
    batcher = OperationBatcher()
    ops = [
        _operation("op1", "test.add", "x", payload={"targetId": "obj:1"}),
        _operation("op2", "test.column", "x", payload={"targetId": "obj:1"}),
        _operation("op3", "test.property", "x", payload={"targetId": "obj:1"}),
    ]
    grouped = batcher.batch_operations_by_type(
        ops,
        get_target_func=lambda operation: cast(str | None, operation.payload.get("targetId")),
        categorize_func=lambda operation: operation.op.split(".")[1],
    )
    assert set(grouped["obj:1"].keys()) == {"add", "column", "property"}


@pytest.mark.integration
def test_dependency_graph_cycle_and_sort_and_validation() -> None:
    graph = DependencyGraph()
    op_a = _operation("a", "test.add", "a", ts="2026-01-01T00:00:01Z")
    op_b = _operation("b", "test.add", "b", ts="2026-01-01T00:00:02Z")
    node_a = DependencyNode(id="a", type="table", hierarchy_level=2, operation=op_a)
    node_b = DependencyNode(id="b", type="view", hierarchy_level=2, operation=op_b)
    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_edge("a", "b", DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)

    deps_for_b = graph.get_dependencies("b")
    assert len(deps_for_b) == 1
    assert deps_for_b[0].to_id == "a"
    assert graph.get_dependents("a") == ["b"]
    assert graph.detect_cycles() == []
    sorted_ops = graph.topological_sort()
    assert [operation.id for operation in sorted_ops] == ["a", "b"]

    graph.add_edge("b", "a", DependencyType.VIEW_TO_VIEW, DependencyEnforcement.ENFORCED)
    cycles = graph.detect_cycles()
    assert cycles
    with pytest.raises(ValueError):
        graph.topological_sort()

    warnings = graph.validate_dependencies()
    assert isinstance(warnings, list)


@pytest.mark.integration
def test_dependency_graph_error_paths() -> None:
    graph = DependencyGraph()
    node = DependencyNode(id="n1", type="schema", hierarchy_level=1)
    graph.add_node(node)
    with pytest.raises(ValueError):
        graph.add_node(node)
    with pytest.raises(ValueError):
        graph.add_edge("missing", "n1", DependencyType.CONSTRAINT)
    with pytest.raises(ValueError):
        graph.add_edge("n1", "missing", DependencyType.CONSTRAINT)
    assert graph.get_dependencies("missing") == []
    assert graph.get_dependents("missing") == []


@pytest.mark.integration
def test_base_sql_generator_dependency_order_and_breaking_changes() -> None:
    generator = _DummySQLGenerator(state={"catalogs": []})
    ops = [
        _operation(
            "op_add_table",
            "test.add",
            "table_1",
            payload={"targetId": "table_1", "level": 1},
        ),
        _operation(
            "op_add_view",
            "test.add",
            "view_1",
            payload={"targetId": "view_1", "deps": ["table_1"], "level": 2},
        ),
        _operation(
            "op_drop_table",
            "test.drop",
            "table_1",
            payload={"targetId": "table_1", "name": "orders", "level": 3},
        ),
    ]
    result = generator.generate_sql_with_dependencies(ops)
    assert result.sql
    assert len(result.statements) == 3
    assert any("Breaking change" in warning for warning in result.warnings)

    fqn = generator._build_fqn("cat", "sch", "tbl")
    assert fqn == "`cat`.`sch`.`tbl`"
    assert generator.escape_identifier("a`b") == "`a``b`"
    assert generator.escape_string("O'Brien") == "O''Brien"


@pytest.mark.integration
def test_base_sql_generator_cycle_warning_fallback() -> None:
    generator = _DummySQLGenerator(state={"catalogs": []})
    ops = [
        _operation(
            "op_a",
            "test.add",
            "A",
            payload={"targetId": "A", "deps": ["B"], "level": 1},
            ts="2026-01-01T00:00:02Z",
        ),
        _operation(
            "op_b",
            "test.add",
            "B",
            payload={"targetId": "B", "deps": ["A"], "level": 1},
            ts="2026-01-01T00:00:01Z",
        ),
    ]
    result = generator.generate_sql_with_dependencies(ops)
    assert any("Circular dependencies detected" in warning for warning in result.warnings)
    assert len(result.statements) == 2


@pytest.mark.integration
def test_column_reorder_optimizer_paths() -> None:
    original = ["a", "b", "c", "d"]
    final_single_move = ["a", "c", "b", "d"]
    final_multi_move = ["d", "a", "c", "b"]

    single = ColumnReorderOptimizer.detect_single_column_move(original, final_single_move)
    assert single is not None
    assert single[0] == "c"

    none_same = ColumnReorderOptimizer.detect_single_column_move(original, original)
    assert none_same is None
    none_multi = ColumnReorderOptimizer.detect_single_column_move(original, final_multi_move)
    assert none_multi is None

    distance = ColumnReorderOptimizer.calculate_reorder_distance(original, final_multi_move)
    assert distance > 0
    assert (
        ColumnReorderOptimizer.is_simple_reorder(original, final_single_move, threshold=2) is True
    )
    assert (
        ColumnReorderOptimizer.is_simple_reorder(original, final_multi_move, threshold=1) is False
    )


@pytest.mark.integration
def test_hierarchy_and_exceptions_paths() -> None:
    with pytest.raises(ValueError):
        Hierarchy([])

    hierarchy = Hierarchy(
        [
            HierarchyLevel(
                name="catalog",
                display_name="Catalog",
                plural_name="catalogs",
                icon="db",
                is_container=True,
            ),
            HierarchyLevel(
                name="schema",
                display_name="Schema",
                plural_name="schemas",
                icon="folder",
                is_container=True,
            ),
        ]
    )
    assert hierarchy.get_depth() == 2
    assert hierarchy.get_level(0) is not None
    assert hierarchy.get_level(99) is None
    assert hierarchy.get_level_by_name("schema") is not None
    assert hierarchy.get_level_by_name("missing") is None
    assert hierarchy.get_level_depth("schema") == 1
    assert hierarchy.get_level_depth("missing") == -1

    cycle_error = CircularDependencyError([["a", "b", "a"]])
    assert "Circular dependencies detected" in str(cycle_error)
    missing_error = MissingDependencyError("view_a", "table_b")
    assert "references non-existent object" in str(missing_error)
