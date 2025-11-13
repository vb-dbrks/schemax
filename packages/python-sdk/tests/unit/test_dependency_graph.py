"""
Unit tests for dependency graph, cycle detection, and topological sort

Tests the NetworkX-based dependency graph implementation for SQL generation.
"""

import pytest

from schematic.providers.base.dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)


class TestDependencyGraphBasics:
    """Test basic dependency graph operations"""

    def test_create_empty_graph(self):
        """Test creating an empty graph"""
        graph = DependencyGraph()

        assert len(graph.nodes) == 0
        assert len(graph.graph.nodes) == 0

    def test_add_single_node(self):
        """Test adding a single node"""
        graph = DependencyGraph()

        node = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        graph.add_node(node)

        assert len(graph.nodes) == 1
        assert "table_1" in graph.nodes
        assert graph.get_node("table_1") == node

    def test_add_duplicate_node_raises_error(self):
        """Test that adding duplicate node raises ValueError"""
        graph = DependencyGraph()

        node1 = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        graph.add_node(node1)

        node2 = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        with pytest.raises(ValueError, match="already exists"):
            graph.add_node(node2)

    def test_add_edge(self):
        """Test adding an edge between nodes"""
        graph = DependencyGraph()

        # Add nodes
        node1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        node2 = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        graph.add_node(node1)
        graph.add_node(node2)

        # Add edge: table_1 must be created before view_1 (view_1 depends on table_1)
        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)

        # Check edge exists
        deps = graph.get_dependencies("view_1")
        assert len(deps) == 1
        assert deps[0].from_id == "view_1"
        assert deps[0].to_id == "table_1"
        assert deps[0].dep_type == DependencyType.VIEW_TO_TABLE

    def test_add_edge_to_nonexistent_node_raises_error(self):
        """Test that adding edge to nonexistent node raises ValueError"""
        graph = DependencyGraph()

        node1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        graph.add_node(node1)

        with pytest.raises(ValueError, match="not in graph"):
            graph.add_edge("nonexistent", "view_1", DependencyType.VIEW_TO_TABLE)

    def test_get_dependents(self):
        """Test getting all nodes that depend on a given node"""
        graph = DependencyGraph()

        # Create: view_1 → table_1, view_2 → table_1
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view1)
        graph.add_node(view2)

        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)
        graph.add_edge("table_1", "view_2", DependencyType.VIEW_TO_TABLE)

        # table_1 has two dependents
        dependents = graph.get_dependents("table_1")
        assert len(dependents) == 2
        assert "view_1" in dependents
        assert "view_2" in dependents


class TestCycleDetection:
    """Test cycle detection in dependency graph"""

    def test_no_cycles_in_simple_graph(self):
        """Test that simple DAG has no cycles"""
        graph = DependencyGraph()

        # Create: view_1 → table_1
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view = DependencyNode(id="view_1", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view)
        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)

        cycles = graph.detect_cycles()
        assert len(cycles) == 0

    def test_detect_simple_cycle(self):
        """Test detecting a simple 2-node cycle"""
        graph = DependencyGraph()

        # Create cycle: view_1 → view_2 → view_1
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2)

        graph.add_node(view1)
        graph.add_node(view2)

        graph.add_edge("view_1", "view_2", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("view_2", "view_1", DependencyType.VIEW_TO_VIEW)

        cycles = graph.detect_cycles()
        assert len(cycles) >= 1
        # Cycle should contain both nodes
        cycle = cycles[0]
        assert "view_1" in cycle
        assert "view_2" in cycle

    def test_detect_three_node_cycle(self):
        """Test detecting a 3-node cycle"""
        graph = DependencyGraph()

        # Create cycle: view_1 → view_2 → view_3 → view_1
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2)
        view3 = DependencyNode(id="view_3", type="view", hierarchy_level=2)

        graph.add_node(view1)
        graph.add_node(view2)
        graph.add_node(view3)

        graph.add_edge("view_1", "view_2", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("view_2", "view_3", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("view_3", "view_1", DependencyType.VIEW_TO_VIEW)

        cycles = graph.detect_cycles()
        assert len(cycles) >= 1
        # Cycle should contain all three nodes
        cycle = cycles[0]
        assert "view_1" in cycle
        assert "view_2" in cycle
        assert "view_3" in cycle

    def test_no_cycles_in_complex_dag(self):
        """Test complex DAG with multiple paths has no cycles"""
        graph = DependencyGraph()

        # Create diamond structure: view_1 → view_2, view_1 → view_3, view_2 → table, view_3 → table
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2)
        view3 = DependencyNode(id="view_3", type="view", hierarchy_level=2)

        for node in [table, view1, view2, view3]:
            graph.add_node(node)

        graph.add_edge("view_2", "view_1", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("view_3", "view_1", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("table_1", "view_2", DependencyType.VIEW_TO_TABLE)
        graph.add_edge("table_1", "view_3", DependencyType.VIEW_TO_TABLE)

        cycles = graph.detect_cycles()
        assert len(cycles) == 0


class TestTopologicalSort:
    """Test topological sort for dependency ordering"""

    def test_topological_sort_simple_chain(self):
        """Test topological sort on a simple dependency chain"""
        graph = DependencyGraph()

        # Create chain: view_1 → view_2 → table_1
        op1 = {"id": "op1", "op": "add_view", "target": "view_1"}
        op2 = {"id": "op2", "op": "add_view", "target": "view_2"}
        op3 = {"id": "op3", "op": "add_table", "target": "table_1"}

        table = DependencyNode(id="table_1", type="table", hierarchy_level=2, operation=op3)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2, operation=op2)
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2, operation=op1)

        graph.add_node(table)
        graph.add_node(view2)
        graph.add_node(view1)

        graph.add_edge("view_2", "view_1", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("table_1", "view_2", DependencyType.VIEW_TO_TABLE)

        # Topological sort should order: table_1, view_2, view_1
        sorted_ops = graph.topological_sort()

        assert len(sorted_ops) == 3
        # Find indices
        table_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "table_1")
        view2_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "view_2")
        view1_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "view_1")

        # Check ordering: table < view2 < view1
        assert table_idx < view2_idx
        assert view2_idx < view1_idx

    def test_topological_sort_with_cycle_raises_error(self):
        """Test that topological sort raises error when cycle exists"""
        graph = DependencyGraph()

        # Create cycle: view_1 → view_2 → view_1
        op1 = {"id": "op1", "op": "add_view", "target": "view_1"}
        op2 = {"id": "op2", "op": "add_view", "target": "view_2"}

        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2, operation=op1)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2, operation=op2)

        graph.add_node(view1)
        graph.add_node(view2)

        graph.add_edge("view_2", "view_1", DependencyType.VIEW_TO_VIEW)
        graph.add_edge("view_1", "view_2", DependencyType.VIEW_TO_VIEW)

        # Should raise ValueError due to cycle
        with pytest.raises(ValueError, match="Circular dependencies"):
            graph.topological_sort()

    def test_topological_sort_parallel_branches(self):
        """Test topological sort with parallel branches"""
        graph = DependencyGraph()

        # Create: view_1 → table_1, view_2 → table_1 (parallel)
        op_table = {"id": "op_t", "op": "add_table", "target": "table_1"}
        op_v1 = {"id": "op_v1", "op": "add_view", "target": "view_1"}
        op_v2 = {"id": "op_v2", "op": "add_view", "target": "view_2"}

        table = DependencyNode(id="table_1", type="table", hierarchy_level=2, operation=op_table)
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2, operation=op_v1)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2, operation=op_v2)

        graph.add_node(table)
        graph.add_node(view1)
        graph.add_node(view2)

        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)
        graph.add_edge("table_1", "view_2", DependencyType.VIEW_TO_TABLE)

        sorted_ops = graph.topological_sort()

        assert len(sorted_ops) == 3
        # table_1 must come before both views
        table_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "table_1")
        view1_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "view_1")
        view2_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "view_2")

        assert table_idx < view1_idx
        assert table_idx < view2_idx


class TestBreakingChanges:
    """Test breaking change detection"""

    def test_detect_dependents_for_breaking_changes(self):
        """Test detecting objects that will be affected by dropping a table"""
        graph = DependencyGraph()

        # Create: view_1 → table_1, view_2 → table_1
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view1 = DependencyNode(id="view_1", type="view", hierarchy_level=2)
        view2 = DependencyNode(id="view_2", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view1)
        graph.add_node(view2)

        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)
        graph.add_edge("table_1", "view_2", DependencyType.VIEW_TO_TABLE)

        # Dropping table_1 will affect view_1 and view_2
        affected = graph.get_breaking_changes("table_1")

        assert len(affected) == 2
        assert "view_1" in affected
        assert "view_2" in affected

    def test_no_breaking_changes_for_leaf_node(self):
        """Test that dropping a leaf node has no breaking changes"""
        graph = DependencyGraph()

        # Create: view_1 → table_1
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view = DependencyNode(id="view_1", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view)

        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)

        # Dropping view_1 has no breaking changes (it's a leaf)
        affected = graph.get_breaking_changes("view_1")
        assert len(affected) == 0


class TestDependencyValidation:
    """Test dependency validation"""

    def test_validate_all_dependencies_satisfied(self):
        """Test validation when all dependencies are satisfied"""
        graph = DependencyGraph()

        # Create: view_1 → table_1
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view = DependencyNode(id="view_1", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view)
        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)

        warnings = graph.validate_dependencies()
        assert len(warnings) == 0

    def test_validate_detects_invalid_hierarchy(self):
        """Test validation detects invalid hierarchy (higher level depending on lower)"""
        graph = DependencyGraph()

        # Create invalid: schema (level 1) → table (level 2) - WRONG!
        # This means "create table before schema" which violates hierarchy
        schema = DependencyNode(id="schema_1", type="schema", hierarchy_level=1)
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)

        graph.add_node(schema)
        graph.add_node(table)

        # This is invalid - table must be created before schema
        # (violates hierarchy: schema is level 1, table is level 2)
        graph.add_edge("table_1", "schema_1", DependencyType.CONSTRAINT)

        warnings = graph.validate_dependencies()
        assert len(warnings) > 0
        assert any("Invalid hierarchy" in w for w in warnings)


class TestHierarchicalSort:
    """Test hierarchical sorting (by level)"""

    def test_sort_by_level(self):
        """Test sorting operations within hierarchy levels"""
        graph = DependencyGraph()

        # Create nodes at different levels
        op_cat = {"id": "op_cat", "op": "add_catalog", "target": "catalog_1"}
        op_sch = {"id": "op_sch", "op": "add_schema", "target": "schema_1"}
        op_tbl = {"id": "op_tbl", "op": "add_table", "target": "table_1"}
        op_view = {"id": "op_view", "op": "add_view", "target": "view_1"}

        catalog = DependencyNode(
            id="catalog_1", type="catalog", hierarchy_level=0, operation=op_cat
        )
        schema = DependencyNode(id="schema_1", type="schema", hierarchy_level=1, operation=op_sch)
        table = DependencyNode(id="table_1", type="table", hierarchy_level=2, operation=op_tbl)
        view = DependencyNode(id="view_1", type="view", hierarchy_level=2, operation=op_view)

        graph.add_node(catalog)
        graph.add_node(schema)
        graph.add_node(table)
        graph.add_node(view)

        # Add view depends on table
        graph.add_edge("table_1", "view_1", DependencyType.VIEW_TO_TABLE)

        # Group operations by level
        ops_by_level = {
            0: [op_cat],
            1: [op_sch],
            2: [op_tbl, op_view],
        }

        sorted_ops = graph.topological_sort_by_level(ops_by_level)

        # Should be: catalog, schema, table, view
        assert len(sorted_ops) == 4
        assert sorted_ops[0]["target"] == "catalog_1"
        assert sorted_ops[1]["target"] == "schema_1"

        # Within level 2, table should come before view
        table_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "table_1")
        view_idx = next(i for i, op in enumerate(sorted_ops) if op["target"] == "view_1")
        assert table_idx < view_idx


class TestDependencyEnforcement:
    """Test different dependency enforcement levels"""

    def test_add_enforced_dependency(self):
        """Test adding an enforced dependency"""
        graph = DependencyGraph()

        table = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        view = DependencyNode(id="view_1", type="view", hierarchy_level=2)

        graph.add_node(table)
        graph.add_node(view)

        graph.add_edge(
            "table_1", "view_1", DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED
        )

        deps = graph.get_dependencies("view_1")
        assert len(deps) == 1
        assert deps[0].enforcement == DependencyEnforcement.ENFORCED

    def test_add_warning_dependency(self):
        """Test adding a warning-level dependency"""
        graph = DependencyGraph()

        table1 = DependencyNode(id="table_1", type="table", hierarchy_level=2)
        table2 = DependencyNode(id="table_2", type="table", hierarchy_level=2)

        graph.add_node(table1)
        graph.add_node(table2)

        # Foreign key (not enforced in Unity Catalog)
        graph.add_edge(
            "table_2", "table_1", DependencyType.FOREIGN_KEY, DependencyEnforcement.WARNING
        )

        deps = graph.get_dependencies("table_1")
        assert len(deps) == 1
        assert deps[0].enforcement == DependencyEnforcement.WARNING
