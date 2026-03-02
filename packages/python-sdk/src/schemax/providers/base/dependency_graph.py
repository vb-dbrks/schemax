"""
Dependency Graph for SQL Generation

Implements a directed acyclic graph (DAG) for tracking object dependencies
and ordering operations based on those dependencies using NetworkX.

Key features:
- Hierarchical graph structure (respects provider hierarchy levels)
- Cycle detection with detailed error paths
- Topological sort using NetworkX algorithms
- Support for multiple dependency types (enforced vs warning)
- Optimized for large graphs (2000+ objects)
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import networkx as nx

from schemax.providers.base.operations import Operation


class DependencyType(StrEnum):
    """Type of dependency between objects"""

    VIEW_TO_TABLE = "view_to_table"  # View depends on table
    VIEW_TO_VIEW = "view_to_view"  # View depends on another view
    FOREIGN_KEY = "foreign_key"  # Foreign key reference (informational in Unity)
    CONSTRAINT = "constraint"  # Generic constraint dependency
    CONSTRAINT_ORDERING = "constraint_ordering"  # ADD constraint depends on DROP constraint
    LOCATION = "location"  # External/managed location dependency


class DependencyEnforcement(StrEnum):
    """How strictly a dependency is enforced"""

    ENFORCED = "enforced"  # Hard dependency - must be satisfied
    WARNING = "warning"  # Soft dependency - warn but allow
    NONE = "none"  # Not enforced - informational only


@dataclass
class DependencyNode:
    """Node in the dependency graph"""

    id: str  # Object ID (e.g., table_abc123, view_xyz789)
    type: str  # Object type: catalog, schema, table, view, constraint
    hierarchy_level: int  # 0=catalog, 1=schema, 2=table/view, 3=modifications
    operation: Operation | None = None  # Associated operation (if any)
    metadata: dict[str, Any] | None = None  # Additional metadata


@dataclass
class DependencyEdge:
    """Edge in the dependency graph"""

    from_id: str  # Dependent object ID
    to_id: str  # Dependency object ID
    dep_type: DependencyType  # Type of dependency
    enforcement: DependencyEnforcement  # How strictly enforced


class DependencyGraph:
    """
    Directed acyclic graph for tracking object dependencies using NetworkX.

    This graph is hierarchical: nodes are organized by hierarchy levels
    (catalog → schema → table/view → modifications), and dependencies
    are tracked both within and across levels.
    """

    def __init__(self) -> None:
        # Use NetworkX directed graph
        self.graph: nx.DiGraph = nx.DiGraph()
        # Keep node metadata separate for easy access
        self.nodes: dict[str, DependencyNode] = {}
        # Graph-level metadata (for operations tracking, etc.)
        self.metadata: dict[str, Any] = {}

    def add_node(self, node: DependencyNode) -> None:
        """Add a node to the graph"""
        if node.id in self.nodes:
            raise ValueError(f"Node {node.id} already exists in graph")

        self.nodes[node.id] = node
        self.graph.add_node(
            node.id,
            type=node.type,
            hierarchy_level=node.hierarchy_level,
            operation=node.operation,
            metadata=node.metadata or {},
        )

    def add_edge(
        self,
        from_id: str,
        to_id: str,
        dep_type: DependencyType,
        enforcement: DependencyEnforcement = DependencyEnforcement.ENFORCED,
    ) -> None:
        """
        Add a dependency edge from from_id to to_id.

        Edge direction: from_id must be created BEFORE to_id (to_id depends on from_id).
        Example: add_edge("table_1", "view_1") means view_1 depends on table_1,
                 so table_1 must be created first.

        Args:
            from_id: ID of the dependency object (must be created first)
            to_id: ID of the dependent object (depends on from_id)
            dep_type: Type of dependency
            enforcement: How strictly the dependency is enforced
        """
        if from_id not in self.nodes:
            raise ValueError(f"Source node {from_id} not in graph")
        if to_id not in self.nodes:
            raise ValueError(f"Target node {to_id} not in graph")

        # NetworkX edge: from_id → to_id means from_id must execute before to_id
        self.graph.add_edge(from_id, to_id, dep_type=dep_type.value, enforcement=enforcement.value)

    def get_node(self, node_id: str) -> DependencyNode | None:
        """Get a node by ID"""
        return self.nodes.get(node_id)

    def get_dependencies(self, node_id: str) -> list[DependencyEdge]:
        """
        Get all dependencies of node_id (what does node_id depend on?).

        Since our edges are FROM dependency TO dependent (table -> view),
        we need to get INCOMING edges (predecessors).
        """
        if node_id not in self.graph:
            return []

        edges = []
        for predecessor in self.graph.predecessors(node_id):
            edge_data = self.graph[predecessor][node_id]
            edges.append(
                DependencyEdge(
                    from_id=node_id,
                    to_id=predecessor,
                    dep_type=DependencyType(edge_data["dep_type"]),
                    enforcement=DependencyEnforcement(edge_data["enforcement"]),
                )
            )
        return edges

    def get_dependents(self, node_id: str) -> list[str]:
        """
        Get all nodes that depend on node_id (who depends on this node?).

        Since our edges are FROM dependency TO dependent (table -> view),
        we need to get OUTGOING edges (successors).
        """
        if node_id not in self.graph:
            return []
        return list(self.graph.successors(node_id))

    def detect_cycles(self) -> list[list[str]]:
        """
        Detect cycles in the graph using NetworkX.

        Returns:
            List of cycles, where each cycle is a list of node IDs
        """
        try:
            # NetworkX simple_cycles() works correctly with NetworkX 3.5+
            # Returns a generator of cycles
            return list(nx.simple_cycles(self.graph))
        except Exception:
            # If cycle detection fails, return empty list
            return []

    def topological_sort(self) -> list[Operation]:
        """
        Perform topological sort using NetworkX with timestamp-stable ordering.

        For nodes at the same level with no dependencies between them,
        they are ordered by timestamp (earliest first).

        Returns:
            List of operations in dependency order.
            For objects with multiple operations, they are returned in original order.

        Raises:
            ValueError: If the graph contains cycles
        """
        # First check for cycles
        cycles = self.detect_cycles()
        if cycles:
            cycle_str = "\n".join(
                " → ".join(self.get_node_display_name(nid) for nid in cycle) for cycle in cycles
            )
            raise ValueError(f"Circular dependencies detected:\n{cycle_str}")

        sorted_node_ids = self._topological_sort_node_ids()

        # Extract operations from sorted nodes
        # For each node, return ALL operations (not just the first one)
        sorted_operations: list[Operation] = []
        ops_by_target = self.metadata.get("ops_by_target", {})

        for node_id in sorted_node_ids:
            # If we have multiple operations for this object, add them all
            # Sort them by timestamp to maintain temporal order
            if node_id in ops_by_target:
                ops_for_node = sorted(ops_by_target[node_id], key=self._operation_timestamp)
                sorted_operations.extend(ops_for_node)
            else:
                # Fallback: use single operation from node
                node = self.nodes.get(node_id)
                if node and node.operation:
                    sorted_operations.append(node.operation)

        return sorted_operations

    def _operation_timestamp(self, operation: Operation | dict) -> str:
        """Get operation timestamp for deterministic sorting."""
        if hasattr(operation, "ts"):
            return str(operation.ts)
        if isinstance(operation, dict):
            return str(operation.get("ts", ""))
        return ""

    def _sort_key(self, node_id: str) -> tuple[int, str]:
        """Sort key for lexicographical topological sort."""
        node = self.nodes.get(node_id)
        if not node or not node.operation:
            return (999, "9999-99-99T99:99:99Z")
        if hasattr(node.operation, "ts"):
            timestamp = node.operation.ts
        elif isinstance(node.operation, dict):
            timestamp = node.operation.get("ts", "9999-99-99T99:99:99Z")
        else:
            timestamp = "9999-99-99T99:99:99Z"
        return (node.hierarchy_level, timestamp)

    def _topological_sort_node_ids(self) -> list[str]:
        """Return topologically sorted node IDs with stable ordering."""
        try:
            return list(nx.lexicographical_topological_sort(self.graph, key=self._sort_key))
        except nx.NetworkXError as err:
            raise ValueError(f"Failed to perform topological sort: {err}") from err

    def topological_sort_by_level(
        self, operations_by_level: dict[int, list[Operation]]
    ) -> list[Operation]:
        """
        Perform topological sort within each hierarchy level independently.

        This respects the provider hierarchy (catalog → schema → table → modifications)
        while also respecting dependencies within each level (e.g., view A depends on view B).

        Args:
            operations_by_level: Operations grouped by hierarchy level

        Returns:
            List of operations in dependency order
        """
        result: list[Operation] = []

        # Process each level in order
        for level in sorted(operations_by_level.keys()):
            level_ops = operations_by_level[level]

            # Build subgraph for this level only
            level_graph = self._build_subgraph_for_level(level, level_ops)

            # Topological sort within this level
            sorted_level_ops = level_graph.topological_sort()

            result.extend(sorted_level_ops)

        return result

    def _build_subgraph_for_level(
        self, level: int, _operations: list[Operation]
    ) -> "DependencyGraph":
        """Build a subgraph containing only nodes at the specified level"""
        subgraph = DependencyGraph()

        # Collect node IDs at this level
        level_node_ids = [
            node_id for node_id, node in self.nodes.items() if node.hierarchy_level == level
        ]

        # Add nodes for this level
        for node_id in level_node_ids:
            node = self.nodes[node_id]
            subgraph.add_node(node)

        # Add edges between nodes in this level
        # Note: get_dependencies returns edges with from_id=node, to_id=dependency
        # But our graph stores edges as dependency -> dependent
        # So we need to add edges in the correct direction: to_id -> from_id
        for from_id in level_node_ids:
            for edge in self.get_dependencies(from_id):
                if edge.to_id in level_node_ids:
                    # edge.from_id is the dependent, edge.to_id is the dependency
                    # Add edge: dependency -> dependent
                    subgraph.add_edge(edge.to_id, edge.from_id, edge.dep_type, edge.enforcement)

        return subgraph

    def get_node_display_name(self, node_id: str) -> str:
        """Return a human-readable name for a graph node (for messages and errors)."""
        node = self.nodes.get(node_id)
        if not node:
            return node_id

        if node.operation:
            op_type = node.operation.op if hasattr(node.operation, "op") else "unknown"
            target = node.operation.target if hasattr(node.operation, "target") else node_id
            return f"{op_type}({target})"

        return f"{node.type}({node_id})"

    def get_breaking_changes(self, drop_operation_id: str) -> list[str]:
        """
        Get list of objects that will be affected if the specified object is dropped.

        Args:
            drop_operation_id: ID of the object being dropped

        Returns:
            List of dependent object IDs that will be affected
        """
        return self.get_dependents(drop_operation_id)

    def validate_dependencies(self) -> list[str]:
        """
        Validate all dependencies in the graph.

        Returns:
            List of validation warnings (empty if all dependencies are satisfied)
        """
        warnings: list[str] = []

        for node_id, node in self.nodes.items():
            for edge in self.get_dependencies(node_id):
                # Check if dependency exists
                if edge.to_id not in self.nodes:
                    warnings.append(
                        f"Missing dependency: {self.get_node_display_name(node_id)} "
                        f"depends on {edge.to_id} (type: {edge.dep_type})"
                    )

                # Check if dependency is at correct hierarchy level
                # Dependencies should only go from lower level to higher level (or same level)
                # e.g., table (level 2) can depend on another table (level 2)
                # but catalog (level 0) should NOT depend on schema (level 1)
                dep_node = self.nodes.get(edge.to_id)
                if dep_node and node.hierarchy_level < dep_node.hierarchy_level:
                    warnings.append(
                        f"Invalid hierarchy: {self.get_node_display_name(node_id)} "
                        f"(level {node.hierarchy_level}) depends on "
                        f"{self.get_node_display_name(edge.to_id)} "
                        f"(level {dep_node.hierarchy_level})"
                    )

        return warnings
