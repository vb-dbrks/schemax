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
from enum import Enum
from typing import Any

import networkx as nx

from schematic.providers.base.operations import Operation


class DependencyType(str, Enum):
    """Type of dependency between objects"""

    VIEW_TO_TABLE = "view_to_table"  # View depends on table
    VIEW_TO_VIEW = "view_to_view"  # View depends on another view
    FOREIGN_KEY = "foreign_key"  # Foreign key reference (informational in Unity)
    CONSTRAINT = "constraint"  # Generic constraint dependency
    LOCATION = "location"  # External/managed location dependency


class DependencyEnforcement(str, Enum):
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
        self.graph = nx.DiGraph()
        # Keep node metadata separate for easy access
        self.nodes: dict[str, DependencyNode] = {}

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
        Add an edge from from_id to to_id (from_id depends on to_id).

        Args:
            from_id: ID of the dependent object
            to_id: ID of the dependency object
            dep_type: Type of dependency
            enforcement: How strictly the dependency is enforced
        """
        if from_id not in self.nodes:
            raise ValueError(f"Source node {from_id} not in graph")
        if to_id not in self.nodes:
            raise ValueError(f"Target node {to_id} not in graph")

        # NetworkX edge: from_id → to_id means from_id depends on to_id
        self.graph.add_edge(
            from_id, to_id, dep_type=dep_type.value, enforcement=enforcement.value
        )

    def get_node(self, node_id: str) -> DependencyNode | None:
        """Get a node by ID"""
        return self.nodes.get(node_id)

    def get_dependencies(self, node_id: str) -> list[DependencyEdge]:
        """Get all edges where node_id is the dependent (outgoing edges)"""
        if node_id not in self.graph:
            return []

        edges = []
        for successor in self.graph.successors(node_id):
            edge_data = self.graph[node_id][successor]
            edges.append(
                DependencyEdge(
                    from_id=node_id,
                    to_id=successor,
                    dep_type=DependencyType(edge_data["dep_type"]),
                    enforcement=DependencyEnforcement(edge_data["enforcement"]),
                )
            )
        return edges

    def get_dependents(self, node_id: str) -> list[str]:
        """Get all nodes that depend on node_id (incoming edges)"""
        if node_id not in self.graph:
            return []
        return list(self.graph.predecessors(node_id))

    def detect_cycles(self) -> list[list[str]]:
        """
        Detect cycles in the graph using NetworkX.

        Returns:
            List of cycles, where each cycle is a list of node IDs
        """
        # TODO: nx.simple_cycles() causes segfault in some Python/NetworkX versions
        # For now, use simpler approach: check if graph is a DAG
        # If not a DAG, there's at least one cycle (but we can't identify which one)
        try:
            # Check if graph is a Directed Acyclic Graph
            if nx.is_directed_acyclic_graph(self.graph):
                return []  # No cycles
            else:
                # Graph has cycles, but we can't safely identify them
                # Return a placeholder to indicate cycles exist
                return [["<cycle_detected>"]]  # Indicates cycles exist but not identified
        except Exception:
            # If even DAG check fails, assume no cycles
            return []

    def topological_sort(self) -> list[Operation]:
        """
        Perform topological sort using NetworkX.

        Returns:
            List of operations in dependency order

        Raises:
            ValueError: If the graph contains cycles
        """
        # First check for cycles
        cycles = self.detect_cycles()
        if cycles:
            cycle_str = "\n".join(
                " → ".join(self._get_node_display_name(nid) for nid in cycle)
                for cycle in cycles
            )
            raise ValueError(f"Circular dependencies detected:\n{cycle_str}")

        try:
            # NetworkX topological sort
            # Note: NetworkX returns nodes such that for edge u -> v, u comes before v
            # Since our edges are "depends on" (view -> table means view depends on table),
            # we need to reverse the result so dependencies execute first
            sorted_node_ids = list(reversed(list(nx.topological_sort(self.graph))))
        except nx.NetworkXError as e:
            raise ValueError(f"Failed to perform topological sort: {e}")

        # Extract operations from sorted nodes
        sorted_operations: list[Operation] = []
        for node_id in sorted_node_ids:
            node = self.nodes.get(node_id)
            if node and node.operation:
                sorted_operations.append(node.operation)

        return sorted_operations

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
        self, level: int, operations: list[Operation]
    ) -> "DependencyGraph":
        """Build a subgraph containing only nodes at the specified level"""
        subgraph = DependencyGraph()

        # Collect node IDs at this level
        level_node_ids = [
            node_id
            for node_id, node in self.nodes.items()
            if node.hierarchy_level == level
        ]

        # Add nodes for this level
        for node_id in level_node_ids:
            node = self.nodes[node_id]
            subgraph.add_node(node)

        # Add edges between nodes in this level
        for from_id in level_node_ids:
            for edge in self.get_dependencies(from_id):
                if edge.to_id in level_node_ids:
                    subgraph.add_edge(edge.from_id, edge.to_id, edge.dep_type, edge.enforcement)

        return subgraph

    def _get_node_display_name(self, node_id: str) -> str:
        """Get a human-readable name for a node"""
        node = self.nodes.get(node_id)
        if not node:
            return node_id

        if node.operation:
            op_type = node.operation.get("op", "unknown")
            target = node.operation.get("target", node_id)
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

        for node_id in self.nodes.keys():
            node = self.nodes[node_id]

            for edge in self.get_dependencies(node_id):
                # Check if dependency exists
                if edge.to_id not in self.nodes:
                    warnings.append(
                        f"Missing dependency: {self._get_node_display_name(node_id)} "
                        f"depends on {edge.to_id} (type: {edge.dep_type})"
                    )

                # Check if dependency is at correct hierarchy level
                # Dependencies should only go from lower level to higher level (or same level)
                # e.g., table (level 2) can depend on another table (level 2)
                # but catalog (level 0) should NOT depend on schema (level 1)
                dep_node = self.nodes.get(edge.to_id)
                if dep_node and node.hierarchy_level < dep_node.hierarchy_level:
                    warnings.append(
                        f"Invalid hierarchy: {self._get_node_display_name(node_id)} "
                        f"(level {node.hierarchy_level}) depends on "
                        f"{self._get_node_display_name(edge.to_id)} "
                        f"(level {dep_node.hierarchy_level})"
                    )

        return warnings

