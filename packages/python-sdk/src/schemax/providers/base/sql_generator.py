"""
Base SQL Generator Interface

Defines the contract for generating SQL DDL statements from operations.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from .batching import BatchInfo, OperationBatcher
from .dependency_graph import (
    DependencyEnforcement,
    DependencyGraph,
    DependencyNode,
    DependencyType,
)
from .models import ProviderState
from .operations import Operation
from .optimization import ColumnReorderOptimizer


class StatementInfo(BaseModel):
    """Information about a single SQL statement"""

    sql: str  # The SQL statement
    operation_ids: list[str]  # Operations that generated this statement
    execution_order: int  # Order in which this statement should be executed


class SQLGenerationResult(BaseModel):
    """SQL generation result with explicit operation mapping"""

    sql: str  # Generated SQL statements (combined script)
    statements: list[StatementInfo] = []  # Explicit statement-to-operation mapping
    warnings: list[str] = []  # Warnings or notes
    is_idempotent: bool = True  # Whether SQL is idempotent


class SQLGenerator(ABC):
    """Base SQL Generator interface"""

    def __init__(self, state: ProviderState):
        self.state = state

    @abstractmethod
    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL for a list of operations

        Args:
            ops: Operations to convert to SQL

        Returns:
            SQL script as string
        """

    def generate_sql_with_mapping(self, ops: list[Operation]) -> SQLGenerationResult:
        """
        Generate SQL with explicit operation-to-statement mapping

        Default implementation: calls generate_sql() and returns simple result.
        Providers should override for robust operation tracking.

        Args:
            ops: List of operations to convert to SQL

        Returns:
            SQLGenerationResult with sql and statements mapping
        """
        sql = self.generate_sql(ops)
        # Default: treat entire SQL as one statement with all operations
        statements = []
        if sql and sql.strip():
            statements = [
                StatementInfo(
                    sql=sql, operation_ids=[operation.id for operation in ops], execution_order=1
                )
            ]
        return SQLGenerationResult(sql=sql, statements=statements)

    @abstractmethod
    def generate_sql_for_operation(self, operation: Operation) -> SQLGenerationResult:
        """
        Generate SQL for a single operation

        Args:
            operation: Operation to convert to SQL

        Returns:
            SQL generation result
        """

    @abstractmethod
    def can_generate_sql(self, operation: Operation) -> bool:
        """
        Validate that an operation can be converted to SQL

        Args:
            operation: Operation to validate

        Returns:
            True if operation can be converted to SQL
        """


class BaseSQLGenerator(SQLGenerator):
    """
    Enhanced base implementation with generic optimization algorithms.

    Provides:
    - Operation batching for optimized SQL generation
    - Column reorder optimization
    - Generic utilities (FQN building, escaping)
    - Template pattern for provider-specific SQL generation
    """

    def __init__(self, state: Any, name_mapping: dict[str, str] | None = None):
        """
        Initialize base SQL generator with optimization components.

        Args:
            state: Provider state (catalogs, schemas, tables, etc.) - can be Dict or BaseModel
            name_mapping: Optional name mapping (e.g., logical → physical catalog names)
        """
        super().__init__(state)
        self.name_mapping = name_mapping or {}
        self.batcher = OperationBatcher()
        self.optimizer = ColumnReorderOptimizer()
        self.dependency_graph: DependencyGraph | None = None

    # ====================
    # DEPENDENCY GRAPH
    # ====================

    def _build_dependency_graph(self, ops: list[Operation]) -> DependencyGraph:
        """
        Build dependency graph from operations.

        This analyzes operations to detect dependencies and builds a graph
        that can be used for topological sorting.

        Note: Graph is built per-OBJECT (catalog, schema, table), not per-operation.
        Multiple operations on the same object are tracked separately and returned
        in original order after topological sort.

        Args:
            ops: List of operations to analyze

        Returns:
            DependencyGraph with nodes and edges
        """
        graph = DependencyGraph()

        # Track all operations by target object
        ops_by_target: dict[str, list[Operation]] = {}

        # First pass: Group operations by target object and add nodes
        for operation in ops:
            hierarchy_level = self._get_dependency_level(operation)
            target_id = self._get_target_object_id(operation)

            if not target_id:
                continue  # Skip operations without a clear target

            # Track all operations for this target
            if target_id not in ops_by_target:
                ops_by_target[target_id] = []
            ops_by_target[target_id].append(operation)

            # Add node only once per target object
            if target_id not in graph.nodes:
                node = DependencyNode(
                    id=target_id,
                    type=self._get_object_type_from_operation(operation),
                    hierarchy_level=hierarchy_level,
                    operation=operation,  # Use first operation as representative
                    metadata={"op_type": operation.op, "op_count": 1},
                )
                graph.add_node(node)
            else:
                # Update op count for metadata
                node = graph.nodes[target_id]
                if node and node.metadata:
                    node.metadata["op_count"] += 1

        # Second pass: Extract and add dependencies
        for operation in ops:
            target_id = self._get_target_object_id(operation)
            if not target_id:
                continue

            # Extract dependencies for this operation
            dependencies = self._extract_operation_dependencies(operation)

            for dep_id, dep_type, enforcement in dependencies:
                # Add dependency edge if both nodes exist
                # Note: We add edge FROM dependency TO dependent (reversed direction)
                # This is because NetworkX topological sort returns nodes such that
                # for edge u -> v, u comes before v. So we want table -> view,
                # not view -> table.
                if target_id in graph.nodes and dep_id in graph.nodes:
                    graph.add_edge(dep_id, target_id, dep_type, enforcement)

        # Store operation mapping for topological sort
        graph.metadata["ops_by_target"] = ops_by_target

        self.dependency_graph = graph
        return graph

    def _get_object_type_from_operation(self, operation: Operation) -> str:
        """
        Infer object type from operation type.

        Args:
            operation: Operation to analyze

        Returns:
            Object type string (e.g., "catalog", "schema", "table", "view")
        """
        op_type = operation.op

        if "catalog" in op_type:
            return "catalog"
        if "schema" in op_type:
            return "schema"
        if "table" in op_type:
            return "table"
        if "view" in op_type:
            return "view"
        if "column" in op_type:
            return "column"
        if "constraint" in op_type:
            return "constraint"
        return "unknown"

    def generate_sql_with_dependencies(self, ops: list[Operation]) -> SQLGenerationResult:
        """
        Generate SQL with dependency-aware ordering.

        This is the enhanced version of generate_sql that builds a dependency
        graph and uses topological sort to ensure correct execution order.

        Args:
            ops: Operations to convert to SQL

        Returns:
            SQLGenerationResult with dependency-sorted SQL statements
        """
        if not ops:
            return SQLGenerationResult(sql="", statements=[], warnings=[])

        warnings: list[str] = []

        # Build dependency graph
        try:
            graph = self._build_dependency_graph(ops)

            # Detect breaking changes (warn about dropping objects with dependents)
            breaking_warnings = self._detect_breaking_changes(ops, graph)
            warnings.extend(breaking_warnings)

            # Detect cycles (will raise ValueError if found)
            cycles = graph.detect_cycles()
            if cycles:
                cycle_str = "\n".join(" → ".join(str(nid) for nid in cycle) for cycle in cycles)
                warnings.append(f"Circular dependencies detected:\n{cycle_str}")
                # Fall back to level-based sorting
                sorted_ops = self._sort_operations_by_level(ops)
            else:
                # Use topological sort for optimal ordering
                sorted_ops = graph.topological_sort()

            # Validate dependencies
            dep_warnings = graph.validate_dependencies()
            warnings.extend(dep_warnings)

        except Exception as e:
            # If dependency analysis fails, fall back to level-based sorting
            warnings.append(f"Dependency analysis failed: {e}. Using level-based sorting.")
            sorted_ops = self._sort_operations_by_level(ops)

        # Generate SQL for sorted operations
        statements = []
        for idx, operation in enumerate(sorted_ops):
            if not self.can_generate_sql(operation):
                warnings.append(f"Cannot generate SQL for operation: {operation.op}")
                continue

            result = self.generate_sql_for_operation(operation)

            # Add header comment with operation metadata
            header = f"-- Operation: {operation.id} ({operation.ts})\n"
            header += f"-- Type: {operation.op}"

            # Add to statements list
            statements.append(
                StatementInfo(
                    sql=result.sql,
                    operation_ids=[operation.id],
                    execution_order=idx + 1,
                )
            )

            warnings.extend(result.warnings)

        # Combine all SQL statements
        combined_sql = "\n\n".join(
            f"-- Statement {stmt.execution_order}\n{stmt.sql};" for stmt in statements
        )

        return SQLGenerationResult(
            sql=combined_sql, statements=statements, warnings=warnings, is_idempotent=True
        )

    def _sort_operations_by_level(self, ops: list[Operation]) -> list[Operation]:
        """
        Sort operations by hierarchy level and timestamp (fallback method).

        Args:
            ops: Operations to sort

        Returns:
            Sorted operations
        """
        return sorted(
            ops, key=lambda operation: (self._get_dependency_level(operation), operation.ts)
        )

    def _detect_breaking_changes(
        self, ops: list[Operation], graph: DependencyGraph | None = None
    ) -> list[str]:
        """
        Detect breaking changes from operations (e.g., dropping objects with dependents).

        Args:
            ops: Operations to analyze
            graph: Optional dependency graph (if already built)

        Returns:
            List of warning messages about breaking changes
        """
        warnings: list[str] = []

        # Build graph if not provided
        if graph is None:
            try:
                graph = self._build_dependency_graph(ops)
            except Exception:
                # If graph building fails, return empty warnings
                return warnings

        # Check each drop operation for breaking changes
        for operation in ops:
            op_type = operation.op

            # Detect drop operations
            is_drop = (
                "drop" in op_type.lower()
                or "remove" in op_type.lower()
                or "delete" in op_type.lower()
            )

            if is_drop:
                target_id = self._get_target_object_id(operation)
                if target_id and graph:
                    # Get dependents
                    dependents = graph.get_breaking_changes(target_id)

                    if dependents:
                        # Create warning message
                        object_name = self._get_object_display_name_from_op(operation)
                        dependent_names = [
                            graph._get_node_display_name(dep_id) for dep_id in dependents
                        ]

                        warning = (
                            f"⚠️  Breaking change: Dropping {object_name} will affect "
                            f"{len(dependents)} dependent object(s):\n"
                        )
                        for dep_name in dependent_names:
                            warning += f"  • {dep_name}\n"

                        warnings.append(warning.rstrip())

        return warnings

    def _get_object_display_name_from_op(self, operation: Operation) -> str:
        """
        Get human-readable object name from operation.

        Args:
            operation: Operation

        Returns:
            Display name (e.g., "table my_table", "view my_view")
        """
        op_type = operation.op
        target_id = operation.target

        # Extract object type from operation type
        if "table" in op_type:
            obj_type = "table"
        elif "view" in op_type:
            obj_type = "view"
        elif "schema" in op_type:
            obj_type = "schema"
        elif "catalog" in op_type:
            obj_type = "catalog"
        else:
            obj_type = "object"

        # Try to get name from payload
        name = operation.payload.get("name", target_id) if operation.payload else target_id

        return f"{obj_type} '{name}'"

    # ====================
    # GENERIC UTILITIES
    # ====================

    def _build_fqn(self, *parts: str, separator: str = ".") -> str:
        """
        Build fully-qualified name with each part escaped separately.

        Generic utility that works for any provider. Separator is customizable
        (e.g., "." for most SQL databases, "::" for some systems).

        Args:
            *parts: Name parts (catalog, schema, table, column, etc.)
            separator: Separator between parts (default: ".")

        Returns:
            Escaped FQN like `catalog`.`schema`.`table`

        Example:
            >>> self._build_fqn("my_catalog", "my_schema", "my_table")
            '`my_catalog`.`my_schema`.`my_table`'
        """
        return separator.join(self.escape_identifier(part) for part in parts if part)

    # ====================
    # ABSTRACT METHODS - Providers must implement
    # ====================

    @abstractmethod
    def _get_target_object_id(self, operation: Operation) -> str | None:
        """
        Extract target object ID from operation.

        Provider-specific logic to determine which object an operation targets.
        Used by batching algorithm to group operations.

        Args:
            operation: Operation to analyze

        Returns:
            Object ID (e.g., table_id, schema_id) or None if no target

        Example (Unity):
            >>> def _get_target_object_id(self, operation):
            ...     if operation.op == "unity.add_table":
            ...         return operation.target
            ...     elif operation.op == "unity.add_column":
            ...         return operation.payload.get("tableId")
            ...     return None
        """

    @abstractmethod
    def _is_create_operation(self, operation: Operation) -> bool:
        """
        Check if operation creates a new object.

        Used by batching to distinguish CREATE from ALTER operations.

        Args:
            operation: Operation to check

        Returns:
            True if operation creates new object (e.g., add_table, add_schema)

        Example (Unity):
            >>> def _is_create_operation(self, operation):
            ...     return operation.op in [
            ...         "unity.add_catalog",
            ...         "unity.add_schema",
            ...         "unity.add_table"
            ...     ]
        """

    @abstractmethod
    def _is_drop_operation(self, operation: Operation) -> bool:
        """
        Check if operation drops/deletes an object.

        Used to handle DROP operations separately from batching since they
        cannot be batched with CREATE/ALTER operations.

        Args:
            operation: Operation to check

        Returns:
            True if operation drops an object (e.g., drop_table, drop_schema, drop_catalog)

        Example (Unity):
            >>> def _is_drop_operation(self, operation):
            ...     return operation.op in [
            ...         "unity.drop_catalog",
            ...         "unity.drop_schema",
            ...         "unity.drop_table"
            ...     ]
        """

    @abstractmethod
    def _get_dependency_level(self, operation: Operation) -> int:
        """
        Get dependency level for operation ordering.

        Lower numbers execute first (e.g., 0=catalog, 1=schema, 2=table).
        Ensures proper execution order (catalog before schema before table).

        Args:
            operation: Operation to check

        Returns:
            Dependency level (0 = highest priority, execute first)

        Example (Unity):
            >>> def _get_dependency_level(self, operation):
            ...     if "catalog" in operation.op:
            ...         return 0
            ...     elif "schema" in operation.op:
            ...         return 1
            ...     elif "add_table" in operation.op:
            ...         return 2
            ...     else:
            ...         return 3
        """

    def _extract_operation_dependencies(
        self, _operation: Operation
    ) -> list[tuple[str, DependencyType, DependencyEnforcement]]:
        """
        Extract dependencies from an operation.

        This method should be overridden by providers for operations that have
        dependencies (e.g., views depending on tables/views, foreign keys).

        Args:
            operation: Operation to analyze

        Returns:
            List of tuples: (dependency_id, dependency_type, enforcement)

        Example (Unity - for views):
            >>> def _extract_operation_dependencies(self, operation):
            ...     if operation.op == "unity.add_view":
            ...         # Parse SQL to extract table/view dependencies
            ...         deps = parse_view_definition(operation.payload.get("definition"))
            ...         return [
            ...             (table_id, DependencyType.VIEW_TO_TABLE, DependencyEnforcement.ENFORCED)
            ...             for table_id in deps
            ...         ]
            ...     return []
        """
        # Default: no dependencies
        # Providers should override for view operations, foreign keys, etc.
        return []

    @abstractmethod
    def _generate_batched_create_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate CREATE statement with batched operations.

        For new objects, generate complete CREATE with all properties/columns
        included (not empty CREATE + multiple ALTERs).

        Args:
            object_id: ID of object being created
            batch_info: Batch of operations for this object

        Returns:
            SQL CREATE statement

        Example (Unity):
            >>> def _generate_batched_create_sql(self, object_id, batch_info):
            ...     # Generate: CREATE TABLE ... (col1 type1, col2 type2) USING DELTA
            ...     # Instead of: CREATE TABLE ... (); ALTER TABLE ADD col1; ALTER TABLE ADD col2;
        """

    @abstractmethod
    def _generate_batched_alter_sql(self, object_id: str, batch_info: BatchInfo) -> str:
        """
        Generate ALTER statements for batched operations on existing object.

        Args:
            object_id: ID of object to alter
            batch_info: Batch of modification operations

        Returns:
            SQL ALTER statements (may be multiple, separated by `;`)
        """

    # ====================
    # DEFAULT IMPLEMENTATION (can be overridden)
    # ====================

    def generate_sql(self, ops: list[Operation]) -> str:
        """
        Generate SQL with basic operation-by-operation approach.

        This is the default implementation. Providers can override to use
        batching optimization by calling self.batcher.batch_operations().
        """
        statements = []

        for operation in ops:
            if not self.can_generate_sql(operation):
                print(f"Warning: Cannot generate SQL for operation: {operation.op}")
                continue

            result = self.generate_sql_for_operation(operation)

            # Add header comment with operation metadata
            header = f"-- Operation: {operation.id} ({operation.ts})\n-- Type: {operation.op}"

            # Add warnings if any
            warnings_comment = ""
            if result.warnings:
                warnings_comment = f"\n-- Warnings: {', '.join(result.warnings)}"

            statements.append(f"{header}{warnings_comment}\n{result.sql};")

        return "\n\n".join(statements)

    @staticmethod
    def escape_identifier(identifier: str) -> str:
        """Helper to escape SQL identifiers"""
        return f"`{identifier.replace('`', '``')}`"

    @staticmethod
    def escape_string(value: str) -> str:
        """Helper to escape SQL string literals"""
        return value.replace("'", "''")
