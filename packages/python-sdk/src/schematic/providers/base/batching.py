"""
Generic Operation Batching for SQL Optimization

Provides reusable batching logic to group operations by target object,
enabling optimized SQL generation (complete CREATE statements vs empty CREATE + ALTERs).
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from .operations import Operation


@dataclass
class BatchInfo:
    """Information about a batch of operations for a single object"""

    is_new: bool = False
    create_op: Operation | None = None
    modify_ops: list[Operation] = field(default_factory=list)
    op_ids: list[str] = field(default_factory=list)
    operation_types: set[str] = field(default_factory=set)


class OperationBatcher:
    """
    Generic operation batcher for any provider.

    Groups operations by target object to enable SQL optimization:
    - Complete CREATE statements (columns included, not empty table + ALTERs)
    - Consolidated property settings (one ALTER vs multiple)
    - Reduced network round-trips

    Provider-agnostic: Works with any hierarchy (3-level, 4-level, etc.)
    """

    def batch_operations(
        self,
        ops: list[Operation],
        get_target_func: Callable[[Operation], str | None],
        is_create_func: Callable[[Operation], bool],
    ) -> dict[str, BatchInfo]:
        """
        Batch operations by target object.

        Groups related operations (e.g., all operations on same table) to enable
        optimized SQL generation.

        Args:
            ops: List of operations to batch
            get_target_func: Function to extract target object ID from operation.
                           Provider-specific logic (e.g., extract table_id).
                           Should return None if operation has no target.
            is_create_func: Function to check if operation creates new object.
                          Provider-specific (e.g., "add_table" is create operation).

        Returns:
            Dict mapping object_id to BatchInfo:
            {
                "object_123": BatchInfo(
                    is_new=True,
                    create_op=Operation(...),
                    modify_ops=[Operation(...), ...],
                    op_ids=["op_1", "op_2", ...],
                    operation_types={"add_table", "add_column", ...}
                )
            }

        Example:
            >>> def get_table_id(op):
            ...     if op.op == "add_table":
            ...         return op.target
            ...     return op.payload.get("tableId")
            ...
            >>> def is_create(op):
            ...     return op.op == "add_table"
            ...
            >>> batcher = OperationBatcher()
            >>> batches = batcher.batch_operations(ops, get_table_id, is_create)
        """
        batches: dict[str, BatchInfo] = {}

        for op in ops:
            target_id = get_target_func(op)

            if not target_id:
                # Operation has no target (e.g., global settings)
                continue

            # Create batch if doesn't exist
            if target_id not in batches:
                batches[target_id] = BatchInfo()

            batch = batches[target_id]

            # Track operation
            batch.op_ids.append(op.id)
            batch.operation_types.add(op.op)

            # Categorize operation
            if is_create_func(op):
                # This is a CREATE operation (e.g., add_table)
                batch.is_new = True
                batch.create_op = op
            else:
                # This is a modification operation (e.g., add_column, set_property)
                batch.modify_ops.append(op)

        return batches

    def batch_operations_by_type(
        self,
        ops: list[Operation],
        get_target_func: Callable[[Operation], str | None],
        categorize_func: Callable[[Operation], str],
    ) -> dict[str, dict[str, list[Operation]]]:
        """
        Batch operations by target AND operation type.

        More granular than batch_operations - groups by both target and type.
        Useful for providers that need fine-grained control over operation ordering.

        Args:
            ops: Operations to batch
            get_target_func: Extract target object ID
            categorize_func: Categorize operation type (e.g., "column_op", "property_op")

        Returns:
            Dict mapping object_id to dict of operation_type to operations:
            {
                "table_123": {
                    "column_ops": [Operation(...), ...],
                    "property_ops": [Operation(...), ...],
                    "constraint_ops": [Operation(...), ...]
                }
            }
        """
        batches: dict[str, dict[str, list[Operation]]] = {}

        for op in ops:
            target_id = get_target_func(op)
            if not target_id:
                continue

            if target_id not in batches:
                batches[target_id] = {}

            category = categorize_func(op)
            if category not in batches[target_id]:
                batches[target_id][category] = []

            batches[target_id][category].append(op)

        return batches

    @staticmethod
    def get_batch_statistics(batches: dict[str, BatchInfo]) -> dict:
        """
        Get statistics about batched operations.

        Useful for logging, debugging, and optimization analysis.

        Args:
            batches: Batched operations

        Returns:
            Statistics dict with counts and metrics
        """
        stats = {
            "total_batches": len(batches),
            "total_operations": 0,
            "new_objects": 0,
            "modified_objects": 0,
            "average_ops_per_batch": 0.0,
            "max_ops_in_batch": 0,
            "min_ops_in_batch": float("inf") if batches else 0,
        }

        for batch in batches.values():
            op_count = len(batch.op_ids)
            stats["total_operations"] += op_count

            if batch.is_new:
                stats["new_objects"] += 1
            else:
                stats["modified_objects"] += 1

            stats["max_ops_in_batch"] = max(stats["max_ops_in_batch"], op_count)
            stats["min_ops_in_batch"] = min(stats["min_ops_in_batch"], op_count)

        if batches:
            stats["average_ops_per_batch"] = stats["total_operations"] / len(batches)

        return stats
