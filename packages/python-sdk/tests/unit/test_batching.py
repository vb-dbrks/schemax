"""
Unit tests for base operation batching.

Tests the generic operation batcher that works across all providers.
"""

import pytest

from schemax.providers.base.batching import BatchInfo, OperationBatcher
from schemax.providers.base.operations import Operation


class TestOperationBatcher:
    """Test operation batching logic"""

    @pytest.fixture
    def sample_operations(self):
        """Create sample operations for testing"""
        return [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.add_table",
                target="table_123",
                payload={"name": "users"},
            ),
            Operation(
                id="op_002",
                provider="test",
                ts="2025-01-01T00:01:00Z",
                op="test.add_column",
                target="col_001",
                payload={"tableId": "table_123", "name": "id"},
            ),
            Operation(
                id="op_003",
                provider="test",
                ts="2025-01-01T00:02:00Z",
                op="test.add_column",
                target="col_002",
                payload={"tableId": "table_123", "name": "name"},
            ),
        ]

    def test_batch_empty_operations(self):
        """Test batching with empty operation list"""
        batcher = OperationBatcher()

        def get_target(op):
            return op.target if op.op == "test.add_table" else op.payload.get("tableId")

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations([], get_target, is_create)

        assert len(batches) == 0

    def test_batch_single_operation(self):
        """Test batching with single operation"""
        batcher = OperationBatcher()
        ops = [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.add_table",
                target="table_123",
                payload={"name": "users"},
            )
        ]

        def get_target(op):
            return op.target

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(ops, get_target, is_create)

        assert len(batches) == 1
        assert "table_123" in batches
        assert batches["table_123"].is_new is True
        assert batches["table_123"].create_op == ops[0]
        assert len(batches["table_123"].op_ids) == 1

    def test_batch_multiple_ops_same_object(self, sample_operations):
        """Test batching multiple operations on same object"""
        batcher = OperationBatcher()

        def get_target(op):
            return op.target if op.op == "test.add_table" else op.payload.get("tableId")

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(sample_operations, get_target, is_create)

        assert len(batches) == 1
        assert "table_123" in batches

        batch = batches["table_123"]
        assert batch.is_new is True
        assert batch.create_op.id == "op_001"
        assert len(batch.modify_ops) == 2  # Two add_column operations
        assert len(batch.op_ids) == 3  # All three operations
        assert "test.add_table" in batch.operation_types
        assert "test.add_column" in batch.operation_types

    def test_batch_multiple_objects(self):
        """Test batching operations on multiple objects"""
        batcher = OperationBatcher()
        ops = [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.add_table",
                target="table_123",
                payload={"name": "users"},
            ),
            Operation(
                id="op_002",
                provider="test",
                ts="2025-01-01T00:01:00Z",
                op="test.add_table",
                target="table_456",
                payload={"name": "orders"},
            ),
            Operation(
                id="op_003",
                provider="test",
                ts="2025-01-01T00:02:00Z",
                op="test.add_column",
                target="col_001",
                payload={"tableId": "table_123", "name": "id"},
            ),
        ]

        def get_target(op):
            return op.target if op.op == "test.add_table" else op.payload.get("tableId")

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(ops, get_target, is_create)

        assert len(batches) == 2
        assert "table_123" in batches
        assert "table_456" in batches

        # table_123 has create + modify
        assert batches["table_123"].is_new is True
        assert len(batches["table_123"].modify_ops) == 1

        # table_456 has only create
        assert batches["table_456"].is_new is True
        assert len(batches["table_456"].modify_ops) == 0

    def test_batch_create_and_modify(self):
        """Test batching with create and modify operations"""
        batcher = OperationBatcher()
        ops = [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.add_table",
                target="table_123",
                payload={"name": "users"},
            ),
            Operation(
                id="op_002",
                provider="test",
                ts="2025-01-01T00:01:00Z",
                op="test.set_property",
                target="prop_001",
                payload={"tableId": "table_123", "key": "owner"},
            ),
        ]

        def get_target(op):
            return op.target if op.op == "test.add_table" else op.payload.get("tableId")

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(ops, get_target, is_create)

        batch = batches["table_123"]
        assert batch.is_new is True
        assert batch.create_op.id == "op_001"
        assert len(batch.modify_ops) == 1
        assert batch.modify_ops[0].id == "op_002"

    def test_batch_preserves_operation_order(self, sample_operations):
        """Test that batching preserves operation order"""
        batcher = OperationBatcher()

        def get_target(op):
            return op.target if op.op == "test.add_table" else op.payload.get("tableId")

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(sample_operations, get_target, is_create)

        batch = batches["table_123"]
        assert batch.op_ids == ["op_001", "op_002", "op_003"]

    def test_batch_groups_by_target(self):
        """Test that operations are correctly grouped by target"""
        batcher = OperationBatcher()
        ops = [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.modify_column",
                target="col_001",
                payload={"tableId": "table_123"},
            ),
            Operation(
                id="op_002",
                provider="test",
                ts="2025-01-01T00:01:00Z",
                op="test.modify_column",
                target="col_002",
                payload={"tableId": "table_456"},
            ),
            Operation(
                id="op_003",
                provider="test",
                ts="2025-01-01T00:02:00Z",
                op="test.modify_column",
                target="col_003",
                payload={"tableId": "table_123"},
            ),
        ]

        def get_target(op):
            return op.payload.get("tableId")

        def is_create(op):
            return False

        batches = batcher.batch_operations(ops, get_target, is_create)

        assert len(batches) == 2
        assert len(batches["table_123"].modify_ops) == 2
        assert len(batches["table_456"].modify_ops) == 1

    def test_batch_operations_without_target(self):
        """Test that operations without target are skipped"""
        batcher = OperationBatcher()
        ops = [
            Operation(
                id="op_001",
                provider="test",
                ts="2025-01-01T00:00:00Z",
                op="test.global_setting",
                target="setting_001",
                payload={},
            ),
            Operation(
                id="op_002",
                provider="test",
                ts="2025-01-01T00:01:00Z",
                op="test.add_table",
                target="table_123",
                payload={"name": "users"},
            ),
        ]

        def get_target(op):
            # Only return target for add_table operations
            return op.target if op.op == "test.add_table" else None

        def is_create(op):
            return op.op == "test.add_table"

        batches = batcher.batch_operations(ops, get_target, is_create)

        # Only table_123 should be batched
        assert len(batches) == 1
        assert "table_123" in batches
        assert "setting_001" not in batches


class TestBatchInfo:
    """Test BatchInfo data structure"""

    def test_batch_info_initialization(self):
        """Test BatchInfo initializes with correct defaults"""
        batch = BatchInfo()

        assert batch.is_new is False
        assert batch.create_op is None
        assert batch.modify_ops == []
        assert batch.op_ids == []
        assert batch.operation_types == set()


class TestBatchStatistics:
    """Test batch statistics calculation"""

    def test_get_batch_statistics_empty(self):
        """Test statistics for empty batches"""
        stats = OperationBatcher.get_batch_statistics({})

        assert stats["total_batches"] == 0
        assert stats["total_operations"] == 0
        assert stats["new_objects"] == 0
        assert stats["modified_objects"] == 0

    def test_get_batch_statistics(self):
        """Test statistics calculation"""
        batches = {}

        # Batch 1: New object with 3 operations
        batch1 = BatchInfo()
        batch1.is_new = True
        batch1.op_ids = ["op_001", "op_002", "op_003"]
        batches["table_123"] = batch1

        # Batch 2: Modified object with 2 operations
        batch2 = BatchInfo()
        batch2.is_new = False
        batch2.op_ids = ["op_004", "op_005"]
        batches["table_456"] = batch2

        stats = OperationBatcher.get_batch_statistics(batches)

        assert stats["total_batches"] == 2
        assert stats["total_operations"] == 5
        assert stats["new_objects"] == 1
        assert stats["modified_objects"] == 1
        assert stats["average_ops_per_batch"] == 2.5
        assert stats["max_ops_in_batch"] == 3
        assert stats["min_ops_in_batch"] == 2
