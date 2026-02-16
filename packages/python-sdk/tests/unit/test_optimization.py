"""
Unit tests for base optimization algorithms.

Tests the generic column reorder optimizer that works across all providers.
"""

from schemax.providers.base.optimization import ColumnReorderOptimizer


class TestColumnReorderOptimizer:
    """Test column reorder optimization algorithms"""

    def test_detect_single_column_move_forward(self):
        """Test detecting a single column moved forward in the list"""
        original = ["a", "b", "c", "d", "e"]
        final = ["a", "c", "b", "d", "e"]  # 'c' moved forward

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id == "c"
        assert orig_pos == 2
        assert new_pos == 1

    def test_detect_single_column_move_backward(self):
        """Test detecting a single column moved backward in the list"""
        original = ["a", "b", "c", "d", "e"]
        final = ["a", "c", "d", "b", "e"]  # 'b' moved backward

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id == "b"
        assert orig_pos == 1
        assert new_pos == 3

    def test_detect_single_column_to_first(self):
        """Test detecting a column moved to first position"""
        original = ["a", "b", "c", "d"]
        final = ["c", "a", "b", "d"]  # 'c' moved to first

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id == "c"
        assert orig_pos == 2
        assert new_pos == 0

    def test_detect_single_column_to_last(self):
        """Test detecting a column moved to last position"""
        original = ["a", "b", "c", "d"]
        final = ["a", "c", "d", "b"]  # 'b' moved to last

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id == "b"
        assert orig_pos == 1
        assert new_pos == 3

    def test_detect_no_change_returns_none(self):
        """Test that no change returns None"""
        original = ["a", "b", "c", "d"]
        final = ["a", "b", "c", "d"]

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is None

    def test_detect_multiple_moves_returns_none(self):
        """Test that multiple columns moved returns None"""
        original = ["a", "b", "c", "d"]
        final = ["b", "a", "d", "c"]  # Multiple columns moved

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is None

    def test_empty_lists_return_none(self):
        """Test that empty lists return None"""
        result = ColumnReorderOptimizer.detect_single_column_move([], [])
        assert result is None

        result = ColumnReorderOptimizer.detect_single_column_move(["a"], [])
        assert result is None

        result = ColumnReorderOptimizer.detect_single_column_move([], ["a"])
        assert result is None

    def test_single_column_list_no_change(self):
        """Test single column list with no change"""
        original = ["a"]
        final = ["a"]

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is None  # No change

    def test_two_column_swap(self):
        """Test swapping two columns (should return one of them)"""
        original = ["a", "b"]
        final = ["b", "a"]

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        # Either 'a' or 'b' can be considered as the moved column
        # The algorithm should detect one of them
        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id in ["a", "b"]

    def test_complex_reorder_multiple_moves(self):
        """Test complex reordering with multiple moves"""
        original = ["a", "b", "c", "d", "e"]
        final = ["e", "d", "c", "b", "a"]  # Completely reversed

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is None  # Multiple columns moved

    def test_adjacent_swap(self):
        """Test swapping adjacent columns"""
        original = ["a", "b", "c", "d"]
        final = ["a", "c", "b", "d"]  # 'b' and 'c' swapped

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        # Should detect one of them
        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id in ["b", "c"]

    def test_single_column_move_with_many_columns(self):
        """Test single column move in large list"""
        original = ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8"]
        final = ["col1", "col2", "col6", "col3", "col4", "col5", "col7", "col8"]

        result = ColumnReorderOptimizer.detect_single_column_move(original, final)

        assert result is not None
        col_id, orig_pos, new_pos = result
        assert col_id == "col6"
        assert orig_pos == 5
        assert new_pos == 2


class TestReorderDistance:
    """Test reorder distance calculation"""

    def test_calculate_distance_no_change(self):
        """Test distance is 0 for no change"""
        original = ["a", "b", "c"]
        final = ["a", "b", "c"]

        distance = ColumnReorderOptimizer.calculate_reorder_distance(original, final)

        assert distance == 0

    def test_calculate_distance_single_swap(self):
        """Test distance for single swap"""
        original = ["a", "b", "c"]
        final = ["a", "c", "b"]

        distance = ColumnReorderOptimizer.calculate_reorder_distance(original, final)

        assert distance >= 1  # At least one inversion

    def test_calculate_distance_complete_reversal(self):
        """Test distance for complete reversal"""
        original = ["a", "b", "c", "d"]
        final = ["d", "c", "b", "a"]

        distance = ColumnReorderOptimizer.calculate_reorder_distance(original, final)

        # Complete reversal has maximum inversions
        assert distance > 0

    def test_calculate_distance_empty_lists(self):
        """Test distance for empty lists"""
        distance = ColumnReorderOptimizer.calculate_reorder_distance([], [])
        assert distance == 0


class TestSimpleReorder:
    """Test simple reorder detection"""

    def test_is_simple_reorder_no_change(self):
        """Test that no change is considered simple"""
        original = ["a", "b", "c"]
        final = ["a", "b", "c"]

        result = ColumnReorderOptimizer.is_simple_reorder(original, final)

        assert result is True

    def test_is_simple_reorder_single_move(self):
        """Test that single move is simple"""
        original = ["a", "b", "c", "d"]
        final = ["a", "c", "b", "d"]

        result = ColumnReorderOptimizer.is_simple_reorder(original, final, threshold=2)

        assert result is True

    def test_is_not_simple_reorder_complex_changes(self):
        """Test that complex reordering is not simple"""
        original = ["a", "b", "c", "d", "e"]
        final = ["e", "d", "c", "b", "a"]

        result = ColumnReorderOptimizer.is_simple_reorder(original, final, threshold=2)

        # Complete reversal should exceed threshold
        assert result is False
