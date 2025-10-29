"""
Generic SQL Optimization Algorithms

Provides reusable optimization algorithms for SQL generation across all providers.
Includes novel algorithms like single-column drag detection for minimal SQL generation.
"""

from typing import List, Optional, Tuple


class ColumnReorderOptimizer:
    """
    Optimize column reordering SQL generation.
    
    Novel Algorithm: Detects single-column moves to generate minimal SQL
    (1 statement instead of N statements).
    
    This algorithm is provider-agnostic and works for any SQL database
    that supports column positioning (ALTER COLUMN ... FIRST/AFTER).
    """
    
    @staticmethod
    def detect_single_column_move(
        original_order: List[str],
        final_order: List[str]
    ) -> Optional[Tuple[str, int, int]]:
        """
        Detect if only one column moved position in a reorder operation.
        
        Innovation: Instead of generating N ALTER statements for complex reorders,
        detect the common case of single-column drag-and-drop and generate just
        one optimal statement.
        
        Algorithm:
            For each column in the final order:
              1. Remove that column from both orders
              2. Check if the remaining orders are identical
              3. If yes, this column is the only one that moved
              4. Return (column_id, original_position, new_position)
            
            If no single column is found, multiple columns moved.
        
        Args:
            original_order: List of column IDs before reordering
            final_order: List of column IDs after reordering
        
        Returns:
            Tuple of (column_id, original_pos, new_pos) if single move detected.
            None if no change or multiple columns moved.
        
        Time Complexity: O(n²) worst case, O(n) average case
        
        Example:
            >>> original = ['a', 'b', 'c', 'd']
            >>> final = ['a', 'c', 'b', 'd']
            >>> detect_single_column_move(original, final)
            ('c', 2, 1)  # Column 'c' moved from position 2 to position 1
        """
        if not original_order or not final_order:
            return None
        
        if original_order == final_order:
            return None  # No change
        
        # Check if lists have same elements (just different order)
        if sorted(original_order) != sorted(final_order):
            return None  # Columns added/removed, not just reordered
        
        for i, col_id in enumerate(final_order):
            # Remove this column from both orders
            orig_without = [c for c in original_order if c != col_id]
            final_without = [c for c in final_order if c != col_id]
            
            if orig_without == final_without:
                # This column is the only one that moved!
                original_pos = original_order.index(col_id)
                new_pos = i
                
                if original_pos != new_pos:
                    return (col_id, original_pos, new_pos)
        
        return None  # Multiple columns moved
    
    @staticmethod
    def calculate_reorder_distance(
        original_order: List[str],
        final_order: List[str]
    ) -> int:
        """
        Calculate the "distance" between two column orderings.
        
        Useful for determining complexity of reorder operation.
        
        Args:
            original_order: Original column order
            final_order: Final column order
        
        Returns:
            Number of positions changed (Kendall tau distance)
        """
        if not original_order or not final_order:
            return 0
        
        if original_order == final_order:
            return 0
        
        # Count inversions (pairs that are out of order)
        distance = 0
        n = len(final_order)
        
        for i in range(n):
            for j in range(i + 1, n):
                col_i = final_order[i]
                col_j = final_order[j]
                
                if col_i in original_order and col_j in original_order:
                    orig_i = original_order.index(col_i)
                    orig_j = original_order.index(col_j)
                    
                    # Check if this pair is inverted (relative order changed)
                    if (orig_i < orig_j) != (i < j):
                        distance += 1
        
        return distance
    
    @staticmethod
    def is_simple_reorder(
        original_order: List[str],
        final_order: List[str],
        threshold: int = 2
    ) -> bool:
        """
        Check if reorder is "simple" (few changes).
        
        Args:
            original_order: Original column order
            final_order: Final column order
            threshold: Maximum distance to be considered simple
        
        Returns:
            True if reorder is simple enough to optimize
        """
        distance = ColumnReorderOptimizer.calculate_reorder_distance(
            original_order, final_order
        )
        return distance <= threshold

