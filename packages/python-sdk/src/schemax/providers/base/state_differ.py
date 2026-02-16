"""
Base State Differ - Abstract class for state-based diff generation

Compares two snapshot states and generates operations representing the changes.
Supports rename detection using operation history.
"""

from abc import ABC, abstractmethod
from typing import Any

from .models import ProviderState
from .operations import Operation


class StateDiffer(ABC):
    """Base class for state-based diff generation

    Compares two provider states and generates a list of operations that
    represent the changes between them. Supports smart rename detection
    by analyzing operation history.
    """

    def __init__(
        self,
        old_state: ProviderState,
        new_state: ProviderState,
        old_operations: list[Operation] | None = None,
        new_operations: list[Operation] | None = None,
    ) -> None:
        """Initialize state differ

        Args:
            old_state: Previous state (source)
            new_state: Current state (target)
            old_operations: Operations that created old_state (for context)
            new_operations: Operations that created new_state (for rename detection)
        """
        self.old_state = old_state
        self.new_state = new_state
        self.old_operations = old_operations or []
        self.new_operations = new_operations or []

    @abstractmethod
    def generate_diff_operations(self) -> list[Operation]:
        """Generate list of operations representing state diff

        Returns:
            List of operations that would transform old_state into new_state
        """
        pass

    def _detect_rename(
        self, old_id: str, new_id: str, old_name: str, new_name: str, op_type: str
    ) -> bool:
        """Detect if object was renamed by checking operation history

        Args:
            old_id: ID in old state
            new_id: ID in new state (should match old_id for renames)
            old_name: Name in old state
            new_name: Name in new state
            op_type: Operation type to look for (e.g., "rename_column")

        Returns:
            True if a rename operation is found in history, False otherwise
        """
        # IDs must match for it to be a rename (not add + delete)
        if old_id != new_id:
            return False

        # Check new_operations for rename_* operation matching the ID
        for op in self.new_operations:
            # Handle both Operation objects and dicts
            op_dict = op if isinstance(op, dict) else op.model_dump()
            target = op_dict.get("target")
            op_name = op_dict.get("op", "")
            payload = op_dict.get("payload", {})

            if target == old_id and op_type in op_name:
                if payload.get("oldName") == old_name and payload.get("newName") == new_name:
                    return True

        return False

    def _build_id_map(self, objects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Build a map of object IDs to object data

        Args:
            objects: List of objects with 'id' field

        Returns:
            Dictionary mapping id -> object
        """
        return {obj["id"]: obj for obj in objects}

    def _build_name_map(self, objects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Build a map of object names to object data

        Useful for detecting renames when ID stays the same.

        Args:
            objects: List of objects with 'name' field

        Returns:
            Dictionary mapping name -> object
        """
        return {obj["name"]: obj for obj in objects}
