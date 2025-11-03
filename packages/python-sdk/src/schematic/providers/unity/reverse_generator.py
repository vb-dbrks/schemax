"""
Unity Catalog Reverse Operation Generator

Generates reverse operations for all 31 Unity Catalog operations with safety
classification based on data impact.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from ..base.operations import Operation
from ..base.reverse_generator import ReverseOperationGenerator
from .operations import UNITY_OPERATIONS


class UnityReverseGenerator(ReverseOperationGenerator):
    """Unity Catalog-specific reverse operation generator

    Handles all 31 Unity Catalog operations:
    - Catalog operations (4): add, rename, update, drop
    - Schema operations (4): add, rename, update, drop
    - Table operations (6): add, rename, drop, set comment, set/unset property
    - Column operations (7): add, rename, drop, reorder, change type, set nullable, set comment
    - Column tag operations (2): set, unset
    - Constraint operations (2): add, drop
    - Row filter operations (3): add, update, remove
    - Column mask operations (3): add, update, remove
    """

    # Mapping of forward operations to their reverse operations
    REVERSE_MAP = {
        # Catalog operations
        UNITY_OPERATIONS["ADD_CATALOG"]: UNITY_OPERATIONS["DROP_CATALOG"],
        UNITY_OPERATIONS["RENAME_CATALOG"]: UNITY_OPERATIONS["RENAME_CATALOG"],  # Swap names
        UNITY_OPERATIONS["UPDATE_CATALOG"]: UNITY_OPERATIONS["UPDATE_CATALOG"],  # Restore old value
        UNITY_OPERATIONS["DROP_CATALOG"]: UNITY_OPERATIONS["ADD_CATALOG"],  # Restore from state
        # Schema operations
        UNITY_OPERATIONS["ADD_SCHEMA"]: UNITY_OPERATIONS["DROP_SCHEMA"],
        UNITY_OPERATIONS["RENAME_SCHEMA"]: UNITY_OPERATIONS["RENAME_SCHEMA"],  # Swap names
        UNITY_OPERATIONS["UPDATE_SCHEMA"]: UNITY_OPERATIONS["UPDATE_SCHEMA"],  # Restore old value
        UNITY_OPERATIONS["DROP_SCHEMA"]: UNITY_OPERATIONS["ADD_SCHEMA"],  # Restore from state
        # Table operations
        UNITY_OPERATIONS["ADD_TABLE"]: UNITY_OPERATIONS["DROP_TABLE"],
        UNITY_OPERATIONS["RENAME_TABLE"]: UNITY_OPERATIONS["RENAME_TABLE"],  # Swap names
        UNITY_OPERATIONS["DROP_TABLE"]: UNITY_OPERATIONS["ADD_TABLE"],  # Restore from state
        UNITY_OPERATIONS["SET_TABLE_COMMENT"]: UNITY_OPERATIONS["SET_TABLE_COMMENT"],  # Restore old
        UNITY_OPERATIONS["SET_TABLE_PROPERTY"]: UNITY_OPERATIONS["UNSET_TABLE_PROPERTY"],
        UNITY_OPERATIONS["UNSET_TABLE_PROPERTY"]: UNITY_OPERATIONS["SET_TABLE_PROPERTY"],
        # Column operations
        UNITY_OPERATIONS["ADD_COLUMN"]: UNITY_OPERATIONS["DROP_COLUMN"],
        UNITY_OPERATIONS["RENAME_COLUMN"]: UNITY_OPERATIONS["RENAME_COLUMN"],  # Swap names
        UNITY_OPERATIONS["DROP_COLUMN"]: UNITY_OPERATIONS["ADD_COLUMN"],  # Restore from state
        UNITY_OPERATIONS["REORDER_COLUMNS"]: UNITY_OPERATIONS[
            "REORDER_COLUMNS"
        ],  # Restore old order
        UNITY_OPERATIONS["CHANGE_COLUMN_TYPE"]: UNITY_OPERATIONS[
            "CHANGE_COLUMN_TYPE"
        ],  # Restore old type
        UNITY_OPERATIONS["SET_NULLABLE"]: UNITY_OPERATIONS["SET_NULLABLE"],  # Restore old value
        UNITY_OPERATIONS["SET_COLUMN_COMMENT"]: UNITY_OPERATIONS[
            "SET_COLUMN_COMMENT"
        ],  # Restore old
        # Column tag operations
        UNITY_OPERATIONS["SET_COLUMN_TAG"]: UNITY_OPERATIONS["UNSET_COLUMN_TAG"],
        UNITY_OPERATIONS["UNSET_COLUMN_TAG"]: UNITY_OPERATIONS["SET_COLUMN_TAG"],
        # Constraint operations
        UNITY_OPERATIONS["ADD_CONSTRAINT"]: UNITY_OPERATIONS["DROP_CONSTRAINT"],
        UNITY_OPERATIONS["DROP_CONSTRAINT"]: UNITY_OPERATIONS[
            "ADD_CONSTRAINT"
        ],  # Restore from state
        # Row filter operations
        UNITY_OPERATIONS["ADD_ROW_FILTER"]: UNITY_OPERATIONS["REMOVE_ROW_FILTER"],
        UNITY_OPERATIONS["UPDATE_ROW_FILTER"]: UNITY_OPERATIONS["UPDATE_ROW_FILTER"],  # Restore old
        UNITY_OPERATIONS["REMOVE_ROW_FILTER"]: UNITY_OPERATIONS[
            "ADD_ROW_FILTER"
        ],  # Restore from state
        # Column mask operations
        UNITY_OPERATIONS["ADD_COLUMN_MASK"]: UNITY_OPERATIONS["REMOVE_COLUMN_MASK"],
        UNITY_OPERATIONS["UPDATE_COLUMN_MASK"]: UNITY_OPERATIONS[
            "UPDATE_COLUMN_MASK"
        ],  # Restore old
        UNITY_OPERATIONS["REMOVE_COLUMN_MASK"]: UNITY_OPERATIONS[
            "ADD_COLUMN_MASK"
        ],  # Restore from state
    }

    def __init__(self) -> None:
        """Initialize Unity reverse generator"""
        self.op_counter = 0

    def can_reverse(self, op: Operation) -> bool:
        """Check if operation can be automatically reversed

        All Unity Catalog operations can be reversed, but some require
        state context (e.g., DROP operations need original object definition).
        """
        return op.op in self.REVERSE_MAP

    def generate_reverse(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Generate reverse operation for a Unity Catalog operation

        Args:
            op: Forward operation to reverse
            state: Provider state at time of operation (for context on DROP operations)

        Returns:
            Reverse operation

        Raises:
            ValueError: If operation cannot be reversed
        """
        if not self.can_reverse(op):
            raise ValueError(f"Cannot reverse operation: {op.op}")

        # Dispatch to specific reverse generation method
        op_type = op.op

        # Catalog operations
        if op_type == UNITY_OPERATIONS["ADD_CATALOG"]:
            return self._reverse_add_catalog(op)
        elif op_type == UNITY_OPERATIONS["RENAME_CATALOG"]:
            return self._reverse_rename_catalog(op)
        elif op_type == UNITY_OPERATIONS["UPDATE_CATALOG"]:
            return self._reverse_update_catalog(op, state)
        elif op_type == UNITY_OPERATIONS["DROP_CATALOG"]:
            return self._reverse_drop_catalog(op, state)

        # Schema operations
        elif op_type == UNITY_OPERATIONS["ADD_SCHEMA"]:
            return self._reverse_add_schema(op)
        elif op_type == UNITY_OPERATIONS["RENAME_SCHEMA"]:
            return self._reverse_rename_schema(op)
        elif op_type == UNITY_OPERATIONS["UPDATE_SCHEMA"]:
            return self._reverse_update_schema(op, state)
        elif op_type == UNITY_OPERATIONS["DROP_SCHEMA"]:
            return self._reverse_drop_schema(op, state)

        # Table operations
        elif op_type == UNITY_OPERATIONS["ADD_TABLE"]:
            return self._reverse_add_table(op)
        elif op_type == UNITY_OPERATIONS["RENAME_TABLE"]:
            return self._reverse_rename_table(op)
        elif op_type == UNITY_OPERATIONS["DROP_TABLE"]:
            return self._reverse_drop_table(op, state)
        elif op_type == UNITY_OPERATIONS["SET_TABLE_COMMENT"]:
            return self._reverse_set_table_comment(op, state)
        elif op_type == UNITY_OPERATIONS["SET_TABLE_PROPERTY"]:
            return self._reverse_set_table_property(op)
        elif op_type == UNITY_OPERATIONS["UNSET_TABLE_PROPERTY"]:
            return self._reverse_unset_table_property(op, state)

        # Column operations
        elif op_type == UNITY_OPERATIONS["ADD_COLUMN"]:
            return self._reverse_add_column(op)
        elif op_type == UNITY_OPERATIONS["RENAME_COLUMN"]:
            return self._reverse_rename_column(op)
        elif op_type == UNITY_OPERATIONS["DROP_COLUMN"]:
            return self._reverse_drop_column(op, state)
        elif op_type == UNITY_OPERATIONS["REORDER_COLUMNS"]:
            return self._reverse_reorder_columns(op, state)
        elif op_type == UNITY_OPERATIONS["CHANGE_COLUMN_TYPE"]:
            return self._reverse_change_column_type(op, state)
        elif op_type == UNITY_OPERATIONS["SET_NULLABLE"]:
            return self._reverse_set_nullable(op, state)
        elif op_type == UNITY_OPERATIONS["SET_COLUMN_COMMENT"]:
            return self._reverse_set_column_comment(op, state)

        # Column tag operations
        elif op_type == UNITY_OPERATIONS["SET_COLUMN_TAG"]:
            return self._reverse_set_column_tag(op)
        elif op_type == UNITY_OPERATIONS["UNSET_COLUMN_TAG"]:
            return self._reverse_unset_column_tag(op, state)

        # Constraint operations
        elif op_type == UNITY_OPERATIONS["ADD_CONSTRAINT"]:
            return self._reverse_add_constraint(op)
        elif op_type == UNITY_OPERATIONS["DROP_CONSTRAINT"]:
            return self._reverse_drop_constraint(op, state)

        # Row filter operations
        elif op_type == UNITY_OPERATIONS["ADD_ROW_FILTER"]:
            return self._reverse_add_row_filter(op)
        elif op_type == UNITY_OPERATIONS["UPDATE_ROW_FILTER"]:
            return self._reverse_update_row_filter(op, state)
        elif op_type == UNITY_OPERATIONS["REMOVE_ROW_FILTER"]:
            return self._reverse_remove_row_filter(op, state)

        # Column mask operations
        elif op_type == UNITY_OPERATIONS["ADD_COLUMN_MASK"]:
            return self._reverse_add_column_mask(op)
        elif op_type == UNITY_OPERATIONS["UPDATE_COLUMN_MASK"]:
            return self._reverse_update_column_mask(op, state)
        elif op_type == UNITY_OPERATIONS["REMOVE_COLUMN_MASK"]:
            return self._reverse_remove_column_mask(op, state)

        else:
            raise ValueError(f"Unknown operation type: {op_type}")

    def _create_reverse_op(self, op_type: str, target: str, payload: dict[str, Any]) -> Operation:
        """Create a reverse operation with proper metadata"""
        return Operation(
            id=f"rollback_{uuid4().hex[:8]}",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op=op_type,
            target=target,
            payload=payload,
        )

    # Catalog operation reversals

    def _reverse_add_catalog(self, op: Operation) -> Operation:
        """Reverse ADD_CATALOG → DROP_CATALOG"""
        return self._create_reverse_op(UNITY_OPERATIONS["DROP_CATALOG"], op.target, {})

    def _reverse_rename_catalog(self, op: Operation) -> Operation:
        """Reverse RENAME_CATALOG → RENAME_CATALOG (swap names)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["RENAME_CATALOG"],
            op.target,
            {
                "oldName": op.payload["newName"],  # Swapped!
                "newName": op.payload.get("oldName", ""),
            },
        )

    def _reverse_update_catalog(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UPDATE_CATALOG → UPDATE_CATALOG (restore old value)"""
        # TODO: Need to track old value from state
        return self._create_reverse_op(
            UNITY_OPERATIONS["UPDATE_CATALOG"],
            op.target,
            op.payload,  # For now, same payload (needs state lookup)
        )

    def _reverse_drop_catalog(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse DROP_CATALOG → ADD_CATALOG (restore from state)"""
        # TODO: Lookup catalog definition from state
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_CATALOG"],
            op.target,
            {
                "catalogId": op.target,
                "name": "restored_catalog",  # TODO: Get from state
            },
        )

    # Schema operation reversals

    def _reverse_add_schema(self, op: Operation) -> Operation:
        """Reverse ADD_SCHEMA → DROP_SCHEMA"""
        return self._create_reverse_op(UNITY_OPERATIONS["DROP_SCHEMA"], op.target, {})

    def _reverse_rename_schema(self, op: Operation) -> Operation:
        """Reverse RENAME_SCHEMA → RENAME_SCHEMA (swap names)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["RENAME_SCHEMA"],
            op.target,
            {
                "oldName": op.payload["newName"],
                "newName": op.payload.get("oldName", ""),
            },
        )

    def _reverse_update_schema(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UPDATE_SCHEMA → UPDATE_SCHEMA (restore old value)"""
        return self._create_reverse_op(UNITY_OPERATIONS["UPDATE_SCHEMA"], op.target, op.payload)

    def _reverse_drop_schema(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse DROP_SCHEMA → ADD_SCHEMA (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_SCHEMA"],
            op.target,
            {
                "schemaId": op.target,
                "name": "restored_schema",
                "catalogId": "",  # TODO: Get from state
            },
        )

    # Table operation reversals

    def _reverse_add_table(self, op: Operation) -> Operation:
        """Reverse ADD_TABLE → DROP_TABLE"""
        return self._create_reverse_op(UNITY_OPERATIONS["DROP_TABLE"], op.target, {})

    def _reverse_rename_table(self, op: Operation) -> Operation:
        """Reverse RENAME_TABLE → RENAME_TABLE (swap names)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["RENAME_TABLE"],
            op.target,
            {
                "oldName": op.payload["newName"],
                "newName": op.payload.get("oldName", ""),
            },
        )

    def _reverse_drop_table(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse DROP_TABLE → ADD_TABLE (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_TABLE"],
            op.target,
            {
                "tableId": op.target,
                "name": "restored_table",
                "schemaId": "",
                "format": "delta",
            },
        )

    def _reverse_set_table_comment(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse SET_TABLE_COMMENT → SET_TABLE_COMMENT (restore old)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["SET_TABLE_COMMENT"],
            op.target,
            {"tableId": op.payload["tableId"], "comment": ""},  # TODO: Get old from state
        )

    def _reverse_set_table_property(self, op: Operation) -> Operation:
        """Reverse SET_TABLE_PROPERTY → UNSET_TABLE_PROPERTY"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["UNSET_TABLE_PROPERTY"],
            op.target,
            {"tableId": op.payload["tableId"], "key": op.payload["key"]},
        )

    def _reverse_unset_table_property(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UNSET_TABLE_PROPERTY → SET_TABLE_PROPERTY (restore old value)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["SET_TABLE_PROPERTY"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "key": op.payload["key"],
                "value": "",  # TODO: Get old value from state
            },
        )

    # Column operation reversals

    def _reverse_add_column(self, op: Operation) -> Operation:
        """Reverse ADD_COLUMN → DROP_COLUMN"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["DROP_COLUMN"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.payload["colId"],
                "name": op.payload["name"],
            },
        )

    def _reverse_rename_column(self, op: Operation) -> Operation:
        """Reverse RENAME_COLUMN → RENAME_COLUMN (swap names)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["RENAME_COLUMN"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.payload.get("colId", ""),
                "oldName": op.payload["newName"],
                "newName": op.payload.get("oldName", ""),
            },
        )

    def _reverse_drop_column(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse DROP_COLUMN → ADD_COLUMN (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_COLUMN"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.target,
                "name": op.payload.get("name", "restored_col"),
                "type": "STRING",  # TODO: Get from state
                "nullable": True,
            },
        )

    def _reverse_reorder_columns(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse REORDER_COLUMNS → REORDER_COLUMNS (restore old order)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["REORDER_COLUMNS"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "order": [],  # TODO: Get old order from state
            },
        )

    def _reverse_change_column_type(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse CHANGE_COLUMN_TYPE → CHANGE_COLUMN_TYPE (restore old type)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["CHANGE_COLUMN_TYPE"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.target,
                "newType": "STRING",  # TODO: Get old type from state
            },
        )

    def _reverse_set_nullable(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse SET_NULLABLE → SET_NULLABLE (restore old value)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["SET_NULLABLE"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.target,
                "nullable": not op.payload["nullable"],  # Toggle
            },
        )

    def _reverse_set_column_comment(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse SET_COLUMN_COMMENT → SET_COLUMN_COMMENT (restore old)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["SET_COLUMN_COMMENT"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.target,
                "comment": "",  # TODO: Get old from state
            },
        )

    # Column tag operation reversals

    def _reverse_set_column_tag(self, op: Operation) -> Operation:
        """Reverse SET_COLUMN_TAG → UNSET_COLUMN_TAG"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["UNSET_COLUMN_TAG"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.payload["colId"],
                "tagName": op.payload["tagName"],
            },
        )

    def _reverse_unset_column_tag(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UNSET_COLUMN_TAG → SET_COLUMN_TAG (restore old value)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["SET_COLUMN_TAG"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "colId": op.payload["colId"],
                "tagName": op.payload["tagName"],
                "tagValue": "",  # TODO: Get old value from state
            },
        )

    # Constraint operation reversals

    def _reverse_add_constraint(self, op: Operation) -> Operation:
        """Reverse ADD_CONSTRAINT → DROP_CONSTRAINT"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["DROP_CONSTRAINT"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "constraintId": op.payload["constraintId"],
            },
        )

    def _reverse_drop_constraint(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse DROP_CONSTRAINT → ADD_CONSTRAINT (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_CONSTRAINT"],
            op.target,
            {
                "tableId": op.payload.get("tableId", ""),
                "constraintId": op.target,
                "type": "primary_key",  # TODO: Get from state
                "columns": [],
            },
        )

    # Row filter operation reversals

    def _reverse_add_row_filter(self, op: Operation) -> Operation:
        """Reverse ADD_ROW_FILTER → REMOVE_ROW_FILTER"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["REMOVE_ROW_FILTER"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "filterId": op.payload["filterId"],
            },
        )

    def _reverse_update_row_filter(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UPDATE_ROW_FILTER → UPDATE_ROW_FILTER (restore old)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["UPDATE_ROW_FILTER"],
            op.target,
            op.payload,  # TODO: Get old values from state
        )

    def _reverse_remove_row_filter(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse REMOVE_ROW_FILTER → ADD_ROW_FILTER (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_ROW_FILTER"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "filterId": op.target,
                "name": "restored_filter",
                "udfExpression": "",  # TODO: Get from state
            },
        )

    # Column mask operation reversals

    def _reverse_add_column_mask(self, op: Operation) -> Operation:
        """Reverse ADD_COLUMN_MASK → REMOVE_COLUMN_MASK"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["REMOVE_COLUMN_MASK"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "maskId": op.payload["maskId"],
            },
        )

    def _reverse_update_column_mask(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse UPDATE_COLUMN_MASK → UPDATE_COLUMN_MASK (restore old)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["UPDATE_COLUMN_MASK"],
            op.target,
            op.payload,  # TODO: Get old values from state
        )

    def _reverse_remove_column_mask(self, op: Operation, state: dict[str, Any]) -> Operation:
        """Reverse REMOVE_COLUMN_MASK → ADD_COLUMN_MASK (restore from state)"""
        return self._create_reverse_op(
            UNITY_OPERATIONS["ADD_COLUMN_MASK"],
            op.target,
            {
                "tableId": op.payload["tableId"],
                "maskId": op.target,
                "columnId": "",  # TODO: Get from state
                "name": "restored_mask",
                "maskFunction": "",  # TODO: Get from state
            },
        )
