"""
Operation types and utilities for SchemaX.

These are the 31 operation types that SchemaX supports, ported from the TypeScript implementation.
"""

from typing import Literal

# All operation types (31 total)
OpType = Literal[
    # Catalog operations (3)
    "add_catalog",
    "rename_catalog",
    "drop_catalog",
    # Schema operations (3)
    "add_schema",
    "rename_schema",
    "drop_schema",
    # Table operations (6)
    "add_table",
    "rename_table",
    "drop_table",
    "set_table_comment",
    "set_table_property",
    "unset_table_property",
    # Column operations (7)
    "add_column",
    "rename_column",
    "reorder_columns",
    "drop_column",
    "change_column_type",
    "set_nullable",
    "set_column_comment",
    # Column tag operations (2)
    "set_column_tag",
    "unset_column_tag",
    # Constraint operations (2)
    "add_constraint",
    "drop_constraint",
    # Row filter operations (3)
    "add_row_filter",
    "update_row_filter",
    "remove_row_filter",
    # Column mask operations (3)
    "add_column_mask",
    "update_column_mask",
    "remove_column_mask",
]

# Operation categories for easier handling
CATALOG_OPS = ["add_catalog", "rename_catalog", "drop_catalog"]
SCHEMA_OPS = ["add_schema", "rename_schema", "drop_schema"]
TABLE_OPS = [
    "add_table",
    "rename_table",
    "drop_table",
    "set_table_comment",
    "set_table_property",
    "unset_table_property",
]
COLUMN_OPS = [
    "add_column",
    "rename_column",
    "reorder_columns",
    "drop_column",
    "change_column_type",
    "set_nullable",
    "set_column_comment",
]
TAG_OPS = ["set_column_tag", "unset_column_tag"]
CONSTRAINT_OPS = ["add_constraint", "drop_constraint"]
ROW_FILTER_OPS = ["add_row_filter", "update_row_filter", "remove_row_filter"]
COLUMN_MASK_OPS = ["add_column_mask", "update_column_mask", "remove_column_mask"]

ALL_OP_TYPES = (
    CATALOG_OPS
    + SCHEMA_OPS
    + TABLE_OPS
    + COLUMN_OPS
    + TAG_OPS
    + CONSTRAINT_OPS
    + ROW_FILTER_OPS
    + COLUMN_MASK_OPS
)
