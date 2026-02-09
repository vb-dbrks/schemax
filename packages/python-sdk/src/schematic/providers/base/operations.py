"""
Base Operation Types

Operations represent user actions that modify the schema state.
"""

import random
import string
import time
from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class OperationCategory(StrEnum):
    """Operation category for grouping"""

    CATALOG = "catalog"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    CONSTRAINT = "constraint"
    SECURITY = "security"
    METADATA = "metadata"


class Operation(BaseModel):
    """Base operation structure"""

    id: str  # Unique identifier
    ts: str  # ISO timestamp
    provider: str  # Provider ID (e.g., 'unity', 'hive')
    op: str  # Operation type with provider prefix
    target: str  # ID of target object
    payload: dict[str, Any]  # Operation-specific data


class OperationMetadata(BaseModel):
    """Operation metadata for UI and documentation"""

    type: str  # Operation type
    display_name: str  # Human-readable name
    description: str  # Description
    category: OperationCategory  # Category
    required_fields: list[str]  # Required payload fields
    optional_fields: list[str]  # Optional payload fields
    is_destructive: bool  # Whether operation is destructive


def create_operation(
    provider: str,
    op_type: str,
    target: str,
    payload: dict[str, Any],
    op_id: str | None = None,
) -> Operation:
    """
    Helper to create a new operation with defaults

    Args:
        provider: Provider ID
        op_type: Operation type (without provider prefix)
        target: Target object ID
        payload: Operation payload
        op_id: Optional operation ID (generated if not provided)

    Returns:
        Operation instance
    """
    if op_id is None:
        # Generate a unique ID
        timestamp = int(time.time() * 1000)
        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=9))
        op_id = f"op_{timestamp}_{random_suffix}"

    return Operation(
        id=op_id,
        ts=time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        provider=provider,
        op=f"{provider}.{op_type}",
        target=target,
        payload=payload,
    )
