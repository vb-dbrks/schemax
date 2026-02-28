"""
Provider-agnostic filter for deployment scope (managed categories and existing objects).

Used by apply and sql commands to restrict which operations are emitted per environment
(e.g. governance-only mode, or skip CREATE for existing catalogs).
"""

from typing import Any

from .operations import Operation
from .provider import Provider


def filter_operations_by_managed_scope(
    ops: list[Operation],
    env_config: dict[str, Any],
    provider: Provider,
) -> list[Operation]:
    """
    Filter operations by environment's managed categories and existing objects.

    - If managedCategories is set, only ops whose managed_category is in that list are kept.
    - If managedCategories is missing/empty, all ops pass (full scope).
    - If existingObjects.catalog is set, drop add_catalog ops whose payload name is in that list.

    Args:
        ops: Operations to filter
        env_config: Environment config (may contain managedCategories, existingObjects)
        provider: Provider used to look up operation metadata (managed_category)

    Returns:
        Filtered list of operations
    """
    managed_categories = env_config.get("managedCategories")
    if isinstance(managed_categories, list) and len(managed_categories) == 0:
        managed_categories = None
    existing_objects = env_config.get("existingObjects") or {}
    raw_catalogs = existing_objects.get("catalog") or []
    existing_catalogs = {str(s).strip() for s in raw_catalogs if s is not None and str(s).strip()}

    result: list[Operation] = []
    for operation in ops:
        # Skip add_catalog for catalogs in existingObjects.catalog
        if existing_catalogs and operation.op and operation.op.endswith("add_catalog"):
            name = (operation.payload or {}).get("name")
            if name is not None and str(name).strip() in existing_catalogs:
                continue

        # If no managed scope restriction, keep op
        if not managed_categories:
            result.append(operation)
            continue

        # Look up managed_category from provider metadata
        meta = provider.get_operation_metadata(operation.op)
        if meta is None:
            # Unknown op type: keep it (no filtering by category)
            result.append(operation)
            continue
        cat = getattr(meta, "managed_category", None)
        if cat is None:
            result.append(operation)
            continue
        cat_value = cat.value if hasattr(cat, "value") else str(cat)
        if cat_value in managed_categories:
            result.append(operation)

    return result
