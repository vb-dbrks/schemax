"""Shared hierarchy level builders used by provider adapters."""

from schemax.providers.base.hierarchy import HierarchyLevel


def build_table_level() -> HierarchyLevel:
    """Return a canonical table leaf level for providers with table assets."""
    return HierarchyLevel(
        name="table",
        display_name="Table",
        plural_name="tables",
        icon="table",
        is_container=False,
    )
