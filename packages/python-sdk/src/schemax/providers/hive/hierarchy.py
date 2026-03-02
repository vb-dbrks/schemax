"""Hierarchy definition for Hive Metastore provider."""

from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel
from schemax.providers.base.hierarchy_defaults import build_table_level

hive_hierarchy = Hierarchy(
    [
        HierarchyLevel(
            name="database",
            display_name="Database",
            plural_name="databases",
            icon="database",
            is_container=True,
        ),
        build_table_level(),
    ]
)
