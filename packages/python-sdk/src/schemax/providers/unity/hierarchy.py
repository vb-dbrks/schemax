"""
Unity Catalog Hierarchy Configuration
"""

from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel
from schemax.providers.base.hierarchy_defaults import build_table_level

# Unity Catalog has a 3-level hierarchy: Catalog → Schema → Table
unity_hierarchy_levels = [
    HierarchyLevel(
        name="catalog",
        display_name="Catalog",
        plural_name="catalogs",
        icon="database",
        is_container=True,
    ),
    HierarchyLevel(
        name="schema",
        display_name="Schema",
        plural_name="schemas",
        icon="folder",
        is_container=True,
    ),
    build_table_level(),
]

unity_hierarchy = Hierarchy(unity_hierarchy_levels)
