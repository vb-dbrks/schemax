"""
Unity Catalog Hierarchy Configuration
"""

from ..base.hierarchy import Hierarchy, HierarchyLevel

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
    HierarchyLevel(
        name="table",
        display_name="Table",
        plural_name="tables",
        icon="table",
        is_container=False,
    ),
]

unity_hierarchy = Hierarchy(unity_hierarchy_levels)

