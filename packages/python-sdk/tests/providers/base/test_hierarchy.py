import pytest
from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel

def test_hierarchy_depth():
    """Test hierarchy depth calculation"""
    levels = [
        HierarchyLevel(name="catalog", display_name="Catalog", plural_name="catalogs", icon="database", is_container=True),
        HierarchyLevel(name="schema", display_name="Schema", plural_name="schemas", icon="folder", is_container=True),
        HierarchyLevel(name="table", display_name="Table", plural_name="tables", icon="table", is_container=False),
    ]
    hierarchy = Hierarchy(levels=levels)
    assert hierarchy.get_depth() == 3

def test_hierarchy_get_level():
    """Test getting hierarchy level by depth"""
    levels = [
        HierarchyLevel(name="catalog", display_name="Catalog", plural_name="catalogs", icon="database", is_container=True),
        HierarchyLevel(name="schema", display_name="Schema", plural_name="schemas", icon="folder", is_container=True),
    ]
    hierarchy = Hierarchy(levels=levels)
    assert hierarchy.get_level(0).name == "catalog"
    assert hierarchy.get_level(1).name == "schema"

