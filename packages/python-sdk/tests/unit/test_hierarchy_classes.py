"""
Tests for providers/base/hierarchy.py — covers all branches.
"""

import pytest

from schemax.providers.base.hierarchy import Hierarchy, HierarchyLevel


def _level(name: str, *, container: bool = False, icon: str | None = None) -> HierarchyLevel:
    return HierarchyLevel(
        name=name,
        display_name=name.capitalize(),
        plural_name=f"{name}s",
        icon=icon,
        is_container=container,
    )


class TestHierarchyLevel:
    def test_basic_fields(self):
        level = _level("table")
        assert level.name == "table"
        assert level.display_name == "Table"
        assert level.plural_name == "tables"
        assert level.icon is None
        assert level.is_container is False

    def test_container_with_icon(self):
        level = _level("catalog", container=True, icon="folder")
        assert level.is_container is True
        assert level.icon == "folder"


class TestHierarchy:
    def test_empty_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one level"):
            Hierarchy([])

    def test_get_depth(self):
        h = Hierarchy([_level("catalog"), _level("schema"), _level("table")])
        assert h.get_depth() == 3

    def test_get_level_valid(self):
        h = Hierarchy([_level("catalog"), _level("schema"), _level("table")])
        assert h.get_level(0).name == "catalog"
        assert h.get_level(1).name == "schema"
        assert h.get_level(2).name == "table"

    def test_get_level_out_of_range(self):
        h = Hierarchy([_level("catalog")])
        assert h.get_level(-1) is None
        assert h.get_level(1) is None
        assert h.get_level(99) is None

    def test_get_level_by_name(self):
        h = Hierarchy([_level("catalog"), _level("schema"), _level("table")])
        level = h.get_level_by_name("schema")
        assert level is not None
        assert level.name == "schema"

    def test_get_level_by_name_not_found(self):
        h = Hierarchy([_level("catalog")])
        assert h.get_level_by_name("nonexistent") is None

    def test_get_level_depth(self):
        h = Hierarchy([_level("catalog"), _level("schema"), _level("table")])
        assert h.get_level_depth("catalog") == 0
        assert h.get_level_depth("schema") == 1
        assert h.get_level_depth("table") == 2

    def test_get_level_depth_not_found(self):
        h = Hierarchy([_level("catalog")])
        assert h.get_level_depth("missing") == -1

    def test_single_level(self):
        h = Hierarchy([_level("database")])
        assert h.get_depth() == 1
        assert h.get_level(0).name == "database"
        assert h.get_level_by_name("database") is not None
        assert h.get_level_depth("database") == 0
