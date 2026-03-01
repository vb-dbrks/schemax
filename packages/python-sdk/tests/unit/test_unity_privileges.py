"""Unit tests for Unity privilege constant sets."""

from schemax.providers.unity import privileges


def test_privilege_lists_include_core_permissions() -> None:
    assert "ALL PRIVILEGES" in privileges.CATALOG_PRIVILEGES
    assert "USE SCHEMA" in privileges.SCHEMA_PRIVILEGES
    assert "SELECT" in privileges.TABLE_VIEW_PRIVILEGES
    assert "READ VOLUME" in privileges.VOLUME_PRIVILEGES
    assert "EXECUTE" in privileges.FUNCTION_PRIVILEGES
    assert "REFRESH" in privileges.MATERIALIZED_VIEW_PRIVILEGES


def test_privilege_lists_do_not_contain_duplicates() -> None:
    privilege_sets = [
        privileges.CATALOG_PRIVILEGES,
        privileges.SCHEMA_PRIVILEGES,
        privileges.TABLE_VIEW_PRIVILEGES,
        privileges.VOLUME_PRIVILEGES,
        privileges.FUNCTION_PRIVILEGES,
        privileges.MATERIALIZED_VIEW_PRIVILEGES,
    ]
    for values in privilege_sets:
        assert len(values) == len(set(values))
