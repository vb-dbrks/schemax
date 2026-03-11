"""Tests for providers/base/exceptions.py."""

from schemax.providers.base.exceptions import (
    CircularDependencyError,
    DependencyValidationError,
    MissingDependencyError,
    SchemaXProviderError,
)


class TestExceptions:
    def test_base_error(self):
        err = SchemaXProviderError("base error")
        assert str(err) == "base error"

    def test_circular_dependency_error(self):
        err = CircularDependencyError([["a", "b", "a"], ["c", "d", "c"]])
        assert err.cycles == [["a", "b", "a"], ["c", "d", "c"]]
        assert "a → b → a" in str(err)
        assert "c → d → c" in str(err)
        assert "Circular dependencies" in str(err)

    def test_dependency_validation_error(self):
        err = DependencyValidationError("validation failed")
        assert isinstance(err, SchemaXProviderError)

    def test_missing_dependency_error(self):
        err = MissingDependencyError("my_view", "missing_table")
        assert err.object_name == "my_view"
        assert err.missing_dependency == "missing_table"
        assert "my_view" in str(err)
        assert "missing_table" in str(err)
