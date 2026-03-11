"""Integration tests for version utilities and storage edge cases.

Covers: SemanticVersion comparison/bumping, get_versions_between, parse errors,
storage lock, ensure_project_file, validate_dependencies_internal.
"""

from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file, read_changelog, read_project
from schemax.core.version import (
    get_next_version,
    get_versions_between,
    parse_semantic_version,
)
from tests.utils import OperationBuilder


@pytest.mark.integration
class TestSemanticVersion:
    def test_parse_with_v_prefix(self) -> None:
        version = parse_semantic_version("v1.2.3")
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3
        assert version.prefix == "v"

    def test_parse_without_prefix(self) -> None:
        version = parse_semantic_version("1.2.3")
        assert version.prefix == ""
        assert str(version) == "1.2.3"

    def test_invalid_version_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid semantic version"):
            parse_semantic_version("not-a-version")

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_semantic_version("v1.2")

    def test_bump_major(self) -> None:
        version = parse_semantic_version("v1.2.3")
        bumped = version.bump_major()
        assert str(bumped) == "v2.0.0"

    def test_bump_minor(self) -> None:
        version = parse_semantic_version("v1.2.3")
        bumped = version.bump_minor()
        assert str(bumped) == "v1.3.0"

    def test_bump_patch(self) -> None:
        version = parse_semantic_version("v1.2.3")
        bumped = version.bump_patch()
        assert str(bumped) == "v1.2.4"

    def test_comparisons(self) -> None:
        ver_1 = parse_semantic_version("v0.1.0")
        ver_2 = parse_semantic_version("v0.2.0")
        ver_3 = parse_semantic_version("v1.0.0")

        assert ver_1 < ver_2
        assert ver_2 < ver_3
        assert ver_3 > ver_1
        assert ver_1 <= ver_1  # pylint: disable=comparison-with-itself
        assert ver_2 >= ver_1
        assert ver_1 == parse_semantic_version("v0.1.0")
        assert ver_1 != ver_2

    def test_equality_with_non_version(self) -> None:
        version = parse_semantic_version("v1.0.0")
        assert version != "v1.0.0"
        assert version != 42


@pytest.mark.integration
class TestGetNextVersion:
    def test_minor_bump(self) -> None:
        assert get_next_version("v0.3.0", "minor") == "v0.4.0"

    def test_patch_bump(self) -> None:
        assert get_next_version("v0.3.0", "patch") == "v0.3.1"

    def test_major_bump(self) -> None:
        assert get_next_version("v0.3.0", "major") == "v1.0.0"

    def test_default_is_minor(self) -> None:
        assert get_next_version("v1.0.0") == "v1.1.0"


@pytest.mark.integration
class TestGetVersionsBetween:
    def test_returns_intermediate_versions(self) -> None:
        all_versions = ["v0.1.0", "v0.2.0", "v0.3.0", "v0.4.0", "v0.5.0"]
        between = get_versions_between("v0.1.0", "v0.4.0", all_versions)
        assert between == ["v0.2.0", "v0.3.0"]

    def test_empty_when_adjacent(self) -> None:
        all_versions = ["v0.1.0", "v0.2.0"]
        between = get_versions_between("v0.1.0", "v0.2.0", all_versions)
        assert between == []

    def test_skips_invalid_versions(self) -> None:
        all_versions = ["v0.1.0", "bad", "v0.2.0", "v0.3.0"]
        between = get_versions_between("v0.1.0", "v0.3.0", all_versions)
        assert between == ["v0.2.0"]

    def test_sorted_output(self) -> None:
        all_versions = ["v0.3.0", "v0.1.0", "v0.2.0", "v0.4.0"]
        between = get_versions_between("v0.1.0", "v0.4.0", all_versions)
        assert between == ["v0.2.0", "v0.3.0"]

    def test_empty_list(self) -> None:
        between = get_versions_between("v0.1.0", "v0.5.0", [])
        assert between == []


@pytest.mark.integration
class TestStorageEdgeCases:
    def test_ensure_project_file_creates_structure(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        project = read_project(temp_workspace)
        scope = project.get("defaultTarget", "default")
        assert project["targets"][scope]["type"] == "unity"
        assert "environments" in project["targets"][scope]
        assert "dev" in project["targets"][scope]["environments"]

    def test_ensure_project_file_idempotent(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        ensure_project_file(temp_workspace, provider_id="unity")
        project = read_project(temp_workspace)
        assert project["version"] == 5

    def test_append_ops_adds_to_changelog(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        ops = [
            builder.catalog.add_catalog("c1", "bronze", op_id="op_1"),
            builder.schema.add_schema("s1", "raw", "c1", op_id="op_2"),
        ]
        append_ops(temp_workspace, ops)
        changelog = read_changelog(temp_workspace)
        assert len(changelog["ops"]) == 2

    def test_append_ops_extends_existing(self, temp_workspace: Path) -> None:
        ensure_project_file(temp_workspace, provider_id="unity")
        builder = OperationBuilder()
        append_ops(
            temp_workspace,
            [builder.catalog.add_catalog("c1", "bronze", op_id="op_1")],
        )
        append_ops(
            temp_workspace,
            [builder.schema.add_schema("s1", "raw", "c1", op_id="op_2")],
        )
        changelog = read_changelog(temp_workspace)
        assert len(changelog["ops"]) == 2
