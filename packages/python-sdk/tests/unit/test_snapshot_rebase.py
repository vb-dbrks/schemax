"""
Tests for snapshot rebase functionality
"""

from pathlib import Path

import pytest

from schematic.commands.snapshot_rebase import (
    ConflictError,
    RebaseError,
    detect_stale_snapshots,
    rebase_snapshot,
)
from schematic.core.version import (
    SemanticVersion,
    get_next_version,
    get_versions_between,
    parse_semantic_version,
)


class TestSemanticVersion:
    """Test semantic version parsing and comparison"""

    def test_parse_version_with_prefix(self):
        """Test parsing version with v prefix"""
        version = parse_semantic_version("v0.3.0")

        assert version.major == 0
        assert version.minor == 3
        assert version.patch == 0
        assert version.prefix == "v"
        assert str(version) == "v0.3.0"

    def test_parse_version_without_prefix(self):
        """Test parsing version without prefix"""
        version = parse_semantic_version("0.3.0")

        assert version.major == 0
        assert version.minor == 3
        assert version.patch == 0
        assert version.prefix == ""
        assert str(version) == "0.3.0"

    def test_parse_invalid_version(self):
        """Test parsing invalid version raises error"""
        with pytest.raises(ValueError, match="Invalid semantic version"):
            parse_semantic_version("v0.3")

        with pytest.raises(ValueError, match="Invalid semantic version"):
            parse_semantic_version("invalid")

    def test_version_comparison(self):
        """Test version comparison operators"""
        v1 = SemanticVersion(0, 3, 0)
        v2 = SemanticVersion(0, 3, 1)
        v3 = SemanticVersion(0, 4, 0)

        assert v1 < v2
        assert v2 < v3
        assert v1 < v3
        assert v3 > v1
        assert v2 >= v2
        assert v1 <= v2

    def test_version_bump_major(self):
        """Test major version bump"""
        v = SemanticVersion(0, 3, 5)
        next_v = v.bump_major()

        assert next_v.major == 1
        assert next_v.minor == 0
        assert next_v.patch == 0

    def test_version_bump_minor(self):
        """Test minor version bump"""
        v = SemanticVersion(0, 3, 5)
        next_v = v.bump_minor()

        assert next_v.major == 0
        assert next_v.minor == 4
        assert next_v.patch == 0

    def test_version_bump_patch(self):
        """Test patch version bump"""
        v = SemanticVersion(0, 3, 5)
        next_v = v.bump_patch()

        assert next_v.major == 0
        assert next_v.minor == 3
        assert next_v.patch == 6

    def test_get_next_version(self):
        """Test getting next version"""
        assert get_next_version("v0.3.0", "minor") == "v0.4.0"
        assert get_next_version("v0.3.0", "patch") == "v0.3.1"
        assert get_next_version("v0.3.0", "major") == "v1.0.0"

    def test_get_versions_between(self):
        """Test getting versions between two versions"""
        all_versions = ["v0.3.0", "v0.3.1", "v0.3.2", "v0.4.0", "v0.5.0"]

        between = get_versions_between("v0.3.0", "v0.5.0", all_versions)

        assert between == ["v0.3.1", "v0.3.2", "v0.4.0"]

    def test_get_versions_between_no_intermediates(self):
        """Test getting versions when there are none between"""
        all_versions = ["v0.3.0", "v0.4.0", "v0.5.0"]

        between = get_versions_between("v0.3.0", "v0.4.0", all_versions)

        assert between == []


class TestSnapshotRebase:
    """Test snapshot rebase functionality"""

    def test_detect_stale_snapshots(self, tmp_path):
        """Test detecting stale snapshots after git rebase"""
        # Create project with snapshots
        project_file = tmp_path / ".schematic" / "project.json"
        project_file.parent.mkdir(parents=True)

        import json

        project = {
            "version": 4,
            "name": "test-project",
            "provider": {"type": "unity"},
            "snapshots": [
                {"version": "v0.3.0", "previousSnapshot": None},
                {"version": "v0.3.1", "previousSnapshot": "v0.3.0"},  # Hotfix
                {"version": "v0.4.0", "previousSnapshot": "v0.3.0"},  # Feature (stale!)
            ],
        }

        with open(project_file, "w") as f:
            json.dump(project, f)

        # Detect stale snapshots
        stale = detect_stale_snapshots(tmp_path)

        assert len(stale) == 1
        assert stale[0]["version"] == "v0.4.0"
        assert stale[0]["currentBase"] == "v0.3.0"
        assert stale[0]["shouldBeBase"] == "v0.3.1"
        assert stale[0]["missing"] == ["v0.3.1"]

    def test_detect_no_stale_snapshots(self, tmp_path):
        """Test when all snapshots are up to date"""
        project_file = tmp_path / ".schematic" / "project.json"
        project_file.parent.mkdir(parents=True)

        import json

        project = {
            "version": 4,
            "name": "test-project",
            "provider": {"type": "unity"},
            "snapshots": [
                {"version": "v0.3.0", "previousSnapshot": None},
                {"version": "v0.4.0", "previousSnapshot": "v0.3.0"},
            ],
        }

        with open(project_file, "w") as f:
            json.dump(project, f)

        stale = detect_stale_snapshots(tmp_path)

        assert len(stale) == 0


# Note: Full integration tests for rebase_snapshot() would require
# setting up complete project structure with snapshots, provider registry, etc.
# These would be better as integration tests.

