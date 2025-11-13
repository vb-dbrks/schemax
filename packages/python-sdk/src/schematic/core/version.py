"""
Semantic Version Utilities

Provides version parsing and comparison utilities for semantic versioning (MAJOR.MINOR.PATCH).
"""

import re
from dataclasses import dataclass


@dataclass
class SemanticVersion:
    """Semantic version (MAJOR.MINOR.PATCH)"""

    major: int
    minor: int
    patch: int
    prefix: str = "v"

    def __str__(self) -> str:
        return f"{self.prefix}{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: "SemanticVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other: "SemanticVersion") -> bool:
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __gt__(self, other: "SemanticVersion") -> bool:
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    def __ge__(self, other: "SemanticVersion") -> bool:
        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def bump_major(self) -> "SemanticVersion":
        """Bump major version and reset minor/patch"""
        return SemanticVersion(self.major + 1, 0, 0, self.prefix)

    def bump_minor(self) -> "SemanticVersion":
        """Bump minor version and reset patch"""
        return SemanticVersion(self.major, self.minor + 1, 0, self.prefix)

    def bump_patch(self) -> "SemanticVersion":
        """Bump patch version"""
        return SemanticVersion(self.major, self.minor, self.patch + 1, self.prefix)


def parse_semantic_version(version_str: str) -> SemanticVersion:
    """Parse semantic version string

    Args:
        version_str: Version string (e.g., "v0.3.0", "0.3.0", "v1.2.3")

    Returns:
        SemanticVersion object

    Raises:
        ValueError: If version string is invalid

    Example:
        >>> parse_semantic_version("v0.3.0")
        SemanticVersion(major=0, minor=3, patch=0, prefix='v')
    """
    # Match patterns like: v0.3.0, 0.3.0, v1.2.3
    pattern = r"^(v)?(\d+)\.(\d+)\.(\d+)$"
    match = re.match(pattern, version_str)

    if not match:
        raise ValueError(
            f"Invalid semantic version: {version_str}. "
            f"Expected format: v0.3.0 or 0.3.0 (MAJOR.MINOR.PATCH)"
        )

    prefix = match.group(1) or ""
    major = int(match.group(2))
    minor = int(match.group(3))
    patch = int(match.group(4))

    return SemanticVersion(major, minor, patch, prefix)


def get_next_version(current_version: str, bump_type: str = "minor", prefix: str = "v") -> str:
    """Get next semantic version

    Args:
        current_version: Current version string (e.g., "v0.3.0")
        bump_type: Type of bump ("major", "minor", or "patch")
        prefix: Version prefix (default: "v")

    Returns:
        Next version string

    Example:
        >>> get_next_version("v0.3.0", "minor")
        "v0.4.0"
        >>> get_next_version("v0.3.0", "patch")
        "v0.3.1"
    """
    version = parse_semantic_version(current_version)

    if bump_type == "major":
        next_version = version.bump_major()
    elif bump_type == "patch":
        next_version = version.bump_patch()
    else:  # minor (default)
        next_version = version.bump_minor()

    # Use specified prefix
    next_version.prefix = prefix

    return str(next_version)


def get_versions_between(from_version: str, to_version: str, all_versions: list[str]) -> list[str]:
    """Get all versions between two versions

    Args:
        from_version: Starting version (exclusive)
        to_version: Ending version (exclusive)
        all_versions: List of all available versions

    Returns:
        List of versions between from_version and to_version, sorted

    Example:
        >>> get_versions_between("v0.3.0", "v0.5.0", ["v0.3.0", "v0.3.1", "v0.4.0", "v0.5.0"])
        ["v0.3.1", "v0.4.0"]
    """
    from_parsed = parse_semantic_version(from_version)
    to_parsed = parse_semantic_version(to_version)

    between = []
    for v in all_versions:
        try:
            parsed = parse_semantic_version(v)
            if from_parsed < parsed < to_parsed:
                between.append(v)
        except ValueError:
            # Skip invalid versions
            continue

    # Sort by semantic version
    return sorted(between, key=parse_semantic_version)
