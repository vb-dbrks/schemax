"""
Provider Hierarchy Definitions

Defines the hierarchical structure of objects within a provider.
"""


from pydantic import BaseModel


class HierarchyLevel(BaseModel):
    """Definition of a single level in the provider hierarchy"""

    name: str  # Internal name (e.g., "catalog", "schema", "table")
    display_name: str  # Display name for UI (e.g., "Catalog")
    plural_name: str  # Plural form for UI (e.g., "catalogs")
    icon: str | None = None  # Optional icon identifier
    is_container: bool  # Whether this level contains other levels


class Hierarchy:
    """Provider hierarchy implementation"""

    def __init__(self, levels: list[HierarchyLevel]):
        if not levels:
            raise ValueError("Hierarchy must have at least one level")
        self.levels = levels

    def get_level(self, depth: int) -> HierarchyLevel | None:
        """Get hierarchy level at a specific depth (0-indexed)"""
        if 0 <= depth < len(self.levels):
            return self.levels[depth]
        return None

    def get_depth(self) -> int:
        """Get the total depth of the hierarchy"""
        return len(self.levels)

    def get_level_by_name(self, name: str) -> HierarchyLevel | None:
        """Get level by internal name"""
        for level in self.levels:
            if level.name == name:
                return level
        return None

    def get_level_depth(self, name: str) -> int:
        """Get the depth of a specific level by name"""
        for i, level in enumerate(self.levels):
            if level.name == name:
                return i
        return -1
