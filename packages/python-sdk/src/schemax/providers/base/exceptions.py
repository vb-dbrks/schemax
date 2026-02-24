"""
Provider-level exceptions for schema operations
"""


class SchemaXProviderError(Exception):
    """Base exception for provider-level errors"""


class CircularDependencyError(SchemaXProviderError):
    """Raised when circular dependencies are detected in the dependency graph"""

    def __init__(self, cycles: list[list[str]]):
        self.cycles = cycles
        cycle_strs = []
        for cycle in cycles:
            cycle_str = " → ".join(cycle)
            cycle_strs.append(cycle_str)

        message = "Circular dependencies detected:\n" + "\n".join(
            f"  • {cycle_str}" for cycle_str in cycle_strs
        )
        super().__init__(message)


class DependencyValidationError(SchemaXProviderError):
    """Raised when dependency validation fails"""


class MissingDependencyError(DependencyValidationError):
    """Raised when a referenced object does not exist"""

    def __init__(self, object_name: str, missing_dependency: str):
        self.object_name = object_name
        self.missing_dependency = missing_dependency
        super().__init__(
            f"Object '{object_name}' references non-existent object '{missing_dependency}'"
        )
