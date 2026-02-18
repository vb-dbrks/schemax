"""Base provider abstractions"""

from .batching import BatchInfo, OperationBatcher
from .executor import ExecutionConfig, ExecutionResult, SQLExecutor, StatementResult
from .hierarchy import Hierarchy, HierarchyLevel
from .models import ProviderState, ValidationError, ValidationResult
from .operations import (
    ManagedCategory,
    Operation,
    OperationCategory,
    OperationMetadata,
    create_operation,
)
from .optimization import ColumnReorderOptimizer
from .provider import (
    BaseProvider,
    Provider,
    ProviderCapabilities,
    ProviderInfo,
)
from .scope_filter import filter_operations_by_managed_scope
from .sql_generator import BaseSQLGenerator, SQLGenerationResult, SQLGenerator

__all__ = [
    "Hierarchy",
    "HierarchyLevel",
    "ProviderState",
    "ValidationResult",
    "ValidationError",
    "Operation",
    "OperationMetadata",
    "OperationCategory",
    "ManagedCategory",
    "create_operation",
    "filter_operations_by_managed_scope",
    "Provider",
    "BaseProvider",
    "ProviderInfo",
    "ProviderCapabilities",
    "SQLGenerator",
    "BaseSQLGenerator",
    "SQLGenerationResult",
    "ExecutionConfig",
    "ExecutionResult",
    "SQLExecutor",
    "StatementResult",
    "OperationBatcher",
    "BatchInfo",
    "ColumnReorderOptimizer",
]
