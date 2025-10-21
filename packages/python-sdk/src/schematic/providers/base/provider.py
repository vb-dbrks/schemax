"""
Base Provider Interface

Defines the contract that all catalog providers must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from .executor import ExecutionConfig, SQLExecutor
from .hierarchy import Hierarchy
from .models import ProviderState, ValidationResult
from .operations import Operation, OperationMetadata
from .sql_generator import SQLGenerator


class ProviderCapabilities(BaseModel):
    """Provider capabilities - defines what features this provider supports"""

    supported_operations: List[str]  # e.g., ['unity.add_catalog', 'unity.add_schema']
    supported_object_types: List[str]  # e.g., ['catalog', 'schema', 'table']
    hierarchy: Hierarchy  # Hierarchy definition

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Feature flags
    features: Dict[str, bool] = {
        "constraints": False,
        "row_filters": False,
        "column_masks": False,
        "column_tags": False,
        "table_properties": False,
        "comments": False,
        "partitioning": False,
        "views": False,
        "materialized_views": False,
        "functions": False,
        "indexes": False,
    }


class ProviderInfo(BaseModel):
    """Provider metadata"""

    id: str  # Unique provider ID (e.g., 'unity', 'hive')
    name: str  # Human-readable name
    version: str  # Provider version (semantic versioning)
    description: str  # Provider description
    author: Optional[str] = None  # Provider author/maintainer
    docs_url: Optional[str] = None  # Documentation URL


class Provider(ABC):
    """Main Provider interface"""

    @property
    @abstractmethod
    def info(self) -> ProviderInfo:
        """Provider metadata"""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """Provider capabilities"""
        pass

    @abstractmethod
    def get_operation_metadata(self, operation_type: str) -> Optional[OperationMetadata]:
        """Get operation metadata for a specific operation type"""
        pass

    @abstractmethod
    def get_all_operations(self) -> List[OperationMetadata]:
        """Get all operation metadata"""
        pass

    @abstractmethod
    def validate_operation(self, op: Operation) -> ValidationResult:
        """Validate an operation"""
        pass

    @abstractmethod
    def apply_operation(self, state: ProviderState, op: Operation) -> ProviderState:
        """Apply an operation to state (state reducer)"""
        pass

    @abstractmethod
    def apply_operations(self, state: ProviderState, ops: List[Operation]) -> ProviderState:
        """Apply multiple operations to state"""
        pass

    @abstractmethod
    def get_sql_generator(self, state: ProviderState) -> SQLGenerator:
        """Get SQL generator for this provider"""
        pass

    @abstractmethod
    def create_initial_state(self) -> ProviderState:
        """Create an empty/initial state for this provider"""
        pass

    @abstractmethod
    def validate_state(self, state: ProviderState) -> ValidationResult:
        """Validate the entire state structure"""
        pass

    @abstractmethod
    def get_sql_executor(self, config: ExecutionConfig) -> SQLExecutor:
        """Get SQL executor for this provider

        Args:
            config: Execution configuration (auth, warehouse, etc.)

        Returns:
            SQLExecutor instance for executing SQL statements

        Raises:
            NotImplementedError: If provider doesn't support SQL execution
        """
        pass

    @abstractmethod
    def validate_execution_config(self, config: ExecutionConfig) -> ValidationResult:
        """Validate execution configuration

        Validates authentication, warehouse/endpoint configuration, and
        other provider-specific execution requirements.

        Args:
            config: Execution configuration to validate

        Returns:
            ValidationResult with any configuration errors
        """
        pass


class BaseProvider(Provider):
    """Base implementation of Provider with common functionality"""

    def __init__(self):
        self.operation_metadata: Dict[str, OperationMetadata] = {}

    def get_operation_metadata(self, operation_type: str) -> Optional[OperationMetadata]:
        return self.operation_metadata.get(operation_type)

    def get_all_operations(self) -> List[OperationMetadata]:
        return list(self.operation_metadata.values())

    def apply_operations(self, state: ProviderState, ops: List[Operation]) -> ProviderState:
        current_state = state
        for op in ops:
            current_state = self.apply_operation(current_state, op)
        return current_state

    def register_operation(self, metadata: OperationMetadata) -> None:
        """Helper to register operation metadata"""
        self.operation_metadata[metadata.type] = metadata

    def is_operation_supported(self, operation_type: str) -> bool:
        """Helper to check if operation is supported"""
        return operation_type in self.capabilities.supported_operations
