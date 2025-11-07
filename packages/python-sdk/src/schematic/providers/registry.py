"""
Provider Registry

Central registry for all available catalog providers.
Providers must register themselves here to be available in the system.
"""

from .base.provider import Provider


class ProviderRegistryClass:
    """Registry for managing catalog providers"""

    def __init__(self) -> None:
        self.providers: dict[str, Provider] = {}

    def register(self, provider: Provider) -> None:
        """
        Register a provider

        Args:
            provider: Provider to register

        Raises:
            ValueError: If provider with same ID is already registered
        """
        if provider.info.id in self.providers:
            raise ValueError(f"Provider with ID '{provider.info.id}' is already registered")

        self.providers[provider.info.id] = provider
        import sys

        print(
            f"[Schematic] Registered provider: {provider.info.name} ({provider.info.id})",
            file=sys.stderr,
        )

    def get(self, provider_id: str) -> Provider | None:
        """
        Get a provider by ID

        Args:
            provider_id: Provider ID (e.g., 'unity', 'hive', 'postgres')

        Returns:
            Provider instance or None
        """
        return self.providers.get(provider_id)

    def get_all(self) -> list[Provider]:
        """
        Get all registered providers

        Returns:
            List of all providers
        """
        return list(self.providers.values())

    def get_all_ids(self) -> list[str]:
        """
        Get all provider IDs

        Returns:
            List of provider IDs
        """
        return list(self.providers.keys())

    def has(self, provider_id: str) -> bool:
        """
        Check if a provider is registered

        Args:
            provider_id: Provider ID to check

        Returns:
            True if provider is registered
        """
        return provider_id in self.providers

    def supports(self, provider_id: str, operation: str) -> bool:
        """
        Check if a provider supports a specific operation

        Args:
            provider_id: Provider ID
            operation: Operation type (e.g., 'unity.add_catalog')

        Returns:
            True if provider supports the operation
        """
        provider = self.get(provider_id)
        if provider is None:
            return False

        return operation in provider.capabilities.supported_operations

    def get_provider_for_operation(self, operation: str) -> Provider | None:
        """
        Get provider that supports a specific operation

        Args:
            operation: Operation type with provider prefix

        Returns:
            Provider or None
        """
        # Extract provider ID from operation (e.g., 'unity.add_catalog' -> 'unity')
        provider_id = operation.split(".")[0]
        return self.get(provider_id)

    def clear(self) -> None:
        """Clear all registered providers (useful for testing)"""
        self.providers.clear()

    def unregister(self, provider_id: str) -> None:
        """
        Unregister a provider

        Args:
            provider_id: Provider ID to unregister
        """
        if provider_id in self.providers:
            del self.providers[provider_id]


# Singleton instance
ProviderRegistry = ProviderRegistryClass()
