"""Application-facing provider manifest registry with capability discovery."""

from __future__ import annotations

from dataclasses import dataclass

from schemax.providers.base.provider import Provider

from .contracts import ProviderCapabilitySet, ProviderManifest


@dataclass(slots=True)
class ProviderCapabilitiesView:
    """Resolved capability view for one provider instance."""

    provider: Provider
    manifest: ProviderManifest


class ProviderManifestRegistry:
    """Builds typed capability manifests from registered provider instances."""

    def __init__(self) -> None:
        self._manifests: dict[str, ProviderManifest] = {}

    def register(self, provider: Provider) -> ProviderManifest:
        """Register/refresh manifest for a provider."""
        manifest = ProviderManifest(
            provider_id=provider.info.id,
            name=provider.info.name,
            version=provider.info.version,
            capabilities=self._detect_capabilities(provider),
        )
        self._manifests[provider.info.id] = manifest
        return manifest

    def get(self, provider_id: str) -> ProviderManifest | None:
        """Return manifest for provider if present."""
        return self._manifests.get(provider_id)

    def all(self) -> list[ProviderManifest]:
        """Return all manifests in deterministic order by provider id."""
        return [self._manifests[key] for key in sorted(self._manifests)]

    def _detect_capabilities(self, provider: Provider) -> ProviderCapabilitySet:
        """Infer capability availability from provider methods/features."""
        return ProviderCapabilitySet(
            reducer=hasattr(provider, "apply_operation") and hasattr(provider, "apply_operations"),
            differ=hasattr(provider, "get_state_differ"),
            sql_generator=hasattr(provider, "get_sql_generator"),
            execution=hasattr(provider, "get_sql_executor")
            and hasattr(provider, "validate_execution_config"),
            discovery=_supports_method(provider, "discover_state"),
            import_transform=_supports_method(provider, "prepare_import_state")
            and _supports_method(provider, "update_env_import_mappings"),
            rollback_safety=bool(provider.capabilities.features.get("baseline_adoption", False)),
            deployment_tracking=_supports_method(provider, "adopt_import_baseline"),
            ddl_import=_supports_method(provider, "state_from_ddl"),
        )


def _supports_method(provider: Provider, method_name: str) -> bool:
    """Conservatively detect whether a provider offers a custom capability method."""
    method = getattr(provider, method_name, None)
    if method is None:
        return False
    owner = method.__qualname__.split(".")[0] if hasattr(method, "__qualname__") else ""
    if owner in {"Provider", "BaseProvider"}:
        return False
    return True


def build_manifest_snapshot(providers: list[Provider]) -> list[ProviderManifest]:
    """Create a capability manifest list for all registered providers."""
    registry = ProviderManifestRegistry()
    for provider in providers:
        registry.register(provider)
    return registry.all()
