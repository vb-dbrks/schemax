"""Integration tests for domain provider contracts and manifest registry."""

from __future__ import annotations

from schemax.domain.providers import (
    ProviderCapabilitySet,
    ProviderManifest,
    ProviderManifestRegistry,
    build_manifest_snapshot,
)
from schemax.providers.registry import ProviderRegistry


def _provider_ids(manifests: list[ProviderManifest]) -> list[str]:
    return [manifest.provider_id for manifest in manifests]


def _find_manifest(manifests: list[ProviderManifest], provider_id: str) -> ProviderManifest:
    for manifest in manifests:
        if manifest.provider_id == provider_id:
            return manifest
    raise AssertionError(f"Manifest for provider '{provider_id}' not found")


def test_manifest_snapshot_includes_registered_providers() -> None:
    """build_manifest_snapshot should emit manifests for auto-registered providers."""
    providers = ProviderRegistry.get_all()
    manifests = build_manifest_snapshot(providers)

    ids = _provider_ids(manifests)
    assert "unity" in ids
    assert "hive" in ids

    unity_manifest = _find_manifest(manifests, "unity")
    assert unity_manifest.name
    assert unity_manifest.version
    assert isinstance(unity_manifest.capabilities, ProviderCapabilitySet)
    assert unity_manifest.capabilities.sql_generator is True
    assert unity_manifest.capabilities.differ is True
    assert unity_manifest.capabilities.execution is True


def test_manifest_registry_get_and_all_are_deterministic() -> None:
    """ProviderManifestRegistry should return deterministic, sorted manifest lists."""
    providers = ProviderRegistry.get_all()
    assert providers, "Expected at least one registered provider"

    registry = ProviderManifestRegistry()
    for provider in reversed(providers):
        registry.register(provider)

    manifests = registry.all()
    ids = _provider_ids(manifests)
    assert ids == sorted(ids)

    for provider in providers:
        manifest = registry.get(provider.info.id)
        assert manifest is not None
        assert manifest.provider_id == provider.info.id
        assert isinstance(manifest.capabilities, ProviderCapabilitySet)
