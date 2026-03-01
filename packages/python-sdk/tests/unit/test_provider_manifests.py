"""Tests for provider capability manifest registry."""

from schemax.domain.providers import build_manifest_snapshot
from schemax.providers import ProviderRegistry


def test_manifest_snapshot_includes_registered_providers() -> None:
    manifests = build_manifest_snapshot(ProviderRegistry.get_all())
    manifest_ids = {manifest.provider_id for manifest in manifests}

    assert "unity" in manifest_ids
    assert "hive" in manifest_ids


def test_unity_manifest_reports_core_capabilities() -> None:
    manifests = {
        manifest.provider_id: manifest
        for manifest in build_manifest_snapshot(ProviderRegistry.get_all())
    }
    unity_manifest = manifests["unity"]

    assert unity_manifest.capabilities.reducer is True
    assert unity_manifest.capabilities.sql_generator is True
    assert unity_manifest.capabilities.execution is True
