"""
Integration tests for import + baseline adoption workflows.
"""

import pytest

from schemax.commands.apply import apply_to_environment
from schemax.commands.import_assets import import_from_provider
from schemax.core.storage import ensure_project_file, load_current_state
from schemax.providers import ProviderRegistry


class _FakeDeploymentTracker:
    deployments_by_env: dict[str, list[dict[str, str]]] = {}

    def __init__(self, client, catalog: str, warehouse_id: str):
        del client, catalog, warehouse_id

    def ensure_tracking_schema(self, auto_create: bool = True) -> None:
        del auto_create

    def get_latest_deployment(self, environment: str):
        deployments = self.deployments_by_env.get(environment, [])
        if not deployments:
            return None
        return {"version": deployments[-1]["version"], "id": deployments[-1]["id"]}

    def get_most_recent_deployment_id(self, environment: str):
        deployments = self.deployments_by_env.get(environment, [])
        if not deployments:
            return None
        return deployments[-1]["id"]

    def start_deployment(
        self,
        deployment_id: str,
        environment: str,
        snapshot_version: str,
        project_name: str,
        provider_type: str,
        provider_version: str,
        schemax_version: str = "0.2.0",
        from_snapshot_version: str | None = None,
        previous_deployment_id: str | None = None,
    ) -> None:
        del (
            project_name,
            provider_type,
            provider_version,
            schemax_version,
            from_snapshot_version,
            previous_deployment_id,
        )
        self.deployments_by_env.setdefault(environment, []).append(
            {"id": deployment_id, "version": snapshot_version}
        )

    def complete_deployment(self, deployment_id, result, error_message=None) -> None:
        del deployment_id, result, error_message


@pytest.mark.integration
def test_import_adopt_baseline_then_apply_is_zero_diff(monkeypatch, schemax_demo_workspace):
    # Ensure fixture is initialized correctly if test data is refreshed manually.
    ensure_project_file(schemax_demo_workspace, provider_id="unity")
    state, _, _, _ = load_current_state(schemax_demo_workspace, validate=False)

    provider = ProviderRegistry.get("unity")
    assert provider is not None

    # Use discovered state == local state so import produces no additional operations.
    monkeypatch.setattr(provider, "discover_state", lambda config, scope: state)

    # Reset fake tracker state
    _FakeDeploymentTracker.deployments_by_env = {}

    fake_client = object()
    monkeypatch.setattr(
        "schemax.providers.unity.provider.create_databricks_client", lambda _: fake_client
    )
    monkeypatch.setattr(
        "schemax.providers.unity.auth.create_databricks_client", lambda _: fake_client
    )
    monkeypatch.setattr("schemax.core.deployment.DeploymentTracker", _FakeDeploymentTracker)
    monkeypatch.setattr("schemax.commands.apply.DeploymentTracker", _FakeDeploymentTracker)

    summary = import_from_provider(
        workspace=schemax_demo_workspace,
        target_env="dev",
        profile="",
        warehouse_id="wh_123",
        dry_run=False,
        adopt_baseline=True,
    )
    assert summary["adopt_baseline"] is True
    assert summary["operations_generated"] == 0
    assert summary["snapshot_version"]
    assert summary["deployment_id"].startswith("deploy_import_")

    result = apply_to_environment(
        workspace=schemax_demo_workspace,
        target_env="dev",
        profile="",
        warehouse_id="wh_123",
        dry_run=True,
        no_interaction=True,
    )

    assert result.status == "success"
    assert result.total_statements == 0
    assert result.successful_statements == 0
