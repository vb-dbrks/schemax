"""
Unit tests for import command scaffolding.
"""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from schematic.cli import cli
from schematic.commands.import_assets import ImportError, import_from_provider
from schematic.providers.base.models import ValidationError, ValidationResult
from schematic.providers.base.operations import Operation


def _make_op(op_id: str) -> Operation:
    return Operation(
        id=op_id,
        ts="2026-02-10T00:00:00Z",
        provider="unity",
        op="unity.add_catalog",
        target="cat_1",
        payload={"catalogId": "cat_1", "name": "demo"},
    )


def _make_provider(
    *,
    valid_config: bool = True,
    discovered_state: dict | None = None,
    diff_ops: list[Operation] | None = None,
    prepared_state: dict | None = None,
    prepared_mappings: dict[str, str] | None = None,
    mappings_updated: bool = False,
    reject_information_schema: bool = True,
    supports_baseline_adoption: bool = True,
):
    errors = []
    if not valid_config:
        errors = [ValidationError(field="profile", message="invalid profile")]

    provider = SimpleNamespace()
    provider.info = SimpleNamespace(id="unity", name="Unity Catalog", version="1.0.0")
    provider.capabilities = SimpleNamespace(
        features={"baseline_adoption": supports_baseline_adoption}
    )
    provider.validate_execution_config = lambda _config: ValidationResult(
        valid=valid_config, errors=errors
    )
    provider.validate_import_scope = lambda scope: (
        ValidationResult(
            valid=False,
            errors=[
                ValidationError(
                    field="schema",
                    message=(
                        "Schema 'information_schema' is system-managed in Unity Catalog "
                        "and cannot be imported."
                    ),
                )
            ],
        )
        if reject_information_schema
        and str(scope.get("schema") or "").strip().lower() == "information_schema"
        else ValidationResult(valid=True, errors=[])
    )
    provider.discover_state = lambda config, scope: discovered_state or {"catalogs": []}
    provider.collect_import_warnings = lambda config, scope, discovered_state: []
    provider.prepare_import_state = (
        lambda local_state, discovered_state, env_config, mapping_overrides: (
            prepared_state if prepared_state is not None else discovered_state,
            prepared_mappings or {},
            mappings_updated,
        )
    )
    provider.update_env_import_mappings = lambda env_config, mappings: env_config.update(
        {"catalogMappings": dict(sorted(mappings.items()))}
    )
    provider.adopt_import_baseline = lambda **kwargs: "deploy_import_test"

    differ = SimpleNamespace(generate_diff_operations=lambda: diff_ops or [])
    provider.get_state_differ = lambda old_state, new_state, old_operations, new_operations: differ
    return provider


def _make_project() -> dict:
    return {
        "name": "demo_project",
        "latestSnapshot": "v0.1.0",
        "provider": {
            "environments": {
                "dev": {
                    "topLevelName": "dev_demo_project",
                    "autoCreateSchematicSchema": True,
                    "catalogMappings": {},
                }
            }
        },
    }


class TestImportFromProvider:
    def test_dry_run_does_not_append_ops(self):
        provider = _make_provider(diff_ops=[_make_op("op_1"), _make_op("op_2")])

        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.append_ops") as mock_append:
                with patch("schematic.commands.import_assets.read_project") as mock_project:
                    mock_project.return_value = _make_project()
                    mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

                    summary = import_from_provider(
                        workspace=None,  # workspace is mocked at storage boundary
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_123",
                        catalog="demo",
                        dry_run=True,
                    )

                    assert summary["operations_generated"] == 2
                    assert summary["dry_run"] is True
                    mock_append.assert_not_called()

    def test_non_dry_run_appends_generated_ops(self):
        ops = [_make_op("op_1")]
        provider = _make_provider(diff_ops=ops)

        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.append_ops") as mock_append:
                with patch("schematic.commands.import_assets.read_project") as mock_project:
                    mock_project.return_value = _make_project()
                    mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

                    summary = import_from_provider(
                        workspace=None,  # workspace is mocked at storage boundary
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_123",
                        dry_run=False,
                    )

                    assert summary["operations_generated"] == 1
                    mock_append.assert_called_once()
                    append_workspace, append_ops_value = mock_append.call_args[0]
                    assert append_workspace is None
                    assert append_ops_value == ops

    def test_invalid_execution_config_raises_import_error(self):
        provider = _make_provider(valid_config=False)

        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

            try:
                import_from_provider(
                    workspace=None,
                    target_env="dev",
                    profile="BROKEN",
                    warehouse_id="wh_123",
                )
                assert False, "Expected ImportError"
            except ImportError as err:
                assert "Invalid execution configuration" in str(err)
                assert "profile" in str(err)

    def test_discover_not_implemented_is_wrapped(self):
        provider = _make_provider()
        provider.discover_state = lambda config, scope: (_ for _ in ()).throw(
            NotImplementedError("discovery not implemented")
        )

        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

            try:
                import_from_provider(
                    workspace=None,
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_123",
                )
                assert False, "Expected ImportError"
            except ImportError as err:
                assert "discovery not implemented" in str(err)

    def test_information_schema_scope_is_rejected(self):
        provider = _make_provider()
        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)
            with pytest.raises(ImportError, match="information_schema"):
                import_from_provider(
                    workspace=None,
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_123",
                    catalog="main",
                    schema="information_schema",
                )

    def test_adopt_baseline_creates_snapshot_and_tracks_deployment(self):
        provider = _make_provider(diff_ops=[_make_op("op_1")])
        provider.adopt_import_baseline = Mock(return_value="deploy_import_1234")

        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.append_ops") as mock_append:
                with patch("schematic.commands.import_assets.read_project") as mock_project:
                    with patch("schematic.commands.import_assets.create_snapshot") as mock_snapshot:
                        mock_load.return_value = (
                            {"catalogs": []},
                            {"ops": []},
                            provider,
                            None,
                        )
                        mock_project.return_value = _make_project()
                        mock_snapshot.return_value = (
                            _make_project() | {"latestSnapshot": "v0.2.0"},
                            {"version": "v0.2.0"},
                        )

                        summary = import_from_provider(
                            workspace=None,
                            target_env="dev",
                            profile="DEFAULT",
                            warehouse_id="wh_123",
                            dry_run=False,
                            adopt_baseline=True,
                        )

        mock_append.assert_called_once()
        mock_snapshot.assert_called_once()
        provider.adopt_import_baseline.assert_called_once()
        assert summary["snapshot_version"] == "v0.2.0"
        assert summary["deployment_id"] == "deploy_import_1234"

    def test_adopt_baseline_rejected_when_provider_capability_missing(self):
        provider = _make_provider(supports_baseline_adoption=False)
        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.read_project") as mock_project:
                mock_project.return_value = _make_project()
                mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)
                with pytest.raises(ImportError, match="does not support baseline adoption"):
                    import_from_provider(
                        workspace=None,
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_123",
                        dry_run=False,
                        adopt_baseline=True,
                    )

    def test_import_uses_provider_prepared_state_and_mappings(self):
        discovered_state = {
            "catalogs": [{"id": "cat_phys", "name": "dev_schematic_demo", "schemas": []}]
        }
        prepared_state = {
            "catalogs": [{"id": "cat_local", "name": "schematic_demo", "schemas": []}]
        }
        provider = _make_provider(
            discovered_state=discovered_state,
            prepared_state=prepared_state,
            prepared_mappings={"schematic_demo": "dev_schematic_demo"},
            diff_ops=[],
        )
        provider.get_state_differ = Mock(
            return_value=SimpleNamespace(generate_diff_operations=lambda: [])
        )

        project = _make_project()
        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.read_project") as mock_project:
                mock_project.return_value = project
                mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

                summary = import_from_provider(
                    workspace=None,
                    target_env="dev",
                    profile="DEFAULT",
                    warehouse_id="wh_123",
                    dry_run=True,
                )

        assert summary["catalog_mappings"] == {"schematic_demo": "dev_schematic_demo"}
        kwargs = provider.get_state_differ.call_args.kwargs
        assert kwargs["new_state"] == prepared_state

    def test_non_dry_run_persists_catalog_mappings_via_provider_hook(self):
        provider = _make_provider(
            prepared_mappings={"schematic_demo": "dev_schematic_demo"},
            mappings_updated=True,
            diff_ops=[],
        )
        provider.update_env_import_mappings = Mock(
            side_effect=lambda env_config, mappings: env_config.update(
                {"catalogMappings": dict(sorted(mappings.items()))}
            )
        )

        project = _make_project()
        with patch("schematic.commands.import_assets.load_current_state") as mock_load:
            with patch("schematic.commands.import_assets.read_project") as mock_project:
                with patch("schematic.commands.import_assets.write_project") as mock_write_project:
                    mock_project.return_value = project
                    mock_load.return_value = ({"catalogs": []}, {"ops": []}, provider, None)

                    import_from_provider(
                        workspace=None,
                        target_env="dev",
                        profile="DEFAULT",
                        warehouse_id="wh_123",
                        dry_run=False,
                    )

        provider.update_env_import_mappings.assert_called_once()
        assert mock_write_project.call_count == 1
        persisted_project = mock_write_project.call_args.args[1]
        assert persisted_project["provider"]["environments"]["dev"]["catalogMappings"] == {
            "schematic_demo": "dev_schematic_demo"
        }


class TestImportCli:
    def test_import_cli_requires_catalog_for_schema(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "import",
                "--target",
                "dev",
                "--profile",
                "DEFAULT",
                "--warehouse-id",
                "wh_123",
                "--schema",
                "analytics",
            ],
        )
        assert result.exit_code == 1
        assert "--schema requires --catalog" in result.output

    def test_import_cli_rejects_information_schema(self):
        runner = CliRunner()
        with patch(
            "schematic.cli.import_from_provider",
            side_effect=ImportError("Schema 'information_schema' is system-managed"),
        ):
            result = runner.invoke(
                cli,
                [
                    "import",
                    "--target",
                    "dev",
                    "--profile",
                    "DEFAULT",
                    "--warehouse-id",
                    "wh_123",
                    "--catalog",
                    "main",
                    "--schema",
                    "information_schema",
                ],
            )
            assert result.exit_code == 1
            assert "system-managed" in result.output

    def test_import_cli_routes_to_command(self):
        runner = CliRunner()
        with patch("schematic.cli.import_from_provider") as mock_import:
            result = runner.invoke(
                cli,
                [
                    "import",
                    "--target",
                    "dev",
                    "--profile",
                    "DEFAULT",
                    "--warehouse-id",
                    "wh_123",
                    "--catalog",
                    "demo",
                    "--dry-run",
                ],
            )

            assert result.exit_code == 0
            mock_import.assert_called_once()
            kwargs = mock_import.call_args.kwargs
            assert kwargs["target_env"] == "dev"
            assert kwargs["profile"] == "DEFAULT"
            assert kwargs["warehouse_id"] == "wh_123"
            assert kwargs["catalog"] == "demo"
            assert kwargs["dry_run"] is True

    def test_import_cli_passes_catalog_mapping_overrides(self):
        runner = CliRunner()
        with patch("schematic.cli.import_from_provider") as mock_import:
            result = runner.invoke(
                cli,
                [
                    "import",
                    "--target",
                    "dev",
                    "--profile",
                    "DEFAULT",
                    "--warehouse-id",
                    "wh_123",
                    "--catalog-map",
                    "schematic_demo=dev_schematic_demo",
                    "--dry-run",
                ],
            )

            assert result.exit_code == 0
            kwargs = mock_import.call_args.kwargs
            assert kwargs["catalog_mappings_override"] == {"schematic_demo": "dev_schematic_demo"}

    def test_import_cli_rejects_invalid_catalog_mapping_format(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "import",
                "--target",
                "dev",
                "--profile",
                "DEFAULT",
                "--warehouse-id",
                "wh_123",
                "--catalog-map",
                "invalid-format",
            ],
        )
        assert result.exit_code == 1
        assert "logical=physical" in result.output

    def test_import_cli_prints_summary_from_import_result(self):
        runner = CliRunner()
        with patch(
            "schematic.cli.import_from_provider",
            return_value={
                "operations_generated": 12,
                "dry_run": True,
                "warnings": ["w1"],
                "catalog_mappings": {"samples": "samples"},
            },
        ):
            result = runner.invoke(
                cli,
                [
                    "import",
                    "--target",
                    "dev",
                    "--profile",
                    "DEFAULT",
                    "--warehouse-id",
                    "wh_123",
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0
        assert "Import summary: 12 operation(s) previewed." in result.output
        assert "Catalog mappings: 1" in result.output
        assert "Warnings: 1" in result.output
