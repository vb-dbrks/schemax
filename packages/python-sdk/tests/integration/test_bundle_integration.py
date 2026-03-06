"""Integration tests for the SchemaX bundle command.

Tests the full workflow through multiple commands (init -> add ops -> bundle)
using real SchemaX workspaces. No live Databricks connection required.
"""

import importlib.util
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from schemax.cli import cli
from schemax.commands.bundle import generate_bundle
from schemax.core.storage import append_ops, create_snapshot, ensure_project_file
from schemax.version import SCHEMAX_VERSION
from tests.utils import OperationBuilder


def _write_project_with_envs(
    workspace: Path,
    *,
    name: str = "test_project",
    environments: dict | None = None,
) -> None:
    """Initialize a workspace and patch in custom environments."""
    ensure_project_file(workspace, provider_id="unity")
    project_path = workspace / ".schemax" / "project.json"
    project = json.loads(project_path.read_text())
    project["name"] = name
    if environments is not None:
        project["provider"]["environments"] = environments
    project_path.write_text(json.dumps(project, indent=2))


@pytest.mark.integration
class TestBundleIntegration:
    """Integration tests exercising bundle generation through real workspace workflows."""

    # ------------------------------------------------------------------
    # 1. Full workflow: init -> add ops -> bundle
    # ------------------------------------------------------------------
    def test_init_add_ops_then_bundle(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")

        builder = OperationBuilder()
        append_ops(
            workspace,
            [
                builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1"),
                builder.schema.add_schema("sch_1", "core", "cat_1", op_id="op_2"),
                builder.table.add_table("tbl_1", "users", "sch_1", "delta", op_id="op_3"),
                builder.column.add_column(
                    "col_1", "tbl_1", "id", "INT", nullable=False, op_id="op_4"
                ),
            ],
        )

        output_dir = tmp_path / "resources"
        result = generate_bundle(workspace=workspace, output_dir=output_dir)

        # Verify output files exist
        assert (output_dir / "schemax.yml").exists()
        assert (output_dir / "schemax_deploy.py").exists()

        # Verify result metadata
        assert result["schemax_version"] == SCHEMAX_VERSION
        assert len(result["environments"]) > 0

        # Verify YAML content references the project
        yaml_content = (output_dir / "schemax.yml").read_text()
        assert "schemax_deploy_" in yaml_content
        assert "schemaxpy>=" in yaml_content
        assert "${bundle.target}" in yaml_content

    # ------------------------------------------------------------------
    # 2. Bundle output reflects project config (custom environments)
    # ------------------------------------------------------------------
    def test_bundle_reflects_custom_environments(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        environments = {
            "dev": {"topLevelName": "dev_cat", "catalogMappings": {}},
            "staging": {"topLevelName": "stg_cat", "catalogMappings": {}},
            "prod": {"topLevelName": "prd_cat", "catalogMappings": {}},
        }
        _write_project_with_envs(workspace, name="multi_env_project", environments=environments)

        output_dir = tmp_path / "resources"
        result = generate_bundle(workspace=workspace, output_dir=output_dir)

        yaml_content = (output_dir / "schemax.yml").read_text()

        # Comment should list all three environments (sorted)
        assert "dev, prod, staging" in yaml_content

        # Job name should include sanitized project name
        assert "schemax_deploy_multi_env_project" in yaml_content
        assert 'name: "schemax-deploy-multi_env_project-${bundle.target}"' in yaml_content

        # Result should report all environments
        assert set(result["environments"]) == {"dev", "staging", "prod"}

    # ------------------------------------------------------------------
    # 3. Bundle with snapshot workflow
    # ------------------------------------------------------------------
    def test_bundle_after_snapshot(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")

        builder = OperationBuilder()
        append_ops(
            workspace,
            [
                builder.catalog.add_catalog("cat_1", "analytics", op_id="op_1"),
                builder.schema.add_schema("sch_1", "raw", "cat_1", op_id="op_2"),
            ],
        )

        # Create a snapshot
        create_snapshot(workspace, name="initial", version="v0.1.0")

        # Add more ops after snapshot
        append_ops(
            workspace,
            [
                builder.table.add_table("tbl_1", "events", "sch_1", "delta", op_id="op_3"),
            ],
        )

        output_dir = tmp_path / "resources"
        result = generate_bundle(workspace=workspace, output_dir=output_dir)

        # Bundle should reference current schemax version
        assert result["schemax_version"] == SCHEMAX_VERSION

        yaml_content = (output_dir / "schemax.yml").read_text()
        assert f"schemaxpy>={SCHEMAX_VERSION}" in yaml_content

    # ------------------------------------------------------------------
    # 4. CLI integration via CliRunner
    # ------------------------------------------------------------------
    def test_cli_bundle_command(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")

        output_dir = workspace / "resources"
        runner = CliRunner()

        # The bundle command uses Path.cwd(), so we must invoke from the workspace
        import os

        original_cwd = Path.cwd()
        os.chdir(workspace)
        try:
            result = runner.invoke(
                cli, ["bundle", "--output", str(output_dir)], catch_exceptions=True
            )
        finally:
            os.chdir(original_cwd)

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Generated DAB resources" in result.output
        assert (output_dir / "schemax.yml").exists()
        assert (output_dir / "schemax_deploy.py").exists()

    # ------------------------------------------------------------------
    # 5. Generated deploy script is importable with main() function
    # ------------------------------------------------------------------
    def test_deploy_script_is_importable(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")

        output_dir = tmp_path / "resources"
        generate_bundle(workspace=workspace, output_dir=output_dir)

        deploy_path = output_dir / "schemax_deploy.py"
        assert deploy_path.exists()

        # Import the generated script as a module
        spec = importlib.util.spec_from_file_location("schemax_deploy", deploy_path)
        assert spec is not None
        assert spec.loader is not None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Verify main function exists and is callable
        assert hasattr(module, "main")
        assert callable(module.main)

    # ------------------------------------------------------------------
    # 6. Bundle idempotency (run twice, files overwritten cleanly)
    # ------------------------------------------------------------------
    def test_bundle_idempotency(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        ensure_project_file(workspace, provider_id="unity")

        builder = OperationBuilder()
        append_ops(
            workspace,
            [builder.catalog.add_catalog("cat_1", "warehouse", op_id="op_1")],
        )

        output_dir = tmp_path / "resources"

        # First run
        result_1 = generate_bundle(workspace=workspace, output_dir=output_dir)
        yaml_1 = (output_dir / "schemax.yml").read_text()
        script_1 = (output_dir / "schemax_deploy.py").read_text()

        # Second run (should overwrite without error)
        result_2 = generate_bundle(workspace=workspace, output_dir=output_dir)
        yaml_2 = (output_dir / "schemax.yml").read_text()
        script_2 = (output_dir / "schemax_deploy.py").read_text()

        # Output should be identical
        assert yaml_1 == yaml_2
        assert script_1 == script_2
        assert result_1["environments"] == result_2["environments"]
        assert result_1["schemax_version"] == result_2["schemax_version"]

    # ------------------------------------------------------------------
    # 7. Bundle with complex project names (hyphens, spaces, mixed case)
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "project_name, expected_safe_name",
        [
            ("My Cool-Project", "my_cool_project"),
            ("data-pipeline-v2", "data_pipeline_v2"),
            ("UPPER CASE NAME", "upper_case_name"),
            ("simple", "simple"),
            ("mixed-Case With Spaces", "mixed_case_with_spaces"),
        ],
    )
    def test_bundle_with_complex_project_names(
        self,
        tmp_path: Path,
        project_name: str,
        expected_safe_name: str,
    ) -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        environments = {
            "dev": {"topLevelName": "dev_cat", "catalogMappings": {}},
        }
        _write_project_with_envs(workspace, name=project_name, environments=environments)

        output_dir = tmp_path / "resources"
        result = generate_bundle(workspace=workspace, output_dir=output_dir)

        # Original name preserved in result
        assert result["project_name"] == project_name

        yaml_content = (output_dir / "schemax.yml").read_text()

        # Sanitized name used in YAML identifiers
        assert f"schemax_deploy_{expected_safe_name}:" in yaml_content
        assert f'name: "schemax-deploy-{expected_safe_name}-${{bundle.target}}"' in yaml_content
