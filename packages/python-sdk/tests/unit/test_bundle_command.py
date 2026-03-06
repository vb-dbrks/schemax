"""Unit tests for bundle command module."""

import importlib.util
import json
from pathlib import Path

import pytest

from schemax.commands.bundle import generate_bundle


def _make_project(environments: dict | None = None) -> dict:
    if environments is None:
        environments = {
            "dev": {"topLevelName": "dev_catalog", "catalogMappings": {}},
            "prod": {"topLevelName": "prod_catalog", "catalogMappings": {}},
        }
    return {
        "version": 4,
        "name": "test_project",
        "provider": {
            "type": "unity",
            "version": "1.0.0",
            "environments": environments,
        },
        "settings": {"catalogMode": "multi"},
    }


def _write_project(workspace: Path, project: dict) -> None:
    schemax_dir = workspace / ".schemax"
    schemax_dir.mkdir(parents=True, exist_ok=True)
    (schemax_dir / "project.json").write_text(json.dumps(project))


class TestGenerateBundle:
    def test_generates_resource_and_deploy_files(self, tmp_path: Path) -> None:
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        result = generate_bundle(workspace=tmp_path, output_dir=output_dir)

        assert result["project_name"] == "test_project"
        assert set(result["environments"]) == {"dev", "prod"}
        assert (output_dir / "schemax.yml").exists()
        assert (output_dir / "schemax_deploy.py").exists()

    def test_resource_yaml_contains_job_definition(self, tmp_path: Path) -> None:
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "schemax_deploy_test_project:" in content
        assert "${bundle.target}" in content
        assert "${var.schemax_execution_mode}" in content
        assert "schemaxpy>=" in content
        assert "environment_key: schemax" in content

    def test_resource_yaml_lists_environments_in_comment(self, tmp_path: Path) -> None:
        envs = {
            "dev": {"topLevelName": "d"},
            "staging": {"topLevelName": "s"},
            "prod": {"topLevelName": "p"},
        }
        _write_project(tmp_path, _make_project(envs))
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "dev, prod, staging" in content

    def test_deploy_script_is_valid_python(self, tmp_path: Path) -> None:
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        compile(script, "schemax_deploy.py", "exec")

    def test_raises_on_no_environments(self, tmp_path: Path) -> None:
        _write_project(tmp_path, _make_project(environments={}))
        output_dir = tmp_path / "resources"

        with pytest.raises(ValueError, match="No environments configured"):
            generate_bundle(workspace=tmp_path, output_dir=output_dir)

    def test_sanitizes_project_name(self, tmp_path: Path) -> None:
        project = _make_project()
        project["name"] = "My Cool-Project"
        _write_project(tmp_path, project)
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "schemax_deploy_my_cool_project:" in content

    def test_creates_output_dir_if_missing(self, tmp_path: Path) -> None:
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "deep" / "nested" / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        assert (output_dir / "schemax.yml").exists()

    # ------------------------------------------------------------------
    # DAB compatibility: regressions caught during live deployment
    # ------------------------------------------------------------------

    def test_resource_yaml_uses_environment_version_not_client(self, tmp_path: Path) -> None:
        """client field is deprecated in DAB; must use environment_version."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "environment_version:" in content
        assert "client:" not in content

    def test_resource_yaml_no_environment_variables_field(self, tmp_path: Path) -> None:
        """environment_variables is not a valid DAB field; vars go via parameters."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "environment_variables:" not in content

    def test_resource_yaml_uses_parameters_for_task_inputs(self, tmp_path: Path) -> None:
        """Task inputs (env, execution mode, rollback, warehouse) must be passed via parameters."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "parameters:" in content
        assert '"${bundle.target}"' in content
        assert '"${var.schemax_execution_mode}"' in content
        assert '"${var.schemax_auto_rollback}"' in content
        assert '"${var.warehouse_id}"' in content

    def test_resource_yaml_python_file_is_relative_not_nested(self, tmp_path: Path) -> None:
        """python_file must be relative to the YAML file, not resources/resources/."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "python_file: schemax_deploy.py" in content
        assert "python_file: resources/" not in content

    def test_resource_yaml_output_filename_is_schemax_yml(self, tmp_path: Path) -> None:
        """Output file must be schemax.yml (not schemax_job.yml)."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        assert (output_dir / "schemax.yml").exists()
        assert not (output_dir / "schemax_job.yml").exists()

    def test_resource_yaml_has_job_tags(self, tmp_path: Path) -> None:
        """Generated job must include tags for discoverability."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert 'managed_by: "schemax"' in content
        assert 'schemax_project: "test_project"' in content
        assert "schemax_version:" in content

    def test_deploy_script_invokes_cli_in_process(self, tmp_path: Path) -> None:
        """Deploy script must call CLI in-process (not subprocess) for runtime auth."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        assert "from schemax.cli import cli" in script
        assert "standalone_mode=False" in script
        assert "import subprocess" not in script

    def test_deploy_script_reads_args_from_sys_argv(self, tmp_path: Path) -> None:
        """Deploy script must read inputs from sys.argv (not env vars)."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        assert "sys.argv" in script
        assert "os.environ" not in script

    def test_deploy_script_uses_execution_mode_local_by_default(self, tmp_path: Path) -> None:
        """Deploy script should default to local execution mode."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        assert "--execution-mode" in script
        assert '"local"' in script

    def test_resource_yaml_has_execution_mode_variable(self, tmp_path: Path) -> None:
        """YAML should have schemax_execution_mode variable defaulting to local."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert "schemax_execution_mode:" in content
        assert 'default: "local"' in content

    def test_resource_yaml_warehouse_id_optional(self, tmp_path: Path) -> None:
        """warehouse_id variable should have empty string default (optional in local mode)."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        content = (output_dir / "schemax.yml").read_text()
        assert 'default: ""' in content

    def test_deploy_script_does_not_require_profile(self, tmp_path: Path) -> None:
        """Deploy script must not pass --profile (serverless uses implicit auth)."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        assert "--profile" not in script

    def test_deploy_script_finds_workspace_via_schemax_dir(self, tmp_path: Path) -> None:
        """Deploy script must locate .schemax/ to resolve the workspace root."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        script = (output_dir / "schemax_deploy.py").read_text()
        assert ".schemax" in script
        assert "_find_workspace" in script

    def test_deploy_script_has_importable_find_workspace(self, tmp_path: Path) -> None:
        """_find_workspace must be callable and find .schemax/ from resources/."""
        _write_project(tmp_path, _make_project())
        output_dir = tmp_path / "resources"

        generate_bundle(workspace=tmp_path, output_dir=output_dir)

        deploy_path = output_dir / "schemax_deploy.py"
        spec = importlib.util.spec_from_file_location("schemax_deploy", deploy_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        assert hasattr(module, "_find_workspace")
        # _find_workspace should resolve to tmp_path (parent of resources/)
        found = module._find_workspace()
        assert Path(found) == tmp_path
