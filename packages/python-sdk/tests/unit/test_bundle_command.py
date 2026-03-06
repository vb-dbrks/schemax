"""Unit tests for bundle command module."""

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
        assert "${var.warehouse_id}" in content
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
