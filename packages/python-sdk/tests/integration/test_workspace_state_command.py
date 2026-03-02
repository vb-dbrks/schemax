"""Integration tests for workspace-state CLI transport payload."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


@pytest.mark.integration
def test_workspace_state_json_payload(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [
            builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1"),
            builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2"),
        ],
    )

    result = invoke_cli("workspace-state", "--json", str(temp_workspace))

    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "workspace-state"
    assert envelope["status"] == "success"
    payload = envelope["data"]
    assert payload["provider"]["id"] == "unity"
    assert isinstance(payload["state"], dict)
    assert isinstance(payload["changelog"]["ops"], list)
    assert len(payload["changelog"]["ops"]) >= 2
    assert "supported_operations" in payload["provider"]["capabilities"]


@pytest.mark.integration
def test_workspace_state_validate_dependencies_shape(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")

    result = invoke_cli(
        "workspace-state",
        "--json",
        "--validate-dependencies",
        str(temp_workspace),
    )

    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "workspace-state"
    payload = envelope["data"]
    assert "validation" in payload
    assert "errors" in payload["validation"]
    assert "warnings" in payload["validation"]


@pytest.mark.integration
def test_workspace_state_state_only_payload_mode(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1")],
    )

    result = invoke_cli(
        "workspace-state",
        "--json",
        "--payload-mode",
        "state-only",
        str(temp_workspace),
    )

    assert result.exit_code == 0
    envelope = json.loads(result.output)
    payload = envelope["data"]
    assert payload["changelog"]["ops"] == []
    assert payload["changelog"]["opsCount"] >= 1
