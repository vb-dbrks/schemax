"""JSON contract checks for extension-consumed CLI commands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from schemax.core.storage import append_ops, ensure_project_file
from tests.utils import OperationBuilder
from tests.utils.cli_helpers import invoke_cli


def _read_contract_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parents[4] / "contracts" / "cli-envelopes" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


@pytest.mark.integration
def test_validate_json_contract(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")

    result = invoke_cli("validate", "--json", str(temp_workspace))

    assert result.exit_code in {0, 1}
    envelope = json.loads(result.output)
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "validate"
    assert envelope["status"] in {"success", "error"}
    assert isinstance(envelope["warnings"], list)
    assert isinstance(envelope["errors"], list)
    assert isinstance(envelope["meta"], dict)
    payload = envelope["data"]
    assert "valid" in payload
    assert "errors" in payload
    assert "warnings" in payload
    assert isinstance(payload["errors"], list)
    assert isinstance(payload["warnings"], list)


@pytest.mark.integration
def test_snapshot_validate_json_contract(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("snapshot_validate.success.json")

    result = invoke_cli("snapshot", "validate", "--json", str(temp_workspace))

    assert result.exit_code in {0, 1}
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "snapshot.validate"
    assert envelope["status"] == "success"
    payload = envelope["data"]
    assert "stale" in payload
    assert "count" in payload
    assert isinstance(payload["stale"], list)
    assert isinstance(payload["count"], int)


@pytest.mark.integration
def test_workspace_state_json_contract_shape(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("workspace_state.success.json")

    result = invoke_cli("workspace-state", "--json", str(temp_workspace))

    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "workspace-state"
    assert envelope["status"] == "success"
    payload = envelope["data"]
    assert "state" in payload
    assert "changelog" in payload
    assert "provider" in payload
    assert "project" in payload
    assert "validation" in payload


@pytest.mark.integration
def test_sql_json_contract_shape(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("sql.success.json")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [
            builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1"),
            builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2"),
            builder.table.add_table("table_1", "users", "schema_1", "delta", op_id="op_3"),
        ],
    )

    result = invoke_cli("sql", "--json", str(temp_workspace))
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "sql"
    assert envelope["status"] == "success"
    assert "sql" in envelope["data"]


@pytest.mark.integration
def test_rollback_json_contract_invalid_args(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("rollback.invalid-args.error.json")
    result = invoke_cli("rollback", "--json", str(temp_workspace))
    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "rollback"
    assert envelope["status"] == "error"
    assert envelope["errors"][0]["code"] == "ROLLBACK_INVALID_ARGS"


@pytest.mark.integration
def test_import_json_contract_shape(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("import.success.json")
    sql_file = temp_workspace / "import_fixture.sql"
    sql_file.write_text(
        (
            "CREATE CATALOG IF NOT EXISTS bronze;\n"
            "CREATE SCHEMA IF NOT EXISTS bronze.core;\n"
            "CREATE TABLE IF NOT EXISTS bronze.core.users (id INT);\n"
        ),
        encoding="utf-8",
    )
    result = invoke_cli(
        "import",
        "--json",
        "--from-sql",
        str(sql_file),
        "--mode",
        "diff",
        "--dry-run",
        str(temp_workspace),
    )
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "import"
    assert envelope["status"] == "success"
    assert "summary" in envelope["data"]


@pytest.mark.integration
def test_diff_json_contract_shape(temp_workspace: Path) -> None:
    ensure_project_file(temp_workspace, provider_id="unity")
    contract = _read_contract_fixture("diff.success.json")
    builder = OperationBuilder()
    append_ops(
        temp_workspace,
        [builder.catalog.add_catalog("cat_1", "bronze", op_id="op_1")],
    )
    assert (
        invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Base",
            "--version",
            "v0.1.0",
            str(temp_workspace),
        ).exit_code
        == 0
    )
    append_ops(
        temp_workspace,
        [builder.schema.add_schema("schema_1", "core", "cat_1", op_id="op_2")],
    )
    assert (
        invoke_cli(
            "snapshot",
            "create",
            "--name",
            "Next",
            "--version",
            "v0.2.0",
            str(temp_workspace),
        ).exit_code
        == 0
    )
    result = invoke_cli(
        "diff",
        "--from",
        "v0.1.0",
        "--to",
        "v0.2.0",
        "--json",
        str(temp_workspace),
    )
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "diff"
    assert envelope["status"] == "success"
    assert isinstance(envelope["data"].get("operations"), list)


@pytest.mark.integration
def test_runtime_info_json_contract_shape() -> None:
    contract = _read_contract_fixture("runtime_info.success.json")
    result = invoke_cli("runtime-info", "--json")
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert set(contract.keys()).issubset(set(envelope.keys()))
    assert envelope["schemaVersion"] == "1"
    assert envelope["command"] == "runtime-info"
    assert envelope["status"] == "success"
    assert "cliVersion" in envelope["data"]
    assert "envelopeSchemaVersion" in envelope["data"]
    assert isinstance(envelope["data"].get("supportedCommands"), list)
