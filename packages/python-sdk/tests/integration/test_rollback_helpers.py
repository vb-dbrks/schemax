"""Integration tests for rollback command helper functions.

Covers pure-logic helpers that don't require live Databricks connectivity:
validation, signature building, SQL preview formatting, state collection,
cascade target resolution, and import baseline guards.
"""

import pytest

from schemax.commands.rollback import (
    RollbackError,
    _collect_catalogs_from_state,
    _enforce_import_baseline_guard,
    _format_sql_preview,
    _iter_schema_refs,
    _operation_signature,
    _resolve_cascade_target_name,
    _validate_partial_cli_inputs,
)
from schemax.providers.base.operations import Operation
from schemax.providers.unity.models import UnityState


def _make_op(op_type: str, target: str, payload: dict | None = None) -> Operation:
    return Operation(
        id="test_op",
        ts="2026-01-01T00:00:00Z",
        provider="unity",
        op=op_type,
        target=target,
        payload=payload or {},
    )


@pytest.mark.integration
class TestValidatePartialCliInputs:
    def test_missing_deployment_id(self) -> None:
        with pytest.raises(RollbackError, match="--deployment required"):
            _validate_partial_cli_inputs("", "dev", "prof", "wh_1")

    def test_missing_profile(self) -> None:
        with pytest.raises(RollbackError, match="--profile and --warehouse-id"):
            _validate_partial_cli_inputs("deploy_1", "dev", "", "wh_1")

    def test_missing_warehouse_id(self) -> None:
        with pytest.raises(RollbackError, match="--profile and --warehouse-id"):
            _validate_partial_cli_inputs("deploy_1", "dev", "prof", "")

    def test_missing_target(self) -> None:
        with pytest.raises(RollbackError, match="--target required"):
            _validate_partial_cli_inputs("deploy_1", "", "prof", "wh_1")

    def test_valid_inputs_pass(self) -> None:
        _validate_partial_cli_inputs("deploy_1", "dev", "prof", "wh_1")


@pytest.mark.integration
class TestOperationSignature:
    def test_same_ops_produce_same_signature(self) -> None:
        sig_a = _operation_signature("unity.add_table", "t1", {"name": "orders"})
        sig_b = _operation_signature("unity.add_table", "t1", {"name": "orders"})
        assert sig_a == sig_b

    def test_different_payloads_differ(self) -> None:
        sig_a = _operation_signature("unity.add_table", "t1", {"name": "orders"})
        sig_b = _operation_signature("unity.add_table", "t1", {"name": "items"})
        assert sig_a != sig_b

    def test_none_payload_normalised(self) -> None:
        sig_a = _operation_signature("unity.add_table", "t1", None)
        sig_b = _operation_signature("unity.add_table", "t1", {})
        assert sig_a == sig_b

    def test_key_order_irrelevant(self) -> None:
        sig_a = _operation_signature("unity.add_table", "t1", {"a": 1, "b": 2})
        sig_b = _operation_signature("unity.add_table", "t1", {"b": 2, "a": 1})
        assert sig_a == sig_b


@pytest.mark.integration
class TestFormatSqlPreview:
    def test_short_sql_preserved(self) -> None:
        result = _format_sql_preview("CREATE TABLE t1 (id INT)")
        assert "CREATE TABLE" in result

    def test_long_sql_truncated(self) -> None:
        long_sql = "ALTER TABLE " + "x" * 200
        result = _format_sql_preview(long_sql)
        assert len(result) <= 84  # 80 chars + "..."

    def test_comment_lines_skipped(self) -> None:
        sql = "-- This is a comment\n-- Another comment\nCREATE TABLE t1 (id INT)"
        result = _format_sql_preview(sql)
        assert "CREATE TABLE" in result


@pytest.mark.integration
class TestCollectCatalogsFromState:
    def test_dict_state_with_catalogs(self) -> None:
        state = {"catalogs": [{"id": "c1", "name": "sales"}]}
        catalogs = _collect_catalogs_from_state(state)
        assert len(catalogs) == 1

    def test_dict_state_empty_catalogs(self) -> None:
        catalogs = _collect_catalogs_from_state({"catalogs": []})
        assert catalogs == []

    def test_dict_state_missing_catalogs(self) -> None:
        catalogs = _collect_catalogs_from_state({})
        assert catalogs == []

    def test_pydantic_model_state(self) -> None:
        state = UnityState(catalogs=[])
        catalogs = _collect_catalogs_from_state(state)
        assert catalogs == []

    def test_non_list_catalogs_returns_empty(self) -> None:
        catalogs = _collect_catalogs_from_state({"catalogs": "invalid"})
        assert catalogs == []


@pytest.mark.integration
class TestIterSchemaRefs:
    def test_extracts_schema_refs_from_dicts(self) -> None:
        catalogs = [
            {
                "id": "c1",
                "name": "sales",
                "schemas": [{"id": "s1", "name": "raw"}, {"id": "s2", "name": "curated"}],
            }
        ]
        refs = _iter_schema_refs(catalogs)
        assert len(refs) == 2
        assert ("sales", "raw", "s1") in refs
        assert ("sales", "curated", "s2") in refs

    def test_empty_catalogs(self) -> None:
        assert _iter_schema_refs([]) == []

    def test_catalog_without_schemas(self) -> None:
        catalogs = [{"id": "c1", "name": "sales"}]
        assert _iter_schema_refs(catalogs) == []


@pytest.mark.integration
class TestResolveCascadeTargetName:
    def test_drop_catalog_found(self) -> None:
        operation = _make_op("unity.drop_catalog", "c1")
        catalogs = [{"id": "c1", "name": "sales", "schemas": []}]
        name, impact = _resolve_cascade_target_name(operation, catalogs)
        assert name == "sales"
        assert "ALL schemas" in impact

    def test_drop_catalog_not_found(self) -> None:
        operation = _make_op("unity.drop_catalog", "c_missing")
        name, impact = _resolve_cascade_target_name(operation, [])
        assert name == "c_missing"
        assert "ALL schemas" in impact

    def test_drop_schema_found(self) -> None:
        operation = _make_op("unity.drop_schema", "s1")
        catalogs = [{"id": "c1", "name": "sales", "schemas": [{"id": "s1", "name": "raw"}]}]
        name, impact = _resolve_cascade_target_name(operation, catalogs)
        assert name == "sales.raw"
        assert "ALL tables" in impact

    def test_drop_schema_not_found(self) -> None:
        operation = _make_op("unity.drop_schema", "s_missing")
        name, impact = _resolve_cascade_target_name(operation, [])
        assert name == "s_missing"
        assert "ALL tables" in impact

    def test_non_cascade_op_returns_target(self) -> None:
        operation = _make_op("unity.drop_table", "t1")
        name, impact = _resolve_cascade_target_name(operation, [])
        assert name == "t1"
        assert impact == ""


@pytest.mark.integration
class TestEnforceImportBaselineGuard:
    def test_no_baseline_allows_rollback(self) -> None:
        _enforce_import_baseline_guard({}, "v0.1.0", force=False, no_interaction=True)

    def test_rollback_after_baseline_allowed(self) -> None:
        env_config = {"importBaselineSnapshot": "v0.1.0"}
        _enforce_import_baseline_guard(env_config, "v0.2.0", force=False, no_interaction=True)

    def test_rollback_before_baseline_blocked(self) -> None:
        env_config = {"importBaselineSnapshot": "v0.3.0"}
        with pytest.raises(RollbackError, match="before the import baseline"):
            _enforce_import_baseline_guard(env_config, "v0.1.0", force=False, no_interaction=True)

    def test_rollback_before_baseline_forced(self) -> None:
        env_config = {"importBaselineSnapshot": "v0.3.0"}
        _enforce_import_baseline_guard(env_config, "v0.1.0", force=True, no_interaction=True)

    def test_invalid_version_strings_skip_guard(self) -> None:
        env_config = {"importBaselineSnapshot": "not-a-version"}
        _enforce_import_baseline_guard(env_config, "also-invalid", force=False, no_interaction=True)
