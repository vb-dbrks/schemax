# Python SDK Tests

## Scope
This folder contains deterministic unit/integration tests and opt-in live Databricks tests.

## Live Test Env
Required for live command matrix tests:

- `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION`

You can set these in your environment (e.g. `export DATABRICKS_PROFILE=DEFAULT` or a `.env` file); **if they are set, no prompts are shown**. When you run integration tests with a TTY and any are unset, the test helper will **prompt you interactively** for each missing value. In CI or non-interactive runs, tests are skipped as before.

Optional:

- `SCHEMAX_LIVE_RESOURCE_PREFIX` (default: `schemax_live`)
- `SCHEMAX_LIVE_TEST_TIMEOUT_SECONDS` (default: `300`)

## Live tests (tests/integration/)

Live tests are split by UC object/scope and E2E. Shared helpers: `tests/integration/live_helpers.py`.

| File | Tests |
|------|--------|
| test_live_e2e_command_matrix.py | test_live_command_matrix (full CLI sweep). |
| test_live_catalog_schema.py | test_live_apply_existing_catalog_skips_create. |
| test_live_table.py | test_live_apply_and_rollback_non_dry_run. |
| test_live_volume.py | test_live_import_sees_volume, test_live_e2e_apply_volume. |
| test_live_function.py | test_live_import_sees_function, test_live_e2e_apply_function. |
| test_live_materialized_view.py | test_live_import_sees_materialized_view, test_live_e2e_apply_materialized_view, test_live_e2e_apply_volume_function_materialized_view. |
| test_live_grants.py | test_live_e2e_create_apply_rollback_with_grants. |
| test_live_scope_environment.py | test_live_apply_governance_only, test_live_greenfield_promote_dev_test_prod_then_rollback_prod. |
| test_live_snapshot_rebase.py | test_live_snapshot_rebase_explicit_skip (skipped). |

Run by file (e.g. table or volume/function/MV only):

```bash
uv run pytest tests/integration/test_live_table.py -v
uv run pytest tests/integration/test_live_volume.py tests/integration/test_live_function.py tests/integration/test_live_materialized_view.py -v
```

Run all live tests (with env set):

```bash
uv run pytest tests/integration -m integration -v
```

## Issue #19 Coverage Gap Ledger
The following tests are intentionally skipped until issue #19 feature gaps are implemented:

- `tests/unit/test_sql_generator.py::TestRowFilterSQL::test_add_row_filter`
- `tests/unit/test_sql_generator.py::TestRowFilterSQL::test_update_row_filter`
- `tests/unit/test_sql_generator.py::TestRowFilterSQL::test_remove_row_filter`
- `tests/unit/test_sql_generator.py::TestColumnMaskSQL::test_add_column_mask`
- `tests/unit/test_sql_generator.py::TestColumnMaskSQL::test_update_column_mask`
- `tests/unit/test_sql_generator.py::TestColumnMaskSQL::test_remove_column_mask`
- `tests/unit/test_sql_generator.py::TestSQLGeneration::test_generate_sql_batch`
- `tests/integration/test_workflows.py::TestBasicWorkflow::test_init_to_sql_workflow`
- `tests/integration/test_workflows.py::TestCompleteSchemaWorkflow::test_create_complete_schema`
- `tests/unit/test_provider.py::TestProviderIntegration::test_full_workflow_with_provider`

Reason:
- Missing SQL generation and end-to-end support for issue #19 areas.

Unblock criteria:
- Implement issue #19 features and remove skip markers with passing assertions.

Owner:
- SchemaX SDK maintainers.
