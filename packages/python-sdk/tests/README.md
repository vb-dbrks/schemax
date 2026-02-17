# Python SDK Tests

## Scope
This folder contains deterministic unit/integration tests and opt-in live Databricks tests.

## Live Test Env
Required for live command matrix tests:

- `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION`

Optional:

- `SCHEMAX_LIVE_RESOURCE_PREFIX` (default: `schemax_live`)
- `SCHEMAX_LIVE_TEST_TIMEOUT_SECONDS` (default: `300`)

## Live tests (tests/integration/test_live_command_matrix.py)

- **test_live_command_matrix** – Seed from SQL fixture, init, import, snapshot, validate, sql, diff, apply (dry-run), rollback (dry-run), snapshot validate, bundle.
- **test_live_apply_and_rollback_non_dry_run** – Create project, import adopt-baseline, add table/columns, apply (real), rollback to baseline; asserts table exists then is removed.
- **test_live_e2e_create_apply_rollback_with_grants** – Same flow as above plus: add grant on table, snapshot v0.3.0, apply (runs GRANT live), then rollback to baseline; covers create → apply → grants → rollback against real Databricks.

Run all live tests (with env set):

```bash
uv run pytest tests/integration/test_live_command_matrix.py -v
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
