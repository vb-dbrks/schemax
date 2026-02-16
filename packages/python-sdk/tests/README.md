# Python SDK Tests

## Scope
This folder contains deterministic unit/integration tests and opt-in live Databricks tests.

## Live Test Env
Required for live command matrix tests:

- `SCHEMATIC_RUN_LIVE_COMMAND_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION`

Optional:

- `SCHEMATIC_LIVE_RESOURCE_PREFIX` (default: `schematic_live`)
- `SCHEMATIC_LIVE_TEST_TIMEOUT_SECONDS` (default: `300`)

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
- Schematic SDK maintainers.
