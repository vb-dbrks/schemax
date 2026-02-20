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
- **test_live_greenfield_promote_dev_test_prod_then_rollback_prod** – Greenfield project: create catalog/schema/table (no import), snapshot v0.1.0, apply to dev → test → prod; add second table, snapshot v0.2.0, apply to all three; then rollback **prod only** to v0.1.0; asserts prod loses the new table while dev/test keep it.
- **test_live_apply_governance_only** – Managed scope: set `managedCategories: ["governance"]`; add table + columns, apply (v0.2.0); add set_table_comment, snapshot v0.3.0, apply; asserts SQL has no CREATE CATALOG/SCHEMA/TABLE and has COMMENT; apply runs successfully (governance-only DDL only).
- **test_live_apply_existing_catalog_skips_create** – Managed scope: preseed catalog + schema; set `existingObjects.catalog: [logical]`; add ops for catalog + schema + table + column; snapshot v0.1.0, apply; asserts SQL has no CREATE CATALOG and apply succeeds (catalog “already exists”, only CREATE SCHEMA/TABLE/ADD COLUMN).
- **test_live_import_sees_volume_function_materialized_view** – Seed catalog/schema/table/volume/function/MV via `unity_uc_objects_fixture.sql`; init → import; asserts state contains volume, function, materialized view; validate and sql.
- **test_live_e2e_apply_volume_function_materialized_view** – Preseed catalog+schema, import adopt-baseline; add table + volume + function + materialized view; apply; asserts all exist (live discovery); rollback to baseline; asserts all removed.

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
