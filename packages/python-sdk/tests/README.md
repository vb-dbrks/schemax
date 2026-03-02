# Python SDK Tests

## Scope
This folder contains deterministic unit/integration tests and opt-in live Databricks tests.

## Parallel execution (pytest-xdist)

With dev dependencies installed (`pip install -e ".[dev]"` or `uv pip install -e ".[dev]"`), use **pytest-xdist** for parallel runs:

```bash
pytest tests/ -n auto
```

For serial runs (e.g. live integration tests that share a workspace), omit xdist or use:

```bash
pytest tests/ -n 0
```

## Fixtures and sample data

**In-memory (unit/integration, no DB):**

- **`tests/utils/fixture_data.py`** – Rich Unity Catalog sample data: 22 column types (BIGINT, STRING, DECIMAL, ARRAY, MAP, STRUCT, GEOGRAPHY, GEOMETRY, VARIANT, OBJECT, etc.), Delta and Iceberg tables, partitioned/clustered table, view, SQL and Python functions, managed and external volumes, materialized views with PARTITIONED BY/CLUSTER BY and refresh schedule. Used by `sample_operations` and `sample_unity_state` in `conftest.py`. Regenerate **`tests/resources/projects/unity_full`** with `uv run python tests/scripts/generate_unity_full_project.py` after changing the rich ops.
- **`tests/utils/operation_builders.py`** – `OperationBuilder` for building ops (catalog, schema, table, column, view, volume, function, materialized view) with optional args (e.g. `partition_columns`, `cluster_columns`, `external`, `external_location_name`). Use for tests that need specific op shapes without hand-writing payloads.

**SQL seed files (live tests):**

- **`tests/resources/projects/unity_full/`** – Resource project with a single snapshot built from the rich ops (all data types, tables, views, functions, volumes, MVs). Use `resource_workspace("unity_full")` or the `unity_full_workspace` fixture for tests that need a full catalog without building ops in conftest.
- **`tests/resources/sql/`** – See `tests/resources/sql/README.md`. `unity_uc_objects_fixture.sql` seeds catalog, schema, base table, partitioned table, view, volume, SQL/Python functions, and MVs for import and apply/rollback E2E.

## CLI integration test coverage

**Deterministic (no Databricks):**

- **Unit:** `tests/unit/test_cli_commands.py` – init, sql, validate, diff, apply (stubbed), rollback (partial/complete), snapshot create/validate/rebase; argument routing and error messages.
- **Unit:** `tests/unit/test_import_command.py` – import from SQL file and live (stubbed), catalog mapping, validation.
- **Unit:** `tests/unit/test_rollback_command.py` – rollback command and idempotency.
- **Integration:** `tests/integration/test_command_workflows.py` – end-to-end local workflow (validate → sql → snapshot create → diff), apply + rollback CLI sequence with stubbed execution.
- **Integration:** `tests/integration/test_workflows_e2e.py` – e2e init/ops/snapshot/validate/sql/diff, apply dry-run via CLI, apply + rollback (stubbed), snapshot validate/rebase via CLI.
- **Integration:** `tests/integration/test_workflows.py` – init → sql, complete schema workflow (some tests skipped per Issue #19).
- **CI:** `.github/workflows/integration-tests.yml` runs `schemax validate` and `schemax sql` in `examples/basic-schema` on every push/PR.

**Live (opt-in, requires Databricks):**

- Set `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1` and profile/warehouse/managed-location (see below). Live tests cover: full CLI command matrix, catalog/schema/table/volume/function/materialized view/grants/scope/multi-env promote and rollback, import from live, E2E UI-equivalent → CLI → live apply/rollback.

**Gaps / improvements:**

- No dedicated “CLI smoke” test that runs every command with `--help` on all entrypoints.
- Live tests are manual/CI-optional; consider a scheduled run or a smaller “smoke” live job if needed.

## Cross-platform (Windows, Linux, macOS)

The CLI and SDK use **pathlib.Path** and **Click’s path types** (`click.Path(..., path_type=Path)` where used) so that paths are correct on Windows, Linux, and macOS. Avoid hardcoded `/` or `\\`; use `path / "subdir"` or `Path(...)`.

**How we ensure it:**

1. **CI:** Python SDK CI runs the same test suite on **Ubuntu, Windows, and macOS** (see `.github/workflows/python-sdk-ci.yml`). Any path or platform-specific bug in the CLI or storage layer should be caught there.
2. **Local:** On Windows or macOS, run the same commands:
   ```bash
   pytest tests/unit/test_cli_commands.py tests/unit/test_import_command.py tests/unit/test_rollback_command.py tests/integration/test_command_workflows.py -v
   ```
3. **Manual smoke:** From a project root, run: `schemax validate`, `schemax sql --output out.sql`, `schemax init --provider unity`, and (with env set) `schemax apply --target dev --dry-run` to confirm behavior on your OS.

## Live Test Env
Required for live command matrix tests:

- `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION`

You can set these in your environment (e.g. `export DATABRICKS_PROFILE=DEFAULT` or a `.env` file); **if they are set, no prompts are shown**. When you run integration tests with a TTY and any are unset, the test helper will **prompt you interactively** for each missing value. In CI or non-interactive runs, tests are skipped as before.

**Using a specific workspace/warehouse:** Set the env vars in the **same process** that runs pytest (e.g. `export DATABRICKS_PROFILE=ashw` and `export DATABRICKS_WAREHOUSE_ID=68038909ab980e46`, or prefix the command: `DATABRICKS_PROFILE=ashw DATABRICKS_WAREHOUSE_ID=... pytest tests/integration -m integration -v`). The first live test that runs will print one line to stderr: `[schemax live] Using profile='...' warehouse_id='...'` so you can confirm the correct workspace and warehouse are in use.

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
| test_live_rollback.py | test_live_failed_apply_then_partial_rollback (failed apply → partial rollback, no mocks). |
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
