## SQL Fixtures

### `unity_import_fixture.sql`

Unity Catalog fixture DDL/DML used to seed realistic provider metadata for
import testing.

It creates:

- 2 catalogs
- multiple schemas
- tables with PK/FK constraints
- table/view comments and properties
- table and column tags
- dependent views

Use this when validating `schematic import` behavior end-to-end.

### Optional live integration test

There is an opt-in pytest integration test that applies this fixture SQL and runs
`schematic import` against a real Databricks workspace.

Required environment variables:

- `SCHEMATIC_RUN_LIVE_IMPORT_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION` (example: `abfss://test@mneudlsdevashish001.dfs.core.windows.net/`)

Run:

```bash
cd packages/python-sdk
uv run pytest tests/integration/test_import_live_databricks.py -v
```

The live test creates uniquely-suffixed catalogs per run (similar to `make_random`
style fixtures), appends a random subdirectory under `DATABRICKS_MANAGED_LOCATION`,
and deletes fixture catalogs in teardown.
