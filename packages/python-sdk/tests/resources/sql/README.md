## SQL Fixtures

### `unity_import_fixture.sql`

Unity Catalog fixture DDL/DML used to seed realistic provider metadata for import testing.

It creates:

- 2 catalogs
- multiple schemas
- tables with PK/FK constraints
- table/view comments and properties
- table and column tags
- dependent views

Use this when validating `schematic import` behavior end-to-end.

### `unity_command_fixture.sql`

Unity Catalog fixture used by live command-matrix tests.

It creates:

- catalog + schemas
- tables with PK/FK constraints
- table properties
- table and column tags
- a dependent view with tags

Use this when validating multi-command live workflows (`import`, `sql`, `validate`, `diff`, `apply --dry-run`, `rollback --dry-run`, snapshots).

### Optional live integration tests

There are opt-in pytest integration tests that apply these fixture SQL files and run live commands against Databricks.

Required environment variables:

- `SCHEMATIC_RUN_LIVE_COMMAND_TESTS=1`
- `DATABRICKS_PROFILE`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_MANAGED_LOCATION` (example: `abfss://test@mneudlsdevashish001.dfs.core.windows.net/`)

Run:

```bash
cd packages/python-sdk
uv run pytest tests/integration/test_live_command_matrix.py -v
uv run pytest tests/integration/test_import_live_databricks.py -v
```

Live tests create uniquely-suffixed catalogs per run, append a random subdirectory under `DATABRICKS_MANAGED_LOCATION`, and clean up fixture catalogs in teardown.
