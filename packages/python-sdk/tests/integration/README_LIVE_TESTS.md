# Live Integration Tests – Reorganization & Hardening

## Current state

- **Single file:** `test_live_command_matrix.py` (~1,590 lines) with 9 test functions (one skipped).
- **Shared helpers:** In-file (`_write_project_env_overrides`, `_assert_schema_exists`, `_table_exists`, `_volume_exists`, `_function_exists`, `_materialized_view_exists`) plus `tests/utils/live_databricks.py` (config, executor, cleanup, fixture loading).
- **Fixtures:** 3 SQL files under `tests/resources/sql/`:
  - `unity_command_fixture.sql` – catalog, schemas, tables (customers, orders), view, tags, constraints.
  - `unity_uc_objects_fixture.sql` – catalog, schema, table, volume, function, materialized view.
  - `unity_import_fixture.sql` – (used by import tests elsewhere).

### Test → UC object / flow mapping

| Test | Primary UC objects / flow | Type |
|------|---------------------------|------|
| `test_live_command_matrix` | Catalog, schema, table, view, tags, constraints; full CLI matrix (init, import, snapshot, validate, sql, diff, apply dry-run, rollback dry-run, snapshot validate, bundle) | E2E matrix |
| `test_live_snapshot_rebase_explicit_skip` | (skipped – rebase) | Placeholder |
| `test_live_apply_and_rollback_non_dry_run` | Catalog, schema, **table** (apply + rollback real) | Table E2E |
| `test_live_e2e_create_apply_rollback_with_grants` | Catalog, schema, **table**, **grants** (apply + rollback) | Table + grants E2E |
| `test_live_apply_governance_only` | **managedCategories** scope, table, **set_table_comment** (governance-only apply) | Scope / governance E2E |
| `test_live_apply_existing_catalog_skips_create` | **existingObjects.catalog** scope, catalog, schema, table (apply skips CREATE CATALOG) | Scope / brownfield E2E |
| `test_live_greenfield_promote_dev_test_prod_then_rollback_prod` | **Multi-env** (dev/test/prod), catalog, schema, tables, promote + rollback prod only | Multi-env E2E |
| `test_live_import_sees_volume_function_materialized_view` | **Volume, function, materialized view** (import discovery only) | UC objects – import |
| `test_live_e2e_apply_volume_function_materialized_view` | **Table, volume, function, materialized view** (apply + rollback) | UC objects – apply/rollback |

---

## Proposed structure: split by object/group + E2E

### 1. Per–object/group files (one file per UC “family”)

Each file owns **import/discovery** and **apply/rollback** for that object type (or a small, coherent group). Shared helpers move to a common module so tests stay short and consistent.

| File | Scope | Suggested tests |
|------|--------|------------------|
| `test_live_catalog_schema.py` | Catalog, schema (create, managed location, skip when existing) | Import from fixture with catalog/schemas; apply with add_catalog/add_schema; apply with existingObjects (skips CREATE CATALOG); rollback drops schema/catalog. |
| `test_live_table.py` | Tables, columns, comments, TBLPROPERTIES | Import fixture with tables; apply table+columns; rollback table; optional: constraints/tags if covered by fixture. |
| `test_live_volume_function_mv.py` | Volume, function, materialized view | Import sees volume/function/MV (current import test); E2E apply table+volume+function+MV then rollback (current UC objects E2E). Optional: split into `test_live_volume.py`, `test_live_function.py`, `test_live_materialized_view.py` if you want one object per file. |
| `test_live_grants.py` | Grants on table/schema/catalog | Apply table → add grant → apply → rollback (current grants E2E). |
| `test_live_view.py` | Views (non-materialized) | Import from command fixture (v_orders); optional: E2E add view → apply → rollback if/when view ops are first-class. |
| `test_live_scope_environment.py` | managedCategories, existingObjects, multi-env | Governance-only apply; existing catalog skips create; promote dev→test→prod + rollback prod (current promote test). |

Rationale:

- **Catalog/schema** and **table** are the backbone; separate files keep “infra” vs “data objects” clear.
- **Volume, function, MV** are already grouped in one fixture and two tests; one file keeps dependency (MV on table) in one place. Split later if the file grows.
- **Grants** and **scope/environment** are cross-cutting; dedicated files avoid mixing with “single object” tests.

### 2. E2E / sequence files (dependencies and ordering)

| File | Purpose | Suggested tests |
|------|--------|------------------|
| `test_live_e2e_command_matrix.py` | Full CLI sweep (no or minimal object-specific assertions) | One test: seed from SQL → init → import → snapshot → validate → sql → diff → apply dry-run → rollback dry-run → snapshot validate → bundle. Same as current `test_live_command_matrix` but only “commands run successfully”; object presence can be covered in per-object files. |
| `test_live_e2e_sequences.py` | Ordering and dependencies | e.g. Create table → add MV depending on table → apply (correct order); create table → add grant → apply; multi-snapshot promote then rollback one env. |

This keeps:

- **Command matrix:** one place that asserts “every command exits 0” and basic flow.
- **Sequences:** explicit tests for dependency order (e.g. table before MV, catalog before schema) and multi-step flows.

### 3. Placeholder / future

- `test_live_snapshot_rebase.py` – when rebase is runnable in CI: rebase-specific setup and one or two tests (can stay skipped in matrix until then).

---

## Shared layer (water-tight base)

### 3.1 Conftest / shared helpers

- **`tests/integration/conftest_live.py`** (or `tests/utils/live_helpers.py`):
  - `require_live_command_tests()` – keep in `live_databricks.py`.
  - `make_workspace(tmp_path, suffix)` – create workspace dir, return Path.
  - `make_suffix(config)` – e.g. `make_namespaced_id(config).split("_", 2)[-1]` in one place.
  - Project wiring: `write_project_env_overrides(workspace, top_level_name, catalog_mappings)`, `write_project_managed_scope(...)`, `write_project_promote_envs(...)` (current `_write_*` inlined or moved here).
  - Discovery assertions: `assert_schema_exists`, `table_exists`, `volume_exists`, `function_exists`, `materialized_view_exists` (current `_assert_schema_exists`, `_table_exists`, etc.) with consistent signatures and docstrings.

- **Preseed pattern:** One helper that, given `(executor, config, workspace, logical_catalog, physical_catalog, tracking_catalog, schema_name, managed_root_subpath)`:
  - Runs the standard three statements (CREATE CATALOG tracking, CREATE CATALOG physical, CREATE SCHEMA).
  - Optionally clears changelog so import adopt-baseline is deterministic.
  - Returns something like `(physical_catalog, tracking_catalog)` for use in cleanup.

- **Cleanup:** Always in `finally`: call `cleanup_objects(executor, config, list_of_catalogs)` and `shutil.rmtree(workspace, ignore_errors=True)`. Use a single list of catalogs (e.g. `[physical_catalog, tracking_catalog]`) built at the start of the test so nothing is missed.

### 3.2 Fixtures and tokens

- Keep one SQL fixture per “theme” (command matrix, UC objects). Document tokens and required replacements in `tests/resources/sql/README.md`.
- For UC objects, keep a single fixture that creates table + volume + function + MV so import and apply tests share the same contract. Use tokens for catalog and `__MANAGED_ROOT__` so each run is isolated.

### 3.3 Naming and isolation

- **Catalogs:** Always use a unique suffix per test (and per run if you use `make_namespaced_id`). Pattern: `{resource_prefix}_{test_short_name}_{suffix}` (e.g. `schemax_live_apply_uc_xyz`).
- **Workspace dirs:** `tmp_path / f"workspace_{test_short_name}_{suffix}"` so parallel runs and re-runs don’t clash.
- **Managed locations:** Subpaths under a single root (e.g. `{managed_location}/schemax-command-live/{test_short_name}/{suffix}`) so cleanup is predictable.

### 3.4 Assertions (water-tight)

- **After apply:** For every created object type, assert presence via discovery (table_exists, volume_exists, etc.) instead of only checking CLI exit code.
- **After rollback:** For every dropped object, assert absence via the same discovery helpers; optionally assert that parent schema (or catalog) still exists where expected.
- **SQL content (when relevant):** e.g. governance-only test: assert "CREATE CATALOG" not in sql_output and "COMMENT" in sql_output; existing-catalog test: assert "CREATE CATALOG" not in sql_output. This guards filtering logic.
- **Exit codes:** Assert `exit_code == 0` for every CLI call that is expected to succeed; capture and assert on `result.output` in failure messages so logs are useful.

### 3.5 Ordering and dependencies

- **Rollback order:** State differ already produces a safe drop order (e.g. MV → function → volume → table). Rely on that; add one test that applies “table + volume + function + MV” and then rollback, and assert all four are gone and schema remains. That’s already covered by `test_live_e2e_apply_volume_function_materialized_view`; after split it can live in `test_live_volume_function_mv.py`.
- **Apply order:** Generator produces catalog → schema → table → then volume/function/MV. One E2E that creates all in one snapshot and applies once is enough to lock in ordering; no need to duplicate in every file.

### 3.6 Skipped test

- Keep `test_live_snapshot_rebase_explicit_skip` in a small file (e.g. `test_live_snapshot_rebase.py`) with a clear skip reason, so the “rebase” slot is documented and can be replaced by a real test later.

---

## File list (implemented)

```
tests/integration/
  live_helpers.py                # shared: project writers, discovery helpers, preseed
  test_e2e_ui_to_live.py         # E2E: UI-equivalent state → CLI → SDK → live (table + volume + function)
  test_live_catalog_schema.py    # catalog, schema, existingObjects
  test_live_table.py             # table, columns, apply/rollback
  test_live_volume.py            # volume (import + E2E apply/rollback)
  test_live_function.py          # function (import + E2E apply/rollback)
  test_live_materialized_view.py # materialized view (import + E2E apply/rollback + combined UC objects E2E)
  test_live_grants.py            # grants on table
  test_live_scope_environment.py # managedCategories, promote dev/test/prod
  test_live_e2e_command_matrix.py # full CLI sweep
  test_live_snapshot_rebase.py   # rebase placeholder (skip)
```

The former single file `test_live_command_matrix.py` has been removed; all tests live in the files above.

---

## Migration order (minimal risk)

1. **Add shared module** – Create `conftest_live.py` or `live_helpers.py` with project writers and discovery helpers; have one existing test use it and pass.
2. **Extract by object** – Move “table only” tests into `test_live_table.py`, “volume/function/MV” into `test_live_volume_function_mv.py`, etc. Use the same shared helpers and cleanup pattern.
3. **Rename and slim matrix** – Rename current file to `test_live_e2e_command_matrix.py` and trim it to “commands succeed” only, or leave one “full” test that still seeds and runs the whole flow.
4. **Add scope/environment file** – Move governance-only, existing-catalog, and promote tests into `test_live_scope_environment.py`.
5. **Add E2E sequences** – If you want explicit dependency tests, add `test_live_e2e_sequences.py` with one or two tests (e.g. table+MV apply then rollback; multi-env promote then rollback one).
6. **Update README** – Update `tests/README.md` (and this file) with the new file map and how to run per-file or per-object live tests.

---

## E2E pipeline: UI → CLI → SDK → live environment

The live tests validate the **full E2E pipeline**:

1. **UI (Designer)** produces `.schemax/` state (project.json, changelog.json with ops).
2. **CLI** runs `schemax apply`, `schemax rollback`, etc.
3. **SDK** (storage, sql_generator, executor) reads state and executes SQL.
4. **Live env** is a real Databricks workspace; tests assert objects exist after apply and are gone after rollback.

We do not automate the VS Code webview. Tests build the **same state the UI would produce** (same op shapes via `OperationBuilder`, `append_ops`) and run the CLI against a live workspace. So **UI → CLI → SDK → live** is tested end-to-end.

- **Dedicated E2E:** `test_e2e_ui_to_live.py` — one test that runs UI-equivalent changelog (table + volume + function) → apply → assert in live → rollback → assert gone.
- **Per-object / combined:** `test_live_function.py`, `test_live_volume.py`, `test_live_table.py`, `test_live_materialized_view.py`, etc. — same pipeline per object type or combined.

## Running live tests

- **All live:**  
  `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 DATABRICKS_*=... uv run pytest tests/integration -m integration -v`
- **By object group:**  
  `... pytest tests/integration/test_live_table.py -v`  
  `... pytest tests/integration/test_live_volume_function_mv.py -v`
- **E2E only:**  
  `... pytest tests/integration/test_live_e2e_command_matrix.py tests/integration/test_live_e2e_sequences.py -v`

This layout keeps live tests split by UC object/group and by E2E/sequence, with a single shared layer and clear patterns for isolation and assertions so they stay water-tight as you add more object types or environments.
