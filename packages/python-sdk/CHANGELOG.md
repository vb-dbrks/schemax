# Changelog

## [0.2.11] - 2026-03-11

### Added

- **v5 multi-target project schema** — Projects now use a `targets` dict instead of a single `provider` object, enabling multiple provider instances (e.g., multiple Unity Catalog catalogs or mixed providers) in one project. Existing v4 projects auto-migrate to v5 on first load.
- **`--scope` CLI parameter** — New `--scope` flag on `sql`, `validate`, `apply`, and `workspace-state` commands. Currently plumbed through CLI and interfaces; target-scoped state loading will be enabled in a follow-up release.
- **`Operation.scope` field** — Operations now carry a `scope` field to associate them with a specific target in multi-target projects.
- **Provider-specific settings UI** — Project Settings panel renders target-specific configuration per provider type, with tabs for each target in multi-target projects.

### Changed

- **Renamed `target_name` to `scope`** — All internal parameters, CLI flags, and Operation fields renamed from `target_name` to `scope` to avoid confusion with `target` (environment). Breaking change for any code using `--target-name` CLI flag or `Operation.target_name` field.
- **New projects created as v5** — `ensure_project_file()` and extension "New Project" flow now emit v5 project schema directly.

### Fixed

- **Apply command `target_name` kwarg error** — Fixed `TypeError` where `_build_apply_request()` passed `target_name` to `apply_to_environment()` which didn't accept it, causing `UNEXPECTED_ERROR` instead of proper error codes.
- **Integration tests updated for v5** — All test files updated to use v5 project schema (`project["targets"][scope]` instead of `project["provider"]`).

## [0.2.10] - 2026-03-10

### Added

- **View tags** — Full support for `ALTER VIEW SET TAGS` and `ALTER VIEW UNSET TAGS` (Databricks DBR 13.3+). Add, update, and remove tags on views through the Designer or state files. Tags are tracked in the changelog, included in SQL generation, and applied during deployment.
- **Bulk view tag operations** — Apply the same tag to all views in a catalog or schema scope using the bulk operations panel.

### Changed

- **State differ refactored by responsibility** — Split the monolithic `state_differ.py` into focused modules: `operation_builders.py` (factory functions), `grant_differ.py` (grant comparison), `metadata_differ.py` (tags, properties, constraints), and `bulk_operations.py` (recursive traversal). The public `UnityStateDiffer` API is unchanged.

### Fixed

- **Live integration test performance** — Existence checks in test helpers (`table_exists`, `volume_exists`, `function_exists`, `materialized_view_exists`, `assert_schema_exists`) now use single `information_schema` queries instead of full catalog discovery. Tests run approximately 7x faster per check on large metastores.

## [0.2.9] - 2026-03-06

### Added

- **Databricks Asset Bundles integration** — New `schemax bundle` command generates DAB-compatible resource YAML and deploy script. Include the generated files in an existing DAB project to deploy SchemaX migrations as a serverless Python task. The job uses `${bundle.target}` as the SchemaX environment, so DAB target names map directly to your environments (dev/test/prod).
- **Documentation** — New [Databricks Asset Bundles guide](https://vb-dbrks.github.io/schemax/guide/databricks-asset-bundles) with step-by-step setup, CI/CD integration, and architecture overview.
- **Demo script** — End-to-end demo script (`examples/demo/run_demo.sh`) covering init, design, snapshot, SQL generation, apply, import, and rollback workflows.
- **Expanded integration test coverage** — Added ~56 new integration tests covering rollback helper functions, SQL generation for catalog/schema updates, column modifications, function operations, materialized view options, constraint variants (CHECK, FK, PK NOT ENFORCED/RELY), volume updates, revoke-all grants, table partitions/clusters/tags, and column tags.

### Changed

- **`--profile` now optional on `apply` and `rollback`** — Defaults to `None` for Databricks serverless runtime auth. No `~/.databrickscfg` needed in DAB jobs. `ExecutionConfig.profile` accepts `str | None`.
- **Deploy script runs CLI in-process** — Uses `schemax_cli(args, standalone_mode=False)` instead of `subprocess.run` to inherit Databricks serverless runtime authentication context.
- **Statement execution polls on PENDING** — `_poll_statement()` retries PENDING/RUNNING states up to 120s instead of failing immediately, handling SQL warehouse cold starts gracefully.
- **DAB resource YAML correctness** — Uses `environment_version` (not deprecated `client`), `spark_python_task.parameters` (not invalid `environment_variables`), relative `python_file` path, and job tags (`managed_by`, `schemax_project`, `schemax_version`) for discoverability.
- **CLI runtime-info** — `bundle` now reported in `supportedCommands` for extension compatibility checks.
- **`schemax bundle --json`** — Added JSON envelope output support required by the VS Code extension's `runJson` interface.
- **GitHub Actions workflow** — Updated `deploy-prod.yml` to use `schemax bundle` for DAB-based deployment instead of manual script invocation.

### Fixed

- **`SystemExit(0)` in Databricks serverless** — Deploy script catches successful exit codes so Databricks IPython runtime doesn't report success as failure.
- **Workspace resolution in DAB jobs** — Deploy script locates `.schemax/` project root by walking up from script location, fixing `resources/.schemax/changelog.json` not found errors.
- **`No module named schemax.__main__`** — Deploy script uses `schemax.cli` module (no `__main__.py` exists).

## [0.2.8] - 2026-02-27

### Changed

- **Version bump** — Bumped to 0.2.8 for coordinated release with extension (Open VSX publishing, workflow clarity). No SDK behavior changes.

## [0.2.7] - 2026-03-02

### Added

- **Legacy workspace hard-break guardrails** — Added deterministic detection and rejection for removed implicit single-catalog workspace markers.
- **Error code contract** — Added `LEGACY_SINGLE_CATALOG_UNSUPPORTED` failure path for CLI/JSON workflows.

### Changed

- **Unity default model** — New projects now initialize with explicit multi-catalog mode (no implicit bootstrap catalog operation).
- **Rollback execution config** — Removed implicit catalog fallback in rollback execution paths; execution now uses explicit environment deployment catalog.
- **Versioning architecture** — Centralized runtime version to `schemax.version.SCHEMAX_VERSION` and removed per-command hardcoded version literals.
- **Release automation** — Fixed version bump script replacement logic to avoid Perl backreference corruption on `0.x.y` versions.

### Fixed

- **Extension/SDK compatibility surfacing** — Legacy workspace failures now propagate cleanly as structured command errors.
- **Release tooling integrity** — Repaired and hardened version sync checks to validate the shared version source.

## [0.2.6] - 2026-03-02

### Added

- **Provider-contract architecture baseline** — Added explicit domain/provider contracts and registry surfaces to support multi-provider execution paths.
- **Hive MVP provider** — Added first non-Unity provider implementation scaffold (`hive`) for breadth validation.
- **CLI JSON envelope contracts** — Added/expanded machine-readable envelope fixtures and contract tests for extension-facing workflows.
- **Architecture fitness tests** — Added guardrails to prevent provider-runtime leakage across layers.

### Changed

- **Python-first semantics** — Consolidated schema semantics into the Python SDK and aligned extension workflows to backend envelope transport.
- **Storage/session layer** — Introduced repository/session abstractions for cleaner workspace mutation boundaries.
- **Unity internals refactor** — Decomposed Unity parser/differ/reducer/sql-generator paths for lint/type compliance and maintainability.
- **Cross-platform auth profile lookup** — Databricks profile detection now resolves config paths consistently across macOS/Linux/Windows.
- **Unity catalog model hard cutover** — New workspaces are explicit multi-catalog by default; implicit bootstrap catalog creation was removed.

### Fixed

- **CLI envelope correctness** — Fixed `apply` and `snapshot validate` JSON status/exit-code mismatches on failure/stale paths.
- **Snapshot rebase service contract** — `SnapshotService.rebase()` now propagates success/failure from the underlying rebase result.
- **Parser/test parity** — Updated DDL parser + tests for `ALTER ... SET TAGS` behavior and branch coverage.

### Breaking

- **Legacy implicit workspaces removed** — Workspaces carrying implicit single-catalog markers (`catalogMode: single`, `__implicit__`, or `cat_implicit` bootstrap ops) now fail fast with `LEGACY_SINGLE_CATALOG_UNSUPPORTED`. Migrate to explicit logical catalogs and environment mappings before running `sql/apply/rollback/import/workspace-state`.

## [0.2.5] - 2026-02-24

### Changed

- **Library refactoring and improvements** — Internal refactoring of validate, snapshot rebase, and test utilities (operation builders). No functional changes for users.

### Breaking

- **Test API (operation_builders)** — `builder.add_catalog(...)` is now `builder.catalog.add_catalog(...)`; same for all domains. Use `builder.<domain>.<method>(...)` in tests.

## [0.2.1] - 2026-02-21

### Added

- **Volumes, functions, and materialized views** — Full support in the CLI and SDK: design them in the Designer, generate SQL, and apply or import. Live import from Databricks and import from SQL files both support the new object types. View and MV definitions and dependencies are extracted when importing from SQL.
- **Materialized view dependency ordering** — Generated SQL creates base tables and views before dependent views and materialized views. Dependencies come from the UI or from parsing view/MV definitions.
- **First deployment** — When you run apply for the first time (or the deployment tracking table doesn’t exist yet), the CLI no longer errors. It treats “table not found” as “no previous deployment” and continues.

### Changed

- **Materialized view SQL** — Comments and DROP statements for materialized views follow Databricks syntax. DDL import extracts dependencies from view and MV definitions.
- **Live import** — Import from Databricks no longer fails when your workspace has materialized views. Discovery skips table-only metadata for MVs so it works correctly.

### Fixed

- **Live import with materialized views** — Import from Databricks works when catalogs or schemas contain materialized views.
- **Apply on a fresh environment** — Apply no longer fails when the deployment tracking table hasn’t been created yet.

## [0.2.0] - 2025-02-12

### Added

- **Import from SQL file**: `schemax import --from-sql PATH [--mode diff|replace] [--dry-run] [--target ENV]` parses a Unity Catalog DDL file and diffs against the current project state (or replaces as new baseline). No Databricks connection required. Statements are applied in file order (e.g. CREATE TABLE then ALTER TABLE ADD COLUMN then SET TBLPROPERTIES).
- **Core**: `schemax.core.sql_utils.split_sql_statements()` for splitting SQL scripts (preserves quoted semicolons; skips comment-only lines).
- **Provider contract**: `state_from_ddl(sql_path=..., sql_statements=..., dialect=...)` on the base provider; Unity provider implements full DDL parsing and state building.
- **Unity DDL parser**: Parses CREATE CATALOG/SCHEMA/TABLE/VIEW, COMMENT ON, and ALTER TABLE (ADD/DROP/RENAME column, ALTER COLUMN, RENAME TO, SET TBLPROPERTIES), ALTER CATALOG/SCHEMA/TABLE SET TAGS. Command-path fallback for CREATE CATALOG/SCHEMA (e.g. when MANAGED LOCATION is present) with comment extraction. State builder uses immutable updates (Pydantic `model_copy`).
- **Documentation**: CLI reference "Import from SQL file" section; workflows table row; statement-order note.

### Changed

- **Import command**: `schemax import` now supports two sources: live Databricks (requires `--target`, `--profile`, `--warehouse-id`) and SQL file (`--from-sql`). When using `--from-sql`, target/profile/warehouse are optional.

## [0.1.4] - 2025-02-19

### Fixed

- **SQL generator**: `CREATE CATALOG` / `CREATE SCHEMA` with tags now emits separate statements (one per API call). Previously, `CREATE ... ; ALTER ... SET TAGS` was concatenated into a single `StatementInfo`, causing `PARSE_SYNTAX_ERROR` on Databricks Statement Execution API. Added `_split_sql_statements` to catalog and schema statement loops in `generate_sql_with_mapping`.
- Regression tests added: `test_catalog_with_tags_produces_separate_statements_for_apply` and `test_schema_with_tags_produces_separate_statements_for_apply`.

## [0.1.3] - 2025-02-12

### Added

- **Grants**: `add_grant` and `revoke_grant` operations for Unity Catalog (catalog, schema, table, view). State reducer, state differ, and SQL generator support; state differ skips empty principals.
- **Deployment scope**: `managedCategories` and `existingObjects` in environment config. Provider-agnostic `scope_filter` filters operations before SQL generation and apply (e.g. governance-only mode, skip `CREATE CATALOG` for existing catalogs). Use with `schemax sql --target ENV` and `schemax apply --target ENV`.
- SQL generation: comments and tags for `add_catalog` and `add_schema`; stricter error handling for invalid operations. Grant SQL includes full output and FQN fallback when id_name_map is missing.
- Unit tests for scope filter, state differ (grants), state reducer (grants), SQL generator (tags/comments, grants). Integration and live E2E tests for apply and workflows.

### Changed

- Apply/sql: catalog mapping for deployment tracking always uses environment `topLevelName`; scope filter applied before SQL generation and execution.
- Unity provider: simplified table/column tag SQL (single query path); privileges module and operation metadata for managed categories.

### Fixed

- Catalog mapping logic for deployment tracking simplified and aligned across rollback, record-deployment, and CLI.

### Removed

- Deployment tracker: removed `previous_deployment_id` migration; tables created with current schema only.

## [0.1.2] - 2025-02-18

### Changed

- Version aligned with SchemaX VS Code extension 0.1.2. Single-tag release flow: push tag `v0.1.2` to publish PyPI package, extension, and docs together.

## [0.1.0] - 2025-02-16

### Added

- Python SDK and CLI for Databricks Unity Catalog schema management.
- Commands: `apply`, `rollback`, `sql`, `validate`, `diff`, `snapshot create/validate/rebase`, `record-deployment`.
- Multi-environment support, deployment tracking, and auto-rollback.
