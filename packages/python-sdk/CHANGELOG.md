# Changelog

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
