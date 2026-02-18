# Changelog

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
