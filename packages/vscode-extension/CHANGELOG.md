# Changelog

## [0.1.1] - 2025-02-17

### Changed

- Marketplace icon: added 128×128 PNG so the extension shows a logo on the VS Code Marketplace.
- **SchemaX: Install Python SDK** now uses the Python interpreter selected in VS Code (`python.defaultInterpreterPath`) when set, then falls back to `python3`/`python` on PATH.
- README updated for the marketplace: user-focused copy, link to docs, and clearer Python SDK instructions.

### Fixed

- Logo/icon not appearing on the extension’s Marketplace listing (icon must be PNG, not SVG).

## [0.1.0] - 2025-02-16

### Added

- **Visual Schema Designer**: Create and edit Unity Catalog schemas (catalogs, schemas, tables, columns) in a visual UI.
- **Snapshot-based versioning**: Create snapshots, view changelog, and manage versions.
- **SQL generation**: Generate idempotent migration SQL from the Designer (Generate SQL Migration).
- **Import from Databricks**: Import existing catalogs/schemas/tables into your project (SchemaX: Import Existing Assets).
- **Data governance**: Support for constraints, column tags, row filters, column masks, and table properties.
- **SchemaX: Install Python SDK**: Command to install the SchemaX CLI via `pip install schemaxpy` for apply, rollback, validate, and import from terminal.
- Prompt to install Python SDK when a feature that needs the CLI is used and the CLI is not found.

### Requirements

- VS Code 1.90.0 or newer.
- For full features (apply, rollback, import, snapshot validate): install the Python SDK with **SchemaX: Install Python SDK** or run `pip install schemaxpy` in your environment.
