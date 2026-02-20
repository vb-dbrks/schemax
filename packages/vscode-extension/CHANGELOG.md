# Changelog

## [0.2.0] - 2025-02-12

### Added

- **Import from SQL file**: New "From SQL file" tab in the Import assets modal. Pick a `.sql` file, choose mode (diff/replace), optional target and dry-run, then Run to execute `schemax import --from-sql` and bring DDL into the project without a live Databricks connection. Supports CREATE + ALTER in file order (e.g. create table then alter to add columns).

### Changed

- **Import modal**: Two tabs — "From Databricks" (existing live import) and "From SQL file" (new). Validation errors are cleared when switching tabs to avoid showing the wrong tab’s error message.

## [0.1.4] - 2025-02-19

### Added

- **Help button**: New `?` icon button in the top-right toolbar opens the Quickstart documentation in the browser.

### Changed

- **Docs URLs**: All hardcoded documentation URLs updated from `/schemax-vscode/` to `/schemax/` to match the production GitHub Pages deployment.

### Fixed

- **EnvironmentSummary Docs button**: Trailing slash added to the environments-and-scope URL (avoids redirect on GitHub Pages).

## [0.1.3] - 2025-02-12

### Added

- **Grants (Unity Catalog)**: Add and revoke grants on catalogs, schemas, tables, and views from the Security Governance UI. State differ and SQL generator support grant operations; generated SQL includes full grant/revoke output.
- **Deployment scope**: Per-environment **managed categories** (limit which DDL SchemaX emits, e.g. governance-only) and **existing objects** (skip `CREATE CATALOG` for catalogs that already exist). Configure in Project Settings → Environment → Deployment scope and Existing objects.
- Documentation for [Environments and deployment scope](https://vb-dbrks.github.io/schemax/guide/environments-and-scope) and [Unity Catalog grants](https://vb-dbrks.github.io/schemax/guide/unity-catalog-grants) in the docs site.

### Changed

- **Docs**: Contributing and long-form docs moved to the Docusaurus site. Root CONTRIBUTING.md and README.md now point to the docs site; legacy `docs/*.md` files removed.
- **Environment summary “Docs” button**: Opens the environments-and-scope guide in the browser (fixed broken link to removed `docs/QUICKSTART.md`).
- Security Governance UI: Layout and styling for grants, row filters, and column masks; consistent use of VSCodeButton and delete icon.

### Fixed

- “Docs” button in Environment summary no longer fails when the old `docs/QUICKSTART.md` path was removed; extension now supports opening a docs URL in the browser.

## [0.1.2] - 2025-02-18

### Changed

- Version aligned with SchemaX Python SDK 0.1.2. Single-tag release flow: push tag `v0.1.2` to publish extension, PyPI package, and docs together.

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
