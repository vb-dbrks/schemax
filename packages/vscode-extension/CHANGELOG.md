# Changelog

## [0.2.10] - 2026-03-10

### Added

- **View tags in bulk operations** — The bulk operations panel now supports adding tags to all views in a catalog or schema scope, alongside the existing table, schema, and catalog tag operations.
- **Multi-principal grants** — Enter multiple principals (comma-separated) in the bulk operations panel to grant privileges to several users, groups, or service principals at once.

### Changed

- **Marketplace content** — Rewrote the VS Code Marketplace and Open VSX listing with user-focused descriptions: what schema management is, why it matters alongside DLT and Spark, and how to get started. Added multi-provider roadmap hint (Unity Catalog now, Lakebase/PostgreSQL in v0.3.x).

### Fixed

- **Python environment detection** — The extension now finds `schemax` and Python in conda, miniconda, venv, uv, pyenv, and poetry environments. It reads the VS Code Python interpreter setting (`python.defaultInterpreterPath`) and tries that first, then falls back to PATH. Commands are spawned through the user's shell so conda-activated environments resolve correctly.
- **Bulk operations label alignment** — Fixed labels floating above input fields and dropdowns in the bulk operations modal.

## [0.2.9] - 2026-03-06

### Added

- **Sidebar buttons** — Added "Import Existing Assets" and "Generate DAB Resources" buttons to the SchemaX sidebar welcome view.
- **Generate DAB Resources command** — New `schemax.generateBundle` command invokes `schemax bundle` to generate Databricks Asset Bundle resource YAML and deploy script. Opens the generated `schemax.yml` in the editor on success.

### Changed

- **Runtime compatibility floor** — Updated minimum supported CLI version to `0.2.9`.
- **Supported commands** — Runtime info contract now includes `bundle` and `changelog.undo`.

### Fixed

- **Bundle resource filename** — Extension now looks for `schemax.yml` (matching actual generator output) instead of `schemax_job.yml`.

## [0.2.8] - 2026-02-27

### Added

- **Open VSX publishing** — New GitHub workflow `publish-openvsx.yml` publishes the extension to Open VSX (namespace `schemax`) for Cursor, Antigravity, and other Open VSX-based editors. Triggered on `v*` tag push or manual workflow dispatch.
- **Publishing documentation** — README "Publishing (maintainers)" section documents both VS Code Marketplace and Open VSX workflows, and CI-only publishing.

### Changed

- **VS Code publish workflow** — Clarified workflow name and comments for Visual Studio Marketplace only; added `workflow_dispatch` for manual re-publish.
- **Open VSX token handling** — Publish step uses `OVSX_PAT` from environment only (removed redundant `--pat` from command).

## [0.2.7] - 2026-03-02

### Added

- **Legacy workspace UX handling** — Added explicit user-facing error messaging for backend `LEGACY_SINGLE_CATALOG_UNSUPPORTED` failures.

### Changed

- **Workspace-state error propagation** — Backend envelope error codes are now preserved in extension load/update flows.
- **Runtime compatibility floor** — Updated minimum supported CLI version to `0.2.7`.

### Fixed

- **Release versioning resilience** — Updated extension version metadata alignment as part of shared release-tooling hardening.

## [0.2.6] - 2026-03-02

### Added

- **Python backend transport layer** — Added unified `PythonBackendClient` command envelope transport for extension workflows.
- **Runtime compatibility checks** — Added CLI/runtime compatibility validation before workflow execution.
- **Architecture fitness tests** — Added test coverage to enforce Python-first semantic boundaries.

### Changed

- **Semantic de-duplication** — Removed extension-side provider runtime semantics from active command paths; Python SDK is now authoritative for diff/sql/import/apply/rollback/snapshot checks.
- **Designer/runtime integration** — Updated workspace and command flows to consume typed backend envelopes.
- **Lint/type stabilization** — Completed extension lint/typecheck stabilization for the migration baseline.

### Removed

- **Legacy TS provider runtime modules** — Removed duplicate provider/base semantic runtime implementations from the extension package.

## [0.2.5] - 2026-02-24

### Added

- **Bulk operations** — New "Bulk operations" panel in the Designer. From a catalog or schema detail view, click "Bulk operations" to apply **grants** (principal + privileges to all objects in scope) or **tags** (table tag, schema tag, or catalog tag) to the selected catalog or schema and all objects under it. Scope is resolved from project state (catalog + schemas + tables + views + volumes + functions + materialized views for grants; tables/schemas/catalog for tags). Operations are emitted like other designer changes and appear in the changelog and generated SQL.

### Changed

- **README** — New SchemaX text logo (PNG); old SVG removed.

## [0.2.1] - 2026-02-21

### Added

- **Volumes, functions, and materialized views** — Design and manage Unity Catalog volumes, functions, and materialized views in the Designer. Create, comment, grant, and control deployment scope per environment.
- **View and materialized view dependencies** — In View and Materialized View details, you can see and edit which tables or views they depend on. Dependencies are inferred from SQL and you can add or remove them. Generated SQL creates base tables and views before dependent views and MVs.
- **Edit grants** — Change who has access to what: edit grants on catalogs, schemas, tables, views, volumes, functions, and materialized views from the UI. Use the edit dialog to change principals and privileges; generated SQL stays correct.

### Changed

- **Materialized view SQL** — Comments and DROP statements for materialized views now follow Databricks syntax. Generated migrations create dependencies in the right order.
- **Documentation** — Docs and README updated for volumes, functions, and materialized views. Unity Catalog grants documentation now covers all supported object types.

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
