# SchemaX

**Declarative schema management and migration for Databricks Unity Catalog** — design schemas visually, version changes in Git, and deploy consistently across environments.

SchemaX brings schema-as-code to the lakehouse. Define your catalogs, schemas, tables, views, volumes, functions, and governance policies in a visual designer or through the CLI, track every change as a versioned operation, and generate dependency-ordered SQL that deploys cleanly from dev through production.

<!-- TODO: Add screenshot — designer-overview.png -->
<!-- ![SchemaX Designer](images/designer-overview.png) -->

## Why schema management matters

As your lakehouse grows, so does the complexity of managing its structure. Catalogs, schemas, tables, views, grants, tags, row filters, column masks — these accumulate across environments and teams. Without a structured approach:

- Schema changes are ad hoc and hard to audit
- Governance policies drift between dev, test, and production
- Promoting changes across environments means manual SQL and guesswork
- Rolling back a bad deployment is painful or impossible

SchemaX solves this by treating your catalog structure as code — versioned, reviewable, and deployable through the same Git and CI/CD workflows you use for everything else.

### Works alongside Spark and Lakeflow Declarative Pipelines

If your tables are created by Spark jobs or DLT pipelines, SchemaX complements that workflow. Use **governance-only mode** to manage comments, tags, grants, row filters, and column masks on existing objects — without touching CREATE TABLE statements. Your pipelines handle the data; SchemaX handles the governance layer.

## Get started

1. **Install the extension**
   Extensions (Ctrl+Shift+X / Cmd+Shift+X) → search **SchemaX** → Install.

2. **Open the designer**
   Click the **SchemaX** icon in the Activity Bar, then **Open SchemaX Designer**.
   Or: Command Palette (Ctrl+Shift+P / Cmd+Shift+P) → **SchemaX: Open Designer**.

3. **Design your schema**
   Add catalogs, schemas, and tables. Edit columns, properties, and governance in the detail panels. Create snapshots and generate SQL when ready.

4. **Install the CLI** (for import, apply, rollback)
   Command Palette → **SchemaX: Install Python SDK**, or `pip install schemaxpy`.

## What you can do

### Schema design

- **Full object hierarchy** — Catalogs, schemas, tables, views, volumes, functions, and materialized views
- **Column management** — Add, rename, reorder, change types, set nullability, comments, and tags
- **Table configuration** — Delta and Iceberg formats, partitioning, liquid clustering, external tables with storage locations
- **View dependencies** — Automatic dependency extraction from SQL with correct creation ordering
- **Constraints** — Primary keys, foreign keys, and check constraints

### Data governance

- **Grants** — GRANT and REVOKE on all securable object types (catalogs through columns)
- **Tags** — Governance tags on catalogs, schemas, tables, views, and columns
- **Row filters** — Row-level security policies
- **Column masks** — Column-level data masking
- **Table properties** — Delta Lake TBLPROPERTIES configuration
- **Bulk operations** — Apply the same grant or tag across an entire catalog or schema in one action

<!-- TODO: Add screenshot — governance-panel.png -->
<!-- ![Governance](images/governance-panel.png) -->

### Version control and deployment

- **Snapshots** — Semantic versioned snapshots with changelog tracking
- **Environment mapping** — Logical catalog names mapped to physical names per environment (dev/test/prod)
- **SQL generation** — Dependency-ordered, idempotent DDL with environment-specific catalog names
- **Apply and rollback** — Deploy via Databricks Statement Execution API with deployment tracking
- **Safety validation** — Operations classified as SAFE, RISKY, or DESTRUCTIVE before rollback
- **Governance-only mode** — Deploy only governance DDL, skip CREATE statements for pipeline-managed objects
- **CI/CD ready** — GitHub Actions, GitLab CI, Azure DevOps, and Databricks Asset Bundles integration

<!-- TODO: Add screenshot — sql-preview.png -->
<!-- ![SQL Generation](images/sql-preview.png) -->

### Import existing assets

- **From Databricks** — Import your live Unity Catalog hierarchy into a SchemaX project
- **From SQL** — Parse DDL files and import the schema structure

## Commands

| Command | Description |
|---------|-------------|
| **SchemaX: Open Designer** | Open the visual schema designer |
| **SchemaX: Create Snapshot** | Save a versioned snapshot |
| **SchemaX: Generate SQL Migration** | Generate SQL from pending changes |
| **SchemaX: Show Last Emitted Changes** | View recent operations |
| **SchemaX: Import Existing Assets** | Import from Databricks or SQL (needs CLI) |
| **SchemaX: Install Python SDK** | Install `schemaxpy` for CLI features |

## Multi-provider roadmap

SchemaX is built on a provider architecture. Unity Catalog is fully supported today (v0.2.x). Lakebase (PostgreSQL) support is in active development for v0.3.x — same workflow, same Git integration, different catalog system.

| Provider | Status |
|----------|--------|
| **Unity Catalog** | Available (v0.2.x) |
| **Lakebase (PostgreSQL)** | In development (v0.3.x) |

## Project structure

SchemaX stores project data in a `.schemax` folder in your workspace:

- `project.json` — Project configuration and environment settings
- `changelog.json` — Pending changes (uncommitted operations)
- `snapshots/` — Version snapshots (e.g. `v0.1.0.json`)

## Requirements

- VS Code 1.90.0 or newer
- A workspace folder open
- Python SDK (`pip install schemaxpy`) for import, apply, rollback, and validate

## Links

- [Documentation](https://vb-dbrks.github.io/schemax/) — Setup, quickstart, and reference
- [Python SDK on PyPI](https://pypi.org/project/schemaxpy/) — CLI and automation
- [GitHub Repository](https://github.com/vb-dbrks/schemax-vscode) — Source code and issues
- [Report an issue](https://github.com/vb-dbrks/schemax-vscode/issues)

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.
