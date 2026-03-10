# SchemaX Python SDK & CLI

**Declarative schema management and migration for Databricks Unity Catalog.** Version control your catalog structure, generate SQL migrations, and deploy consistently across environments — from a single developer workflow through CI/CD.

## What is schema management?

**Schema management** is defining and maintaining the structure of your data catalog — catalogs, schemas, tables, views, columns, constraints, and their metadata. In Unity Catalog, this is the layer that determines what exists and who can access it.

**Schema migration** is applying changes to that structure over time in a controlled way: add a column, create a table, change grants — and track those changes so they can be reviewed, versioned, and deployed per environment (dev → test → prod).

Without it, schema changes are ad hoc, hard to audit, and risky when promoting to production. With it, changes are declarative, stored in Git, and applied consistently via CI/CD or manual deployment.

### Works alongside Spark and Lakeflow Declarative Pipelines

If your tables are created by Spark jobs or DLT pipelines, SchemaX complements that workflow. Use **governance-only mode** to version and deploy comments, tags, grants, row filters, and column masks on existing objects — without touching CREATE TABLE statements. Your pipelines handle the data; SchemaX handles the governance layer.

## Features

### Full Unity Catalog object support

- **Catalogs and schemas** — create, rename, update managed locations, comments, tags, grants
- **Tables** — managed and external (Delta, Iceberg), partitioning, liquid clustering, column mapping
- **Views** — definitions, dependency tracking with automatic SQL extraction via sqlglot
- **Materialized views** — definitions, refresh schedules, dependency ordering
- **Volumes** — managed and external with storage locations
- **Functions** — SQL and Python UDFs, table functions, parameters with types and defaults
- **Columns** — add, rename, drop, reorder, change types, nullability, comments, tags
- **Constraints** — primary keys, foreign keys, check constraints (with NOT ENFORCED / RELY)

### Data governance

- **Grants** — GRANT and REVOKE on all securable types
- **Tags** — governance tags on catalogs, schemas, tables, views, and columns
- **Row filters** — row-level security policies
- **Column masks** — column-level data masking
- **Table and view properties** — TBLPROPERTIES configuration
- **Governance-only mode** — deploy only governance DDL, skip CREATE for pipeline-managed objects

### Deployment and CI/CD

- **Multi-environment** — logical catalog names mapped to physical names per environment
- **Apply with tracking** — deploy via Databricks Statement Execution API with database-backed audit trail
- **Rollback** — partial (revert a failed deployment) and complete (to a snapshot), with safety classification
- **Dry run** — preview SQL without executing
- **Auto-rollback** — automatically revert on partial failure
- **Deployment scope** — governance-only mode, existing-object awareness
- **CI/CD templates** — GitHub Actions, GitLab CI, Azure DevOps
- **Databricks Asset Bundles** — generate DAB resource YAML with `schemax bundle`

### Version control

- **Snapshots** — semantic versioned state captures
- **Changelogs** — every change tracked as a typed operation
- **State diffing** — compute minimal operations between any two snapshots
- **Dependency-ordered SQL** — correct creation ordering for views and materialized views
- **Stale detection** — detect and rebase snapshots after Git rebases

## Installation

```bash
pip install schemaxpy
```

## Quick start

### 1. Initialize a project

```bash
schemax init
```

Creates a `.schemax/` directory with project configuration.

### 2. Import from a live workspace

```bash
schemax import --profile my-databricks --warehouse-id abc123
```

Brings your existing Unity Catalog hierarchy into a SchemaX project.

### 3. Make changes and generate SQL

Use the [VS Code extension](https://marketplace.visualstudio.com/items?itemName=schematic-dev.schemax-vscode) to design visually, or modify the state directly. Then:

```bash
# Preview SQL for an environment
schemax sql --target dev

# Save to file
schemax sql --target prod --output migration.sql
```

### 4. Deploy

```bash
# Dry run
schemax apply --target dev --profile my-databricks --warehouse-id abc123 --dry-run

# Apply
schemax apply --target dev --profile my-databricks --warehouse-id abc123

# With auto-rollback on failure
schemax apply --target prod --profile my-databricks --warehouse-id abc123 --auto-rollback
```

### 5. Rollback if needed

```bash
# Partial — revert a failed deployment
schemax rollback --partial --deployment <id> --target dev --profile my-databricks --warehouse-id abc123

# Complete — rollback to a snapshot
schemax rollback --to-snapshot v0.2.0 --target dev --profile my-databricks --warehouse-id abc123
```

## CLI reference

| Command | Description |
|---------|-------------|
| `schemax init` | Initialize a new project |
| `schemax validate` | Validate project structure and schema |
| `schemax sql` | Generate SQL migration from changes |
| `schemax apply` | Deploy to a Databricks environment |
| `schemax rollback` | Rollback a deployment (partial or complete) |
| `schemax import` | Import from live Databricks or SQL file |
| `schemax snapshot create` | Create a versioned snapshot |
| `schemax snapshot validate` | Detect stale snapshots |
| `schemax snapshot rebase` | Rebase a stale snapshot |
| `schemax diff` | Compare two versions with optional SQL preview |
| `schemax bundle` | Generate Databricks Asset Bundles resource YAML |
| `schemax record-deployment` | Manually record deployment metadata |

Run `schemax <command> --help` for detailed options.

## CI/CD integration

### GitHub Actions

```yaml
name: Deploy Schema
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: pip install schemaxpy
      - run: schemax validate

      - name: Apply to production
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          schemax apply \
            --target prod \
            --profile default \
            --warehouse-id ${{ secrets.WAREHOUSE_ID }} \
            --no-interaction \
            --auto-rollback
```

## Python API

```python
from pathlib import Path
from schemax.core.storage import load_current_state, read_project, get_environment_config
from schemax.providers.base.operations import Operation

workspace = Path.cwd()
state, changelog, provider = load_current_state(workspace)

# Generate SQL with environment-specific catalog names
project = read_project(workspace)
env_config = get_environment_config(project, "prod")

catalog_mapping = {}
for catalog in state.get("catalogs", []):
    logical = str(catalog.get("name"))
    physical = env_config.get("catalogMappings", {}).get(logical, logical)
    catalog_mapping[logical] = physical

generator = provider.get_sql_generator(state)
generator.catalog_name_mapping = catalog_mapping

operations = [Operation(**op) for op in changelog["ops"]]
print(generator.generate_sql(operations))
```

## Multi-provider roadmap

SchemaX is built on a provider architecture. Unity Catalog is fully supported today (v0.2.x). Lakebase (PostgreSQL) support is in active development for v0.3.x.

| Provider | Status | Hierarchy |
|----------|--------|-----------|
| **Unity Catalog** | Available (v0.2.x) | Catalog → Schema → Table / View / Volume / Function / MV |
| **Lakebase (PostgreSQL)** | In development (v0.3.x) | Database → Schema → Table |

## Requirements

- Python 3.11+
- A SchemaX project (`.schemax/` directory)
- For deployment: Databricks workspace with SQL Warehouse access

## Links

- [Documentation](https://vb-dbrks.github.io/schemax/) — Setup, quickstart, and reference
- [VS Code Extension](https://marketplace.visualstudio.com/items?itemName=schematic-dev.schemax-vscode) — Visual schema designer
- [GitHub Repository](https://github.com/vb-dbrks/schemax-vscode) — Source code and issues
- [PyPI](https://pypi.org/project/schemaxpy/)

Apache License 2.0 — see [LICENSE](https://github.com/vb-dbrks/schemax-vscode/blob/main/LICENSE) for details.
