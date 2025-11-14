# Schematic Quickstart Guide

Complete guide to get started with Schematic - both the VS Code extension and Python SDK/CLI.

## Table of Contents

- [Providers](#providers)
- [VS Code Extension](#vs-code-extension)
- [Python SDK & CLI](#python-sdk--cli)
- [Your First Schema](#your-first-schema)
- [Generating SQL](#generating-sql)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)

---

## Providers

Schematic uses a **provider-based architecture** to support different data catalog systems.

### Supported Providers

| Provider | Status | When to Use |
|----------|--------|-------------|
| **Unity Catalog** | ✅ Available (v1.0) | Databricks Unity Catalog projects |
| **Hive Metastore** | 🔜 Coming Q1 2026 | Apache Hive / legacy Databricks |
| **PostgreSQL** | 🔜 Coming Q1 2026 | PostgreSQL with Lakebase extensions |

### Default Provider

**Unity Catalog is the default provider** for all new projects. When you create a new Schematic project (by opening the designer for the first time), it automatically initializes with Unity Catalog.

### Provider Selection (Future)

In v0.3.0+, you'll be able to select a provider when creating a new project:

```bash
# CLI (future)
schematic init --provider unity      # Unity Catalog (default)
schematic init --provider hive       # Hive Metastore
schematic init --provider postgres   # PostgreSQL

# For now, all projects use Unity Catalog
schematic init
```

**For this quickstart, we'll use Unity Catalog (the current provider).**

---

## VS Code Extension

### Installation & Launch

**Step 1**: Open the Project

```bash
cd /path/to/schematic
code .
```

**Step 2**: Launch Extension Development Host

Press **F5** (or **Fn+F5** on Mac)

This will:
- Build the extension automatically
- Open a new VS Code window called "Extension Development Host"
- Load Schematic in that window

**Step 3**: Open a Workspace

In the Extension Development Host window:
1. **File → Open Folder**
2. Create or select a project folder (e.g., `~/my-schema-project`)

### Using the Designer

**Step 4**: Launch Schematic Designer

1. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
2. Type: **Schematic: Open Designer**
3. Press Enter

The visual designer opens!

**Step 5**: Create Your First Catalog

1. Click **"Add Catalog"** button
2. Enter name: `main`
3. Click OK

**Step 6**: Add a Schema

1. Select the `main` catalog in the tree
2. Click **"Add Schema"** button
3. Enter name: `sales`
4. Click OK

**Step 7**: Add a Table

1. Select the `sales` schema
2. Click **"Add Table"** button
3. Enter name: `customers`
4. Select format: `delta`
5. Click OK

**Step 8**: Add Columns

1. Select the `customers` table
2. Click **"Add Column"** button
3. Fill in details:
   - **Name**: `customer_id`
   - **Type**: `BIGINT`
   - **Nullable**: No
   - **Comment**: `Primary key`
4. Add more columns as needed

**Step 9**: Add a View (Optional)

1. Select the `sales` schema
2. Click **"+"** button → Choose **"View"**
3. Enter SQL definition:
   ```sql
   SELECT customer_id, COUNT(*) as order_count
   FROM customers
   GROUP BY customer_id
   ```
4. View name will be auto-extracted
5. Click OK

**Note**: Schematic automatically:
- Extracts dependencies from your view SQL
- Qualifies table references with fully-qualified names (FQN)
- Orders SQL generation so tables are created before views
- Detects circular dependencies between views

**Step 10**: Create a Snapshot

1. Press `Cmd+Shift+P`
2. Type: **Schematic: Create Snapshot**
3. Enter name: `v0.1.0`
4. Enter comment: `Initial schema`

Your schema is now versioned!

### Checking the Files

```bash
ls -la .schematic/
cat .schematic/project.json
cat .schematic/changelog.json
ls -la .schematic/snapshots/
```

### Available Commands

- `Schematic: Open Designer` - Launch visual designer
- `Schematic: Create Snapshot` - Version your schema
- `Schematic: Generate SQL Migration` - Export to SQL
- `Schematic: Show Last Emitted Changes` - View operations

---

## Python SDK & CLI

### Installation

**Option 1**: Install from Source (Development)

```bash
cd schematic/packages/python-sdk
pip install -e .
```

**Option 2**: Install from PyPI (When Published)

```bash
pip install schematic-py
```

### Verify Installation

```bash
schematic --version
# Output: schematic, version 0.1.0
```

### CLI Commands

#### Validate Schema

```bash
cd your-project
schematic validate
```

Output:
```
Validating project files...
  ✓ project.json (version 2)
  ✓ changelog.json (5 operations)

Project: my_project
  Catalogs: 1
  Schemas: 1
  Tables: 2

✓ Schema files are valid
```

#### Generate SQL

```bash
# Output to stdout
schematic sql

# Save to file
schematic sql --output migration.sql

# View the file
cat migration.sql
```

#### Track Deployment

```bash
# Record deployment
schematic deploy \
  --environment prod \
  --version v1.0.0 \
  --mark-deployed
```

Output:
```
✓ Deployment recorded
  Deployment ID: deploy_abc123
  Environment: prod
  Version: v1.0.0
  Operations: 5
  Status: success
```

### Python API

Create a script to use Schematic programmatically:

```python
#!/usr/bin/env python3
from pathlib import Path
from schematic.storage_v3 import load_current_state, read_project
from schematic.providers.base.operations import Operation

# Load schema with provider
workspace = Path.cwd()
state, changelog, provider = load_current_state(workspace)

# Show summary
project = read_project(workspace)
print(f"Project: {project['name']}")
print(f"Provider: {provider.info.name}")
if "catalogs" in state:
    print(f"Catalogs: {len(state['catalogs'])}")
print(f"Pending operations: {len(changelog['ops'])}")

# Generate SQL
if changelog["ops"]:
    operations = [Operation(**op) for op in changelog["ops"]]
    generator = provider.get_sql_generator(state)
    sql = generator.generate_sql(operations)
    
    # Write to file
    Path("migration.sql").write_text(sql)
    print("✓ SQL generated: migration.sql")
```

---

## Your First Schema

Let's create a complete example from scratch.

### Step 1: Create Project Directory

```bash
mkdir ~/my-first-schema
cd ~/my-first-schema
```

### Step 2: Open in VS Code

```bash
code .
```

### Step 3: Launch Schematic (in Extension Development Host)

1. Press F5 in the main VS Code window
2. In the new window, open the `~/my-first-schema` folder
3. Press `Cmd+Shift+P` → **Schematic: Open Designer**

### Step 4: Build Schema

**Create Catalog**: `ecommerce`

**Create Schema**: `production`

**Create Tables**:

**Table 1: customers**
- Columns:
  - `id` (BIGINT, NOT NULL, Primary Key)
  - `email` (STRING, NOT NULL, Unique)
  - `name` (STRING, NOT NULL)
  - `created_at` (TIMESTAMP, NOT NULL)
- Properties:
  - `delta.enableChangeDataFeed` = `true`
- Constraints:
  - PRIMARY KEY (`id`)

**Table 2: orders**
- Columns:
  - `id` (BIGINT, NOT NULL, Primary Key)
  - `customer_id` (BIGINT, NOT NULL, Foreign Key)
  - `amount` (DECIMAL(10,2), NOT NULL)
  - `status` (STRING, NOT NULL)
  - `created_at` (TIMESTAMP, NOT NULL)
- Constraints:
  - PRIMARY KEY (`id`)
  - FOREIGN KEY (`customer_id`) REFERENCES `customers`(`id`)

### Step 5: Create Snapshot

Press `Cmd+Shift+P` → **Schematic: Create Snapshot**
- Name: `v1.0.0`
- Comment: `Initial e-commerce schema`

### Step 6: Verify Files

```bash
tree .schematic/
```

Output:
```
.schematic/
├── changelog.json
├── project.json
└── snapshots/
    └── v1.0.0.json
```

---

## Generating SQL

### From VS Code

1. Make some changes (add columns, tables, etc.)
2. Press `Cmd+Shift+P`
3. Type: **Schematic: Generate SQL Migration**
4. Review the SQL file that opens

The SQL is saved to:
```
.schematic/migrations/migration_YYYY-MM-DD_HH-MM-SS.sql
```

### From CLI

```bash
# Generate SQL from changelog
schematic sql --output deploy.sql

# Review
cat deploy.sql
```

### Example Generated SQL

```sql
-- Op: op_abc123 (2025-10-13T12:00:00Z)
-- Type: add_catalog
CREATE CATALOG IF NOT EXISTS `ecommerce`;

-- Op: op_def456 (2025-10-13T12:01:00Z)
-- Type: add_schema
CREATE SCHEMA IF NOT EXISTS `ecommerce`.`production`;

-- Op: op_ghi789 (2025-10-13T12:02:00Z)
-- Type: add_table
CREATE TABLE IF NOT EXISTS `ecommerce`.`production`.`customers` () USING DELTA;

-- Op: op_jkl012 (2025-10-13T12:03:00Z)
-- Type: add_column
ALTER TABLE `ecommerce`.`production`.`customers` 
ADD COLUMN `id` BIGINT NOT NULL COMMENT 'Primary key';
```

### Deploy to Databricks

```bash
# Option 1: Using Databricks SQL CLI
databricks sql execute \
  --file deploy.sql \
  --warehouse-id <your-warehouse-id>

# Option 2: Using Python
databricks-sql-cli \
  --hostname <workspace>.databricks.com \
  --http-path <http-path> \
  --file deploy.sql
```

---

## CI/CD Integration

### GitHub Actions

Create `.github/workflows/deploy-schema.yml`:

```yaml
name: Deploy Schema to Databricks

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install Schematic
        run: pip install schematic-py
      
      - name: Validate Schema
        run: schematic validate
      
      - name: Generate SQL
        run: schematic sql --output migration.sql
      
      - name: Upload SQL
        uses: actions/upload-artifact@v3
        with:
          name: migration-sql
          path: migration.sql

  deploy-dev:
    needs: validate
    runs-on: ubuntu-latest
    environment: development
    steps:
      - uses: actions/checkout@v3
      
      - name: Download SQL
        uses: actions/download-artifact@v3
        with:
          name: migration-sql
      
      - name: Deploy to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_DEV }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        run: |
          databricks sql execute \
            --file migration.sql \
            --warehouse-id ${{ secrets.WAREHOUSE_ID_DEV }}
      
      - name: Track Deployment
        run: |
          pip install schematic-py
          schematic deploy \
            --environment dev \
            --version ${{ github.sha }} \
            --mark-deployed

  deploy-prod:
    needs: deploy-dev
    runs-on: ubuntu-latest
    environment: production
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Download SQL
        uses: actions/download-artifact@v3
        with:
          name: migration-sql
      
      - name: Deploy to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
        run: |
          databricks sql execute \
            --file migration.sql \
            --warehouse-id ${{ secrets.WAREHOUSE_ID_PROD }}
      
      - name: Track Deployment
        run: |
          pip install schematic-py
          schematic deploy \
            --environment prod \
            --version ${{ github.sha }} \
            --mark-deployed
```

### GitLab CI

Create `.gitlab-ci.yml`:

```yaml
stages:
  - validate
  - deploy-dev
  - deploy-prod

validate-schema:
  stage: validate
  image: python:3.11
  script:
    - pip install schematic-py
    - schematic validate
    - schematic sql --output migration.sql
  artifacts:
    paths:
      - migration.sql
    expire_in: 1 day

deploy-dev:
  stage: deploy-dev
  image: python:3.11
  script:
    - pip install schematic-py databricks-cli
    - databricks sql execute --file migration.sql
    - schematic deploy --environment dev --mark-deployed
  only:
    - develop

deploy-prod:
  stage: deploy-prod
  image: python:3.11
  script:
    - pip install schematic-py databricks-cli
    - databricks sql execute --file migration.sql
    - schematic deploy --environment prod --mark-deployed
  only:
    - main
  when: manual
```

---

## Troubleshooting

### VS Code Extension

**Problem**: Extension commands not appearing

**Solution**:
1. Make sure you pressed F5 (not just opened VS Code)
2. Look for "Extension Development Host" window title
3. Open a folder in the Extension Development Host window
4. Check "Extension Host" output channel for errors

**Problem**: F5 doesn't work

**Solution**:
```bash
# Make sure you're in the right directory
cd /path/to/schematic
code .

# Wait for VS Code to fully load, then press F5

# Or use Run menu → Start Debugging
```

**Problem**: Webview doesn't open

**Solution**:
1. Check "Schematic" output channel (View → Output → Schematic)
2. Look for build errors
3. Rebuild: `cd packages/vscode-extension && npm run build`

### Python CLI

**Problem**: `schematic` command not found

**Solution**:
```bash
# Check if installed
pip list | grep schematic

# Reinstall
cd packages/python-sdk
pip install -e .

# Verify
which schematic
schematic --version
```

**Problem**: Import errors

**Solution**:
```bash
# Install with dependencies
cd packages/python-sdk
pip install -e ".[dev]"
```

**Problem**: Validation fails

**Solution**:
```bash
# Check file structure
ls .schematic/

# Validate JSON
python -m json.tool .schematic/project.json
python -m json.tool .schematic/changelog.json

# Check permissions
ls -la .schematic/
```

### SQL Generation

**Problem**: No SQL generated

**Solution**:
- Make sure there are operations in the changelog
- Check: `cat .schematic/changelog.json`
- Create some changes in the designer first

**Problem**: SQL has errors

**Solution**:
- Review the generated SQL
- Check operation IDs in comments to trace back
- Verify table/column names in the visual designer

---

## Next Steps

1. **Explore Examples**: Check `examples/basic-schema/`
2. **Read Architecture**: See `docs/ARCHITECTURE.md`
3. **Set Up CI/CD**: Use templates in `examples/github-actions/`
4. **Join Community**: GitHub Discussions

## Quick Reference

### VS Code Commands

| Command | What It Does |
|---------|-------------|
| F5 | Launch Extension Development Host |
| `Schematic: Open Designer` | Open visual designer |
| `Schematic: Create Snapshot` | Version your schema |
| `Schematic: Generate SQL Migration` | Export to SQL |

### CLI Commands

| Command | What It Does |
|---------|-------------|
| `schematic validate` | Check schema files |
| `schematic sql` | Generate SQL |
| `schematic deploy` | Track deployment |
| `schematic diff` | Compare versions |

### File Structure

```
.schematic/
├── project.json          # Metadata & configuration
├── changelog.json        # Pending operations
├── snapshots/           # Version snapshots
│   └── v*.json
└── migrations/          # Generated SQL
    └── migration_*.sql
```

---

**You're all set! Start building your schemas! 🚀**
