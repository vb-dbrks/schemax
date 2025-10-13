# SchemaX

**Multi-provider data catalog schema management with version control**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

SchemaX is an extensible toolkit for managing data catalog schemas (Unity Catalog, Hive Metastore, PostgreSQL) using a declarative, version-controlled approach. Design schemas visually in VS Code or manage them programmatically with Python, then generate SQL migrations for deployment.

**Current Support:** Databricks Unity Catalog (v1.0) | **Coming Soon:** Hive Metastore, PostgreSQL/Lakebase

## Features

### 🎨 Visual Schema Designer (VS Code Extension)
- Intuitive drag-and-drop interface for schema modeling
- **Provider-based**: Unity Catalog (now), Hive/PostgreSQL (coming soon)
- Adapts to provider-specific hierarchy and features
- Data governance features (constraints, tags, row filters, column masks)
- Snapshot-based versioning with semantic versions
- Real-time SQL generation from changes

### 🐍 Python SDK & CLI
- Command-line tools for automation and CI/CD
- Python API for custom workflows
- Provider-aware SQL migration generation
- Deployment tracking across environments
- Schema validation and comparison

### 🚀 Key Capabilities
- **Extensible Provider System**: Easy to add new catalog types
- **31+ Operation Types**: Complete coverage of Unity Catalog DDL
- **Dual Implementation**: TypeScript (VS Code) + Python (CLI/SDK)
- **SQL Generation**: Provider-specific, idempotent DDL statements
- **Version Control**: Git-friendly JSON format with snapshots
- **CI/CD Ready**: Integrate with GitHub Actions, GitLab CI, etc.

### 🔌 Supported Providers

| Provider | Status | Hierarchy | Features |
|----------|--------|-----------|----------|
| **Unity Catalog** | ✅ Available (v1.0) | Catalog → Schema → Table | Full governance (constraints, tags, filters, masks) |
| **Hive Metastore** | 🔜 Coming Soon | Database → Table | Tables, partitions, views |
| **PostgreSQL** | 🔜 Coming Soon | Database → Schema → Table | Tables, indexes, constraints |

**For Provider Developers:** See [PROVIDER_CONTRACT.md](docs/PROVIDER_CONTRACT.md) for implementing custom providers.

## Quick Start

### VS Code Extension

1. **Launch Extension Development Host**:
   ```bash
   cd schemax
   code .
   # Press F5 (or Fn+F5)
   ```

2. **In the new window**:
   - Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
   - Type: **SchemaX: Open Designer**
   - Start designing your schema!

3. **Generate SQL**:
   - After making changes
   - Press `Cmd+Shift+P`
   - Type: **SchemaX: Generate SQL Migration**

### Python CLI

1. **Install**:
   ```bash
   cd packages/python-sdk
   pip install -e .
   ```

2. **Use CLI**:
   ```bash
   # Validate schema files
   schemax validate
   
   # Generate SQL migration
   schemax sql --output migration.sql
   
   # Track deployment
   schemax deploy --environment prod --version v1.0.0 --mark-deployed
   ```

3. **Python API**:
   ```python
   from pathlib import Path
   from schemax.storage import load_current_state
   from schemax.sql_generator import SQLGenerator
   
   state, changelog = load_current_state(Path.cwd())
   generator = SQLGenerator(state)
   sql = generator.generate_sql(changelog.ops)
   print(sql)
   ```

## Documentation

| Document | Description |
|----------|-------------|
| **[Quickstart Guide](docs/QUICKSTART.md)** | Complete getting started guide |
| **[Architecture](docs/ARCHITECTURE.md)** | **V3** provider-based technical design |
| **[Development](docs/DEVELOPMENT.md)** | Contributing, building, **provider development** |
| **[Provider Contract](docs/PROVIDER_CONTRACT.md)** | **NEW** - Guide for implementing providers |
| **[Testing Guide](TESTING.md)** | How to test all components |
| **[VS Code Extension](packages/vscode-extension/README.md)** | Extension-specific documentation |
| **[Python SDK](packages/python-sdk/README.md)** | SDK and CLI reference |

## Repository Structure

```
schemax/
├── packages/
│   ├── vscode-extension/       # VS Code Extension (TypeScript + React)
│   │   ├── src/
│   │   │   ├── providers/            # Provider system (V3)
│   │   │   │   ├── base/             # Base interfaces
│   │   │   │   ├── unity/            # Unity Catalog provider
│   │   │   │   └── registry.ts       # Provider registry
│   │   │   ├── storage-v3.ts         # Provider-aware storage
│   │   │   ├── extension.ts          # Extension commands
│   │   │   └── webview/              # React UI
│   │   └── package.json
│   │
│   └── python-sdk/             # Python SDK & CLI
│       ├── src/schemax/
│       │   ├── providers/            # Provider system (V3)
│       │   │   ├── base/             # Base interfaces
│       │   │   ├── unity/            # Unity Catalog provider
│       │   │   └── registry.py       # Provider registry
│       │   ├── storage_v3.py         # Provider-aware storage
│       │   └── cli.py                # CLI commands
│       └── pyproject.toml
│
├── examples/                   # Working examples
│   ├── basic-schema/          # Sample project
│   ├── github-actions/        # CI/CD templates
│   └── python-scripts/        # SDK usage examples
│
├── docs/                       # Documentation
│   ├── QUICKSTART.md          # Getting started
│   ├── ARCHITECTURE.md        # V3 provider architecture
│   ├── DEVELOPMENT.md         # Contributing + provider dev
│   └── PROVIDER_CONTRACT.md   # Provider implementation guide
│
├── scripts/                    # Development scripts
│   └── smoke-test.sh          # Quick validation
│
└── .github/workflows/          # CI/CD
    ├── extension-ci.yml
    ├── python-sdk-ci.yml
    └── integration-tests.yml
```

## How It Works

### 1. Design Schema

Use the VS Code visual designer or directly edit `.schemax/` files:

```
.schemax/
├── project.json          # Project metadata
├── changelog.json        # Uncommitted operations
└── snapshots/
    └── v*.json          # Version snapshots
```

### 2. Track Changes

Every modification generates a provider-prefixed operation:
```json
{
  "id": "op_abc123",
  "ts": "2025-10-13T12:00:00Z",
  "provider": "unity",
  "op": "unity.add_column",
  "target": "col_001",
  "payload": {
    "tableId": "table_001",
    "colId": "col_001",
    "name": "customer_id",
    "type": "BIGINT",
    "nullable": false
  }
}
```

### 3. Generate SQL

Convert operations to provider-specific SQL DDL:
```sql
-- Operation: unity.add_column (op_abc123)
-- Timestamp: 2025-10-13T12:00:00Z
ALTER TABLE `main`.`sales`.`customers` 
ADD COLUMN `customer_id` BIGINT NOT NULL;
```

### 4. Deploy

Execute SQL on Databricks and track deployment:
```bash
# Generate SQL
schemax sql --environment prod --output deploy.sql

# Execute on Databricks
databricks sql execute --file deploy.sql --warehouse-id <id>

# Track deployment
schemax deploy --environment prod --version v1.0.0 --mark-deployed
```

## Unity Catalog Support

SchemaX supports all major Unity Catalog features:

### Core Objects
- ✅ Catalogs (CREATE, ALTER, DROP)
- ✅ Schemas (CREATE, ALTER, DROP)
- ✅ Tables (CREATE, ALTER, DROP)
  - Delta and Iceberg formats
  - Column mapping modes
- ✅ Columns (ADD, RENAME, ALTER TYPE, DROP)

### Data Governance
- ✅ **Constraints**: PRIMARY KEY, FOREIGN KEY, CHECK
- ✅ **Column Tags**: Key-value metadata for classification
- ✅ **Row Filters**: Row-level security with UDF expressions
- ✅ **Column Masks**: Data masking functions
- ✅ **Table Properties**: TBLPROPERTIES for Delta Lake configuration

### Example Schema

```typescript
{
  "catalogs": [{
    "name": "main",
    "schemas": [{
      "name": "sales",
      "tables": [{
        "name": "customers",
        "format": "delta",
        "columns": [
          {
            "name": "customer_id",
            "type": "BIGINT",
            "nullable": false,
            "comment": "Primary key"
          },
          {
            "name": "email",
            "type": "STRING",
            "nullable": false,
            "tags": {
              "PII": "sensitive",
              "category": "contact"
            }
          }
        ],
        "constraints": [{
          "type": "primary_key",
          "name": "pk_customers",
          "columns": ["col_001"]
        }],
        "properties": {
          "delta.enableChangeDataFeed": "true"
        }
      }]
    }]
  }]
}
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy Schema
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install SchemaX
        run: pip install schemax-py
      
      - name: Validate Schema
        run: schemax validate
      
      - name: Generate SQL
        run: schemax sql --environment prod --output migration.sql
      
      - name: Deploy to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks sql execute \
            --file migration.sql \
            --warehouse-id ${{ secrets.WAREHOUSE_ID }}
```

See [examples/github-actions/](examples/github-actions/) for more templates.

## Quality Checks & CI/CD

### Quick Quality Checks

Run all quality checks locally (formatting + smoke tests):

```bash
./devops/run-checks.sh
```

This will:
- ✅ Check Python code formatting (Black)
- ✅ Run smoke tests (build, install, validate)
- ✅ Report any issues

### CI/CD Pipeline

The project includes automated quality checks via GitHub Actions:
- Code formatting validation
- Smoke tests
- GPG commit signature verification

See [devops/README.md](devops/README.md) for pipeline details.

## Testing

### Quick Smoke Test

```bash
./scripts/smoke-test.sh
```

### Manual Testing

See [TESTING.md](TESTING.md) for comprehensive testing guide.

### Example Project

```bash
cd examples/basic-schema
schemax validate
schemax sql
```

## Requirements

- **VS Code Extension**: VS Code 1.90.0+
- **Python SDK**: Python 3.11+
- **Databricks**: Unity Catalog-enabled workspace

## Development

### Build VS Code Extension

```bash
cd packages/vscode-extension
npm install
npm run build
```

### Install Python SDK

```bash
cd packages/python-sdk
pip install -e ".[dev]"
```

### Run Tests

```bash
# Quality checks (formatting + smoke tests)
./devops/run-checks.sh

# Smoke test only
./scripts/smoke-test.sh

# Extension build
cd packages/vscode-extension && npm run build

# Python tests (when added)
cd packages/python-sdk && pytest
```

## Roadmap

### ✅ Completed

**v0.1.0 - Unity Catalog MVP**
- Visual schema designer
- Python SDK & CLI
- SQL generation (TypeScript + Python)
- Deployment tracking
- All 31 Unity Catalog operation types
- Examples and documentation

**v0.2.0 - Provider Architecture (Current)**
- ✅ Extensible provider system
- ✅ Provider registry
- ✅ Unity Catalog provider (v1.0)
- ✅ Provider-aware storage (V3)
- ✅ Comprehensive provider documentation
- ✅ Provider development guide

### 🔜 Next (v0.3.0 - Q1 2026)
- [ ] **Hive Metastore provider**
- [ ] **PostgreSQL/Lakebase provider**
- [ ] Provider compliance test suite
- [ ] Dynamic UI components
- [ ] Extended Unity Catalog (volumes, functions)

### 🔄 Future
- [ ] Multi-provider projects
- [ ] Databricks Asset Bundle (DAB) generation
- [ ] Schema import from Databricks
- [ ] Drift detection
- [ ] Visual diff viewer
- [ ] Template library
- [ ] Provider marketplace
- [ ] VS Code Marketplace publication
- [ ] PyPI publication

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Team

**Development Team**: Professional Services 
**Developer**: [Varun Bhandary](https://github.com/vb-dbrks)

## Support

- **Issues**: [GitHub Issues](https://github.com/vb-dbrks/schemax/issues)
- **Documentation**: [docs/](docs/)
- **Examples**: [examples/](examples/)

---

**SchemaX** - Making data catalog schema management declarative, extensible, and version-controlled. 🚀

**Current**: Unity Catalog | **Coming Soon**: Hive Metastore, PostgreSQL | **Extensible**: Add your own provider!
