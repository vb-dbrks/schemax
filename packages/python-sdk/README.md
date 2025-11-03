# Schematic Python SDK & CLI

**Declarative schema management** for modern data catalogs. Version control your schemas, generate SQL migrations, and deploy with confidence across multiple environments.

## Features

- **Multi-Provider Architecture**: Unity Catalog (Databricks), Hive, PostgreSQL, and more
- **Version-Controlled Schemas**: Git-based workflow with snapshots and changelogs
- **SQL Migration Generation**: Generate idempotent SQL DDL from schema changes
- **Environment Management**: Dev, test, prod with catalog name mapping
- **Deployment Tracking**: Know what's deployed where with database-backed tracking
- **Type-Safe**: Full type annotations, validated with mypy
- **CI/CD Ready**: Designed for GitHub Actions, GitLab CI, and other pipelines
- **Extensible**: Plugin architecture for custom catalog providers

## Why Schematic?

**Provider-agnostic design**: Write your schema once, deploy to any catalog system. Start with Unity Catalog (Databricks) and easily extend to Hive, PostgreSQL, Snowflake, or custom providers.

**Git-based workflow**: Your schemas are code. Version them, review them, and deploy them with confidence using familiar Git workflows.

**Environment-aware**: Manage dev, test, and prod environments with automatic catalog name mapping. No more hardcoded catalog names in SQL.

**Type-safe and tested**: Built with Python 3.11+ type hints, validated with mypy, and covered by 138+ tests. Production-ready from day one.

## Installation

```bash
pip install schematic-py
```

### Development Install

```bash
git clone https://github.com/vb-dbrks/schematic.git
cd schematic/packages/python-sdk
pip install -e ".[dev]"
```

## Quick Start

### 1. Initialize a New Project

```bash
# Unity Catalog (Databricks) - default
schematic init

# PostgreSQL
schematic init --provider postgres

# Hive Metastore
schematic init --provider hive
```

This creates a `.schematic/` directory with your project configuration.

### 2. Validate Your Schema

```bash
schematic validate
```

Validates project structure, provider compatibility, and schema correctness.

### 3. Generate SQL Migration

```bash
# Generate SQL from changelog
schematic sql --output migration.sql

# Generate for specific environment (with catalog mapping)
schematic sql --target dev --output dev-migration.sql
```

### 4. Apply Changes (Unity Catalog)

```bash
# Preview changes
schematic apply --target dev --profile my-databricks --warehouse-id abc123 --dry-run

# Apply to environment
schematic apply --target dev --profile my-databricks --warehouse-id abc123
```

### 5. Track Deployments

```bash
# Record deployment (works for all providers)
schematic record-deployment --environment prod --version v1.0.0 --mark-deployed
```

## CLI Commands

### `schematic sql`

Generate SQL DDL migration scripts from schema changes.

**Options:**
- `--output, -o`: Output file path (default: stdout)
- `--target, -t`: Target environment (applies catalog name mapping)

**Examples:**
```bash
# Output to stdout
schematic sql

# Save to file
schematic sql --output migration.sql

# Generate for specific environment
schematic sql --target prod --output prod-migration.sql
```

### `schematic apply` (Unity Catalog only)

Execute SQL migrations against a Databricks Unity Catalog environment.

**Options:**
- `--target, -t`: Target environment (required)
- `--profile, -p`: Databricks CLI profile (required)
- `--warehouse-id, -w`: SQL Warehouse ID (required)
- `--sql`: SQL file to execute (optional, generates from changelog if not provided)
- `--dry-run`: Preview changes without executing
- `--no-interaction`: Skip confirmation prompts (for CI/CD)

**Examples:**
```bash
# Preview changes
schematic apply --target dev --profile default --warehouse-id abc123 --dry-run

# Apply with confirmation
schematic apply --target prod --profile prod --warehouse-id xyz789

# Non-interactive (CI/CD)
schematic apply --target prod --profile prod --warehouse-id xyz789 --no-interaction
```

### `schematic validate`

Validate `.schematic/` project files for correctness and provider compatibility.

**Examples:**
```bash
# Validate current directory
schematic validate

# Validate specific directory
schematic validate /path/to/project
```

### `schematic record-deployment`

Manually record deployment metadata (useful for non-Unity Catalog providers).

**Options:**
- `--environment, -e`: Environment name (required)
- `--version, -v`: Version deployed (default: latest snapshot)
- `--mark-deployed`: Mark as successfully deployed

**Examples:**
```bash
# Record successful deployment
schematic record-deployment --environment prod --version v1.0.0 --mark-deployed
```

### `schematic diff`

Compare two schema versions and show the operations needed to transform one into the other.

**Examples:**
```bash
# Basic diff
schematic diff --from v0.1.0 --to v0.2.0

# Show generated SQL with logical catalog names
schematic diff --from v0.1.0 --to v0.2.0 --show-sql

# Show SQL with environment-specific catalog names
schematic diff --from v0.1.0 --to v0.2.0 --show-sql --target dev

# Show detailed operation payloads
schematic diff --from v0.1.0 --to v0.2.0 --show-details
```

## Python API

### Generate SQL Programmatically

```python
from pathlib import Path
from schematic.storage_v4 import load_current_state, read_project, get_environment_config
from schematic.providers.base.operations import Operation

# Load schema with provider
workspace = Path.cwd()
state, changelog, provider = load_current_state(workspace)

print(f"Provider: {provider.info.name} v{provider.info.version}")

# Convert ops to Operation objects
operations = [Operation(**op) for op in changelog["ops"]]

# Generate SQL using provider's SQL generator
generator = provider.get_sql_generator(state)
sql = generator.generate_sql(operations)

print(sql)
```

### Environment-Specific SQL Generation

```python
from pathlib import Path
from schematic.storage_v4 import load_current_state, read_project, get_environment_config

workspace = Path.cwd()
state, changelog, provider = load_current_state(workspace)

# Get environment configuration
project = read_project(workspace)
env_config = get_environment_config(project, "prod")

# Build catalog name mapping (logical -> physical)
catalog_mapping = {}
for catalog in state.get("catalogs", []):
    logical_name = catalog.get("name", "__implicit__")
    physical_name = env_config.get("catalog", logical_name)
    catalog_mapping[logical_name] = physical_name

# Generate SQL with environment-specific catalog names
generator = provider.get_sql_generator(state)
generator.catalog_name_mapping = catalog_mapping  # For Unity provider

operations = [Operation(**op) for op in changelog["ops"]]
sql = generator.generate_sql(operations)

print(sql)  # Contains prod catalog names
```

### Working with Multiple Providers

```python
from schematic.providers import ProviderRegistry

# List available providers
providers = ProviderRegistry.get_all_ids()
print(f"Available providers: {providers}")

# Get specific provider
unity_provider = ProviderRegistry.get("unity")
if unity_provider:
    print(f"Name: {unity_provider.info.name}")
    print(f"Version: {unity_provider.info.version}")
    print(f"Operations: {len(unity_provider.info.capabilities.supported_operations)}")
```

### Validate Schema

```python
from pathlib import Path
from schematic.storage_v4 import read_project, load_current_state

try:
    workspace = Path.cwd()
    project = read_project(workspace)
    state, changelog, provider = load_current_state(workspace)
    
    # Validate with provider
    validation = provider.validate_state(state)
    if validation.valid:
        print("✓ Schema is valid")
    else:
        print("✗ Validation failed:")
        for error in validation.errors:
            print(f"  - {error.field}: {error.message}")
except Exception as e:
    print(f"✗ Error: {e}")
```

## CI/CD Integration

### GitHub Actions (Generic)

```yaml
name: Schema Management
on:
  pull_request:
  push:
    branches: [main]

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
      
      - name: Generate SQL Preview
        run: schematic sql --target prod --output migration.sql
      
      - name: Upload SQL
        uses: actions/upload-artifact@v3
        with:
          name: migration-sql
          path: migration.sql
```

### GitHub Actions (Unity Catalog - Automated Deployment)

```yaml
name: Deploy to Unity Catalog
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v3
      
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install Schematic
        run: pip install schematic-py
      
      - name: Validate Schema
        run: schematic validate
      
      - name: Apply to Production
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          schematic apply \
            --target prod \
            --profile default \
            --warehouse-id ${{ secrets.WAREHOUSE_ID }} \
            --no-interaction
```

### GitLab CI

```yaml
validate-schema:
  stage: test
  image: python:3.11
  script:
    - pip install schematic-py
    - schematic validate
    - schematic sql --target prod --output migration.sql
  artifacts:
    paths:
      - migration.sql
    expire_in: 1 week
```

## Supported Providers

| Provider | Status | Operations | Apply Command | Notes |
|----------|--------|------------|---------------|-------|
| **Unity Catalog** | ✅ Stable | 29 | ✅ `schematic apply` | Full Databricks integration |
| **Hive Metastore** | 🚧 Planned | TBD | Manual | SQL generation only |
| **PostgreSQL** | 🚧 Planned | TBD | Manual | SQL generation only |

Want to add a provider? See [PROVIDER_CONTRACT.md](../../docs/PROVIDER_CONTRACT.md).

## Requirements

- **Python 3.11+**
- A Schematic project (`.schematic/` directory)
- For Unity Catalog: Databricks workspace with SQL Warehouse access

## Documentation

- [Quick Start Guide](../../docs/QUICKSTART.md)
- [Architecture Overview](../../docs/ARCHITECTURE.md)
- [Development Guide](./SETUP.md)
- [Provider Contract](../../docs/PROVIDER_CONTRACT.md)

## Development

See [SETUP.md](./SETUP.md) for complete development setup instructions.

**Quick setup:**
```bash
cd packages/python-sdk
uv pip install -e ".[dev]"  # Or use pip
pre-commit install
make all  # Run all quality checks
```

**Commands:**
```bash
make format      # Format code
make lint        # Lint code
make typecheck   # Type check
make test        # Run tests
make all         # Run all checks
```

## License

MIT License - see [LICENSE](../../LICENSE) for details.

## Links

- **Repository**: https://github.com/vb-dbrks/Schematic
- **Issues**: https://github.com/vb-dbrks/Schematic/issues
- **VS Code Extension**: [schematic-vscode](../vscode-extension/)
- **PyPI**: https://pypi.org/project/schematic-py/ (coming soon)

