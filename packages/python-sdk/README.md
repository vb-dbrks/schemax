# SchemaX Python SDK & CLI

Python toolkit for managing catalog schemas with a provider-based architecture. Supports multiple catalog providers including Unity Catalog, Hive, PostgreSQL, and more.

## Features

- **Multi-Provider Support**: Works with Unity Catalog, Hive, PostgreSQL, and more
- **SQL Generation**: Generate SQL migration scripts from schema changes
- **Schema Validation**: Validate `.schemax/` project files
- **Deployment Tracking**: Track what's deployed to each environment
- **DAB Generation**: Create Databricks Asset Bundles for deployment (coming soon)
- **CI/CD Ready**: Designed for GitHub Actions, GitLab CI, and other pipelines

## Installation

```bash
pip install schemax-py
```

### Development Install

```bash
git clone https://github.com/vb-dbrks/schemax.git
cd schemax/packages/python-sdk
pip install -e ".[dev]"
```

## Quick Start

### Initialize a New Project

```bash
# Initialize with Unity Catalog provider (default)
schemax init

# Initialize with a specific provider
schemax init --provider postgres
```

### Validate Schema Files

```bash
cd your-project/
schemax validate
```

### Generate SQL Migration

```bash
# Generate SQL from changelog
schemax sql --output migration.sql

# Generate SQL for specific version range (coming soon)
schemax sql --from-version v0.1.0 --to-version v0.2.0 --output migration.sql
```

### Track Deployment

```bash
# Record deployment to environment
schemax deploy --environment prod --version v1.0.0

# Generate incremental SQL based on what's deployed
schemax sql --environment prod --output incremental.sql
```

### Generate Databricks Asset Bundle

```bash
schemax bundle --environment prod --version v1.0.0 --output .schemax/dab

# Then deploy with Databricks CLI
cd .schemax/dab/prod
databricks bundle deploy
databricks bundle run schemax_migration
```

## CLI Commands

### `schemax init`

Initialize a new SchemaX project with provider selection.

**Options:**
- `--provider, -p`: Catalog provider (unity, hive, postgres) - default: unity

**Examples:**
```bash
# Initialize with Unity Catalog
schemax init

# Initialize with PostgreSQL
schemax init --provider postgres
```

### `schemax sql`

Generate SQL migration scripts from operations.

**Options:**
- `--output, -o`: Output file path (default: stdout)
- `--from-version`: Generate from this version
- `--to-version`: Generate to this version
- `--environment, -e`: Target environment (generates incremental SQL)

**Examples:**
```bash
# Output to stdout
schemax sql

# Save to file
schemax sql -o migration.sql

# Incremental migration for environment
schemax sql -e prod -o prod-migration.sql
```

### `schemax validate`

Validate `.schemax/` project files for correctness.

**Examples:**
```bash
schemax validate
```

### `schemax deploy`

Track deployment metadata.

**Options:**
- `--environment, -e`: Environment name (required)
- `--version, -v`: Version to deploy (default: latest)

**Examples:**
```bash
# Record deployment
schemax deploy -e prod -v v1.0.0
```

### `schemax bundle`

Generate Databricks Asset Bundle for deployment.

**Options:**
- `--environment, -e`: Environment name (required)
- `--version, -v`: Version to bundle (required)
- `--output, -o`: Output directory (default: .schemax/dab)

**Examples:**
```bash
schemax bundle -e prod -v v1.0.0
```

### `schemax diff`

Compare two schema versions (coming soon).

**Examples:**
```bash
schemax diff v0.1.0 v0.2.0
```

## Python API

### Generate SQL Programmatically

```python
from pathlib import Path
from schemax.storage_v3 import load_current_state
from schemax.providers.base.operations import Operation

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

### Working with Multiple Providers

```python
from schemax import ProviderRegistry

# List available providers
providers = ProviderRegistry.get_all_ids()
print(f"Available providers: {providers}")

# Get specific provider
unity_provider = ProviderRegistry.get("unity")
print(f"Unity Catalog: {unity_provider.info.name}")
print(f"Supported operations: {unity_provider.info.capabilities.supported_operations}")

# Validate state
validation = unity_provider.validate_state(state)
if not validation.valid:
    for error in validation.errors:
        print(f"Error: {error.message}")
```

### Validate Schema

```python
from pathlib import Path
from schemax.storage_v3 import read_project, load_current_state

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

### GitHub Actions

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
      
      - name: Upload SQL
        uses: actions/upload-artifact@v3
        with:
          name: migration-sql
          path: migration.sql
```

### GitLab CI

```yaml
validate-schema:
  stage: test
  image: python:3.11
  script:
    - pip install schemax-py
    - schemax validate

deploy-prod:
  stage: deploy
  image: python:3.11
  script:
    - pip install schemax-py
    - schemax sql --environment prod --output migration.sql
    - schemax bundle --environment prod --version $CI_COMMIT_TAG
  only:
    - tags
```

## Requirements

- Python 3.11 or higher
- A SchemaX project (`.schemax/` directory)

## Documentation

- [Getting Started](../../docs/GETTING-STARTED.md)
- [SQL Generation](../../docs/SQL-GENERATION.md)
- [CI/CD Integration](../../docs/CICD-INTEGRATION.md)
- [API Reference](./docs/api-reference.md)

## Development

### Setup

```bash
# Clone repository
git clone https://github.com/vb-dbrks/schemax.git
cd schemax/packages/python-sdk

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
pytest --cov=schemax
```

### Linting

```bash
ruff check src/
mypy src/
```

## License

MIT License - see [LICENSE](../../LICENSE) for details.

## Links

- **Repository**: https://github.com/vb-dbrks/schemax
- **Issues**: https://github.com/vb-dbrks/schemax/issues
- **PyPI**: https://pypi.org/project/schemax-py/ (coming soon)

