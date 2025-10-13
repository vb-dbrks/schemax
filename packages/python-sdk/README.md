# SchemaX Python SDK & CLI

Python toolkit for managing Databricks Unity Catalog schemas with CI/CD integration.

## Features

- **SQL Generation**: Generate SQL migration scripts from schema changes
- **Schema Validation**: Validate `.schemax/` project files
- **Deployment Tracking**: Track what's deployed to each environment
- **DAB Generation**: Create Databricks Asset Bundles for deployment
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

### Validate Schema Files

```bash
cd your-project/
schemax validate
```

### Generate SQL Migration

```bash
# Generate SQL from changelog
schemax sql --output migration.sql

# Generate SQL for specific version range
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
from schemax.storage import load_current_state
from schemax.sql_generator import SQLGenerator

# Load schema
workspace = Path.cwd()
state, changelog = load_current_state(workspace)

# Generate SQL
generator = SQLGenerator(state)
sql = generator.generate_sql(changelog.ops)

print(sql)
```

### Validate Schema

```python
from schemax.storage import read_project, read_changelog

try:
    project = read_project(Path.cwd())
    changelog = read_changelog(Path.cwd())
    print("✓ Schema is valid")
except Exception as e:
    print(f"✗ Validation failed: {e}")
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

- Python 3.9 or higher
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

