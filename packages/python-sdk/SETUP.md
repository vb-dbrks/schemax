# Development Setup

This guide walks you through setting up the development environment for the Schematic Python SDK.

## Prerequisites

- Python 3.11 or higher
- pip or uv (recommended)
- VS Code or Cursor (recommended IDE)

## Quick Setup

### 1. Install Dependencies

```bash
cd packages/python-sdk

# Using uv (recommended/faster)
uv pip install -e ".[dev]"

# Using pip
pip install -e ".[dev]"
```

This installs:
- Schematic SDK in editable mode
- pytest, pytest-cov (testing)
- ruff (formatting & linting)
- mypy (type checking)
- sqlglot (SQL validation)
- pre-commit (git hooks)

### 2. Install Pre-Commit Hooks

```bash
pre-commit install
```

This installs git hooks that automatically run before every commit:
- ✅ Ruff format (auto-format code)
- ✅ Ruff lint (auto-fix issues)
- ✅ Mypy type check (enforce type annotations)

**Your commits will be blocked** if these checks fail!

### 3. Install VS Code Extensions (Recommended)

Install these extensions in VS Code/Cursor:
1. **Ruff** (`charliermarsh.ruff`) - Formatting & linting
2. **Pylance** (Microsoft) - Type checking
3. **Mypy Type Checker** (`ms-python.mypy-type-checker`) - Additional type checking

The workspace settings (`.vscode/settings.json`) are already configured to:
- Format on save
- Show type errors inline
- Auto-fix linting issues

## Development Workflow

### Quick Commands (using Makefile)

```bash
# Format code
make format

# Lint code
make lint

# Type check
make typecheck

# Run tests
make test

# Run all checks (recommended before committing)
make all
```

### Manual Commands

```bash
# Format
ruff format src/ tests/

# Lint
ruff check src/ tests/ --fix

# Type check
mypy src/

# Test
pytest tests/ -v

# Test with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

### Before Every Commit

Pre-commit hooks run automatically, but you can also run them manually:

```bash
# Run pre-commit hooks on all files
pre-commit run --all-files

# Or just run the quality checks
make all
```

## Configuration Files

### `pyproject.toml`
- Ruff configuration (line length: 100, Python 3.11+)
- Mypy configuration (strict type checking enabled)
- Pytest configuration

### `.pre-commit-config.yaml`
- Pre-commit hooks for Ruff and mypy
- Auto-runs before every git commit

### `.vscode/settings.json` (workspace root)
- Format on save enabled
- Ruff as default formatter
- Mypy type checking enabled

### `Makefile`
- Convenient commands for development tasks

## Code Quality Standards

### Type Annotations (REQUIRED)

ALL functions must have complete type annotations:

```python
# ✅ Good
def process_data(name: str, count: int, options: Optional[Dict[str, Any]] = None) -> List[str]:
    return []

# ❌ Bad - missing type annotations
def process_data(name, count, options=None):
    return []
```

### Formatting

- Line length: 100 characters
- Quote style: double quotes
- Ruff automatically formats on commit

### Testing

- Run tests before committing: `pytest tests/ -v`
- Maintain test coverage: `pytest tests/ --cov=src`
- Current status: 168 passed, 12 skipped

## Troubleshooting

### Pre-commit hook fails

If pre-commit fails, the commit is blocked. Fix the issues and commit again:

```bash
# See what failed
pre-commit run --all-files

# Fix issues manually or let Ruff auto-fix
ruff check src/ tests/ --fix

# Try committing again
git commit -m "..."
```

### Mypy errors

If mypy reports type errors:

```bash
# Check specific file
mypy src/schematic/your_file.py

# Check all files
mypy src/

# See detailed error with context
mypy src/ --show-error-context
```

### Tests fail

If tests fail:

```bash
# Run specific test
pytest tests/unit/test_file.py::TestClass::test_method -xvs

# Run with verbose output
pytest tests/ -v

# Run with debugging
pytest tests/ -x --pdb
```

## CI/CD

GitHub Actions automatically run quality checks on every PR:
1. Ruff format check
2. Ruff lint check
3. Mypy type check
4. Pytest with coverage

PRs are blocked if any check fails.

## Getting Help

- Check `.cursorrules` for coding standards
- See `docs/DEVELOPMENT.md` for architecture details
- See `TESTING.md` for testing guide
- Run `make help` for available commands

