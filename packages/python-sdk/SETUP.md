# Development Setup

Complete setup guide for the SchemaX Python SDK development environment.

---

## 🚀 Quick Start (2 Minutes)

```bash
cd packages/python-sdk

# 1. Install dependencies (uv is faster, or use pip)
uv pip install -e ".[dev]"

# 2. Install pre-commit hooks
pre-commit install

# 3. Verify setup
make all
```

**Done!** ✅ You're ready to develop.

---

## Prerequisites

- **Python 3.11+**
- **pip** or **uv** (recommended - faster)
- **VS Code/Cursor** (recommended IDE)
- **Git** with commit signing configured

---

## Detailed Setup

### 1. Install Dependencies

```bash
cd packages/python-sdk

# Using uv (recommended/faster)
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

**What gets installed:**
- SchemaX SDK in editable mode
- pytest, pytest-cov (testing)
- ruff (formatting & linting)
- mypy (type checking)
- sqlglot (SQL validation)
- pre-commit (git hooks)

### 2. Install Pre-Commit Hooks

```bash
pre-commit install
```

**What this does:**
- Installs git hooks that run before every commit
- ✅ Ruff format (auto-format code)
- ✅ Ruff lint (auto-fix issues)
- ✅ Mypy type check (enforce type annotations)
- ❌ **Blocks commits if errors remain**

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

### Daily Development

**While coding:**
- ✅ Format on save is enabled (Ruff)
- ✅ Type errors show inline (Pylance + Mypy)
- ✅ Linting issues get auto-fixed

**Before committing:**
```bash
# Option 1: Run pre-commit checks (faster - skips tests)
make pre-commit

# Option 2: Run all checks including tests (recommended)
make all

# Option 3: Simulate exact CI checks (what CI will run)
make ci
```

**When committing:**
```bash
git add .
git commit -m "your message"
```

Pre-commit hooks automatically:
1. ✅ Format your code (Ruff)
2. ✅ Fix linting issues (Ruff)
3. ✅ Check types (mypy)
4. ❌ **Block commit if errors remain**

If blocked, fix the errors and commit again.

### 🎯 Recommended Workflow

```bash
# 1. Make your changes
# 2. Format and check (this auto-fixes issues)
make pre-commit

# 3. Add files and commit
git add -A
git commit -S -m "your commit message"

# 4. Push
git push origin your-branch
```

This ensures your code is formatted, linted, and type-checked before CI runs!

### Quick Commands (Makefile)

```bash
make format      # Format code with Ruff (auto-fix)
make lint        # Lint code with Ruff (auto-fix)
make typecheck   # Type check with mypy
make test        # Run tests with pytest
make all         # Run all checks (format, lint, typecheck, test)
make check       # Run format/lint checks without modifying files (like CI)
make ci          # Run exact CI checks (format check + lint check + typecheck + test)
make pre-commit  # Format, lint, typecheck (recommended before git commit)
make install     # Install package in dev mode
make clean       # Remove build artifacts
make help        # Show all available commands
```

**🔥 Most Used:**
- `make pre-commit` - Before committing (formats and checks, no tests)
- `make all` - Full check before pushing (includes tests)
- `make ci` - Verify what CI will see

### Manual Commands (if needed)

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

### Pre-Commit Hooks

Hooks run automatically on commit, but you can run them manually:

```bash
# Run on all files
pre-commit run --all-files

# Run only on staged files
pre-commit run
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

### What's Enforced

✅ **Type annotations** on all functions (required)  
✅ **100 character line length** (auto-formatted)  
✅ **Double quotes** for strings (auto-formatted)  
✅ **Proper imports** (auto-organized)  
✅ **No type errors** (mypy strict mode)  
✅ **All tests pass** (138 passed, 12 skipped)

### Type Annotations (REQUIRED)

**ALL functions must have complete type annotations:**

```python
# ✅ Good - Complete type annotations
def process_data(name: str, count: int, options: Optional[Dict[str, Any]] = None) -> List[str]:
    return []

def __init__(self, config: Config) -> None:
    self.config = config

# ❌ Bad - Missing type annotations
def process_data(name, count, options=None):
    return []
```

See `.cursorrules` for complete type annotation guidelines.

### Formatting

- **Line length**: 100 characters (enforced by Ruff)
- **Quote style**: Double quotes (enforced by Ruff)
- **Auto-formatted**: On save in VS Code, or via `make format`

### Testing

- **Run before committing**: `pytest tests/ -v` or `make test`
- **Coverage**: `pytest tests/ --cov=src --cov-report=term-missing`
- **Current status**: 138 passed, 12 skipped

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
mypy src/schemax/your_file.py

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

### Pull Requests

GitHub Actions automatically runs on every PR:
1. ✅ Ruff format check (must pass)
2. ✅ Ruff lint check (must pass)
3. ✅ Mypy type check (must pass)
4. ✅ Pytest with coverage (must pass)

**PRs are blocked if any check fails.**

### Current Status

**All checks passing:**
- ✅ Mypy: 0 errors in 29 files
- ✅ Tests: 138 passed, 12 skipped
- ✅ Ruff: All files formatted
- ✅ Pre-commit hooks: Configured and active

---

## Getting Help

- **Coding standards**: See `.cursorrules`
- **Architecture**: See the Docusaurus docs site (`docs/schemax/`) — Architecture
- **Testing**: See `TESTING.md` (root)
- **Commands**: Run `make help`
- **Provider development**: See the Docusaurus docs site (`docs/schemax/`) — Provider contract

---

**You're all set!** 🎉 Run `make all` to verify everything works.

