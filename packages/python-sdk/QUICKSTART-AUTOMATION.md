# Code Quality Automation - Quick Start

All automation is now set up! Here's how to get started:

## 🚀 One-Time Setup (5 minutes)

```bash
cd packages/python-sdk

# 1. Install dependencies (if not already done)
pip install -e ".[dev]"

# 2. Install pre-commit hooks
pre-commit install
```

That's it! You're ready to go.

## 📝 Your New Workflow

### Writing Code

Just code normally in VS Code/Cursor:
- **Format on save** is enabled (Ruff)
- **Type errors** show inline (Pylance + Mypy)
- **Linting issues** get fixed automatically

### Before Committing

Run all checks:
```bash
make all
```

Or individual checks:
```bash
make format    # Format code
make lint      # Lint code
make typecheck # Type check
make test      # Run tests
```

### Committing

Just commit normally:
```bash
git add .
git commit -m "your message"
```

Pre-commit hooks will automatically:
1. ✅ Format your code (Ruff)
2. ✅ Fix linting issues (Ruff)
3. ✅ Check types (mypy)
4. ❌ **Block commit if errors remain**

If blocked, fix the errors and commit again.

### Pull Requests

GitHub Actions automatically runs:
1. Format check (must pass)
2. Lint check (must pass)
3. Type check (must pass)
4. Tests (must pass)

**PRs are blocked if any check fails.**

## 🛠️ Troubleshooting

### "Pre-commit hook failed"

```bash
# See what failed
pre-commit run --all-files

# Fix manually
ruff check src/ tests/ --fix
mypy src/

# Try again
git commit -m "..."
```

### "Mypy type errors"

```bash
# Check errors
mypy src/

# Fix by adding type annotations (see .cursorrules)
```

### "Need help?"

```bash
# See all commands
make help

# Read setup guide
cat SETUP.md
```

## 📚 Files Created

- `.pre-commit-config.yaml` - Pre-commit hooks configuration
- `Makefile` - Convenient development commands
- `SETUP.md` - Detailed setup guide
- `pyproject.toml` - Enhanced with strict Ruff & mypy config

## 🎯 What's Enforced

✅ **Type annotations** on all functions (required)  
✅ **100 character line length** (auto-formatted)  
✅ **Double quotes** for strings (auto-formatted)  
✅ **Proper imports** (auto-organized)  
✅ **No type errors** (mypy strict mode)  
✅ **All tests pass** (168 passed, 12 skipped)

## 🚦 Current Status

**All checks passing:**
- ✅ Mypy: 0 errors in 29 files
- ✅ Tests: 168 passed, 12 skipped
- ✅ Ruff: All files formatted
- ✅ CI/CD: Ready to enforce on PRs

You're all set! Happy coding! 🎉

