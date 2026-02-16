# DevOps

This directory contains CI/CD pipelines and DevOps automation scripts for SchemaX.

## Quick Start

Run all quality checks locally:

```bash
./devops/run-checks.sh
```

## Contents

### Pipelines

- **`pipelines/quality-checks.yml`** - GitHub Actions workflow for automated quality checks
  - Code formatting validation (Black/Ruff for Python)
  - Python SDK tests (pytest with 136 tests + SQLGlot validation)
  - Smoke tests (extension build, SDK install, CLI validation)
  - GPG commit signature verification
  - Runs on push, pull requests, and manual trigger

### Scripts

- **`run-checks.sh`** - Local script to run all quality checks before committing
  - Checks Python code formatting with Black
  - Lints Python code with Ruff
  - Runs Python SDK tests (pytest)
  - Runs smoke tests
  - Returns exit code 0 if all pass, 1 if any fail

## Setup

### Local Development

1. **Install dependencies:**
   ```bash
   # Python formatting and linting
   pip install black ruff
   
   # Extension dependencies
   cd packages/vscode-extension && npm install
   
   # SDK dependencies with dev tools (includes pytest, SQLGlot)
   cd packages/python-sdk && pip install -e ".[dev]"
   ```

2. **Run checks before committing:**
   ```bash
   ./devops/run-checks.sh
   ```

### GitHub Actions

The `quality-checks.yml` pipeline is configured to run automatically:

- **On push** to `main` or `develop` branches
- **On pull requests** to `main` or `develop` branches
- **Manually** via workflow_dispatch

**To deploy:**

```bash
# Copy to GitHub workflows directory
cp devops/pipelines/quality-checks.yml .github/workflows/
git add .github/workflows/quality-checks.yml
git commit -m "ci: add quality checks pipeline"
git push
```

## Quality Checks

### 1. Code Formatting

**What it checks:**
- All Python code in `packages/python-sdk/src/` and `packages/python-sdk/tests/`
- All Python examples in `examples/python-scripts/`
- Uses Black with 100 character line length
- Uses Ruff for linting

**Run locally:**
```bash
# Check formatting
cd packages/python-sdk
black src/ tests/ --check --line-length 100
# Or use Ruff format (faster)
ruff format src/ tests/ --check

# Fix formatting
black src/ tests/ --line-length 100
# Or
ruff format src/ tests/

# Check linting
ruff check src/ tests/

# Fix linting issues
ruff check src/ tests/ --fix
```

**CI behavior:**
- ✅ Pass: All files are properly formatted and linted
- ❌ Fail: Files need formatting or have lint errors (see job logs for details)

### 2. Python SDK Tests

**What it checks:**
- 136 pytest tests covering all SDK functionality
- Unit tests for storage, state reducer, SQL generator, provider system
- Integration tests for complete workflows
- SQL validation using SQLGlot for Databricks dialect
- Code coverage reporting

**Run locally:**
```bash
cd packages/python-sdk

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/schemax --cov-report=term-missing

# Run specific test file
pytest tests/unit/test_sql_generator.py -v

# Run specific test
pytest tests/unit/test_sql_generator.py::TestCatalogSQL::test_add_catalog -v
```

**CI behavior:**
- ✅ Pass: All 124 tests pass (12 skipped for unimplemented features)
- ❌ Fail: One or more tests failed (see job logs for details)
- Coverage reports uploaded to Codecov

### 3. Smoke Tests

**What it checks:**
1. VS Code extension builds successfully
2. Python SDK installs without errors
3. CLI commands are available
4. Example project validates
5. File structure is correct

**Run locally:**
```bash
./scripts/smoke-test.sh
```

**CI behavior:**
- ✅ Pass: All smoke tests pass
- ❌ Fail: One or more tests failed (see job logs for details)

### 4. GPG Commit Signatures

**What it checks:**
- All commits in PR/push are signed with GPG
- Signature is valid (not expired or revoked)
- Ensures code authenticity and integrity

**Setup locally:**

See [CONTRIBUTING.md - Commit Signing](../CONTRIBUTING.md#commit-signing) for detailed setup.

Quick setup:
```bash
# Generate GPG key
gpg --full-generate-key

# Configure Git
git config --global commit.gpgsign true

# Test
git commit --allow-empty -m "test: gpg signing"
git log --show-signature -1
```

**CI behavior:**
- ✅ Pass: All commits are properly signed
- ❌ Fail: One or more commits are unsigned or have invalid signatures

## Pipeline Architecture

```
Quality Checks Pipeline
├── code-formatting job
│   ├── Install Black + Ruff
│   ├── Check Python SDK formatting (src/ + tests/)
│   ├── Check examples formatting
│   └── Run Ruff linting
│
├── python-tests job
│   ├── Install Python + UV
│   ├── Install SDK with dev dependencies (pytest, SQLGlot)
│   ├── Run 136 pytest tests
│   └── Upload coverage to Codecov
│
├── smoke-tests job
│   ├── Install Node.js + Python
│   ├── Install dependencies
│   └── Run smoke test script
│
├── commit-signatures job
│   ├── Fetch git history
│   ├── Install GPG
│   ├── Check all commits in PR/push
│   └── Verify signatures are valid
│
└── all-checks job (needs: all above)
    └── Report success (4 jobs)
```

## Adding New Checks

### Add to Local Script

Edit `devops/run-checks.sh`:

```bash
# Add new check section
echo "3️⃣  Running new check..."
echo "--------------------------------------"

if your-new-check-command; then
    echo -e "${GREEN}✓${NC} New check passed"
else
    echo -e "${RED}✗${NC} New check failed"
    FAILURES=$((FAILURES + 1))
fi
```

### Add to CI Pipeline

Edit `devops/pipelines/quality-checks.yml`:

```yaml
new-check:
  name: New Check
  runs-on: ubuntu-latest
  
  steps:
    - uses: actions/checkout@v4
    
    - name: Run new check
      run: your-new-check-command
```

Update `all-checks` job needs:
```yaml
all-checks:
  needs: [code-formatting, python-tests, smoke-tests, new-check]
```

## Pre-commit Hook

Automatically run checks before each commit:

**Create `.git/hooks/pre-commit`:**

```bash
#!/bin/bash

echo "Running quality checks..."
./devops/run-checks.sh

if [ $? -ne 0 ]; then
    echo ""
    echo "Quality checks failed. Commit aborted."
    echo "Fix the issues or use --no-verify to skip checks."
    exit 1
fi

echo "Quality checks passed. Proceeding with commit."
```

**Make executable:**
```bash
chmod +x .git/hooks/pre-commit
```

## Troubleshooting

### Black/Ruff Not Found

```bash
pip install black ruff
# or install all dev dependencies
cd packages/python-sdk
pip install -e ".[dev]"
```

### Pytest Tests Fail

```bash
# Ensure dev dependencies are installed
cd packages/python-sdk
pip install -e ".[dev]"

# Run tests with verbose output
pytest tests/ -v

# Run specific failing test
pytest tests/unit/test_sql_generator.py -xvs
```

### Smoke Test Fails

```bash
# Clean and rebuild
rm -rf packages/vscode-extension/dist
rm -rf packages/vscode-extension/node_modules
cd packages/vscode-extension
npm install
npm run build

# Reinstall SDK
pip uninstall schemax-py
cd packages/python-sdk
pip install -e .
```

### CI Pipeline Not Running

1. Ensure `.github/workflows/quality-checks.yml` exists
2. Check GitHub Actions tab for errors
3. Verify YAML syntax with `yamllint`

### Permission Denied

```bash
chmod +x devops/run-checks.sh
chmod +x scripts/smoke-test.sh
```

## Best Practices

### Before Committing

Always run:
```bash
./devops/run-checks.sh
```

### Before Pull Request

1. Run local checks
2. Ensure all tests pass
3. Fix any formatting issues
4. Check CI pipeline results

### When CI Fails

1. Check the failed job logs
2. Run the same checks locally
3. Fix the issues
4. Push the fixes
5. Wait for CI to re-run

## Metrics

Track these metrics from CI runs:

- **Code Formatting Pass Rate**: % of commits with properly formatted code
- **Test Pass Rate**: % of commits where all 136 pytest tests pass
- **Test Coverage**: Code coverage percentage (tracked via Codecov)
- **Smoke Test Pass Rate**: % of commits that pass all smoke tests
- **Build Time**: Time to run all quality checks
- **Failure Rate**: % of failed CI runs

Current Status:
- ✅ 124 passing pytest tests
- ⏭️ 12 skipped tests (features in development)
- ✅ SQLGlot validation integrated
- ✅ Code coverage reporting enabled

## Future Enhancements

Planned additions to the DevOps pipeline:

- [x] ~~Unit test suite (pytest for Python)~~ ✅ **Completed**
- [x] ~~Test coverage reporting~~ ✅ **Completed**
- [ ] Jest test suite for TypeScript/VS Code extension
- [ ] Performance benchmarks
- [ ] Security scanning (dependency vulnerabilities)
- [ ] Automated changelog generation
- [ ] Release automation
- [ ] Docker containerization
- [ ] Multi-platform testing (macOS, Windows, Linux)

## Integration with Other Tools

### VS Code

Add to `.vscode/tasks.json`:

```json
{
  "label": "Run Quality Checks",
  "type": "shell",
  "command": "./devops/run-checks.sh",
  "problemMatcher": [],
  "group": {
    "kind": "test",
    "isDefault": true
  }
}
```

Run with: `Cmd+Shift+P` → "Run Task" → "Run Quality Checks"

### Make

Add to `Makefile`:

```makefile
.PHONY: check
check:
	@./devops/run-checks.sh

.PHONY: format
format:
	@cd packages/python-sdk && ruff format src/ tests/
	@cd examples/python-scripts && black . --line-length 100

.PHONY: test
test:
	@cd packages/python-sdk && pytest tests/ -v

.PHONY: coverage
coverage:
	@cd packages/python-sdk && pytest tests/ --cov=src/schemax --cov-report=html
	@open packages/python-sdk/htmlcov/index.html
```

Run with: `make check`, `make format`, `make test`, or `make coverage`

## Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Black Documentation](https://black.readthedocs.io/)
- [Git Hooks Documentation](https://git-scm.com/docs/githooks)

---

**For questions or issues, see:** [CONTRIBUTING.md](../CONTRIBUTING.md)

