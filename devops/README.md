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
  - Code formatting validation (Black for Python)
  - Smoke tests (extension build, SDK install, CLI validation)
  - GPG commit signature verification
  - Runs on push, pull requests, and manual trigger

### Scripts

- **`run-checks.sh`** - Local script to run all quality checks before committing
  - Checks Python code formatting with Black
  - Runs smoke tests
  - Returns exit code 0 if all pass, 1 if any fail

## Setup

### Local Development

1. **Install dependencies:**
   ```bash
   # Python formatting
   pip install black
   
   # Extension dependencies
   cd packages/vscode-extension && npm install
   
   # SDK dependencies
   cd packages/python-sdk && pip install -e .
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
- All Python code in `packages/python-sdk/src/`
- All Python examples in `examples/python-scripts/`
- Uses Black with 100 character line length

**Run locally:**
```bash
# Check formatting
cd packages/python-sdk
black src/ --check --line-length 100

# Fix formatting
black src/ --line-length 100
```

**CI behavior:**
- ✅ Pass: All files are properly formatted
- ❌ Fail: Files need formatting (see job logs for details)

### 2. Smoke Tests

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

### 3. GPG Commit Signatures

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
│   ├── Install Black
│   ├── Check Python SDK formatting
│   └── Check examples formatting
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
    └── Report success
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
  needs: [code-formatting, smoke-tests, new-check]
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

### Black Not Found

```bash
pip install black
# or
pip install -e "packages/python-sdk[dev]"
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
- **Smoke Test Pass Rate**: % of commits that pass all tests
- **Build Time**: Time to run all quality checks
- **Failure Rate**: % of failed CI runs

## Future Enhancements

Planned additions to the DevOps pipeline:

- [ ] Unit test suite (pytest for Python, jest for TypeScript)
- [ ] Test coverage reporting
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
	@cd packages/python-sdk && black src/ --line-length 100
	@cd examples/python-scripts && black . --line-length 100
```

Run with: `make check` or `make format`

## Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Black Documentation](https://black.readthedocs.io/)
- [Git Hooks Documentation](https://git-scm.com/docs/githooks)

---

**For questions or issues, see:** [CONTRIBUTING.md](../CONTRIBUTING.md)

