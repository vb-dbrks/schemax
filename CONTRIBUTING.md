# Contributing to SchemaX

Thank you for your interest in contributing to SchemaX! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Code Formatting](#code-formatting)
- [Coding Standards](#coding-standards)
- [Documentation Standards](#documentation-standards)
- [Testing](#testing)
- [Commit Messages](#commit-messages)
- [Review Process](#review-process)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates.

**When filing a bug report, include:**
- VS Code version
- Operating system and version
- SchemaX version
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- SchemaX output logs (View → Output → SchemaX)
- Webview console errors (if applicable)

### Suggesting Features

We welcome feature suggestions! Please open an issue with:
- Clear description of the feature
- Use case and motivation
- Expected behavior
- Examples of similar features in other tools (if applicable)

### Contributing a Provider

SchemaX uses a provider-based architecture to support multiple data catalogs. Contributing a provider is a significant contribution!

**Before Starting:**
1. Read [docs/PROVIDER_CONTRACT.md](docs/PROVIDER_CONTRACT.md) - Provider interface specification
2. Read [docs/DEVELOPMENT.md Provider Development](docs/DEVELOPMENT.md#provider-development) - Step-by-step guide
3. Review Unity provider as reference implementation

**Provider Workflow:**
1. Open issue using "New Provider Proposal" template (if available)
2. Fork repository and create branch: `feature/provider-{name}`
3. Implement provider in TypeScript and Python:
   - Define models and hierarchy
   - Implement operations
   - Create state reducer
   - Build SQL generator
   - Register provider
4. Write tests (minimum 80% coverage required)
5. Update documentation
6. Submit PR with provider checklist complete

**Required for Provider PRs:**
- All provider interface methods implemented
- Provider registered in registry
- Compliance tests pass
- 80% code coverage minimum
- All operations tested
- SQL generator tested
- Documentation complete
- Examples provided

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md#provider-development) for complete guide.

### Pull Requests

1. **Fork the repository** and create a branch from `main`
2. **Make your changes** following the coding guidelines
3. **Format your code** (see [Code Formatting](#code-formatting))
4. **Run quality checks** (see [Quality Checks](#quality-checks))
5. **Test thoroughly** using the testing checklist
6. **Update documentation** (see [Documentation Standards](#documentation-standards))
7. **Sign your commits** with GPG (see [Commit Signing](#commit-signing))
8. **Commit with clear messages** (see [Commit Messages](#commit-messages))
9. **Submit a pull request** with a detailed description

**Pull Request Guidelines:**
- Keep changes focused (one feature/fix per PR)
- Include tests if adding new functionality
- Update CHANGELOG.md (extension only)
- Ensure all checks pass (CI will verify)
- Sign all commits with GPG
- Respond to review feedback promptly

## Development Setup

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for detailed setup instructions.

### Quick Start

**Clone and Setup:**
```bash
git clone https://github.com/vb-dbrks/schemax-vscode.git
cd schemax-vscode
```

**VS Code Extension:**
```bash
cd packages/vscode-extension
npm install
npm run build
# Press F5 in VS Code to launch Extension Development Host
```

**Python SDK:**
```bash
cd packages/python-sdk
pip install -e ".[dev]"
schemax --version
```

## Code Formatting

We use automated code formatters to maintain consistent code style. **Always format your code before committing.**

### Python - Black

We use [Black](https://black.readthedocs.io/) with 100 character line length.

**Install:**
```bash
pip install black
```

**Format all Python code:**
```bash
# From project root
cd packages/python-sdk
black src/ --line-length 100

# Format examples too
cd ../../examples/python-scripts
black . --line-length 100
```

**Check formatting without changes:**
```bash
black src/ --check --line-length 100
```

**Configure in pyproject.toml:**
```toml
[tool.black]
line-length = 100
target-version = ['py39', 'py310', 'py311', 'py312']
```

### TypeScript - Prettier (Optional)

While we don't enforce Prettier, we recommend these settings:

```json
{
  "semi": true,
  "trailingComma": "es5",
  "singleQuote": true,
  "printWidth": 100,
  "tabWidth": 2
}
```

**Manual formatting guidelines:**
- 2 spaces for indentation
- Semicolons required
- Single quotes for strings
- Max line length: 100 characters

### Pre-commit Hook (Recommended)

Use the DevOps quality checks in your pre-commit hook:

Create `.git/hooks/pre-commit`:

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

Make executable:
```bash
chmod +x .git/hooks/pre-commit
```

## Quality Checks

Before submitting a PR, run all quality checks:

```bash
./devops/run-checks.sh
```

This script runs:
1. **Code Formatting Check** - Validates Python code with Black
2. **Smoke Tests** - Verifies extension builds and SDK installs

**CI Pipeline** will automatically run these checks plus:
- GPG commit signature verification
- All checks must pass before merge

See [devops/README.md](devops/README.md) for details.

## Coding Standards

### TypeScript

**General Principles:**
- Use TypeScript strict mode
- Avoid `any` types when possible
- Use interfaces for public APIs
- Use type aliases for unions and complex types
- Document complex functions with JSDoc comments

**Code Style:**
```typescript
// ✅ Good
import * as vscode from 'vscode';
import { Provider } from './providers/base/provider';
import { UnityState } from './providers/unity/models';

interface Options {
  name: string;
  version?: string;
}

export async function loadProject(uri: vscode.Uri, options?: Options): Promise<ProjectFile> {
  const name = options?.name ?? 'default';
  try {
    const content = await fs.readFile(path.join(uri.fsPath, '.schemax', 'project.json'));
    return JSON.parse(content);
  } catch (error) {
    outputChannel.appendLine(`[SchemaX] ERROR: Failed to load project: ${error}`);
    throw new Error(`Failed to load project: ${error}`);
  }
}

// ❌ Bad
export function loadProject(uri: any) {
  const content = fs.readFileSync(uri + '/.schemax/project.json');
  return JSON.parse(content); // No error handling
}
```

**Naming Conventions:**
- `camelCase` for variables and functions
- `PascalCase` for types, interfaces, and classes
- `UPPER_CASE` for constants
- Descriptive names over abbreviations

**Error Handling:**
```typescript
// ✅ Good: Log details, show user-friendly message
try {
  const result = await dangerousOperation();
} catch (error) {
  outputChannel.appendLine(`[SchemaX] ERROR: ${error}`);
  if (error instanceof Error) {
    outputChannel.appendLine(`[SchemaX] Stack: ${error.stack}`);
  }
  vscode.window.showErrorMessage('Operation failed. See SchemaX output for details.');
}

// ❌ Bad: Silent failure
try {
  const result = await dangerousOperation();
} catch {}
```

**Logging:**
```typescript
// Extension code - use output channel (NOT console.log)
outputChannel.appendLine('[SchemaX] Loading project...');
outputChannel.appendLine(`[SchemaX] Loaded ${catalogs.length} catalogs`);

// Webview code - use console (development only)
console.log('[SchemaX Webview] Component mounted');
```

### Python

**General Principles:**
- Follow PEP 8 (enforced by Black)
- Use type hints for all function signatures
- Use Pydantic models for data structures
- Document public APIs with docstrings
- Raise exceptions with clear messages

**Code Style:**
```python
# ✅ Good
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel

class Table(BaseModel):
    id: str
    name: str
    columns: List[Column] = []

def load_project(workspace: Path, validate: bool = True) -> ProjectFile:
    """Load project from .schemax/project.json.
    
    Args:
        workspace: Path to workspace directory
        validate: Whether to validate the schema (default: True)
        
    Returns:
        ProjectFile: Loaded and validated project
        
    Raises:
        FileNotFoundError: If project.json doesn't exist
        ValidationError: If validation fails
    """
    project_path = workspace / ".schemax" / "project.json"
    if not project_path.exists():
        raise FileNotFoundError(f"Project file not found: {project_path}")
    
    content = project_path.read_text()
    data = json.loads(content)
    return ProjectFile(**data)

# ❌ Bad
def load_project(workspace):
    return json.loads(open(workspace + "/.schemax/project.json").read())
```

**Naming Conventions:**
- `snake_case` for variables, functions, and methods
- `PascalCase` for classes
- `UPPER_CASE` for constants
- Descriptive names over abbreviations

**Error Handling:**
```python
# ✅ Good: Specific exceptions with context
try:
    project = load_project(workspace)
except FileNotFoundError as e:
    console.print(f"[red]✗ Error:[/red] Project file not found: {e}", err=True)
    sys.exit(1)
except ValidationError as e:
    console.print(f"[red]✗ Validation error:[/red] {e}", err=True)
    sys.exit(1)

# ❌ Bad: Bare except with generic message
try:
    project = load_project(workspace)
except:
    print("Error")
```

## Documentation Standards

### Documentation Structure

We maintain a clean, focused documentation structure. **Do not create redundant documentation files.**

**Current Structure:**
```
schemax-vscode/
├── README.md                    # Project overview
├── TESTING.md                   # Testing guide
├── CONTRIBUTING.md              # This file
├── docs/
│   ├── README.md               # Documentation index
│   ├── QUICKSTART.md           # Getting started
│   ├── ARCHITECTURE.md         # Technical design
│   └── DEVELOPMENT.md          # Development guide
└── packages/
    ├── vscode-extension/README.md    # Extension docs
    └── python-sdk/README.md          # SDK docs
```

### When to Update Documentation

**Feature added?** → Update:
- `README.md` (if user-facing)
- `docs/QUICKSTART.md` (if affects usage)
- Relevant package README
- `CHANGELOG.md` (extension only)

**Architecture changed?** → Update:
- `docs/ARCHITECTURE.md`

**Build process changed?** → Update:
- `docs/DEVELOPMENT.md`

**New tests added?** → Update:
- `TESTING.md`

### Documentation Best Practices

**✅ DO:**
- Keep documentation in sync with code
- Use clear, concise language
- Include code examples
- Add cross-references to related docs
- Test all code examples
- Use proper markdown formatting
- Keep one source of truth per topic

**❌ DON'T:**
- Create duplicate documentation
- Write outdated examples
- Use jargon without explanation
- Create "status" or "summary" files
- Leave broken links
- Mix multiple topics in one file

### Writing Style

```markdown
# ✅ Good: Clear, actionable, with examples

## Adding a Column

To add a column to a table:

1. Select the table in the sidebar
2. Click "Add Column"
3. Fill in the column details:
   - **Name**: Column identifier (e.g., `customer_id`)
   - **Type**: Databricks data type (e.g., `BIGINT`)
   - **Nullable**: Whether NULL values are allowed

Example:
\`\`\`typescript
const column: Column = {
  id: 'col_001',
  name: 'customer_id',
  type: 'BIGINT',
  nullable: false
};
\`\`\`

# ❌ Bad: Vague, no examples

## Columns

You can add columns. Set the properties as needed.
```

### Code Documentation

**TypeScript - JSDoc:**
```typescript
/**
 * Generates SQL DDL statements from a list of operations.
 * 
 * @param ops - List of operations to convert to SQL
 * @returns SQL script with idempotent DDL statements
 * 
 * @example
 * ```typescript
 * const generator = new SQLGenerator(state);
 * const sql = generator.generateSQL(changelog.ops);
 * ```
 */
generateSQL(ops: Op[]): string {
  // ...
}
```

**Python - Docstrings:**
```python
def generate_sql(self, ops: List[Op]) -> str:
    """Generate SQL DDL statements from operations.
    
    Creates idempotent DDL statements that can be safely executed
    multiple times without causing errors.
    
    Args:
        ops: List of operations to convert to SQL
        
    Returns:
        str: SQL script with DDL statements
        
    Example:
        >>> generator = SQLGenerator(state)
        >>> sql = generator.generate_sql(changelog.ops)
        >>> print(sql)
        CREATE CATALOG IF NOT EXISTS `main`;
    """
```

## Testing

### Before Submitting PR

**Run these tests:**

1. **Smoke Test:**
   ```bash
   ./scripts/smoke-test.sh
   ```

2. **Extension Build:**
   ```bash
   cd packages/vscode-extension
   npm run build
   ```

3. **Python Tests:**
   ```bash
   cd packages/python-sdk
   pip install -e .
   schemax validate examples/basic-schema/
   ```

4. **Manual Testing:**
   - Press F5, test in Extension Development Host
   - Try all modified features
   - Check SchemaX output logs
   - Verify no console errors

### Manual Testing Checklist

**Core Functionality:**
- [ ] Extension activates without errors
- [ ] Designer opens successfully
- [ ] Can create catalogs, schemas, tables
- [ ] Can add and edit columns
- [ ] Column inline editing works
- [ ] Can reorder columns via drag-and-drop
- [ ] Can rename objects
- [ ] Can delete objects
- [ ] Snapshots create successfully
- [ ] SQL generation works
- [ ] Files persist correctly

**Quality Checks:**
- [ ] No errors in SchemaX output
- [ ] No console errors in webview
- [ ] No linter errors
- [ ] Code is formatted (Black for Python)
- [ ] Documentation updated

### Testing on Different Platforms

If possible, test on:
- macOS
- Windows
- Linux

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting)
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `test:` Adding or updating tests
- `chore:` Build process, tooling, dependencies
- `ci:` CI/CD changes

**Scopes (optional):**
- `extension:` VS Code extension changes
- `sdk:` Python SDK changes
- `cli:` CLI changes
- `docs:` Documentation changes
- `examples:` Example code changes

**Examples:**

```
feat(extension): add SQL generation command

Implements the "Generate SQL Migration" command that exports
changelog operations to SQL DDL files.

- Adds sql-generator.ts module
- Registers schemax.generateSQL command
- Saves SQL to .schemax/migrations/ directory
- Opens generated file in editor

Closes #42
```

```
fix(sdk): correct SQL escaping for single quotes

Single quotes in comments were not being escaped correctly,
causing SQL syntax errors. Fixed by replacing ' with ''.

Fixes #58
```

```
docs: consolidate and clean up documentation

Removed 6 redundant documentation files and consolidated
content into clear, focused documents.

- Removed duplicate quick start guides
- Consolidated into docs/QUICKSTART.md
- Created docs/README.md as index
- Updated all cross-references
```

```
style(sdk): format Python code with Black

Ran Black formatter on all Python source files with
100 character line length.
```

## Commit Signing

**All commits must be signed with GPG.**

### Why Commit Signing?

- Verifies commit authenticity
- Ensures code integrity
- Required by CI pipeline
- Best practice for security

### Setup GPG Signing

**1. Generate GPG key (if you don't have one):**

```bash
gpg --full-generate-key
# Choose RSA and RSA
# Key size: 4096 bits
# Expiration: Your preference
# Enter your name and email
```

**2. List your GPG keys:**

```bash
gpg --list-secret-keys --keyid-format=long
```

Output:
```
sec   rsa4096/ABC123DEF456 2025-01-01 [SC]
uid                 [ultimate] Your Name <your.email@example.com>
```

**3. Configure Git to use GPG:**

```bash
# Set your GPG key (use the ID from above, e.g., ABC123DEF456)
git config --global user.signingkey ABC123DEF456

# Enable automatic signing
git config --global commit.gpgsign true
git config --global tag.gpgsign true
```

**4. Add GPG key to GitHub:**

```bash
# Export your public key
gpg --armor --export ABC123DEF456

# Copy the output (starts with -----BEGIN PGP PUBLIC KEY BLOCK-----)
```

Go to GitHub → Settings → SSH and GPG keys → New GPG key → Paste

**5. Verify signing works:**

```bash
git commit -m "test: verify gpg signing" --allow-empty
git log --show-signature -1
```

You should see "Good signature from..." in the output.

### Troubleshooting GPG

**Error: "gpg failed to sign the data"**

```bash
# Test GPG
echo "test" | gpg --clearsign

# If it asks for passphrase in terminal, configure GPG agent
export GPG_TTY=$(tty)
echo 'export GPG_TTY=$(tty)' >> ~/.bashrc  # or ~/.zshrc
```

**Error: "No secret key"**

```bash
# Verify key exists
gpg --list-secret-keys

# Reconfigure Git with correct key ID
git config --global user.signingkey YOUR_KEY_ID
```

**macOS: "Inappropriate ioctl for device"**

```bash
# Add to ~/.zshrc or ~/.bashrc
export GPG_TTY=$(tty)

# Use pinentry-mac
brew install pinentry-mac
echo "pinentry-program /opt/homebrew/bin/pinentry-mac" >> ~/.gnupg/gpg-agent.conf
gpgconf --kill gpg-agent
```

### Signing Existing Commits

If you forgot to sign commits:

```bash
# Amend last commit with signature
git commit --amend --no-edit -S

# Rebase and sign multiple commits
git rebase --exec 'git commit --amend --no-edit -n -S' -i HEAD~5

# Force push (if already pushed)
git push --force-with-lease
```

## Branching Strategy

- `main` - Stable release branch
- `develop` - Integration branch for next release (if needed)
- `feature/<name>` - New features
- `fix/<name>` - Bug fixes
- `docs/<name>` - Documentation updates
- `refactor/<name>` - Code refactoring

**Branch Naming:**
```bash
# Good
feature/add-dab-generation
fix/sql-escaping-quotes
docs/update-quickstart
refactor/simplify-state-reducer

# Bad
feature
my-changes
update
```

## Review Process

### Pull Request Review

All PRs require at least one approval. Reviewers check:

**Code Quality:**
- [ ] Code follows style guidelines
- [ ] Code is properly formatted
- [ ] No linter errors
- [ ] Proper error handling
- [ ] Appropriate logging

**Functionality:**
- [ ] Feature works as described
- [ ] No regressions
- [ ] Edge cases handled
- [ ] Performance is acceptable

**Documentation:**
- [ ] Code is documented
- [ ] User docs updated
- [ ] Examples provided
- [ ] CHANGELOG updated (if applicable)

**Testing:**
- [ ] Manual testing performed
- [ ] Tests pass
- [ ] No console errors

### Addressing Review Feedback

- Respond to all comments
- Make requested changes
- Push updates to the same branch
- Mark conversations as resolved when fixed
- Request re-review when ready

## Release Process

Releases are managed by maintainers:

### VS Code Extension

1. Update version in `packages/vscode-extension/package.json`
2. Update `packages/vscode-extension/CHANGELOG.md`
3. Build: `npm run build`
4. Create git tag: `git tag vscode-v0.1.0`
5. Push tag: `git push origin vscode-v0.1.0`
6. Build VSIX: `npm run package`
7. Create GitHub release
8. Publish to VS Code Marketplace: `vsce publish`

### Python SDK

1. Update version in `packages/python-sdk/pyproject.toml`
2. Build: `python -m build`
3. Create git tag: `git tag py-v0.1.0`
4. Push tag: `git push origin py-v0.1.0`
5. Create GitHub release
6. Publish to PyPI: `python -m twine upload dist/*`

## Project-Specific Guidelines

### Operation Types

When adding new operation types to a provider:

1. **Define in provider's operations.ts:**
   ```typescript
   // packages/vscode-extension/src/providers/unity/operations.ts
   export const UNITY_OPERATIONS = [
     'add_column',
     'your_new_op',
     // ...
   ] as const;
   ```

2. **Update Python provider's ops:**
   ```python
   # packages/python-sdk/src/schemax/providers/unity/operations.py
   UNITY_OPERATIONS = [
       "add_column",
       "your_new_op",
       # ...
   ]
   ```

3. **Implement state reducer in provider:**
   - TypeScript: `providers/unity/state-reducer.ts` → `applyOperation()`
   - Python: `providers/unity/state_reducer.py` → `apply_operation()`

4. **Add SQL generation in provider:**
   - TypeScript: `providers/unity/sql-generator.ts` → add new method
   - Python: `providers/unity/sql_generator.py` → add new method

5. **Update documentation:**
   - `.cursorrules` - Add to operation list
   - `docs/ARCHITECTURE.md` - If architecture impact

### State Management

- State must be immutable
- Always create new objects when modifying
- Use structured cloning or spread operators
- Never mutate function parameters

### File Format

- All `.schemax/` files are JSON
- Use consistent indentation (2 spaces)
- Validate with Zod (TS) or Pydantic (Python)
- Include version fields for compatibility

## Getting Help

- **Questions?** Open a GitHub discussion
- **Stuck?** Check [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Bugs?** File an issue with details
- **Feature ideas?** Open an issue for discussion

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition

Contributors are recognized in:
- [GitHub contributors list](https://github.com/vb-dbrks/schemax/graphs/contributors)
- Release notes for significant contributions
- Special mentions in README for major features

---

**Thank you for contributing to SchemaX! 🚀**
