# Contributing to SchemaX

Thank you for your interest in contributing to SchemaX! This document provides guidelines and instructions for contributing.

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

### Pull Requests

1. **Fork the repository** and create a branch from `main`
2. **Make your changes** following the coding guidelines
3. **Test thoroughly** using the testing checklist
4. **Update documentation** if needed
5. **Commit with clear messages** describing what and why
6. **Submit a pull request** with a detailed description

**Pull Request Guidelines:**
- Keep changes focused (one feature/fix per PR)
- Include tests if adding new functionality
- Update CHANGELOG.md
- Ensure all checks pass
- Respond to review feedback promptly

## Development Setup

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for detailed setup instructions.

**Quick Start:**

```bash
git clone https://github.com/vb-dbrks/schemax-vscode.git
cd schemax-vscode
npm install
npm run build
```

Press `F5` in VS Code to launch the Extension Development Host.

## Coding Standards

### TypeScript

- Use TypeScript strict mode
- Avoid `any` types when possible
- Use interfaces for public APIs
- Use type aliases for unions and complex types
- Document complex functions with JSDoc comments

### Code Style

- 2 spaces for indentation
- Semicolons required
- Single quotes for strings
- Trailing commas in multiline objects/arrays
- Max line length: 100 characters

### Naming Conventions

- `camelCase` for variables and functions
- `PascalCase` for types, interfaces, and classes
- `UPPER_CASE` for constants
- Prefix interfaces with `I` only for public APIs
- Descriptive names over abbreviations

### File Organization

```typescript
// 1. Imports (grouped)
import * as vscode from 'vscode';
import { ProjectFile } from './shared/model';

// 2. Constants
const DEFAULT_NAME = 'Untitled';

// 3. Types/Interfaces
interface Options {
  name: string;
}

// 4. Functions
export function doSomething(): void {
  // ...
}
```

### Error Handling

```typescript
// Good: Catch errors, log details, show user-friendly messages
try {
  const project = await readProject(uri);
} catch (error) {
  outputChannel.appendLine(`[SchemaX] ERROR: ${error}`);
  vscode.window.showErrorMessage('Failed to load project. See output for details.');
}

// Bad: Silent failures or generic errors
try {
  const project = await readProject(uri);
} catch (error) {
  // No logging or user feedback
}
```

### Logging

```typescript
// Extension code - use output channel
outputChannel.appendLine('[SchemaX] Loading project...');

// Webview code - use console (development only)
console.log('[SchemaX Webview] Component mounted');
```

## Testing

### Manual Testing Checklist

Before submitting a PR, verify:

- [ ] Extension activates without errors
- [ ] Designer opens successfully
- [ ] Can create catalogs, schemas, tables
- [ ] Can add and edit columns
- [ ] Column inline editing works (name, type, nullable, comment)
- [ ] Can reorder columns via drag-and-drop
- [ ] Can rename objects via sidebar
- [ ] Can delete objects
- [ ] Snapshots create successfully
- [ ] Snapshot panel shows version history
- [ ] Changelog clears after snapshot
- [ ] Files persist after closing and reopening
- [ ] No errors in SchemaX output log
- [ ] No errors in webview console

### Testing on Different Platforms

If possible, test on:
- macOS
- Windows
- Linux

### Testing with Different VS Code Versions

Test with:
- Latest stable VS Code
- VS Code Insiders (if making API changes)

## Documentation

### Code Documentation

- Add JSDoc comments for public functions
- Document complex algorithms inline
- Explain non-obvious design decisions
- Include examples for complex APIs

### User Documentation

When adding features, update:
- `README.md` - User-facing features
- `docs/DEVELOPMENT.md` - Developer setup/workflow
- `docs/ARCHITECTURE.md` - Technical design
- `CHANGELOG.md` - Version history

### Writing Style

- Use clear, concise language
- Write in present tense ("Returns the project" not "Will return")
- Use active voice ("The function parses" not "The data is parsed")
- Avoid jargon unless necessary
- Include code examples where helpful

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <description>

[optional body]

[optional footer]
```

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting, no logic change)
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `test:` Adding or updating tests
- `chore:` Build process, tooling, dependencies

**Examples:**

```
feat: add support for table constraints

Implements CHECK and FOREIGN KEY constraints in the table designer.
Constraints are stored in the table model and can be edited in the
properties panel.

Closes #42
```

```
fix: snapshot panel not showing latest version

The panel was using the first snapshot instead of the last one.
Changed to use latestSnapshot from project metadata.

Fixes #58
```

## Branching Strategy

- `main` - Stable release branch
- `develop` - Integration branch for next release
- `feature/*` - New features
- `fix/*` - Bug fixes
- `docs/*` - Documentation updates

## Review Process

1. All PRs require at least one approval
2. Automated checks must pass
3. Code review focuses on:
   - Correctness and logic
   - Code quality and maintainability
   - Test coverage
   - Documentation completeness
   - Performance implications

## Release Process

Releases are managed by maintainers:

1. Update version in `package.json`
2. Update `CHANGELOG.md`
3. Create git tag: `git tag v0.1.0`
4. Push tag: `git push origin v0.1.0`
5. Build VSIX: `npm run package`
6. Create GitHub release with VSIX artifact
7. Publish to VS Code Marketplace

## Getting Help

- **Questions?** Open a GitHub discussion
- **Stuck?** Check [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Bugs?** File an issue with details
- **Feature ideas?** Open an issue for discussion

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition

Contributors are recognized in:
- GitHub contributors list
- Release notes for significant contributions
- Special mentions in README for major features

Thank you for contributing to SchemaX!

