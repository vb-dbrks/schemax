# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

SchemaX is a declarative schema management toolkit for Databricks Unity Catalog. It consists of:
- **Python SDK/CLI** (`packages/python-sdk/`) — published to PyPI as `schemaxpy`
- **VS Code Extension** (`packages/vscode-extension/`) — React webview + TypeScript extension host
- **Docs site** (`docs/schemax/`) — Docusaurus

## Commands

### Setup (one-time)
```bash
# Python SDK
cd packages/python-sdk && pip install -e ".[dev]"

# Extension
cd packages/vscode-extension && npm install

# Root
npm install
```

### Root-level (Makefile)
```bash
make fmt          # Format Python + TypeScript
make lint         # Lint both
make typecheck    # mypy for Python
make test         # All tests
make test-python  # Python SDK tests only
make test-ext     # Extension Jest tests only
make ci           # Full CI check (format, lint, typecheck, tests)
make pre-commit   # Format + lint + typecheck
```

### Python SDK (`packages/python-sdk/`)
```bash
pytest tests/ -q                                              # All tests
pytest tests/unit/test_sql_generator.py -v                   # Single file
pytest tests/unit/test_sql_generator.py::TestCatalogSQL::test_add_catalog -xvs  # Single test
pytest tests/ --cov=src/schemax --cov-report=term-missing    # With coverage

ruff format src/ tests/     # Format
ruff check src/ tests/      # Lint
ruff check src/ tests/ --fix
mypy src/                   # Type check
```

### VS Code Extension (`packages/vscode-extension/`)
```bash
npm run build       # Build webview + extension
npm run watch       # Dev mode (watch)
npm test            # Jest tests
npm run lint        # ESLint
npm run format      # Prettier
npm run package     # Create VSIX
```

### Docs
```bash
cd docs/schemax && npm start   # Dev server
cd docs/schemax && npm run build
```

## Architecture

### File Format (V4 `.schemax/`)
```
.schemax/
├── project.json      # Project metadata, environments, managed/external locations, deployments
├── changelog.json    # Append-only log of ops since last snapshot (working directory)
└── snapshots/
    └── vX.Y.Z.json  # Full state snapshots (SHA-256 integrity hash)
```

**State loading**: Load latest snapshot → apply changelog ops → current state shown in UI.

### Core Concepts
- **Ops**: Immutable, UUID-identified actions (`{id, ts, op, target, payload}`). Never edited after creation.
- **Changelog**: Working set of ops cleared when a snapshot is taken.
- **Snapshots**: Point-in-time full state (`state: { catalogs: Catalog[] }`) with semantic versioning.
- **SQL Generation**: Converts changelog ops to idempotent DDL, dependency-ordered via NetworkX.

### Python SDK Layers
1. **CLI** (`cli.py`) — Click commands: `init`, `validate`, `sql`, `apply`, `rollback`, `import`, `snapshot`, `diff`, `bundle`
2. **Commands** (`commands/`) — One module per CLI command
3. **Core** (`core/`) — `storage.py` (V4 file I/O), `deployment.py` (audit trail), `version.py`
4. **Provider Architecture** (`providers/`)
   - `base/` — Abstract protocols: `provider.py`, `sql_generator.py`, `executor.py`, `state_differ.py`
   - `unity/` — Databricks Unity Catalog implementation; includes `auth.py`, `executor.py`, `safety_validator.py`, `ddl_parser.py`
   - `registry.py` — Provider registration (Unity implemented; Hive in progress)
5. **Domain** (`domain/`) — `errors.py`, `results.py`, `envelopes.py` (CLI output contracts)

### VS Code Extension Layers
1. **Extension host** (`extension.ts`) — Command registration, storage, webview lifecycle, Python backend bridge
2. **Webview UI** (`webview/`) — React 18 + Zustand (`useDesignerStore`); all state mutations go through the store → emit ops
3. **Backend client** (`backend/pythonBackendClient.ts`) — Spawns Python CLI, passes structured JSON envelopes
4. **Storage** (`storage-v4.ts`) — V4 file format, multi-environment support
5. **Providers** (`providers/`) — TypeScript mirror of Python provider architecture with Zod schemas

### Data Flow
```
UI action → Zustand store → op emitted → Python CLI invoked via pythonBackendClient
→ Provider (Unity/Hive) → Databricks API → Result envelope → UI update
```

## Code Conventions

### Python
- All functions **must** have complete type annotations (parameters + return type); enforced by mypy strict mode
- Ruff formatter, 100-char line length, double quotes
- Pydantic v2 models for all data structures
- `snake_case` variables/functions, `PascalCase` classes, `UPPER_CASE` constants
- Docstrings required for public APIs

### TypeScript
- Strict mode, no `any`
- 2-space indent, single quotes, semicolons required, 100-char line length
- Zod schemas for all data models
- `camelCase` variables/functions, `PascalCase` types/interfaces, `UPPER_CASE` constants
- Use `outputChannel.appendLine()` for logging — **never** `console.log()`
- Op IDs: always `id: \`op_${uuidv4()}\``

### Testing
- Python test markers: `unit`, `integration`, `compliance`
- SQLGlot validates all generated SQL against Databricks dialect in tests
- DDL parser requires ≥90% coverage
- `OperationBuilder` pattern used for test utilities

### Immutability
Never mutate state directly — always create new objects (both Python and TypeScript).

## CI/CD

- `python-sdk-ci.yml` — Python 3.11/3.12, Linux/Windows/macOS matrix
- `extension-ci.yml` — Lint, Jest, build, VSIX package
- `integration-tests.yml` — Manual/scheduled, requires live Databricks environment
- `publish-pypi.yml` / `publish-vscode-extension.yml` / `publish-openvsx.yml` — Triggered on release tags

Version sync: All packages share the same version. Use `scripts/bump-version.sh` and verify with `scripts/check-version-sync.sh`.
