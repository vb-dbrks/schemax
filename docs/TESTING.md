# Testing Guide

This document describes how to run tests for the Schematic project.

## Overview

Schematic uses different testing frameworks for different components:
- **Python SDK**: pytest with coverage reporting (306 tests)
- **VS Code Extension**: Jest with React Testing Library
- **Integration**: End-to-end workflow tests

## Python SDK Testing

### Prerequisites

```bash
cd packages/python-sdk
pip install -e ".[dev]"
# Or use UV (faster):
# uv pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests (fast)
pytest tests/ -q

# Run with verbose output
pytest tests/ -v

# Run specific test file
pytest tests/unit/test_sql_generator.py -v

# Run specific test
pytest tests/unit/test_sql_generator.py::TestCatalogSQL::test_add_catalog -xvs

# Run with coverage
pytest tests/ --cov=src/schematic --cov-report=term-missing

# Run with coverage HTML report
pytest tests/ --cov=src/schematic --cov-report=html
open htmlcov/index.html
```

### Test Structure

```
packages/python-sdk/tests/
├── unit/                          # Unit tests
│   ├── test_storage_v4.py        # V4 storage layer tests
│   ├── test_sql_generator.py     # SQL generation tests
│   ├── test_state_reducer.py     # State reducer tests
│   ├── test_state_differ.py      # State differ tests (including views)
│   ├── test_catalog_mapping.py   # Catalog name mapping tests
│   ├── test_unity_executor.py    # Databricks executor tests
│   ├── test_deployment_tracker.py # Deployment tracking tests
│   ├── test_dependency_graph.py  # Dependency graph & cycle detection tests
│   └── test_apply_command.py     # Apply command tests (interactive/non-interactive)
├── integration/                   # Integration tests
│   ├── test_workflows.py         # End-to-end workflow tests
│   └── test_view_dependencies.py # View dependency and FQN qualification tests
├── providers/                     # Provider-specific tests
│   └── base/
│       └── test_hierarchy.py     # Hierarchy tests
├── fixtures/                      # Test fixtures
└── utils/                         # Test utilities
    └── operation_builders.py     # Operation builder helpers
```

### Current Status

- ✅ **Python SDK**: 306 tests passing (12 skipped)
- ✅ **VS Code Extension**: 25 Jest tests passing
- ✅ **SQLGlot validation** integrated for SQL syntax checking
- ✅ **Dependency graph tests** with cycle detection and topological sorting
- ✅ **View support tests** including FQN qualification regression tests
- ✅ **State differ generic tests** to catch missing object type implementations
- ✅ **Code coverage reporting** enabled for both Python and TypeScript
- ✅ **Non-interactive mode tests** for CI/CD compatibility

### Apply Command Tests

The `test_apply_command.py` test suite ensures the `schematic apply` command works correctly in both interactive and non-interactive modes. This is critical for CI/CD pipelines where user prompts would cause the command to hang.

**Tests included:**
1. ✅ `test_noninteractive_mode_auto_creates_snapshot` - Verifies snapshot auto-creation without prompting
2. ✅ `test_interactive_mode_prompts_for_snapshot` - Verifies user prompt with 3 choices (create/continue/abort)
3. ✅ `test_interactive_mode_create_snapshot` - Verifies snapshot creation when user chooses "create"
4. ✅ `test_sql_preview_noninteractive_skips_prompt` - Verifies SQL preview doesn't prompt in CI/CD mode
5. ✅ `test_workspace_without_uncommitted_ops` - Verifies no prompts when changelog is empty

**Why these tests matter:**
- Prevents regressions that would break CI/CD pipelines
- Ensures `--no-interaction` flag is respected throughout the command
- Validates that interactive mode provides proper user choices
- Guarantees the command never hangs waiting for input in automated environments

**Running apply command tests:**
```bash
# Run all apply command tests
pytest tests/unit/test_apply_command.py -v

# Run specific test
pytest tests/unit/test_apply_command.py::TestApplyCommand::test_noninteractive_mode_auto_creates_snapshot -xvs
```

### View Dependency Tests

The view test suites ensure dependency-aware SQL generation works correctly, including FQN qualification and batching optimizations.

**Integration Tests** (`test_view_dependencies.py`):
1. ✅ `test_unqualified_table_refs_are_qualified_with_backticks` - **REGRESSION TEST** for FQN qualification
2. ✅ `test_view_with_multiple_tables_all_qualified` - Validates JOINs are properly qualified
3. ✅ `test_create_and_update_view_batched` - Verifies CREATE + UPDATE squashing
4. ✅ `test_create_and_multiple_updates_batched` - Ensures last UPDATE wins
5. ✅ `test_view_dependencies_are_sorted_correctly` - Validates topological sorting
6. ✅ `test_circular_view_dependency_detection` - Detects cycles and falls back gracefully

**State Differ Tests** (`test_state_differ.py`):
1. ✅ `test_diff_added_view` - Detects newly added views
2. ✅ `test_diff_removed_view` - Detects removed views
3. ✅ `test_diff_renamed_view_with_history` - Detects view renames
4. ✅ `test_diff_view_definition_change` - Detects SQL definition changes
5. ✅ `test_diff_view_comment_change` - Detects comment changes
6. ✅ `test_diff_multiple_views_added` - **REGRESSION TEST** for bulk view additions
7. ✅ `test_new_schema_with_views_includes_all_views` - **REGRESSION TEST** for views in new schemas
8. ✅ `test_new_catalog_with_views_includes_all_views` - **REGRESSION TEST** for views in new catalogs
9. ✅ Generic object coverage tests to catch future bugs with any object type

**Dependency Graph Tests** (`test_dependency_graph.py`):
1. ✅ Cycle detection with detailed paths
2. ✅ Topological sorting
3. ✅ Breaking change detection

**Why these tests matter:**
- Catches FQN qualification bugs that cause `TABLE_OR_VIEW_NOT_FOUND` errors
- Validates view SQL is generated in correct order (tables before views)
- Ensures state differ includes all views when diffing snapshots
- Generic tests prevent similar bugs for future object types

**Running view tests:**
```bash
# Run all view dependency tests
pytest tests/integration/test_view_dependencies.py -v

# Run specific regression test
pytest tests/integration/test_view_dependencies.py::TestViewFQNQualification::test_unqualified_table_refs_are_qualified_with_backticks -xvs

# Run state differ view tests
pytest tests/unit/test_state_differ.py::TestUnityStateDifferViews -v

# Run state differ regression tests
pytest tests/unit/test_state_differ.py::TestUnityStateDifferViewRegression -v

# Run dependency graph tests
pytest tests/unit/test_dependency_graph.py -v
```

## VS Code Extension Testing

### Prerequisites

```bash
cd packages/vscode-extension
npm install
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage

# Run specific test file
npm test -- storage-v4.test.ts

# Run tests with verbose output
npm test -- --verbose
```

### Test Structure

```
packages/vscode-extension/tests/
├── unit/                              # Unit tests
│   └── storage-v4.test.ts            # Storage layer tests (5 tests)
├── webview/                           # React component tests
│   └── components/
│       ├── ColumnGrid.test.tsx       # Column grid tests (7 tests)
│       ├── Sidebar.test.tsx          # Sidebar tests (2 tests)
│       └── TableConstraints.test.tsx # Constraints tests (7 tests)
├── providers/                         # Provider tests
│   └── base/
│       └── hierarchy.test.ts         # Hierarchy tests (4 tests)
├── fixtures/                          # Test fixtures
│   ├── sample-project-v4.json        # V4 project fixture
│   ├── sample-changelog.json         # Changelog fixture
│   ├── sample-snapshot.json          # Snapshot fixture
│   └── sample-state.ts               # State fixtures
├── __mocks__/                         # Mocks
│   ├── vscode.ts                     # VS Code API mock
│   └── styleMock.js                  # CSS mock
├── setup.ts                           # Test setup
└── utils/                             # Test utilities
```

**Test Summary**: 25 passing tests across 5 test files

### Testing Patterns

#### Storage Layer Testing

```typescript
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import * as fs from 'fs/promises';
import { ensureProjectFile, readProject } from '../../src/storage-v4';

// Mock fs/promises
jest.mock('fs/promises');
const mockFs = fs as jest.Mocked<typeof fs>;

describe('Storage V4', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should create project file', async () => {
    mockFs.writeFile.mockResolvedValue(undefined);
    // Test implementation
  });
});
```

#### React Component Testing

```typescript
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ColumnGrid } from '../../../src/webview/components/ColumnGrid';

// Mock Zustand store
jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    addColumn: jest.fn(),
    renameColumn: jest.fn(),
    // ... other store methods
  }),
}));

describe('ColumnGrid Component', () => {
  test('should render columns', () => {
    render(<ColumnGrid tableId="table_001" columns={mockColumns} />);
    expect(screen.getByText('id')).toBeInTheDocument();
  });
});
```

#### SQL Generator Testing

```typescript
import { UnitySQLGenerator } from '../../src/providers/unity/sql-generator';
import { UnityState } from '../../src/providers/unity/models';

describe('Unity SQL Generator', () => {
  test('should generate SQL for add_catalog', () => {
    const state: UnityState = { catalogs: [] };
    const generator = new UnitySQLGenerator(state);
    
    const op = {
      id: 'op_001',
      op: 'add_catalog',
      payload: { catalogId: 'cat_001', name: 'test_catalog' },
    };
    
    const result = generator.generateSQLForOperation(op);
    expect(result.sql).toContain('CREATE CATALOG IF NOT EXISTS test_catalog');
  });
});
```

### Mocking VS Code API

The VS Code API is mocked in `tests/__mocks__/vscode.ts`:

```typescript
export const window = {
  showInformationMessage: jest.fn(),
  showErrorMessage: jest.fn(),
  createOutputChannel: jest.fn(() => ({
    appendLine: jest.fn(),
    show: jest.fn(),
  })),
};

export const workspace = {
  workspaceFolders: [],
  getConfiguration: jest.fn(),
};
```

### Coverage Thresholds

The project has baseline coverage thresholds configured in `jest.config.js`:

```javascript
coverageThreshold: {
  global: {
    branches: 2,
    functions: 2,
    lines: 4,
    statements: 4,
  },
}
```

**Current Coverage**: ~4% overall (baseline established)
- React components: ~10-25% coverage
- Core providers: Initial test coverage
- Focus: Demonstrating testing infrastructure and patterns

## Integration Tests

### Running Integration Tests

```bash
# From project root
cd examples/basic-schema

# Validate schema
schematic validate

# Generate SQL
schematic sql --output test-migration.sql
```

### CI/CD Integration

Integration tests run automatically in the `integration-tests.yml` workflow:
- Builds VS Code extension
- Installs Python SDK
- Tests CLI on example project

## Smoke Tests

Quick validation to ensure everything builds correctly:

```bash
# From project root
./scripts/smoke-test.sh
```

The smoke test script:
1. ✅ Builds VS Code extension
2. ✅ Validates extension package
3. ✅ Installs Python SDK
4. ✅ Validates example schema with CLI

## Quality Checks (Local)

Run all quality checks locally before committing:

```bash
# From project root
./devops/run-checks.sh
```

This script runs:
1. ✅ Python code formatting check (Black, 100 char line length)
2. ✅ Python linting (Ruff)
3. ✅ Python SDK tests (pytest - 201 tests)
4. ✅ VS Code Extension tests (Jest - 25 tests)
5. ✅ Smoke tests (extension build, SDK install, CLI validation)

**Exit code 0** = All checks passed, ready to commit  
**Exit code 1** = One or more checks failed, fix before committing

## CI/CD Pipeline

The quality checks pipeline runs automatically on:
- Push to `main` or `develop`
- Pull requests to `main` or `develop`
- Manual trigger

### Pipeline Jobs

1. **code-formatting**: Black + Ruff checks
2. **python-tests**: 138 pytest tests + coverage reporting
3. **extension-tests**: Jest tests + coverage reporting
4. **smoke-tests**: Extension build + SDK validation
5. **commit-signatures**: GPG signature verification

### Status

✅ All checks configured and passing locally

## Test Development Guidelines

### Writing Good Tests

1. **Arrange-Act-Assert Pattern**
   ```typescript
   test('should add catalog', () => {
     // Arrange
     const state = { catalogs: [] };
     const op = { op: 'add_catalog', ... };
     
     // Act
     const newState = applyOpsToState(state, [op]);
     
     // Assert
     expect(newState.catalogs).toHaveLength(1);
   });
   ```

2. **Test Naming Convention**
   - Use descriptive names: `should create project file with v4 schema`
   - Use action-oriented names: `should apply add_catalog operation`

3. **Test Isolation**
   - Each test should be independent
   - Use `beforeEach` to reset state
   - Mock external dependencies

4. **Test Coverage**
   - Aim for 70%+ overall coverage
   - 80%+ for critical files like `storage-v4.ts`
   - Test edge cases and error conditions

### Test Fixtures

Reusable test data in `tests/fixtures/`:
- `sample-project-v4.json`: Valid v4 project
- `sample-changelog.json`: Changelog with operations
- `sample-snapshot.json`: Snapshot with state
- `sample-state.ts`: TypeScript state objects

### Using Operation Builders (Python)

The Python SDK includes operation builder utilities:

```python
from tests.utils.operation_builders import OperationBuilder

op = OperationBuilder.add_catalog('cat_001', 'test_catalog')
```

## Debugging Tests

### Python Tests

```bash
# Run with print statements visible
pytest tests/unit/test_storage_v4.py -xvs

# Drop into debugger on failure
pytest tests/unit/test_storage_v4.py --pdb

# Run only failed tests from last run
pytest --lf
```

### VS Code Extension Tests

```bash
# Run with verbose output
npm test -- --verbose

# Run specific test
npm test -- --testNamePattern="should create project file"

# Update snapshots
npm test -- --updateSnapshot
```

## Continuous Testing

### Watch Mode (Python)

```bash
cd packages/python-sdk
pytest-watch tests/
```

### Watch Mode (TypeScript)

```bash
cd packages/vscode-extension
npm run test:watch
```

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'schematic'`  
**Solution**: Install SDK in editable mode: `pip install -e ".[dev]"`

**Issue**: Jest tests fail with "Cannot find module 'vscode'"  
**Solution**: Ensure `tests/__mocks__/vscode.ts` exists and is properly configured

**Issue**: React component tests fail with "acquireVsCodeApi is not defined"  
**Solution**: Check `tests/setup.ts` has the webview API mock

**Issue**: Coverage thresholds not met  
**Solution**: Add tests for uncovered code or adjust thresholds in `jest.config.js`

## Resources

- [Jest Documentation](https://jestjs.io/)
- [React Testing Library](https://testing-library.com/react)
- [Pytest Documentation](https://docs.pytest.org/)
- [SQLGlot Documentation](https://sqlglot.com/)

---

**Last Updated**: 2025-10-28  
**Test Count**: 138 Python tests (12 skipped), 25 VS Code Extension tests  
**Status**: ✅ All tests passing
**Coverage**: Python SDK 70%+, VS Code Extension ~4% (baseline)

