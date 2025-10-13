# Development Guide

This guide covers building, testing, and contributing to SchemaX.

## Prerequisites

- Node.js 18 or higher
- npm 9 or higher
- Visual Studio Code 1.90.0 or higher

## Getting Started

### Clone and Install

```bash
git clone https://github.com/vb-dbrks/schemax-vscode.git
cd schemax-vscode
npm install
```

### Build

Build both the extension and webview:

```bash
npm run build
```

Or build individually:

```bash
npm run build:ext      # Extension only
npm run build:webview  # Webview only
```

### Development Mode

Start watch mode for automatic rebuilds:

```bash
npm run watch
```

This runs both extension and webview watchers concurrently.

### Testing

1. Open the project in VS Code
2. Press `F5` to launch the Extension Development Host
3. In the new window, open a test workspace
4. Press `Cmd+Shift+P` and run **SchemaX: Open Designer**
5. Check logs: View → Output → Select "SchemaX"

## Project Structure

```
schemax-vscode/
├── packages/
│   ├── vscode-extension/          # VS Code Extension
│   │   ├── src/
│   │   │   ├── extension.ts               # Extension entry point
│   │   │   ├── storage-v3.ts              # Provider-aware storage
│   │   │   ├── providers/                 # Provider system
│   │   │   │   ├── base/                  # Base interfaces
│   │   │   │   │   ├── provider.ts
│   │   │   │   │   ├── models.ts
│   │   │   │   │   ├── operations.ts
│   │   │   │   │   ├── sql-generator.ts
│   │   │   │   │   └── hierarchy.ts
│   │   │   │   ├── registry.ts            # Provider registry
│   │   │   │   ├── index.ts               # Auto-registration
│   │   │   │   └── unity/                 # Unity Catalog provider
│   │   │   │       ├── index.ts
│   │   │   │       ├── models.ts
│   │   │   │       ├── operations.ts
│   │   │   │       ├── sql-generator.ts
│   │   │   │       ├── state-reducer.ts
│   │   │   │       ├── hierarchy.ts
│   │   │   │       └── provider.ts
│   │   │   ├── shared/                    # Legacy (V2 compat)
│   │   │   │   ├── model.ts
│   │   │   │   └── ops.ts
│   │   │   └── webview/
│   │   │       ├── main.tsx               # Webview entry
│   │   │       ├── App.tsx                # Main component
│   │   │       ├── state/
│   │   │       │   └── useDesignerStore.ts # Provider-aware store
│   │   │       └── components/
│   │   │           ├── Sidebar.tsx
│   │   │           ├── TableDesigner.tsx
│   │   │           ├── ColumnGrid.tsx
│   │   │           └── SnapshotPanel.tsx
│   │   ├── dist/                          # Compiled extension
│   │   ├── media/                         # Compiled webview
│   │   └── package.json
│   │
│   └── python-sdk/                # Python SDK & CLI
│       ├── src/schemax/
│       │   ├── providers/                 # Provider system
│       │   │   ├── base/
│       │   │   │   ├── provider.py
│       │   │   │   ├── models.py
│       │   │   │   ├── operations.py
│       │   │   │   ├── sql_generator.py
│       │   │   │   └── hierarchy.py
│       │   │   ├── registry.py
│       │   │   └── unity/
│       │   │       ├── __init__.py
│       │   │       ├── models.py
│       │   │       ├── operations.py
│       │   │       ├── sql_generator.py
│       │   │       ├── state_reducer.py
│       │   │       ├── hierarchy.py
│       │   │       └── provider.py
│       │   ├── storage_v3.py              # Provider-aware storage
│       │   ├── cli.py                     # CLI commands
│       │   └── models.py                  # Legacy (V2 compat)
│       └── pyproject.toml
│
├── docs/                          # Documentation
├── examples/                      # Working examples
└── scripts/                       # Utility scripts
```

## Architecture

### Provider-Based System

SchemaX uses a **provider-based architecture** to support multiple catalog types (Unity Catalog, Hive, PostgreSQL, etc.). Each provider implements a standard interface.

**Key Components:**
- **Provider Registry** - Manages available providers
- **Provider Interface** - Standard contract all providers implement
- **Storage V3** - Provider-aware file operations
- **State Reducer** - Provider-specific state modifications
- **SQL Generator** - Provider-specific DDL generation

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture documentation.

### Extension Host

The extension host (`src/extension.ts`) runs in Node.js and:
- Registers VS Code commands
- Manages file I/O via `storage-v3.ts`
- Loads provider from project file
- Creates and controls the webview
- Handles message passing with the webview
- Validates operations using provider

### Webview

The webview (`src/webview/`) is a React application that:
- Provides the visual designer UI
- Uses Zustand for state management
- Stores current provider information
- Generates provider-prefixed operations
- Sends operations to the extension via `postMessage`

### Communication Flow (V3)

```
User Action → Webview (React)
           → Get provider from store
           → Generate Op with provider prefix (e.g., unity.add_catalog)
           → postMessage('append-ops')
           → Extension
           → Get provider from registry
           → Validate operation
           → Append to changelog.json
           → Apply op using provider's state reducer
           → postMessage('project-updated' with provider info)
           → Webview updates UI
```

### Storage Architecture (V3)

SchemaX V3 adds provider awareness to the snapshot-based architecture:

**Files:**
- `.schemax/project.json` - Metadata + **provider selection**
- `.schemax/changelog.json` - Uncommitted **provider-prefixed** operations
- `.schemax/snapshots/vX.Y.Z.json` - Full state snapshots

**Loading:**
1. Read project.json → get provider type
2. Load provider from registry
3. Read latest snapshot file (or use provider's initial state)
4. Read changelog
5. **Apply operations using provider's state reducer**
6. Result = current state

**Migration:**
- V2 projects automatically migrate to V3 on first load
- Operations get prefixed with provider ID (e.g., `unity.`)
- Provider field added to project.json

**Saving:**
- Operations append to `changelog.json`
- State changes are computed by applying operations
- Snapshots freeze the state and clear changelog

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design information.

## Key Concepts

### Operations

Operations are immutable records of user actions:

```typescript
{
  id: "op_uuid",
  ts: "2025-10-06T...",
  op: "add_column",
  target: "col_uuid",
  payload: { tableId, colId, name, type, nullable }
}
```

All schema changes generate operations. Operations are:
- Append-only (never modified)
- Tracked with UUIDs
- Stored in `changelog.json`
- Used to generate migrations (future)

### Snapshots

Snapshots capture complete schema state:

**Metadata** (in `project.json`):
```typescript
{
  id: "snap_uuid",
  version: "v0.1.0",
  name: "Initial schema",
  file: ".schemax/snapshots/v0.1.0.json",
  opsCount: 15,
  ...
}
```

**Full Snapshot** (in `snapshots/v0.1.0.json`):
```typescript
{
  id: "snap_uuid",
  version: "v0.1.0",
  state: { catalogs: [...] },  // Complete schema
  opsIncluded: ["op_1", "op_2", ...],
  hash: "sha256...",
  ...
}
```

## Build System

### Extension Build (esbuild)

- Entry: `src/extension.ts`
- Output: `dist/extension.js`
- Target: Node.js (CommonJS)
- Externals: `vscode` module

### Webview Build (Vite)

- Entry: `src/webview/main.tsx`
- Output: `media/` directory
- Target: Browser (ES modules)
- Bundles: React, Zustand, CSS

## Coding Guidelines

### Logging

Always use the output channel for logging:

```typescript
outputChannel.appendLine('[SchemaX] Your message');
```

Never use `console.log()` in extension code (it goes to Extension Host console, not visible to users).

### File I/O

All file operations should go through `storage-v2.ts`:

```typescript
import * as storageV2 from './storage-v2';

// Good
const project = await storageV2.readProject(workspaceUri);
await storageV2.appendOps(workspaceUri, ops);

// Bad - don't access files directly
const content = await fs.readFile('schemax.project.json');
```

### State Management

The webview uses Zustand. All mutations must generate operations:

```typescript
// In useDesignerStore.ts
addColumn: (tableId, name, type, nullable) => {
  const op: Op = {
    id: `op_${uuidv4()}`,
    ts: new Date().toISOString(),
    op: 'add_column',
    target: colId,
    payload: { tableId, colId, name, type, nullable }
  };
  emitOps([op]);  // Sends to extension
}
```

### Data Validation

All external data must be validated with Zod:

```typescript
import { ProjectFile } from './shared/model';

const parsed = JSON.parse(content);
const project = ProjectFile.parse(parsed);  // Throws if invalid
```

## Testing Checklist

Before submitting a pull request, verify:

- [ ] Extension builds without errors (`npm run build`)
- [ ] No TypeScript errors
- [ ] Designer opens successfully
- [ ] Can create catalogs, schemas, tables, columns
- [ ] Can edit column properties inline
- [ ] Can create snapshots
- [ ] Snapshots persist after reloading
- [ ] Changelog clears after snapshot
- [ ] SchemaX output logs show no errors

## Debugging

### Extension Logs

View → Output → Select "SchemaX" from dropdown

Shows:
- Extension activation
- File operations
- Operation appends
- Snapshot creation
- Errors with stack traces

### Webview Console

1. With designer open: Help → Toggle Developer Tools
2. Go to Console tab
3. Look for `[SchemaX Webview]` logs

### Common Issues

**Blank webview:**
- Check webview console for JavaScript errors
- Verify `media/` directory has `index.html` and `assets/`
- Rebuild webview: `npm run build:webview`

**Operations not saving:**
- Check SchemaX output logs
- Verify `.schemax/` directory exists
- Check file permissions

**Snapshots disappearing:**
- Verify `project.json` has `snapshots` array
- Check `changelog.json` exists
- Look for errors in output logs

---

## Provider Development

### Overview

Adding a new provider to SchemaX involves implementing the provider interface in both TypeScript and Python. This section guides you through the process.

**See also:** [PROVIDER_CONTRACT.md](PROVIDER_CONTRACT.md) for detailed API documentation.

### Quick Start: Adding a New Provider

**1. Create Provider Directories**

```bash
# TypeScript
mkdir -p packages/vscode-extension/src/providers/myprovider
touch packages/vscode-extension/src/providers/myprovider/{index,models,operations,sql-generator,state-reducer,hierarchy,provider}.ts

# Python
mkdir -p packages/python-sdk/src/schemax/providers/myprovider
touch packages/python-sdk/src/schemax/providers/myprovider/{__init__,models,operations,sql_generator,state_reducer,hierarchy,provider}.py
```

**2. Define Provider Metadata**

```typescript
// providers/myprovider/provider.ts
export const myProvider: Provider = {
  info: {
    id: 'myprovider',
    name: 'My Provider',
    version: '1.0.0',
    description: 'Support for MyDB',
    author: 'Your Name',
    docsUrl: 'https://docs.mydb.com',
  },
  capabilities: {
    supportedOperations: ['myprovider.add_database', 'myprovider.add_table'],
    supportedObjectTypes: ['database', 'table'],
    hierarchy: myProviderHierarchy,
    features: {
      constraints: true,
      rowFilters: false,
      columnMasks: false,
      columnTags: false,
    },
  },
  // ... implement interface methods
};
```

**3. Define Hierarchy**

```typescript
// providers/myprovider/hierarchy.ts
export const myProviderHierarchy = new Hierarchy([
  {
    name: 'database',
    displayName: 'Database',
    pluralName: 'databases',
    icon: 'database',
    isContainer: true,
  },
  {
    name: 'table',
    displayName: 'Table',
    pluralName: 'tables',
    icon: 'table',
    isContainer: false,
  },
]);
```

**4. Define Models**

```typescript
// providers/myprovider/models.ts
import { z } from 'zod';

export const MyDatabase = z.object({
  id: z.string(),
  name: z.string(),
  tables: z.array(MyTable),
});

export const MyTable = z.object({
  id: z.string(),
  name: z.string(),
  columns: z.array(MyColumn),
});

export const MyProviderState = z.object({
  databases: z.array(MyDatabase),
});

export type MyDatabase = z.infer<typeof MyDatabase>;
export type MyTable = z.infer<typeof MyTable>;
export type MyProviderState = z.infer<typeof MyProviderState>;
```

**5. Define Operations**

```typescript
// providers/myprovider/operations.ts
export const MY_PROVIDER_OPERATIONS = {
  ADD_DATABASE: 'myprovider.add_database',
  RENAME_DATABASE: 'myprovider.rename_database',
  DROP_DATABASE: 'myprovider.drop_database',
  ADD_TABLE: 'myprovider.add_table',
  // ... more operations
};

export const myProviderOperationMetadata: OperationMetadata[] = [
  {
    type: MY_PROVIDER_OPERATIONS.ADD_DATABASE,
    displayName: 'Add Database',
    description: 'Create a new database',
    category: OperationCategory.DATABASE,
    requiredFields: ['databaseId', 'name'],
    optionalFields: [],
    isDestructive: false,
  },
  // ... more metadata
];
```

**6. Implement State Reducer**

```typescript
// providers/myprovider/state-reducer.ts
export function applyOperation(
  state: MyProviderState,
  op: Operation
): MyProviderState {
  // Deep clone for immutability
  const newState = JSON.parse(JSON.stringify(state));
  
  // Strip provider prefix
  const opType = op.op.replace('myprovider.', '');
  
  switch (opType) {
    case 'add_database': {
      const database: MyDatabase = {
        id: op.payload.databaseId,
        name: op.payload.name,
        tables: [],
      };
      newState.databases.push(database);
      break;
    }
    
    case 'add_table': {
      const db = newState.databases.find(d => d.id === op.payload.databaseId);
      if (db) {
        const table: MyTable = {
          id: op.payload.tableId,
          name: op.payload.name,
          columns: [],
        };
        db.tables.push(table);
      }
      break;
    }
    
    // ... more cases
  }
  
  return newState;
}
```

**7. Implement SQL Generator**

```typescript
// providers/myprovider/sql-generator.ts
export class MyProviderSQLGenerator extends BaseSQLGenerator {
  generateSQLForOperation(op: Operation): SQLGenerationResult {
    const opType = op.op.replace('myprovider.', '');
    
    switch (opType) {
      case 'add_database':
        return {
          sql: `CREATE DATABASE IF NOT EXISTS ${this.escapeIdentifier(op.payload.name)}`,
          warnings: [],
          isIdempotent: true,
        };
      
      case 'add_table':
        const dbName = this.idNameMap[op.payload.databaseId];
        const tableName = op.payload.name;
        return {
          sql: `CREATE TABLE IF NOT EXISTS ${dbName}.${this.escapeIdentifier(tableName)} ()`,
          warnings: [],
          isIdempotent: true,
        };
      
      // ... more cases
    }
  }
}
```

**8. Implement Provider Class**

```typescript
// providers/myprovider/provider.ts
export class MyProvider extends BaseProvider {
  get info(): ProviderInfo {
    return {
      id: 'myprovider',
      name: 'My Provider',
      version: '1.0.0',
      description: 'Support for MyDB',
      author: 'Your Name',
      docsUrl: 'https://docs.mydb.com',
    };
  }
  
  get capabilities(): ProviderCapabilities {
    return {
      supportedOperations: Object.values(MY_PROVIDER_OPERATIONS),
      supportedObjectTypes: ['database', 'table'],
      hierarchy: myProviderHierarchy,
      features: {
        constraints: true,
        rowFilters: false,
        columnMasks: false,
        columnTags: false,
      },
    };
  }
  
  createInitialState(): ProviderState {
    return { databases: [] };
  }
  
  applyOperation(state: ProviderState, op: Operation): ProviderState {
    return applyOperation(state as MyProviderState, op);
  }
  
  getSQLGenerator(state: ProviderState): SQLGenerator {
    return new MyProviderSQLGenerator(state);
  }
  
  validateOperation(op: Operation): ValidationResult {
    // Implementation
  }
  
  validateState(state: ProviderState): ValidationResult {
    // Implementation
  }
}
```

**9. Register Provider**

```typescript
// providers/myprovider/index.ts
export * from './models';
export * from './operations';
export * from './provider';

import { MyProvider } from './provider';
export const myProvider = new MyProvider();

// providers/index.ts - Add to auto-registration
import { myProvider } from './myprovider';

export function initializeProviders() {
  ProviderRegistry.register(unityProvider);
  ProviderRegistry.register(myProvider); // NEW!
}

initializeProviders();
```

**10. Python Implementation**

Follow the same structure in Python:

```python
# packages/python-sdk/src/schemax/providers/myprovider/provider.py
class MyProvider(BaseProvider):
    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            id="myprovider",
            name="My Provider",
            version="1.0.0",
            description="Support for MyDB",
            author="Your Name",
            docs_url="https://docs.mydb.com",
        )
    
    # ... implement all methods
```

### Testing Your Provider

**1. Create Test File**

```typescript
// providers/myprovider/__tests__/myprovider.test.ts
import { testProviderCompliance } from '../../base/__tests__/provider-compliance.test';
import { myProvider } from '../provider';

// Run compliance tests
testProviderCompliance(myProvider);

// Provider-specific tests
describe('MyProvider', () => {
  test('creates initial state', () => {
    const state = myProvider.createInitialState();
    expect(state.databases).toEqual([]);
  });
  
  test('applies add_database operation', () => {
    // ... test implementation
  });
});
```

**2. Run Tests**

```bash
# TypeScript
cd packages/vscode-extension
npm test

# Python
cd packages/python-sdk
pytest tests/providers/myprovider/
```

### Common Patterns

**Immutable State Updates:**

```typescript
// ❌ BAD - Mutates state
state.databases.push(newDb);

// ✅ GOOD - Creates new state
const newState = {
  ...state,
  databases: [...state.databases, newDb],
};
```

**Operation Validation:**

```typescript
validateOperation(op: Operation): ValidationResult {
  const errors: ValidationError[] = [];
  const metadata = this.getOperationMetadata(op.op);
  
  // Check required fields
  for (const field of metadata.requiredFields) {
    if (!op.payload[field]) {
      errors.push({
        field: `payload.${field}`,
        message: `Required field missing: ${field}`,
        code: 'MISSING_REQUIRED_FIELD',
      });
    }
  }
  
  return { valid: errors.length === 0, errors };
}
```

**SQL Escaping:**

```typescript
// Always escape identifiers
this.escapeIdentifier(name); // `name`

// Always escape strings
this.escapeString(value); // 'value'
```

### Provider Development Checklist

Before submitting a provider:

- [ ] All required interface methods implemented
- [ ] Provider registered in registry
- [ ] Hierarchy defined
- [ ] All operations have metadata
- [ ] State reducer handles all operations
- [ ] State reducer is pure (no side effects)
- [ ] SQL generator handles all operations
- [ ] SQL is idempotent (safe to run multiple times)
- [ ] Operation validation implemented
- [ ] State validation implemented
- [ ] Provider compliance tests pass
- [ ] Provider-specific tests written
- [ ] Python implementation matches TypeScript
- [ ] Documentation written
- [ ] Examples provided

### Resources

- **Provider Contract**: [PROVIDER_CONTRACT.md](PROVIDER_CONTRACT.md)
- **Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Unity Provider**: Reference implementation in `providers/unity/`
- **Base Provider**: Abstract base in `providers/base/`

---

## Contributing

### Reporting Issues

When reporting bugs, include:
- Steps to reproduce
- Expected vs actual behavior
- SchemaX output logs
- Webview console errors (if any)
- VS Code version
- Operating system

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make changes and test thoroughly
4. Commit with clear messages
5. Push and create a pull request

### Code Style

- Use TypeScript strict mode
- Follow existing code formatting
- Add comments for complex logic
- Keep functions focused and small
- Use descriptive variable names

## Release Process

1. Update version in `package.json`
2. Update CHANGELOG.md
3. Build: `npm run build`
4. Package: `npm run package`
5. Test the `.vsix` file
6. Create GitHub release
7. Publish to marketplace

## Resources

- [VS Code Extension API](https://code.visualstudio.com/api)
- [Webview API](https://code.visualstudio.com/api/extension-guides/webview)
- [React Documentation](https://react.dev/)
- [Zustand Documentation](https://zustand-demo.pmnd.rs/)
- [Zod Documentation](https://zod.dev/)

## Getting Help

- Check [docs/](.) for other documentation
- Review [.cursorrules](../.cursorrules) for project context
- Open an issue on GitHub
- Review existing issues and discussions

