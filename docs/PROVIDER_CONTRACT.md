# Provider Contract

This document defines the contract that all SchemaX catalog providers must implement. Following this contract ensures your provider integrates seamlessly with both the VSCode extension and Python SDK.

## Overview

A **Provider** is a plugin that adds support for a specific catalog system (Unity Catalog, Hive Metastore, PostgreSQL, etc.). Each provider implements a standard interface that SchemaX uses for:

- **State Management** - How objects are stored and modified
- **Operations** - What actions users can perform  
- **SQL Generation** - How to convert operations to DDL statements
- **Validation** - What constraints and rules to enforce
- **UI Metadata** - How to display objects in the interface

## Quick Start

### 1. Provider Identification

Every provider must have a unique ID and metadata:

```typescript
// TypeScript
const info: ProviderInfo = {
  id: 'myprovider',           // Short, lowercase identifier
  name: 'My Provider',         // Human-readable name
  version: '1.0.0',           // Semantic version
  description: 'Provider for MyDB',
  author: 'Your Name',
  docsUrl: 'https://...',
};
```

```python
# Python
info = ProviderInfo(
    id="myprovider",
    name="My Provider",
    version="1.0.0",
    description="Provider for MyDB",
    author="Your Name",
    docs_url="https://...",
)
```

### 2. Define Hierarchy

Specify the object hierarchy for your catalog:

```typescript
// TypeScript - Example: 2-level hierarchy
const hierarchy = new Hierarchy([
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

**Common Hierarchies:**
- Unity Catalog: Catalog → Schema → Table (3 levels)
- Hive Metastore: Database → Table (2 levels)
- PostgreSQL: Database → Schema → Table (3 levels)

### 3. Define Models

Create type-safe models for your objects:

```typescript
// TypeScript
export interface MyDatabase extends BaseObject {
  id: string;
  name: string;
  tables: MyTable[];
}

export interface MyTable extends BaseObject {
  id: string;
  name: string;
  columns: MyColumn[];
}

export interface MyProviderState extends ProviderState {
  databases: MyDatabase[];
}
```

```python
# Python
class MyDatabase(BaseModel):
    id: str
    name: str
    tables: List[MyTable] = []

class MyProviderState(BaseModel):
    databases: List[MyDatabase] = []
```

### 4. Define Operations

List all operations your provider supports:

```typescript
// TypeScript
export const MY_OPERATIONS = {
  ADD_DATABASE: 'myprovider.add_database',
  ADD_TABLE: 'myprovider.add_table',
  // ... more operations
} as const;

export const myOperationMetadata: OperationMetadata[] = [
  {
    type: MY_OPERATIONS.ADD_DATABASE,
    displayName: 'Add Database',
    description: 'Create a new database',
    category: OperationCategory.Schema,
    requiredFields: ['databaseId', 'name'],
    optionalFields: [],
    isDestructive: false,
  },
  // ... more metadata
];
```

**Operation Naming Convention:** `{providerId}.{action}_{object}`

Examples:
- `unity.add_catalog`
- `hive.add_database`
- `postgres.drop_table`

### 5. Implement State Reducer

Apply operations to state immutably:

```typescript
// TypeScript
export function applyOperation(state: MyProviderState, op: Operation): MyProviderState {
  const newState = JSON.parse(JSON.stringify(state)); // Deep clone
  
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
    // ... handle all operations
  }
  
  return newState;
}
```

**Key Rules:**
- ✅ Always deep clone state (immutability)
- ✅ Handle ALL operations your provider supports
- ✅ Return new state, never mutate input
- ✅ Be defensive - check if objects exist before modifying

### 6. Implement SQL Generator

Convert operations to SQL DDL:

```typescript
// TypeScript
export class MySQLGenerator extends BaseSQLGenerator {
  constructor(protected state: MyProviderState) {
    super(state);
  }
  
  generateSQLForOperation(op: Operation): SQLGenerationResult {
    const opType = op.op.replace('myprovider.', '');
    
    switch (opType) {
      case 'add_database':
        return {
          sql: `CREATE DATABASE IF NOT EXISTS ${this.escapeIdentifier(op.payload.name)}`,
          warnings: [],
          isIdempotent: true,
        };
      // ... handle all operations
    }
  }
  
  canGenerateSQL(op: Operation): boolean {
    return op.provider === 'myprovider';
  }
}
```

**Key Rules:**
- ✅ Generate idempotent SQL (use IF NOT EXISTS, IF EXISTS, etc.)
- ✅ Escape identifiers and strings properly
- ✅ Add warnings for operations that can't be fully translated
- ✅ Return empty string or comment for unsupported operations

### 7. Implement Provider Class

Tie everything together:

```typescript
// TypeScript
export class MyProvider extends BaseProvider implements Provider {
  readonly info = myProviderInfo;
  readonly capabilities = myCapabilities;
  
  constructor() {
    super();
    myOperationMetadata.forEach(m => this.registerOperation(m));
  }
  
  validateOperation(op: Operation): ValidationResult {
    // Implement validation logic
  }
  
  applyOperation(state: ProviderState, op: Operation): ProviderState {
    return applyOperation(state as MyProviderState, op);
  }
  
  getSQLGenerator(state: ProviderState): SQLGenerator {
    return new MySQLGenerator(state as MyProviderState);
  }
  
  createInitialState(): ProviderState {
    return { databases: [] };
  }
  
  validateState(state: ProviderState): ValidationResult {
    // Implement state validation
  }
}

export const myProvider = new MyProvider();
```

### 8. Register Provider

```typescript
// TypeScript - in providers/index.ts
import { myProvider } from './myprovider';

export function initializeProviders(): void {
  ProviderRegistry.register(myProvider);
  // ... register other providers
}
```

```python
# Python - in providers/__init__.py
from .my_provider import my_provider

ProviderRegistry.register(my_provider)
```

## Required Interface Methods

### Provider Info & Capabilities

```typescript
interface Provider {
  readonly info: ProviderInfo;
  readonly capabilities: ProviderCapabilities;
}
```

**ProviderCapabilities** must specify:
- `supportedOperations`: Array of operation types
- `supportedObjectTypes`: Array of object type names
- `hierarchy`: Your hierarchy definition
- `features`: Feature flags (constraints, tags, etc.)

### Operation Metadata

```typescript
getOperationMetadata(operationType: string): OperationMetadata | undefined;
getAllOperations(): OperationMetadata[];
```

Return metadata for UI and validation.

### Validation

```typescript
validateOperation(op: Operation): ValidationResult;
validateState(state: ProviderState): ValidationResult;
```

Validate operations and state structure.

### State Management

```typescript
applyOperation(state: ProviderState, op: Operation): ProviderState;
applyOperations(state: ProviderState, ops: Operation[]): ProviderState;
createInitialState(): ProviderState;
```

Immutably apply operations to state.

### SQL Generation

```typescript
getSQLGenerator(state: ProviderState): SQLGenerator;
```

Return a configured SQL generator instance.

## File Structure

```
providers/
└── myprovider/
    ├── index.ts/py              # Exports
    ├── hierarchy.ts/py          # Hierarchy definition
    ├── models.ts/py             # Data models
    ├── operations.ts/py         # Operation definitions
    ├── state-reducer.ts/py      # State reducer
    ├── sql-generator.ts/py      # SQL generator
    └── provider.ts/py           # Provider implementation
```

## Testing Requirements

Every provider must have tests for:

1. **Provider Compliance** - Passes `testProviderCompliance()` suite
2. **Operation Application** - Each operation correctly modifies state
3. **SQL Generation** - Each operation generates valid SQL
4. **Validation** - Validates operations and state correctly
5. **Idempotency** - Operations can be applied multiple times safely

```typescript
// TypeScript example
import { testProviderCompliance } from '../base/__tests__/provider-compliance.test';
import { myProvider } from './provider';

describe('MyProvider', () => {
  testProviderCompliance(myProvider);
  
  // ... provider-specific tests
});
```

## Best Practices

### DO ✅

- **Use type-safe models** - Leverage TypeScript/Pydantic types
- **Make operations atomic** - One operation = one logical change
- **Generate idempotent SQL** - Safe to run multiple times
- **Validate early** - Check operation validity before applying
- **Document operations** - Clear descriptions in metadata
- **Test thoroughly** - Cover all operations and edge cases
- **Handle missing objects** - Check existence before modifying
- **Use meaningful IDs** - UUIDs or structured identifiers

### DON'T ❌

- **Don't mutate state** - Always return new state objects
- **Don't assume order** - Operations should be order-independent when possible
- **Don't skip validation** - Always validate before applying
- **Don't generate destructive SQL** - No DROP without IF EXISTS
- **Don't hardcode names** - Use ID-to-name mapping
- **Don't forget edge cases** - Handle nulls, empty arrays, etc.

## Operation Design Guidelines

### Granular Operations

Prefer small, focused operations over large composite ones:

✅ **Good:**
```typescript
unity.add_table      // Just creates table
unity.add_column     // Adds one column
```

❌ **Bad:**
```typescript
unity.create_table_with_columns  // Does too much
```

### Payload Structure

Use consistent payload structure:

```typescript
{
  id: 'op_123',
  ts: '2024-01-01T00:00:00Z',
  provider: 'myprovider',
  op: 'myprovider.add_table',
  target: 'table_id',      // ID of object being modified
  payload: {
    // Operation-specific data
    tableId: 'table_id',   // Often duplicates target for convenience
    name: 'my_table',
    parentId: 'schema_id', // ID of parent object
    // ... other fields
  }
}
```

### Naming Conventions

- **Objects:** `snake_case` internally, display names in UI
- **Operations:** `{provider}.{verb}_{noun}` (e.g., `unity.add_catalog`)
- **IDs:** Prefixed by type (e.g., `cat_uuid`, `tbl_uuid`)

## Examples

See the Unity Catalog provider as a reference implementation:

- TypeScript: `packages/vscode-extension/src/providers/unity/`
- Python: `packages/python-sdk/src/schemax/providers/unity/`

## Getting Help

- Check existing providers for patterns
- Review base provider tests for requirements
- Ask in GitHub Discussions
- Submit draft PR for early feedback

## Checklist

Before submitting your provider:

- [ ] Implements all required interface methods
- [ ] Has complete operation metadata
- [ ] Generates valid SQL for all operations
- [ ] Passes provider compliance tests
- [ ] Has provider-specific tests
- [ ] Includes documentation
- [ ] Follows file structure convention
- [ ] Registers with ProviderRegistry
- [ ] Handles all edge cases
- [ ] Uses immutable state updates

---

**Next Steps:** See [PROVIDER_EXAMPLES.md](./PROVIDER_EXAMPLES.md) for complete example implementations.

