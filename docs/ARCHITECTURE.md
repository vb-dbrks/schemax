# Architecture

This document describes the technical architecture and design decisions behind SchemaX.

## Overview

SchemaX implements a snapshot-based schema versioning system. The core principle is to maintain an append-only operation log with periodic snapshots, enabling both state-based and change-based workflows.

## Design Goals

1. **Git-Friendly**: Store schema definitions in human-readable JSON that produces clean diffs
2. **Reproducible**: Replay operations from a snapshot to reconstruct current state
3. **Performant**: Fast loading even with hundreds of operations
4. **Auditable**: Complete history of who changed what and when
5. **Migration-Ready**: Operations can be converted to SQL migration scripts

## File Structure

### Version 2 Architecture

```
workspace-root/
└── .schemax/
    ├── project.json           # Project metadata (lightweight)
    ├── changelog.json         # Uncommitted operations
    └── snapshots/
        ├── v0.1.0.json       # Full state snapshot
        ├── v0.2.0.json
        └── v0.3.0.json
```

**Why separate files?**
- Snapshots are immutable and rarely change → better for git
- Changelog is active development → frequent updates
- Only need to load latest snapshot, not all historical state
- Clear separation between stable releases (snapshots) and work-in-progress (changelog)

### Project File Schema

`project.json` contains only metadata:

```json
{
  "version": 2,
  "name": "my-databricks-schemas",
  "environments": ["dev", "test", "prod"],
  "latestSnapshot": "v0.2.0",
  "snapshots": [
    {
      "id": "snap_uuid",
      "version": "v0.1.0",
      "name": "Initial schema",
      "file": ".schemax/snapshots/v0.1.0.json",
      "ts": "2025-10-06T10:00:00Z",
      "opsCount": 15,
      "hash": "sha256...",
      "previousSnapshot": null
    }
  ],
  "deployments": [],
  "settings": {}
}
```

### Snapshot File Schema

`snapshots/vX.Y.Z.json` contains the full state:

```json
{
  "id": "snap_uuid",
  "version": "v0.1.0",
  "name": "Initial schema",
  "ts": "2025-10-06T10:00:00Z",
  "createdBy": "user@example.com",
  "state": {
    "catalogs": [
      {
        "id": "cat_uuid",
        "name": "my_catalog",
        "schemas": [
          {
            "id": "sch_uuid",
            "name": "my_schema",
            "tables": [...]
          }
        ]
      }
    ]
  },
  "opsIncluded": ["op_1", "op_2", ...],
  "previousSnapshot": null,
  "hash": "sha256...",
  "tags": ["production"],
  "comment": "First release"
}
```

### Changelog File Schema

`changelog.json` tracks operations since the last snapshot:

```json
{
  "version": 1,
  "sinceSnapshot": "v0.1.0",
  "ops": [
    {
      "id": "op_uuid",
      "ts": "2025-10-06T11:00:00Z",
      "op": "add_column",
      "target": "col_uuid",
      "payload": {
        "tableId": "tbl_uuid",
        "colId": "col_uuid",
        "name": "customer_id",
        "type": "BIGINT",
        "nullable": false
      }
    }
  ],
  "lastModified": "2025-10-06T11:00:00Z"
}
```

## Data Model

### Unity Catalog Hierarchy

```
Catalog
├── id: string (UUID)
├── name: string
└── schemas: Schema[]

Schema
├── id: string (UUID)
├── name: string
└── tables: Table[]

Table
├── id: string (UUID)
├── name: string
├── format: "delta" | "iceberg"
├── columnMapping?: "name" | "id"
├── columns: Column[]
├── properties: Record<string, string>
├── constraints: Constraint[]
├── grants: Grant[]
└── comment?: string

Column
├── id: string (UUID)
├── name: string
├── type: ColumnType
├── nullable: boolean
└── comment?: string
```

**Why UUIDs?**
- Stable identifiers that don't change when objects are renamed
- Enable unambiguous tracking across operations
- Generated client-side, no coordination needed
- Users never see them (internal only)

### Operations

Every user action generates one or more operations:

```typescript
type Op = {
  id: string;           // Unique operation ID
  ts: string;           // ISO 8601 timestamp
  op: OpType;           // Operation type
  target: string;       // ID of affected object
  payload: Record<string, any>;  // Operation-specific data
}
```

**Operation Types:**

- **Catalog**: `add_catalog`, `rename_catalog`, `drop_catalog`
- **Schema**: `add_schema`, `rename_schema`, `drop_schema`
- **Table**: `add_table`, `rename_table`, `drop_table`, `set_table_comment`
- **Column**: `add_column`, `rename_column`, `drop_column`, `reorder_columns`, `change_column_type`, `set_nullable`, `set_column_comment`
- **Property**: `set_table_property`, `unset_table_property` - Manage Unity Catalog TBLPROPERTIES

**Example - Adding a Column:**

```json
{
  "id": "op_abc123",
  "ts": "2025-10-06T10:30:00Z",
  "op": "add_column",
  "target": "col_xyz789",
  "payload": {
    "tableId": "tbl_def456",
    "colId": "col_xyz789",
    "name": "customer_id",
    "type": "BIGINT",
    "nullable": false,
    "comment": "Unique customer identifier"
  }
}
```

**Example - Setting a Table Property:**

```json
{
  "id": "op_def456",
  "ts": "2025-10-06T10:35:00Z",
  "op": "set_table_property",
  "target": "tbl_def456",
  "payload": {
    "tableId": "tbl_def456",
    "key": "delta.appendOnly",
    "value": "true"
  }
}
```

This operation sets the `delta.appendOnly` property to `true`, making the table append-only (disabling UPDATE and DELETE operations). See the [Unity Catalog TBLPROPERTIES documentation](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-tblproperties) for all available properties.

## State Management

### Loading Current State

```
1. Read project.json → Get latestSnapshot version
2. Read snapshots/v{version}.json → Load full state
3. Read changelog.json → Get operations since snapshot
4. Apply changelog operations to snapshot state → Current state
```

This approach provides:
- Fast loading (single snapshot + changelog)
- Complete history (snapshot files are preserved)
- Bounded memory usage (don't load all ops ever)

### Saving Operations

```
1. User makes change in UI
2. Webview generates operation(s)
3. Webview sends operations to extension
4. Extension appends to changelog.json
5. Extension applies operations to current state
6. Extension sends updated state back to webview
```

### Creating Snapshots

```
1. User runs "Create Snapshot" command
2. Extension reads current state (snapshot + changelog)
3. Extension writes new snapshot file:
   - Full state
   - List of included operation IDs
   - Metadata (version, name, timestamp, hash)
4. Extension updates project.json:
   - Add snapshot metadata
   - Update latestSnapshot
5. Extension clears changelog.json
6. Extension notifies webview
```

## Operation Reducer

The core logic for applying operations to state:

```typescript
function applyOpsToState(state: State, ops: Op[]): State {
  let current = state;
  for (const op of ops) {
    current = applyOp(current, op);
  }
  return current;
}

function applyOp(state: State, op: Op): State {
  switch (op.op) {
    case 'add_catalog':
      return {
        ...state,
        catalogs: [...state.catalogs, createCatalog(op.payload)]
      };
    case 'add_column':
      return updateTable(state, op.payload.tableId, table => ({
        ...table,
        columns: [...table.columns, createColumn(op.payload)]
      }));
    // ... other operations
  }
}
```

**Key Properties:**
- Pure functions (no side effects)
- Immutable updates (always create new objects)
- Deterministic (same ops + state = same result)
- Composable (ops can be replayed in any order)

## Extension Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│ VS Code                                                   │
│                                                           │
│  ┌─────────────────┐         ┌──────────────────────┐   │
│  │  Extension Host │◄───────►│  Webview (React)     │   │
│  │  (Node.js)      │         │  (Browser Context)   │   │
│  │                 │  Message│                      │   │
│  │  • Commands     │  Passing│  • Visual Designer   │   │
│  │  • File I/O     │         │  • State Management  │   │
│  │  • Storage      │         │  • User Actions      │   │
│  └────────┬────────┘         └──────────────────────┘   │
│           │                                              │
│           │                                              │
│  ┌────────▼──────────────────────┐                      │
│  │  Workspace Filesystem         │                      │
│  │  .schemax/                    │                      │
│  │  ├── project.json             │                      │
│  │  ├── changelog.json           │                      │
│  │  └── snapshots/               │                      │
│  └───────────────────────────────┘                      │
└─────────────────────────────────────────────────────────┘
```

### Message Protocol

**Webview → Extension:**

```typescript
// Load project data
{ type: 'load-project' }

// Save operations
{
  type: 'append-ops',
  payload: [
    { id, ts, op, target, payload }
  ]
}
```

**Extension → Webview:**

```typescript
// Initial load complete
{
  type: 'project-loaded',
  payload: {
    ...projectMetadata,
    state: { catalogs: [...] },
    ops: [...]  // Current changelog
  }
}

// Update after operations saved
{
  type: 'project-updated',
  payload: {
    ...projectMetadata,
    state: { catalogs: [...] },
    ops: [...]
  }
}
```

### VS Code API Usage

**Commands:**
- `vscode.commands.registerCommand()` - Register custom commands
- `vscode.window.showInputBox()` - Get user input
- `vscode.window.withProgress()` - Show progress indicators

**Webview:**
- `vscode.window.createWebviewPanel()` - Create webview
- `webview.postMessage()` - Send messages to webview
- `webview.onDidReceiveMessage()` - Receive messages from webview

**Logging:**
- `vscode.window.createOutputChannel()` - Create output channel
- `outputChannel.appendLine()` - Log messages

## Webview Architecture

### React Component Tree

```
App
├── Toolbar (Add/Delete actions)
├── Layout
│   ├── Sidebar (Tree view)
│   │   ├── Catalog items
│   │   ├── Schema items
│   │   └── Table items
│   ├── TableDesigner (Main content)
│   │   ├── Table properties
│   │   └── ColumnGrid (Inline editing)
│   └── SnapshotPanel (Version timeline)
```

### State Management (Zustand)

```typescript
type DesignerStore = {
  // State
  project: ProjectFile | null;
  selectedTable: string | null;

  // Actions
  setProject: (project: ProjectFile) => void;
  selectTable: (tableId: string | null) => void;
  
  // Mutations (generate ops)
  addCatalog: (name: string) => void;
  addColumn: (tableId, name, type, nullable) => void;
  // ... etc
};
```

**All mutations:**
1. Generate operation with UUID
2. Call `emitOps([op])` to send to extension
3. Extension saves and returns updated state
4. Store updates with new state

### VS Code API Bridge

The webview runs in a sandboxed browser context and needs special handling:

```typescript
// src/webview/vscode-api.ts
let vscodeApi: any;

export function getVsCodeApi() {
  if (!vscodeApi) {
    vscodeApi = acquireVsCodeApi();
  }
  return vscodeApi;
}
```

**Why?** `acquireVsCodeApi()` can only be called once per webview session. The singleton pattern ensures it's called exactly once.

## Design Decisions

### Why JSON Instead of SQLite?

**Advantages:**
- Human-readable and git-friendly
- No binary dependencies
- Easy to inspect and debug
- Works on all platforms
- Standard tools (jq, diff, merge)

**Trade-offs:**
- Not suitable for thousands of tables
- No built-in indexing or querying
- Requires parsing entire file

**Verdict:** JSON is appropriate for typical Unity Catalog schemas (10-100 tables). For larger schemas, we could add SQLite in a future version.

### Why Append-Only Operations?

**Benefits:**
- Complete audit trail
- Can replay history
- Generate migration scripts
- Support rollback/undo
- Enable drift detection

**Challenges:**
- Unbounded growth (mitigated by snapshots)
- Need to track operation IDs
- More complex than simple save/load

### Why Snapshots?

Without snapshots, the changelog would grow forever. Snapshots provide:
- **Bounded growth**: Changelog only since last snapshot
- **Fast loading**: Don't replay 1000s of operations
- **Clean releases**: Tag snapshots for deployments
- **Git efficiency**: Snapshots rarely change after creation

### Why Separate Snapshot Files?

**V1 (Single File):**
```json
{
  "version": 1,
  "state": {...},
  "ops": [1000s of operations],
  "snapshots": [metadata]
}
```

**Problems:**
- Large file (100s of KB)
- Every operation changes the file
- Git diffs are huge
- Slow to parse

**V2 (Separate Files):**
```
project.json       # 2 KB, rarely changes
changelog.json     # 5 KB, changes frequently
snapshots/*.json   # 20 KB each, never change
```

**Benefits:**
- Clean git diffs (only changelog changes)
- Fast parsing (only load what's needed)
- Immutable snapshots (git-friendly)
- Better organization

## Security Considerations

### Webview Sandboxing

The webview runs in a sandboxed context with:
- Limited API access
- No direct file system access
- Content Security Policy (CSP)
- No eval() or inline scripts

**Implications:**
- Must use `postMessage` for all extension communication
- Cannot use browser APIs like `prompt()`, `confirm()`, `alert()`
- Custom modal dialogs required

### Content Security Policy

```html
<meta http-equiv="Content-Security-Policy" 
      content="default-src 'none'; 
               script-src ${cspSource}; 
               style-src ${cspSource} 'unsafe-inline';">
```

This restricts:
- External network requests (none allowed)
- Script sources (only bundled code)
- Inline event handlers (none allowed)

## Performance Considerations

### Loading

- **Fast Path**: Load latest snapshot + changelog (~25 KB)
- **Full History**: Only load on demand (snapshot files)
- **Memory**: Keep only current state in memory

### Saving

- **Incremental**: Append operations to changelog
- **Batch**: Group related operations when possible
- **Debounce**: UI updates debounced to avoid thrashing

### UI Rendering

- **Lazy**: Tree nodes expanded on demand
- **Virtual**: Large column lists (100+) use virtualization
- **Memoized**: React components memoized to avoid re-renders

## Future Enhancements

### Phase 2: Migration Generation

Convert operations to SQL:

```typescript
function generateMigration(ops: Op[]): string {
  return ops.map(op => {
    switch (op.op) {
      case 'add_column':
        return `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${type};`;
      // ... etc
    }
  }).join('\n');
}
```

### Phase 3: Deployment

Apply migrations to Databricks:

```typescript
async function deploy(env: Environment, snapshot: Snapshot) {
  const client = new DatabricksClient(env);
  const migration = generateMigration(changelog.ops);
  await client.executeSql(migration);
}
```

### Phase 4: Drift Detection

Compare local schema to Unity Catalog:

```typescript
async function detectDrift(catalog: string): Promise<Drift[]> {
  const local = loadCurrentState();
  const remote = await databricks.getCatalog(catalog);
  return compare(local, remote);
}
```

### Phase 5: Multi-User Workflows

- Merge conflict resolution
- Operation ordering and dependencies
- Concurrent editing detection
- Team collaboration features

## Testing Strategy

### Unit Tests (Future)

- Operation reducer logic
- State transformations
- Validation logic
- File I/O mocking

### Integration Tests

- Extension activation
- Command execution
- File creation and reading
- Webview communication

### Manual Testing

- Designer opens successfully
- All CRUD operations work
- Snapshots persist correctly
- Changelog clears after snapshot
- UI reflects state accurately

## References

- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
- [VS Code Extension Guidelines](https://code.visualstudio.com/api/extension-guides/overview)

