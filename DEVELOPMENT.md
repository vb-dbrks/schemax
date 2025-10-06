# Development Guide

## Architecture Overview

### Extension Host (Node.js)

The extension host (`src/extension.ts`) runs in VS Code's Node.js process and:

1. Registers commands (`schemax.openDesigner`, `schemax.showLastOps`)
2. Manages the webview lifecycle
3. Handles file I/O for `schemax.project.json`
4. Implements the message bridge between webview and extension

### Webview (React + Vite)

The webview (`src/webview/`) runs in an isolated browser context and:

1. Renders the Unity Catalog tree (Sidebar)
2. Provides table/column editing UI (TableDesigner, ColumnGrid)
3. Manages local state with Zustand
4. Communicates with extension via `postMessage` API

### Shared Code

The `src/shared/` folder contains:

- **model.ts**: Zod schemas for UC objects (Catalog, Schema, Table, Column)
- **ops.ts**: Zod schemas for operations (add, rename, drop, etc.)

These are shared between extension and webview for type safety.

## Data Flow

```
User Action (Webview)
  ↓
Zustand Store (generates Op with UUID)
  ↓
postMessage({ type: 'append-ops', payload: [op] })
  ↓
Extension Host receives message
  ↓
appendOps() in storage.ts
  ↓
Read schemax.project.json
  ↓
Validate & append ops
  ↓
Apply ops to state (reducer)
  ↓
Write schemax.project.json
  ↓
postMessage({ type: 'project-updated', payload: project })
  ↓
Webview receives message
  ↓
Zustand Store updates
  ↓
React re-renders
```

## Key Design Decisions

### Append-Only Operations

- Every mutation generates an operation record
- Operations are never edited or deleted
- State is derived by replaying operations
- This enables:
  - Audit trail
  - Migration planning
  - Rename/reorder tracking (no ambiguity)

### Stable IDs

- All objects have UUID v4 IDs
- IDs are generated client-side (webview)
- IDs are never shown to users
- This enables:
  - Reliable operation targeting
  - Conflict-free merging (future)
  - Cross-reference integrity

### Single Source of Truth

- The extension owns the project file
- The webview never directly writes files
- All mutations go through the extension
- The extension sends authoritative updates back

## Component Hierarchy

```
App
├── Toolbar (Add buttons, modals)
├── Content
    ├── Sidebar (Tree view of catalogs/schemas/tables)
    └── TableDesigner
        ├── Table metadata
        └── ColumnGrid (Column editor with drag-reorder)
```

## State Management

### Zustand Store (useDesignerStore)

State:
- `project: ProjectFile | null` - The full project
- `selectedCatalogId: string | null`
- `selectedSchemaId: string | null`
- `selectedTableId: string | null`

Actions:
- Selection actions: `selectCatalog`, `selectSchema`, `selectTable`
- Mutation actions: `addCatalog`, `renameTable`, `addColumn`, etc.
- Helper actions: `findCatalog`, `findSchema`, `findTable`

All mutation actions:
1. Generate a new `Op` with timestamp and UUID
2. Call `emitOps([op])` which posts message to extension
3. Do NOT directly mutate local state
4. Wait for extension to send back updated project

## File I/O

### storage.ts Functions

- `ensureProjectFile(workspaceUri)` - Create if missing
- `readProject(workspaceUri)` - Read and parse
- `writeProject(workspaceUri, project)` - Write with stable key order
- `appendOps(workspaceUri, ops)` - Atomic append + apply
- `applyOpsToState(state, ops)` - Reducer function

### applyOpsToState Reducer

Implements all operation types:
- `add_catalog` → push to `state.catalogs`
- `rename_catalog` → find catalog, update name
- `add_column` → find table, insert column
- `reorder_columns` → find table, reorder by ID array
- etc.

Critical: Must handle all ops in `src/shared/ops.ts`.

## Webview → Extension Message Protocol

### Webview sends:

```typescript
{ type: 'load-project' }
{ type: 'append-ops', payload: Op[] }
```

### Extension sends:

```typescript
{ type: 'project-loaded', payload: ProjectFile }
{ type: 'project-updated', payload: ProjectFile }
```

## Building

### Extension (esbuild)

- Entry: `src/extension.ts`
- Output: `dist/extension.js`
- Format: CommonJS (for Node.js)
- External: `vscode` module

### Webview (Vite)

- Entry: `src/webview/index.html` → `src/webview/main.tsx`
- Output: `media/index.html` + `media/assets/index.{js,css}`
- Format: ES modules (for browser)
- Plugins: `@vitejs/plugin-react`

## VS Code Integration

### package.json Contributions

```json
"activationEvents": [
  "onCommand:schemax.openDesigner",
  "onCommand:schemax.showLastOps"
],
"contributes": {
  "commands": [...]
}
```

### Webview Configuration

```typescript
const panel = vscode.window.createWebviewPanel(
  'schemaxDesigner',
  'SchemaX Designer',
  vscode.ViewColumn.One,
  {
    enableScripts: true,
    retainContextWhenHidden: true,
    localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
  }
);
```

## Testing Checklist

- [ ] Extension activates on command
- [ ] Webview opens and loads project
- [ ] Add catalog → creates op and updates state
- [ ] Add schema → creates op under correct catalog
- [ ] Add table → creates op under correct schema
- [ ] Add column → creates op with stable ID
- [ ] Rename column → creates rename_column op
- [ ] Reorder columns (drag) → creates reorder_columns op
- [ ] Drop column → creates drop_column op
- [ ] Set table comment → creates set_table_comment op
- [ ] File `schemax.project.json` created on first mutation
- [ ] File persists across webview reloads
- [ ] "Show Last Emitted Changes" displays ops correctly
- [ ] Warning badge appears for rename/drop column (column mapping)

## Future Enhancements

1. **Migration Plan Generation**
   - Replay ops since last snapshot
   - Generate SQL DDL per environment
   - Handle renames vs drop+add

2. **Conflict Resolution**
   - Merge ops from multiple users
   - Detect conflicting changes
   - Interactive resolution UI

3. **Validation**
   - Name regex enforcement
   - Type compatibility checks
   - Constraint validation

4. **Git Integration**
   - Diff viewer for schemax.project.json
   - Commit/push from designer
   - Branch-based workflows

5. **Import/Export**
   - Import from existing Unity Catalog
   - Export to Terraform/dbt
   - Round-trip sync

## Troubleshooting

### Webview not loading

- Check `media/index.html` exists
- Check browser console (Help → Toggle Developer Tools → "Console")
- Verify asset paths in HTML (`/assets/index.js`)

### Operations not saving

- Check extension host console (Help → Toggle Developer Tools → "Output" → "SchemaX")
- Verify workspace folder is open
- Check file permissions on workspace

### Build errors

- Run `npm install` to ensure dependencies
- Check Node version (requires 18+)
- Clear `dist/` and `media/` folders, rebuild

### TypeScript errors

- Run `npm run build` to see full error output
- Check `tsconfig.json` paths
- Ensure shared types are imported correctly

