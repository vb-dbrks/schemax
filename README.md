# SchemaX VS Code Extension

Visual SchemaX Designer for Databricks Unity Catalog.

## Features

- **Visual Designer**: Create and manage Unity Catalog objects (catalogs, schemas, tables, columns) in a user-friendly React interface
- **Change Journal**: Every mutation is recorded as an operation in an append-only journal (`schemax.project.json`)
- **Forward-Compatible**: Stable IDs and operation history enable migration tooling and rename/reorder tracking
- **Local-First**: All data is stored in your workspace—no external servers

## Commands

- **SchemaX: Open Designer** (`schemax.openDesigner`): Open the visual designer webview
- **SchemaX: Show Last Emitted Changes** (`schemax.showLastOps`): View the last 20 operations in the output channel

## Getting Started

1. Install the extension
2. Open a workspace folder
3. Run command: "SchemaX: Open Designer"
4. Start creating catalogs, schemas, tables, and columns
5. All changes are automatically saved to `schemax.project.json`

## Development

### Prerequisites

- Node.js 18+
- npm

### Build

```bash
npm install
npm run build
```

### Watch Mode

```bash
npm run watch
```

### Package Extension

```bash
npm run package
```

### Testing

1. Press F5 in VS Code to launch the extension in debug mode
2. In the Extension Development Host window:
   - Open a workspace folder (or create a new one)
   - Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
   - Run command: "SchemaX: Open Designer"
   - The designer webview should open
3. Test the functionality:
   - Click "Add Catalog" to create a catalog
   - Select the catalog and click "Add Schema"
   - Select the schema and click "Add Table"
   - Select the table and click "Add Column"
   - Try renaming, reordering columns (drag and drop)
   - Check that `schemax.project.json` is created in the workspace
4. Test the operations view:
   - Run command: "SchemaX: Show Last Emitted Changes"
   - Verify the operations appear in the output channel

## Project Structure

```
schemax-vscode/
  src/
    extension.ts          # Extension host code
    storage.ts            # Project file I/O and state management
    telemetry.ts          # Telemetry stub
    shared/
      model.ts            # Zod schemas for UC objects
      ops.ts              # Zod schemas for operations
    webview/
      main.tsx            # React entry point
      App.tsx             # Main React app
      styles.css          # Styling
      components/         # React components
      state/              # Zustand store
  media/                  # Built webview assets
  dist/                   # Built extension code
```

## Data Model

The extension stores data in `schemax.project.json`:

- **version**: Schema version (currently 1)
- **name**: Project name
- **environments**: Target environments (dev, test, prod)
- **state**: Current state of catalogs, schemas, tables, columns
- **ops**: Append-only operation journal
- **lastSnapshotHash**: For future use

## Operations

All mutations emit operations:

- `add_catalog`, `rename_catalog`, `drop_catalog`
- `add_schema`, `rename_schema`, `drop_schema`
- `add_table`, `rename_table`, `drop_table`
- `add_column`, `rename_column`, `reorder_columns`, `drop_column`
- `change_column_type`, `set_nullable`
- `set_table_comment`, `set_column_comment`
- `set_table_property`, `unset_table_property`

## License

MIT

