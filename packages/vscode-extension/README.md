# SchemaX VS Code Extension

Visual designer for Databricks Unity Catalog schema definitions with version control.

## Features

- **Visual Schema Designer**: Intuitive UI for creating and managing catalogs, schemas, tables, and columns
- **Data Governance**: Full support for constraints, tags, row filters, and column masks
- **Table Properties**: Configure Delta Lake TBLPROPERTIES
- **Version Control**: Snapshot-based versioning with semantic versioning
- **Change Tracking**: Append-only operation log tracks every modification
- **Inline Editing**: Edit table and column properties directly in the grid
- **Git-Friendly**: JSON-based storage optimized for version control

## Installation

### From VS Code Marketplace

1. Open VS Code
2. Go to Extensions (Cmd+Shift+X)
3. Search for "SchemaX"
4. Click Install

### From VSIX (Development)

1. Download the latest `.vsix` file
2. Open VS Code
3. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
4. Type "Install from VSIX" and select the file

## Quick Start

1. Open a workspace folder in VS Code
2. Press `Cmd+Shift+P` and run **SchemaX: Open Designer**
3. Click "Add Catalog" to create your first catalog
4. Build your schema using the visual designer
5. Create a snapshot: **SchemaX: Create Snapshot**

## Commands

- `SchemaX: Open Designer` - Open the visual schema designer
- `SchemaX: Create Snapshot` - Create a version snapshot
- `SchemaX: Show Last Emitted Changes` - View recent operations
- `SchemaX: Generate SQL Migration` - Generate SQL from changes

## File Structure

SchemaX creates a `.schemax` directory in your workspace:

```
.schemax/
├── project.json           # Project metadata
├── changelog.json         # Uncommitted changes
└── snapshots/
    ├── v0.1.0.json       # Snapshot files
    └── v0.2.0.json
```

## Unity Catalog Features

### Tables & Columns

- Multiple table formats (Delta, Iceberg)
- Column mapping modes (name, id)
- Nullable columns
- Comments and descriptions

### Data Governance

- **Column Tags**: Key-value metadata for classification
- **Constraints**: PRIMARY KEY, FOREIGN KEY, CHECK constraints
- **Row Filters**: Row-level security with UDF expressions
- **Column Masks**: Data masking functions

### Table Properties

Configure Delta Lake behavior:
- `delta.appendOnly` - Disable UPDATE/DELETE
- `delta.enableChangeDataFeed` - Enable CDC
- `delta.logRetentionDuration` - History retention
- Custom properties for metadata

## Publishing

### One-time setup

1. **Create a publisher** (if needed) at [Marketplace: Manage](https://marketplace.visualstudio.com/manage). The extension uses publisher ID `schematic-dev` (set in `package.json`).
2. **Create a Personal Access Token (PAT)** for the Marketplace:
   - Go to [Azure DevOps → Personal access tokens](https://dev.azure.com) (sign in with the same Microsoft account).
   - New token: **Organization** = *All accessible organizations*, **Scopes** = **Marketplace** → **Manage**. Copy the token (it’s shown only once).

### Release

From the repo root:

```bash
cd packages/vscode-extension
# Bump version in package.json, then:
npm run build
npm run deploy
# When prompted, paste your PAT (or set VSCE_PAT in the environment).
```

## Development

See the [main repository](https://github.com/vb-dbrks/schemax-vscode) for:
- Building from source
- Contributing guidelines
- Architecture documentation

## Requirements

- Visual Studio Code 1.90.0 or higher
- A workspace folder

## License

Apache License 2.0 - see [LICENSE](../../LICENSE) for details.

## Links

- **Repository**: https://github.com/vb-dbrks/schemax-vscode
- **Issues**: https://github.com/vb-dbrks/schemax-vscode/issues
- **Documentation**: https://github.com/vb-dbrks/schemax-vscode/tree/main/docs

