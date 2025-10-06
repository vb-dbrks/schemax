# SchemaX

A Visual Studio Code extension for managing Databricks Unity Catalog schema definitions with version control.

## Overview

SchemaX provides a visual designer for defining and managing Unity Catalog schemas. It uses an append-only operation log architecture with snapshots, enabling version-controlled schema evolution.

## Features

- **Visual Schema Designer**: Intuitive UI for creating and managing catalogs, schemas, tables, and columns
- **Table Properties**: Full support for Unity Catalog TBLPROPERTIES including Delta Lake configuration
- **Version Control**: Snapshot-based versioning with semantic versioning (v0.1.0, v0.2.0, etc.)
- **Change Tracking**: Append-only operation log tracks every schema modification
- **Inline Editing**: Edit table and column properties directly in the grid
- **Git-Friendly**: JSON-based storage optimized for version control systems

## Installation

### From VSIX (Development)

1. Download the latest `.vsix` file from releases
2. Open VS Code
3. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
4. Type "Install from VSIX" and select the downloaded file

### From Marketplace (Coming Soon)

Search for "SchemaX" in the VS Code Extensions marketplace.

## Quick Start

1. Open a workspace folder in VS Code
2. Press `Cmd+Shift+P` and run **SchemaX: Open Designer**
3. Use the toolbar to add catalogs, schemas, and tables
4. Make changes to your schema
5. Create a snapshot: `Cmd+Shift+P` → **SchemaX: Create Snapshot**

## Usage

### Creating Schema Objects

- **Add Catalog**: Click "Add Catalog" in the toolbar
- **Add Schema**: Select a catalog, then click "Add Schema"
- **Add Table**: Select a schema, then click "Add Table"
- **Add Column**: Select a table, then click "Add Column"

### Editing Columns

Click the "Edit" button on any column to modify:
- Column name
- Data type (STRING, INT, BIGINT, DOUBLE, etc.)
- Nullable flag
- Comment

Click "Save" to apply changes.

### Managing Table Properties

Table properties (TBLPROPERTIES) allow you to configure Delta Lake settings and add custom metadata:

1. Select a table to view its properties
2. Scroll to the **Table Properties** section
3. Click **+ Add Property** to add a new property
4. Enter the property key and value
5. Click **Add** to save

**Common Delta Lake properties:**
- `delta.appendOnly` - Disable UPDATE/DELETE operations
- `delta.logRetentionDuration` - History retention (e.g., `interval 30 days`)
- `delta.deletedFileRetentionDuration` - VACUUM retention (e.g., `interval 7 days`)
- `delta.enableChangeDataFeed` - Enable change data capture

For a complete list, see the [Unity Catalog TBLPROPERTIES documentation](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-tblproperties).

### Creating Snapshots

Snapshots capture the complete state of your schema at a point in time:

1. Make changes to your schema
2. Run `Cmd+Shift+P` → **SchemaX: Create Snapshot**
3. Enter a name (e.g., "Initial schema")
4. Optionally add a description

Snapshots are stored in `.schemax/snapshots/` and can be used to:
- Track schema evolution over time
- Generate migration scripts (coming soon)
- Deploy to different environments (coming soon)

### Viewing Changes

- **Snapshot Panel**: Shows version history and uncommitted changes
- **Last Operations**: Run **SchemaX: Show Last Emitted Changes** to view recent operations

## File Structure

SchemaX creates a `.schemax` directory in your workspace:

```
.schemax/
├── project.json           # Project metadata
├── changelog.json         # Uncommitted changes
└── snapshots/
    ├── v0.1.0.json       # Snapshot files
    ├── v0.2.0.json
    └── v0.3.0.json
```

### Version Control

We recommend committing the entire `.schemax` directory to version control:
- Snapshots represent stable releases
- Changelog shows work in progress
- Easy to review changes in pull requests

## Architecture

SchemaX uses a two-tier architecture:

1. **Snapshots**: Point-in-time captures of complete schema state
   - Stored as separate files for each version
   - Immutable once created
   - Lightweight metadata in `project.json`

2. **Changelog**: Tracks operations since the last snapshot
   - Append-only operation log
   - Cleared when a snapshot is created
   - Represents uncommitted work

This design provides:
- Fast loading (only latest snapshot + changelog)
- Clean git diffs (snapshots rarely change)
- Efficient storage (bounded growth)

## Development

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for information on:
- Building from source
- Extension architecture
- Contributing guidelines
- Testing procedures

## Roadmap

- [ ] SQL migration script generation
- [ ] Databricks workspace deployment
- [ ] Drift detection
- [ ] Multi-environment support
- [ ] Rollback capabilities

## Requirements

- Visual Studio Code 1.90.0 or higher
- A workspace folder (schemas are workspace-specific)

## Known Limitations

- Currently supports schema definition only (no deployment)
- Single-user workflow (no concurrent editing)
- Limited to Unity Catalog object types (catalogs, schemas, tables, columns)

## Support

For issues, feature requests, or questions:
- GitHub Issues: [Create an issue](https://github.com/vb-dbrks/schemax-vscode/issues)
- Documentation: [docs/](docs/)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Team

### Development Team
Developed by **Field Engineering**

### Contributors
- [Varun Bhandary](https://github.com/vb-dbrks) - Creator & Lead Developer

We welcome contributions from the community! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
