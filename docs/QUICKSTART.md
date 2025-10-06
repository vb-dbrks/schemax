# Quick Start Guide

Get started with SchemaX in 5 minutes.

## Installation

1. Install the extension from the VS Code Marketplace (search for "SchemaX")
2. Open a workspace folder in VS Code
3. You're ready to go!

## Create Your First Schema

### Step 1: Open the Designer

1. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
2. Type "SchemaX: Open Designer"
3. Press Enter

The designer window will open with an empty canvas.

### Step 2: Create a Catalog

1. Click **Add Catalog** in the toolbar
2. Enter a name (e.g., "my_catalog")
3. Press Enter

You'll see your catalog appear in the sidebar tree.

### Step 3: Create a Schema

1. Click on the catalog in the sidebar to select it
2. Click **Add Schema** in the toolbar
3. Enter a name (e.g., "sales")
4. Press Enter

The schema appears under the catalog.

### Step 4: Create a Table

1. Click on the schema in the sidebar
2. Click **Add Table** in the toolbar
3. Enter a name (e.g., "orders")
4. Select table format: **Delta** or **Iceberg**
5. Choose column mapping mode (optional)
6. Click **Create**

The table appears under the schema.

### Step 5: Add Columns

1. Click on the table in the sidebar
2. The table designer appears in the main panel
3. Click **Add Column**
4. Enter column details:
   - Name: `order_id`
   - Type: `BIGINT`
   - Nullable: unchecked
   - Comment: "Unique order identifier"
5. Click **Create**

The column appears in the column grid.

### Step 6: Edit Columns

To edit an existing column:

1. Find the column in the grid
2. Click the **Edit** button
3. Modify the name, type, nullable flag, or comment
4. Click **Save** (or **Cancel** to discard changes)

### Step 7: Reorder Columns

To change column order:

1. Hover over the handle icon (⋮⋮) on the left of a column row
2. Click and drag to a new position
3. Release to drop

### Step 8: Create a Snapshot

Once you're happy with your schema:

1. Press `Cmd+Shift+P` / `Ctrl+Shift+P`
2. Run **SchemaX: Create Snapshot**
3. Enter a name (e.g., "Initial schema")
4. Add an optional comment
5. Press Enter

Your snapshot is created! Check the **Snapshots** panel on the right to see it.

## Understanding the UI

### Toolbar

- **Add Catalog** - Create a new catalog
- **Add Schema** - Create a schema in the selected catalog
- **Add Table** - Create a table in the selected schema
- **Add Column** - Add a column to the selected table

### Sidebar

The tree view shows your schema hierarchy:

```
📁 Catalog
  └── 📂 Schema
      └── 📋 Table
```

**Actions:**
- Click to select
- Right-click for rename/delete options

### Table Designer

The main panel shows:
- Table properties (name, format, column mapping)
- Column grid with inline editing
- Add/edit/delete column actions

### Snapshot Panel

Shows:
- Version history
- Uncommitted changes badge
- Snapshot metadata (name, date, operations count)

## Working with Changes

### View Recent Changes

To see what operations you've performed:

1. Press `Cmd+Shift+P` / `Ctrl+Shift+P`
2. Run **SchemaX: Show Last Emitted Changes**
3. View the operations in the output panel

### Uncommitted Changes

After creating a snapshot, any new changes are "uncommitted":
- They appear in the changelog
- They're shown with a badge in the Snapshot panel
- Create another snapshot to commit them

### File Structure

SchemaX creates a `.schemax` directory in your workspace:

```
.schemax/
├── project.json           # Project metadata
├── changelog.json         # Uncommitted changes
└── snapshots/
    └── v0.1.0.json       # Your first snapshot
```

**Pro tip:** Commit this directory to git for version control!

## Common Tasks

### Rename an Object

1. Right-click the object in the sidebar
2. Select "Rename"
3. Enter new name
4. Press Enter

### Delete an Object

1. Right-click the object in the sidebar
2. Select "Drop" (or "Delete")
3. Confirm the action

**Warning:** Deleting a catalog/schema also deletes all child objects!

### Change Column Type

1. Select the table
2. Find the column in the grid
3. Click **Edit**
4. Change the type from the dropdown
5. Click **Save**

### Add Table Comment

1. Select the table
2. Click **Set Comment** in the table properties section
3. Enter the comment
4. Click **Save**

### Add Column Comment

1. Select the table
2. Find the column in the grid
3. Click **Edit**
4. Enter the comment in the Comment field
5. Click **Save**

### Add Table Properties

Table properties configure Delta Lake behavior and store custom metadata:

1. Select a table
2. Scroll to **Table Properties (TBLPROPERTIES)** section
3. Click **+ Add Property**
4. Enter property key (e.g., `delta.appendOnly`)
5. Enter value (e.g., `true`)
6. Click **Add**

**Example Properties:**
- `delta.appendOnly = true` - Make table append-only
- `delta.logRetentionDuration = interval 30 days` - Keep 30 days of history
- `custom.owner = data-engineering` - Custom metadata

### Edit or Delete Properties

- **Edit**: Click the **Edit** button, modify key/value, click **Save**
- **Delete**: Click the **Delete** button, confirm deletion

## Tips and Tricks

### Keyboard Shortcuts

- `Cmd+Shift+P` / `Ctrl+Shift+P` - Command palette (access all SchemaX commands)

### Best Practices

1. **Create snapshots regularly** - They represent stable states
2. **Use descriptive names** - Both for objects and snapshots
3. **Add comments** - Document your schema inline
4. **Commit to git** - Version control your schema definitions
5. **Review changes** - Use "Show Last Changes" before creating snapshots

### Organizing Schemas

**By domain:**
```
my_catalog
  ├── sales_schema
  ├── marketing_schema
  └── finance_schema
```

**By environment:**
```
dev_catalog
test_catalog
prod_catalog
```

**By team:**
```
shared_catalog
  ├── team_a_schema
  ├── team_b_schema
  └── common_schema
```

## Next Steps

- Read the [README](../README.md) for feature overview
- Check [ARCHITECTURE.md](ARCHITECTURE.md) to understand the design
- See [DEVELOPMENT.md](DEVELOPMENT.md) if you want to contribute

## Getting Help

- **Issues or bugs:** [GitHub Issues](https://github.com/vb-dbrks/schemax-vscode/issues)
- **Questions:** Check the docs or open a discussion
- **Output logs:** View → Output → SchemaX

## Troubleshooting

### Designer is blank

1. Check VS Code Output panel (View → Output)
2. Select "SchemaX" from dropdown
3. Look for errors
4. Try reloading: `Cmd+R` / `Ctrl+R`

### Changes not saving

1. Check SchemaX output logs
2. Verify `.schemax/` directory exists
3. Check file permissions
4. Ensure workspace folder is open (not just files)

### Snapshots not appearing

1. Check `project.json` exists in `.schemax/`
2. Verify snapshots are listed in metadata
3. Reload the designer
4. Check for errors in output logs

---

**Happy schema designing!**

