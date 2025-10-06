# Testing SchemaX Extension on Mac

## Prerequisites

- VS Code installed
- Node.js 18+ installed
- Dependencies installed (already done with `npm install`)

## Method 1: F5 Debug Launch (Recommended)

### Step 1: Open Project in VS Code

```bash
cd /Users/varun.bhandary/Documents/side-projects/schemax-vscode
code .
```

### Step 2: Launch Extension

1. Press **F5** (or click Run → Start Debugging)
2. VS Code will:
   - Build the extension automatically
   - Open a new "Extension Development Host" window
   - Load your extension in that window

### Step 3: Create Test Workspace

In the Extension Development Host window:
1. Press **Cmd+Shift+N** for new window, or
2. File → Open Folder...
3. Create a new folder (e.g., `test-workspace`) or use existing
4. Select it

### Step 4: Open SchemaX Designer

1. Press **Cmd+Shift+P** to open command palette
2. Type: `SchemaX: Open Designer`
3. Press Enter

The designer webview will open!

### Step 5: Test Features

**Create a Catalog:**
1. Click "Add Catalog" button
2. Enter name: `test_catalog`
3. Click Create

**Add a Schema:**
1. Click on the catalog to select it (should highlight)
2. Click "Add Schema" button (now enabled)
3. Enter name: `bronze`
4. Click Create

**Add a Table:**
1. Click on the schema to select it
2. Click "Add Table" button
3. Enter name: `orders`
4. Select format: Delta
5. Click Create

**Add Columns:**
1. Click on the table to select it
2. Click "Add Column" button
3. Create columns:
   - `id` (BIGINT, not nullable)
   - `created_at` (TIMESTAMP, not nullable)
   - `amount` (DECIMAL(10,2), nullable)

**Try Advanced Features:**
- **Rename**: Click ✏️ icon next to any object name
- **Reorder columns**: Drag the ⋮⋮ handle
- **Add comment**: Click "Edit" next to comment field
- **Delete**: Click 🗑️ icon (with confirmation)

### Step 6: View Operations

1. Press **Cmd+Shift+P**
2. Type: `SchemaX: Show Last Emitted Changes`
3. Press Enter

You'll see all operations in the Output panel.

### Step 7: Check Project File

In your test workspace folder, you should now have:
```
test-workspace/
  └── schemax.project.json
```

Open it to see the complete project state and operation history!

## Method 2: Manual Build & Install

If you want to install the extension permanently:

### Step 1: Build the Extension

```bash
cd /Users/varun.bhandary/Documents/side-projects/schemax-vscode
npm run build
```

### Step 2: Package the Extension

```bash
npm run package
```

This creates `schemax-vscode-0.0.1.vsix`

### Step 3: Install the VSIX

```bash
code --install-extension schemax-vscode-0.0.1.vsix
```

Or in VS Code:
1. Press **Cmd+Shift+P**
2. Type: `Extensions: Install from VSIX...`
3. Select the `.vsix` file

### Step 4: Reload VS Code

1. Press **Cmd+Shift+P**
2. Type: `Developer: Reload Window`
3. Press Enter

### Step 5: Use the Extension

Now it's installed like any other extension:
1. Open any workspace folder
2. Press **Cmd+Shift+P**
3. Type: `SchemaX: Open Designer`

## Method 3: Watch Mode (For Development)

If you're making changes and want auto-rebuild:

### Terminal 1: Start Watch Mode

```bash
npm run watch
```

This will auto-rebuild both extension and webview when you save files.

### VS Code: Launch in Debug

1. Press **F5** as usual
2. Make code changes
3. In the Extension Development Host, press **Cmd+R** to reload the window
4. Your changes will be reflected

## Debugging Tips for Mac

### View Extension Host Console

In your main VS Code window (not Extension Development Host):
1. Go to Help → Toggle Developer Tools
2. Click "Console" tab
3. You'll see extension host logs

### View Webview Console

In the Extension Development Host window (where the designer is open):
1. Go to Help → Toggle Developer Tools
2. Click "Console" tab
3. You'll see React/webview logs

### View SchemaX Output

1. In Extension Development Host, press **Cmd+Shift+U** (Output panel)
2. In the dropdown, select "SchemaX"
3. You'll see extension-specific logs

### Restart Extension Host

If something seems stuck:
1. In Extension Development Host window
2. Press **Cmd+Shift+P**
3. Type: `Developer: Reload Window`
4. Press Enter

## Common Issues on Mac

### Port Already in Use

If you see build errors:
```bash
# Kill any processes using the ports
lsof -ti:5173 | xargs kill -9  # Vite dev server
```

### Permission Errors

If file writes fail:
```bash
# Check workspace folder permissions
ls -la /path/to/your/test-workspace
```

### Node Version Issues

Check your Node version:
```bash
node --version  # Should be 18+
```

If too old:
```bash
# Install latest LTS via nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
nvm install --lts
nvm use --lts
```

### Clean Build

If something seems broken:
```bash
# Clean everything and rebuild
rm -rf dist/ media/ node_modules/
npm install
npm run build
```

## Keyboard Shortcuts Summary

| Action | Mac Shortcut |
|--------|--------------|
| Start debugging | **F5** |
| Command palette | **Cmd+Shift+P** |
| Reload window | **Cmd+R** (in Extension Development Host) |
| Toggle Developer Tools | **Cmd+Option+I** |
| Open Output panel | **Cmd+Shift+U** |

## Testing Checklist

- [ ] Extension builds without errors (`npm run build`)
- [ ] F5 launches Extension Development Host
- [ ] "SchemaX: Open Designer" appears in command palette
- [ ] Designer webview opens
- [ ] Can create catalog
- [ ] Can create schema (button enables when catalog selected)
- [ ] Can create table (button enables when schema selected)
- [ ] Can create column (button enables when table selected)
- [ ] Table designer shows selected table
- [ ] Can add multiple columns
- [ ] Can rename column (✏️ button)
- [ ] Can drag to reorder columns (⋮⋮ handle)
- [ ] Can set table comment
- [ ] Can delete objects (with confirmation)
- [ ] "Show Last Emitted Changes" displays operations
- [ ] `schemax.project.json` appears in workspace
- [ ] File has correct JSON structure
- [ ] Reload window preserves state
- [ ] No console errors in Developer Tools

## Quick Smoke Test (2 minutes)

```bash
# 1. Launch
cd /Users/varun.bhandary/Documents/side-projects/schemax-vscode
code .
# Press F5

# 2. In Extension Development Host:
# - Open a folder
# - Cmd+Shift+P → "SchemaX: Open Designer"

# 3. Create objects:
# - Add catalog: "test"
# - Add schema: "test_schema"  
# - Add table: "test_table"
# - Add column: "id" (BIGINT)

# 4. Verify:
# - Cmd+Shift+P → "SchemaX: Show Last Emitted Changes"
# - Check workspace for schemax.project.json
```

If all of this works, you're good to go! 🎉

## Using the Example File

Want to test with pre-populated data?

1. Copy the example file to your test workspace:
   ```bash
   cp example-schemax.project.json ~/Desktop/test-workspace/schemax.project.json
   ```

2. Open the designer (Cmd+Shift+P → SchemaX: Open Designer)

3. You'll see a pre-populated catalog with schemas, tables, and columns!

## Next Steps

Once testing is complete:
- Push to GitHub: `git push -u origin main`
- Share with team members
- Consider publishing to VS Code Marketplace

---

**Happy testing!** 🚀

Need help? Check the console logs (Help → Toggle Developer Tools).

