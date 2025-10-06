# Troubleshooting Guide

## Issue: Blank Webview / Designer Not Loading

### Quick Fix (Just Applied!)

The webview HTML generation has been fixed. Now:

1. **Rebuild the extension:**
   ```bash
   npm run build:ext
   ```

2. **Reload the Extension Development Host:**
   - In the Extension Development Host window
   - Press **Cmd+R** (Mac) or **Ctrl+R** (Windows/Linux)
   - Or press **Cmd+Shift+P** → "Developer: Reload Window"

3. **Reopen the designer:**
   - Press **Cmd+Shift+P**
   - Type: `SchemaX: Open Designer`

### How to Debug Webview Issues

#### Step 1: Open Developer Tools

In the Extension Development Host window (where the designer is):
1. Go to **Help → Toggle Developer Tools**
2. Click the **Console** tab

#### Step 2: Check for Errors

Look for:
- ❌ Red error messages
- ⚠️ Yellow warning messages
- 🔵 Blue network errors (failed to load scripts/styles)

Common errors and fixes:

**Error: "Refused to load script"**
- **Cause**: Content Security Policy blocking scripts
- **Fix**: Ensure nonce is properly set (now fixed in extension.ts)

**Error: "Failed to load resource"**
- **Cause**: Asset paths incorrect
- **Fix**: Check that `media/assets/index.js` and `index.css` exist
- **Verify**: `ls -la media/assets/`

**Error: "Cannot find module 'vscode'"**
- **Cause**: Wrong context (trying to import vscode in webview)
- **Fix**: Use `acquireVsCodeApi()` in webview, not `import vscode`

#### Step 3: Check Network Tab

1. In Developer Tools, click **Network** tab
2. Reload the webview (Cmd+R)
3. Look for failed requests (red text)

All assets should load successfully:
- ✅ `index.js` - Status 200
- ✅ `index.css` - Status 200

#### Step 4: Check Console Logs

Look for our debug messages:
- `[Telemetry] extension_activated`
- `[Telemetry] designer_opened`
- `[Telemetry] project_loaded`

#### Step 5: Verify Files Exist

```bash
# Check media folder
ls -la media/
# Should show: index.html, assets/

# Check assets
ls -la media/assets/
# Should show: index.js, index.css

# Check extension
ls -la dist/
# Should show: extension.js, extension.js.map
```

## Issue: "Cannot read properties of null"

### Symptoms
- Webview opens but shows errors in console
- Parts of UI don't render

### Solution

1. **Check if project file exists:**
   ```bash
   ls -la <your-workspace>/schemax.project.json
   ```

2. **If missing, it will be auto-created** when you try to make changes

3. **Check project file format:**
   ```bash
   cat <your-workspace>/schemax.project.json | jq .
   ```
   Should show valid JSON with `version`, `state`, `ops`, etc.

## Issue: Changes Not Saving

### Symptoms
- Can create objects in UI
- But `schemax.project.json` not created/updated
- Or changes disappear on reload

### Debug Steps

1. **Check Extension Host Output:**
   - In main VS Code window (not Extension Development Host)
   - View → Output → Select "SchemaX" from dropdown
   - Look for error messages

2. **Check workspace folder:**
   ```bash
   # Ensure you have a workspace folder open
   # Not just individual files
   ```

3. **Check file permissions:**
   ```bash
   # Can you write to the workspace?
   touch <workspace>/test.txt
   rm <workspace>/test.txt
   ```

4. **Check console for errors:**
   - In Extension Development Host
   - Help → Toggle Developer Tools → Console
   - Look for "Failed to append operations"

## Issue: "Command not found"

### Symptoms
- "SchemaX: Open Designer" doesn't appear in command palette

### Solution

1. **Check extension is activated:**
   - Look for activation event in Extension Host console
   - Main VS Code → Help → Toggle Developer Tools → Console

2. **Rebuild and reload:**
   ```bash
   npm run build
   ```
   Then press **Cmd+Shift+P** → "Developer: Reload Window"

3. **Check package.json:**
   - Ensure `activationEvents` includes commands
   - Ensure `contributes.commands` lists both commands

## Issue: Build Errors

### "Cannot find module"

```bash
# Install dependencies
npm install

# If still failing, clean install
rm -rf node_modules package-lock.json
npm install
```

### "ESBUILD: errors"

```bash
# Check for TypeScript errors first
npx tsc --noEmit -p tsconfig.json

# Clean build
rm -rf dist/
npm run build:ext
```

### "Vite: errors"

```bash
# Check webview TypeScript
npx tsc --noEmit -p tsconfig.webview.json

# Clean build
rm -rf media/
npm run build:webview
```

## Issue: Webview Shows Stale Data

### Symptoms
- Made changes to code
- Webview still shows old behavior

### Solution

1. **Hard reload the webview:**
   - In Extension Development Host
   - Press **Cmd+Shift+R** (or Ctrl+Shift+R)
   - This reloads without cache

2. **Rebuild and reload:**
   ```bash
   npm run build
   ```
   Then **Cmd+R** in Extension Development Host

3. **Clear VS Code cache:**
   ```bash
   # Close VS Code completely
   # Delete cache
   rm -rf ~/.vscode/extensions/.obsolete
   # Reopen and press F5
   ```

## Issue: Drag-and-Drop Not Working

### Symptoms
- Can't reorder columns
- Drag handle (⋮⋮) doesn't work

### Debug

1. **Check browser console for errors:**
   - Help → Toggle Developer Tools → Console

2. **Verify table is selected:**
   - You must select a table first
   - Table designer should show in right pane

3. **Try in different browser:**
   - Webview uses system browser engine
   - Check if drag events are firing

## Issue: React Not Rendering

### Symptoms
- Blank webview with no errors
- React components not appearing

### Debug

1. **Check if React loaded:**
   ```javascript
   // In webview console (Help → Toggle Developer Tools)
   window.React
   // Should show React object, not undefined
   ```

2. **Check if root element exists:**
   ```javascript
   document.getElementById('root')
   // Should show <div id="root">...</div>
   ```

3. **Check if Zustand store initialized:**
   ```javascript
   // Should see store methods in console
   ```

## Issue: Operations Not Appearing

### Symptoms
- "Show Last Emitted Changes" shows "No operations yet"
- But you made changes in the UI

### Debug

1. **Check if ops were actually sent:**
   - In webview console: Look for `postMessage` calls
   - Should see messages like: `{type: 'append-ops', payload: [...]}`

2. **Check if extension received them:**
   - In Extension Host output: Look for "ops_appended"
   - View → Output → "SchemaX"

3. **Check project file:**
   ```bash
   cat <workspace>/schemax.project.json | jq '.ops'
   # Should show array of operations
   ```

## Debugging Checklist

Before asking for help, verify:

- [ ] `npm install` completed successfully
- [ ] `npm run build` completed without errors
- [ ] `media/assets/index.js` exists
- [ ] `media/assets/index.css` exists
- [ ] `dist/extension.js` exists
- [ ] Workspace folder is open (not just files)
- [ ] Extension Development Host launched via F5
- [ ] Developer Tools console shows no errors
- [ ] Network tab shows all resources loaded (200 status)

## Getting More Debug Info

### Enable Verbose Logging

Edit `src/telemetry.ts`:
```typescript
export function trackEvent(eventName: string, properties?: Record<string, any>): void {
  console.log(`[SchemaX] ${eventName}`, properties);
}
```

Rebuild and watch console for detailed logs.

### Check Extension Host Logs

Main VS Code window → Help → Toggle Developer Tools → Console

Look for:
- Extension activation messages
- File I/O operations
- Error stack traces

### Check Webview Logs

Extension Development Host → Help → Toggle Developer Tools → Console

Look for:
- React render errors
- Store mutations
- Message passing
- Network requests

## Common Fixes Summary

| Problem | Quick Fix |
|---------|-----------|
| Blank webview | Rebuild: `npm run build:ext`, then reload window |
| Stale UI | Hard reload: Cmd+Shift+R in Extension Development Host |
| Build errors | `rm -rf node_modules && npm install && npm run build` |
| Can't save | Check workspace folder is open |
| No commands | `npm run build`, then reload window |
| Console errors | Check Developer Tools → Console for specific error |

## Still Not Working?

1. **Try the nuclear option:**
   ```bash
   # Complete clean build
   rm -rf node_modules dist media package-lock.json
   npm install
   npm run build
   ```

2. **Check Node version:**
   ```bash
   node --version  # Should be 18+
   ```

3. **Check VS Code version:**
   ```bash
   code --version  # Should be 1.90.0+
   ```

4. **Try in a new workspace:**
   - Create a brand new empty folder
   - Open it in Extension Development Host
   - Try the designer again

5. **Check the example file:**
   ```bash
   # Copy example to workspace
   cp example-schemax.project.json <workspace>/schemax.project.json
   # Reopen designer - should show populated data
   ```

## Success Indicators

When everything is working:

✅ Webview opens showing toolbar and sidebar
✅ Can click "Add Catalog" and modal appears
✅ Created catalog appears in tree view
✅ Console shows no errors
✅ Network tab shows all resources loaded
✅ `schemax.project.json` created in workspace
✅ "Show Last Changes" displays operations

---

**Still having issues?** Check the console logs and compare against this guide.

