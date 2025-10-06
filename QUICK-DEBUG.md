# Quick Debug - Let's Find the Issue

## Important: You're looking at the WRONG console!

The logs you shared are from the **main VS Code window** (your development window).

You need to look at **TWO different places**:

## 1. Extension Host Console (Main Window)

In your **MAIN VS Code window** (the one with the schemax-vscode project open):

1. Open **Help → Toggle Developer Tools**
2. In Developer Tools, click **Console** tab
3. Type this in the filter box at the top: `SchemaX`
4. Now run the command to open the designer
5. You should see:
   ```
   [SchemaX] Setting webview HTML
   [SchemaX] Webview HTML set
   [SchemaX] Received message from webview: load-project
   ```

## 2. Webview Console (Extension Development Host Window)

In the **Extension Development Host** window (the NEW window that opened when you pressed F5):

1. Open **Help → Toggle Developer Tools**  
2. In Developer Tools, click **Console** tab
3. You should see:
   ```
   [SchemaX Webview] App mounted
   [SchemaX Webview] Requesting project load
   ```

## If You Don't See [SchemaX] Messages

That means the extension isn't activating. Let's verify:

### Check 1: Is the extension loaded?

In the Extension Development Host window:
1. Press **Cmd+Shift+P**
2. Type: `Developer: Show Running Extensions`
3. Search for "schemax"
4. Is it in the list?

### Check 2: Try activating manually

In the Extension Development Host window console, type:
```javascript
vscode.commands.getCommands().then(cmds => console.log(cmds.filter(c => c.includes('schemax'))))
```

You should see:
```
['schemax.openDesigner', 'schemax.showLastOps']
```

## Still Nothing?

Try a complete rebuild:

```bash
cd /Users/varun.bhandary/Documents/side-projects/schemax-vscode
rm -rf dist/ media/
npm run build
```

Then:
1. Close ALL VS Code windows
2. Reopen the project
3. Press F5
4. Try again

## What to Tell Me

Please share:
1. ✅ Screenshot of Extension Development Host console after opening designer
2. ✅ Screenshot of main window console (filtered by "SchemaX")
3. ✅ Output of "Show Running Extensions" - is schemax-vscode listed?
4. ✅ What you see in the webview - blank white? blank with loading text? completely black?

