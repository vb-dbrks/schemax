# Debug Steps - Blank Webview

Follow these steps exactly to help me diagnose the issue:

## Step 1: Check Browser Console

1. In the **Extension Development Host** window (where the blank designer is)
2. Go to **Help → Toggle Developer Tools**
3. Click the **Console** tab
4. Copy and paste ALL the output here

Look for:
- Red error messages
- Failed to load messages
- Any warnings

## Step 2: Check Network Tab

1. Still in Developer Tools
2. Click the **Network** tab
3. Refresh the webview (Cmd+R)
4. Look for any failed requests (red text)
5. Take a screenshot or list the files and their status codes

## Step 3: Check Extension Host Console

1. In the **MAIN** VS Code window (not Extension Development Host)
2. Go to **Help → Toggle Developer Tools**
3. Click the **Console** tab
4. Look for any errors related to "schemax"
5. Copy any relevant errors

## Step 4: Check File Structure

Run this in terminal:
```bash
ls -la media/assets/
ls -la dist/
```

## Step 5: Check Webview HTML

The webview should be loading. Let's verify the HTML is being generated correctly.

In the Extension Host Console (main window), look for any errors when creating the webview.

