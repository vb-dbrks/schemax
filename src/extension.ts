import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { ensureProjectFile, readProject, appendOps, createSnapshot, getUncommittedOps } from './storage';
import { Op } from './shared/ops';
import { trackEvent } from './telemetry';

let outputChannel: vscode.OutputChannel;
let currentPanel: vscode.WebviewPanel | undefined;

export function activate(context: vscode.ExtensionContext) {
  console.log('[SchemaX] Extension activating...');
  outputChannel = vscode.window.createOutputChannel('SchemaX');
  outputChannel.appendLine('SchemaX Extension Activated!');
  outputChannel.appendLine(`Extension path: ${context.extensionPath}`);

  // Register commands
  const openDesignerCommand = vscode.commands.registerCommand(
    'schemax.openDesigner',
    () => openDesigner(context)
  );

  const showLastOpsCommand = vscode.commands.registerCommand(
    'schemax.showLastOps',
    () => showLastOps()
  );

  const createSnapshotCommand = vscode.commands.registerCommand(
    'schemax.createSnapshot',
    () => createSnapshotCommand_impl()
  );

  context.subscriptions.push(openDesignerCommand, showLastOpsCommand, createSnapshotCommand, outputChannel);

  console.log('[SchemaX] Extension activated successfully!');
  console.log('[SchemaX] Commands registered: schemax.openDesigner, schemax.showLastOps');
  vscode.window.showInformationMessage('SchemaX Extension Activated!');
  trackEvent('extension_activated');
}

export function deactivate() {
  trackEvent('extension_deactivated');
}

/**
 * Open the SchemaX Designer webview
 */
async function openDesigner(context: vscode.ExtensionContext) {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  // Ensure project file exists
  await ensureProjectFile(workspaceFolder.uri);

  // If panel already exists, just reveal it
  if (currentPanel) {
    currentPanel.reveal(vscode.ViewColumn.One);
    return;
  }

  // Create webview panel
  currentPanel = vscode.window.createWebviewPanel(
    'schemaxDesigner',
    'SchemaX Designer',
    vscode.ViewColumn.One,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
      enableForms: true,
    }
  );

  // Set webview content
  console.log('[SchemaX] Setting webview HTML');
  currentPanel.webview.html = getWebviewContent(context, currentPanel.webview);
  console.log('[SchemaX] Webview HTML set');

  // Reset when panel is closed
  currentPanel.onDidDispose(() => {
    currentPanel = undefined;
  });

  // Handle messages from webview
  currentPanel.webview.onDidReceiveMessage(
    async (message) => {
      console.log('[SchemaX] Received message from webview:', message.type);
      switch (message.type) {
        case 'load-project': {
          try {
            console.log('[SchemaX] Loading project from:', workspaceFolder.uri.fsPath);
            const project = await readProject(workspaceFolder.uri);
            console.log('[SchemaX] Project loaded, sending to webview');
            currentPanel?.webview.postMessage({
              type: 'project-loaded',
              payload: project,
            });
            trackEvent('project_loaded');
          } catch (error) {
            console.error('[SchemaX] Failed to load project:', error);
            vscode.window.showErrorMessage(`Failed to load project: ${error}`);
          }
          break;
        }
        case 'append-ops': {
          try {
            const ops: Op[] = message.payload;
            console.log('[SchemaX] Appending ops:', ops.length);
            const updatedProject = await appendOps(workspaceFolder.uri, ops);
            console.log('[SchemaX] Updated project snapshots count:', updatedProject.snapshots?.length || 0);
            console.log('[SchemaX] Updated project ops count:', updatedProject.ops.length);
            currentPanel?.webview.postMessage({
              type: 'project-updated',
              payload: updatedProject,
            });
            trackEvent('ops_appended', { count: ops.length });
          } catch (error) {
            console.error('[SchemaX] Failed to append operations:', error);
            vscode.window.showErrorMessage(`Failed to append operations: ${error}`);
          }
          break;
        }
      }
    },
    undefined,
    context.subscriptions
  );

  trackEvent('designer_opened');
}

/**
 * Show last operations in output channel
 */
async function showLastOps() {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  try {
    const project = await readProject(workspaceFolder.uri);
    const lastOps = project.ops.slice(-20);

    outputChannel.clear();
    outputChannel.appendLine('SchemaX: Last 20 Emitted Changes');
    outputChannel.appendLine('='.repeat(80));
    outputChannel.appendLine('');

    if (lastOps.length === 0) {
      outputChannel.appendLine('No operations yet.');
    } else {
      lastOps.forEach((op, idx) => {
        outputChannel.appendLine(`${idx + 1}. [${op.ts}] ${op.op}`);
        outputChannel.appendLine(`   Target: ${op.target}`);
        outputChannel.appendLine(`   Payload: ${JSON.stringify(op.payload, null, 2)}`);
        outputChannel.appendLine('');
      });
    }

    outputChannel.show();
    trackEvent('last_ops_shown', { count: lastOps.length });
  } catch (error) {
    vscode.window.showErrorMessage(`Failed to read operations: ${error}`);
  }
}

/**
 * Get webview HTML content
 */
function getWebviewContent(context: vscode.ExtensionContext, webview: vscode.Webview): string {
  const scriptUri = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, 'media', 'assets', 'index.js')
  );
  const styleUri = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, 'media', 'assets', 'index.css')
  );

  // Use a nonce to only allow specific scripts to be run
  const nonce = getNonce();

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <link href="${styleUri}" rel="stylesheet">
  <title>SchemaX Designer</title>
</head>
<body>
  <div id="root"></div>
  <script type="module" nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
}

function getNonce() {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

/**
 * Create snapshot command implementation
 */
async function createSnapshotCommand_impl() {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  try {
    const project = await readProject(workspaceFolder.uri);
    const uncommittedOps = getUncommittedOps(project);

    if (uncommittedOps.length === 0) {
      vscode.window.showInformationMessage('No changes to snapshot. All operations are already included in the last snapshot.');
      return;
    }

    // Get snapshot name
    const name = await vscode.window.showInputBox({
      prompt: 'Snapshot name',
      placeHolder: 'e.g., "Added customer tables"',
      validateInput: (value) => {
        return value.trim() ? null : 'Snapshot name is required';
      }
    });

    if (!name) {
      return; // User cancelled
    }

    // Get optional comment
    const comment = await vscode.window.showInputBox({
      prompt: 'Description (optional)',
      placeHolder: 'Describe what changed in this snapshot'
    });

    // Create snapshot
    await vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: 'Creating snapshot...',
      cancellable: false
    }, async (progress) => {
      const updatedProject = await createSnapshot(workspaceFolder.uri, name, undefined, comment);
      const latestSnapshot = updatedProject.snapshots[updatedProject.snapshots.length - 1];
      
      progress.report({ increment: 100 });
      
      // Notify webview if it's open
      if (currentPanel) {
        console.log('[SchemaX] Notifying webview of snapshot creation');
        currentPanel.webview.postMessage({
          type: 'project-updated',
          payload: updatedProject
        });
      }
      
      vscode.window.showInformationMessage(
        `Snapshot created: ${latestSnapshot.version} - ${latestSnapshot.name} (${uncommittedOps.length} operations)`
      );
      
      trackEvent('snapshot_created', { 
        version: latestSnapshot.version, 
        opsCount: uncommittedOps.length 
      });
    });

  } catch (error) {
    vscode.window.showErrorMessage(`Failed to create snapshot: ${error}`);
    console.error('[SchemaX] Snapshot creation failed:', error);
  }
}

