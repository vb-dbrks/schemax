import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as storageV3 from './storage-v3';
import { Operation } from './providers/base/operations';
import { trackEvent } from './telemetry';
import './providers'; // Initialize providers

let outputChannel: vscode.OutputChannel;
let currentPanel: vscode.WebviewPanel | undefined;

export function activate(context: vscode.ExtensionContext) {
  outputChannel = vscode.window.createOutputChannel('Schematic');
  outputChannel.appendLine('[Schematic] Extension activating...');
  outputChannel.appendLine('[Schematic] Extension Activated!');
  outputChannel.appendLine(`[Schematic] Extension path: ${context.extensionPath}`);

  // Register commands
  const openDesignerCommand = vscode.commands.registerCommand(
    'schematic.openDesigner',
    () => openDesigner(context)
  );

  const showLastOpsCommand = vscode.commands.registerCommand(
    'schematic.showLastOps',
    () => showLastOps()
  );

  const createSnapshotCommand = vscode.commands.registerCommand(
    'schematic.createSnapshot',
    () => createSnapshotCommand_impl()
  );

  const generateSQLCommand = vscode.commands.registerCommand(
    'schematic.generateSQL',
    () => generateSQLMigration()
  );

  context.subscriptions.push(openDesignerCommand, showLastOpsCommand, createSnapshotCommand, generateSQLCommand, outputChannel);

  outputChannel.appendLine('[Schematic] Extension activated successfully!');
  outputChannel.appendLine('[Schematic] Commands registered: schematic.openDesigner, schematic.showLastOps, schematic.createSnapshot');
  vscode.window.showInformationMessage('Schematic Extension Activated!');
  trackEvent('extension_activated');
}

export function deactivate() {
  trackEvent('extension_deactivated');
}

/**
 * Open the Schematic Designer webview
 */
async function openDesigner(context: vscode.ExtensionContext) {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('Schematic: Please open a workspace folder first.');
    return;
  }

  // Ensure project file exists (v3) - defaults to Unity provider
  await storageV3.ensureProjectFile(workspaceFolder.uri, 'unity');

  // If panel already exists, just reveal it
  if (currentPanel) {
    currentPanel.reveal(vscode.ViewColumn.One);
    return;
  }

  // Create webview panel
  currentPanel = vscode.window.createWebviewPanel(
    'schematicDesigner',
    'Schematic Designer',
    vscode.ViewColumn.One,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
      enableForms: true,
    }
  );

  // Set webview content
  outputChannel.appendLine('[Schematic] Setting webview HTML');
  currentPanel.webview.html = getWebviewContent(context, currentPanel.webview);
  outputChannel.appendLine('[Schematic] Webview HTML set');

  // Reset when panel is closed
  currentPanel.onDidDispose(() => {
    outputChannel.appendLine('[Schematic] Webview panel disposed');
    currentPanel = undefined;
  });

  // Handle messages from webview
  currentPanel.webview.onDidReceiveMessage(
    async (message) => {
      outputChannel.appendLine(`[Schematic] Received message from webview: ${message.type}`);
      switch (message.type) {
        case 'load-project': {
          try {
            outputChannel.appendLine(`[Schematic] Loading project from: ${workspaceFolder.uri.fsPath}`);
            
            // Load v3: project metadata + current state (snapshot + changelog) + provider
            const project = await storageV3.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV3.loadCurrentState(workspaceFolder.uri);
            
            outputChannel.appendLine(`[Schematic] Project loaded successfully (v${project.version})`);
            outputChannel.appendLine(`[Schematic] - Provider: ${provider.info.name} v${provider.info.version}`);
            outputChannel.appendLine(`[Schematic] - Snapshots: ${project.snapshots.length}`);
            outputChannel.appendLine(`[Schematic] - Latest snapshot: ${project.latestSnapshot || 'none'}`);
            outputChannel.appendLine(`[Schematic] - Changelog ops: ${changelog.ops.length}`);
            
            // For Unity provider, log catalog count (provider-specific)
            if (state && 'catalogs' in state) {
              outputChannel.appendLine(`[Schematic] - Catalogs: ${(state as any).catalogs.length}`);
            }
            
            // Send combined data to webview including provider info
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                id: provider.info.id,
                name: provider.info.name,
                version: provider.info.version,
                capabilities: provider.capabilities,
              },
            };
            
            outputChannel.appendLine(`[Schematic] Sending to webview:`);
            outputChannel.appendLine(`[Schematic] - Provider: ${provider.info.name}`);
            outputChannel.appendLine(`[Schematic] - Project version: ${project.version}`);
            
            currentPanel?.webview.postMessage({
              type: 'project-loaded',
              payload: payloadForWebview,
            });
            trackEvent('project_loaded', { provider: provider.info.id });
          } catch (error) {
            outputChannel.appendLine(`[Schematic] ERROR: Failed to load project: ${error}`);
            vscode.window.showErrorMessage(`Failed to load project: ${error}`);
          }
          break;
        }
        case 'append-ops': {
          try {
            const ops: Operation[] = message.payload;
            outputChannel.appendLine(`[Schematic] Appending ${ops.length} operation(s) to changelog`);
            
            // Append to changelog (v3 validates operations via provider)
            await storageV3.appendOps(workspaceFolder.uri, ops);
            
            // Reload state and provider
            const project = await storageV3.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV3.loadCurrentState(workspaceFolder.uri);
            
            outputChannel.appendLine(`[Schematic] Operations appended successfully`);
            outputChannel.appendLine(`[Schematic] - Changelog ops: ${changelog.ops.length}`);
            outputChannel.appendLine(`[Schematic] - Snapshots: ${project.snapshots.length}`);
            
            // Send updated data to webview including provider info
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                id: provider.info.id,
                name: provider.info.name,
                version: provider.info.version,
                capabilities: provider.capabilities,
              },
            };
            
            currentPanel?.webview.postMessage({
              type: 'project-updated',
              payload: payloadForWebview,
            });
            trackEvent('ops_appended', { count: ops.length, provider: provider.info.id });
          } catch (error) {
            outputChannel.appendLine(`[Schematic] ERROR: Failed to append operations: ${error}`);
            vscode.window.showErrorMessage(`Failed to append operations: ${error}`);
          }
          break;
        }
      }
    },
    undefined,
    context.subscriptions
  );

  outputChannel.appendLine('[Schematic] Designer opened successfully');
  trackEvent('designer_opened');
}

/**
 * Show last operations in output channel
 */
async function showLastOps() {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('Schematic: Please open a workspace folder first.');
    return;
  }

  try {
    const project = await storageV3.readProject(workspaceFolder.uri);
    const changelog = await storageV3.readChangelog(workspaceFolder.uri);
    const lastOps = changelog.ops.slice(-20);

    outputChannel.clear();
    outputChannel.appendLine('Schematic: Last 20 Emitted Changes');
    outputChannel.appendLine('='.repeat(80));
    outputChannel.appendLine(`Provider: ${project.provider.type}`);
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
    trackEvent('last_ops_shown', { count: lastOps.length, provider: project.provider.type });
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
  <title>Schematic Designer</title>
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
  outputChannel.appendLine('[Schematic] Create snapshot command invoked');
  
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    outputChannel.appendLine('[Schematic] ERROR: No workspace folder open');
    vscode.window.showErrorMessage('Schematic: Please open a workspace folder first.');
    return;
  }

  try {
    outputChannel.appendLine(`[Schematic] Reading project from: ${workspaceFolder.uri.fsPath}`);
    const project = await storageV3.readProject(workspaceFolder.uri);
    const changelog = await storageV3.readChangelog(workspaceFolder.uri);
    const uncommittedOpsCount = changelog.ops.length;
    
    outputChannel.appendLine(`[Schematic] Provider: ${project.provider.type}`);
    outputChannel.appendLine(`[Schematic] Uncommitted operations: ${uncommittedOpsCount}`);
    outputChannel.appendLine(`[Schematic] Existing snapshots: ${project.snapshots.length}`);

    if (uncommittedOpsCount === 0) {
      outputChannel.appendLine('[Schematic] No uncommitted operations, aborting snapshot creation');
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
      outputChannel.appendLine('[Schematic] Snapshot creation cancelled by user');
      return; // User cancelled
    }

    // Get optional comment
    const comment = await vscode.window.showInputBox({
      prompt: 'Description (optional)',
      placeHolder: 'Describe what changed in this snapshot'
    });

    outputChannel.appendLine(`[Schematic] Creating snapshot: "${name}"`);
    if (comment) {
      outputChannel.appendLine(`[Schematic] Comment: "${comment}"`);
    }

    // Create snapshot
    await vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: 'Creating snapshot...',
      cancellable: false
    }, async (progress) => {
      const { project: updatedProject, snapshot } = await storageV3.createSnapshot(
        workspaceFolder.uri, 
        name, 
        undefined, 
        comment
      );
      
      outputChannel.appendLine(`[Schematic] Snapshot created successfully!`);
      outputChannel.appendLine(`[Schematic] - ID: ${snapshot.id}`);
      outputChannel.appendLine(`[Schematic] - Version: ${snapshot.version}`);
      outputChannel.appendLine(`[Schematic] - Name: ${snapshot.name}`);
      outputChannel.appendLine(`[Schematic] - Operations included: ${snapshot.opsIncluded.length}`);
      outputChannel.appendLine(`[Schematic] - Total snapshots: ${updatedProject.snapshots.length}`);
      outputChannel.appendLine(`[Schematic] - Snapshot file: ${updatedProject.snapshots[updatedProject.snapshots.length - 1].file}`);
      
      progress.report({ increment: 100 });
      
      // Notify webview if it's open
      if (currentPanel) {
        outputChannel.appendLine('[Schematic] Notifying webview of snapshot creation');
        
        // Reload state and provider for webview
        const { state, changelog, provider } = await storageV3.loadCurrentState(workspaceFolder.uri);
        const payloadForWebview = {
          ...updatedProject,
          state,
          ops: changelog.ops,
          provider: {
            id: provider.info.id,
            name: provider.info.name,
            version: provider.info.version,
            capabilities: provider.capabilities,
          },
        };
        
        currentPanel.webview.postMessage({
          type: 'project-updated',
          payload: payloadForWebview
        });
      } else {
        outputChannel.appendLine('[Schematic] No webview panel open, skipping notification');
      }
      
      vscode.window.showInformationMessage(
        `Snapshot created: ${snapshot.version} - ${snapshot.name} (${uncommittedOpsCount} operations)`
      );
      
      trackEvent('snapshot_created', { 
        version: snapshot.version, 
        opsCount: uncommittedOpsCount,
        provider: updatedProject.provider.type
      });
    });

  } catch (error) {
    outputChannel.appendLine(`[Schematic] ERROR: Snapshot creation failed: ${error}`);
    if (error instanceof Error) {
      outputChannel.appendLine(`[Schematic] Stack trace: ${error.stack}`);
    }
    vscode.window.showErrorMessage(`Failed to create snapshot: ${error}`);
  }
}

/**
 * Generate SQL migration command implementation
 */
async function generateSQLMigration() {
  outputChannel.appendLine('[Schematic] Generate SQL migration command invoked');
  
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    outputChannel.appendLine('[Schematic] ERROR: No workspace folder open');
    vscode.window.showErrorMessage('Schematic: Please open a workspace folder first.');
    return;
  }

  try {
    outputChannel.appendLine(`[Schematic] Loading current state from: ${workspaceFolder.uri.fsPath}`);
    
    // Load current state, changelog, and provider
    const { state, changelog, provider } = await storageV3.loadCurrentState(workspaceFolder.uri);
    
    outputChannel.appendLine(`[Schematic] Provider: ${provider.info.name} v${provider.info.version}`);
    outputChannel.appendLine(`[Schematic] Changelog operations: ${changelog.ops.length}`);
    
    // For Unity provider, log catalog count (provider-specific)
    if (state && 'catalogs' in state) {
      outputChannel.appendLine(`[Schematic] Loaded state: ${(state as any).catalogs.length} catalogs`);
    }

    if (changelog.ops.length === 0) {
      outputChannel.appendLine('[Schematic] No operations in changelog, nothing to generate');
      vscode.window.showInformationMessage('No changes to generate SQL for. Changelog is empty.');
      return;
    }

    // Generate SQL using provider's SQL generator
    const generator = provider.getSQLGenerator(state);
    const sql = generator.generateSQL(changelog.ops);
    
    outputChannel.appendLine(`[Schematic] Generated SQL (${sql.length} characters)`);

    // Create migrations directory
    const migrationsDir = path.join(workspaceFolder.uri.fsPath, '.schematic', 'migrations');
    if (!fs.existsSync(migrationsDir)) {
      fs.mkdirSync(migrationsDir, { recursive: true });
      outputChannel.appendLine(`[Schematic] Created migrations directory: ${migrationsDir}`);
    }

    // Generate filename with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').substring(0, 19);
    const filename = `migration_${timestamp}.sql`;
    const filepath = path.join(migrationsDir, filename);
    
    // Write SQL to file
    fs.writeFileSync(filepath, sql, 'utf8');
    outputChannel.appendLine(`[Schematic] SQL written to: ${filepath}`);

    // Open the file in editor
    const doc = await vscode.workspace.openTextDocument(filepath);
    await vscode.window.showTextDocument(doc);
    
    vscode.window.showInformationMessage(
      `SQL migration generated: ${filename} (${changelog.ops.length} operations)`
    );
    
    trackEvent('sql_generated', { 
      opsCount: changelog.ops.length,
      sqlLength: sql.length,
      provider: provider.info.id
    });

  } catch (error) {
    outputChannel.appendLine(`[Schematic] ERROR: SQL generation failed: ${error}`);
    if (error instanceof Error) {
      outputChannel.appendLine(`[Schematic] Stack trace: ${error.stack}`);
    }
    vscode.window.showErrorMessage(`Failed to generate SQL: ${error}`);
  }
}

