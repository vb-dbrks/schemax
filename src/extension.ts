import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { ensureProjectFile, readProject, appendOps } from './storage';
import { Op } from './shared/ops';
import { trackEvent } from './telemetry';

let outputChannel: vscode.OutputChannel;

export function activate(context: vscode.ExtensionContext) {
  outputChannel = vscode.window.createOutputChannel('SchemaX');

  // Register commands
  const openDesignerCommand = vscode.commands.registerCommand(
    'schemax.openDesigner',
    () => openDesigner(context)
  );

  const showLastOpsCommand = vscode.commands.registerCommand(
    'schemax.showLastOps',
    () => showLastOps()
  );

  context.subscriptions.push(openDesignerCommand, showLastOpsCommand, outputChannel);

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

  // Create webview panel
  const panel = vscode.window.createWebviewPanel(
    'schemaxDesigner',
    'SchemaX Designer',
    vscode.ViewColumn.One,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'media')],
    }
  );

  // Set webview content
  panel.webview.html = getWebviewContent(context, panel.webview);

  // Handle messages from webview
  panel.webview.onDidReceiveMessage(
    async (message) => {
      switch (message.type) {
        case 'load-project': {
          try {
            const project = await readProject(workspaceFolder.uri);
            panel.webview.postMessage({
              type: 'project-loaded',
              payload: project,
            });
            trackEvent('project_loaded');
          } catch (error) {
            vscode.window.showErrorMessage(`Failed to load project: ${error}`);
          }
          break;
        }
        case 'append-ops': {
          try {
            const ops: Op[] = message.payload;
            const updatedProject = await appendOps(workspaceFolder.uri, ops);
            panel.webview.postMessage({
              type: 'project-updated',
              payload: updatedProject,
            });
            trackEvent('ops_appended', { count: ops.length });
          } catch (error) {
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
  const htmlPath = path.join(context.extensionPath, 'media', 'index.html');
  let html = fs.readFileSync(htmlPath, 'utf8');

  // Replace script/style sources with webview URIs
  const mediaUri = webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'media'));
  html = html.replace(/src="\/assets\//g, `src="${mediaUri}/assets/`);
  html = html.replace(/href="\/assets\//g, `href="${mediaUri}/assets/`);

  return html;
}

