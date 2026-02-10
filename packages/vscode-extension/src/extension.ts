import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { ChildProcess, spawn } from 'child_process';
import * as storageV4 from './storage-v4';
import { Operation } from './providers/base/operations';
import { ProviderRegistry } from './providers/registry';
import { trackEvent } from './telemetry';
import './providers'; // Initialize providers

let outputChannel: vscode.OutputChannel;
let currentPanel: vscode.WebviewPanel | undefined;

interface ImportRequest {
  target: string;
  profile: string;
  warehouseId: string;
  catalog?: string;
  schema?: string;
  table?: string;
  catalogMappings?: Record<string, string>;
  dryRun: boolean;
  adoptBaseline: boolean;
}

interface ImportExecutionResult {
  success: boolean;
  command: string;
  stdout: string;
  stderr: string;
  cancelled?: boolean;
}

interface ImportProgressUpdate {
  phase: string;
  message: string;
  percent: number;
  level?: 'info' | 'warning' | 'error' | 'success';
}

let activeImportProcess: ChildProcess | null = null;
let activeImportCancelled = false;

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

  const importAssetsCommand = vscode.commands.registerCommand(
    'schematic.importAssets',
    () => runImportFromPrompts()
  );

  context.subscriptions.push(openDesignerCommand, showLastOpsCommand, createSnapshotCommand, generateSQLCommand, importAssetsCommand, outputChannel);

  outputChannel.appendLine('[Schematic] Extension activated successfully!');
  outputChannel.appendLine('[Schematic] Commands registered: schematic.openDesigner, schematic.showLastOps, schematic.createSnapshot, schematic.generateSQL, schematic.importAssets');
  vscode.window.showInformationMessage('Schematic Extension Activated!');
  trackEvent('extension_activated');
}

export function deactivate() {
  trackEvent('extension_deactivated');
}

function runCommand(
  command: string,
  args: string[],
  cwd: string,
  onStdout?: (chunk: string) => void,
  onStderr?: (chunk: string) => void
): Promise<{ code: number | null; stdout: string; stderr: string; spawnError?: any; cancelled?: boolean }> {
  return new Promise((resolve) => {
    if (activeImportCancelled) {
      resolve({ code: null, stdout: '', stderr: '', cancelled: true });
      return;
    }

    const child = spawn(command, args, { cwd, shell: false });
    activeImportProcess = child;
    let stdout = '';
    let stderr = '';
    let spawnError: any;

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      stdout += text;
      onStdout?.(text);
    });
    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderr += text;
      onStderr?.(text);
    });
    child.on('error', (err) => {
      spawnError = err;
    });
    child.on('close', (code) => {
      const cancelled = activeImportCancelled;
      if (activeImportProcess === child) {
        activeImportProcess = null;
      }
      resolve({ code, stdout, stderr, spawnError, cancelled });
    });

    if (activeImportCancelled) {
      try {
        child.kill('SIGTERM');
      } catch {}
    }
  });
}

async function executeImport(
  workspacePath: string,
  request: ImportRequest,
  onProgress?: (update: ImportProgressUpdate) => void
): Promise<ImportExecutionResult> {
  const importArgs = [
    'import',
    '--target', request.target,
    '--profile', request.profile,
    '--warehouse-id', request.warehouseId,
  ];

  if (request.catalog?.trim()) importArgs.push('--catalog', request.catalog.trim());
  if (request.schema?.trim()) importArgs.push('--schema', request.schema.trim());
  if (request.table?.trim()) importArgs.push('--table', request.table.trim());
  if (request.catalogMappings) {
    for (const [logical, physical] of Object.entries(request.catalogMappings)) {
      importArgs.push('--catalog-map', `${logical}=${physical}`);
    }
  }
  if (request.dryRun) importArgs.push('--dry-run');
  if (request.adoptBaseline) importArgs.push('--adopt-baseline');

  const candidates: Array<{ cmd: string; args: string[] }> = [
    { cmd: 'schematic', args: importArgs },
    { cmd: 'python3', args: ['-m', 'schematic.cli', ...importArgs] },
    { cmd: 'python', args: ['-m', 'schematic.cli', ...importArgs] },
  ];

  let lastResult: ImportExecutionResult = {
    success: false,
    command: '',
    stdout: '',
    stderr: 'No import command candidates were executed',
  };

  activeImportCancelled = false;

  let currentPercent = 10;
  const emitProgress = (update: ImportProgressUpdate) => {
    currentPercent = Math.max(currentPercent, update.percent);
    onProgress?.({ ...update, percent: currentPercent });
  };

  emitProgress({
    phase: 'starting',
    message: 'Preparing import command',
    percent: 10,
    level: 'info',
  });

  // Smooth progress while waiting for CLI output.
  const runningTimer = setInterval(() => {
    if (currentPercent < 92) {
      currentPercent = Math.min(92, currentPercent + 3);
      onProgress?.({
        phase: 'running',
        message: 'Import in progress...',
        percent: currentPercent,
        level: 'info',
      });
    }
  }, 1200);

  try {
    for (const candidate of candidates) {
    if (activeImportCancelled) {
      onProgress?.({
        phase: 'cancelled',
        message: 'Import cancelled by user.',
        percent: 100,
        level: 'warning',
      });
      return {
        success: false,
        command: '',
        stdout: '',
        stderr: 'Import cancelled by user',
        cancelled: true,
      };
    }

    const renderedCommand = `${candidate.cmd} ${candidate.args.join(' ')}`;
    emitProgress({
      phase: 'launch',
      message: `Launching import command: ${candidate.cmd}`,
      percent: 20,
      level: 'info',
    });

    const result = await runCommand(
      candidate.cmd,
      candidate.args,
      workspacePath,
      (stdoutChunk) => {
        for (const line of stdoutChunk.split('\n')) {
          const update = parseImportProgressLine(line);
          if (update) {
            emitProgress(update);
          }
        }
      },
      (stderrChunk) => {
        const text = stderrChunk.trim();
        if (text) {
          emitProgress({
            phase: 'stderr',
            message: text,
            percent: 30,
            level: 'warning',
          });
        }
      }
    );

    if (result.cancelled) {
      onProgress?.({
        phase: 'cancelled',
        message: 'Import cancelled by user.',
        percent: 100,
        level: 'warning',
      });
      return {
        success: false,
        command: renderedCommand,
        stdout: result.stdout.trim(),
        stderr: result.stderr.trim() || 'Import cancelled by user',
        cancelled: true,
      };
    }

    if (result.spawnError) {
      lastResult = {
        success: false,
        command: renderedCommand,
        stdout: result.stdout,
        stderr: `${result.stderr}\n${result.spawnError.message || result.spawnError}`,
      };
      emitProgress({
        phase: 'spawn-error',
        message: `Import launcher failed for ${candidate.cmd}: ${result.spawnError.message || result.spawnError}`,
        percent: 25,
        level: 'warning',
      });
      continue;
    }

    if (result.code === 0) {
      onProgress?.({
        phase: 'completed',
        message: request.dryRun ? 'Dry-run completed' : 'Import completed',
        percent: 100,
        level: 'success',
      });
      return {
        success: true,
        command: renderedCommand,
        stdout: result.stdout.trim(),
        stderr: result.stderr.trim(),
      };
    }

    lastResult = {
      success: false,
      command: renderedCommand,
      stdout: result.stdout.trim(),
      stderr: result.stderr.trim(),
    };
    emitProgress({
      phase: 'command-failed',
      message: `Import command failed (${candidate.cmd})`,
      percent: 35,
      level: 'warning',
    });
    }

    onProgress?.({
    phase: 'failed',
    message: 'Import failed. Check stderr/output for details.',
    percent: 100,
    level: 'error',
  });
    return lastResult;
  } finally {
    clearInterval(runningTimer);
    activeImportProcess = null;
    activeImportCancelled = false;
  }
}

function parseImportProgressLine(line: string): ImportProgressUpdate | null {
  const trimmed = line.replace(/\x1b\[[0-9;]*m/g, '').trim();
  if (!trimmed) return null;

  if (trimmed.includes('Schematic Import')) {
    return { phase: 'start', message: 'Import started', percent: 25, level: 'info' };
  }
  if (trimmed.includes('Provider:')) {
    return { phase: 'provider', message: trimmed, percent: 30, level: 'info' };
  }
  if (trimmed.includes('Discovered:')) {
    return { phase: 'discovered', message: trimmed, percent: 60, level: 'info' };
  }
  if (trimmed.includes('Planned operations:')) {
    return { phase: 'planned', message: trimmed, percent: 72, level: 'info' };
  }
  if (trimmed.includes('Updated catalog mappings')) {
    return { phase: 'mappings', message: trimmed, percent: 82, level: 'info' };
  }
  if (trimmed.includes('Imported ') && trimmed.includes('operation')) {
    return { phase: 'write', message: trimmed, percent: 90, level: 'info' };
  }
  if (trimmed.includes('Created baseline snapshot')) {
    return { phase: 'snapshot', message: trimmed, percent: 94, level: 'info' };
  }
  if (trimmed.includes('Adopted baseline deployment')) {
    return { phase: 'baseline', message: trimmed, percent: 97, level: 'info' };
  }
  if (trimmed.startsWith('Warning:') || trimmed.includes('Unsupported Unity object type')) {
    return { phase: 'warning', message: trimmed, percent: 75, level: 'warning' };
  }
  if (trimmed.includes('Dry-run:')) {
    return { phase: 'dry-run-complete', message: trimmed, percent: 100, level: 'success' };
  }
  return null;
}

async function promptImportRequest(workspaceUri: vscode.Uri): Promise<ImportRequest | null> {
  const project = await storageV4.readProject(workspaceUri);
  const envNames = Object.keys(project.provider.environments || {});
  if (envNames.length === 0) {
    vscode.window.showErrorMessage('Schematic: No environments configured in project.json');
    return null;
  }

  const target = await vscode.window.showQuickPick(envNames, {
    placeHolder: 'Select target environment for import',
    ignoreFocusOut: true
  });
  if (!target) return null;

  const profile = await vscode.window.showInputBox({
    prompt: 'Databricks profile name',
    value: 'DEFAULT',
    placeHolder: 'DEFAULT',
    validateInput: (value) => value.trim() ? null : 'Profile is required',
    ignoreFocusOut: true
  });
  if (!profile) return null;

  const warehouseId = await vscode.window.showInputBox({
    prompt: 'SQL Warehouse ID',
    placeHolder: 'e.g. 1234abcd5678efgh',
    validateInput: (value) => value.trim() ? null : 'Warehouse ID is required',
    ignoreFocusOut: true
  });
  if (!warehouseId) return null;

  const catalog = await vscode.window.showInputBox({
    prompt: 'Catalog to import (optional, blank = all visible catalogs)',
    placeHolder: 'main',
    ignoreFocusOut: true
  });
  if (catalog === undefined) return null;

  const schema = await vscode.window.showInputBox({
    prompt: 'Schema to import (optional, requires catalog)',
    placeHolder: 'analytics',
    validateInput: (value) => {
      if (value.trim() && !catalog?.trim()) return 'Schema requires catalog';
      return null;
    },
    ignoreFocusOut: true
  });
  if (schema === undefined) return null;

  const table = await vscode.window.showInputBox({
    prompt: 'Table to import (optional, requires schema)',
    placeHolder: 'users',
    validateInput: (value) => {
      if (value.trim() && !schema?.trim()) return 'Table requires schema';
      return null;
    },
    ignoreFocusOut: true
  });
  if (table === undefined) return null;

  const mode = await vscode.window.showQuickPick(
    [
      { label: 'Dry run (recommended)', dryRun: true, adoptBaseline: false },
      { label: 'Import only', dryRun: false, adoptBaseline: false },
      { label: 'Import + adopt baseline', dryRun: false, adoptBaseline: true },
    ],
    {
      placeHolder: 'Select import mode',
      ignoreFocusOut: true
    }
  );
  if (!mode) return null;

  const bindingsInput = await vscode.window.showInputBox({
    prompt: 'Catalog mappings (optional): logical=physical, comma-separated',
    placeHolder: 'schematic_demo=dev_schematic_demo',
    ignoreFocusOut: true
  });
  if (bindingsInput === undefined) return null;

  const catalogMappings = parseCatalogMappingsInput(bindingsInput);

  return {
    target,
    profile: profile.trim(),
    warehouseId: warehouseId.trim(),
    catalog: catalog?.trim() || undefined,
    schema: schema?.trim() || undefined,
    table: table?.trim() || undefined,
    catalogMappings,
    dryRun: mode.dryRun,
    adoptBaseline: mode.adoptBaseline,
  };
}

function parseCatalogMappingsInput(input: string): Record<string, string> | undefined {
  const trimmed = input.trim();
  if (!trimmed) return undefined;

  const bindings: Record<string, string> = {};
  const entries = trimmed.split(',').map((entry) => entry.trim()).filter(Boolean);
  for (const entry of entries) {
    const eqIndex = entry.indexOf('=');
    if (eqIndex <= 0 || eqIndex === entry.length - 1) {
      throw new Error(`Invalid catalog mapping '${entry}'. Expected logical=physical`);
    }
    const logical = entry.slice(0, eqIndex).trim();
    const physical = entry.slice(eqIndex + 1).trim();
    if (!logical || !physical) {
      throw new Error(`Invalid catalog mapping '${entry}'. Expected logical=physical`);
    }
    bindings[logical] = physical;
  }
  return bindings;
}

async function runImportFromPrompts(): Promise<void> {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('Schematic: Please open a workspace folder first.');
    return;
  }

  try {
    const request = await promptImportRequest(workspaceFolder.uri);
    if (!request) return;
    await runImportWithRequest(workspaceFolder, request);
  } catch (error) {
    vscode.window.showErrorMessage(`Schematic import failed: ${error}`);
  }
}

async function runImportWithRequest(
  workspaceFolder: vscode.WorkspaceFolder,
  request: ImportRequest,
  panel?: vscode.WebviewPanel
): Promise<ImportExecutionResult> {
  outputChannel.appendLine('[Schematic] Running import workflow...');
  outputChannel.appendLine(`[Schematic] Import request: ${JSON.stringify(request)}`);

  const result = await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: request.dryRun ? 'Running import dry-run...' : 'Importing assets...',
      cancellable: true,
    },
    async (_progress, token) => {
      token.onCancellationRequested(() => {
        cancelActiveImport();
      });
      return executeImport(workspaceFolder.uri.fsPath, request, (update) => {
        if (panel) {
          panel.webview.postMessage({
            type: 'import-progress',
            payload: update,
          });
        }
      });
    }
  );

  if (result.stdout) {
    outputChannel.appendLine('[Schematic] Import stdout:');
    outputChannel.appendLine(result.stdout);
  }
  if (result.stderr) {
    outputChannel.appendLine('[Schematic] Import stderr:');
    outputChannel.appendLine(result.stderr);
  }

  if (result.cancelled) {
    vscode.window.showInformationMessage('Schematic import cancelled');
    trackEvent('import_cancelled', {
      dryRun: request.dryRun,
      adoptBaseline: request.adoptBaseline,
      target: request.target,
    });
  } else if (result.success) {
    vscode.window.showInformationMessage(
      request.dryRun
        ? 'Schematic import dry-run completed'
        : 'Schematic import completed successfully'
    );
    trackEvent('import_completed', {
      dryRun: request.dryRun,
      adoptBaseline: request.adoptBaseline,
      target: request.target,
    });
  } else {
    vscode.window.showErrorMessage(`Schematic import failed. See Output > Schematic for details.`);
    trackEvent('import_failed', {
      dryRun: request.dryRun,
      adoptBaseline: request.adoptBaseline,
      target: request.target,
    });
  }

  if (panel) {
    panel.webview.postMessage({
      type: 'import-progress',
      payload: {
        phase: result.cancelled ? 'cancelled' : (result.success ? 'completed' : 'failed'),
        message: result.cancelled
          ? 'Import cancelled by user.'
          : (result.success
          ? (request.dryRun ? 'Dry-run completed' : 'Import completed')
          : 'Import failed. See output details below.'),
        percent: 100,
        level: result.cancelled ? 'warning' : (result.success ? 'success' : 'error'),
      } satisfies ImportProgressUpdate,
    });
    panel.webview.postMessage({
      type: 'import-result',
      payload: result,
    });
  }

  return result;
}

function cancelActiveImport(): void {
  activeImportCancelled = true;
  if (!activeImportProcess) return;

  try {
    activeImportProcess.kill('SIGTERM');
  } catch {}
  setTimeout(() => {
    if (!activeImportProcess) return;
    try {
      activeImportProcess.kill('SIGKILL');
    } catch {}
  }, 3000);
}

/**
 * Environment presets with their configurations
 */
interface EnvironmentPreset {
  label: string;
  description: string;
  config: Partial<storageV4.EnvironmentConfig>;
}

const ENVIRONMENT_PRESETS: EnvironmentPreset[] = [
  {
    label: 'Development-like',
    description: '✅ Manual changes allowed, ✅ Direct changelog deployment, ✅ Auto-creates catalog',
    config: {
      allowDrift: true,
      requireSnapshot: false,
      autoCreateTopLevel: true,
      autoCreateSchematicSchema: true,
    }
  },
  {
    label: 'Staging-like',
    description: '⚠️ No manual changes [planned], ✅ Requires snapshot, ✅ Auto-creates catalog',
    config: {
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: true,
      autoCreateSchematicSchema: true,
    }
  },
  {
    label: 'Production-like',
    description: '⚠️ No manual changes [planned], ✅ Requires snapshot, Catalog must exist',
    config: {
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: false,
      autoCreateSchematicSchema: true,
    }
  }
];

/**
 * Configure custom environments (add to defaults)
 */
async function configureCustomEnvironments(
  projectName: string,
  sanitizedProjectName: string,
  outputChannel: vscode.OutputChannel
): Promise<Record<string, storageV4.EnvironmentConfig> | null> {
  const customEnvironments: Record<string, storageV4.EnvironmentConfig> = {};
  
  outputChannel.appendLine('[Schematic] Starting custom environment configuration...');
  
  // Loop to add custom environments
  while (true) {
    // Ask if user wants to add a custom environment
    const addEnvironment = await vscode.window.showQuickPick(
      [
        { label: 'Add custom environment', value: true },
        { label: 'Done - use configured environments', value: false }
      ],
      {
        placeHolder: `${Object.keys(customEnvironments).length} custom environment(s) configured. Add another?`,
        ignoreFocusOut: true
      }
    );
    
    if (!addEnvironment || !addEnvironment.value) {
      break; // Done adding environments
    }
    
    // Step 1: Environment name
    const envName = await vscode.window.showInputBox({
      prompt: 'Enter environment name (alphanumeric and underscore only)',
      placeHolder: 'staging',
      validateInput: (value) => {
        if (!value) {
          return 'Environment name is required';
        }
        if (!/^[a-zA-Z0-9_]+$/.test(value)) {
          return 'Only alphanumeric characters and underscores allowed';
        }
        if (customEnvironments[value] || ['dev', 'test', 'prod'].includes(value)) {
          return 'Environment name already exists';
        }
        return null;
      },
      ignoreFocusOut: true
    });
    
    if (!envName) {
      continue; // User cancelled or invalid, try again
    }
    
    outputChannel.appendLine(`[Schematic]   Adding environment: ${envName}`);
    
    // Step 2: Physical catalog name (with suggestion)
    const suggestedCatalog = `${envName}_${sanitizedProjectName}`;
    const catalogName = await vscode.window.showInputBox({
      prompt: `Enter physical catalog name for '${envName}' environment`,
      value: suggestedCatalog,
      placeHolder: suggestedCatalog,
      validateInput: (value) => {
        if (!value) {
          return 'Catalog name is required';
        }
        if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(value)) {
          return 'Catalog name must start with a letter and contain only alphanumeric characters and underscores';
        }
        return null;
      },
      ignoreFocusOut: true
    });
    
    if (!catalogName) {
      continue; // User cancelled, try again
    }
    
    outputChannel.appendLine(`[Schematic]     Catalog: ${catalogName}`);
    
    // Step 3: Select environment preset
    const presetOptions = ENVIRONMENT_PRESETS.map(preset => ({
      label: preset.label,
      description: preset.description,
      preset: preset
    }));
    
    const selectedPreset = await vscode.window.showQuickPick(presetOptions, {
      placeHolder: `Select environment type for '${envName}'`,
      ignoreFocusOut: true
    });
    
    if (!selectedPreset) {
      continue; // User cancelled, try again
    }
    
    outputChannel.appendLine(`[Schematic]     Type: ${selectedPreset.label}`);
    
    // Step 4: Optional description
    const description = await vscode.window.showInputBox({
      prompt: `Enter description for '${envName}' environment (optional)`,
      placeHolder: `${envName.charAt(0).toUpperCase() + envName.slice(1)} environment`,
      ignoreFocusOut: true
    });
    
    // Create environment config
    const presetConfig = selectedPreset.preset.config;
    customEnvironments[envName] = {
      topLevelName: catalogName,
      description: description || `${envName.charAt(0).toUpperCase() + envName.slice(1)} environment`,
      allowDrift: presetConfig.allowDrift ?? false,
      requireSnapshot: presetConfig.requireSnapshot ?? false,
      autoCreateTopLevel: presetConfig.autoCreateTopLevel ?? false,
      autoCreateSchematicSchema: presetConfig.autoCreateSchematicSchema ?? true,
      requireApproval: presetConfig.requireApproval,
    };
    
    outputChannel.appendLine(`[Schematic]   ✓ Environment '${envName}' configured`);
  }
  
  return customEnvironments;
}

/**
 * Prompt user for project setup (provider, environments, catalog names)
 */
async function promptForProjectSetup(workspaceUri: vscode.Uri, outputChannel: vscode.OutputChannel): Promise<boolean> {
  outputChannel.appendLine('[Schematic] Starting project setup wizard...');
  
  // Step 1: Select provider (for now, only Unity is available)
  const providers = [
    { label: 'Unity Catalog', description: 'Databricks Unity Catalog', id: 'unity' }
  ];
  
  const selectedProvider = await vscode.window.showQuickPick(providers, {
    placeHolder: 'Select your catalog provider',
    ignoreFocusOut: true
  });
  
  if (!selectedProvider) {
    return false; // User cancelled
  }
  
  outputChannel.appendLine(`[Schematic] Provider selected: ${selectedProvider.label}`);
  
  // Step 2: Project name (default to workspace folder name)
  const workspaceName = path.basename(workspaceUri.fsPath);
  const projectName = await vscode.window.showInputBox({
    prompt: 'Enter project name',
    value: workspaceName,
    placeHolder: 'my-analytics-project',
    ignoreFocusOut: true
  });
  
  if (!projectName) {
    return false; // User cancelled
  }
  
  outputChannel.appendLine(`[Schematic] Project name: ${projectName}`);
  
  // Step 3: Configure environments and catalogs
  const useDefaultEnvs = await vscode.window.showQuickPick(
    [
      { label: 'Use default environments', description: 'dev, test, prod with standard catalog naming', value: true },
      { label: 'Configure custom environments', description: 'Manually configure environment settings', value: false }
    ],
    {
      placeHolder: 'Environment configuration',
      ignoreFocusOut: true
    }
  );
  
  if (!useDefaultEnvs) {
    return false; // User cancelled
  }
  
  let environments: Record<string, storageV4.EnvironmentConfig>;
  
  // Create default environments first
  const sanitizedName = projectName.replace(/[^a-zA-Z0-9_]/g, '_');
  environments = {
    dev: {
      topLevelName: `dev_${sanitizedName}`,
      description: 'Development environment',
      allowDrift: true,
      requireSnapshot: false,
      autoCreateTopLevel: true,
      autoCreateSchematicSchema: true,
    },
    test: {
      topLevelName: `test_${sanitizedName}`,
      description: 'Test/staging environment',
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: true,
      autoCreateSchematicSchema: true,
    },
    prod: {
      topLevelName: `prod_${sanitizedName}`,
      description: 'Production environment',
      allowDrift: false,
      requireSnapshot: true,
      requireApproval: false,
      autoCreateTopLevel: false,
      autoCreateSchematicSchema: true,
    },
  };
  
  outputChannel.appendLine('[Schematic] Default environments created:');
  outputChannel.appendLine(`  - dev → ${environments.dev.topLevelName}`);
  outputChannel.appendLine(`  - test → ${environments.test.topLevelName}`);
  outputChannel.appendLine(`  - prod → ${environments.prod.topLevelName}`);
  
  // If custom configuration requested, allow adding more environments
  if (!useDefaultEnvs.value) {
    const customEnvs = await configureCustomEnvironments(projectName, sanitizedName, outputChannel);
    if (customEnvs === null) {
      return false; // User cancelled
    }
    
    // Merge custom environments with defaults
    Object.assign(environments, customEnvs);
  }
  
  // Step 4: Prompt for logical catalog name
  const logicalCatalogName = await vscode.window.showInputBox({
    prompt: 'Enter logical catalog name (used in design, maps to physical names per environment)',
    value: sanitizedName,
    placeHolder: 'analytics_platform',
    validateInput: (value) => {
      if (!value) {
        return 'Catalog name is required';
      }
      if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(value)) {
        return 'Catalog name must start with a letter and contain only alphanumeric characters and underscores';
      }
      return null;
    },
    ignoreFocusOut: true
  });
  
  if (!logicalCatalogName) {
    return false; // User cancelled
  }

  Object.values(environments).forEach((config) => {
    config.catalogMappings = {
      ...(config.catalogMappings || {}),
      [logicalCatalogName]: config.topLevelName,
    };
  });
  
  outputChannel.appendLine(`[Schematic] Logical catalog name: ${logicalCatalogName}`);
  outputChannel.appendLine('[Schematic] Physical catalog mappings:');
  Object.entries(environments).forEach(([env, config]) => {
    outputChannel.appendLine(`  - ${logicalCatalogName} → ${config.topLevelName} (${env})`);
  });
  
  // Step 5: Create the project
  try {
    await storageV4.ensureSchematicDir(workspaceUri);
    
    const provider = ProviderRegistry.get(selectedProvider.id);
    if (!provider) {
      throw new Error(`Provider '${selectedProvider.id}' not found`);
    }
    
    // Create v4 project
    const newProject: storageV4.ProjectFileV4 = {
      version: 4,
      name: projectName,
      provider: {
        type: selectedProvider.id,
        version: provider.info.version,
        environments: environments,
      },
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };
    
    // Initialize changelog with the logical catalog
    const initialOps: Operation[] = [];
    const catalogId = `cat_${logicalCatalogName}`;
    initialOps.push({
      id: `op_init_catalog`,
      ts: new Date().toISOString(),
      provider: selectedProvider.id,
      op: `${selectedProvider.id}.add_catalog`,
      target: catalogId,
      payload: {
        catalogId,
        name: logicalCatalogName
      }
    });
    
    outputChannel.appendLine(`[Schematic] Created logical catalog: ${logicalCatalogName}`);
    
    const newChangelog: storageV4.ChangelogFile = {
      version: 1,
      sinceSnapshot: null,
      ops: initialOps,
      lastModified: new Date().toISOString(),
    };
    
    await storageV4.writeProject(workspaceUri, newProject);
    await storageV4.writeChangelog(workspaceUri, newChangelog);
    
    outputChannel.appendLine('[Schematic] ✓ Project created successfully');
    
    // Show success message with next steps
    const envCount = Object.keys(environments).length;
    const customCount = envCount - 3; // Subtract default dev/test/prod
    
    const envList = Object.entries(environments)
      .map(([name, config]) => {
        const isDefault = ['dev', 'test', 'prod'].includes(name);
        const badge = isDefault ? '' : ' [custom]';
        return `  • ${name}${badge} → ${config.topLevelName}`;
      })
      .join('\n');
    
    const message = `Schematic project initialized!\n\n` +
      `Project: ${projectName}\n` +
      `Provider: ${selectedProvider.label}\n` +
      `Environments: ${envCount} (${3} default${customCount > 0 ? ` + ${customCount} custom` : ''})\n\n` +
      envList + '\n\n' +
      `Next steps:\n` +
      `  1. Design your schema in the visual designer\n` +
      `  2. Generate SQL for an environment (Cmd+Shift+P → "Generate SQL")\n` +
      `  3. Apply changes with the CLI: schematic apply --target dev`;
    
    vscode.window.showInformationMessage(message);
    
    return true;
    
  } catch (error) {
    outputChannel.appendLine(`[Schematic] ERROR: Failed to create project: ${error}`);
    vscode.window.showErrorMessage(`Failed to initialize project: ${error}`);
    return false;
  }
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

  // Check if project already exists
  const projectPath = vscode.Uri.joinPath(workspaceFolder.uri, '.schematic', 'project.json');
  let projectExists = false;
  
  try {
    await vscode.workspace.fs.stat(projectPath);
    projectExists = true;
  } catch {
    // Project doesn't exist, will prompt for setup
  }

  // If project doesn't exist, prompt for initial setup
  if (!projectExists) {
    const setupComplete = await promptForProjectSetup(workspaceFolder.uri, outputChannel);
    if (!setupComplete) {
      return; // User cancelled
    }
  } else {
    // Ensure existing project is v4 (will error if not)
    await storageV4.ensureProjectFile(workspaceFolder.uri, outputChannel, 'unity');
  }

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

  // Helper function to detect stale snapshots using Python SDK
  async function detectStaleSnapshots(workspacePath: string): Promise<any[]> {
    try {
      const { exec } = require('child_process');
      const { promisify } = require('util');
      const execAsync = promisify(exec);

      // Call schematic CLI with --json flag
      const { stdout } = await execAsync('schematic snapshot validate --json', {
        cwd: workspacePath,
      });

      // Parse only the JSON line (last non-empty line)
      const lines = stdout.trim().split('\n').filter((line: string) => line.trim());
      const jsonLine = lines[lines.length - 1];
      const result = JSON.parse(jsonLine);
      return result.stale || [];
    } catch (error: any) {
      // If exit code is 1, parse stdout (stale snapshots found)
      if (error.stdout) {
        try {
          // Parse only the JSON line (last non-empty line)
          const lines = error.stdout.trim().split('\n').filter((line: string) => line.trim());
          const jsonLine = lines[lines.length - 1];
          const result = JSON.parse(jsonLine);
          return result.stale || [];
        } catch (parseError) {
          outputChannel.appendLine(`[Schematic] Failed to parse stale snapshot output: ${parseError}`);
        }
      }
      
      outputChannel.appendLine(`[Schematic] Failed to detect stale snapshots: ${error.message}`);
      return [];
    }
  }

  // Helper function to reload project data and send to webview
  async function reloadProject(
    workspaceFolder: vscode.WorkspaceFolder,
    panel: vscode.WebviewPanel | undefined
  ) {
    if (!panel) return;

    try {
      const project = await storageV4.readProject(workspaceFolder.uri);
      const { state, changelog, provider, validationResult } = await storageV4.loadCurrentState(workspaceFolder.uri, true);

      // Check for conflicts
      const conflictsDir = vscode.Uri.joinPath(workspaceFolder.uri, '.schematic', 'conflicts');
      let conflicts = null;
      try {
        const conflictFiles = await vscode.workspace.fs.readDirectory(conflictsDir);
        if (conflictFiles.length > 0) {
          const latestConflictFile = conflictFiles.sort((a, b) => b[0].localeCompare(a[0]))[0];
          const conflictFilePath = vscode.Uri.joinPath(conflictsDir, latestConflictFile[0]);
          const conflictContent = await vscode.workspace.fs.readFile(conflictFilePath);
          conflicts = JSON.parse(Buffer.from(conflictContent).toString('utf8'));
          outputChannel.appendLine(`[Schematic] - Rebase conflict detected: ${latestConflictFile[0]}`);
        }
      } catch (error) {
        // No conflicts directory or no conflicts - that's fine
      }

      // Check for stale snapshots (using Python SDK)
      const staleSnapshots = await detectStaleSnapshots(workspaceFolder.uri.fsPath);
      if (staleSnapshots.length > 0) {
        outputChannel.appendLine(`[Schematic] - Detected ${staleSnapshots.length} stale snapshot(s)`);
      }

      // Log validation results
      if (validationResult) {
        if (validationResult.errors.length > 0) {
          outputChannel.appendLine(`[Schematic] - Dependency validation ERRORS:`);
          validationResult.errors.forEach((error) => {
            outputChannel.appendLine(`  ✗ ${error}`);
          });
        }
        if (validationResult.warnings.length > 0) {
          outputChannel.appendLine(`[Schematic] - Dependency validation warnings:`);
          validationResult.warnings.forEach((warning) => {
            outputChannel.appendLine(`  ⚠️  ${warning}`);
          });
        }
      }

      const payloadForWebview = {
        ...project,
        state,
        ops: changelog.ops,
        conflicts,
        staleSnapshots: staleSnapshots.length > 0 ? staleSnapshots : null,
        validationResult,
        provider: {
          ...project.provider,
          id: provider.info.id,
          name: provider.info.name,
          version: provider.info.version,
          capabilities: provider.capabilities,
        },
      };

      panel.webview.postMessage({
        type: 'project-loaded',
        payload: payloadForWebview,
      });
    } catch (error) {
      outputChannel.appendLine(`[Schematic] ERROR: Failed to reload project: ${error}`);
    }
  }

  // Set webview content
  outputChannel.appendLine('[Schematic] Setting webview HTML');
  currentPanel.webview.html = getWebviewContent(context, currentPanel.webview);
  outputChannel.appendLine('[Schematic] Webview HTML set');

  // Watch for conflict files and reload when they appear
  const conflictsPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schematic/conflicts/*.json'
  );
  const conflictWatcher = vscode.workspace.createFileSystemWatcher(conflictsPattern);

  // Reload project when conflict files are created or deleted
  conflictWatcher.onDidCreate(async () => {
    outputChannel.appendLine('[Schematic] Conflict file detected - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  conflictWatcher.onDidDelete(async () => {
    outputChannel.appendLine('[Schematic] Conflict file removed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Watch for snapshot file changes (to detect stale snapshots)
  const snapshotsPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schematic/snapshots/*.json'
  );
  const snapshotsWatcher = vscode.workspace.createFileSystemWatcher(snapshotsPattern);

  // Reload project when snapshots are created, changed, or deleted
  snapshotsWatcher.onDidCreate(async () => {
    outputChannel.appendLine('[Schematic] Snapshot file created - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  snapshotsWatcher.onDidChange(async () => {
    outputChannel.appendLine('[Schematic] Snapshot file changed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  snapshotsWatcher.onDidDelete(async () => {
    outputChannel.appendLine('[Schematic] Snapshot file deleted - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Watch for project.json changes (snapshot metadata)
  const projectJsonPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schematic/project.json'
  );
  const projectJsonWatcher = vscode.workspace.createFileSystemWatcher(projectJsonPattern);

  projectJsonWatcher.onDidChange(async () => {
    outputChannel.appendLine('[Schematic] project.json changed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Reset when panel is closed
  currentPanel.onDidDispose(() => {
    outputChannel.appendLine('[Schematic] Webview panel disposed');
    conflictWatcher.dispose();
    snapshotsWatcher.dispose();
    projectJsonWatcher.dispose();
    currentPanel = undefined;
  });

  // Handle messages from webview
  currentPanel.webview.onDidReceiveMessage(
    async (message) => {
      outputChannel.appendLine(`[Schematic] Received message from webview: ${message.type}`);
      switch (message.type) {
        case 'refresh-project': {
          outputChannel.appendLine('[Schematic] Manual refresh requested');
          await reloadProject(workspaceFolder, currentPanel);
          break;
        }

        case 'load-project': {
          try {
            outputChannel.appendLine(`[Schematic] Loading project from: ${workspaceFolder.uri.fsPath}`);
            
            // Load v3: project metadata + current state (snapshot + changelog) + provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[Schematic] Project loaded successfully (v${project.version})`);
            outputChannel.appendLine(`[Schematic] - Provider: ${provider.info.name} v${provider.info.version}`);
            outputChannel.appendLine(`[Schematic] - Snapshots: ${project.snapshots.length}`);
            outputChannel.appendLine(`[Schematic] - Latest snapshot: ${project.latestSnapshot || 'none'}`);
            outputChannel.appendLine(`[Schematic] - Changelog ops: ${changelog.ops.length}`);
            
            // For Unity provider, log catalog count (provider-specific)
            if (state && 'catalogs' in state) {
              outputChannel.appendLine(`[Schematic] - Catalogs: ${(state as any).catalogs.length}`);
            }
            
            // Check for rebase conflicts
            const conflictsDir = vscode.Uri.joinPath(workspaceFolder.uri, '.schematic', 'conflicts');
            let conflicts = null;
            try {
              const conflictFiles = await vscode.workspace.fs.readDirectory(conflictsDir);
              if (conflictFiles.length > 0) {
                // Read the latest conflict file
                const latestConflictFile = conflictFiles.sort((a, b) => b[0].localeCompare(a[0]))[0];
                const conflictFilePath = vscode.Uri.joinPath(conflictsDir, latestConflictFile[0]);
                const conflictContent = await vscode.workspace.fs.readFile(conflictFilePath);
                conflicts = JSON.parse(Buffer.from(conflictContent).toString('utf8'));
                outputChannel.appendLine(`[Schematic] - Rebase conflict detected: ${latestConflictFile[0]}`);
              }
            } catch (error) {
              // No conflicts directory or no conflicts - that's fine
            }
            
            // Send combined data to webview including provider info
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              conflicts, // Include conflict info if present
              provider: {
                ...project.provider, // Keep environments and other project provider config
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
        case 'show-error': {
          const { message: errorMessage, detail } = message.payload;
          vscode.window.showErrorMessage(errorMessage, { modal: false, detail });
          break;
        }
        case 'show-conflict-details': {
          const conflictInfo = message.payload;
          if (conflictInfo && conflictInfo.conflicting_operations && conflictInfo.conflicting_operations.length > 0) {
            const firstConflict = conflictInfo.conflicting_operations[0];
            const operation = firstConflict.operation;
            const reason = firstConflict.reason;
            
            const detailMessage = `Snapshot: ${conflictInfo.snapshot_version}\n` +
              `Old base: ${conflictInfo.old_base}\n` +
              `New base: ${conflictInfo.new_base}\n\n` +
              `Conflicting Operation: ${operation.op}\n` +
              `Target: ${operation.target}\n\n` +
              `Reason: ${reason}\n\n` +
              `Resolution:\n` +
              `1. Review the conflicting change in the designer\n` +
              `2. Manually apply your changes in the UI\n` +
              `3. Run: schematic snapshot create --version ${conflictInfo.snapshot_version}`;
            
            vscode.window.showWarningMessage(
              '⚠️ Snapshot Rebase Conflict',
              { modal: true, detail: detailMessage }
            ).then((choice) => {
              // User acknowledged the conflict
              outputChannel.appendLine('[Schematic] User acknowledged rebase conflict');
            });
          }
          break;
        }

        case 'show-stale-snapshot-details': {
          const staleSnapshots = message.payload;
          if (staleSnapshots && staleSnapshots.length > 0) {
            const detailLines = staleSnapshots.map((snap: any) => {
              return `Snapshot: ${snap.version}\n` +
                `  Current base: ${snap.currentBase}\n` +
                `  Should be: ${snap.shouldBeBase}\n` +
                `  Missing: ${snap.missing.join(', ')}`;
            }).join('\n\n');
            
            const detailMessage = `Found ${staleSnapshots.length} stale snapshot(s):\n\n` +
              detailLines + '\n\n' +
              `Resolution:\n` +
              `Run the following command(s) in terminal:\n` +
              staleSnapshots.map((snap: any) => `  schematic snapshot rebase ${snap.version}`).join('\n');
            
            vscode.window.showWarningMessage(
              '⚠️ Stale Snapshots Detected',
              { modal: true, detail: detailMessage }
            ).then((choice) => {
              // User acknowledged the stale snapshots
              outputChannel.appendLine('[Schematic] User acknowledged stale snapshots');
            });
          }
          break;
        }
        case 'open-docs': {
          try {
            const docPath = message.payload?.path as string | undefined;
            const fragment = message.payload?.fragment as string | undefined;

            if (!docPath) {
              vscode.window.showWarningMessage('Schematic: Documentation path was not provided.');
              break;
            }

            const targetUri = vscode.Uri.joinPath(workspaceFolder.uri, docPath);
            const document = await vscode.workspace.openTextDocument(targetUri);
            const editor = await vscode.window.showTextDocument(document, { preview: true });

            if (fragment) {
              const lowerFragment = fragment.toLowerCase();
              const text = document.getText().toLowerCase();
              const index = text.indexOf(lowerFragment);

              if (index >= 0) {
                const position = document.positionAt(index);
                editor.revealRange(new vscode.Range(position, position), vscode.TextEditorRevealType.AtTop);
              }
            }

            trackEvent('docs_opened', { path: docPath });
          } catch (error) {
            outputChannel.appendLine(`[Schematic] ERROR: Failed to open docs: ${error}`);
            vscode.window.showErrorMessage('Schematic: Unable to open documentation.');
          }
          break;
        }
        case 'append-ops': {
          try {
            const ops: Operation[] = message.payload;
            outputChannel.appendLine(`[Schematic] Appending ${ops.length} operation(s) to changelog`);
            
            // Append to changelog (v3 validates operations via provider)
            await storageV4.appendOps(workspaceFolder.uri, ops);
            
            // Reload state and provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[Schematic] Operations appended successfully`);
            outputChannel.appendLine(`[Schematic] - Changelog ops: ${changelog.ops.length}`);
            outputChannel.appendLine(`[Schematic] - Snapshots: ${project.snapshots.length}`);
            
            // Send updated data to webview including provider info
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                ...project.provider, // Keep environments and other project provider config
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
        case 'update-project-config': {
          try {
            const updatedProject = message.payload;
            outputChannel.appendLine(`[Schematic] Updating project configuration`);
            
            // Write updated project to disk
            await storageV4.writeProject(workspaceFolder.uri, updatedProject);
            
            // Reload state and provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[Schematic] Project configuration updated successfully`);
            
            // Send updated data to webview
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                ...project.provider,
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
            
            vscode.window.showInformationMessage('Schematic: Project settings saved successfully');
            trackEvent('project_config_updated', { provider: provider.info.id });
          } catch (error) {
            outputChannel.appendLine(`[Schematic] ERROR: Failed to update project config: ${error}`);
            vscode.window.showErrorMessage(`Failed to save project settings: ${error}`);
          }
          break;
        }
        case 'run-import': {
          try {
            const request = message.payload as ImportRequest;
            const result = await runImportWithRequest(workspaceFolder, request, currentPanel);
            if (result.success && !request.dryRun) {
              await reloadProject(workspaceFolder, currentPanel);
            } else if (result.cancelled && !request.dryRun) {
              // Re-sync UI with disk in case cancellation happened during file writes.
              await reloadProject(workspaceFolder, currentPanel);
            }
          } catch (error) {
            outputChannel.appendLine(`[Schematic] ERROR: Import run failed: ${error}`);
            currentPanel?.webview.postMessage({
              type: 'import-result',
              payload: {
                success: false,
                command: '',
                stdout: '',
                stderr: String(error),
              } as ImportExecutionResult,
            });
          }
          break;
        }
        case 'cancel-import': {
          cancelActiveImport();
          outputChannel.appendLine('[Schematic] Import cancellation requested from webview');
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
    const project = await storageV4.readProject(workspaceFolder.uri);
    const changelog = await storageV4.readChangelog(workspaceFolder.uri);
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
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline' https:; font-src ${webview.cspSource} https:; script-src 'nonce-${nonce}';">
  <link href="https://cdn.jsdelivr.net/npm/@vscode/codicons@0.0.36/dist/codicon.css" rel="stylesheet">
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
    const project = await storageV4.readProject(workspaceFolder.uri);
    const changelog = await storageV4.readChangelog(workspaceFolder.uri);
    const uncommittedOpsCount = changelog.ops.length;
    
    outputChannel.appendLine(`[Schematic] Provider: ${project.provider.type}`);
    outputChannel.appendLine(`[Schematic] Uncommitted operations: ${uncommittedOpsCount}`);
    outputChannel.appendLine(`[Schematic] Existing snapshots: ${project.snapshots.length}`);

    if (uncommittedOpsCount === 0) {
      outputChannel.appendLine('[Schematic] No uncommitted operations, aborting snapshot creation');
      vscode.window.showInformationMessage('No changes to snapshot. All operations are already included in the last snapshot.');
      return;
    }

    // Calculate next version (same logic as Python SDK)
    const calculateNextVersion = (currentVersion: string | null, versionPrefix: string = 'v'): string => {
      if (!currentVersion) {
        return versionPrefix + '0.1.0';
      }
      
      const match = currentVersion.match(/(\d+)\.(\d+)\.(\d+)/);
      if (!match) {
        return versionPrefix + '0.1.0';
      }
      
      const [, major, minor] = match;
      const nextMinor = parseInt(minor) + 1;
      return `${versionPrefix}${major}.${nextMinor}.0`;
    };

    const versionPrefix = project.settings?.versionPrefix || 'v';
    const suggestedVersion = calculateNextVersion(project.latestSnapshot, versionPrefix);

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

    // Get optional version (with suggested default)
    const versionInput = await vscode.window.showInputBox({
      prompt: 'Snapshot version (optional)',
      placeHolder: `Press Enter for auto-generated: ${suggestedVersion}`,
      value: '',
      validateInput: (value) => {
        if (!value.trim()) {
          return null; // Empty is OK (will use auto-generated)
        }
        // Validate semantic version format
        if (!/^v?\d+\.\d+\.\d+$/.test(value)) {
          return 'Version must be in format: v0.1.0 or 0.1.0';
        }
        return null;
      }
    });

    if (versionInput === undefined) {
      outputChannel.appendLine('[Schematic] Snapshot creation cancelled by user');
      return; // User cancelled
    }

    const version = versionInput.trim() || undefined; // Empty string becomes undefined

    // Get optional comment
    const comment = await vscode.window.showInputBox({
      prompt: 'Description (optional)',
      placeHolder: 'Describe what changed in this snapshot'
    });

    outputChannel.appendLine(`[Schematic] Creating snapshot: "${name}"`);
    if (version) {
      outputChannel.appendLine(`[Schematic] Version: "${version}"`);
    } else {
      outputChannel.appendLine(`[Schematic] Version: auto-generated (${suggestedVersion})`);
    }
    if (comment) {
      outputChannel.appendLine(`[Schematic] Comment: "${comment}"`);
    }

    // Create snapshot
    await vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: 'Creating snapshot...',
      cancellable: false
    }, async (progress) => {
      const { project: updatedProject, snapshot } = await storageV4.createSnapshot(
        workspaceFolder.uri, 
        name, 
        version, 
        comment
      );
      
      outputChannel.appendLine(`[Schematic] Snapshot created successfully!`);
      outputChannel.appendLine(`[Schematic] - ID: ${snapshot.id}`);
      outputChannel.appendLine(`[Schematic] - Version: ${snapshot.version}`);
      outputChannel.appendLine(`[Schematic] - Name: ${snapshot.name}`);
      outputChannel.appendLine(`[Schematic] - Operations included: ${snapshot.operations.length}`);
      outputChannel.appendLine(`[Schematic] - Total snapshots: ${updatedProject.snapshots.length}`);
      outputChannel.appendLine(`[Schematic] - Snapshot file: ${updatedProject.snapshots[updatedProject.snapshots.length - 1].file}`);
      
      progress.report({ increment: 100 });
      
      // Notify webview if it's open
      if (currentPanel) {
        outputChannel.appendLine('[Schematic] Notifying webview of snapshot creation');
        
        // Reload state and provider for webview
        const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
        const payloadForWebview = {
          ...updatedProject,
          state,
          ops: changelog.ops,
          provider: {
            ...updatedProject.provider, // Keep environments and other project provider config
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
/**
 * Build catalog name mapping (logical → physical) for environment-specific SQL generation.
 *
 * Uses provider.environments.<env>.catalogMappings and requires mappings for all logical
 * catalogs currently present in state.
 */
function buildCatalogMapping(state: any, envConfig: storageV4.EnvironmentConfig): Record<string, string> {
  const catalogs = state.catalogs || [];

  if (catalogs.length === 0) {
    return {};
  }

  const rawMappings = envConfig.catalogMappings || {};
  const catalogNames = catalogs
    .map((catalog: any) => String(catalog?.name || '').trim())
    .filter(Boolean);

  const missing = catalogNames.filter((name: string) => !(name in rawMappings));
  if (missing.length > 0) {
    throw new Error(
      `Missing catalog mapping(s) for logical catalog(s): ${missing.join(', ')}. ` +
      `Update provider.environments.<env>.catalogMappings in project settings.`
    );
  }

  const resolved: Record<string, string> = {};
  for (const logicalName of catalogNames) {
    resolved[logicalName] = String(rawMappings[logicalName]);
  }
  return resolved;
}

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
    
    // Load project and state
    const project = await storageV4.readProject(workspaceFolder.uri);
    const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
    
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

    // Ask for target environment
    const environments = Object.keys(project.provider.environments);
    if (environments.length === 0) {
      vscode.window.showErrorMessage('No environments configured in project.json');
      return;
    }

    const targetEnv = await vscode.window.showQuickPick(environments, {
      placeHolder: 'Select target environment for SQL generation',
      ignoreFocusOut: true
    });

    if (!targetEnv) {
      outputChannel.appendLine('[Schematic] SQL generation cancelled - no environment selected');
      return;
    }

    const envConfig = storageV4.getEnvironmentConfig(project, targetEnv);
    outputChannel.appendLine(`[Schematic] Target environment: ${targetEnv}`);
    outputChannel.appendLine(`[Schematic] Tracking catalog: ${envConfig.topLevelName}`);

    // Build catalog name mapping (logical → physical)
    const catalogMapping = buildCatalogMapping(state, envConfig);
    outputChannel.appendLine(`[Schematic] Catalog mapping: ${JSON.stringify(catalogMapping)}`);

    // Generate SQL using provider's SQL generator
    // Note: Catalog mapping is applied during SQL generation via the generator's internal mapping
    const generator = provider.getSQLGenerator(state, catalogMapping, {
      managedLocations: project.managedLocations,
      externalLocations: project.externalLocations,
      environmentName: targetEnv,
    });
    const sql = generator.generateSQL(changelog.ops);
    
    outputChannel.appendLine(`[Schematic] Generated SQL (${sql.length} characters)`);

    // Create migrations directory
    const migrationsDir = path.join(workspaceFolder.uri.fsPath, '.schematic', 'migrations');
    if (!fs.existsSync(migrationsDir)) {
      fs.mkdirSync(migrationsDir, { recursive: true });
      outputChannel.appendLine(`[Schematic] Created migrations directory: ${migrationsDir}`);
    }

    // Generate filename with timestamp and environment
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').substring(0, 19);
    const filename = `migration_${targetEnv}_${timestamp}.sql`;
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
