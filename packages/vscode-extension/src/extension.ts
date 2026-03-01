import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { exec as execCallback } from 'child_process';
import { promisify } from 'util';
import * as storageV4 from './storage-v4';
import type { Operation } from './contracts/workspace';
import { trackEvent } from './telemetry';
import { PythonBackendClient } from './backend/pythonBackendClient';
import type { CommandEnvelope, RuntimeInfoData } from './backend/contracts';
import { validateRuntimeInfo } from './backend/runtimeCompatibility';

let outputChannel: vscode.OutputChannel;
let currentPanel: vscode.WebviewPanel | undefined;
const pythonBackend = new PythonBackendClient();
let extensionContextRef: vscode.ExtensionContext | undefined;
const REQUIRED_ENVELOPE_SCHEMA_VERSION = '1';
const MIN_SUPPORTED_CLI_VERSION = '0.2.0';

interface BackendCompatibilityState {
  checked: boolean;
  ok: boolean;
  reason: string | null;
  runtimeInfo: RuntimeInfoData | null;
}

const backendCompatibilityState: BackendCompatibilityState = {
  checked: false,
  ok: false,
  reason: null,
  runtimeInfo: null,
};

interface ImportRequestFromSql {
  fromSql: {
    sqlPath: string;
    mode: 'diff' | 'replace';
    dryRun: boolean;
    target?: string;
  };
}

interface ImportRequestLive {
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

type ImportRequest = ImportRequestLive | ImportRequestFromSql;
type CatalogState = { catalogs?: Array<{ name?: string }> };
type StaleSnapshot = { version: string; currentBase: string; shouldBeBase: string; missing: string[] };

function isImportFromSql(req: ImportRequest): req is ImportRequestFromSql {
  return 'fromSql' in req && req.fromSql != null;
}

interface ImportExecutionResult {
  success: boolean;
  command: string;
  stdout: string;
  stderr: string;
  envelope?: CommandEnvelope<unknown>;
  cancelled?: boolean;
}

interface ImportProgressUpdate {
  phase: string;
  message: string;
  percent: number;
  level?: 'info' | 'warning' | 'error' | 'success';
}

let activeImportCancelled = false;
let activeImportController: AbortController | null = null;

export function activate(context: vscode.ExtensionContext) {
  extensionContextRef = context;
  outputChannel = vscode.window.createOutputChannel('SchemaX');
  outputChannel.appendLine('[SchemaX] Extension activating...');
  outputChannel.appendLine('[SchemaX] Extension Activated!');
  outputChannel.appendLine(`[SchemaX] Extension path: ${context.extensionPath}`);

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

  const generateSQLCommand = vscode.commands.registerCommand(
    'schemax.generateSQL',
    () => generateSQLMigration()
  );

  const importAssetsCommand = vscode.commands.registerCommand(
    'schemax.importAssets',
    () => runImportFromPrompts()
  );

  const installPythonSdkCommand = vscode.commands.registerCommand(
    'schemax.installPythonSdk',
    () => installPythonSdk()
  );

  // Empty tree provider for SchemaX sidebar view so viewsWelcome is shown (Open Designer, etc.)
  const schemaxTreeProvider: vscode.TreeDataProvider<null> = {
    getTreeItem: () => new vscode.TreeItem('', vscode.TreeItemCollapsibleState.None),
    getChildren: () => Promise.resolve([]),
  };
  const schemaxTree = vscode.window.registerTreeDataProvider('schemax.welcome', schemaxTreeProvider);
  context.subscriptions.push(schemaxTree);

  context.subscriptions.push(
    openDesignerCommand,
    showLastOpsCommand,
    createSnapshotCommand,
    generateSQLCommand,
    importAssetsCommand,
    installPythonSdkCommand,
    outputChannel
  );

  outputChannel.appendLine('[SchemaX] Extension activated successfully!');
  outputChannel.appendLine('[SchemaX] Commands registered: schemax.openDesigner, schemax.showLastOps, schemax.createSnapshot, schemax.generateSQL, schemax.importAssets');
  vscode.window.showInformationMessage('SchemaX Extension Activated!');
  trackEvent('extension_activated');
  void ensureBackendCompatibility(true);
}

export function deactivate() {
  trackEvent('extension_deactivated');
}

function backendCwd(): string {
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (workspaceFolder) {
    return workspaceFolder.uri.fsPath;
  }
  if (extensionContextRef) {
    return extensionContextRef.extensionPath;
  }
  return process.cwd();
}

async function ensureBackendCompatibility(showUserMessage: boolean = false): Promise<boolean> {
  if (backendCompatibilityState.checked && backendCompatibilityState.ok) {
    return true;
  }
  const envelope = await pythonBackend.runJson<RuntimeInfoData>(
    'runtime-info',
    ['runtime-info'],
    backendCwd()
  );
  if (envelope.status !== 'success') {
    backendCompatibilityState.checked = true;
    backendCompatibilityState.ok = false;
    backendCompatibilityState.reason = envelope.errors[0]?.message || 'runtime-info failed';
    if (showUserMessage) {
      vscode.window.showWarningMessage(
        'SchemaX Python SDK runtime check failed. Install/update SDK with: pip install -U schemaxpy'
      );
    }
    return false;
  }
  if (!envelope.data) {
    backendCompatibilityState.checked = true;
    backendCompatibilityState.ok = false;
    backendCompatibilityState.reason = 'runtime-info returned empty payload';
    if (showUserMessage) {
      vscode.window.showWarningMessage(
        'SchemaX Python SDK runtime check returned no data. Please update SDK: pip install -U schemaxpy'
      );
    }
    return false;
  }

  const validation = validateRuntimeInfo(
    envelope.data,
    MIN_SUPPORTED_CLI_VERSION,
    REQUIRED_ENVELOPE_SCHEMA_VERSION
  );
  backendCompatibilityState.checked = true;
  backendCompatibilityState.ok = validation.ok;
  backendCompatibilityState.reason = validation.reason ?? null;
  backendCompatibilityState.runtimeInfo = envelope.data;
  if (!validation.ok && showUserMessage) {
    const cliVersion = envelope.data?.cliVersion ?? 'unknown';
    vscode.window.showErrorMessage(
      `SchemaX Python SDK incompatible (${cliVersion}). ${validation.reason ?? ''} ` +
      `Please update SDK: pip install -U schemaxpy`
    );
  }
  return validation.ok;
}

async function requireCompatibleSdk(workflowName: string): Promise<boolean> {
  const compatible = await ensureBackendCompatibility(true);
  if (compatible) {
    return true;
  }
  outputChannel.appendLine(
    `[SchemaX] Blocked workflow '${workflowName}' due to SDK incompatibility: ` +
    `${backendCompatibilityState.reason ?? 'unknown reason'}`
  );
  return false;
}

/** Check if the SchemaX CLI (schemax) is available on PATH. */
async function checkSchemaxCliAvailable(): Promise<boolean> {
  const execAsync = promisify(execCallback);
  try {
    await execAsync('schemax --version', { timeout: 5000 });
    return true;
  } catch {
    return false;
  }
}

/** If SchemaX CLI is missing, show a message with an option to install the Python SDK. */
export async function promptToInstallPythonSdkIfNeeded(): Promise<void> {
  const available = await checkSchemaxCliAvailable();
  if (available) return;
  const choice = await vscode.window.showInformationMessage(
    'SchemaX CLI not found. Some features (import, snapshot validate, apply) need the Python SDK. Install it?',
    'Install (pip install schemaxpy)',
    'Dismiss'
  );
  if (choice === 'Install (pip install schemaxpy)') {
    await installPythonSdk();
  }
}

/** Get Python executable to use: prefer VS Code Python interpreter setting, then PATH. */
function getPythonExecutable(): string[] {
  const configured = vscode.workspace.getConfiguration('python').get<string>('defaultInterpreterPath');
  if (configured && configured.trim()) {
    return [configured.trim()];
  }
  return ['python3', 'python'];
}

async function installPythonSdk(): Promise<void> {
  const candidates = getPythonExecutable();
  for (const py of candidates) {
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Installing SchemaX Python SDK',
          cancellable: false,
        },
        async () => {
          const execAsync = promisify(execCallback);
          await execAsync(`"${py}" -m pip install schemaxpy`, { timeout: 120000 });
        }
      );
      vscode.window.showInformationMessage(
        'SchemaX Python SDK installed. You can now use the schemax CLI (e.g. schemax validate, schemax apply).'
      );
      outputChannel.appendLine('[SchemaX] Python SDK installed via pip install schemaxpy');
      return;
    } catch (err: unknown) {
      const errMessage = err instanceof Error ? err.message : String(err);
      outputChannel.appendLine(`[SchemaX] ${py} -m pip install schemaxpy failed: ${errMessage}`);
    }
  }
  vscode.window.showErrorMessage(
    'Could not install SchemaX Python SDK. Install it in your Python environment: pip install schemaxpy (or set python.defaultInterpreterPath and try again).'
  );
}

async function executeImport(
  workspacePath: string,
  request: ImportRequest,
  onProgress?: (update: ImportProgressUpdate) => void
): Promise<ImportExecutionResult> {
  let importArgs: string[];

  if (isImportFromSql(request)) {
    importArgs = [
      'import',
      '--from-sql', request.fromSql.sqlPath,
      '--mode', request.fromSql.mode,
    ];
    if (request.fromSql.dryRun) importArgs.push('--dry-run');
    if (request.fromSql.target?.trim()) importArgs.push('--target', request.fromSql.target.trim());
  } else {
    importArgs = [
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
  }

  activeImportCancelled = false;
  activeImportController = new AbortController();

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

    const envelope = await pythonBackend.runJson<Record<string, unknown>>(
      'import',
      importArgs,
      workspacePath,
      {
        signal: activeImportController.signal,
        onStdout: (stdoutChunk) => {
        for (const line of stdoutChunk.split('\n')) {
          const update = parseImportProgressLine(line);
          if (update) {
            emitProgress(update);
          }
        }
      },
        onStderr: (stderrChunk) => {
        const text = stderrChunk.trim();
        if (text) {
          emitProgress({
            phase: 'stderr',
            message: text,
            percent: 30,
            level: 'warning',
          });
        }
        },
      }
    );

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

    if (envelope.status === 'success') {
      const isDryRun = isImportFromSql(request) ? request.fromSql.dryRun : request.dryRun;
      onProgress?.({
        phase: 'completed',
        message: isDryRun ? 'Dry-run completed' : 'Import completed',
        percent: 100,
        level: 'success',
      });
      return {
        success: true,
        command: envelope.meta.executedCommand,
        stdout: JSON.stringify(envelope.data),
        stderr: '',
        envelope,
      };
    }

    onProgress?.({
      phase: 'failed',
      message: 'Import failed. Check stderr/output for details.',
      percent: 100,
      level: 'error',
    });
    return {
      success: false,
      command: envelope.meta.executedCommand,
      stdout: JSON.stringify(envelope.data),
      stderr: envelope.errors.map((error) => error.message).join('\n') || 'Import command failed',
      envelope,
    };
  } finally {
    clearInterval(runningTimer);
    activeImportController = null;
    activeImportCancelled = false;
  }
}

function parseImportProgressLine(line: string): ImportProgressUpdate | null {
  const trimmed = line.replace(/\x1b\[[0-9;]*m/g, '').trim();
  if (!trimmed) return null;

  if (trimmed.includes('SchemaX Import')) {
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
    vscode.window.showErrorMessage('SchemaX: No environments configured in project.json');
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
    placeHolder: 'schemax_demo=dev_schemax_demo',
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
  if (!(await requireCompatibleSdk('import'))) {
    return;
  }
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  try {
    const request = await promptImportRequest(workspaceFolder.uri);
    if (!request) return;
    await runImportWithRequest(workspaceFolder, request);
  } catch (error) {
    vscode.window.showErrorMessage(`SchemaX import failed: ${error}`);
  }
}

async function runImportWithRequest(
  workspaceFolder: vscode.WorkspaceFolder,
  request: ImportRequest,
  panel?: vscode.WebviewPanel
): Promise<ImportExecutionResult> {
  outputChannel.appendLine('[SchemaX] Running import workflow...');
  outputChannel.appendLine(`[SchemaX] Import request: ${JSON.stringify(request)}`);

  const dryRun = isImportFromSql(request) ? request.fromSql.dryRun : request.dryRun;
  const result = await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: dryRun ? 'Running import dry-run...' : 'Importing assets...',
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
    outputChannel.appendLine('[SchemaX] Import stdout:');
    outputChannel.appendLine(result.stdout);
  }
  if (result.stderr) {
    outputChannel.appendLine('[SchemaX] Import stderr:');
    outputChannel.appendLine(result.stderr);
  }

  const targetForTelemetry = isImportFromSql(request) ? request.fromSql.target : request.target;
  const adoptBaselineForTelemetry = isImportFromSql(request) ? undefined : request.adoptBaseline;

  if (result.cancelled) {
    vscode.window.showInformationMessage('SchemaX import cancelled');
    trackEvent('import_cancelled', {
      dryRun,
      adoptBaseline: adoptBaselineForTelemetry,
      target: targetForTelemetry,
    });
  } else if (result.success) {
    vscode.window.showInformationMessage(
      dryRun
        ? 'SchemaX import dry-run completed'
        : 'SchemaX import completed successfully'
    );
    trackEvent('import_completed', {
      dryRun,
      adoptBaseline: adoptBaselineForTelemetry,
      target: targetForTelemetry,
    });
  } else {
    vscode.window.showErrorMessage(`SchemaX import failed. See Output > SchemaX for details.`);
    const stderr = (result.stderr || '').toLowerCase();
    if (
      stderr.includes('not found') ||
      stderr.includes('command not found') ||
      stderr.includes('no module named')
    ) {
      promptToInstallPythonSdkIfNeeded();
    }
    trackEvent('import_failed', {
      dryRun,
      adoptBaseline: adoptBaselineForTelemetry,
      target: targetForTelemetry,
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
          ? (dryRun ? 'Dry-run completed' : 'Import completed')
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
  activeImportController?.abort();
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
      autoCreateSchemaxSchema: true,
    }
  },
  {
    label: 'Staging-like',
    description: '⚠️ No manual changes [planned], ✅ Requires snapshot, ✅ Auto-creates catalog',
    config: {
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: true,
      autoCreateSchemaxSchema: true,
    }
  },
  {
    label: 'Production-like',
    description: '⚠️ No manual changes [planned], ✅ Requires snapshot, Catalog must exist',
    config: {
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: false,
      autoCreateSchemaxSchema: true,
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
  
  outputChannel.appendLine('[SchemaX] Starting custom environment configuration...');
  
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
    
    outputChannel.appendLine(`[SchemaX]   Adding environment: ${envName}`);
    
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
    
    outputChannel.appendLine(`[SchemaX]     Catalog: ${catalogName}`);
    
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
    
    outputChannel.appendLine(`[SchemaX]     Type: ${selectedPreset.label}`);
    
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
      autoCreateSchemaxSchema: presetConfig.autoCreateSchemaxSchema ?? true,
      requireApproval: presetConfig.requireApproval,
    };
    
    outputChannel.appendLine(`[SchemaX]   ✓ Environment '${envName}' configured`);
  }
  
  return customEnvironments;
}

/**
 * Prompt user for project setup (provider, environments, catalog names)
 */
async function promptForProjectSetup(workspaceUri: vscode.Uri, outputChannel: vscode.OutputChannel): Promise<boolean> {
  outputChannel.appendLine('[SchemaX] Starting project setup wizard...');
  
  // Step 1: Select provider (for now, only Unity is available)
  const providers = [
    {
      label: 'Unity Catalog',
      description: 'Databricks Unity Catalog',
      id: 'unity',
      version: '1.0.0',
    },
  ];
  
  const selectedProvider = await vscode.window.showQuickPick(providers, {
    placeHolder: 'Select your catalog provider',
    ignoreFocusOut: true
  });
  
  if (!selectedProvider) {
    return false; // User cancelled
  }
  
  outputChannel.appendLine(`[SchemaX] Provider selected: ${selectedProvider.label}`);
  
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
  
  outputChannel.appendLine(`[SchemaX] Project name: ${projectName}`);
  
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
      autoCreateSchemaxSchema: true,
    },
    test: {
      topLevelName: `test_${sanitizedName}`,
      description: 'Test/staging environment',
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: true,
      autoCreateSchemaxSchema: true,
    },
    prod: {
      topLevelName: `prod_${sanitizedName}`,
      description: 'Production environment',
      allowDrift: false,
      requireSnapshot: true,
      requireApproval: false,
      autoCreateTopLevel: false,
      autoCreateSchemaxSchema: true,
    },
  };
  
  outputChannel.appendLine('[SchemaX] Default environments created:');
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
  
  outputChannel.appendLine(`[SchemaX] Logical catalog name: ${logicalCatalogName}`);
  outputChannel.appendLine('[SchemaX] Physical catalog mappings:');
  Object.entries(environments).forEach(([env, config]) => {
    outputChannel.appendLine(`  - ${logicalCatalogName} → ${config.topLevelName} (${env})`);
  });
  
  // Step 5: Create the project
  try {
    await storageV4.ensureSchemaxDir(workspaceUri);
    
    // Create v4 project
    const newProject: storageV4.ProjectFileV4 = {
      version: 4,
      name: projectName,
      provider: {
        type: selectedProvider.id,
        version: selectedProvider.version,
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
    
    outputChannel.appendLine(`[SchemaX] Created logical catalog: ${logicalCatalogName}`);
    
    const newChangelog: storageV4.ChangelogFile = {
      version: 1,
      sinceSnapshot: null,
      ops: initialOps,
      lastModified: new Date().toISOString(),
    };
    
    await storageV4.writeProject(workspaceUri, newProject);
    await storageV4.writeChangelog(workspaceUri, newChangelog);
    
    outputChannel.appendLine('[SchemaX] ✓ Project created successfully');
    
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
    
    const message = `SchemaX project initialized!\n\n` +
      `Project: ${projectName}\n` +
      `Provider: ${selectedProvider.label}\n` +
      `Environments: ${envCount} (${3} default${customCount > 0 ? ` + ${customCount} custom` : ''})\n\n` +
      envList + '\n\n' +
      `Next steps:\n` +
      `  1. Design your schema in the visual designer\n` +
      `  2. Generate SQL for an environment (Cmd+Shift+P → "Generate SQL")\n` +
      `  3. Apply changes with the CLI: schemax apply --target dev`;
    
    vscode.window.showInformationMessage(message);
    
    return true;
    
  } catch (error) {
    outputChannel.appendLine(`[SchemaX] ERROR: Failed to create project: ${error}`);
    vscode.window.showErrorMessage(`Failed to initialize project: ${error}`);
    return false;
  }
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

  // Check if project already exists
  const projectPath = vscode.Uri.joinPath(workspaceFolder.uri, '.schemax', 'project.json');
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
    'schemaxDesigner',
    'SchemaX Designer',
    vscode.ViewColumn.One,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [
        vscode.Uri.joinPath(context.extensionUri, 'media'),
        vscode.Uri.joinPath(context.extensionUri, 'images'),
      ],
      enableForms: true,
    }
  );

  // Helper function to detect stale snapshots using Python SDK
  async function detectStaleSnapshots(workspacePath: string): Promise<StaleSnapshot[]> {
    if (!(await ensureBackendCompatibility(false))) {
      return [];
    }
    const envelope = await pythonBackend.runJson<{ stale?: StaleSnapshot[]; data?: { stale_snapshots?: StaleSnapshot[] } }>(
      'snapshot.validate',
      ['snapshot', 'validate'],
      workspacePath
    );
    if (envelope.status === 'error') {
      const firstError = envelope.errors[0];
      outputChannel.appendLine(
        `[SchemaX] Failed to detect stale snapshots: ${firstError?.message || 'unknown error'}`
      );
      if (firstError?.message?.includes('not found') || firstError?.message?.includes('ENOENT')) {
        await promptToInstallPythonSdkIfNeeded();
      }
      return [];
    }
    const data = envelope.data ?? {};
    return data.stale ?? data.data?.stale_snapshots ?? [];
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
      const conflictsDir = vscode.Uri.joinPath(workspaceFolder.uri, '.schemax', 'conflicts');
      let conflicts = null;
      try {
        const conflictFiles = await vscode.workspace.fs.readDirectory(conflictsDir);
        if (conflictFiles.length > 0) {
          const latestConflictFile = conflictFiles.sort((a, b) => b[0].localeCompare(a[0]))[0];
          const conflictFilePath = vscode.Uri.joinPath(conflictsDir, latestConflictFile[0]);
          const conflictContent = await vscode.workspace.fs.readFile(conflictFilePath);
          conflicts = JSON.parse(Buffer.from(conflictContent).toString('utf8'));
          outputChannel.appendLine(`[SchemaX] - Rebase conflict detected: ${latestConflictFile[0]}`);
        }
      } catch {
        // No conflicts directory or no conflicts - that's fine
      }

      // Check for stale snapshots (using Python SDK)
      const staleSnapshots = await detectStaleSnapshots(workspaceFolder.uri.fsPath);
      if (staleSnapshots.length > 0) {
        outputChannel.appendLine(`[SchemaX] - Detected ${staleSnapshots.length} stale snapshot(s)`);
      }

      // Log validation results
      if (validationResult) {
        if (validationResult.errors.length > 0) {
          outputChannel.appendLine(`[SchemaX] - Dependency validation ERRORS:`);
          validationResult.errors.forEach((error) => {
            outputChannel.appendLine(`  ✗ ${error}`);
          });
        }
        if (validationResult.warnings.length > 0) {
          outputChannel.appendLine(`[SchemaX] - Dependency validation warnings:`);
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
          id: provider.id,
          name: provider.name,
          version: provider.version,
          capabilities: provider.capabilities,
        },
      };

      panel.webview.postMessage({
        type: 'project-loaded',
        payload: payloadForWebview,
      });
    } catch (error) {
      outputChannel.appendLine(`[SchemaX] ERROR: Failed to reload project: ${error}`);
    }
  }

  // Set webview content
  outputChannel.appendLine('[SchemaX] Setting webview HTML');
  currentPanel.webview.html = getWebviewContent(context, currentPanel.webview);
  outputChannel.appendLine('[SchemaX] Webview HTML set');

  // Watch for conflict files and reload when they appear
  const conflictsPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schemax/conflicts/*.json'
  );
  const conflictWatcher = vscode.workspace.createFileSystemWatcher(conflictsPattern);

  // Reload project when conflict files are created or deleted
  conflictWatcher.onDidCreate(async () => {
    outputChannel.appendLine('[SchemaX] Conflict file detected - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  conflictWatcher.onDidDelete(async () => {
    outputChannel.appendLine('[SchemaX] Conflict file removed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Watch for snapshot file changes (to detect stale snapshots)
  const snapshotsPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schemax/snapshots/*.json'
  );
  const snapshotsWatcher = vscode.workspace.createFileSystemWatcher(snapshotsPattern);

  // Reload project when snapshots are created, changed, or deleted
  snapshotsWatcher.onDidCreate(async () => {
    outputChannel.appendLine('[SchemaX] Snapshot file created - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  snapshotsWatcher.onDidChange(async () => {
    outputChannel.appendLine('[SchemaX] Snapshot file changed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  snapshotsWatcher.onDidDelete(async () => {
    outputChannel.appendLine('[SchemaX] Snapshot file deleted - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Watch for project.json changes (snapshot metadata)
  const projectJsonPattern = new vscode.RelativePattern(
    workspaceFolder,
    '.schemax/project.json'
  );
  const projectJsonWatcher = vscode.workspace.createFileSystemWatcher(projectJsonPattern);

  projectJsonWatcher.onDidChange(async () => {
    outputChannel.appendLine('[SchemaX] project.json changed - reloading project');
    await reloadProject(workspaceFolder, currentPanel);
  });

  // Reset when panel is closed
  currentPanel.onDidDispose(() => {
    outputChannel.appendLine('[SchemaX] Webview panel disposed');
    conflictWatcher.dispose();
    snapshotsWatcher.dispose();
    projectJsonWatcher.dispose();
    currentPanel = undefined;
  });

  // Handle messages from webview
  currentPanel.webview.onDidReceiveMessage(
    async (message) => {
      outputChannel.appendLine(`[SchemaX] Received message from webview: ${message.type}`);
      switch (message.type) {
        case 'refresh-project': {
          outputChannel.appendLine('[SchemaX] Manual refresh requested');
          await reloadProject(workspaceFolder, currentPanel);
          break;
        }

        case 'load-project': {
          try {
            outputChannel.appendLine(`[SchemaX] Loading project from: ${workspaceFolder.uri.fsPath}`);
            
            // Load v3: project metadata + current state (snapshot + changelog) + provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[SchemaX] Project loaded successfully (v${project.version})`);
            outputChannel.appendLine(`[SchemaX] - Provider: ${provider.name} v${provider.version}`);
            outputChannel.appendLine(`[SchemaX] - Snapshots: ${project.snapshots.length}`);
            outputChannel.appendLine(`[SchemaX] - Latest snapshot: ${project.latestSnapshot || 'none'}`);
            outputChannel.appendLine(`[SchemaX] - Changelog ops: ${changelog.ops.length}`);
            
            // For Unity provider, log catalog count (provider-specific)
            if (state && 'catalogs' in state) {
              outputChannel.appendLine(`[SchemaX] - Catalogs: ${(state as CatalogState).catalogs?.length ?? 0}`);
            }
            
            // Check for rebase conflicts
            const conflictsDir = vscode.Uri.joinPath(workspaceFolder.uri, '.schemax', 'conflicts');
            let conflicts = null;
            try {
              const conflictFiles = await vscode.workspace.fs.readDirectory(conflictsDir);
              if (conflictFiles.length > 0) {
                // Read the latest conflict file
                const latestConflictFile = conflictFiles.sort((a, b) => b[0].localeCompare(a[0]))[0];
                const conflictFilePath = vscode.Uri.joinPath(conflictsDir, latestConflictFile[0]);
                const conflictContent = await vscode.workspace.fs.readFile(conflictFilePath);
                conflicts = JSON.parse(Buffer.from(conflictContent).toString('utf8'));
                outputChannel.appendLine(`[SchemaX] - Rebase conflict detected: ${latestConflictFile[0]}`);
              }
            } catch {
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
                id: provider.id,
                name: provider.name,
                version: provider.version,
                capabilities: provider.capabilities,
              },
            };
            
            outputChannel.appendLine(`[SchemaX] Sending to webview:`);
            outputChannel.appendLine(`[SchemaX] - Provider: ${provider.name}`);
            outputChannel.appendLine(`[SchemaX] - Project version: ${project.version}`);
            
            currentPanel?.webview.postMessage({
              type: 'project-loaded',
              payload: payloadForWebview,
            });
            trackEvent('project_loaded', { provider: provider.id });
          } catch (error) {
            outputChannel.appendLine(`[SchemaX] ERROR: Failed to load project: ${error}`);
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
              `3. Run: schemax snapshot create --version ${conflictInfo.snapshot_version}`;
            
            vscode.window.showWarningMessage(
              '⚠️ Snapshot Rebase Conflict',
              { modal: true, detail: detailMessage }
            ).then(() => {
              // User acknowledged the conflict
              outputChannel.appendLine('[SchemaX] User acknowledged rebase conflict');
            });
          }
          break;
        }

        case 'show-stale-snapshot-details': {
          const staleSnapshots = message.payload;
          if (staleSnapshots && staleSnapshots.length > 0) {
            const detailLines = staleSnapshots.map((snap: StaleSnapshot) => {
              return `Snapshot: ${snap.version}\n` +
                `  Current base: ${snap.currentBase}\n` +
                `  Should be: ${snap.shouldBeBase}\n` +
                `  Missing: ${snap.missing.join(', ')}`;
            }).join('\n\n');
            
            const detailMessage = `Found ${staleSnapshots.length} stale snapshot(s):\n\n` +
              detailLines + '\n\n' +
              `Resolution:\n` +
              `Run the following command(s) in terminal:\n` +
              staleSnapshots.map((snap: StaleSnapshot) => `  schemax snapshot rebase ${snap.version}`).join('\n');
            
            vscode.window.showWarningMessage(
              '⚠️ Stale Snapshots Detected',
              { modal: true, detail: detailMessage }
            ).then(() => {
              // User acknowledged the stale snapshots
              outputChannel.appendLine('[SchemaX] User acknowledged stale snapshots');
            });
          }
          break;
        }
        case 'open-docs': {
          try {
            const docUrl = message.payload?.url as string | undefined;
            const docPath = message.payload?.path as string | undefined;
            const fragment = message.payload?.fragment as string | undefined;

            if (docUrl) {
              await vscode.env.openExternal(vscode.Uri.parse(docUrl));
              trackEvent('docs_opened', { url: docUrl });
              break;
            }

            if (!docPath) {
              vscode.window.showWarningMessage('SchemaX: Documentation path or URL was not provided.');
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
            outputChannel.appendLine(`[SchemaX] ERROR: Failed to open docs: ${error}`);
            vscode.window.showErrorMessage('SchemaX: Unable to open documentation.');
          }
          break;
        }
        case 'append-ops': {
          try {
            const ops: Operation[] = message.payload;
            outputChannel.appendLine(`[SchemaX] Appending ${ops.length} operation(s) to changelog`);
            
            // Append to changelog (v3 validates operations via provider)
            await storageV4.appendOps(workspaceFolder.uri, ops);
            
            // Reload state and provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[SchemaX] Operations appended successfully`);
            outputChannel.appendLine(`[SchemaX] - Changelog ops: ${changelog.ops.length}`);
            outputChannel.appendLine(`[SchemaX] - Snapshots: ${project.snapshots.length}`);
            
            // Send updated data to webview including provider info
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                ...project.provider, // Keep environments and other project provider config
                id: provider.id,
                name: provider.name,
                version: provider.version,
                capabilities: provider.capabilities,
              },
            };
            
            currentPanel?.webview.postMessage({
              type: 'project-updated',
              payload: payloadForWebview,
            });
            trackEvent('ops_appended', { count: ops.length, provider: provider.id });
          } catch (error) {
            outputChannel.appendLine(`[SchemaX] ERROR: Failed to append operations: ${error}`);
            vscode.window.showErrorMessage(`Failed to append operations: ${error}`);
          }
          break;
        }
        case 'update-project-config': {
          try {
            const updatedProject = message.payload;
            outputChannel.appendLine(`[SchemaX] Updating project configuration`);
            
            // Write updated project to disk
            await storageV4.writeProject(workspaceFolder.uri, updatedProject);
            
            // Reload state and provider
            const project = await storageV4.readProject(workspaceFolder.uri);
            const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
            
            outputChannel.appendLine(`[SchemaX] Project configuration updated successfully`);
            
            // Send updated data to webview
            const payloadForWebview = {
              ...project,
              state,
              ops: changelog.ops,
              provider: {
                ...project.provider,
                id: provider.id,
                name: provider.name,
                version: provider.version,
                capabilities: provider.capabilities,
              },
            };
            
            currentPanel?.webview.postMessage({
              type: 'project-updated',
              payload: payloadForWebview,
            });
            
            vscode.window.showInformationMessage('SchemaX: Project settings saved successfully');
            trackEvent('project_config_updated', { provider: provider.id });
          } catch (error) {
            outputChannel.appendLine(`[SchemaX] ERROR: Failed to update project config: ${error}`);
            vscode.window.showErrorMessage(`Failed to save project settings: ${error}`);
          }
          break;
        }
        case 'import-pick-sql-file': {
          const folder = vscode.workspace.workspaceFolders?.[0];
          const defaultUri = folder ? folder.uri : undefined;
          const selected = await vscode.window.showOpenDialog({
            defaultUri,
            canSelectFiles: true,
            canSelectFolders: false,
            canSelectMany: false,
            filters: { 'SQL': ['sql'] },
          });
          const path = selected?.length ? selected[0].fsPath : null;
          currentPanel?.webview.postMessage({
            type: 'import-sql-file-picked',
            payload: { path },
          });
          break;
        }
        case 'run-import': {
          try {
            const request = message.payload as ImportRequest;
            const result = await runImportWithRequest(workspaceFolder, request, currentPanel);
            const wasDryRun = isImportFromSql(request) ? request.fromSql.dryRun : request.dryRun;
            if (result.success && !wasDryRun) {
              await reloadProject(workspaceFolder, currentPanel);
            } else if (result.cancelled && !wasDryRun) {
              // Re-sync UI with disk in case cancellation happened during file writes.
              await reloadProject(workspaceFolder, currentPanel);
            }
          } catch (error) {
            outputChannel.appendLine(`[SchemaX] ERROR: Import run failed: ${error}`);
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
          outputChannel.appendLine('[SchemaX] Import cancellation requested from webview');
          break;
        }
      }
    },
    undefined,
    context.subscriptions
  );

  outputChannel.appendLine('[SchemaX] Designer opened successfully');
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
    const project = await storageV4.readProject(workspaceFolder.uri);
    const changelog = await storageV4.readChangelog(workspaceFolder.uri);
    const lastOps = changelog.ops.slice(-20);

    outputChannel.clear();
    outputChannel.appendLine('SchemaX: Last 20 Emitted Changes');
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
  const logoUri = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, 'images', 'schemax_logov2.svg')
  );

  // Use a nonce to only allow specific scripts to be run
  const nonce = getNonce();

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src ${webview.cspSource} data:; style-src ${webview.cspSource} 'unsafe-inline' https:; font-src ${webview.cspSource} https:; script-src 'nonce-${nonce}';">
  <link href="https://cdn.jsdelivr.net/npm/@vscode/codicons@0.0.36/dist/codicon.css" rel="stylesheet">
  <link href="${styleUri}" rel="stylesheet">
  <title>SchemaX Designer</title>
</head>
<body>
  <div id="root" data-logo-uri="${logoUri}"></div>
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
  outputChannel.appendLine('[SchemaX] Create snapshot command invoked');
  
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    outputChannel.appendLine('[SchemaX] ERROR: No workspace folder open');
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  try {
    outputChannel.appendLine(`[SchemaX] Reading project from: ${workspaceFolder.uri.fsPath}`);
    const project = await storageV4.readProject(workspaceFolder.uri);
    const changelog = await storageV4.readChangelog(workspaceFolder.uri);
    const uncommittedOpsCount = changelog.ops.length;
    
    outputChannel.appendLine(`[SchemaX] Provider: ${project.provider.type}`);
    outputChannel.appendLine(`[SchemaX] Uncommitted operations: ${uncommittedOpsCount}`);
    outputChannel.appendLine(`[SchemaX] Existing snapshots: ${project.snapshots.length}`);

    if (uncommittedOpsCount === 0) {
      outputChannel.appendLine('[SchemaX] No uncommitted operations, aborting snapshot creation');
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
      outputChannel.appendLine('[SchemaX] Snapshot creation cancelled by user');
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
      outputChannel.appendLine('[SchemaX] Snapshot creation cancelled by user');
      return; // User cancelled
    }

    const version = versionInput.trim() || undefined; // Empty string becomes undefined

    // Get optional comment
    const comment = await vscode.window.showInputBox({
      prompt: 'Description (optional)',
      placeHolder: 'Describe what changed in this snapshot'
    });

    outputChannel.appendLine(`[SchemaX] Creating snapshot: "${name}"`);
    if (version) {
      outputChannel.appendLine(`[SchemaX] Version: "${version}"`);
    } else {
      outputChannel.appendLine(`[SchemaX] Version: auto-generated (${suggestedVersion})`);
    }
    if (comment) {
      outputChannel.appendLine(`[SchemaX] Comment: "${comment}"`);
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
      
      outputChannel.appendLine(`[SchemaX] Snapshot created successfully!`);
      outputChannel.appendLine(`[SchemaX] - ID: ${snapshot.id}`);
      outputChannel.appendLine(`[SchemaX] - Version: ${snapshot.version}`);
      outputChannel.appendLine(`[SchemaX] - Name: ${snapshot.name}`);
      outputChannel.appendLine(`[SchemaX] - Operations included: ${snapshot.operations.length}`);
      outputChannel.appendLine(`[SchemaX] - Total snapshots: ${updatedProject.snapshots.length}`);
      outputChannel.appendLine(`[SchemaX] - Snapshot file: ${updatedProject.snapshots[updatedProject.snapshots.length - 1].file}`);
      
      progress.report({ increment: 100 });
      
      // Notify webview if it's open
      if (currentPanel) {
        outputChannel.appendLine('[SchemaX] Notifying webview of snapshot creation');
        
        // Reload state and provider for webview
        const { state, changelog, provider } = await storageV4.loadCurrentState(workspaceFolder.uri, false);
        const payloadForWebview = {
          ...updatedProject,
          state,
          ops: changelog.ops,
          provider: {
            ...updatedProject.provider, // Keep environments and other project provider config
            id: provider.id,
            name: provider.name,
            version: provider.version,
            capabilities: provider.capabilities,
          },
        };
        
        currentPanel.webview.postMessage({
          type: 'project-updated',
          payload: payloadForWebview
        });
      } else {
        outputChannel.appendLine('[SchemaX] No webview panel open, skipping notification');
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
    outputChannel.appendLine(`[SchemaX] ERROR: Snapshot creation failed: ${error}`);
    if (error instanceof Error) {
      outputChannel.appendLine(`[SchemaX] Stack trace: ${error.stack}`);
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
async function generateSQLMigration() {
  if (!(await requireCompatibleSdk('sql'))) {
    return;
  }
  outputChannel.appendLine('[SchemaX] Generate SQL migration command invoked');
  
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    outputChannel.appendLine('[SchemaX] ERROR: No workspace folder open');
    vscode.window.showErrorMessage('SchemaX: Please open a workspace folder first.');
    return;
  }

  try {
    outputChannel.appendLine(`[SchemaX] Loading project from: ${workspaceFolder.uri.fsPath}`);
    const project = await storageV4.readProject(workspaceFolder.uri);

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
      outputChannel.appendLine('[SchemaX] SQL generation cancelled - no environment selected');
      return;
    }

    outputChannel.appendLine(`[SchemaX] Target environment: ${targetEnv}`);

    // Create migrations directory
    const migrationsDir = path.join(workspaceFolder.uri.fsPath, '.schemax', 'migrations');
    if (!fs.existsSync(migrationsDir)) {
      fs.mkdirSync(migrationsDir, { recursive: true });
      outputChannel.appendLine(`[SchemaX] Created migrations directory: ${migrationsDir}`);
    }

    // Generate filename with timestamp and environment
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').substring(0, 19);
    const filename = `migration_${targetEnv}_${timestamp}.sql`;
    const filepath = path.join(migrationsDir, filename);
    
    const sqlResult = await pythonBackend.runJson<{ sql?: string }>(
      'sql',
      ['sql', '--target', targetEnv, '--output', filepath],
      workspaceFolder.uri.fsPath
    );
    if (sqlResult.status !== 'success') {
      throw new Error(
        sqlResult.errors.map((error) => error.message).join('\n') ||
        'SQL generation command failed'
      );
    }
    const fileContent = fs.readFileSync(filepath, 'utf8');
    outputChannel.appendLine(`[SchemaX] SQL written to: ${filepath}`);

    // Open the file in editor
    const doc = await vscode.workspace.openTextDocument(filepath);
    await vscode.window.showTextDocument(doc);
    
    vscode.window.showInformationMessage(
      `SQL migration generated: ${filename}`
    );

    trackEvent('sql_generated', {
      sqlLength: fileContent.length,
      provider: project.provider.type
    });

  } catch (error) {
    outputChannel.appendLine(`[SchemaX] ERROR: SQL generation failed: ${error}`);
    if (error instanceof Error) {
      outputChannel.appendLine(`[SchemaX] Stack trace: ${error.stack}`);
    }
    vscode.window.showErrorMessage(`Failed to generate SQL: ${error}`);
  }
}
