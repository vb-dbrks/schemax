/**
 * Storage Layer V4 - Multi-Environment Support
 * 
 * Supports environment-specific catalog configurations with logical → physical name mapping.
 * Breaking change from v3: environments are now rich objects instead of simple arrays.
 */

import * as vscode from 'vscode';
import * as path from 'path';
import * as crypto from 'crypto';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { PythonBackendClient } from './backend/pythonBackendClient';
import type { Operation, ProviderCapabilities } from './contracts/workspace';

const SCHEMAX_DIR = '.schemax';
const PROJECT_FILENAME = 'project.json';
const CHANGELOG_FILENAME = 'changelog.json';
const SNAPSHOTS_DIR = 'snapshots';
const pythonBackend = new PythonBackendClient();
const DEFAULT_PROVIDER_VERSION = '1.0.0';
const STATE_CACHE_TTL_MS = 750;

type ProviderState = Record<string, unknown>;
type WorkspaceStatePayloadMode = 'full' | 'state-only';

export interface ProviderMetadata {
  id: string;
  name: string;
  version: string;
  capabilities: ProviderCapabilities;
}

interface RawProviderMetadata {
  id: string;
  name: string;
  version: string;
  capabilities?: Record<string, unknown>;
}

interface RawWorkspaceStatePayload {
  state?: ProviderState;
  changelog?: ChangelogFile;
  provider?: RawProviderMetadata;
  validation?: {
    errors?: Array<{ message?: string } | string>;
    warnings?: Array<{ message?: string } | string>;
  };
}

interface LoadStateOptions {
  payloadMode?: WorkspaceStatePayloadMode;
}

interface CachedStateEntry {
  cachedAtMs: number;
  value: {
    state: ProviderState;
    changelog: ChangelogFile;
    provider: ProviderMetadata;
    validationResult: ValidationResult | null;
  };
}

const workspaceStateCache = new Map<string, CachedStateEntry>();
const lastWriteHashByPath = new Map<string, string>();

function getProviderDisplayName(providerId: string): string {
  if (providerId === 'unity') {
    return 'Unity Catalog';
  }
  if (providerId === 'hive') {
    return 'Hive Metastore';
  }
  return providerId;
}

// Location Configuration Types
export interface LocationDefinition {
  description?: string;
  paths: Record<string, string>; // environmentName -> physical path
}

export interface ExistingObjectsConfig {
  catalog?: string[];
  schema?: string[];
  table?: string[];
}

export interface EnvironmentConfig {
  topLevelName: string;
  catalogMappings?: Record<string, string>;
  description?: string;
  allowDrift: boolean;
  requireSnapshot: boolean;
  requireApproval?: boolean;
  autoCreateTopLevel: boolean;
  autoCreateSchemaxSchema: boolean;
  /** Deployment scope: which managed categories to emit (default: all) */
  managedCategories?: string[];
  /** Objects that already exist; skip CREATE for these */
  existingObjects?: ExistingObjectsConfig;
}

// Project File V4 Types
interface ProviderConfigV4 {
  type: string; // 'unity', 'hive', 'postgres'
  version: string; // Provider schema version
  environments: Record<string, EnvironmentConfig>;
}

interface ProjectSettings {
  autoIncrementVersion: boolean;
  versionPrefix: string;
}

interface SnapshotMetadata {
  id: string;
  version: string;
  name: string;
  ts: string;
  createdBy?: string;
  file: string;
  previousSnapshot: string | null;
  opsCount: number;
  hash: string;
  tags: string[];
  comment?: string;
}

interface Deployment {
  id: string;
  environment: string;
  ts: string;
  deployedBy?: string;
  snapshotId: string | null;
  opsApplied: string[];
  schemaVersion: string;
  sqlGenerated?: string;
  status: 'success' | 'failed' | 'partial' | 'rolled_back';
  error?: string;
  driftDetected?: boolean;
  driftDetails?: unknown[];
}

export interface ProjectFileV4 {
  version: 4;
  name: string;
  provider: ProviderConfigV4;
  
  // Physical Isolation (for managed tables) - project-level with per-environment paths
  managedLocations?: Record<string, LocationDefinition>;
  
  // External Locations (for external tables) - project-level with per-environment paths
  externalLocations?: Record<string, LocationDefinition>;
  
  snapshots: SnapshotMetadata[];
  deployments: Deployment[];
  settings: ProjectSettings;
  latestSnapshot: string | null;
}

export interface ChangelogFile {
  version: 1;
  sinceSnapshot: string | null;
  ops: Operation[];
  lastModified: string;
}

export interface SnapshotFile {
  id: string;
  version: string;
  name: string;
  ts: string;
  createdBy?: string;
  state: ProviderState;
  operations: Operation[]; // Full operation objects
  previousSnapshot: string | null;
  hash: string;
  tags: string[];
  comment?: string;
}

/**
 * Get schemax directory path
 */
function getSchemaxDir(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMAX_DIR);
}

/**
 * Get project file path
 */
function getProjectFilePath(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMAX_DIR, PROJECT_FILENAME);
}

/**
 * Get changelog file path
 */
function getChangelogFilePath(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMAX_DIR, CHANGELOG_FILENAME);
}

function getWorkspaceStateCacheKey(
  workspaceUri: vscode.Uri,
  validate: boolean,
  payloadMode: WorkspaceStatePayloadMode
): string {
  return `${workspaceUri.fsPath}::${validate ? 'validate' : 'no-validate'}::${payloadMode}`;
}

function invalidateWorkspaceStateCache(workspaceUri: vscode.Uri): void {
  const prefix = `${workspaceUri.fsPath}::`;
  for (const key of workspaceStateCache.keys()) {
    if (key.startsWith(prefix)) {
      workspaceStateCache.delete(key);
    }
  }
}

function cloneStateResult<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function hashText(text: string): string {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function writeJsonFileCoalesced(uri: vscode.Uri, payload: unknown): Promise<void> {
  const text = JSON.stringify(payload, null, 2);
  const nextHash = hashText(text);
  const key = uri.fsPath;
  if (lastWriteHashByPath.get(key) === nextHash) {
    return;
  }
  await vscode.workspace.fs.writeFile(uri, Buffer.from(text, 'utf8'));
  lastWriteHashByPath.set(key, nextHash);
}

/**
 * Get snapshots directory path
 */
function getSnapshotsDir(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMAX_DIR, SNAPSHOTS_DIR);
}

/**
 * Get snapshot file path
 */
function getSnapshotFilePath(workspaceUri: vscode.Uri, version: string): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMAX_DIR, SNAPSHOTS_DIR, `${version}.json`);
}

function isFileNotFoundError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: string }).code === 'FileNotFound'
  );
}

/**
 * Ensure .schemax/ directory structure exists
 */
export async function ensureSchemaxDir(workspaceUri: vscode.Uri): Promise<void> {
  const schemaxDir = getSchemaxDir(workspaceUri);
  const snapshotsDir = getSnapshotsDir(workspaceUri);

  try {
    await vscode.workspace.fs.createDirectory(schemaxDir);
    await vscode.workspace.fs.createDirectory(snapshotsDir);
  } catch (_error) {
    // Directory might already exist, ignore
  }
}

/**
 * Initialize a new v4 project with environment configuration
 */
export async function ensureProjectFile(
  workspaceUri: vscode.Uri,
  outputChannel: vscode.OutputChannel,
  providerId: string = 'unity'
): Promise<void> {
  const projectPath = getProjectFilePath(workspaceUri);
  
  try {
    // Check if project file already exists
    await vscode.workspace.fs.stat(projectPath);
    
    // File exists, check version
    const content = await vscode.workspace.fs.readFile(projectPath);
    const project = JSON.parse(content.toString()) as ProjectFileV4;
    
    if (project.version === 4) {
      outputChannel.appendLine('[SchemaX] Project file already exists (v4)');
      return;
    } else {
      throw new Error(
        `Project version ${project.version} not supported. ` +
        'This version of SchemaX requires v4 projects. ' +
        'Please create a new project or manually migrate to v4.'
      );
    }
  } catch (error: unknown) {
    if (!isFileNotFoundError(error)) {
      throw error;
    }
  }

  // Create new v4 project
  const workspaceName = path.basename(workspaceUri.fsPath);
  
  // Create v4 project with environment configuration
  const newProject: ProjectFileV4 = {
    version: 4,
    name: workspaceName,
    provider: {
      type: providerId,
      version: DEFAULT_PROVIDER_VERSION,
      environments: {
        dev: {
          topLevelName: `dev_${workspaceName}`,
          catalogMappings: {},
          description: 'Development environment',
          allowDrift: true,
          requireSnapshot: false,
          autoCreateTopLevel: true,
          autoCreateSchemaxSchema: true,
        },
        test: {
          topLevelName: `test_${workspaceName}`,
          catalogMappings: {},
          description: 'Test/staging environment',
          allowDrift: false,
          requireSnapshot: true,
          autoCreateTopLevel: true,
          autoCreateSchemaxSchema: true,
        },
        prod: {
          topLevelName: `prod_${workspaceName}`,
          catalogMappings: {},
          description: 'Production environment',
          allowDrift: false,
          requireSnapshot: true,
          requireApproval: false,
          autoCreateTopLevel: false,
          autoCreateSchemaxSchema: true,
        },
      },
    },
    snapshots: [],
    deployments: [],
    settings: {
      autoIncrementVersion: true,
      versionPrefix: 'v',
    },
    latestSnapshot: null,
  };

  const newChangelog: ChangelogFile = {
    version: 1,
    sinceSnapshot: null,
    ops: [],
    lastModified: new Date().toISOString(),
  };

  await ensureSchemaxDir(workspaceUri);

  // Write project file
  const projectContent = Buffer.from(JSON.stringify(newProject, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(projectPath, projectContent);

  // Write changelog file
  const changelogPath = getChangelogFilePath(workspaceUri);
  const changelogContent = Buffer.from(JSON.stringify(newChangelog, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(changelogPath, changelogContent);

  outputChannel.appendLine(`[SchemaX] Initialized new v4 project: ${workspaceName}`);
  outputChannel.appendLine(`[SchemaX] Provider: ${getProviderDisplayName(providerId)}`);
  outputChannel.appendLine('[SchemaX] Environments: dev, test, prod');
}

/**
 * Read project file (v4 only)
 */
export async function readProject(workspaceUri: vscode.Uri): Promise<ProjectFileV4> {
  const projectPath = getProjectFilePath(workspaceUri);

  try {
    const content = await vscode.workspace.fs.readFile(projectPath);
    const project = JSON.parse(content.toString()) as ProjectFileV4;

    // Enforce v4
    if (project.version !== 4) {
      throw new Error(
        `Project version ${project.version} not supported. ` +
        'This version of SchemaX requires v4 projects. ' +
        'Please create a new project or manually migrate to v4.'
      );
    }

    return project;
  } catch (error: unknown) {
    if (isFileNotFoundError(error)) {
      throw new Error(
        'Project file not found. Please initialize a new project first.'
      );
    }
    throw error;
  }
}

/**
 * Write project file
 */
export async function writeProject(
  workspaceUri: vscode.Uri,
  project: ProjectFileV4
): Promise<void> {
  const projectPath = getProjectFilePath(workspaceUri);
  await writeJsonFileCoalesced(projectPath, project);
  invalidateWorkspaceStateCache(workspaceUri);
}

/**
 * Read changelog file
 */
export async function readChangelog(workspaceUri: vscode.Uri): Promise<ChangelogFile> {
  const changelogPath = getChangelogFilePath(workspaceUri);

  try {
    const content = await vscode.workspace.fs.readFile(changelogPath);
    return JSON.parse(content.toString()) as ChangelogFile;
  } catch (error: unknown) {
    if (isFileNotFoundError(error)) {
      // Changelog doesn't exist, create empty one
      const changelog: ChangelogFile = {
        version: 1,
        sinceSnapshot: null,
        ops: [],
        lastModified: new Date().toISOString(),
      };

      const content = Buffer.from(JSON.stringify(changelog, null, 2), 'utf8');
      await vscode.workspace.fs.writeFile(changelogPath, content);

      return changelog;
    }
    throw error;
  }
}

/**
 * Write changelog file
 */
export async function writeChangelog(
  workspaceUri: vscode.Uri,
  changelog: ChangelogFile
): Promise<void> {
  changelog.lastModified = new Date().toISOString();
  const changelogPath = getChangelogFilePath(workspaceUri);
  await writeJsonFileCoalesced(changelogPath, changelog);
  invalidateWorkspaceStateCache(workspaceUri);
}

/**
 * Read snapshot file
 */
export async function readSnapshot(
  workspaceUri: vscode.Uri,
  version: string
): Promise<SnapshotFile> {
  const snapshotPath = getSnapshotFilePath(workspaceUri, version);
  const content = await vscode.workspace.fs.readFile(snapshotPath);
  return JSON.parse(content.toString()) as SnapshotFile;
}

/**
 * Write snapshot file
 */
export async function writeSnapshot(
  workspaceUri: vscode.Uri,
  snapshot: SnapshotFile
): Promise<void> {
  await ensureSchemaxDir(workspaceUri);
  const snapshotPath = getSnapshotFilePath(workspaceUri, snapshot.version);
  await writeJsonFileCoalesced(snapshotPath, snapshot);
  invalidateWorkspaceStateCache(workspaceUri);
}

/**
 * Load current state (snapshot + changelog ops)
 */
export interface ValidationResult {
  errors: string[];
  warnings: string[];
}

export function normalizeProviderCapabilities(
  capabilities: Record<string, unknown> | undefined
): ProviderCapabilities {
  const rawLevels = ((capabilities?.hierarchy as { levels?: unknown[] } | undefined)?.levels ??
    []) as Array<Record<string, unknown>>;
  const levels = rawLevels.map((level) => ({
    name: typeof level.name === 'string' ? level.name : undefined,
    displayName:
      typeof level.display_name === 'string'
        ? level.display_name
        : typeof level.displayName === 'string'
          ? level.displayName
          : undefined,
    pluralName:
      typeof level.plural_name === 'string'
        ? level.plural_name
        : typeof level.pluralName === 'string'
          ? level.pluralName
          : undefined,
    icon: typeof level.icon === 'string' ? level.icon : undefined,
    isContainer:
      typeof level.is_container === 'boolean'
        ? level.is_container
        : typeof level.isContainer === 'boolean'
          ? level.isContainer
          : undefined,
  }));
  const rawFeatures = capabilities?.features;
  const features: Record<string, boolean> =
    typeof rawFeatures === 'object' && rawFeatures !== null
      ? Object.fromEntries(
          Object.entries(rawFeatures).filter((entry): entry is [string, boolean] => {
            const [, value] = entry;
            return typeof value === 'boolean';
          })
        )
      : {};
  return {
    supportedOperations: Array.isArray(capabilities?.supported_operations)
      ? (capabilities?.supported_operations as string[])
      : Array.isArray(capabilities?.supportedOperations)
        ? (capabilities?.supportedOperations as string[])
        : [],
    supportedObjectTypes: Array.isArray(capabilities?.supported_object_types)
      ? (capabilities?.supported_object_types as string[])
      : Array.isArray(capabilities?.supportedObjectTypes)
        ? (capabilities?.supportedObjectTypes as string[])
        : [],
    hierarchy: { levels },
    features,
  };
}

export async function loadCurrentState(
  workspaceUri: vscode.Uri,
  validate: boolean = false,
  options: LoadStateOptions = {}
): Promise<{
  state: ProviderState;
  changelog: ChangelogFile;
  provider: ProviderMetadata;
  validationResult: ValidationResult | null;
}> {
  const payloadMode = options.payloadMode ?? 'full';
  const cacheKey = getWorkspaceStateCacheKey(workspaceUri, validate, payloadMode);
  const cached = workspaceStateCache.get(cacheKey);
  if (cached && Date.now() - cached.cachedAtMs <= STATE_CACHE_TTL_MS) {
    return cloneStateResult(cached.value);
  }

  const args = ['workspace-state'];
  if (validate) {
    args.push('--validate-dependencies');
  }
  if (payloadMode !== 'full') {
    args.push('--payload-mode', payloadMode);
  }
  const envelope = await pythonBackend.runJson<RawWorkspaceStatePayload>(
    'workspace-state',
    args,
    workspaceUri.fsPath
  );
  if (envelope.status === 'error' || !envelope.data?.state || !envelope.data?.changelog || !envelope.data?.provider) {
    const message = envelope.errors[0]?.message || 'Could not load workspace state from Python backend';
    throw new Error(message);
  }

  const data = envelope.data;
  const state = data.state;
  const changelog = data.changelog;
  const provider = data.provider;
  if (!state || !changelog || !provider) {
    throw new Error('Python backend returned incomplete workspace-state payload');
  }
  const validation = data.validation;
  const result = {
    state,
    changelog,
    provider: {
      ...provider,
      capabilities: normalizeProviderCapabilities(provider.capabilities),
    },
    validationResult: validation
      ? {
          errors: (validation.errors ?? []).map((item) => {
            if (typeof item === 'string') return item;
            return item.message || 'Unknown validation error';
          }),
          warnings: (validation.warnings ?? []).map((item) => {
            if (typeof item === 'string') return item;
            return item.message || 'Unknown validation warning';
          }),
        }
      : null,
  };
  workspaceStateCache.set(cacheKey, {
    cachedAtMs: Date.now(),
    value: cloneStateResult(result),
  });
  return result;
}

/**
 * Sanitize a logical catalog name for use as a physical name segment (alphanumeric + underscore).
 */
function sanitizeCatalogNameForPhysical(name: string): string {
  return (name || '').replace(/[^a-zA-Z0-9_]/g, '_');
}

/**
 * Ensure every catalog created by add_catalog ops has a mapping in each environment.
 * When the user drops the default catalog and adds a new one, the new catalog's logical name
 * is not in project.provider.environments.<env>.catalogMappings; apply/sql would then fail.
 * This adds default mappings (envName_sanitizedCatalogName) for any new catalog so apply works.
 */
export function ensureCatalogMappingsForNewCatalogs(
  project: ProjectFileV4,
  ops: Operation[]
): { project: ProjectFileV4; updated: boolean } {
  const addCatalogOps = ops.filter(
    (op) => op.op?.endsWith('add_catalog') && (op.payload as { name?: string })?.name
  );
  const newNames = [...new Set(addCatalogOps.map((op) => (op.payload as { name: string }).name))];
  if (newNames.length === 0 || !project.provider?.environments) {
    return { project, updated: false };
  }

  let updated = false;
  const environments: Record<string, EnvironmentConfig> = {};

  for (const [envName, config] of Object.entries(project.provider.environments)) {
    const catalogMappings = { ...(config.catalogMappings || {}) };
    for (const logicalName of newNames) {
      if (catalogMappings[logicalName] !== undefined) continue;
      const sanitized = sanitizeCatalogNameForPhysical(logicalName);
      catalogMappings[logicalName] = `${envName}_${sanitized}`;
      updated = true;
    }
    environments[envName] = { ...config, catalogMappings };
  }

  if (!updated) return { project, updated: false };

  return {
    project: {
      ...project,
      provider: {
        ...project.provider,
        environments,
      },
    },
    updated: true,
  };
}

/**
 * Append operations to changelog
 */
export async function appendOps(
  workspaceUri: vscode.Uri,
  ops: Operation[]
): Promise<void> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);
  validateOperationBatch(ops);

  // Append ops
  changelog.ops.push(...ops);

  // Write back changelog
  const writeTasks: Promise<void>[] = [writeChangelog(workspaceUri, changelog)];

  // When user adds a new catalog (e.g. after dropping the default), add its logical name to
  // each environment's catalogMappings so apply/sql don't fail with "Missing catalog mapping(s)"
  const { project: projectAfterMappings, updated } = ensureCatalogMappingsForNewCatalogs(
    project,
    ops
  );
  if (updated) {
    writeTasks.push(writeProject(workspaceUri, projectAfterMappings));
  }
  await Promise.all(writeTasks);
}

/**
 * Create a snapshot
 */
export async function createSnapshot(
  workspaceUri: vscode.Uri,
  name: string,
  version?: string,
  comment?: string,
  tags: string[] = []
): Promise<{ project: ProjectFileV4; snapshot: SnapshotFile }> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);

  // Load current state
  const { state } = await loadCurrentState(workspaceUri, false, { payloadMode: 'state-only' });

  // Determine version
  const snapshotVersion = version || getNextVersion(project.latestSnapshot, project.settings);

  // Generate IDs for ops that don't have them
  const opsWithIds = changelog.ops.map((op, i) => {
    if (!op.id) {
      return { ...op, id: `op_${i}_${op.ts}_${op.target}` };
    }
    return op;
  });

  // Calculate hash (includes full operations)
  const stateHash = calculateStateHash(state, opsWithIds);

  // Get username
  const username = os.userInfo().username || 'unknown';

  // Create snapshot file
  const snapshotFile: SnapshotFile = {
    id: `snap_${uuidv4()}`,
    version: snapshotVersion,
    name,
    ts: new Date().toISOString(),
    createdBy: username,
    state,
    operations: opsWithIds, // Full operation objects
    previousSnapshot: project.latestSnapshot,
    hash: stateHash,
    tags,
    comment,
  };

  // Write snapshot file
  await writeSnapshot(workspaceUri, snapshotFile);

  // Create snapshot metadata
  const snapshotMetadata: SnapshotMetadata = {
    id: snapshotFile.id,
    version: snapshotVersion,
    name,
    ts: snapshotFile.ts,
    createdBy: snapshotFile.createdBy,
    file: `.schemax/snapshots/${snapshotVersion}.json`,
    previousSnapshot: project.latestSnapshot,
    opsCount: opsWithIds.length,
    hash: stateHash,
    tags,
    comment,
  };

  // Update project
  project.snapshots.push(snapshotMetadata);
  project.latestSnapshot = snapshotVersion;
  await writeProject(workspaceUri, project);

  // Clear changelog
  const newChangelog: ChangelogFile = {
    version: 1,
    sinceSnapshot: snapshotVersion,
    ops: [],
    lastModified: new Date().toISOString(),
  };
  await writeChangelog(workspaceUri, newChangelog);

  return { project, snapshot: snapshotFile };
}

/**
 * Get uncommitted operations count
 */
export async function getUncommittedOpsCount(workspaceUri: vscode.Uri): Promise<number> {
  const changelog = await readChangelog(workspaceUri);
  return changelog.ops.length;
}

/**
 * Get environment configuration
 */
export function getEnvironmentConfig(
  project: ProjectFileV4,
  environment: string
): EnvironmentConfig {
  const envConfig = project.provider.environments[environment];
  
  if (!envConfig) {
    const available = Object.keys(project.provider.environments).join(', ');
    throw new Error(
      `Environment '${environment}' not found in project. ` +
      `Available environments: ${available}`
    );
  }
  
  return envConfig;
}

/**
 * Calculate state hash for integrity checking
 */
function calculateStateHash(state: ProviderState, operations: Operation[]): string {
  const content = JSON.stringify({ state, operations }, null, 0);
  return crypto.createHash('sha256').update(content).digest('hex');
}

/**
 * Get next version number
 */
function getNextVersion(currentVersion: string | null, settings: ProjectSettings): string {
  if (!currentVersion) {
    return settings.versionPrefix + '0.1.0';
  }

  // Parse version (e.g., "v0.1.0" or "0.1.0")
  const match = currentVersion.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) {
    return settings.versionPrefix + '0.1.0';
  }

  const [, major, minor] = match;
  const nextMinor = parseInt(minor, 10) + 1;

  return `${settings.versionPrefix}${major}.${nextMinor}.0`;
}

function validateOperationBatch(ops: Operation[]): void {
  for (const operation of ops) {
    const missingFields: string[] = [];
    if (!operation.provider?.trim()) missingFields.push('provider');
    if (!operation.op?.trim()) missingFields.push('op');
    if (!operation.target?.trim()) missingFields.push('target');
    if (!operation.ts?.trim()) missingFields.push('ts');
    if (missingFields.length > 0) {
      throw new Error(`Invalid operation: missing required field(s): ${missingFields.join(', ')}`);
    }
  }
}
