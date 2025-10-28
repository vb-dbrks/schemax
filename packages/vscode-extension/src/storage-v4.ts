/**
 * Storage Layer V4 - Multi-Environment Support
 * 
 * Supports environment-specific catalog configurations with logical → physical name mapping.
 * Breaking change from v3: environments are now rich objects instead of simple arrays.
 */

import * as vscode from 'vscode';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { ProviderRegistry } from './providers/registry';
import { Provider } from './providers/base/provider';
import { Operation } from './providers/base/operations';
import { ProviderState } from './providers/base/models';

const SCHEMATIC_DIR = '.schematic';
const PROJECT_FILENAME = 'project.json';
const CHANGELOG_FILENAME = 'changelog.json';
const SNAPSHOTS_DIR = 'snapshots';

// Environment Configuration Types
export interface EnvironmentConfig {
  topLevelName: string;
  description?: string;
  allowDrift: boolean;
  requireSnapshot: boolean;
  requireApproval?: boolean;
  autoCreateTopLevel: boolean;
  autoCreateSchematicSchema: boolean;
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
  driftDetails?: any[];
}

export interface ProjectFileV4 {
  version: 4;
  name: string;
  provider: ProviderConfigV4;
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
  opsIncluded: string[];
  previousSnapshot: string | null;
  hash: string;
  tags: string[];
  comment?: string;
}

/**
 * Get schematic directory path
 */
function getSchematicDir(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMATIC_DIR);
}

/**
 * Get project file path
 */
function getProjectFilePath(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMATIC_DIR, PROJECT_FILENAME);
}

/**
 * Get changelog file path
 */
function getChangelogFilePath(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMATIC_DIR, CHANGELOG_FILENAME);
}

/**
 * Get snapshots directory path
 */
function getSnapshotsDir(workspaceUri: vscode.Uri): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMATIC_DIR, SNAPSHOTS_DIR);
}

/**
 * Get snapshot file path
 */
function getSnapshotFilePath(workspaceUri: vscode.Uri, version: string): vscode.Uri {
  return vscode.Uri.joinPath(workspaceUri, SCHEMATIC_DIR, SNAPSHOTS_DIR, `${version}.json`);
}

/**
 * Ensure .schematic/ directory structure exists
 */
export async function ensureSchematicDir(workspaceUri: vscode.Uri): Promise<void> {
  const schematicDir = getSchematicDir(workspaceUri);
  const snapshotsDir = getSnapshotsDir(workspaceUri);

  try {
    await vscode.workspace.fs.createDirectory(schematicDir);
    await vscode.workspace.fs.createDirectory(snapshotsDir);
  } catch (error) {
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
      outputChannel.appendLine('[Schematic] Project file already exists (v4)');
      return;
    } else {
      throw new Error(
        `Project version ${project.version} not supported. ` +
        'This version of Schematic requires v4 projects. ' +
        'Please create a new project or manually migrate to v4.'
      );
    }
  } catch (error: any) {
    if (error.code !== 'FileNotFound') {
      throw error;
    }
  }

  // Create new v4 project
  const workspaceName = path.basename(workspaceUri.fsPath);
  
  // Get provider
  const provider = ProviderRegistry.get(providerId);
  if (!provider) {
    const available = ProviderRegistry.getAllIds().join(', ');
    throw new Error(
      `Provider '${providerId}' not found. Available providers: ${available}`
    );
  }

  // Create v4 project with environment configuration
  const newProject: ProjectFileV4 = {
    version: 4,
    name: workspaceName,
    provider: {
      type: providerId,
      version: provider.info.version,
      environments: {
        dev: {
          topLevelName: `dev_${workspaceName}`,
          description: 'Development environment',
          allowDrift: true,
          requireSnapshot: false,
          autoCreateTopLevel: true,
          autoCreateSchematicSchema: true,
        },
        test: {
          topLevelName: `test_${workspaceName}`,
          description: 'Test/staging environment',
          allowDrift: false,
          requireSnapshot: true,
          autoCreateTopLevel: true,
          autoCreateSchematicSchema: true,
        },
        prod: {
          topLevelName: `prod_${workspaceName}`,
          description: 'Production environment',
          allowDrift: false,
          requireSnapshot: true,
          requireApproval: false,
          autoCreateTopLevel: false,
          autoCreateSchematicSchema: true,
        },
      },
    },
    snapshots: [],
    deployments: [],
    settings: {
      autoIncrementVersion: true,
      versionPrefix: 'v',
      catalogMode: 'single',
    },
    latestSnapshot: null,
  };

  const newChangelog: ChangelogFile = {
    version: 1,
    sinceSnapshot: null,
    ops: [],
    lastModified: new Date().toISOString(),
  };

  await ensureSchematicDir(workspaceUri);

  // Write project file
  const projectContent = Buffer.from(JSON.stringify(newProject, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(projectPath, projectContent);

  // Write changelog file
  const changelogPath = getChangelogFilePath(workspaceUri);
  const changelogContent = Buffer.from(JSON.stringify(newChangelog, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(changelogPath, changelogContent);

  outputChannel.appendLine(`[Schematic] Initialized new v4 project: ${workspaceName}`);
  outputChannel.appendLine(`[Schematic] Provider: ${provider.info.name}`);
  outputChannel.appendLine('[Schematic] Environments: dev, test, prod');
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
        'This version of Schematic requires v4 projects. ' +
        'Please create a new project or manually migrate to v4.'
      );
    }

    return project;
  } catch (error: any) {
    if (error.code === 'FileNotFound') {
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
  const content = Buffer.from(JSON.stringify(project, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(projectPath, content);
}

/**
 * Read changelog file
 */
export async function readChangelog(workspaceUri: vscode.Uri): Promise<ChangelogFile> {
  const changelogPath = getChangelogFilePath(workspaceUri);

  try {
    const content = await vscode.workspace.fs.readFile(changelogPath);
    return JSON.parse(content.toString()) as ChangelogFile;
  } catch (error: any) {
    if (error.code === 'FileNotFound') {
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
  const content = Buffer.from(JSON.stringify(changelog, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(changelogPath, content);
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
  await ensureSchematicDir(workspaceUri);
  const snapshotPath = getSnapshotFilePath(workspaceUri, snapshot.version);
  const content = Buffer.from(JSON.stringify(snapshot, null, 2), 'utf8');
  await vscode.workspace.fs.writeFile(snapshotPath, content);
}

/**
 * Load current state (snapshot + changelog ops)
 */
export async function loadCurrentState(
  workspaceUri: vscode.Uri
): Promise<{
  state: ProviderState;
  changelog: ChangelogFile;
  provider: Provider;
}> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);

  // Get provider
  const provider = ProviderRegistry.get(project.provider.type);
  if (!provider) {
    throw new Error(
      `Provider '${project.provider.type}' not found. ` +
      'Please ensure the provider is installed.'
    );
  }

  // Load state
  let state: ProviderState;
  if (project.latestSnapshot) {
    // Load latest snapshot
    const snapshot = await readSnapshot(workspaceUri, project.latestSnapshot);
    state = snapshot.state;
  } else {
    // No snapshots yet, start with empty state
    state = provider.createInitialState();
  }

  // Apply changelog ops using provider's state reducer
  state = provider.applyOperations(state, changelog.ops);

  return { state, changelog, provider };
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
  const provider = ProviderRegistry.get(project.provider.type);

  if (!provider) {
    throw new Error(`Provider '${project.provider.type}' not found`);
  }

  // Validate operations
  for (const op of ops) {
    const validation = provider.validateOperation(op);
    if (!validation.valid) {
      const errors = validation.errors.map(e => `${e.field}: ${e.message}`).join(', ');
      throw new Error(`Invalid operation: ${errors}`);
    }
  }

  // Append ops
  changelog.ops.push(...ops);

  // Write back
  await writeChangelog(workspaceUri, changelog);
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
  const { state } = await loadCurrentState(workspaceUri);

  // Determine version
  const snapshotVersion = version || getNextVersion(project.latestSnapshot, project.settings);

  // Generate IDs for ops that don't have them
  const opsWithIds = changelog.ops.map((op, i) => {
    if (!op.id) {
      return { ...op, id: `op_${i}_${op.ts}_${op.target}` };
    }
    return op;
  });

  // Calculate hash
  const stateHash = calculateStateHash(state, opsWithIds.map(op => op.id));

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
    opsIncluded: opsWithIds.map(op => op.id),
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
    file: `.schematic/snapshots/${snapshotVersion}.json`,
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
function calculateStateHash(state: ProviderState, opsIncluded: string[]): string {
  const content = JSON.stringify({ state, opsIncluded }, null, 0);
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

