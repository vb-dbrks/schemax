/**
 * Storage Layer V3 - Provider-Aware
 * 
 * New storage layer that supports multiple catalog providers through the provider system.
 * Migrates from v2 storage format to v3 format with provider metadata.
 */

import * as vscode from 'vscode';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { ProviderRegistry } from './providers/registry';
import { Provider } from './providers/base/provider';
import { Operation } from './providers/base/operations';
import { ProviderState } from './providers/base/models';

const SCHEMAX_DIR = '.schemax';
const PROJECT_FILENAME = 'project.json';
const CHANGELOG_FILENAME = 'changelog.json';
const SNAPSHOTS_DIR = 'snapshots';

// Project File V3 Types
interface ProviderConfig {
  type: string; // 'unity', 'hive', 'postgres'
  version: string; // Provider schema version
  config?: Record<string, any>; // Provider-specific config
}

interface ProjectSettings {
  autoIncrementVersion: boolean;
  versionPrefix: string;
  requireSnapshotForProd: boolean;
  allowDrift: boolean;
  requireComments: boolean;
  warnOnBreakingChanges: boolean;
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
  status: 'success' | 'failed' | 'rolled_back';
  error?: string;
  driftDetected: boolean;
  driftDetails?: any[];
}

export interface ProjectFileV3 {
  version: 3;
  name: string;
  provider: ProviderConfig;
  environments: string[];
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
 * Get the project file path
 */
function getProjectFilePath(workspaceUri: vscode.Uri): string {
  return path.join(getSchemaxDir(workspaceUri), PROJECT_FILENAME);
}

/**
 * Get the changelog file path
 */
function getChangelogFilePath(workspaceUri: vscode.Uri): string {
  return path.join(getSchemaxDir(workspaceUri), CHANGELOG_FILENAME);
}

/**
 * Get the .schemax directory path
 */
function getSchemaxDir(workspaceUri: vscode.Uri): string {
  return path.join(workspaceUri.fsPath, SCHEMAX_DIR);
}

/**
 * Get the snapshots directory path
 */
function getSnapshotsDir(workspaceUri: vscode.Uri): string {
  return path.join(getSchemaxDir(workspaceUri), SNAPSHOTS_DIR);
}

/**
 * Get snapshot file path
 */
function getSnapshotFilePath(workspaceUri: vscode.Uri, version: string): string {
  return path.join(getSnapshotsDir(workspaceUri), `${version}.json`);
}

/**
 * Ensure .schemax/snapshots/ directory exists
 */
async function ensureSchemaxDir(workspaceUri: vscode.Uri): Promise<void> {
  const schemaxDir = getSchemaxDir(workspaceUri);
  const snapshotsDir = getSnapshotsDir(workspaceUri);
  
  try {
    await fs.mkdir(schemaxDir, { recursive: true });
    await fs.mkdir(snapshotsDir, { recursive: true });
  } catch (error) {
    // Directory might already exist, that's ok
  }
}

/**
 * Initialize a new project with provider selection
 */
export async function ensureProjectFile(
  workspaceUri: vscode.Uri,
  providerId: string = 'unity'
): Promise<void> {
  const projectPath = getProjectFilePath(workspaceUri);
  const changelogPath = getChangelogFilePath(workspaceUri);
  
  try {
    await fs.access(projectPath);
    // Project exists - check if migration is needed
    const content = await fs.readFile(projectPath, 'utf8');
    const project = JSON.parse(content);
    
    if (project.version === 2) {
      // Migrate from v2 to v3
      await migrateV2ToV3(workspaceUri, project, providerId);
    }
  } catch {
    // Create new v3 project
    const workspaceName = path.basename(workspaceUri.fsPath);
    
    // Verify provider exists
    const provider = ProviderRegistry.get(providerId);
    if (!provider) {
      throw new Error(`Provider '${providerId}' not found. Available providers: ${ProviderRegistry.getAllIds().join(', ')}`);
    }
    
    const newProject: ProjectFileV3 = {
      version: 3,
      name: workspaceName,
      provider: {
        type: providerId,
        version: provider.info.version,
      },
      environments: ['dev', 'test', 'prod'],
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
        requireSnapshotForProd: true,
        allowDrift: false,
        requireComments: false,
        warnOnBreakingChanges: true,
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
    await fs.writeFile(projectPath, JSON.stringify(newProject, null, 2), 'utf8');
    await fs.writeFile(changelogPath, JSON.stringify(newChangelog, null, 2), 'utf8');
    
    console.log(`[SchemaX] Initialized new v3 project: ${workspaceName} with provider: ${provider.info.name}`);
  }
}

/**
 * Migrate v2 project to v3 format
 */
async function migrateV2ToV3(
  workspaceUri: vscode.Uri,
  v2Project: any,
  providerId: string = 'unity'
): Promise<void> {
  console.log(`[SchemaX] Migrating project from v2 to v3...`);
  
  const provider = ProviderRegistry.get(providerId);
  if (!provider) {
    throw new Error(`Provider '${providerId}' not found`);
  }
  
  const v3Project: ProjectFileV3 = {
    version: 3,
    name: v2Project.name,
    provider: {
      type: providerId,
      version: provider.info.version,
    },
    environments: v2Project.environments || ['dev', 'test', 'prod'],
    snapshots: v2Project.snapshots || [],
    deployments: v2Project.deployments || [],
    settings: v2Project.settings || {
      autoIncrementVersion: true,
      versionPrefix: 'v',
      requireSnapshotForProd: true,
      allowDrift: false,
      requireComments: false,
      warnOnBreakingChanges: true,
    },
    latestSnapshot: v2Project.latestSnapshot || null,
  };
  
  // Migrate operations in changelog to add provider prefix
  try {
    const changelog = await readChangelog(workspaceUri);
    const migratedOps = changelog.ops.map((op: Operation) => {
      // Add provider field if missing
      if (!op.provider) {
        op.provider = providerId;
      }
      // Add provider prefix to operation type if missing
      if (!op.op.includes('.')) {
        op.op = `${providerId}.${op.op}`;
      }
      return op;
    });
    
    await fs.writeFile(
      getChangelogFilePath(workspaceUri),
      JSON.stringify({ ...changelog, ops: migratedOps }, null, 2),
      'utf8'
    );
  } catch (error) {
    console.warn('[SchemaX] Could not migrate changelog operations:', error);
  }
  
  // Write migrated project file
  const projectPath = getProjectFilePath(workspaceUri);
  await fs.writeFile(projectPath, JSON.stringify(v3Project, null, 2), 'utf8');
  
  console.log(`[SchemaX] Migration complete: v2 → v3`);
}

/**
 * Read project file
 */
export async function readProject(workspaceUri: vscode.Uri): Promise<ProjectFileV3> {
  const filePath = getProjectFilePath(workspaceUri);
  const content = await fs.readFile(filePath, 'utf8');
  const parsed = JSON.parse(content);
  
  // Auto-migrate if needed
  if (parsed.version === 2) {
    await migrateV2ToV3(workspaceUri, parsed);
    return readProject(workspaceUri); // Re-read after migration
  }
  
  if (parsed.version !== 3) {
    throw new Error(`Unsupported project version: ${parsed.version}`);
  }
  
  return parsed as ProjectFileV3;
}

/**
 * Write project file
 */
async function writeProject(workspaceUri: vscode.Uri, project: ProjectFileV3): Promise<void> {
  const filePath = getProjectFilePath(workspaceUri);
  const content = JSON.stringify(project, null, 2);
  await fs.writeFile(filePath, content, 'utf8');
}

/**
 * Read changelog file
 */
export async function readChangelog(workspaceUri: vscode.Uri): Promise<ChangelogFile> {
  const filePath = getChangelogFilePath(workspaceUri);
  try {
    const content = await fs.readFile(filePath, 'utf8');
    const parsed = JSON.parse(content);
    return parsed as ChangelogFile;
  } catch {
    // Changelog doesn't exist, create empty one
    const changelog: ChangelogFile = {
      version: 1,
      sinceSnapshot: null,
      ops: [],
      lastModified: new Date().toISOString(),
    };
    await fs.writeFile(filePath, JSON.stringify(changelog, null, 2), 'utf8');
    return changelog;
  }
}

/**
 * Write changelog file
 */
async function writeChangelog(workspaceUri: vscode.Uri, changelog: ChangelogFile): Promise<void> {
  const filePath = getChangelogFilePath(workspaceUri);
  changelog.lastModified = new Date().toISOString();
  const content = JSON.stringify(changelog, null, 2);
  await fs.writeFile(filePath, content, 'utf8');
}

/**
 * Read a snapshot file
 */
export async function readSnapshot(workspaceUri: vscode.Uri, version: string): Promise<SnapshotFile> {
  const filePath = getSnapshotFilePath(workspaceUri, version);
  const content = await fs.readFile(filePath, 'utf8');
  const parsed = JSON.parse(content);
  return parsed as SnapshotFile;
}

/**
 * Write a snapshot file
 */
async function writeSnapshot(workspaceUri: vscode.Uri, snapshot: SnapshotFile): Promise<void> {
  await ensureSchemaxDir(workspaceUri);
  const filePath = getSnapshotFilePath(workspaceUri, snapshot.version);
  const content = JSON.stringify(snapshot, null, 2);
  await fs.writeFile(filePath, content, 'utf8');
}

/**
 * Load current state using provider
 */
export async function loadCurrentState(workspaceUri: vscode.Uri): Promise<{
  state: ProviderState;
  changelog: ChangelogFile;
  provider: Provider;
}> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);
  
  // Get provider
  const provider = ProviderRegistry.get(project.provider.type);
  if (!provider) {
    throw new Error(`Provider '${project.provider.type}' not found. Please ensure the provider is installed.`);
  }
  
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
export async function appendOps(workspaceUri: vscode.Uri, ops: Operation[]): Promise<void> {
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
): Promise<{ project: ProjectFileV3; snapshot: SnapshotFile }> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);
  
  // Load current state
  const { state } = await loadCurrentState(workspaceUri);
  
  // Determine version
  const snapshotVersion = version || getNextVersion(project.latestSnapshot, project.settings);
  
  // Generate IDs for ops that don't have them (backwards compatibility)
  const opsWithIds = changelog.ops.map((op, index) => {
    if (!op.id) {
      op.id = `op_${index}_${op.ts}_${op.target}`;
    }
    return op;
  });
  
  // Calculate hash
  const hash = calculateStateHash(state, opsWithIds.map(op => op.id!));
  
  // Create snapshot file
  const snapshotFile: SnapshotFile = {
    id: `snap_${uuidv4()}`,
    version: snapshotVersion,
    name,
    ts: new Date().toISOString(),
    createdBy: process.env.USER || process.env.USERNAME || 'unknown',
    state,
    opsIncluded: opsWithIds.map(op => op.id!),
    previousSnapshot: project.latestSnapshot,
    hash,
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
    hash,
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
  
  console.log(`[SchemaX] Created snapshot ${snapshotVersion}: ${name}`);
  console.log(`[SchemaX] Snapshot file: ${snapshotMetadata.file}`);
  console.log(`[SchemaX] Ops included: ${opsWithIds.length}`);
  
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
 * Calculate hash of state for integrity checking
 */
function calculateStateHash(state: any, opsIncluded: string[]): string {
  const content = JSON.stringify({ state, opsIncluded });
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
  const versionMatch = currentVersion.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!versionMatch) {
    return settings.versionPrefix + '0.1.0';
  }
  
  const [, major, minor, patch] = versionMatch;
  const nextMinor = parseInt(minor) + 1;
  
  return `${settings.versionPrefix}${major}.${nextMinor}.0`;
}

