import * as vscode from 'vscode';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import { ProjectFile, SnapshotMetadata, SnapshotFile, ChangelogFile, Catalog, Schema, Table, Column, Constraint, RowFilter, ColumnMask } from './shared/model';
import { Op } from './shared/ops';
import { v4 as uuidv4 } from 'uuid';

const SCHEMAX_DIR = '.schemax';
const PROJECT_FILENAME = 'project.json';
const CHANGELOG_FILENAME = 'changelog.json';
const SNAPSHOTS_DIR = 'snapshots';

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
 * Initialize a new project (creates schemax.project.json and schemax.changelog.json)
 */
export async function ensureProjectFile(workspaceUri: vscode.Uri): Promise<void> {
  const projectPath = getProjectFilePath(workspaceUri);
  const changelogPath = getChangelogFilePath(workspaceUri);
  
  try {
    await fs.access(projectPath);
    // Project exists
  } catch {
    // Create new project
    const workspaceName = path.basename(workspaceUri.fsPath);
    
    const newProject: ProjectFile = {
      version: 2,
      name: workspaceName,
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
    
    console.log(`[SchemaX] Initialized new v2 project: ${workspaceName}`);
  }
}

/**
 * Read project file
 */
export async function readProject(workspaceUri: vscode.Uri): Promise<ProjectFile> {
  const filePath = getProjectFilePath(workspaceUri);
  const content = await fs.readFile(filePath, 'utf8');
  const parsed = JSON.parse(content);
  return ProjectFile.parse(parsed);
}

/**
 * Write project file
 */
async function writeProject(workspaceUri: vscode.Uri, project: ProjectFile): Promise<void> {
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
    return ChangelogFile.parse(parsed);
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
  return SnapshotFile.parse(parsed);
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
 * Load current state (latest snapshot + changelog ops applied)
 */
export async function loadCurrentState(workspaceUri: vscode.Uri): Promise<{ state: ProjectFile['snapshots'][0]['file'] extends string ? any : never, changelog: ChangelogFile }> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);
  
  let state: { catalogs: Catalog[] };
  
  if (project.latestSnapshot) {
    // Load latest snapshot
    const snapshot = await readSnapshot(workspaceUri, project.latestSnapshot);
    state = snapshot.state;
  } else {
    // No snapshots yet, start with empty state
    state = { catalogs: [] };
  }
  
  // Apply changelog ops to state
  state = applyOpsToState(state, changelog.ops);
  
  return { state, changelog };
}

/**
 * Append operations to changelog
 */
export async function appendOps(workspaceUri: vscode.Uri, ops: Op[]): Promise<void> {
  const changelog = await readChangelog(workspaceUri);
  
  // Validate ops
  for (const op of ops) {
    Op.parse(op);
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
): Promise<{ project: ProjectFile, snapshot: SnapshotFile }> {
  const project = await readProject(workspaceUri);
  const changelog = await readChangelog(workspaceUri);
  
  // Load current state
  const { state } = await loadCurrentState(workspaceUri);
  
  // Determine version
  const snapshotVersion = version || getNextVersion(project.latestSnapshot, project.settings);
  
  // Generate IDs for ops that don't have them (backwards compatibility)
  const opsWithIds = changelog.ops.map((op: Op, index) => {
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
function getNextVersion(currentVersion: string | null, settings: ProjectFile['settings']): string {
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

/**
 * Apply operations to state (reducer)
 */
function applyOpsToState(state: { catalogs: Catalog[] }, ops: Op[]): { catalogs: Catalog[] } {
  const newState = JSON.parse(JSON.stringify(state)); // Deep clone
  
  for (const op of ops) {
    switch (op.op) {
      case 'add_catalog': {
        const catalog: Catalog = {
          id: op.payload.catalogId,
          name: op.payload.name,
          schemas: [],
        };
        newState.catalogs.push(catalog);
        break;
      }
      case 'rename_catalog': {
        const catalog = newState.catalogs.find((c: Catalog) => c.id === op.target);
        if (catalog) catalog.name = op.payload.newName;
        break;
      }
      case 'drop_catalog': {
        newState.catalogs = newState.catalogs.filter((c: Catalog) => c.id !== op.target);
        break;
      }
      case 'add_schema': {
        const catalog = newState.catalogs.find((c: Catalog) => c.id === op.payload.catalogId);
        if (catalog) {
          const schema: Schema = {
            id: op.payload.schemaId,
            name: op.payload.name,
            tables: [],
          };
          catalog.schemas.push(schema);
        }
        break;
      }
      case 'rename_schema': {
        for (const catalog of newState.catalogs) {
          const schema = catalog.schemas.find((s: Schema) => s.id === op.target);
          if (schema) {
            schema.name = op.payload.newName;
            break;
          }
        }
        break;
      }
      case 'drop_schema': {
        for (const catalog of newState.catalogs) {
          catalog.schemas = catalog.schemas.filter((s: Schema) => s.id !== op.target);
        }
        break;
      }
      case 'add_table': {
        for (const catalog of newState.catalogs) {
          const schema = catalog.schemas.find((s: Schema) => s.id === op.payload.schemaId);
          if (schema) {
            const table: Table = {
              id: op.payload.tableId,
              name: op.payload.name,
              format: op.payload.format,
              columns: [],
              properties: {},
              constraints: [],
              grants: [],
            };
            schema.tables.push(table);
            break;
          }
        }
        break;
      }
      case 'rename_table': {
        const table = findTable(newState, op.target);
        if (table) table.name = op.payload.newName;
        break;
      }
      case 'drop_table': {
        for (const catalog of newState.catalogs) {
          for (const schema of catalog.schemas) {
            schema.tables = schema.tables.filter((t: Table) => t.id !== op.target);
          }
        }
        break;
      }
      case 'set_table_comment': {
        const table = findTable(newState, op.payload.tableId);
        if (table) table.comment = op.payload.comment;
        break;
      }
      case 'add_column': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column: Column = {
            id: op.payload.colId,
            name: op.payload.name,
            type: op.payload.type,
            nullable: op.payload.nullable,
          };
          if (op.payload.comment) column.comment = op.payload.comment;
          table.columns.push(column);
        }
        break;
      }
      case 'rename_column': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column) column.name = op.payload.newName;
        }
        break;
      }
      case 'drop_column': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          table.columns = table.columns.filter((c: Column) => c.id !== op.target);
        }
        break;
      }
      case 'reorder_columns': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const order = op.payload.order;
          table.columns.sort((a: Column, b: Column) => {
            return order.indexOf(a.id) - order.indexOf(b.id);
          });
        }
        break;
      }
      case 'change_column_type': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column) column.type = op.payload.newType;
        }
        break;
      }
      case 'set_nullable': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column) column.nullable = op.payload.nullable;
        }
        break;
      }
      case 'set_column_comment': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column) column.comment = op.payload.comment;
        }
        break;
      }
      case 'set_table_property': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          table.properties[op.payload.key] = op.payload.value;
        }
        break;
      }
      case 'unset_table_property': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          delete table.properties[op.payload.key];
        }
        break;
      }
      
      // Column tag operations
      case 'set_column_tag': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column) {
            if (!column.tags) column.tags = {};
            column.tags[op.payload.tagName] = op.payload.tagValue;
          }
        }
        break;
      }
      case 'unset_column_tag': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === op.target);
          if (column && column.tags) {
            delete column.tags[op.payload.tagName];
          }
        }
        break;
      }
      
      // Constraint operations
      case 'add_constraint': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          const constraint: Constraint = {
            id: op.payload.constraintId,
            type: op.payload.type,
            name: op.payload.name,
            columns: op.payload.columns,
          };
          
          // Add type-specific fields
          if (op.payload.timeseries !== undefined) constraint.timeseries = op.payload.timeseries;
          if (op.payload.parentTable) constraint.parentTable = op.payload.parentTable;
          if (op.payload.parentColumns) constraint.parentColumns = op.payload.parentColumns;
          if (op.payload.matchFull !== undefined) constraint.matchFull = op.payload.matchFull;
          if (op.payload.onUpdate) constraint.onUpdate = op.payload.onUpdate;
          if (op.payload.onDelete) constraint.onDelete = op.payload.onDelete;
          if (op.payload.expression) constraint.expression = op.payload.expression;
          if (op.payload.notEnforced !== undefined) constraint.notEnforced = op.payload.notEnforced;
          if (op.payload.deferrable !== undefined) constraint.deferrable = op.payload.deferrable;
          if (op.payload.initiallyDeferred !== undefined) constraint.initiallyDeferred = op.payload.initiallyDeferred;
          if (op.payload.rely !== undefined) constraint.rely = op.payload.rely;
          
          table.constraints.push(constraint);
        }
        break;
      }
      case 'drop_constraint': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          table.constraints = table.constraints.filter((c: Constraint) => c.id !== op.target);
        }
        break;
      }
      
      // Row filter operations
      case 'add_row_filter': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          if (!table.rowFilters) table.rowFilters = [];
          const filter: RowFilter = {
            id: op.payload.filterId,
            name: op.payload.name,
            enabled: op.payload.enabled ?? true,
            udfExpression: op.payload.udfExpression,
            description: op.payload.description,
          };
          table.rowFilters.push(filter);
        }
        break;
      }
      case 'update_row_filter': {
        const table = findTable(newState, op.payload.tableId);
        if (table && table.rowFilters) {
          const filter = table.rowFilters.find((f: RowFilter) => f.id === op.target);
          if (filter) {
            if (op.payload.name !== undefined) filter.name = op.payload.name;
            if (op.payload.enabled !== undefined) filter.enabled = op.payload.enabled;
            if (op.payload.udfExpression !== undefined) filter.udfExpression = op.payload.udfExpression;
            if (op.payload.description !== undefined) filter.description = op.payload.description;
          }
        }
        break;
      }
      case 'remove_row_filter': {
        const table = findTable(newState, op.payload.tableId);
        if (table && table.rowFilters) {
          table.rowFilters = table.rowFilters.filter((f: RowFilter) => f.id !== op.target);
        }
        break;
      }
      
      // Column mask operations
      case 'add_column_mask': {
        const table = findTable(newState, op.payload.tableId);
        if (table) {
          if (!table.columnMasks) table.columnMasks = [];
          const mask: ColumnMask = {
            id: op.payload.maskId,
            columnId: op.payload.columnId,
            name: op.payload.name,
            enabled: op.payload.enabled ?? true,
            maskFunction: op.payload.maskFunction,
            description: op.payload.description,
          };
          table.columnMasks.push(mask);
          
          // Link mask to column
          const column = table.columns.find((c: Column) => c.id === op.payload.columnId);
          if (column) column.maskId = op.payload.maskId;
        }
        break;
      }
      case 'update_column_mask': {
        const table = findTable(newState, op.payload.tableId);
        if (table && table.columnMasks) {
          const mask = table.columnMasks.find((m: ColumnMask) => m.id === op.target);
          if (mask) {
            if (op.payload.name !== undefined) mask.name = op.payload.name;
            if (op.payload.enabled !== undefined) mask.enabled = op.payload.enabled;
            if (op.payload.maskFunction !== undefined) mask.maskFunction = op.payload.maskFunction;
            if (op.payload.description !== undefined) mask.description = op.payload.description;
          }
        }
        break;
      }
      case 'remove_column_mask': {
        const table = findTable(newState, op.payload.tableId);
        if (table && table.columnMasks) {
          const mask = table.columnMasks.find((m: ColumnMask) => m.id === op.target);
          if (mask) {
            // Unlink mask from column
            const column = table.columns.find((c: Column) => c.id === mask.columnId);
            if (column) column.maskId = undefined;
          }
          table.columnMasks = table.columnMasks.filter((m: ColumnMask) => m.id !== op.target);
        }
        break;
      }
    }
  }
  
  return newState;
}

/**
 * Find a table by ID
 */
function findTable(state: { catalogs: Catalog[] }, tableId: string): Table | undefined {
  for (const catalog of state.catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find((t: Table) => t.id === tableId);
      if (table) return table;
    }
  }
  return undefined;
}

