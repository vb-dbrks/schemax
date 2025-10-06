import * as vscode from 'vscode';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import { ProjectFile, Catalog, Schema, Table, Column, Snapshot } from './shared/model';
import { Op } from './shared/ops';
import { v4 as uuidv4 } from 'uuid';

const PROJECT_FILENAME = 'schemax.project.json';

/**
 * Get the project file path for a workspace
 */
function getProjectFilePath(workspaceUri: vscode.Uri): string {
  return path.join(workspaceUri.fsPath, PROJECT_FILENAME);
}

/**
 * Apply operations to state to keep it in sync
 */
function applyOpsToState(state: ProjectFile['state'], ops: Op[]): ProjectFile['state'] {
  const newState = JSON.parse(JSON.stringify(state)); // Deep clone

  for (const op of ops) {
    switch (op.op) {
      case 'add_catalog': {
        const { catalogId, name } = op.payload;
        newState.catalogs.push({
          id: catalogId,
          name,
          schemas: [],
        });
        break;
      }
      case 'rename_catalog': {
        const catalog = findCatalog(newState, op.target);
        if (catalog) {
          catalog.name = op.payload.newName;
        }
        break;
      }
      case 'drop_catalog': {
        newState.catalogs = newState.catalogs.filter((c: Catalog) => c.id !== op.target);
        break;
      }
      case 'add_schema': {
        const { schemaId, name, catalogId } = op.payload;
        const catalog = findCatalog(newState, catalogId);
        if (catalog) {
          catalog.schemas.push({
            id: schemaId,
            name,
            tables: [],
          });
        }
        break;
      }
      case 'rename_schema': {
        const schema = findSchema(newState, op.target);
        if (schema) {
          schema.name = op.payload.newName;
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
        const { tableId, name, schemaId, format, columnMapping } = op.payload;
        const schema = findSchema(newState, schemaId);
        if (schema) {
          schema.tables.push({
            id: tableId,
            name,
            format: format || 'delta',
            columnMapping,
            columns: [],
            properties: {},
            constraints: [],
            grants: [],
          });
        }
        break;
      }
      case 'rename_table': {
        const table = findTable(newState, op.target);
        if (table) {
          table.name = op.payload.newName;
        }
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
      case 'add_column': {
        const { tableId, colId, name, type, nullable, after } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          const newColumn: Column = {
            id: colId,
            name,
            type: type || 'STRING',
            nullable: nullable !== false,
          };
          if (after) {
            const afterIdx = table.columns.findIndex((c: Column) => c.id === after);
            if (afterIdx >= 0) {
              table.columns.splice(afterIdx + 1, 0, newColumn);
            } else {
              table.columns.push(newColumn);
            }
          } else {
            table.columns.push(newColumn);
          }
        }
        break;
      }
      case 'rename_column': {
        const { tableId, colId, newName } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === colId);
          if (column) {
            column.name = newName;
          }
        }
        break;
      }
      case 'reorder_columns': {
        const { tableId, order } = op.payload;
        const table = findTable(newState, tableId);
        if (table && Array.isArray(order)) {
          const reordered: Column[] = [];
          for (const colId of order) {
            const col = table.columns.find((c: Column) => c.id === colId);
            if (col) {
              reordered.push(col);
            }
          }
          table.columns = reordered;
        }
        break;
      }
      case 'drop_column': {
        const { tableId, colId } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          table.columns = table.columns.filter((c: Column) => c.id !== colId);
        }
        break;
      }
      case 'change_column_type': {
        const { tableId, colId, newType } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === colId);
          if (column) {
            column.type = newType;
          }
        }
        break;
      }
      case 'set_nullable': {
        const { tableId, colId, nullable } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === colId);
          if (column) {
            column.nullable = nullable;
          }
        }
        break;
      }
      case 'set_table_property': {
        const { key, value } = op.payload;
        const table = findTable(newState, op.target);
        if (table) {
          table.properties[key] = value;
        }
        break;
      }
      case 'unset_table_property': {
        const { key } = op.payload;
        const table = findTable(newState, op.target);
        if (table) {
          delete table.properties[key];
        }
        break;
      }
      case 'set_table_comment': {
        const table = findTable(newState, op.target);
        if (table) {
          table.comment = op.payload.comment;
        }
        break;
      }
      case 'set_column_comment': {
        const { tableId, colId, comment } = op.payload;
        const table = findTable(newState, tableId);
        if (table) {
          const column = table.columns.find((c: Column) => c.id === colId);
          if (column) {
            column.comment = comment;
          }
        }
        break;
      }
    }
  }

  return newState;
}

function findCatalog(state: ProjectFile['state'], catalogId: string): Catalog | undefined {
  return state.catalogs.find((c) => c.id === catalogId);
}

function findSchema(state: ProjectFile['state'], schemaId: string): Schema | undefined {
  for (const catalog of state.catalogs) {
    const schema = catalog.schemas.find((s) => s.id === schemaId);
    if (schema) {
      return schema;
    }
  }
  return undefined;
}

function findTable(state: ProjectFile['state'], tableId: string): Table | undefined {
  for (const catalog of state.catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find((t) => t.id === tableId);
      if (table) {
        return table;
      }
    }
  }
  return undefined;
}

/**
 * Ensure project file exists; create if missing
 */
export async function ensureProjectFile(workspaceUri: vscode.Uri): Promise<ProjectFile> {
  const filePath = getProjectFilePath(workspaceUri);
  try {
    await fs.access(filePath);
    return await readProject(workspaceUri);
  } catch {
    // File doesn't exist, create it
    const workspaceName = path.basename(workspaceUri.fsPath);
    const newProject: ProjectFile = {
      version: 1,
      name: workspaceName,
      environments: ['dev', 'test', 'prod'],
      state: {
        catalogs: [],
      },
      ops: [],
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
      lastSnapshotHash: null,
    };
    await writeProject(workspaceUri, newProject);
    return newProject;
  }
}

/**
 * Read project file
 */
export async function readProject(workspaceUri: vscode.Uri): Promise<ProjectFile> {
  const filePath = getProjectFilePath(workspaceUri);
  const content = await fs.readFile(filePath, 'utf8');
  const parsed = JSON.parse(content);
  
  // Parse with Zod to apply defaults
  const project = ProjectFile.parse(parsed);
  
  // Ensure new fields exist (for backwards compatibility with old files)
  if (!project.snapshots) {
    project.snapshots = [];
  }
  if (!project.deployments) {
    project.deployments = [];
  }
  if (!project.settings) {
    project.settings = {
      autoIncrementVersion: true,
      versionPrefix: 'v',
      requireSnapshotForProd: true,
      allowDrift: false,
      requireComments: false,
      warnOnBreakingChanges: true,
    };
  }
  
  return project;
}

/**
 * Write project file with stable key order and pretty printing
 */
export async function writeProject(workspaceUri: vscode.Uri, project: ProjectFile): Promise<void> {
  const filePath = getProjectFilePath(workspaceUri);
  
  // Sort keys for stable output
  const sorted = {
    version: project.version,
    name: project.name,
    environments: project.environments,
    state: project.state,
    ops: project.ops,
    lastSnapshotHash: project.lastSnapshotHash,
  };
  
  const content = JSON.stringify(sorted, null, 2);
  await fs.writeFile(filePath, content, 'utf8');
}

/**
 * Append operations to the project file
 */
export async function appendOps(workspaceUri: vscode.Uri, ops: Op[]): Promise<ProjectFile> {
  const project = await readProject(workspaceUri);
  
  // Validate ops
  for (const op of ops) {
    Op.parse(op);
  }
  
  // Append ops
  project.ops.push(...ops);
  
  // Apply ops to state to keep in sync
  project.state = applyOpsToState(project.state, ops);
  
  // Write back
  await writeProject(workspaceUri, project);
  
  return project;
}

/**
 * Calculate hash of state for integrity checking
 */
function calculateStateHash(state: ProjectFile['state'], ops: string[]): string {
  const content = JSON.stringify({ state, ops });
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
 * Create a snapshot of the current state
 */
export async function createSnapshot(
  workspaceUri: vscode.Uri,
  name: string,
  version?: string,
  comment?: string,
  tags: string[] = []
): Promise<ProjectFile> {
  const project = await readProject(workspaceUri);
  
  // Determine version
  const snapshotVersion = version || getNextVersion(
    project.snapshots.length > 0 ? project.snapshots[project.snapshots.length - 1].version : null,
    project.settings
  );
  
  // Get all ops since last snapshot
  const lastSnapshot = project.snapshots[project.snapshots.length - 1];
  const opsIncluded = lastSnapshot
    ? project.ops.filter(op => !lastSnapshot.opsIncluded.includes(op.id)).map(op => op.id)
    : project.ops.map(op => op.id);
  
  // Calculate hash
  const hash = calculateStateHash(project.state, opsIncluded);
  
  // Create snapshot
  const snapshot: Snapshot = {
    id: `snap_${uuidv4()}`,
    version: snapshotVersion,
    name,
    ts: new Date().toISOString(),
    createdBy: process.env.USER || process.env.USERNAME || 'unknown',
    state: JSON.parse(JSON.stringify(project.state)), // Deep clone
    opsIncluded,
    previousSnapshot: lastSnapshot ? lastSnapshot.id : null,
    hash,
    tags,
    comment,
  };
  
  // Add to project
  project.snapshots.push(snapshot);
  project.lastSnapshotHash = hash;
  
  // Write back
  await writeProject(workspaceUri, project);
  
  console.log(`[SchemaX] Created snapshot ${snapshotVersion}: ${name}`);
  
  return project;
}

/**
 * Get uncommitted operations (ops since last snapshot)
 */
export function getUncommittedOps(project: ProjectFile): Op[] {
  const lastSnapshot = project.snapshots[project.snapshots.length - 1];
  if (!lastSnapshot) {
    return project.ops;
  }
  
  return project.ops.filter(op => !lastSnapshot.opsIncluded.includes(op.id));
}

