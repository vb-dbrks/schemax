/**
 * Unity Catalog State Reducer
 * 
 * Applies operations to Unity Catalog state immutably.
 * Migrated from storage-v2.ts applyOpsToState function.
 */

import { Operation } from '../base/operations';
import {
  UnityState,
  UnityCatalog,
  UnitySchema,
  UnityTable,
  UnityColumn,
  UnityConstraint,
  UnityRowFilter,
  UnityColumnMask,
  UnityGrant,
  UnityView,
} from './models';
import { UNITY_OPERATIONS } from './operations';

/**
 * Apply a single operation to Unity Catalog state
 */
export function applyOperation(state: UnityState, op: Operation): UnityState {
  // Deep clone state for immutability
  const newState: UnityState = JSON.parse(JSON.stringify(state));
  
  // Strip provider prefix from operation type for switch statement
  const opType = op.op.replace('unity.', '');
  
  switch (opType) {
    // Catalog operations
    case 'add_catalog': {
      const catalog: UnityCatalog = {
        id: op.payload.catalogId,
        name: op.payload.name,
        managedLocationName: op.payload.managedLocationName,
        comment: op.payload.comment,
        tags: op.payload.tags || {},
        schemas: [],
        grants: [],
      };
      newState.catalogs.push(catalog);
      break;
    }
    case 'rename_catalog': {
      const catalog = newState.catalogs.find(c => c.id === op.target);
      if (catalog) catalog.name = op.payload.newName;
      break;
    }
    case 'update_catalog': {
      const catalog = newState.catalogs.find(c => c.id === op.target);
      if (catalog) {
        if ('managedLocationName' in op.payload) {
          catalog.managedLocationName = op.payload.managedLocationName == null
            ? undefined
            : op.payload.managedLocationName;
        }
        if ('comment' in op.payload) {
          catalog.comment = op.payload.comment;
        }
        if ('tags' in op.payload) {
          catalog.tags = op.payload.tags || {};
        }
      }
      break;
    }
    case 'drop_catalog': {
      newState.catalogs = newState.catalogs.filter(c => c.id !== op.target);
      break;
    }
    
    // Schema operations
    case 'add_schema': {
      const catalog = newState.catalogs.find(c => c.id === op.payload.catalogId);
      if (catalog) {
        const schema: UnitySchema = {
          id: op.payload.schemaId,
          name: op.payload.name,
          managedLocationName: op.payload.managedLocationName,
          comment: op.payload.comment,
          tags: op.payload.tags || {},
          tables: [],
          views: [],
          grants: [],
        };
        catalog.schemas.push(schema);
      }
      break;
    }
    case 'rename_schema': {
      for (const catalog of newState.catalogs) {
        const schema = catalog.schemas.find(s => s.id === op.target);
        if (schema) {
          schema.name = op.payload.newName;
          break;
        }
      }
      break;
    }
    case 'update_schema': {
      for (const catalog of newState.catalogs) {
        const schema = catalog.schemas.find(s => s.id === op.target);
        if (schema) {
          if ('managedLocationName' in op.payload) {
            schema.managedLocationName = op.payload.managedLocationName == null
              ? undefined
              : op.payload.managedLocationName;
          }
          if ('comment' in op.payload) {
            schema.comment = op.payload.comment;
          }
          if ('tags' in op.payload) {
            schema.tags = op.payload.tags || {};
          }
          break;
        }
      }
      break;
    }
    case 'drop_schema': {
      for (const catalog of newState.catalogs) {
        catalog.schemas = catalog.schemas.filter(s => s.id !== op.target);
      }
      break;
    }
    
    // Table operations
    case 'add_table': {
      for (const catalog of newState.catalogs) {
        const schema = catalog.schemas.find(s => s.id === op.payload.schemaId);
        if (schema) {
          const table: UnityTable = {
            id: op.payload.tableId,
            name: op.payload.name,
            format: op.payload.format,
            external: op.payload.external,
            externalLocationName: op.payload.externalLocationName,
            path: op.payload.path,
            partitionColumns: op.payload.partitionColumns,
            clusterColumns: op.payload.clusterColumns,
            comment: op.payload.comment,
            columns: [],
            properties: {},
            tags: {},
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
          schema.tables = schema.tables.filter(t => t.id !== op.target);
        }
      }
      break;
    }
    case 'set_table_comment': {
      const table = findTable(newState, op.payload.tableId);
      if (table) table.comment = op.payload.comment;
      break;
    }

    // View operations
    case 'add_view': {
      for (const catalog of newState.catalogs) {
        const schema = catalog.schemas.find(s => s.id === op.payload.schemaId);
        if (schema) {
          const view: UnityView = {
            id: op.payload.viewId,
            name: op.payload.name,
            definition: op.payload.definition,
            comment: op.payload.comment,
            dependencies: op.payload.dependencies,
            extractedDependencies: op.payload.extractedDependencies,
            tags: {},
            properties: {},
            grants: [],
          };
          if (!schema.views) {
            schema.views = [];
          }
          schema.views.push(view);
          break;
        }
      }
      break;
    }
    case 'rename_view': {
      const view = findView(newState, op.target);
      if (view) view.name = op.payload.newName;
      break;
    }
    case 'drop_view': {
      for (const catalog of newState.catalogs) {
        for (const schema of catalog.schemas) {
          if (schema.views) {
            schema.views = schema.views.filter(v => v.id !== op.target);
          }
        }
      }
      break;
    }
    case 'update_view': {
      const view = findView(newState, op.target);
      if (view) {
        if ('definition' in op.payload) {
          view.definition = op.payload.definition;
        }
        if ('dependencies' in op.payload) {
          view.dependencies = op.payload.dependencies;
        }
        if ('extractedDependencies' in op.payload) {
          view.extractedDependencies = op.payload.extractedDependencies;
        }
      }
      break;
    }
    case 'set_view_comment': {
      const view = findView(newState, op.payload.viewId);
      if (view) view.comment = op.payload.comment;
      break;
    }
    case 'set_view_property': {
      const view = findView(newState, op.payload.viewId);
      if (view) view.properties[op.payload.key] = op.payload.value;
      break;
    }
    case 'unset_view_property': {
      const view = findView(newState, op.payload.viewId);
      if (view && op.payload.key in view.properties) {
        delete view.properties[op.payload.key];
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
    case 'set_table_tag': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        table.tags[op.payload.tagName] = op.payload.tagValue;
      }
      break;
    }
    case 'unset_table_tag': {
      const table = findTable(newState, op.payload.tableId);
      if (table && table.tags) {
        delete table.tags[op.payload.tagName];
      }
      break;
    }
    
    // Column operations
    case 'add_column': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const column: UnityColumn = {
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
        const column = table.columns.find(c => c.id === op.target);
        if (column) column.name = op.payload.newName;
      }
      break;
    }
    case 'drop_column': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        table.columns = table.columns.filter(c => c.id !== op.target);
      }
      break;
    }
    case 'reorder_columns': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const order = op.payload.order;
        table.columns.sort((a, b) => {
          return order.indexOf(a.id) - order.indexOf(b.id);
        });
      }
      break;
    }
    case 'change_column_type': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const column = table.columns.find(c => c.id === op.target);
        if (column) column.type = op.payload.newType;
      }
      break;
    }
    case 'set_nullable': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const column = table.columns.find(c => c.id === op.target);
        if (column) column.nullable = op.payload.nullable;
      }
      break;
    }
    case 'set_column_comment': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const column = table.columns.find(c => c.id === op.target);
        if (column) column.comment = op.payload.comment;
      }
      break;
    }
    
    // Column tag operations
    case 'set_column_tag': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        const column = table.columns.find(c => c.id === op.target);
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
        const column = table.columns.find(c => c.id === op.target);
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
        const constraint: UnityConstraint = {
          id: op.payload.constraintId,
          type: op.payload.type,
          name: op.payload.name,
          columns: op.payload.columns,
        };
        
        // Add type-specific fields
        if (op.payload.timeseries !== undefined) constraint.timeseries = op.payload.timeseries;
        if (op.payload.parentTable) constraint.parentTable = op.payload.parentTable;
        if (op.payload.parentColumns) constraint.parentColumns = op.payload.parentColumns;
        if (op.payload.expression) constraint.expression = op.payload.expression;
        
        // Insert at specific position if provided (for updates), otherwise append
        if (op.payload.insertAt !== undefined && op.payload.insertAt >= 0) {
          table.constraints.splice(op.payload.insertAt, 0, constraint);
        } else {
          table.constraints.push(constraint);
        }
      }
      break;
    }
    case 'drop_constraint': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        table.constraints = table.constraints.filter(c => c.id !== op.target);
      }
      break;
    }
    
    // Row filter operations
    case 'add_row_filter': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        if (!table.rowFilters) table.rowFilters = [];
        const filter: UnityRowFilter = {
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
        const filter = table.rowFilters.find(f => f.id === op.target);
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
        table.rowFilters = table.rowFilters.filter(f => f.id !== op.target);
      }
      break;
    }
    
    // Column mask operations
    case 'add_column_mask': {
      const table = findTable(newState, op.payload.tableId);
      if (table) {
        if (!table.columnMasks) table.columnMasks = [];
        const mask: UnityColumnMask = {
          id: op.payload.maskId,
          columnId: op.payload.columnId,
          name: op.payload.name,
          enabled: op.payload.enabled ?? true,
          maskFunction: op.payload.maskFunction,
          description: op.payload.description,
        };
        table.columnMasks.push(mask);
        
        // Link mask to column
        const column = table.columns.find(c => c.id === op.payload.columnId);
        if (column) column.maskId = op.payload.maskId;
      }
      break;
    }
    case 'update_column_mask': {
      const table = findTable(newState, op.payload.tableId);
      if (table && table.columnMasks) {
        const mask = table.columnMasks.find(m => m.id === op.target);
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
        const mask = table.columnMasks.find(m => m.id === op.target);
        if (mask) {
          // Unlink mask from column
          const column = table.columns.find(c => c.id === mask.columnId);
          if (column) column.maskId = undefined;
        }
        table.columnMasks = table.columnMasks.filter(m => m.id !== op.target);
      }
      break;
    }

    // Grant operations
    case 'add_grant': {
      const { targetType, targetId, principal, privileges = [] } = op.payload;
      if (!targetType || !targetId || principal == null) break;
      const obj = findGrantTarget(newState, targetType, targetId);
      if (obj && obj.grants) {
        const grant: UnityGrant = { principal, privileges: [...privileges] };
        const existing = obj.grants.filter(g => g.principal !== principal);
        existing.push(grant);
        obj.grants = existing;
      }
      break;
    }
    case 'revoke_grant': {
      const { targetType, targetId, principal, privileges: privilegesToRemove } = op.payload;
      if (!targetType || !targetId || principal == null) break;
      const obj = findGrantTarget(newState, targetType, targetId);
      if (obj && obj.grants) {
        if (privilegesToRemove == null || privilegesToRemove.length === 0) {
          obj.grants = obj.grants.filter(g => g.principal !== principal);
        } else {
          const setRemove = new Set(privilegesToRemove);
          obj.grants = obj.grants.flatMap(g => {
            if (g.principal !== principal) return [g];
            const remaining = g.privileges.filter(p => !setRemove.has(p));
            return remaining.length ? [{ principal: g.principal, privileges: remaining }] : [];
          });
        }
      }
      break;
    }
  }

  return newState;
}

/**
 * Apply multiple operations to state
 */
export function applyOperations(state: UnityState, ops: Operation[]): UnityState {
  let currentState = state;
  for (const op of ops) {
    currentState = applyOperation(currentState, op);
  }
  return currentState;
}

/**
 * Find a table by ID across all catalogs and schemas
 */
function findTable(state: UnityState, tableId: string): UnityTable | undefined {
  for (const catalog of state.catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find(t => t.id === tableId);
      if (table) return table;
    }
  }
  return undefined;
}

/**
 * Find a view by ID across all catalogs and schemas
 */
function findView(state: UnityState, viewId: string): UnityView | undefined {
  for (const catalog of state.catalogs) {
    for (const schema of catalog.schemas) {
      if (schema.views) {
        const view = schema.views.find(v => v.id === viewId);
        if (view) return view;
      }
    }
  }
  return undefined;
}

type GrantTarget = UnityCatalog | UnitySchema | UnityTable | UnityView;

/**
 * Find a securable object by type and ID for grant operations
 */
function findGrantTarget(
  state: UnityState,
  targetType: string,
  targetId: string
): GrantTarget | undefined {
  if (targetType === 'catalog') {
    return state.catalogs.find(c => c.id === targetId);
  }
  if (targetType === 'schema') {
    for (const catalog of state.catalogs) {
      const schema = catalog.schemas.find(s => s.id === targetId);
      if (schema) return schema;
    }
    return undefined;
  }
  if (targetType === 'table') return findTable(state, targetId);
  if (targetType === 'view') return findView(state, targetId);
  return undefined;
}

