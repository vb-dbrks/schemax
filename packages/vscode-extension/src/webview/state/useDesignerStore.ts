import { create } from 'zustand';
import { v4 as uuidv4 } from 'uuid';
import { ProjectFile, Catalog, Schema, Table, Column, Constraint, RowFilter, ColumnMask } from '../../providers/unity/models';
import { Operation } from '../../providers/base/operations';
import { ProviderInfo, ProviderCapabilities } from '../../providers/base/provider';
import { getVsCodeApi } from '../vscode-api';

const vscode = getVsCodeApi();

// Provider info sent from extension
interface ProviderMetadata {
  id: string;
  name: string;
  version: string;
  capabilities: ProviderCapabilities;
}

interface DesignerState {
  project: ProjectFile | null;
  provider: ProviderMetadata | null; // NEW: Provider information
  selectedCatalogId: string | null;
  selectedSchemaId: string | null;
  selectedTableId: string | null;
  
  // Actions
  setProject: (project: ProjectFile) => void;
  setProvider: (provider: ProviderMetadata) => void;
  selectCatalog: (catalogId: string | null) => void;
  selectSchema: (schemaId: string | null) => void;
  selectTable: (tableId: string | null) => void;
  
  // Mutations (all emit ops)
  addCatalog: (name: string, options?: { managedLocationName?: string; comment?: string }) => void;
  renameCatalog: (catalogId: string, newName: string) => void;
  updateCatalog: (catalogId: string, updates: { managedLocationName?: string }) => void;
  dropCatalog: (catalogId: string) => void;
  
  addSchema: (catalogId: string, name: string, options?: { managedLocationName?: string; comment?: string }) => void;
  renameSchema: (schemaId: string, newName: string) => void;
  updateSchema: (schemaId: string, updates: { managedLocationName?: string }) => void;
  dropSchema: (schemaId: string) => void;
  
  addTable: (
    schemaId: string, 
    name: string, 
    format: 'delta' | 'iceberg',
    options?: {
      external?: boolean;
      externalLocationName?: string;
      path?: string;
      partitionColumns?: string[];
      clusterColumns?: string[];
      comment?: string;
    }
  ) => void;
  renameTable: (tableId: string, newName: string) => void;
  dropTable: (tableId: string) => void;
  setTableComment: (tableId: string, comment: string) => void;
  
  addColumn: (tableId: string, name: string, type: string, nullable: boolean, after?: string) => void;
  renameColumn: (tableId: string, colId: string, newName: string) => void;
  dropColumn: (tableId: string, colId: string) => void;
  reorderColumns: (tableId: string, order: string[]) => void;
  changeColumnType: (tableId: string, colId: string, newType: string) => void;
  setColumnNullable: (tableId: string, colId: string, nullable: boolean) => void;
  setColumnComment: (tableId: string, colId: string, comment: string) => void;
  
  setTableProperty: (tableId: string, key: string, value: string) => void;
  unsetTableProperty: (tableId: string, key: string) => void;
  
  // Column tag operations (NEW)
  setColumnTag: (tableId: string, colId: string, tagName: string, tagValue: string) => void;
  unsetColumnTag: (tableId: string, colId: string, tagName: string) => void;
  
  // Constraint operations (NEW)
  addConstraint: (tableId: string, constraint: Omit<Constraint, 'id'>) => void;
  dropConstraint: (tableId: string, constraintId: string) => void;
  
  // Row filter operations (NEW)
  addRowFilter: (tableId: string, name: string, udfExpression: string, enabled?: boolean, description?: string) => void;
  updateRowFilter: (tableId: string, filterId: string, updates: Partial<Omit<RowFilter, 'id'>>) => void;
  removeRowFilter: (tableId: string, filterId: string) => void;
  
  // Column mask operations (NEW)
  addColumnMask: (tableId: string, columnId: string, name: string, maskFunction: string, enabled?: boolean, description?: string) => void;
  updateColumnMask: (tableId: string, maskId: string, updates: Partial<Omit<ColumnMask, 'id' | 'columnId'>>) => void;
  removeColumnMask: (tableId: string, maskId: string) => void;
  
  // Helper to find objects
  findCatalog: (catalogId: string) => Catalog | undefined;
  findSchema: (schemaId: string) => { catalog: Catalog; schema: Schema } | undefined;
  findTable: (tableId: string) => { catalog: Catalog; schema: Schema; table: Table } | undefined;
}

function emitOps(ops: Operation[]) {
  vscode.postMessage({ type: 'append-ops', payload: ops });
}

// Helper to create an operation with provider context
function createOperation(
  store: DesignerState,
  opType: string,
  target: string,
  payload: Record<string, any>
): Operation {
  const provider = store.provider;
  if (!provider) {
    throw new Error('Provider not initialized. Cannot create operations.');
  }

  return {
    id: `op_${uuidv4()}`,
    ts: new Date().toISOString(),
    provider: provider.id,
    op: `${provider.id}.${opType}`, // Prefix with provider ID
    target,
    payload,
  };
}

export const useDesignerStore = create<DesignerState>((set, get) => ({
  project: null,
  provider: null, // Initialize as null
  selectedCatalogId: null,
  selectedSchemaId: null,
  selectedTableId: null,

  setProject: (project) => set({ project }),
  setProvider: (provider) => set({ provider }),
  selectCatalog: (catalogId) => set({ selectedCatalogId: catalogId }),
  selectSchema: (schemaId) => set({ selectedSchemaId: schemaId }),
  selectTable: (tableId) => set({ selectedTableId: tableId }),

  addCatalog: (name, options) => {
    const state = get();
    const existingCatalogs = state.project?.state?.catalogs || [];
    
    // Block multi-catalog projects (MVP limitation)
    if (existingCatalogs.length >= 1) {
      vscode.postMessage({
        type: 'show-error',
        payload: {
          message: 'Multi-Catalog Support Coming Soon',
          detail: 'Currently, only single-catalog projects are supported. Multi-catalog support with per-catalog environment mappings is planned for a future release.'
        }
      });
      return;
    }
    
    const catalogId = `cat_${uuidv4()}`;
    const op = createOperation(get(), 'add_catalog', catalogId, { 
      catalogId, 
      name,
      ...options
    });
    emitOps([op]);
  },

  renameCatalog: (catalogId, newName) => {
    const state = get();
    const catalog = state.findCatalog(catalogId);
    if (!catalog) {
      throw new Error(`Cannot rename catalog: catalog ${catalogId} not found`);
    }
    const oldName = catalog.name;
    const op = createOperation(state, 'rename_catalog', catalogId, { oldName, newName });
    emitOps([op]);
  },

  updateCatalog: (catalogId, updates) => {
    const state = get();
    const catalog = state.findCatalog(catalogId);
    if (!catalog) {
      throw new Error(`Cannot update catalog: catalog ${catalogId} not found`);
    }
    const op = createOperation(state, 'update_catalog', catalogId, updates);
    emitOps([op]);
  },

  dropCatalog: (catalogId) => {
    const op = createOperation(get(), 'drop_catalog', catalogId, {});
    emitOps([op]);
  },

  addSchema: (catalogId, name, options) => {
    const schemaId = `sch_${uuidv4()}`;
    const op = createOperation(get(), 'add_schema', schemaId, { 
      schemaId, 
      name, 
      catalogId,
      ...options
    });
    emitOps([op]);
  },

  renameSchema: (schemaId, newName) => {
    const state = get();
    const schemaInfo = state.findSchema(schemaId);
    if (!schemaInfo) {
      throw new Error(`Cannot rename schema: schema ${schemaId} not found`);
    }
    const oldName = schemaInfo.schema.name;
    const op = createOperation(state, 'rename_schema', schemaId, { oldName, newName });
    emitOps([op]);
  },

  updateSchema: (schemaId, updates) => {
    const state = get();
    const schemaInfo = state.findSchema(schemaId);
    if (!schemaInfo) {
      throw new Error(`Cannot update schema: schema ${schemaId} not found`);
    }
    const op = createOperation(state, 'update_schema', schemaId, updates);
    emitOps([op]);
  },

  dropSchema: (schemaId) => {
    const op = createOperation(get(), 'drop_schema', schemaId, {});
    emitOps([op]);
  },

  addTable: (schemaId, name, format, options) => {
    const tableId = `tbl_${uuidv4()}`;
    const op = createOperation(get(), 'add_table', tableId, { 
      tableId, 
      name, 
      schemaId, 
      format,
      ...options
    });
    emitOps([op]);
  },

  renameTable: (tableId, newName) => {
    const state = get();
    const tableInfo = state.findTable(tableId);
    if (!tableInfo) {
      throw new Error(`Cannot rename table: table ${tableId} not found`);
    }
    const oldName = tableInfo.table.name;
    const op = createOperation(state, 'rename_table', tableId, { oldName, newName });
    emitOps([op]);
  },

  dropTable: (tableId) => {
    const op = createOperation(get(), 'drop_table', tableId, {});
    emitOps([op]);
  },

  setTableComment: (tableId, comment) => {
    const op = createOperation(get(), 'set_table_comment', tableId, { tableId, comment });
    emitOps([op]);
  },

  addColumn: (tableId, name, type, nullable, after) => {
    const colId = `col_${uuidv4()}`;
    const op = createOperation(get(), 'add_column', colId, { tableId, colId, name, type, nullable, after });
    emitOps([op]);
  },

  renameColumn: (tableId, colId, newName) => {
    const state = get();
    const tableInfo = state.findTable(tableId);
    if (!tableInfo) {
      throw new Error(`Cannot rename column: table ${tableId} not found`);
    }
    const column = tableInfo.table.columns.find(c => c.id === colId);
    if (!column) {
      throw new Error(`Cannot rename column: column ${colId} not found in table ${tableId}`);
    }
    const oldName = column.name;
    const op = createOperation(state, 'rename_column', colId, { tableId, oldName, newName });
    emitOps([op]);
  },

  dropColumn: (tableId, colId) => {
    const op = createOperation(get(), 'drop_column', colId, { tableId });
    emitOps([op]);
  },

  reorderColumns: (tableId, order) => {
    const state = get();
    
    // Find the current table to capture previous column order
    let previousOrder: string[] = [];
    if (state.project?.state?.catalogs) {
      for (const catalog of state.project.state.catalogs) {
        for (const schema of catalog.schemas || []) {
          for (const table of schema.tables || []) {
            if (table.id === tableId) {
              previousOrder = table.columns.map(col => col.id);
              break;
            }
          }
        }
      }
    }
    
    const op = createOperation(state, 'reorder_columns', tableId, { 
      tableId, 
      order,
      previousOrder // Capture the previous order for ALTER TABLE generation
    });
    emitOps([op]);
  },

  changeColumnType: (tableId, colId, newType) => {
    const op = createOperation(get(), 'change_column_type', colId, { tableId, newType });
    emitOps([op]);
  },

  setColumnNullable: (tableId, colId, nullable) => {
    const op = createOperation(get(), 'set_nullable', colId, { tableId, nullable });
    emitOps([op]);
  },

  setColumnComment: (tableId, colId, comment) => {
    const op = createOperation(get(), 'set_column_comment', colId, { tableId, comment });
    emitOps([op]);
  },

  setTableProperty: (tableId, key, value) => {
    const op = createOperation(get(), 'set_table_property', tableId, { tableId, key, value });
    emitOps([op]);
  },

  unsetTableProperty: (tableId, key) => {
    const op = createOperation(get(), 'unset_table_property', tableId, { tableId, key });
    emitOps([op]);
  },

  // Column tag operations
  setColumnTag: (tableId, colId, tagName, tagValue) => {
    const op = createOperation(get(), 'set_column_tag', colId, { tableId, tagName, tagValue });
    emitOps([op]);
  },

  unsetColumnTag: (tableId, colId, tagName) => {
    const op = createOperation(get(), 'unset_column_tag', colId, { tableId, tagName });
    emitOps([op]);
  },

  // Constraint operations
  addConstraint: (tableId, constraint) => {
    const constraintId = `const_${uuidv4()}`;
    const op = createOperation(get(), 'add_constraint', tableId, { 
      tableId, 
      constraintId,
      ...constraint 
    });
    emitOps([op]);
  },

  dropConstraint: (tableId, constraintId) => {
    const op = createOperation(get(), 'drop_constraint', constraintId, { tableId });
    emitOps([op]);
  },

  // Row filter operations
  addRowFilter: (tableId, name, udfExpression, enabled = true, description) => {
    const filterId = `filter_${uuidv4()}`;
    const op = createOperation(get(), 'add_row_filter', filterId, { 
      tableId, filterId, name, udfExpression, enabled, description 
    });
    emitOps([op]);
  },

  updateRowFilter: (tableId, filterId, updates) => {
    const op = createOperation(get(), 'update_row_filter', filterId, { tableId, ...updates });
    emitOps([op]);
  },

  removeRowFilter: (tableId, filterId) => {
    const op = createOperation(get(), 'remove_row_filter', filterId, { tableId });
    emitOps([op]);
  },

  // Column mask operations
  addColumnMask: (tableId, columnId, name, maskFunction, enabled = true, description) => {
    const maskId = `mask_${uuidv4()}`;
    const op = createOperation(get(), 'add_column_mask', maskId, { 
      tableId, maskId, columnId, name, maskFunction, enabled, description 
    });
    emitOps([op]);
  },

  updateColumnMask: (tableId, maskId, updates) => {
    const op = createOperation(get(), 'update_column_mask', maskId, { tableId, ...updates });
    emitOps([op]);
  },

  removeColumnMask: (tableId, maskId) => {
    const op = createOperation(get(), 'remove_column_mask', maskId, { tableId });
    emitOps([op]);
  },

  findCatalog: (catalogId) => {
    const { project } = get();
    return (project as any)?.state.catalogs.find((c: Catalog) => c.id === catalogId);
  },

  findSchema: (schemaId) => {
    const { project } = get();
    if (!project) return undefined;
    for (const catalog of (project as any).state.catalogs) {
      const schema = catalog.schemas.find((s: Schema) => s.id === schemaId);
      if (schema) return { catalog, schema };
    }
    return undefined;
  },

  findTable: (tableId) => {
    const { project } = get();
    if (!project) return undefined;
    for (const catalog of (project as any).state.catalogs) {
      for (const schema of catalog.schemas) {
        const table = schema.tables.find((t: Table) => t.id === tableId);
        if (table) return { catalog, schema, table };
      }
    }
    return undefined;
  },
}));

