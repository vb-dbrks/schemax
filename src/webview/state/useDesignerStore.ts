import { create } from 'zustand';
import { v4 as uuidv4 } from 'uuid';
import { ProjectFile, Catalog, Schema, Table, Column } from '../../shared/model';
import { Op } from '../../shared/ops';

declare const acquireVsCodeApi: () => {
  postMessage: (message: any) => void;
};

const vscode = acquireVsCodeApi();

interface DesignerState {
  project: ProjectFile | null;
  selectedCatalogId: string | null;
  selectedSchemaId: string | null;
  selectedTableId: string | null;
  
  // Actions
  setProject: (project: ProjectFile) => void;
  selectCatalog: (catalogId: string | null) => void;
  selectSchema: (schemaId: string | null) => void;
  selectTable: (tableId: string | null) => void;
  
  // Mutations (all emit ops)
  addCatalog: (name: string) => void;
  renameCatalog: (catalogId: string, newName: string) => void;
  dropCatalog: (catalogId: string) => void;
  
  addSchema: (catalogId: string, name: string) => void;
  renameSchema: (schemaId: string, newName: string) => void;
  dropSchema: (schemaId: string) => void;
  
  addTable: (schemaId: string, name: string, format: 'delta' | 'iceberg') => void;
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
  
  // Helper to find objects
  findCatalog: (catalogId: string) => Catalog | undefined;
  findSchema: (schemaId: string) => { catalog: Catalog; schema: Schema } | undefined;
  findTable: (tableId: string) => { catalog: Catalog; schema: Schema; table: Table } | undefined;
}

function emitOps(ops: Op[]) {
  vscode.postMessage({ type: 'append-ops', payload: ops });
}

export const useDesignerStore = create<DesignerState>((set, get) => ({
  project: null,
  selectedCatalogId: null,
  selectedSchemaId: null,
  selectedTableId: null,

  setProject: (project) => set({ project }),
  selectCatalog: (catalogId) => set({ selectedCatalogId: catalogId }),
  selectSchema: (schemaId) => set({ selectedSchemaId: schemaId }),
  selectTable: (tableId) => set({ selectedTableId: tableId }),

  addCatalog: (name) => {
    const catalogId = `cat_${uuidv4()}`;
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'add_catalog',
      target: catalogId,
      payload: { catalogId, name },
    };
    emitOps([op]);
  },

  renameCatalog: (catalogId, newName) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'rename_catalog',
      target: catalogId,
      payload: { newName },
    };
    emitOps([op]);
  },

  dropCatalog: (catalogId) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'drop_catalog',
      target: catalogId,
      payload: {},
    };
    emitOps([op]);
  },

  addSchema: (catalogId, name) => {
    const schemaId = `sch_${uuidv4()}`;
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'add_schema',
      target: schemaId,
      payload: { schemaId, name, catalogId },
    };
    emitOps([op]);
  },

  renameSchema: (schemaId, newName) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'rename_schema',
      target: schemaId,
      payload: { newName },
    };
    emitOps([op]);
  },

  dropSchema: (schemaId) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'drop_schema',
      target: schemaId,
      payload: {},
    };
    emitOps([op]);
  },

  addTable: (schemaId, name, format) => {
    const tableId = `tbl_${uuidv4()}`;
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'add_table',
      target: tableId,
      payload: { tableId, name, schemaId, format },
    };
    emitOps([op]);
  },

  renameTable: (tableId, newName) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'rename_table',
      target: tableId,
      payload: { newName },
    };
    emitOps([op]);
  },

  dropTable: (tableId) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'drop_table',
      target: tableId,
      payload: {},
    };
    emitOps([op]);
  },

  setTableComment: (tableId, comment) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'set_table_comment',
      target: tableId,
      payload: { comment },
    };
    emitOps([op]);
  },

  addColumn: (tableId, name, type, nullable, after) => {
    const colId = `col_${uuidv4()}`;
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'add_column',
      target: colId,
      payload: { tableId, colId, name, type, nullable, after },
    };
    emitOps([op]);
  },

  renameColumn: (tableId, colId, newName) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'rename_column',
      target: colId,
      payload: { tableId, colId, newName },
    };
    emitOps([op]);
  },

  dropColumn: (tableId, colId) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'drop_column',
      target: colId,
      payload: { tableId, colId },
    };
    emitOps([op]);
  },

  reorderColumns: (tableId, order) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'reorder_columns',
      target: tableId,
      payload: { tableId, order },
    };
    emitOps([op]);
  },

  changeColumnType: (tableId, colId, newType) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'change_column_type',
      target: colId,
      payload: { tableId, colId, newType },
    };
    emitOps([op]);
  },

  setColumnNullable: (tableId, colId, nullable) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'set_nullable',
      target: colId,
      payload: { tableId, colId, nullable },
    };
    emitOps([op]);
  },

  setColumnComment: (tableId, colId, comment) => {
    const op: Op = {
      ts: new Date().toISOString(),
      op: 'set_column_comment',
      target: colId,
      payload: { tableId, colId, comment },
    };
    emitOps([op]);
  },

  findCatalog: (catalogId) => {
    const { project } = get();
    return project?.state.catalogs.find((c) => c.id === catalogId);
  },

  findSchema: (schemaId) => {
    const { project } = get();
    if (!project) return undefined;
    for (const catalog of project.state.catalogs) {
      const schema = catalog.schemas.find((s) => s.id === schemaId);
      if (schema) return { catalog, schema };
    }
    return undefined;
  },

  findTable: (tableId) => {
    const { project } = get();
    if (!project) return undefined;
    for (const catalog of project.state.catalogs) {
      for (const schema of catalog.schemas) {
        const table = schema.tables.find((t) => t.id === tableId);
        if (table) return { catalog, schema, table };
      }
    }
    return undefined;
  },
}));

