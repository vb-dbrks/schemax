/**
 * Unit tests for useDesignerStore - Zustand store with all state mutations
 *
 * Strategy: Import the real store, seed it with project data + provider,
 * call actions, and assert resulting state / postMessage calls.
 */

import { describe, test, expect, jest, beforeEach, afterEach } from '@jest/globals';
import { useDesignerStore } from '../../../src/webview/state/useDesignerStore';
import type { ProjectFile } from '../../../src/webview/models/unity';

// Capture postMessage calls via global mock (set up in tests/setup.ts)
let capturedOps: any[];

// A deterministic counter for uuid mocks
let uuidCounter = 0;
jest.mock('uuid', () => ({
  v4: () => `test-uuid-${++uuidCounter}`,
}));

function makeProject(): ProjectFile {
  return {
    version: 5,
    name: 'test-project',
    targets: {
      default: {
        type: 'unity',
        version: '1.0.0',
        environments: {
          dev: { topLevelName: 'dev_catalog' },
        },
      },
    },
    defaultTarget: 'default',
    state: {
      catalogs: [
        {
          id: 'cat_1',
          name: 'bronze',
          comment: 'Bronze layer',
          tags: { layer: 'bronze' },
          schemas: [
            {
              id: 'sch_1',
              name: 'raw',
              comment: 'Raw data',
              tables: [
                {
                  id: 'tbl_1',
                  name: 'events',
                  format: 'delta' as const,
                  columns: [
                    { id: 'col_1', name: 'id', type: 'INT', nullable: false },
                    { id: 'col_2', name: 'payload', type: 'STRING', nullable: true },
                  ],
                  properties: { 'delta.appendOnly': 'true' },
                  tags: { pii: 'false' },
                  constraints: [
                    { id: 'const_1', name: 'pk_id', type: 'primary_key' as const, columns: ['id'] },
                  ],
                  grants: [{ principal: 'readers', privileges: ['SELECT'] }],
                  rowFilters: [
                    {
                      id: 'rf_1',
                      name: 'region_filter',
                      enabled: true,
                      udfExpression: 'region = current_user()',
                    },
                  ],
                  columnMasks: [
                    {
                      id: 'cm_1',
                      name: 'email_mask',
                      columnId: 'col_2',
                      enabled: true,
                      maskFunction: 'REDACT(payload)',
                    },
                  ],
                },
              ],
              views: [
                {
                  id: 'view_1',
                  name: 'events_view',
                  definition: 'SELECT * FROM events',
                  tags: {},
                  properties: {},
                  grants: [{ principal: 'analysts', privileges: ['SELECT'] }],
                },
              ],
              volumes: [
                {
                  id: 'vol_1',
                  name: 'landing',
                  volumeType: 'managed' as const,
                  comment: 'Landing zone',
                },
              ],
              functions: [
                {
                  id: 'func_1',
                  name: 'parse_json',
                  language: 'SQL' as const,
                  body: 'SELECT from_json(data)',
                  returnType: 'STRING',
                },
              ],
              materializedViews: [
                {
                  id: 'mv_1',
                  name: 'events_agg',
                  definition: 'SELECT count(*) FROM events',
                },
              ],
              grants: [{ principal: 'writers', privileges: ['CREATE TABLE'] }],
            },
          ],
          grants: [{ principal: 'admins', privileges: ['USE CATALOG'] }],
        },
      ],
    },
    ops: [],
    snapshots: [],
    deployments: [],
    settings: { autoIncrementVersion: true, versionPrefix: 'v' },
    latestSnapshot: null,
  };
}

const defaultProvider = {
  id: 'unity',
  name: 'Unity Catalog',
  version: '1.0.0',
  capabilities: {
    supportedOperations: [],
    supportedObjectTypes: [],
    hierarchy: { levels: [] },
    features: {},
  },
};

describe('useDesignerStore', () => {
  beforeEach(() => {
    uuidCounter = 0;
    capturedOps = [];
    (global as any).__capturedOps = capturedOps;

    // Reset store to clean state
    const store = useDesignerStore.getState();
    useDesignerStore.setState({
      project: null,
      provider: null,
      activeTarget: null,
      selectedCatalogId: null,
      selectedSchemaId: null,
      selectedTableId: null,
      pendingUndoBatches: {},
      undoStack: [],
    });
  });

  afterEach(() => {
    (global as any).__capturedOps = undefined;
  });

  // ── Setters ──────────────────────────────────────────────────────

  describe('setters', () => {
    test('setProject stores project', () => {
      const project = makeProject();
      useDesignerStore.getState().setProject(project);
      expect(useDesignerStore.getState().project).toBe(project);
    });

    test('setProvider stores provider metadata', () => {
      useDesignerStore.getState().setProvider(defaultProvider);
      expect(useDesignerStore.getState().provider).toBe(defaultProvider);
    });

    test('setActiveTarget updates active target', () => {
      useDesignerStore.getState().setActiveTarget('staging');
      expect(useDesignerStore.getState().activeTarget).toBe('staging');
    });

    test('setActiveTarget accepts null', () => {
      useDesignerStore.getState().setActiveTarget('staging');
      useDesignerStore.getState().setActiveTarget(null);
      expect(useDesignerStore.getState().activeTarget).toBeNull();
    });

    test('selectCatalog stores selected catalog id', () => {
      useDesignerStore.getState().selectCatalog('cat_1');
      expect(useDesignerStore.getState().selectedCatalogId).toBe('cat_1');
    });

    test('selectSchema stores selected schema id', () => {
      useDesignerStore.getState().selectSchema('sch_1');
      expect(useDesignerStore.getState().selectedSchemaId).toBe('sch_1');
    });

    test('selectTable stores selected table id', () => {
      useDesignerStore.getState().selectTable('tbl_1');
      expect(useDesignerStore.getState().selectedTableId).toBe('tbl_1');
    });
  });

  // ── Finders ──────────────────────────────────────────────────────

  describe('finders', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
    });

    test('findCatalog returns catalog by id', () => {
      const catalog = useDesignerStore.getState().findCatalog('cat_1');
      expect(catalog).toBeDefined();
      expect(catalog!.name).toBe('bronze');
    });

    test('findCatalog returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findCatalog('cat_999')).toBeUndefined();
    });

    test('findSchema returns catalog + schema', () => {
      const result = useDesignerStore.getState().findSchema('sch_1');
      expect(result).toBeDefined();
      expect(result!.catalog.name).toBe('bronze');
      expect(result!.schema.name).toBe('raw');
    });

    test('findSchema returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findSchema('sch_999')).toBeUndefined();
    });

    test('findTable returns catalog + schema + table', () => {
      const result = useDesignerStore.getState().findTable('tbl_1');
      expect(result).toBeDefined();
      expect(result!.table.name).toBe('events');
      expect(result!.schema.name).toBe('raw');
    });

    test('findTable returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findTable('tbl_999')).toBeUndefined();
    });

    test('findView returns catalog + schema + view', () => {
      const result = useDesignerStore.getState().findView('view_1');
      expect(result).toBeDefined();
      expect(result!.view.name).toBe('events_view');
    });

    test('findView returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findView('view_999')).toBeUndefined();
    });

    test('findVolume returns catalog + schema + volume', () => {
      const result = useDesignerStore.getState().findVolume('vol_1');
      expect(result).toBeDefined();
      expect(result!.volume.name).toBe('landing');
    });

    test('findVolume returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findVolume('vol_999')).toBeUndefined();
    });

    test('findFunction returns catalog + schema + func', () => {
      const result = useDesignerStore.getState().findFunction('func_1');
      expect(result).toBeDefined();
      expect(result!.func.name).toBe('parse_json');
    });

    test('findFunction returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findFunction('func_999')).toBeUndefined();
    });

    test('findMaterializedView returns catalog + schema + mv', () => {
      const result = useDesignerStore.getState().findMaterializedView('mv_1');
      expect(result).toBeDefined();
      expect(result!.mv.name).toBe('events_agg');
    });

    test('findMaterializedView returns undefined for missing id', () => {
      expect(useDesignerStore.getState().findMaterializedView('mv_999')).toBeUndefined();
    });

    test('finders return undefined when project is null', () => {
      useDesignerStore.setState({ project: null });
      expect(useDesignerStore.getState().findCatalog('cat_1')).toBeUndefined();
      expect(useDesignerStore.getState().findSchema('sch_1')).toBeUndefined();
      expect(useDesignerStore.getState().findTable('tbl_1')).toBeUndefined();
      expect(useDesignerStore.getState().findView('view_1')).toBeUndefined();
      expect(useDesignerStore.getState().findVolume('vol_1')).toBeUndefined();
      expect(useDesignerStore.getState().findFunction('func_1')).toBeUndefined();
      expect(useDesignerStore.getState().findMaterializedView('mv_1')).toBeUndefined();
    });
  });

  // ── Catalog mutations ────────────────────────────────────────────

  describe('catalog mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addCatalog emits add_catalog op', () => {
      useDesignerStore.getState().addCatalog('silver', { comment: 'Silver layer' });
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.add_catalog');
      expect(capturedOps[0].payload.name).toBe('silver');
      expect(capturedOps[0].payload.comment).toBe('Silver layer');
    });

    test('renameCatalog emits rename_catalog op', () => {
      useDesignerStore.getState().renameCatalog('cat_1', 'bronze_v2');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.rename_catalog');
      expect(capturedOps[0].payload.oldName).toBe('bronze');
      expect(capturedOps[0].payload.newName).toBe('bronze_v2');
    });

    test('renameCatalog throws for missing catalog', () => {
      expect(() => useDesignerStore.getState().renameCatalog('cat_999', 'x')).toThrow(
        /catalog cat_999 not found/
      );
    });

    test('updateCatalog emits update_catalog op', () => {
      useDesignerStore.getState().updateCatalog('cat_1', { comment: 'Updated' });
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.update_catalog');
      expect(capturedOps[0].payload.comment).toBe('Updated');
    });

    test('updateCatalog throws for missing catalog', () => {
      expect(() => useDesignerStore.getState().updateCatalog('cat_999', {})).toThrow(
        /catalog cat_999 not found/
      );
    });

    test('dropCatalog emits drop_catalog op', () => {
      useDesignerStore.getState().dropCatalog('cat_1');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.drop_catalog');
    });
  });

  // ── Schema mutations ─────────────────────────────────────────────

  describe('schema mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addSchema emits add_schema op', () => {
      useDesignerStore.getState().addSchema('cat_1', 'curated');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.add_schema');
      expect(capturedOps[0].payload.name).toBe('curated');
      expect(capturedOps[0].payload.catalogId).toBe('cat_1');
    });

    test('renameSchema emits rename_schema op', () => {
      useDesignerStore.getState().renameSchema('sch_1', 'raw_v2');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.rename_schema');
      expect(capturedOps[0].payload.oldName).toBe('raw');
      expect(capturedOps[0].payload.newName).toBe('raw_v2');
    });

    test('renameSchema throws for missing schema', () => {
      expect(() => useDesignerStore.getState().renameSchema('sch_999', 'x')).toThrow(
        /schema sch_999 not found/
      );
    });

    test('updateSchema emits update_schema op', () => {
      useDesignerStore.getState().updateSchema('sch_1', { comment: 'Updated raw' });
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.update_schema');
    });

    test('dropSchema emits drop_schema op', () => {
      useDesignerStore.getState().dropSchema('sch_1');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.drop_schema');
    });
  });

  // ── Table mutations ──────────────────────────────────────────────

  describe('table mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addTable emits add_table op', () => {
      useDesignerStore.getState().addTable('sch_1', 'users', 'delta');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.add_table');
      expect(capturedOps[0].payload.name).toBe('users');
      expect(capturedOps[0].payload.format).toBe('delta');
    });

    test('addTable with options includes external fields', () => {
      useDesignerStore.getState().addTable('sch_1', 'ext_tbl', 'delta', {
        external: true,
        externalLocationName: 'lake',
        path: 'data/',
        comment: 'External table',
      });
      expect(capturedOps[0].payload.external).toBe(true);
      expect(capturedOps[0].payload.externalLocationName).toBe('lake');
    });

    test('renameTable emits rename_table op', () => {
      useDesignerStore.getState().renameTable('tbl_1', 'events_v2');
      expect(capturedOps[0].op).toBe('unity.rename_table');
      expect(capturedOps[0].payload.oldName).toBe('events');
      expect(capturedOps[0].payload.newName).toBe('events_v2');
    });

    test('renameTable throws for missing table', () => {
      expect(() => useDesignerStore.getState().renameTable('tbl_999', 'x')).toThrow(
        /table tbl_999 not found/
      );
    });

    test('dropTable emits drop_table op', () => {
      useDesignerStore.getState().dropTable('tbl_1');
      expect(capturedOps[0].op).toBe('unity.drop_table');
    });

    test('setTableComment emits set_table_comment op', () => {
      useDesignerStore.getState().setTableComment('tbl_1', 'New comment');
      expect(capturedOps[0].op).toBe('unity.set_table_comment');
      expect(capturedOps[0].payload.comment).toBe('New comment');
    });
  });

  // ── Column mutations ─────────────────────────────────────────────

  describe('column mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addColumn emits add_column op', () => {
      useDesignerStore.getState().addColumn('tbl_1', 'email', 'STRING', true, 'User email');
      expect(capturedOps.length).toBe(1);
      expect(capturedOps[0].op).toBe('unity.add_column');
      expect(capturedOps[0].payload.name).toBe('email');
      expect(capturedOps[0].payload.type).toBe('STRING');
      expect(capturedOps[0].payload.nullable).toBe(true);
    });

    test('addColumn with tags emits add_column + set_column_tag ops', () => {
      useDesignerStore.getState().addColumn('tbl_1', 'ssn', 'STRING', false, undefined, {
        pii: 'true',
        classification: 'sensitive',
      });
      expect(capturedOps.length).toBe(3); // 1 add_column + 2 set_column_tag
      expect(capturedOps[0].op).toBe('unity.add_column');
      expect(capturedOps[1].op).toBe('unity.set_column_tag');
      expect(capturedOps[2].op).toBe('unity.set_column_tag');
    });

    test('renameColumn emits rename_column op', () => {
      useDesignerStore.getState().renameColumn('tbl_1', 'col_1', 'event_id');
      expect(capturedOps[0].op).toBe('unity.rename_column');
      expect(capturedOps[0].payload.oldName).toBe('id');
      expect(capturedOps[0].payload.newName).toBe('event_id');
    });

    test('renameColumn throws for missing table', () => {
      expect(() => useDesignerStore.getState().renameColumn('tbl_999', 'col_1', 'x')).toThrow(
        /table tbl_999 not found/
      );
    });

    test('renameColumn throws for missing column', () => {
      expect(() => useDesignerStore.getState().renameColumn('tbl_1', 'col_999', 'x')).toThrow(
        /column col_999 not found/
      );
    });

    test('dropColumn emits drop_column op', () => {
      useDesignerStore.getState().dropColumn('tbl_1', 'col_2');
      expect(capturedOps[0].op).toBe('unity.drop_column');
      expect(capturedOps[0].payload.name).toBe('payload');
    });

    test('dropColumn throws for missing table', () => {
      expect(() => useDesignerStore.getState().dropColumn('tbl_999', 'col_1')).toThrow(
        /table tbl_999 not found/
      );
    });

    test('changeColumnType emits change_column_type op', () => {
      useDesignerStore.getState().changeColumnType('tbl_1', 'col_1', 'BIGINT');
      expect(capturedOps[0].op).toBe('unity.change_column_type');
      expect(capturedOps[0].payload.newType).toBe('BIGINT');
    });

    test('setColumnNullable emits set_nullable op', () => {
      useDesignerStore.getState().setColumnNullable('tbl_1', 'col_1', true);
      expect(capturedOps[0].op).toBe('unity.set_nullable');
      expect(capturedOps[0].payload.nullable).toBe(true);
    });

    test('setColumnComment emits set_column_comment op', () => {
      useDesignerStore.getState().setColumnComment('tbl_1', 'col_1', 'Primary key');
      expect(capturedOps[0].op).toBe('unity.set_column_comment');
      expect(capturedOps[0].payload.comment).toBe('Primary key');
    });

    test('reorderColumns emits reorder_columns op with previous order', () => {
      useDesignerStore.getState().reorderColumns('tbl_1', ['col_2', 'col_1']);
      expect(capturedOps[0].op).toBe('unity.reorder_columns');
      expect(capturedOps[0].payload.order).toEqual(['col_2', 'col_1']);
      expect(capturedOps[0].payload.previousOrder).toEqual(['col_1', 'col_2']);
    });
  });

  // ── View mutations ───────────────────────────────────────────────

  describe('view mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addView emits add_view op', () => {
      useDesignerStore.getState().addView('sch_1', 'summary', 'SELECT 1');
      expect(capturedOps[0].op).toBe('unity.add_view');
      expect(capturedOps[0].payload.name).toBe('summary');
      expect(capturedOps[0].payload.definition).toBe('SELECT 1');
    });

    test('renameView emits rename_view op', () => {
      useDesignerStore.getState().renameView('view_1', 'events_view_v2');
      expect(capturedOps[0].op).toBe('unity.rename_view');
      expect(capturedOps[0].payload.oldName).toBe('events_view');
    });

    test('renameView throws for missing view', () => {
      expect(() => useDesignerStore.getState().renameView('view_999', 'x')).toThrow(
        /view view_999 not found/
      );
    });

    test('updateView emits update_view op', () => {
      useDesignerStore.getState().updateView('view_1', 'SELECT id FROM events', {
        tables: ['events'],
        views: [],
      });
      expect(capturedOps[0].op).toBe('unity.update_view');
      expect(capturedOps[0].payload.definition).toBe('SELECT id FROM events');
    });

    test('dropView emits drop_view op', () => {
      useDesignerStore.getState().dropView('view_1');
      expect(capturedOps[0].op).toBe('unity.drop_view');
    });
  });

  // ── Volume mutations ─────────────────────────────────────────────

  describe('volume mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addVolume emits add_volume op', () => {
      useDesignerStore.getState().addVolume('sch_1', 'exports', 'external', {
        location: 's3://bucket/exports',
      });
      expect(capturedOps[0].op).toBe('unity.add_volume');
      expect(capturedOps[0].payload.name).toBe('exports');
      expect(capturedOps[0].payload.volumeType).toBe('external');
    });

    test('renameVolume emits rename_volume op', () => {
      useDesignerStore.getState().renameVolume('vol_1', 'landing_v2');
      expect(capturedOps[0].op).toBe('unity.rename_volume');
      expect(capturedOps[0].payload.oldName).toBe('landing');
    });

    test('renameVolume throws for missing volume', () => {
      expect(() => useDesignerStore.getState().renameVolume('vol_999', 'x')).toThrow(
        /Volume vol_999 not found/
      );
    });

    test('updateVolume emits update_volume op', () => {
      useDesignerStore.getState().updateVolume('vol_1', { comment: 'Updated' });
      expect(capturedOps[0].op).toBe('unity.update_volume');
    });

    test('dropVolume emits drop_volume op', () => {
      useDesignerStore.getState().dropVolume('vol_1');
      expect(capturedOps[0].op).toBe('unity.drop_volume');
    });
  });

  // ── Function mutations ───────────────────────────────────────────

  describe('function mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addFunction emits add_function op', () => {
      useDesignerStore.getState().addFunction('sch_1', 'to_upper', 'SQL', 'UPPER(x)', {
        returnType: 'STRING',
      });
      expect(capturedOps[0].op).toBe('unity.add_function');
      expect(capturedOps[0].payload.name).toBe('to_upper');
      expect(capturedOps[0].payload.language).toBe('SQL');
    });

    test('renameFunction emits rename_function op', () => {
      useDesignerStore.getState().renameFunction('func_1', 'parse_json_v2');
      expect(capturedOps[0].op).toBe('unity.rename_function');
      expect(capturedOps[0].payload.oldName).toBe('parse_json');
    });

    test('renameFunction throws for missing function', () => {
      expect(() => useDesignerStore.getState().renameFunction('func_999', 'x')).toThrow(
        /Function func_999 not found/
      );
    });

    test('updateFunction emits update_function op', () => {
      useDesignerStore.getState().updateFunction('func_1', { body: 'SELECT 1' });
      expect(capturedOps[0].op).toBe('unity.update_function');
    });

    test('dropFunction emits drop_function op', () => {
      useDesignerStore.getState().dropFunction('func_1');
      expect(capturedOps[0].op).toBe('unity.drop_function');
    });
  });

  // ── Materialized view mutations ──────────────────────────────────

  describe('materialized view mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addMaterializedView emits add_materialized_view op', () => {
      useDesignerStore.getState().addMaterializedView('sch_1', 'daily_agg', 'SELECT count(*) FROM events', {
        refreshSchedule: 'CRON 0 0 * * *',
      });
      expect(capturedOps[0].op).toBe('unity.add_materialized_view');
      expect(capturedOps[0].payload.name).toBe('daily_agg');
      expect(capturedOps[0].payload.refreshSchedule).toBe('CRON 0 0 * * *');
    });

    test('renameMaterializedView emits rename_materialized_view op', () => {
      useDesignerStore.getState().renameMaterializedView('mv_1', 'events_agg_v2');
      expect(capturedOps[0].op).toBe('unity.rename_materialized_view');
      expect(capturedOps[0].payload.oldName).toBe('events_agg');
    });

    test('renameMaterializedView throws for missing mv', () => {
      expect(() => useDesignerStore.getState().renameMaterializedView('mv_999', 'x')).toThrow(
        /Materialized view mv_999 not found/
      );
    });

    test('updateMaterializedView emits update_materialized_view op', () => {
      useDesignerStore.getState().updateMaterializedView('mv_1', 'SELECT sum(id) FROM events');
      expect(capturedOps[0].op).toBe('unity.update_materialized_view');
      expect(capturedOps[0].payload.definition).toBe('SELECT sum(id) FROM events');
    });

    test('dropMaterializedView emits drop_materialized_view op', () => {
      useDesignerStore.getState().dropMaterializedView('mv_1');
      expect(capturedOps[0].op).toBe('unity.drop_materialized_view');
    });
  });

  // ── Property & tag mutations ─────────────────────────────────────

  describe('property and tag mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('setTableProperty emits set_table_property op', () => {
      useDesignerStore.getState().setTableProperty('tbl_1', 'delta.appendOnly', 'false');
      expect(capturedOps[0].op).toBe('unity.set_table_property');
      expect(capturedOps[0].payload.key).toBe('delta.appendOnly');
      expect(capturedOps[0].payload.value).toBe('false');
    });

    test('unsetTableProperty emits unset_table_property op', () => {
      useDesignerStore.getState().unsetTableProperty('tbl_1', 'delta.appendOnly');
      expect(capturedOps[0].op).toBe('unity.unset_table_property');
      expect(capturedOps[0].payload.key).toBe('delta.appendOnly');
    });

    test('setTableTag emits set_table_tag op', () => {
      useDesignerStore.getState().setTableTag('tbl_1', 'env', 'prod');
      expect(capturedOps[0].op).toBe('unity.set_table_tag');
      expect(capturedOps[0].payload.tagName).toBe('env');
      expect(capturedOps[0].payload.tagValue).toBe('prod');
    });

    test('unsetTableTag emits unset_table_tag op', () => {
      useDesignerStore.getState().unsetTableTag('tbl_1', 'pii');
      expect(capturedOps[0].op).toBe('unity.unset_table_tag');
      expect(capturedOps[0].payload.tagName).toBe('pii');
    });

    test('setColumnTag emits set_column_tag op', () => {
      useDesignerStore.getState().setColumnTag('tbl_1', 'col_1', 'sensitive', 'true');
      expect(capturedOps[0].op).toBe('unity.set_column_tag');
      expect(capturedOps[0].payload.tagName).toBe('sensitive');
    });

    test('unsetColumnTag emits unset_column_tag op', () => {
      useDesignerStore.getState().unsetColumnTag('tbl_1', 'col_1', 'sensitive');
      expect(capturedOps[0].op).toBe('unity.unset_column_tag');
    });
  });

  // ── Constraint mutations ─────────────────────────────────────────

  describe('constraint mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addConstraint emits add_constraint op', () => {
      useDesignerStore.getState().addConstraint('tbl_1', {
        name: 'fk_user',
        type: 'foreign_key',
        columns: ['user_id'],
        parentTable: 'users',
        parentColumns: ['id'],
      });
      expect(capturedOps[0].op).toBe('unity.add_constraint');
      expect(capturedOps[0].payload.name).toBe('fk_user');
      expect(capturedOps[0].payload.type).toBe('foreign_key');
    });

    test('updateConstraint emits drop + add ops', () => {
      useDesignerStore.getState().updateConstraint('tbl_1', 'const_1', {
        name: 'pk_event_id',
        type: 'primary_key',
        columns: ['id'],
      });
      expect(capturedOps.length).toBe(2);
      expect(capturedOps[0].op).toBe('unity.drop_constraint');
      expect(capturedOps[0].payload.name).toBe('pk_id');
      expect(capturedOps[1].op).toBe('unity.add_constraint');
      expect(capturedOps[1].payload.name).toBe('pk_event_id');
    });

    test('dropConstraint emits drop_constraint op with name', () => {
      useDesignerStore.getState().dropConstraint('tbl_1', 'const_1');
      expect(capturedOps[0].op).toBe('unity.drop_constraint');
      expect(capturedOps[0].payload.name).toBe('pk_id');
    });
  });

  // ── Row filter & column mask mutations ───────────────────────────

  describe('row filter and column mask mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addRowFilter emits add_row_filter op', () => {
      useDesignerStore.getState().addRowFilter('tbl_1', 'dept_filter', 'dept = "eng"', true, 'By department');
      expect(capturedOps[0].op).toBe('unity.add_row_filter');
      expect(capturedOps[0].payload.name).toBe('dept_filter');
      expect(capturedOps[0].payload.enabled).toBe(true);
    });

    test('updateRowFilter emits update_row_filter op', () => {
      useDesignerStore.getState().updateRowFilter('tbl_1', 'rf_1', { enabled: false });
      expect(capturedOps[0].op).toBe('unity.update_row_filter');
      expect(capturedOps[0].payload.enabled).toBe(false);
    });

    test('removeRowFilter emits remove_row_filter op', () => {
      useDesignerStore.getState().removeRowFilter('tbl_1', 'rf_1');
      expect(capturedOps[0].op).toBe('unity.remove_row_filter');
    });

    test('addColumnMask emits add_column_mask op', () => {
      useDesignerStore.getState().addColumnMask('tbl_1', 'col_1', 'id_mask', 'HASH(id)', true, 'Hash ID');
      expect(capturedOps[0].op).toBe('unity.add_column_mask');
      expect(capturedOps[0].payload.maskFunction).toBe('HASH(id)');
    });

    test('updateColumnMask emits update_column_mask op', () => {
      useDesignerStore.getState().updateColumnMask('tbl_1', 'cm_1', { enabled: false });
      expect(capturedOps[0].op).toBe('unity.update_column_mask');
    });

    test('removeColumnMask emits remove_column_mask op', () => {
      useDesignerStore.getState().removeColumnMask('tbl_1', 'cm_1');
      expect(capturedOps[0].op).toBe('unity.remove_column_mask');
    });
  });

  // ── Grant mutations ──────────────────────────────────────────────

  describe('grant mutations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('addGrant emits add_grant op for catalog', () => {
      useDesignerStore.getState().addGrant('catalog', 'cat_1', 'team_a', ['USE CATALOG']);
      expect(capturedOps[0].op).toBe('unity.add_grant');
      expect(capturedOps[0].payload.targetType).toBe('catalog');
      expect(capturedOps[0].payload.principal).toBe('team_a');
      expect(capturedOps[0].payload.privileges).toEqual(['USE CATALOG']);
    });

    test('addGrant emits add_grant op for table', () => {
      useDesignerStore.getState().addGrant('table', 'tbl_1', 'analysts', ['SELECT', 'MODIFY']);
      expect(capturedOps[0].payload.targetType).toBe('table');
      expect(capturedOps[0].payload.privileges).toEqual(['SELECT', 'MODIFY']);
    });

    test('revokeGrant emits revoke_grant op', () => {
      useDesignerStore.getState().revokeGrant('table', 'tbl_1', 'readers');
      expect(capturedOps[0].op).toBe('unity.revoke_grant');
      expect(capturedOps[0].payload.principal).toBe('readers');
    });

    test('revokeGrant with specific privileges', () => {
      useDesignerStore.getState().revokeGrant('schema', 'sch_1', 'writers', ['CREATE TABLE']);
      expect(capturedOps[0].payload.privileges).toEqual(['CREATE TABLE']);
    });
  });

  // ── Operation structure ──────────────────────────────────────────

  describe('operation structure', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('operations have required fields', () => {
      useDesignerStore.getState().addCatalog('test');
      const op = capturedOps[0];
      expect(op.id).toMatch(/^op_test-uuid-/);
      expect(op.ts).toBeDefined();
      expect(op.provider).toBe('unity');
      expect(op.op).toBe('unity.add_catalog');
      expect(op.target).toBeDefined();
      expect(op.payload).toBeDefined();
    });

    test('operations include scope when activeTarget is set', () => {
      useDesignerStore.getState().setActiveTarget('staging');
      useDesignerStore.getState().addCatalog('scoped');
      expect(capturedOps[0].scope).toBe('staging');
    });

    test('operations have no scope when activeTarget is null', () => {
      useDesignerStore.getState().setActiveTarget(null);
      useDesignerStore.getState().addCatalog('unscoped');
      expect(capturedOps[0].scope).toBeUndefined();
    });

    test('createOperation throws when provider not set', () => {
      useDesignerStore.setState({ provider: null });
      expect(() => useDesignerStore.getState().addCatalog('x')).toThrow(
        /Provider not initialized/
      );
    });
  });

  // ── Undo stack ───────────────────────────────────────────────────

  describe('undo stack', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('recordUndoBatch creates a pending batch', () => {
      const ops = [{ id: 'op_1', ts: '', provider: 'unity', op: 'unity.test', target: 't', payload: {} }];
      const actionId = useDesignerStore.getState().recordUndoBatch(ops as any, 'Test action');
      expect(actionId).toMatch(/^action_test-uuid-/);
      expect(useDesignerStore.getState().pendingUndoBatches[actionId]).toBeDefined();
      expect(useDesignerStore.getState().pendingUndoBatches[actionId].actionLabel).toBe('Test action');
    });

    test('confirmUndoBatch moves pending to undo stack', () => {
      const ops = [{ id: 'op_1', ts: '', provider: 'unity', op: 'unity.test', target: 't', payload: {} }];
      const actionId = useDesignerStore.getState().recordUndoBatch(ops as any, 'Test');
      useDesignerStore.getState().confirmUndoBatch(actionId, ['op_1']);
      expect(useDesignerStore.getState().pendingUndoBatches[actionId]).toBeUndefined();
      expect(useDesignerStore.getState().undoStack.length).toBe(1);
      expect(useDesignerStore.getState().undoStack[0].opIds).toEqual(['op_1']);
    });

    test('confirmUndoBatch uses pending opIds when empty array passed', () => {
      const ops = [{ id: 'op_x', ts: '', provider: 'unity', op: 'unity.test', target: 't', payload: {} }];
      const actionId = useDesignerStore.getState().recordUndoBatch(ops as any);
      useDesignerStore.getState().confirmUndoBatch(actionId, []);
      expect(useDesignerStore.getState().undoStack[0].opIds).toEqual(['op_x']);
    });

    test('confirmUndoBatch is a no-op for unknown actionId', () => {
      useDesignerStore.getState().confirmUndoBatch('unknown_id', ['op_1']);
      expect(useDesignerStore.getState().undoStack.length).toBe(0);
    });

    test('discardUndoBatch removes pending batch', () => {
      const ops = [{ id: 'op_1', ts: '', provider: 'unity', op: 'unity.test', target: 't', payload: {} }];
      const actionId = useDesignerStore.getState().recordUndoBatch(ops as any);
      useDesignerStore.getState().discardUndoBatch(actionId);
      expect(useDesignerStore.getState().pendingUndoBatches[actionId]).toBeUndefined();
    });

    test('discardUndoBatch is a no-op for unknown actionId', () => {
      const before = { ...useDesignerStore.getState().pendingUndoBatches };
      useDesignerStore.getState().discardUndoBatch('unknown');
      expect(useDesignerStore.getState().pendingUndoBatches).toEqual(before);
    });

    test('undoLastAction pops from undo stack', () => {
      const ops = [{ id: 'op_1', ts: '', provider: 'unity', op: 'unity.test', target: 't', payload: {} }];
      const actionId = useDesignerStore.getState().recordUndoBatch(ops as any, 'Test');
      useDesignerStore.getState().confirmUndoBatch(actionId, ['op_1']);
      expect(useDesignerStore.getState().undoStack.length).toBe(1);

      const batch = useDesignerStore.getState().undoLastAction();
      expect(batch).not.toBeNull();
      expect(batch!.opIds).toEqual(['op_1']);
      expect(useDesignerStore.getState().undoStack.length).toBe(0);
    });

    test('undoLastAction returns null when stack empty', () => {
      expect(useDesignerStore.getState().undoLastAction()).toBeNull();
    });

    test('restoreUndoBatch pushes batch back to stack', () => {
      const batch = { actionId: 'a1', actionLabel: 'Test', opIds: ['op_1'] };
      useDesignerStore.getState().restoreUndoBatch(batch);
      expect(useDesignerStore.getState().undoStack.length).toBe(1);
      expect(useDesignerStore.getState().undoStack[0]).toEqual(batch);
    });
  });

  // ── Bulk operations ──────────────────────────────────────────────

  describe('bulk operations', () => {
    beforeEach(() => {
      useDesignerStore.getState().setProject(makeProject());
      useDesignerStore.getState().setProvider(defaultProvider);
    });

    test('getObjectsInScope returns objects for catalog scope', () => {
      const result = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      expect(result.catalog).toBeDefined();
      expect(result.catalog!.name).toBe('bronze');
      expect(result.schemas.length).toBeGreaterThan(0);
      expect(result.tables.length).toBeGreaterThan(0);
    });

    test('getObjectsInScope returns objects for schema scope', () => {
      const result = useDesignerStore.getState().getObjectsInScope('schema', 'cat_1', 'sch_1');
      expect(result.schemas.length).toBe(1);
      expect(result.tables.length).toBeGreaterThan(0);
    });

    test('applyBulkOps emits ops via postMessage', () => {
      const op = {
        id: 'op_bulk_1',
        ts: new Date().toISOString(),
        provider: 'unity',
        op: 'unity.add_grant',
        target: 'tbl_1',
        payload: { targetType: 'table', targetId: 'tbl_1', principal: 'x', privileges: ['SELECT'] },
      };
      useDesignerStore.getState().applyBulkOps([op as any]);
      expect(capturedOps.length).toBe(1);
    });

    test('applyBulkOps is a no-op for empty array', () => {
      useDesignerStore.getState().applyBulkOps([]);
      expect(capturedOps.length).toBe(0);
    });

    test('buildBulkGrantOps creates ops for all grant targets', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkGrantOps(scope, 'team_a', ['SELECT']);
      expect(ops.length).toBeGreaterThan(0);
      expect(ops[0].op).toBe('unity.add_grant');
      expect(ops[0].payload.principal).toBe('team_a');
    });

    test('buildBulkGrantOps filters by targetType', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkGrantOps(scope, 'x', ['SELECT'], 'table');
      for (const op of ops) {
        expect(op.payload.targetType).toBe('table');
      }
    });

    test('buildBulkTableTagOps creates tag ops for all tables', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkTableTagOps(scope, 'env', 'prod');
      expect(ops.length).toBe(scope.tables.length);
      expect(ops[0].op).toBe('unity.set_table_tag');
    });

    test('buildBulkViewTagOps creates tag ops for all views', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkViewTagOps(scope, 'env', 'prod');
      expect(ops.length).toBe(scope.views.length);
    });

    test('buildBulkSchemaTagOps creates ops for all schemas', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkSchemaTagOps(scope, 'env', 'prod');
      expect(ops.length).toBe(scope.schemas.length);
      expect(ops[0].op).toBe('unity.update_schema');
    });

    test('buildBulkCatalogTagOps creates op for catalog', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('catalog', 'cat_1');
      const ops = useDesignerStore.getState().buildBulkCatalogTagOps(scope, 'env', 'prod');
      expect(ops.length).toBe(1);
      expect(ops[0].op).toBe('unity.update_catalog');
    });

    test('buildBulkCatalogTagOps returns empty when no catalog', () => {
      const scope = useDesignerStore.getState().getObjectsInScope('schema', undefined, 'sch_1');
      // Schema scope doesn't include catalog in scope result
      if (!scope.catalog) {
        const ops = useDesignerStore.getState().buildBulkCatalogTagOps(scope, 'x', 'y');
        expect(ops.length).toBe(0);
      }
    });
  });
});
