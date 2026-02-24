/**
 * Unit tests for bulk operations scope resolution and preview formatting.
 */

import { describe, test, expect } from '@jest/globals';
import {
  getObjectsInScope,
  formatScopePreview,
  type ScopeResult,
  type ProjectFile,
} from '../../../src/webview/utils/bulkUtils';

function makeProject(catalogs: any[]): ProjectFile {
  return {
    version: 4,
    name: 'test',
    provider: { type: 'unity', version: '1.0.0' },
    state: { catalogs },
    ops: [],
    snapshots: [],
    deployments: [],
    settings: { autoIncrementVersion: true, versionPrefix: 'v' },
    latestSnapshot: null,
  };
}

describe('getObjectsInScope', () => {
  test('returns empty when project is null', () => {
    const result = getObjectsInScope(null, 'catalog', 'cat_1');
    expect(result.schemas).toHaveLength(0);
    expect(result.tables).toHaveLength(0);
    expect(result.grantTargets).toHaveLength(0);
    expect(result.catalog).toBeUndefined();
  });

  test('returns empty when project has no state.catalogs', () => {
    const project = makeProject([]);
    (project as any).state = undefined;
    const result = getObjectsInScope(project, 'catalog', 'cat_1');
    expect(result.grantTargets).toHaveLength(0);
  });

  test('returns empty for catalog scope when catalogId is missing', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'c1', schemas: [], grants: [] },
    ]);
    const result = getObjectsInScope(project, 'catalog', undefined);
    expect(result.grantTargets).toHaveLength(0);
    expect(result.catalog).toBeUndefined();
  });

  test('returns empty for catalog scope when catalog is not found', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'c1', schemas: [], grants: [] },
    ]);
    const result = getObjectsInScope(project, 'catalog', 'cat_999');
    expect(result.grantTargets).toHaveLength(0);
    expect(result.catalog).toBeUndefined();
  });

  test('catalog scope: returns catalog and grant target for catalog only when no schemas', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'my_catalog', schemas: [], grants: [], tags: { env: 'dev' } },
    ]);
    const result = getObjectsInScope(project, 'catalog', 'cat_1');
    expect(result.catalog).toEqual({ id: 'cat_1', name: 'my_catalog', tags: { env: 'dev' } });
    expect(result.schemas).toHaveLength(0);
    expect(result.tables).toHaveLength(0);
    expect(result.grantTargets).toHaveLength(1);
    expect(result.grantTargets[0]).toEqual({ targetType: 'catalog', targetId: 'cat_1' });
  });

  test('catalog scope: includes schemas, tables, views, volumes, functions, materialized views', () => {
    const project = makeProject([
      {
        id: 'cat_1',
        name: 'c1',
        tags: {},
        grants: [],
        schemas: [
          {
            id: 'sch_1',
            name: 's1',
            tags: { domain: 'sales' },
            tables: [{ id: 't1', name: 'table1' }],
            views: [{ id: 'v1', name: 'view1' }],
            volumes: [{ id: 'vol_1', name: 'vol1' }],
            functions: [{ id: 'f1', name: 'fn1' }],
            materializedViews: [{ id: 'mv1', name: 'mv1' }],
            grants: [],
          },
        ],
      },
    ]);
    const result = getObjectsInScope(project, 'catalog', 'cat_1');
    expect(result.catalog?.id).toBe('cat_1');
    expect(result.schemas).toHaveLength(1);
    expect(result.schemas[0]).toEqual({ id: 'sch_1', name: 's1', tags: { domain: 'sales' } });
    expect(result.tables).toHaveLength(1);
    expect(result.tables[0]).toEqual({ id: 't1', name: 'table1' });
    expect(result.views).toHaveLength(1);
    expect(result.views[0]).toEqual({ id: 'v1', name: 'view1' });
    expect(result.volumes).toHaveLength(1);
    expect(result.functions).toHaveLength(1);
    expect(result.materializedViews).toHaveLength(1);
    expect(result.grantTargets).toHaveLength(7); // catalog + schema + table + view + volume + function + mv
    const types = result.grantTargets.map((g) => g.targetType);
    expect(types).toContain('catalog');
    expect(types).toContain('schema');
    expect(types).toContain('table');
    expect(types).toContain('view');
    expect(types).toContain('volume');
    expect(types).toContain('function');
    expect(types).toContain('materialized_view');
  });

  test('catalog scope: multiple schemas and tables', () => {
    const project = makeProject([
      {
        id: 'cat_1',
        name: 'c1',
        schemas: [
          { id: 'sch_1', name: 's1', tables: [{ id: 't1', name: 't1' }], views: [], grants: [] },
          { id: 'sch_2', name: 's2', tables: [{ id: 't2', name: 't2' }, { id: 't3', name: 't3' }], views: [], grants: [] },
        ],
        grants: [],
      },
    ]);
    const result = getObjectsInScope(project, 'catalog', 'cat_1');
    expect(result.schemas).toHaveLength(2);
    expect(result.tables).toHaveLength(3);
    expect(result.grantTargets).toHaveLength(1 + 2 + 3); // 1 catalog + 2 schemas + 3 tables
  });

  test('returns empty for schema scope when schemaId is missing', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'c1', schemas: [{ id: 'sch_1', name: 's1', tables: [], views: [], grants: [] }], grants: [] },
    ]);
    const result = getObjectsInScope(project, 'schema', undefined, undefined);
    expect(result.grantTargets).toHaveLength(0);
    expect(result.catalog).toBeUndefined();
  });

  test('returns empty for schema scope when schema is not found', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'c1', schemas: [{ id: 'sch_1', name: 's1', tables: [], views: [], grants: [] }], grants: [] },
    ]);
    const result = getObjectsInScope(project, 'schema', undefined, 'sch_999');
    expect(result.grantTargets).toHaveLength(0);
  });

  test('schema scope: returns schema and its children only', () => {
    const project = makeProject([
      {
        id: 'cat_1',
        name: 'c1',
        schemas: [
          { id: 'sch_1', name: 's1', tables: [{ id: 't1', name: 't1' }], views: [{ id: 'v1', name: 'v1' }], grants: [] },
          { id: 'sch_2', name: 's2', tables: [{ id: 't2', name: 't2' }], views: [], grants: [] },
        ],
        grants: [],
      },
    ]);
    const result = getObjectsInScope(project, 'schema', undefined, 'sch_1');
    expect(result.catalog).toBeUndefined();
    expect(result.schemas).toHaveLength(1);
    expect(result.schemas[0]).toEqual({ id: 'sch_1', name: 's1', tags: undefined });
    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].id).toBe('t1');
    expect(result.views).toHaveLength(1);
    expect(result.grantTargets).toHaveLength(3); // schema + table + view
    expect(result.grantTargets[0]).toEqual({ targetType: 'schema', targetId: 'sch_1' });
  });

  test('schema scope: finds schema in second catalog', () => {
    const project = makeProject([
      { id: 'cat_1', name: 'c1', schemas: [{ id: 'sch_a', name: 'sa', tables: [], views: [], grants: [] }], grants: [] },
      {
        id: 'cat_2',
        name: 'c2',
        schemas: [{ id: 'sch_b', name: 'sb', tables: [{ id: 'tb', name: 'tb' }], views: [], grants: [] }],
        grants: [],
      },
    ]);
    const result = getObjectsInScope(project, 'schema', undefined, 'sch_b');
    expect(result.schemas[0].name).toBe('sb');
    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].id).toBe('tb');
    expect(result.grantTargets).toHaveLength(2); // schema + table
  });
});

describe('formatScopePreview', () => {
  test('returns "No objects in scope" for empty result', () => {
    const empty: ScopeResult = {
      schemas: [],
      tables: [],
      views: [],
      volumes: [],
      functions: [],
      materializedViews: [],
      grantTargets: [],
    };
    expect(formatScopePreview(empty)).toBe('No objects in scope');
  });

  test('includes catalog when present', () => {
    const result: ScopeResult = {
      catalog: { id: 'c1', name: 'c1' },
      schemas: [],
      tables: [],
      views: [],
      volumes: [],
      functions: [],
      materializedViews: [],
      grantTargets: [],
    };
    expect(formatScopePreview(result)).toBe('1 catalog');
  });

  test('formats counts for multiple types', () => {
    const result: ScopeResult = {
      schemas: [{ id: 's1', name: 's1' }, { id: 's2', name: 's2' }],
      tables: [{ id: 't1', name: 't1' }, { id: 't2', name: 't2' }, { id: 't3', name: 't3' }],
      views: [{ id: 'v1', name: 'v1' }],
      volumes: [],
      functions: [],
      materializedViews: [],
      grantTargets: [],
    };
    const text = formatScopePreview(result);
    expect(text).toContain('2 schemas');
    expect(text).toContain('3 tables');
    expect(text).toContain('1 view');
  });

  test('singular schema when count is 1', () => {
    const result: ScopeResult = {
      schemas: [{ id: 's1', name: 's1' }],
      tables: [],
      views: [],
      volumes: [],
      functions: [],
      materializedViews: [],
      grantTargets: [],
    };
    expect(formatScopePreview(result)).toContain('1 schema');
  });

  test('includes volumes, functions, materialized views when present', () => {
    const result: ScopeResult = {
      schemas: [],
      tables: [],
      views: [],
      volumes: [{ id: 'v1', name: 'v1' }],
      functions: [{ id: 'f1', name: 'f1' }],
      materializedViews: [{ id: 'mv1', name: 'mv1' }],
      grantTargets: [],
    };
    const text = formatScopePreview(result);
    expect(text).toContain('1 volume');
    expect(text).toContain('1 function');
    expect(text).toContain('1 materialized view');
  });
});
