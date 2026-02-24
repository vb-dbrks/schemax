/**
 * Bulk operations: scope resolution and types.
 * Pure helpers over project.state for use by BulkOperationsPanel and store.
 */

import type { ProjectFile, UnityCatalog, UnitySchema } from '../../providers/unity/models';

export type GrantTargetType =
  | 'catalog'
  | 'schema'
  | 'table'
  | 'view'
  | 'volume'
  | 'function'
  | 'materialized_view';

export interface GrantTarget {
  targetType: GrantTargetType;
  targetId: string;
}

export interface ScopeResult {
  /** When scope is catalog, the single catalog (for catalog tag) */
  catalog?: { id: string; name: string; tags?: Record<string, string> };
  /** All schemas in scope (for schema tag + grant targets) */
  schemas: Array<{ id: string; name: string; tags?: Record<string, string> }>;
  tables: Array<{ id: string; name: string }>;
  views: Array<{ id: string; name: string }>;
  volumes: Array<{ id: string; name: string }>;
  functions: Array<{ id: string; name: string }>;
  materializedViews: Array<{ id: string; name: string }>;
  /** Flat list for bulk grant: catalog + schemas + tables + views + volumes + functions + MVs */
  grantTargets: GrantTarget[];
}

function isUnityCatalog(c: any): c is UnityCatalog {
  return c && Array.isArray(c.schemas);
}

function isUnitySchema(s: any): s is UnitySchema {
  return s && Array.isArray(s.tables);
}

/**
 * Returns all objects in the given scope for bulk operations.
 * Scope is derived from selection: catalog (all under that catalog) or schema (all under that schema).
 */
export function getObjectsInScope(
  project: ProjectFile | null,
  scope: 'catalog' | 'schema',
  catalogId?: string | null,
  schemaId?: string | null
): ScopeResult {
  const empty: ScopeResult = {
    schemas: [],
    tables: [],
    views: [],
    volumes: [],
    functions: [],
    materializedViews: [],
    grantTargets: [],
  };

  if (!project?.state?.catalogs) return empty;

  const catalogs = project.state.catalogs as UnityCatalog[];

  if (scope === 'catalog') {
    if (!catalogId) return empty;
    const catalog = catalogs.find((c) => c.id === catalogId);
    if (!catalog || !isUnityCatalog(catalog)) return empty;

    const schemas: ScopeResult['schemas'] = [];
    const tables: ScopeResult['tables'] = [];
    const views: ScopeResult['views'] = [];
    const volumes: ScopeResult['volumes'] = [];
    const functions: ScopeResult['functions'] = [];
    const materializedViews: ScopeResult['materializedViews'] = [];
    const grantTargets: GrantTarget[] = [];

    grantTargets.push({ targetType: 'catalog', targetId: catalog.id });

    for (const schema of catalog.schemas || []) {
      if (!isUnitySchema(schema)) continue;
      schemas.push({
        id: schema.id,
        name: schema.name,
        tags: schema.tags,
      });
      grantTargets.push({ targetType: 'schema', targetId: schema.id });

      for (const t of schema.tables || []) {
        tables.push({ id: t.id, name: t.name });
        grantTargets.push({ targetType: 'table', targetId: t.id });
      }
      for (const v of schema.views || []) {
        views.push({ id: v.id, name: v.name });
        grantTargets.push({ targetType: 'view', targetId: v.id });
      }
      for (const v of schema.volumes || []) {
        volumes.push({ id: v.id, name: v.name });
        grantTargets.push({ targetType: 'volume', targetId: v.id });
      }
      for (const f of schema.functions || []) {
        functions.push({ id: f.id, name: f.name });
        grantTargets.push({ targetType: 'function', targetId: f.id });
      }
      for (const mv of schema.materializedViews || []) {
        materializedViews.push({ id: mv.id, name: mv.name });
        grantTargets.push({ targetType: 'materialized_view', targetId: mv.id });
      }
    }

    return {
      catalog: {
        id: catalog.id,
        name: catalog.name,
        tags: catalog.tags,
      },
      schemas,
      tables,
      views,
      volumes,
      functions,
      materializedViews,
      grantTargets,
    };
  }

  // scope === 'schema'
  if (!schemaId) return empty;
  for (const catalog of catalogs) {
    const schema = (catalog.schemas || []).find((s) => s.id === schemaId);
    if (!schema || !isUnitySchema(schema)) continue;

    const grantTargets: GrantTarget[] = [];
    grantTargets.push({ targetType: 'schema', targetId: schema.id });

    const tables: ScopeResult['tables'] = [];
    const views: ScopeResult['views'] = [];
    const volumes: ScopeResult['volumes'] = [];
    const functions: ScopeResult['functions'] = [];
    const materializedViews: ScopeResult['materializedViews'] = [];

    for (const t of schema.tables || []) {
      tables.push({ id: t.id, name: t.name });
      grantTargets.push({ targetType: 'table', targetId: t.id });
    }
    for (const v of schema.views || []) {
      views.push({ id: v.id, name: v.name });
      grantTargets.push({ targetType: 'view', targetId: v.id });
    }
    for (const v of schema.volumes || []) {
      volumes.push({ id: v.id, name: v.name });
      grantTargets.push({ targetType: 'volume', targetId: v.id });
    }
    for (const f of schema.functions || []) {
      functions.push({ id: f.id, name: f.name });
      grantTargets.push({ targetType: 'function', targetId: f.id });
    }
    for (const mv of schema.materializedViews || []) {
      materializedViews.push({ id: mv.id, name: mv.name });
      grantTargets.push({ targetType: 'materialized_view', targetId: mv.id });
    }

    return {
      schemas: [{ id: schema.id, name: schema.name, tags: schema.tags }],
      tables,
      views,
      volumes,
      functions,
      materializedViews,
      grantTargets,
    };
  }

  return empty;
}

function plural(n: number, singular: string, pluralForm: string): string {
  return n === 1 ? `1 ${singular}` : `${n} ${pluralForm}`;
}

/**
 * Human-readable summary of scope for preview (e.g. "5 tables, 2 views, 1 schema").
 */
export function formatScopePreview(result: ScopeResult): string {
  const parts: string[] = [];
  if (result.catalog) parts.push('1 catalog');
  if (result.schemas.length) parts.push(plural(result.schemas.length, 'schema', 'schemas'));
  if (result.tables.length) parts.push(plural(result.tables.length, 'table', 'tables'));
  if (result.views.length) parts.push(plural(result.views.length, 'view', 'views'));
  if (result.volumes.length) parts.push(plural(result.volumes.length, 'volume', 'volumes'));
  if (result.functions.length) parts.push(plural(result.functions.length, 'function', 'functions'));
  if (result.materializedViews.length) {
    parts.push(
      plural(result.materializedViews.length, 'materialized view', 'materialized views')
    );
  }
  return parts.length ? parts.join(', ') : 'No objects in scope';
}
