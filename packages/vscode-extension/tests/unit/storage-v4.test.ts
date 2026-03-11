/**
 * Tests for storage-v4.ts - storage layer functions
 */

import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import * as vscode from 'vscode';
import {
  ensureCatalogMappingsForNewCatalogs,
  ensureProjectFile,
  ensureSchemaxDir,
  getEnvironmentConfig,
  getTargetConfig,
  loadCurrentState,
  normalizeProviderCapabilities,
  readChangelog,
  readProject,
  readSnapshot,
  writeChangelog,
  writeProject,
  writeSnapshot,
  appendOps,
  ProjectFileV5,
  ProjectFileV4,
  ChangelogFile,
  SnapshotFile,
} from '../../src/storage-v4';
import { PythonBackendClient } from '../../src/backend/pythonBackendClient';
import type { Operation } from '../../src/contracts/workspace';

describe('Storage - Environment Configuration', () => {
  test('should retrieve environment configuration', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: {
              topLevelName: 'dev_catalog',
              allowDrift: true,
              requireSnapshot: false,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
            },
            prod: {
              topLevelName: 'prod_catalog',
              allowDrift: false,
              requireSnapshot: true,
              autoCreateTopLevel: false,
              autoCreateSchemaxSchema: true,
            },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    const devConfig = getEnvironmentConfig(project, 'dev');
    expect(devConfig.topLevelName).toBe('dev_catalog');
    expect(devConfig.allowDrift).toBe(true);
    expect(devConfig.requireSnapshot).toBe(false);

    const prodConfig = getEnvironmentConfig(project, 'prod');
    expect(prodConfig.topLevelName).toBe('prod_catalog');
    expect(prodConfig.allowDrift).toBe(false);
    expect(prodConfig.requireSnapshot).toBe(true);
  });

  test('should throw error for non-existent environment', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: {
              topLevelName: 'dev_catalog',
              allowDrift: true,
              requireSnapshot: false,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
            },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    expect(() => getEnvironmentConfig(project, 'staging')).toThrow();
  });

  test('should handle multiple environments', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: {
              topLevelName: 'dev_catalog',
              allowDrift: true,
              requireSnapshot: false,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
            },
            test: {
              topLevelName: 'test_catalog',
              allowDrift: false,
              requireSnapshot: true,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
            },
            prod: {
              topLevelName: 'prod_catalog',
              allowDrift: false,
              requireSnapshot: true,
              autoCreateTopLevel: false,
              autoCreateSchemaxSchema: true,
            },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    const testConfig = getEnvironmentConfig(project, 'test');
    expect(testConfig.topLevelName).toBe('test_catalog');
    expect(testConfig.requireSnapshot).toBe(true);
    expect(testConfig.autoCreateTopLevel).toBe(true);
  });
});

describe('Storage - Project Schema', () => {
  test('should validate v5 project structure', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: {
              topLevelName: 'dev_catalog',
              allowDrift: true,
              requireSnapshot: false,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
            },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    expect(project.version).toBe(5);
    expect(project.targets['default'].type).toBe('unity');
    expect(project.targets['default'].environments).toHaveProperty('dev');
  });

  test('should handle empty snapshots and deployments', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {},
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    expect(project.snapshots).toEqual([]);
    expect(project.deployments).toEqual([]);
    expect(project.latestSnapshot).toBeNull();
  });
});

describe('Storage - Catalog mapping defaults', () => {
  function baseProject(): ProjectFileV5 {
    return {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: {
              topLevelName: 'dev_catalog',
              allowDrift: true,
              requireSnapshot: false,
              autoCreateTopLevel: true,
              autoCreateSchemaxSchema: true,
              catalogMappings: {},
            },
            prod: {
              topLevelName: 'prod_catalog',
              allowDrift: false,
              requireSnapshot: true,
              autoCreateTopLevel: false,
              autoCreateSchemaxSchema: true,
              catalogMappings: {
                existing: 'prod_existing',
              },
            },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };
  }

  test('adds missing mappings for new add_catalog operations', () => {
    const project = baseProject();
    const ops: Operation[] = [
      {
        id: 'op_1',
        ts: '2026-01-01T00:00:00Z',
        provider: 'unity',
        op: 'unity.add_catalog',
        target: 'cat_1',
        payload: { name: 'bronze-layer' },
      },
    ];

    const result = ensureCatalogMappingsForNewCatalogs(project, ops);

    expect(result.updated).toBe(true);
    const target = result.project.targets['default'];
    expect(target.environments.dev.catalogMappings?.['bronze-layer']).toBe('dev_bronze_layer');
    expect(target.environments.prod.catalogMappings?.['bronze-layer']).toBe('prod_bronze_layer');
    expect(target.environments.prod.catalogMappings?.existing).toBe('prod_existing');
  });

  test('does not update when all mappings already exist', () => {
    const project = baseProject();
    const target = project.targets['default'];
    target.environments.dev.catalogMappings = {
      'bronze-layer': 'dev_bronze_layer',
    };
    target.environments.prod.catalogMappings = {
      ...target.environments.prod.catalogMappings,
      'bronze-layer': 'prod_bronze_layer',
    };
    const ops: Operation[] = [
      {
        id: 'op_1',
        ts: '2026-01-01T00:00:00Z',
        provider: 'unity',
        op: 'unity.add_catalog',
        target: 'cat_1',
        payload: { name: 'bronze-layer' },
      },
    ];

    const result = ensureCatalogMappingsForNewCatalogs(project, ops);

    expect(result.updated).toBe(false);
    expect(result.project).toBe(project);
  });
});

describe('Storage - Provider capability normalization', () => {
  test('normalizes snake_case capability payload to camelCase contract', () => {
    const normalized = normalizeProviderCapabilities({
      supported_operations: ['unity.add_catalog'],
      supported_object_types: ['catalog'],
      hierarchy: {
        levels: [
          {
            name: 'catalog',
            display_name: 'Catalog',
            plural_name: 'catalogs',
            icon: 'db',
            is_container: true,
          },
        ],
      },
      features: {
        views: true,
      },
    });

    expect(normalized.supportedOperations).toEqual(['unity.add_catalog']);
    expect(normalized.supportedObjectTypes).toEqual(['catalog']);
    expect(normalized.hierarchy.levels?.[0]).toEqual({
      name: 'catalog',
      displayName: 'Catalog',
      pluralName: 'catalogs',
      icon: 'db',
      isContainer: true,
    });
    expect(normalized.features).toEqual({ views: true });
  });

  test('handles missing capability fields safely', () => {
    const normalized = normalizeProviderCapabilities(undefined);
    expect(normalized.supportedOperations).toEqual([]);
    expect(normalized.supportedObjectTypes).toEqual([]);
    expect(normalized.hierarchy.levels).toEqual([]);
    expect(normalized.features).toEqual({});
  });
});

describe('Storage - Python transport contract', () => {
  test('loadCurrentState normalizes provider capabilities and validation payload', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: {
            version: 1,
            sinceSnapshot: null,
            ops: [],
            lastModified: '2026-01-01T00:00:00Z',
          },
          provider: {
            id: 'unity',
            name: 'Unity Catalog',
            version: '1.0.0',
            capabilities: {
              supported_operations: ['unity.add_catalog'],
              supported_object_types: ['catalog'],
              hierarchy: { levels: [{ name: 'catalog', display_name: 'Catalog' }] },
              features: { views: true },
            },
          },
          validation: {
            errors: [{ message: 'missing dep' }],
            warnings: ['soft warning'],
          },
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    const result = await loadCurrentState(vscode.Uri.file('/tmp/workspace'), true);

    expect(result.provider.capabilities.supportedOperations).toEqual(['unity.add_catalog']);
    expect(result.provider.capabilities.hierarchy.levels?.[0]?.displayName).toBe('Catalog');
    expect(result.validationResult).toEqual({
      errors: ['missing dep'],
      warnings: ['soft warning'],
    });
    expect(runJsonSpy).toHaveBeenCalledWith(
      'workspace-state',
      ['workspace-state', '--validate-dependencies'],
      '/tmp/workspace'
    );
    runJsonSpy.mockRestore();
  });

  test('loadCurrentState throws on backend envelope error', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'error',
        data: null,
        warnings: [],
        errors: [{ code: 'PYTHON_COMMAND_FAILED', message: 'boom' }],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 1 },
      });

    await expect(loadCurrentState(vscode.Uri.file('/tmp/workspace'), false)).rejects.toThrow(
      '[PYTHON_COMMAND_FAILED] boom'
    );
    expect(runJsonSpy).toHaveBeenCalledWith(
      'workspace-state',
      ['workspace-state'],
      '/tmp/workspace'
    );
    runJsonSpy.mockRestore();
  });

  test('loadCurrentState uses short-lived cache to reduce backend calls', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValue({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: {
            version: 1,
            sinceSnapshot: null,
            ops: [],
            lastModified: '2026-01-01T00:00:00Z',
          },
          provider: {
            id: 'unity',
            name: 'Unity Catalog',
            version: '1.0.0',
            capabilities: {},
          },
          validation: null,
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    await loadCurrentState(vscode.Uri.file('/tmp/workspace-cache'), false);
    await loadCurrentState(vscode.Uri.file('/tmp/workspace-cache'), false);

    expect(runJsonSpy).toHaveBeenCalledTimes(1);
    runJsonSpy.mockRestore();
  });

  test('loadCurrentState requests state-only payload mode when specified', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: {
            version: 1,
            sinceSnapshot: null,
            ops: [],
            opsCount: 4,
            lastModified: '2026-01-01T00:00:00Z',
          },
          provider: {
            id: 'unity',
            name: 'Unity Catalog',
            version: '1.0.0',
            capabilities: {},
          },
          validation: null,
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    await loadCurrentState(vscode.Uri.file('/tmp/workspace-state-only'), false, {
      payloadMode: 'state-only',
    });
    expect(runJsonSpy).toHaveBeenCalledWith(
      'workspace-state',
      ['workspace-state', '--payload-mode', 'state-only'],
      '/tmp/workspace-state-only'
    );
    runJsonSpy.mockRestore();
  });
});

/* ------------------------------------------------------------------ */
/*  getTargetConfig – covers lines 206-219 (including error at 213-214)  */
/* ------------------------------------------------------------------ */
describe('Storage - getTargetConfig', () => {
  function makeProject(targets: Record<string, { type: string; version: string; environments: Record<string, any> }>, defaultTarget = 'default'): ProjectFileV5 {
    return {
      version: 5,
      name: 'test-project',
      targets: targets as any,
      defaultTarget,
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
  }

  test('returns the default target when scope is undefined', () => {
    const project = makeProject({
      default: { type: 'unity', version: '1.0.0', environments: { dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true } } },
    });
    const target = getTargetConfig(project);
    expect(target.type).toBe('unity');
  });

  test('returns the default target when scope is null', () => {
    const project = makeProject({
      default: { type: 'hive', version: '2.0.0', environments: {} },
    });
    const target = getTargetConfig(project, null);
    expect(target.type).toBe('hive');
  });

  test('returns a named target when scope is provided', () => {
    const project = makeProject({
      default: { type: 'unity', version: '1.0.0', environments: {} },
      secondary: { type: 'postgres', version: '3.0.0', environments: {} },
    });
    const target = getTargetConfig(project, 'secondary');
    expect(target.type).toBe('postgres');
  });

  test('falls back to "default" when defaultTarget is empty string', () => {
    const project = makeProject(
      { default: { type: 'unity', version: '1.0.0', environments: {} } },
      ''
    );
    // scope is falsy, defaultTarget is '', so resolved = 'default'
    const target = getTargetConfig(project, null);
    expect(target.type).toBe('unity');
  });

  test('throws with available targets when target not found', () => {
    const project = makeProject({
      alpha: { type: 'unity', version: '1.0.0', environments: {} },
      beta: { type: 'hive', version: '1.0.0', environments: {} },
    });
    expect(() => getTargetConfig(project, 'missing')).toThrow(
      /Target 'missing' not found.*Available targets: alpha, beta/
    );
  });

  test('throws when defaultTarget itself does not exist in targets map', () => {
    const project = makeProject(
      { alpha: { type: 'unity', version: '1.0.0', environments: {} } },
      'nonexistent'
    );
    expect(() => getTargetConfig(project)).toThrow(/Target 'nonexistent' not found/);
  });
});

/* ------------------------------------------------------------------ */
/*  getEnvironmentConfig with explicit scope                           */
/* ------------------------------------------------------------------ */
describe('Storage - getEnvironmentConfig with scope', () => {
  test('retrieves env config from a non-default target via scope', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: { topLevelName: 'default_dev', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true },
          },
        },
        warehouse: {
          type: 'postgres',
          version: '2.0.0',
          environments: {
            staging: { topLevelName: 'wh_staging', allowDrift: false, requireSnapshot: true, autoCreateTopLevel: false, autoCreateSchemaxSchema: false },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };

    const config = getEnvironmentConfig(project, 'staging', 'warehouse');
    expect(config.topLevelName).toBe('wh_staging');
    expect(config.autoCreateSchemaxSchema).toBe(false);
  });

  test('throws when environment not in the scoped target', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };

    expect(() => getEnvironmentConfig(project, 'prod')).toThrow(/Environment 'prod' not found/);
  });
});

/* ------------------------------------------------------------------ */
/*  ensureCatalogMappingsForNewCatalogs – extra edge cases             */
/* ------------------------------------------------------------------ */
describe('Storage - ensureCatalogMappingsForNewCatalogs edge cases', () => {
  function baseProject(): ProjectFileV5 {
    return {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true },
          },
        },
        secondary: {
          type: 'postgres',
          version: '1.0.0',
          environments: {
            staging: { topLevelName: 'staging_c', allowDrift: false, requireSnapshot: true, autoCreateTopLevel: true, autoCreateSchemaxSchema: true },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
  }

  test('returns unchanged project when ops list is empty', () => {
    const project = baseProject();
    const result = ensureCatalogMappingsForNewCatalogs(project, []);
    expect(result.updated).toBe(false);
    expect(result.project).toBe(project);
  });

  test('returns unchanged project when ops have no add_catalog', () => {
    const project = baseProject();
    const ops: Operation[] = [
      { id: 'op_1', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_schema', target: 's1', payload: { name: 'my_schema' } },
    ];
    const result = ensureCatalogMappingsForNewCatalogs(project, ops);
    expect(result.updated).toBe(false);
  });

  test('uses scope to update a non-default target', () => {
    const project = baseProject();
    const ops: Operation[] = [
      { id: 'op_2', ts: '2026-01-01T00:00:00Z', provider: 'postgres', op: 'postgres.add_catalog', target: 'c1', payload: { name: 'analytics' } },
    ];
    const result = ensureCatalogMappingsForNewCatalogs(project, ops, 'secondary');
    expect(result.updated).toBe(true);
    expect(result.project.targets['secondary'].environments.staging.catalogMappings?.['analytics']).toBe('staging_analytics');
    // default target should be untouched
    expect(result.project.targets['default']).toEqual(project.targets['default']);
  });

  test('sanitizes special characters in catalog name for physical mapping', () => {
    const project = baseProject();
    const ops: Operation[] = [
      { id: 'op_3', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_catalog', target: 'c1', payload: { name: 'my-catalog.v2' } },
    ];
    const result = ensureCatalogMappingsForNewCatalogs(project, ops);
    expect(result.updated).toBe(true);
    // special chars replaced with underscore
    expect(result.project.targets['default'].environments.dev.catalogMappings?.['my-catalog.v2']).toBe('dev_my_catalog_v2');
  });

  test('handles environments with no prior catalogMappings key', () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'test-project',
      targets: {
        default: {
          type: 'unity',
          version: '1.0.0',
          environments: {
            dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true },
          },
        },
      },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    // env has no catalogMappings at all
    const ops: Operation[] = [
      { id: 'op_4', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_catalog', target: 'c1', payload: { name: 'newcat' } },
    ];
    const result = ensureCatalogMappingsForNewCatalogs(project, ops);
    expect(result.updated).toBe(true);
    expect(result.project.targets['default'].environments.dev.catalogMappings?.['newcat']).toBe('dev_newcat');
  });

  test('deduplicates multiple add_catalog ops for the same catalog name', () => {
    const project = baseProject();
    const ops: Operation[] = [
      { id: 'op_5a', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_catalog', target: 'c1', payload: { name: 'dup_cat' } },
      { id: 'op_5b', ts: '2026-01-01T00:00:01Z', provider: 'unity', op: 'unity.add_catalog', target: 'c1', payload: { name: 'dup_cat' } },
    ];
    const result = ensureCatalogMappingsForNewCatalogs(project, ops);
    expect(result.updated).toBe(true);
    expect(result.project.targets['default'].environments.dev.catalogMappings?.['dup_cat']).toBe('dev_dup_cat');
  });
});

/* ------------------------------------------------------------------ */
/*  normalizeProviderCapabilities – additional branches                */
/* ------------------------------------------------------------------ */
describe('Storage - normalizeProviderCapabilities additional branches', () => {
  test('normalizes camelCase fields (supportedOperations, supportedObjectTypes)', () => {
    const normalized = normalizeProviderCapabilities({
      supportedOperations: ['hive.add_db'],
      supportedObjectTypes: ['database'],
      hierarchy: {
        levels: [
          { name: 'db', displayName: 'Database', pluralName: 'databases', icon: 'folder', isContainer: true },
        ],
      },
      features: { materializedViews: false },
    });
    expect(normalized.supportedOperations).toEqual(['hive.add_db']);
    expect(normalized.supportedObjectTypes).toEqual(['database']);
    expect(normalized.hierarchy.levels?.[0]?.displayName).toBe('Database');
    expect(normalized.hierarchy.levels?.[0]?.pluralName).toBe('databases');
    expect(normalized.hierarchy.levels?.[0]?.isContainer).toBe(true);
    expect(normalized.features).toEqual({ materializedViews: false });
  });

  test('filters out non-boolean feature values', () => {
    const normalized = normalizeProviderCapabilities({
      features: { valid: true, invalid_string: 'yes' as any, invalid_number: 42 as any },
    });
    expect(normalized.features).toEqual({ valid: true });
  });

  test('handles null features gracefully', () => {
    const normalized = normalizeProviderCapabilities({ features: null as any });
    expect(normalized.features).toEqual({});
  });

  test('handles hierarchy with empty levels array', () => {
    const normalized = normalizeProviderCapabilities({
      hierarchy: { levels: [] },
    });
    expect(normalized.hierarchy.levels).toEqual([]);
  });

  test('handles hierarchy without levels key', () => {
    const normalized = normalizeProviderCapabilities({
      hierarchy: {},
    });
    expect(normalized.hierarchy.levels).toEqual([]);
  });

  test('handles level with missing optional fields', () => {
    const normalized = normalizeProviderCapabilities({
      hierarchy: {
        levels: [{ name: 'table' }],
      },
    });
    const level = normalized.hierarchy.levels?.[0];
    expect(level?.name).toBe('table');
    expect(level?.displayName).toBeUndefined();
    expect(level?.pluralName).toBeUndefined();
    expect(level?.icon).toBeUndefined();
    expect(level?.isContainer).toBeUndefined();
  });

  test('prefers snake_case over camelCase when both present', () => {
    const normalized = normalizeProviderCapabilities({
      supported_operations: ['snake_op'],
      supportedOperations: ['camel_op'],
      supported_object_types: ['snake_type'],
      supportedObjectTypes: ['camel_type'],
    });
    expect(normalized.supportedOperations).toEqual(['snake_op']);
    expect(normalized.supportedObjectTypes).toEqual(['snake_type']);
  });
});

/* ------------------------------------------------------------------ */
/*  loadCurrentState – additional edge cases                           */
/* ------------------------------------------------------------------ */
describe('Storage - loadCurrentState edge cases', () => {
  test('throws with default error code when errors array is empty', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'error',
        data: null,
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 1 },
      });

    await expect(
      loadCurrentState(vscode.Uri.file('/tmp/ws-err-default'), false)
    ).rejects.toThrow('[WORKSPACE_STATE_FAILED]');
    runJsonSpy.mockRestore();
  });

  test('throws when backend returns success but state is missing', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: null,
          changelog: { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' },
          provider: { id: 'unity', name: 'Unity', version: '1.0.0', capabilities: {} },
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    await expect(
      loadCurrentState(vscode.Uri.file('/tmp/ws-no-state'), false)
    ).rejects.toThrow();
    runJsonSpy.mockRestore();
  });

  test('throws when backend returns success but provider is missing', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' },
          provider: null,
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    await expect(
      loadCurrentState(vscode.Uri.file('/tmp/ws-no-provider'), false)
    ).rejects.toThrow();
    runJsonSpy.mockRestore();
  });

  test('returns null validationResult when validation is absent', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' },
          provider: { id: 'unity', name: 'Unity', version: '1.0.0', capabilities: {} },
          validation: null,
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    const result = await loadCurrentState(vscode.Uri.file('/tmp/ws-no-validation'), false);
    expect(result.validationResult).toBeNull();
    runJsonSpy.mockRestore();
  });

  test('maps validation error objects without message to fallback string', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: [] },
          changelog: { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' },
          provider: { id: 'unity', name: 'Unity', version: '1.0.0', capabilities: {} },
          validation: {
            errors: [{ code: 'ERR' }],   // object with no 'message' field
            warnings: [{ code: 'WARN' }],
          },
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    const result = await loadCurrentState(vscode.Uri.file('/tmp/ws-val-fallback'), false);
    expect(result.validationResult?.errors).toEqual(['Unknown validation error']);
    expect(result.validationResult?.warnings).toEqual(['Unknown validation warning']);
    runJsonSpy.mockRestore();
  });

  test('cached result is a deep clone (mutations do not corrupt cache)', async () => {
    const runJsonSpy = jest
      .spyOn(PythonBackendClient.prototype, 'runJson')
      .mockResolvedValueOnce({
        schemaVersion: '1',
        command: 'workspace-state',
        status: 'success',
        data: {
          state: { catalogs: ['a'] },
          changelog: { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' },
          provider: { id: 'unity', name: 'Unity', version: '1.0.0', capabilities: {} },
          validation: null,
        },
        warnings: [],
        errors: [],
        meta: { durationMs: 1, executedCommand: 'schemax workspace-state --json', exitCode: 0 },
      });

    const r1 = await loadCurrentState(vscode.Uri.file('/tmp/ws-clone-test'), false);
    // mutate returned state
    (r1.state as any).catalogs.push('MUTATED');

    // second call should come from cache but be a fresh clone
    const r2 = await loadCurrentState(vscode.Uri.file('/tmp/ws-clone-test'), false);
    expect((r2.state as any).catalogs).toEqual(['a']);
    expect(runJsonSpy).toHaveBeenCalledTimes(1);
    runJsonSpy.mockRestore();
  });
});

/* ------------------------------------------------------------------ */
/*  Async filesystem-based functions (using vscode.workspace.fs mock)  */
/* ------------------------------------------------------------------ */
/* eslint-disable @typescript-eslint/no-explicit-any */
const fsMock = (vscode.workspace as any).fs as {
  readFile: jest.Mock<any>;
  writeFile: jest.Mock<any>;
  stat: jest.Mock<any>;
  createDirectory: jest.Mock<any>;
};

describe('Storage - ensureSchemaxDir', () => {
  beforeEach(() => {
    fsMock.createDirectory.mockReset();
  });

  test('creates .schemax and snapshots directories', async () => {
    fsMock.createDirectory.mockResolvedValue(undefined);
    const uri = vscode.Uri.file('/tmp/ws-dir');
    await ensureSchemaxDir(uri);
    expect(fsMock.createDirectory).toHaveBeenCalledTimes(2);
  });

  test('ignores errors if directories already exist', async () => {
    fsMock.createDirectory.mockRejectedValue(new Error('exists'));
    const uri = vscode.Uri.file('/tmp/ws-dir-exists');
    // should not throw
    await ensureSchemaxDir(uri);
  });
});

describe('Storage - readProject', () => {
  beforeEach(() => {
    fsMock.readFile.mockReset();
    fsMock.writeFile.mockReset();
  });

  test('reads a v5 project file directly', async () => {
    const v5: ProjectFileV5 = {
      version: 5,
      name: 'myproject',
      targets: { default: { type: 'unity', version: '1.0.0', environments: {} } },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(v5)));
    const result = await readProject(vscode.Uri.file('/tmp/ws-read-v5'));
    expect(result.version).toBe(5);
    expect(result.name).toBe('myproject');
  });

  test('auto-migrates a v4 project to v5 and persists', async () => {
    const v4 = {
      version: 4,
      name: 'legacyproject',
      provider: { type: 'hive', version: '1.0.0', environments: { dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true } } },
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(v4)));
    fsMock.writeFile.mockResolvedValueOnce(undefined);

    const result = await readProject(vscode.Uri.file('/tmp/ws-read-v4'));
    expect(result.version).toBe(5);
    expect(result.targets['default'].type).toBe('hive');
    expect(result.defaultTarget).toBe('default');
    // Should have written the migrated file
    expect(fsMock.writeFile).toHaveBeenCalledTimes(1);
  });

  test('throws for unsupported project version', async () => {
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify({ version: 3 })));
    await expect(readProject(vscode.Uri.file('/tmp/ws-read-v3'))).rejects.toThrow(
      /version 3 not supported/
    );
  });

  test('throws descriptive error when file not found', async () => {
    const err: any = new Error('not found');
    err.code = 'FileNotFound';
    fsMock.readFile.mockRejectedValueOnce(err);
    await expect(readProject(vscode.Uri.file('/tmp/ws-no-file'))).rejects.toThrow(
      /Project file not found/
    );
  });

  test('rethrows non-FileNotFound errors', async () => {
    fsMock.readFile.mockRejectedValueOnce(new Error('permission denied'));
    await expect(readProject(vscode.Uri.file('/tmp/ws-perm'))).rejects.toThrow('permission denied');
  });
});

describe('Storage - writeProject', () => {
  beforeEach(() => {
    fsMock.writeFile.mockReset();
  });

  test('writes project as JSON to filesystem', async () => {
    fsMock.writeFile.mockResolvedValue(undefined);
    const project: ProjectFileV5 = {
      version: 5,
      name: 'wp-test',
      targets: { default: { type: 'unity', version: '1.0.0', environments: {} } },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    await writeProject(vscode.Uri.file('/tmp/ws-write-proj'), project);
    expect(fsMock.writeFile).toHaveBeenCalled();
  });
});

describe('Storage - readChangelog', () => {
  beforeEach(() => {
    fsMock.readFile.mockReset();
    fsMock.writeFile.mockReset();
  });

  test('reads existing changelog', async () => {
    const cl: ChangelogFile = { version: 1, sinceSnapshot: 'v0.1.0', ops: [], lastModified: '2026-01-01T00:00:00Z' };
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(cl)));
    const result = await readChangelog(vscode.Uri.file('/tmp/ws-cl'));
    expect(result.version).toBe(1);
    expect(result.sinceSnapshot).toBe('v0.1.0');
  });

  test('creates empty changelog when file not found', async () => {
    const err: any = new Error('not found');
    err.code = 'FileNotFound';
    fsMock.readFile.mockRejectedValueOnce(err);
    fsMock.writeFile.mockResolvedValueOnce(undefined);

    const result = await readChangelog(vscode.Uri.file('/tmp/ws-cl-new'));
    expect(result.version).toBe(1);
    expect(result.sinceSnapshot).toBeNull();
    expect(result.ops).toEqual([]);
    expect(fsMock.writeFile).toHaveBeenCalled();
  });

  test('rethrows non-FileNotFound errors', async () => {
    fsMock.readFile.mockRejectedValueOnce(new Error('disk error'));
    await expect(readChangelog(vscode.Uri.file('/tmp/ws-cl-err'))).rejects.toThrow('disk error');
  });
});

describe('Storage - writeChangelog', () => {
  beforeEach(() => {
    fsMock.writeFile.mockReset();
  });

  test('writes changelog and updates lastModified', async () => {
    fsMock.writeFile.mockResolvedValue(undefined);
    const cl: ChangelogFile = { version: 1, sinceSnapshot: null, ops: [], lastModified: '2025-01-01T00:00:00Z' };
    await writeChangelog(vscode.Uri.file('/tmp/ws-wcl'), cl);
    // lastModified should have been updated
    expect(cl.lastModified).not.toBe('2025-01-01T00:00:00Z');
    expect(fsMock.writeFile).toHaveBeenCalled();
  });
});

describe('Storage - readSnapshot', () => {
  beforeEach(() => {
    fsMock.readFile.mockReset();
  });

  test('reads a snapshot file by version', async () => {
    const snap: SnapshotFile = {
      id: 'snap_123',
      version: 'v0.1.0',
      name: 'initial',
      ts: '2026-01-01T00:00:00Z',
      state: { catalogs: [] },
      operations: [],
      previousSnapshot: null,
      hash: 'abc',
      tags: [],
    };
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(snap)));
    const result = await readSnapshot(vscode.Uri.file('/tmp/ws-snap'), 'v0.1.0');
    expect(result.id).toBe('snap_123');
    expect(result.version).toBe('v0.1.0');
  });
});

describe('Storage - writeSnapshot', () => {
  beforeEach(() => {
    fsMock.writeFile.mockReset();
    fsMock.createDirectory.mockReset();
  });

  test('writes a snapshot file after ensuring directory', async () => {
    fsMock.createDirectory.mockResolvedValue(undefined);
    fsMock.writeFile.mockResolvedValue(undefined);
    const snap: SnapshotFile = {
      id: 'snap_456',
      version: 'v0.2.0',
      name: 'second',
      ts: '2026-02-01T00:00:00Z',
      state: { catalogs: ['a'] },
      operations: [],
      previousSnapshot: 'v0.1.0',
      hash: 'def',
      tags: ['release'],
    };
    await writeSnapshot(vscode.Uri.file('/tmp/ws-wsnap'), snap);
    expect(fsMock.createDirectory).toHaveBeenCalled();
    expect(fsMock.writeFile).toHaveBeenCalled();
  });
});

describe('Storage - ensureProjectFile', () => {
  beforeEach(() => {
    fsMock.readFile.mockReset();
    fsMock.writeFile.mockReset();
    fsMock.stat.mockReset();
    fsMock.createDirectory.mockReset();
  });

  test('does nothing when v5 project already exists', async () => {
    const v5 = { version: 5 };
    fsMock.stat.mockResolvedValueOnce({});
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(v5)));

    const outputChannel = { appendLine: jest.fn(), show: jest.fn() } as any;
    await ensureProjectFile(vscode.Uri.file('/tmp/ws-epf-v5'), outputChannel);
    // Should not write anything
    expect(fsMock.writeFile).not.toHaveBeenCalled();
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('already exists (v5)'));
  });

  test('auto-migrates v4 to v5 when existing project is v4', async () => {
    const v4 = {
      version: 4,
      name: 'old',
      provider: { type: 'unity', version: '1.0.0', environments: {} },
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    fsMock.stat.mockResolvedValueOnce({});
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify(v4)));
    fsMock.writeFile.mockResolvedValue(undefined);

    const outputChannel = { appendLine: jest.fn(), show: jest.fn() } as any;
    await ensureProjectFile(vscode.Uri.file('/tmp/ws-epf-v4'), outputChannel);
    expect(fsMock.writeFile).toHaveBeenCalledTimes(1);
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('Auto-migrated'));
  });

  test('throws for unsupported project version', async () => {
    fsMock.stat.mockResolvedValueOnce({});
    fsMock.readFile.mockResolvedValueOnce(Buffer.from(JSON.stringify({ version: 2 })));

    const outputChannel = { appendLine: jest.fn(), show: jest.fn() } as any;
    await expect(
      ensureProjectFile(vscode.Uri.file('/tmp/ws-epf-v2'), outputChannel)
    ).rejects.toThrow(/version 2 not supported/);
  });

  test('creates new v5 project when file does not exist', async () => {
    const err: any = new Error('not found');
    err.code = 'FileNotFound';
    fsMock.stat.mockRejectedValueOnce(err);
    fsMock.createDirectory.mockResolvedValue(undefined);
    fsMock.writeFile.mockResolvedValue(undefined);

    const outputChannel = { appendLine: jest.fn(), show: jest.fn() } as any;
    await ensureProjectFile(vscode.Uri.file('/tmp/ws-new-project'), outputChannel, 'hive');
    // Should write project file + changelog file
    expect(fsMock.writeFile).toHaveBeenCalledTimes(2);
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('Initialized new v5 project'));
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('Hive Metastore'));
  });

  test('creates new v5 project with default unity provider', async () => {
    const err: any = new Error('not found');
    err.code = 'FileNotFound';
    fsMock.stat.mockRejectedValueOnce(err);
    fsMock.createDirectory.mockResolvedValue(undefined);
    fsMock.writeFile.mockResolvedValue(undefined);

    const outputChannel = { appendLine: jest.fn(), show: jest.fn() } as any;
    await ensureProjectFile(vscode.Uri.file('/tmp/ws-new-unity'), outputChannel);
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('Unity Catalog'));
    expect(outputChannel.appendLine).toHaveBeenCalledWith(expect.stringContaining('Environments: dev, test, prod'));
  });
});

describe('Storage - appendOps', () => {
  beforeEach(() => {
    fsMock.readFile.mockReset();
    fsMock.writeFile.mockReset();
  });

  test('appends operations to existing changelog', async () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'aop-test',
      targets: { default: { type: 'unity', version: '1.0.0', environments: { dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true } } } },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    const changelog: ChangelogFile = { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' };

    // readProject reads project.json
    fsMock.readFile
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(project)))
      // readChangelog reads changelog.json
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(changelog)));
    fsMock.writeFile.mockResolvedValue(undefined);

    const ops: Operation[] = [
      { id: 'op_1', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_schema', target: 'my_catalog.my_schema', payload: { name: 'my_schema' } },
    ];
    await appendOps(vscode.Uri.file('/tmp/ws-append'), ops);
    expect(fsMock.writeFile).toHaveBeenCalled();
  });

  test('throws on invalid operation missing required fields', async () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'aop-invalid',
      targets: { default: { type: 'unity', version: '1.0.0', environments: {} } },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    const changelog: ChangelogFile = { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' };

    fsMock.readFile
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(project)))
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(changelog)));

    const badOps: Operation[] = [
      { id: 'op_bad', ts: '', provider: '', op: '', target: '', payload: {} },
    ];
    await expect(appendOps(vscode.Uri.file('/tmp/ws-bad-ops'), badOps)).rejects.toThrow(
      /Invalid operation.*missing required field/
    );
  });

  test('also updates project when ops contain add_catalog for unmapped catalog', async () => {
    const project: ProjectFileV5 = {
      version: 5,
      name: 'aop-catalog',
      targets: { default: { type: 'unity', version: '1.0.0', environments: { dev: { topLevelName: 'dev_c', allowDrift: true, requireSnapshot: false, autoCreateTopLevel: true, autoCreateSchemaxSchema: true } } } },
      defaultTarget: 'default',
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };
    const changelog: ChangelogFile = { version: 1, sinceSnapshot: null, ops: [], lastModified: '2026-01-01T00:00:00Z' };

    fsMock.readFile
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(project)))
      .mockResolvedValueOnce(Buffer.from(JSON.stringify(changelog)));
    fsMock.writeFile.mockResolvedValue(undefined);

    const ops: Operation[] = [
      { id: 'op_cat', ts: '2026-01-01T00:00:00Z', provider: 'unity', op: 'unity.add_catalog', target: 'new_cat', payload: { name: 'new_cat' } },
    ];
    await appendOps(vscode.Uri.file('/tmp/ws-append-cat'), ops);
    // Should have written both changelog and project (2 writes, but coalescing may affect count)
    expect(fsMock.writeFile).toHaveBeenCalled();
  });
});
