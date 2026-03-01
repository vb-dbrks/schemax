/**
 * Tests for storage-v4.ts - V4 storage layer with multi-environment support
 */

import { describe, test, expect, jest } from '@jest/globals';
import * as vscode from 'vscode';
import {
  ensureCatalogMappingsForNewCatalogs,
  getEnvironmentConfig,
  loadCurrentState,
  normalizeProviderCapabilities,
  ProjectFileV4,
} from '../../src/storage-v4';
import { PythonBackendClient } from '../../src/backend/pythonBackendClient';
import type { Operation } from '../../src/contracts/workspace';

describe('Storage V4 - Environment Configuration', () => {
  test('should retrieve environment configuration', () => {
    const project: ProjectFileV4 = {
      version: 4,
      name: 'test-project',
      provider: {
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
    const project: ProjectFileV4 = {
      version: 4,
      name: 'test-project',
      provider: {
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
    const project: ProjectFileV4 = {
      version: 4,
      name: 'test-project',
      provider: {
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

describe('Storage V4 - Project Schema', () => {
  test('should validate v4 project structure', () => {
    const project: ProjectFileV4 = {
      version: 4,
      name: 'test-project',
      provider: {
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
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
      },
      latestSnapshot: null,
    };

    expect(project.version).toBe(4);
    expect(project.provider.type).toBe('unity');
    expect(project.provider.environments).toHaveProperty('dev');
  });

  test('should handle empty snapshots and deployments', () => {
    const project: ProjectFileV4 = {
      version: 4,
      name: 'test-project',
      provider: {
        type: 'unity',
        version: '1.0.0',
        environments: {},
      },
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

describe('Storage V4 - Catalog mapping defaults', () => {
  function baseProject(): ProjectFileV4 {
    return {
      version: 4,
      name: 'test-project',
      provider: {
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
    expect(result.project.provider.environments.dev.catalogMappings?.['bronze-layer']).toBe(
      'dev_bronze_layer'
    );
    expect(result.project.provider.environments.prod.catalogMappings?.['bronze-layer']).toBe(
      'prod_bronze_layer'
    );
    expect(result.project.provider.environments.prod.catalogMappings?.existing).toBe(
      'prod_existing'
    );
  });

  test('does not update when all mappings already exist', () => {
    const project = baseProject();
    project.provider.environments.dev.catalogMappings = {
      'bronze-layer': 'dev_bronze_layer',
    };
    project.provider.environments.prod.catalogMappings = {
      ...project.provider.environments.prod.catalogMappings,
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

describe('Storage V4 - Provider capability normalization', () => {
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

describe('Storage V4 - Python transport contract', () => {
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
      'boom'
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
