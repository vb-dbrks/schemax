/**
 * Tests for storage-v4.ts - V4 storage layer with multi-environment support
 */

import { describe, test, expect } from '@jest/globals';
import { getEnvironmentConfig, ProjectFileV4 } from '../../src/storage-v4';

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
            catalog: 'dev_catalog',
            allowDrift: true,
            requireSnapshot: false,
            autoCreateCatalog: true,
            autoCreateSchematicSchema: true,
          },
          prod: {
            catalog: 'prod_catalog',
            allowDrift: false,
            requireSnapshot: true,
            autoCreateCatalog: false,
            autoCreateSchematicSchema: true,
          },
        },
      },
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
        catalogMode: 'single',
      },
      latestSnapshot: null,
    };

    const devConfig = getEnvironmentConfig(project, 'dev');
    expect(devConfig.catalog).toBe('dev_catalog');
    expect(devConfig.allowDrift).toBe(true);
    expect(devConfig.requireSnapshot).toBe(false);

    const prodConfig = getEnvironmentConfig(project, 'prod');
    expect(prodConfig.catalog).toBe('prod_catalog');
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
            catalog: 'dev_catalog',
            allowDrift: true,
            requireSnapshot: false,
            autoCreateCatalog: true,
            autoCreateSchematicSchema: true,
          },
        },
      },
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
        catalogMode: 'single',
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
            catalog: 'dev_catalog',
            allowDrift: true,
            requireSnapshot: false,
            autoCreateCatalog: true,
            autoCreateSchematicSchema: true,
          },
          test: {
            catalog: 'test_catalog',
            allowDrift: false,
            requireSnapshot: true,
            autoCreateCatalog: true,
            autoCreateSchematicSchema: true,
          },
          prod: {
            catalog: 'prod_catalog',
            allowDrift: false,
            requireSnapshot: true,
            autoCreateCatalog: false,
            autoCreateSchematicSchema: true,
          },
        },
      },
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
        catalogMode: 'single',
      },
      latestSnapshot: null,
    };

    const testConfig = getEnvironmentConfig(project, 'test');
    expect(testConfig.catalog).toBe('test_catalog');
    expect(testConfig.requireSnapshot).toBe(true);
    expect(testConfig.autoCreateCatalog).toBe(true);
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
            catalog: 'dev_catalog',
            allowDrift: true,
            requireSnapshot: false,
            autoCreateCatalog: true,
            autoCreateSchematicSchema: true,
          },
        },
      },
      snapshots: [],
      deployments: [],
      settings: {
        autoIncrementVersion: true,
        versionPrefix: 'v',
        catalogMode: 'single',
      },
      latestSnapshot: null,
    };

    expect(project.version).toBe(4);
    expect(project.provider.type).toBe('unity');
    expect(project.provider.environments).toHaveProperty('dev');
    expect(project.settings.catalogMode).toBe('single');
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
        catalogMode: 'single',
      },
      latestSnapshot: null,
    };

    expect(project.snapshots).toEqual([]);
    expect(project.deployments).toEqual([]);
    expect(project.latestSnapshot).toBeNull();
  });
});

