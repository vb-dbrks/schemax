/**
 * E2E test: UI (store) → capture ops → write .schemax workspace → run CLI (schemax apply) → live env.
 *
 * This test drives the same code path the Designer UI uses: the Zustand store produces ops
 * (add_catalog, add_schema, add_table, add_column, add_function) and posts append-ops. We capture
 * those ops, write project.json + changelog.json to a temp workspace, then run `schemax apply`
 * against a live Databricks workspace (when configured).
 *
 * Skips when SCHEMAX_RUN_LIVE_COMMAND_TESTS is not set or Databricks env (profile, warehouse-id)
 * is missing. Requires: SCHEMAX_RUN_LIVE_COMMAND_TESTS=1, and either SCHEMAX_E2E_PROFILE +
 * SCHEMAX_E2E_WAREHOUSE_ID or the same env vars used by Python live tests.
 */

import React from 'react';
import { render, waitFor, act } from '@testing-library/react';
import { describe, test, expect, beforeEach, afterEach } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';
import { App } from '../../../src/webview/App';
import { useDesignerStore } from '../../../src/webview/state/useDesignerStore';

const capturedOps = (): unknown[] => (global as any).__capturedOps ?? [];

function dispatchProjectLoaded(payload: Record<string, unknown>) {
  window.dispatchEvent(
    new MessageEvent('message', {
      data: { type: 'project-loaded', payload },
    })
  );
}

const minimalProjectPayload = (logicalCatalogName: string, physicalCatalog: string, trackingCatalog: string) => ({
  version: 4,
  name: 'e2e_ui',
  state: { catalogs: [] },
  ops: [],
  provider: {
    id: 'unity',
    name: 'Unity Catalog',
    version: '1.0.0',
    type: 'unity',
    capabilities: {},
    environments: {
      dev: {
        topLevelName: trackingCatalog,
        catalogMappings: { [logicalCatalogName]: physicalCatalog },
      },
    },
  },
  snapshots: [],
  latestSnapshot: null,
});

describe('E2E: UI → CLI → live environment', () => {
  let tmpdir: string;

  beforeEach(() => {
    (global as any).__capturedOps = [];
    tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'schemax-e2e-ui-'));
  });

  afterEach(() => {
    try {
      fs.rmSync(tmpdir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  test('drives store (catalog + schema + table + column + function), writes workspace, runs apply when live env configured', async () => {
    const logicalCatalog = 'e2e_cat';
    const suffix = `jest_${Date.now()}`;
    const physicalCatalog = `schemax_e2e_ui_${suffix}`;
    const trackingCatalog = `schemax_e2e_track_${suffix}`;

    render(<App />);
    await act(() => {
      dispatchProjectLoaded(minimalProjectPayload(logicalCatalog, physicalCatalog, trackingCatalog));
    });

    await waitFor(() => {
      const s = useDesignerStore.getState();
      expect(s.project).toBeTruthy();
      expect(s.provider).toBeTruthy();
    });

    await act(() => {
      const store = useDesignerStore.getState();
      store.addCatalog(logicalCatalog);
    });
    const catalogId = (capturedOps()[0] as { payload: { catalogId: string } }).payload.catalogId;

    await act(() => {
      useDesignerStore.getState().addSchema(catalogId, 'core');
    });
    const schemaId = (capturedOps()[1] as { payload: { schemaId: string } }).payload.schemaId;

    await act(() => {
      useDesignerStore.getState().addTable(schemaId, 'e2e_table', 'delta');
    });
    const tableId = (capturedOps()[2] as { payload: { tableId: string } }).payload.tableId;

    await act(() => {
      useDesignerStore.getState().addColumn(tableId, 'id', 'BIGINT', false, undefined, undefined);
    });
    await act(() => {
      useDesignerStore.getState().addFunction(schemaId, 'e2e_func', 'SQL', '1', { returnType: 'INT' });
    });

    const ops = capturedOps();
    expect(ops.length).toBeGreaterThanOrEqual(5);

    const schemaxDir = path.join(tmpdir, '.schemax');
    fs.mkdirSync(schemaxDir, { recursive: true });

    const project = {
      version: 4,
      name: 'e2e_ui',
      provider: {
        type: 'unity',
        version: '1.0.0',
        environments: {
          dev: {
            topLevelName: trackingCatalog,
            catalogMappings: { [logicalCatalog]: physicalCatalog },
            description: 'Development',
            allowDrift: true,
            requireSnapshot: false,
            autoCreateTopLevel: true,
            autoCreateSchemaxSchema: true,
          },
        },
      },
      snapshots: [],
      deployments: [],
      settings: { autoIncrementVersion: true, versionPrefix: 'v' },
      latestSnapshot: null,
    };

    const changelog = {
      version: 1,
      sinceSnapshot: null,
      ops: ops.map((op: any) => ({
        id: op.id,
        ts: op.ts,
        provider: op.provider,
        op: op.op,
        target: op.target,
        payload: op.payload,
      })),
      lastModified: new Date().toISOString(),
    };

    fs.writeFileSync(path.join(schemaxDir, 'project.json'), JSON.stringify(project, null, 2));
    fs.writeFileSync(path.join(schemaxDir, 'changelog.json'), JSON.stringify(changelog, null, 2));

    const runLive = process.env.SCHEMAX_RUN_LIVE_COMMAND_TESTS === '1';
    const profile = process.env.SCHEMAX_E2E_PROFILE || process.env.DATABRICKS_CONFIG_PROFILE;
    const warehouseId = process.env.SCHEMAX_E2E_WAREHOUSE_ID || process.env.DATABRICKS_WAREHOUSE_ID;

    if (!runLive || !profile || !warehouseId) {
      expect(ops.length).toBeGreaterThanOrEqual(5);
      return;
    }

    const sdkRoot = path.resolve(__dirname, '../../../../python-sdk');
    const result = spawnSync(
      'uv',
      [
        'run',
        'schemax',
        'apply',
        '--target',
        'dev',
        '--profile',
        profile,
        '--warehouse-id',
        warehouseId,
        '--no-interaction',
        tmpdir,
      ],
      {
        cwd: sdkRoot,
        encoding: 'utf-8',
        env: { ...process.env, SCHEMAX_RUN_LIVE_COMMAND_TESTS: '1' },
      }
    );

    expect(result.status).toBe(0);
    if (result.status !== 0) {
      throw new Error(`schemax apply failed: ${result.stderr || result.stdout}`);
    }
  });
});
